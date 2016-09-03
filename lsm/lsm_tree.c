/************************************************************************
* LSM TREE的创建、打开、关闭、compact等操作的定义
************************************************************************/
#include "wt_internal.h"

static int __lsm_tree_cleanup_old(WT_SESSION_IMPL *, const char*);
static int __lsm_tree_open_check(WT_SESSION_IMPL *, WT_LSM_TREE *);
static int __lsm_tree_open(WT_SESSION_IMPL *, const char *, WT_LSM_TREE **);
static int __lsm_tree_set_name(WT_SESSION_IMPL *, WT_LSM_TREE *, const char *);

/*释放内存中的lsm tree对象*/
static int __lsm_tree_discard(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, int final)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	u_int i;

	WT_UNUSED(final);

	/*从connection中的lsm queue中移除对象，防止其他事务引用*/
	if(F_ISSET(lsm_tree, WT_LSM_TREE_OPEN)){
		/*必须持有session handler list lock*/
		WT_ASSERT(session, final || F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));
		TAILQ_REMOVE(&S2C(session)->lsmqh, lsm_tree, q);
	}

	/*终止比较器任务*/
	if (lsm_tree->collator_owned && lsm_tree->collator->terminate != NULL)
		WT_TRET(lsm_tree->collator->terminate(lsm_tree->collator, &session->iface));

	__wt_free(session, lsm_tree->name);
	__wt_free(session, lsm_tree->config);
	__wt_free(session, lsm_tree->key_format);
	__wt_free(session, lsm_tree->value_format);
	__wt_free(session, lsm_tree->collator_name);
	__wt_free(session, lsm_tree->bloom_config);
	__wt_free(session, lsm_tree->file_config);

	/*释放掉lsm tree的读写锁对象*/
	WT_TRET(__wt_rwlock_destroy(session, &lsm_tree->rwlock));

	/*释放chunks数组占用的内存*/
	for(i = 0; i < lsm_tree->nchunks; i++){
		if ((chunk = lsm_tree->chunk[i]) == NULL)
			continue;

		__wt_free(session, chunk->bloom_uri);
		__wt_free(session, chunk->uri);
		__wt_free(session, chunk);
	}
	__wt_free(session, lsm_tree->chunk);

	/*释放old chunks*/
	for(i = 0; i < lsm_tree->nold_chunks; i ++){
		chunk = lsm_tree->old_chunks[i];
		WT_ASSERT(session, chunk != NULL);

		__wt_free(session, chunk->bloom_uri);
		__wt_free(session, chunk->uri);
		__wt_free(session, chunk);
	}
	__wt_free(session, lsm_tree->old_chunks);
	/*释放lsm tree对象*/
	__wt_free(session, lsm_tree);
	
	return ret;
}

/*关闭lsm tree*/
static int __lsm_tree_close(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree)
{
	WT_DECL_RET;
	int i;

	/*将lsm tree active状态清除掉，防止merger动作,只有在active状态下才会进行merge*/
	F_CLR(lsm_tree, WT_LSM_TREE_ACTIVE);

	/*反复进行尝试，直到lsm tree没有其他模块引用为止*/
	for(i = 0; lsm_tree->refcnt > 1 || lsm_tree->queue_ref > 0; ++i){
		/*相当于spin wait*/
		if (i % 1000 == 0) {
			/*从任务队列中清除掉正在等待处理这个lsm tree的任务,例如flush/drop file等 */
			WT_WITHOUT_LOCKS(session, ret = __wt_lsm_manager_clear_tree(session, lsm_tree));
			WT_RET(ret);
		}

		__wt_yield();
	}

	return 0;
}

/*撤销（先关闭，后释放）所有的lsm tree 对象, 一般用于停服时调用*/
int __wt_lsm_tree_close_all(WT_SESSION_IMPL* session)
{
	WT_DECL_RET;
	WT_LSM_TREE *lsm_tree;

	/*逐步从lsmqh队列中取出lsm tree来释放,一般是在数据库shtting down过程调用，不用进行锁保护*/
	while((lsm_tree = TAILQ_FIRST(&S2C(session)->lsmqh)) != NULL){
		/*引用计数增加，表示在释放这个lsm_tree*/
		WT_ATOMIC_ADD4(lsm_tree->refcnt, 1);
		WT_TRET(__lsm_tree_close(session, lsm_tree));
		WT_TRET(__lsm_tree_discard(session, lsm_tree, 1));
	}

	return ret;
}

/*修改lsm tree name*/
static int __lsm_tree_set_name(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, const char *uri)
{
	if (lsm_tree->name != NULL)
		__wt_free(session, lsm_tree->name);

	WT_RET(__wt_strdup(session, uri, &lsm_tree->name));
	lsm_tree->filename = lsm_tree->name + strlen("lsm:");

	return 0;
}

/*从lsm tree的filename构建出一个bloom uri,并通过retp返回*/
int __wt_lsm_tree_bloom_name(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, uint32_t id, const char **retp)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 0, &tmp));
	WT_ERR(__wt_buf_fmt(session, tmp, "file:%s-%06" PRIu32 ".bf", lsm_tree->filename, id));

	WT_ERR(__wt_strndup(session, tmp->data, tmp->size, retp));

err:	
	__wt_scr_free(session, &tmp);
	return ret;
}

/*设置chunk name*/
int __wt_lsm_tree_chunk_name(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, uint32_t id, const char **retp)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 0, &tmp));
	WT_ERR(__wt_buf_fmt(session, tmp, "file:%s-%06" PRIu32 ".lsm", lsm_tree->filename, id));
	WT_ERR(__wt_strndup(session, tmp->data, tmp->size, retp));

err:	
	__wt_scr_free(session, &tmp);
	return (ret);
}

/*设置chunk的chunk size*/
int __wt_lsm_tree_set_chunk_size(WT_SESSION_IMPL *session, WT_LSM_CHUNK *chunk)
{
	wt_off_t size;
	const char *filename;

	filename = chunk->uri;

	if (!WT_PREFIX_SKIP(filename, "file:"))
		WT_RET_MSG(session, EINVAL, "Expected a 'file:' URI: %s", chunk->uri);

	WT_RET(__wt_filesize_name(session, filename, &size));

	chunk->size = (uint64_t)size;

	return 0;
}

/*清除掉一个lsm chunk文件，这个文件可能是BTree文件，也有可能是bloom数据文件*/
static int __lsm_tree_cleanup_old(WT_SESSION_IMPL* session, const char* uri)
{
	WT_DECL_RET;
	const char *cfg[] = { WT_CONFIG_BASE(session, session_drop), "force", NULL };
	int exists;

	WT_RET(__wt_exist(session, uri + strlen("file:"), &exists));
	if (exists)
		WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_drop(session, uri, cfg));

	return ret;
}

/*初始化一个lsm tree的chunk信息, 主要包括：chunk name,设置时间戳、创建schema等*/
int __wt_lsm_tree_setup_chunk(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, WT_LSM_CHUNK *chunk)
{
	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED));
	/*获得chunk创建时间戳*/
	WT_RET(__wt_epoch(session, &chunk->create_ts));

	WT_RET(__wt_lsm_tree_chunk_name(session, lsm_tree, chunk->id, &chunk->uri));

	/*这个chunk前面已经使用过，将前面过去的产生文件删除*/
	if (chunk->id > 1)
		WT_RET(__lsm_tree_cleanup_old(session, chunk->uri));

	/*在schema层创建chunk*/
	return __wt_schema_create(session, chunk->uri, lsm_tree->file_config);
}

/*初始化lsm tree的bloom filter */
int __wt_lsm_tree_setup_bloom(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, WT_LSM_CHUNK *chunk)
{
	WT_DECL_RET;

	if(chunk->bloom_uri == NULL)
		WT_RET(__wt_lsm_tree_bloom_name(session, lsm_tree, chunk->id, &chunk->bloom_uri));
	/*清除掉chunk原来的bloom filter文件*/
	WT_RET(__lsm_tree_cleanup_old(session, chunk->bloom_uri));

	return ret;
}

/*创建一个LSM TREE，并将其meta信息写入到meta管理器中*/
int __wt_lsm_tree_create(WT_SESSION_IMPL *session, const char *uri, int exclusive, const char *config)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	WT_LSM_TREE *lsm_tree;
	const char *cfg[] = { WT_CONFIG_BASE(session, session_create), config, NULL };
	char *tmpconfig;
	
	/*判断uri对应的LSM TREE对象是否在meta管理器中,如果已经在管理器中，说明lsm tree对应已经存在*/
	WT_WITH_DHANDLE_LOCK(session, uri, 0, &lsm_tree);
	ret = __wt_lsm_tree_get(session, uri, 0, &lsm_tree);
	if(ret == 0){
		__wt_lsm_tree_release(session, lsm_tree);
		return (exclusive ? EEXIST : 0);
	}
	WT_RET_NOTFOUND_OK(ret);

	/*查找URI对应的meta信息是否存在，如果存在，说明lsm tree也已经建立*/
	if (__wt_metadata_search(session, uri, &tmpconfig) == 0) {
		__wt_free(session, tmpconfig);
		return (exclusive ? EEXIST : 0);
	}
	WT_RET_NOTFOUND_OK(ret);

	/*如果是可以新建一个lsm tree，那么必须先从配置文件读取其meta信息，并将meta写入meta管理中*/
	WT_RET(__wt_config_gets(session, cfg, "key_format", &cval));
	if (WT_STRING_MATCH("r", cval.str, cval.len))
		WT_RET_MSG(session, EINVAL,
		"LSM trees cannot be configured as column stores");

	WT_RET(__wt_calloc_one(session, &lsm_tree));
	/*对lsm tree进行配置*/
	WT_ERR(__lsm_tree_set_name(session, lsm_tree, uri));

	WT_ERR(__wt_config_gets(session, cfg, "key_format", &cval));
	WT_ERR(__wt_strndup(
		session, cval.str, cval.len, &lsm_tree->key_format));
	WT_ERR(__wt_config_gets(session, cfg, "value_format", &cval));
	WT_ERR(__wt_strndup(
		session, cval.str, cval.len, &lsm_tree->value_format));

	WT_ERR(__wt_config_gets_none(session, cfg, "collator", &cval));
	WT_ERR(__wt_strndup(
		session, cval.str, cval.len, &lsm_tree->collator_name));

	WT_ERR(__wt_config_gets(session, cfg, "lsm.auto_throttle", &cval));
	if (cval.val)
		F_SET(lsm_tree, WT_LSM_TREE_THROTTLE);
	else
		F_CLR(lsm_tree, WT_LSM_TREE_THROTTLE);
	WT_ERR(__wt_config_gets(session, cfg, "lsm.bloom", &cval));
	FLD_SET(lsm_tree->bloom, (cval.val == 0 ? WT_LSM_BLOOM_OFF : WT_LSM_BLOOM_MERGED));
	WT_ERR(__wt_config_gets(session, cfg, "lsm.bloom_oldest", &cval));
	if (cval.val != 0)
		FLD_SET(lsm_tree->bloom, WT_LSM_BLOOM_OLDEST);

	if (FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_OFF) &&
		FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_OLDEST))
		WT_ERR_MSG(session, EINVAL,
		"Bloom filters can only be created on newest and oldest "
		"chunks if bloom filters are enabled");

	WT_ERR(__wt_config_gets(session, cfg, "lsm.bloom_config", &cval));
	if (cval.type == WT_CONFIG_ITEM_STRUCT) {
		cval.str++;
		cval.len -= 2;
	}
	WT_ERR(__wt_config_check(session,
		WT_CONFIG_REF(session, session_create), cval.str, cval.len));
	WT_ERR(__wt_strndup(
		session, cval.str, cval.len, &lsm_tree->bloom_config));

	WT_ERR(__wt_config_gets(session, cfg, "lsm.bloom_bit_count", &cval));
	lsm_tree->bloom_bit_count = (uint32_t)cval.val;
	WT_ERR(__wt_config_gets(session, cfg, "lsm.bloom_hash_count", &cval));
	lsm_tree->bloom_hash_count = (uint32_t)cval.val;
	WT_ERR(__wt_config_gets(session, cfg, "lsm.chunk_count_limit", &cval));
	lsm_tree->chunk_count_limit = (uint32_t)cval.val;
	if (cval.val == 0)
		F_SET(lsm_tree, WT_LSM_TREE_MERGES);
	else
		F_CLR(lsm_tree, WT_LSM_TREE_MERGES);
	WT_ERR(__wt_config_gets(session, cfg, "lsm.chunk_max", &cval));
	lsm_tree->chunk_max = (uint64_t)cval.val;
	WT_ERR(__wt_config_gets(session, cfg, "lsm.chunk_size", &cval));
	lsm_tree->chunk_size = (uint64_t)cval.val;
	if (lsm_tree->chunk_size > lsm_tree->chunk_max)
		WT_ERR_MSG(session, EINVAL, "Chunk size (chunk_size) must be smaller than or equal to the maximum chunk size (chunk_max)");

	WT_ERR(__wt_config_gets(session, cfg, "lsm.merge_max", &cval));
	lsm_tree->merge_max = (uint32_t)cval.val;
	WT_ERR(__wt_config_gets(session, cfg, "lsm.merge_min", &cval));
	lsm_tree->merge_min = (uint32_t)cval.val;
	if (lsm_tree->merge_min > lsm_tree->merge_max)
		WT_ERR_MSG(session, EINVAL, "LSM merge_min must be less than or equal to merge_max");

	/*先将lsm tree的meta信息写入到meta管理器中，并关闭掉临时创建的lsm tree对象*/
	WT_ERR(__wt_scr_alloc(session, 0, &buf));
	WT_ERR(__wt_buf_fmt(session, buf,"%s,key_format=u,value_format=u,memory_page_max=%" PRIu64,
		config, 2 * lsm_tree->chunk_max));
	WT_ERR(__wt_strndup(session, buf->data, buf->size, &lsm_tree->file_config));
	WT_ERR(__wt_lsm_meta_write(session, lsm_tree));
	/*废弃掉临时的对象*/
	ret = __lsm_tree_discard(session, lsm_tree, 0);
	lsm_tree = NULL;

	/*重新打开uri对应的lsm tree对象，并设置为打开状态*/
	if (ret == 0)
		WT_WITH_DHANDLE_LOCK(session, ret = __lsm_tree_open(session, uri, &lsm_tree));
	if (ret == 0)
		__wt_lsm_tree_release(session, lsm_tree);

	if(0){
err:
		WT_TRET(__lsm_tree_discard(session, lsm_tree, 0));
	}
	__wt_scr_free(session, &buf);

	return ret;
}

/*通过lsm tree的uri name找到对应的lsm的内存对象*/
static int __lsm_tree_find(WT_SESSION_IMPL *session, const char *uri, int exclusive, WT_LSM_TREE **treep)
{
	WT_LSM_TREE *lsm_tree;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));

	/*判断uri对应的lsm tree对象是否已经打开*/
	TAILQ_FOREACH(lsm_tree, &S2C(session)->lsmqh, q)
		if(strcmp(uri, lsm_tree->name) == 0){
			/*已经有其他的任务在使用lsm tree对象,并设置了exclusive排他占用属性*/
			if ((exclusive && lsm_tree->refcnt > 0) || lsm_tree->exclusive)
				return (EBUSY);

			if(exclusive){
				/*设置排他属性*/
				if (!WT_ATOMIC_CAS1(lsm_tree->exclusive, 0, 1))
					return (EBUSY);

				if (!WT_ATOMIC_CAS4(lsm_tree->refcnt, 0, 1)) {
					lsm_tree->exclusive = 0;
					return (EBUSY);
				}
			}
			else{
				/*增加引用计数*/
				(void)WT_ATOMIC_ADD4(lsm_tree->refcnt, 1);

				/*如果在此刻又设置排他属性，放弃查询并返回忙*/
				if (lsm_tree->exclusive) {
					WT_ASSERT(session, lsm_tree->refcnt > 0);
					(void)WT_ATOMIC_SUB4(lsm_tree->refcnt, 1);

					return (EBUSY);
				}
			}
			*treep = lsm_tree;
		}

		return WT_NOTFOUND;
}

/*检查lsm tree的配置信息是否合理*/
static int __lsm_tree_open_check(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_CONFIG_ITEM cval;
	uint64_t maxleafpage, required;
	const char *cfg[] = { WT_CONFIG_BASE(session, session_create), lsm_tree->file_config, NULL };

	WT_RET(__wt_config_gets(session, cfg, "leaf_page_max", &cval));
	maxleafpage = (uint64_t)cval.val;
	/*
	 * Three chunks, plus one page for each participant in up to three concurrent merges.
	 */
	required = 3 * lsm_tree->chunk_size + 3 * (lsm_tree->merge_max * maxleafpage);
	if(S2C(session)->cache_size < required){ /*超出设置的cache size，打印一个错误信息，并返回一个错误值*/
		WT_RET_MSG(session, EINVAL,
			"LSM cache size %" PRIu64 " (%" PRIu64 "MB) too small, "
			"must be at least %" PRIu64 " (%" PRIu64 "MB)",
			S2C(session)->cache_size,
			S2C(session)->cache_size / WT_MEGABYTE,
			required, required / WT_MEGABYTE);
	}

	return 0;
}

/*打开一个lsm tree,其实就是将lsm tree载入内存中*/
static int __lsm_tree_open(WT_SESSION_IMPL* session, const char* uri, WT_LSM_TREE** treep)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LSM_TREE *lsm_tree;

	conn = S2C(session);
	lsm_tree = NULL;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));

	/* Start the LSM manager thread if it isn't running. */
	if (WT_ATOMIC_CAS4(conn->lsm_manager.lsm_workers, 0, 1))
		WT_RET(__wt_lsm_manager_start(session));

	/*查找lsm tree对象*/
	if(ret = __lsm_tree_find(session, uri, 0, treep) != WT_NOTFOUND)
		return ret;

	/*构建open lsm tree对象*/
	WT_RET(__wt_calloc_one(session, &lsm_tree));
	WT_ERR(__wt_rwlock_alloc(session, &lsm_tree->rwlock, "lsm tree"));
	WT_ERR(__lsm_tree_set_name(session, lsm_tree, uri));
	WT_ERR(__wt_lsm_meta_read(session, lsm_tree));

	WT_ERR(__lsm_tree_open_check(session, lsm_tree));

	lsm_tree->dsk_gen = 1;
	lsm_tree->refcnt = 1;
	lsm_tree->queue_ref = 0;
	/*设置flush的时间戳*/
	WT_ERR(__wt_epoch(session, &lsm_tree->last_flush_ts));

	/*设置lsm tree的状态*/
	TAILQ_INSERT_HEAD(&S2C(session)->lsmqh, lsm_tree, q);
	F_SET(lsm_tree, WT_LSM_TREE_ACTIVE | WT_LSM_TREE_OPEN);

	*treep = lsm_tree;

	if (0) {
err:		WT_TRET(__lsm_tree_discard(session, lsm_tree, 0));
	}

	return ret;
}

/*通过uri查找lsm tree信息，如果没有找到对应的lsm tree，就通过meta信息open一个lsm tree对象*/
int __wt_lsm_tree_get(WT_SESSION_IMPL *session, const char *uri, int exclusive, WT_LSM_TREE **treep)
{
	WT_DECL_RET;
	WT_ASSERT(session, F_ISSET(session, uri, exclusive, treep));
	ret = __lsm_tree_find(session, uri, exclusive, treep);
	if(ret == WT_NOTFOUND)
		ret = __lsm_tree_open(session, uri, treep);

	return ret;
}

/*release一个lsm tree对象，一般在使用(wt_lsm_tree_open)后调用*/
void __wt_lsm_tree_release(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree)
{
	WT_ASSERT(session, lsm_tree->refcnt > 1);
	if (lsm_tree->exclusive)
		lsm_tree->exclusive = 0;
	(void)WT_ATOMIC_SUB4(lsm_tree->refcnt, 1);
}

/* How aggressively to ramp up or down throttle due to level 0 merging */
#define	WT_LSM_MERGE_THROTTLE_BUMP_PCT	(100 / lsm_tree->merge_max)
/* Number of level 0 chunks that need to be present to throttle inserts */
#define	WT_LSM_MERGE_THROTTLE_THRESHOLD	(2 * lsm_tree->merge_min)
/* Minimal throttling time */
#define	WT_LSM_THROTTLE_START			20

#define	WT_LSM_MERGE_THROTTLE_INCREASE(val)	do {				\
	(val) += ((val) * WT_LSM_MERGE_THROTTLE_BUMP_PCT) / 100;	\
	if ((val) < WT_LSM_THROTTLE_START)							\
	(val) = WT_LSM_THROTTLE_START;								\
} while (0)

#define	WT_LSM_MERGE_THROTTLE_DECREASE(val)	do {				\
	(val) -= ((val) * WT_LSM_MERGE_THROTTLE_BUMP_PCT) / 100;	\
	if ((val) < WT_LSM_THROTTLE_START)							\
	(val) = 0;													\
} while (0)

/*通过LRU Cache状态计算checkpoint/merge的时间阈值，并调整chunk填充的平均时长*/
void __wt_lsm_tree_throttle(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, int decrease_only)
{
	WT_LSM_CHUNK *last_chunk, **cp, *ondisk, *prev_chunk;
	uint64_t cache_sz, cache_used, oldtime, record_count, timediff;
	uint32_t in_memory, gen0_chunks;

	/* lsm tree的chunk太少，无需调整lsm tree的阈值参数 */
	if (lsm_tree->nchunks < 3) {
		lsm_tree->ckpt_throttle = lsm_tree->merge_throttle = 0;
		return;
	}

	cache_sz = S2C(session)->cache_size;

	/*确定最后一个落盘的chunk和内存中chunk的个数*/
	record_count = 1;
	gen0_chunks = in_memory = 0;
	ondisk = NULL;
	for (cp = lsm_tree->chunk + lsm_tree->nchunks - 1; cp >= lsm_tree->chunk; --cp)
		if (!F_ISSET(*cp, WT_LSM_CHUNK_ONDISK)) { /*计算内存中chunk数量和内存中记录数*/
			record_count += (*cp)->count;
			++in_memory;
		} else {
			/*确定最后一个落盘的chunk*/
			if (ondisk == NULL && ((*cp)->generation == 0 && !F_ISSET(*cp, WT_LSM_CHUNK_STABLE)))
				ondisk = *cp;

			/*这个chunk没有进行过merge操作*/
			if ((*cp)->generation == 0 && !F_ISSET(*cp, WT_LSM_CHUNK_MERGING))
				++gen0_chunks;
		}

	last_chunk = lsm_tree->chunk[lsm_tree->nchunks - 1];

	/*内存中的chunk数量太少还不需要checkpoint*/
	if(!F_ISSET(lsm_tree, WT_LSM_TREE_THROTTLE) || in_memory <= 3)
		lsm_tree->ckpt_throttle = 0;
	else if(decrease_only)
		;
	else if(ondisk == NULL) /*没有chunk落盘，那么checkpoint 时间阈值参数放大2倍*/
		lsm_tree->ckpt_throttle = WT_MAX(WT_LSM_THROTTLE_START, 2 * lsm_tree->ckpt_throttle);
	else{
		/*内存中chunk单条记录操作的时间间隔，单位是微秒*/
		WT_ASSERT(session, WT_TIMECMP(last_chunk->create_ts, ondisk->create_ts) >= 0);
		timediff = WT_TIMEDIFF(last_chunk->create_ts, ondisk->create_ts);
		lsm_tree->ckpt_throttle = (in_memory - 2) * timediff / (20 * record_count);

		/*判断内存中的chunk占用的内存是否超过了cache的最大容忍度，如果超过，将checkpoint 阈值放到5倍,加速checkpoint的建立*/
		cache_used = in_memory * lsm_tree->chunk_size * 2;
		if (cache_used > cache_sz * 0.8)
			lsm_tree->ckpt_throttle *= 5;
	}

	/*lsm tree 有chunk在merge或者正在merge, 那么需要计算merge throttle*/
	if (F_ISSET(lsm_tree, WT_LSM_TREE_MERGES)) {
		/*没有超过merge的最大chunk,那么放慢merge的速度*/
		if (lsm_tree->nchunks < lsm_tree->merge_max)
			lsm_tree->merge_throttle = 0;
		else if (gen0_chunks < WT_LSM_MERGE_THROTTLE_THRESHOLD) /*没有操作的chunk数小于merge的chunk的最小值，放慢merge的速度*/
			WT_LSM_MERGE_THROTTLE_DECREASE(lsm_tree->merge_throttle);
		else if (!decrease_only)
			WT_LSM_MERGE_THROTTLE_INCREASE(lsm_tree->merge_throttle);
	}

	/*checkpoint和merge的频率不小于1秒*/
	lsm_tree->ckpt_throttle = WT_MIN(1000000, lsm_tree->ckpt_throttle);
	lsm_tree->merge_throttle = WT_MIN(1000000, lsm_tree->merge_throttle);

	/*如果有chunk落盘了，那么调整chunk 的填充平均时间,还没弄明白为什么是这样计算的？*/
	if (in_memory > 1 && ondisk != NULL) {
		prev_chunk = lsm_tree->chunk[lsm_tree->nchunks - 2];
		WT_ASSERT(session, prev_chunk->generation == 0);

		WT_ASSERT(session, WT_TIMECMP(last_chunk->create_ts, prev_chunk->create_ts) >= 0);
		timediff = WT_TIMEDIFF(last_chunk->create_ts, prev_chunk->create_ts);

		WT_ASSERT(session, WT_TIMECMP(prev_chunk->create_ts, ondisk->create_ts) >= 0);
		oldtime = WT_TIMEDIFF(prev_chunk->create_ts, ondisk->create_ts);
		if (timediff < 10 * oldtime)
			lsm_tree->chunk_fill_ms = (3 * lsm_tree->chunk_fill_ms + timediff / 1000000) / 4;
	}
}

/*在内存中切换lsm tree*/
int __wt_lsm_tree_switch(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk, *last_chunk;
	uint32_t chunks_moved, nchunks, new_id;
	int first_switch;

	WT_RET(__wt_lsm_tree_writelock(session, lsm_tree));

	nchunks = lsm_tree->nchunks;
	first_switch = (nchunks == 0 ? 1 : 0); /*树上没有任何chunk,表明是第一次switch*/

	last_chunk = NULL;
	if (!first_switch && (last_chunk = lsm_tree->chunk[nchunks - 1]) != NULL &&
		!F_ISSET(last_chunk, WT_LSM_CHUNK_ONDISK) && !F_ISSET(lsm_tree, WT_LSM_TREE_NEED_SWITCH))
		goto err;

	/* Update the throttle time. */
	__wt_lsm_tree_throttle(session, lsm_tree, 0);

	/*确定一个新的chunk ID*/
	new_id = WT_ATOMIC_ADD4(lsm_tree->last, 1);

	WT_ERR(__wt_realloc_def(session, &lsm_tree->chunk_alloc, nchunks + 1, &lsm_tree->chunk));

	WT_ERR(__wt_verbose(session, WT_VERB_LSM,
		"Tree %s switch to: %" PRIu32 ", checkpoint throttle %" PRIu64
		", merge throttle %" PRIu64, lsm_tree->name, new_id, lsm_tree->ckpt_throttle, lsm_tree->merge_throttle));

	WT_ERR(__wt_calloc_one(session, &chunk));
	chunk->id = new_id;
	chunk->switch_txn = WT_TXN_NONE;
	lsm_tree->chunk[lsm_tree->nchunks++] = chunk;
	/*初始化lsm tree chunk块*/
	WT_ERR(__wt_lsm_tree_setup_chunk(session, lsm_tree, chunk));
	/*写入meta信息*/
	WT_ERR(__wt_lsm_meta_write(session, lsm_tree));
	F_CLR(lsm_tree, WT_LSM_TREE_NEED_SWITCH);
	++lsm_tree->dsk_gen;

	lsm_tree->modified = 1;

	/*确定一个新的事务ID*/
	if (last_chunk != NULL && last_chunk->switch_txn == WT_TXN_NONE && !F_ISSET(last_chunk, WT_LSM_CHUNK_ONDISK))
		last_chunk->switch_txn = __wt_txn_new_id(session);

	/*假如lsm tree 的设置了chunk 数量限制，那么需要chunk对象的淘汰,将淘汰的chunk对象移入old chunks列表中*/
	if (lsm_tree->chunk_count_limit != 0 && lsm_tree->nchunks > lsm_tree->chunk_count_limit) {
		chunks_moved = lsm_tree->nchunks - lsm_tree->chunk_count_limit;
		/* Move the last chunk onto the old chunk list. */
		WT_ERR(__wt_lsm_tree_retire_chunks(session, lsm_tree, 0, chunks_moved));

		lsm_tree->nchunks -= chunks_moved;
		memmove(lsm_tree->chunk, lsm_tree->chunk + chunks_moved, lsm_tree->nchunks * sizeof(*lsm_tree->chunk));
		memset(lsm_tree->chunk + lsm_tree->nchunks, 0, chunks_moved * sizeof(*lsm_tree->chunk));

		/* 清空掉old chunks中的多余chunk对象，只有里面有没有任何引用记录，才删除对应的文件*/
		WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_DROP, 0, lsm_tree));
	}

err:	
	WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));
	if (ret != 0)
		WT_PANIC_RET(session, ret, "Failed doing LSM switch");
	else if (!first_switch) /*如果不是第一次switch,不需要启动一个flush磁盘的操作*/
		WT_RET(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_FLUSH, 0, lsm_tree));
	return ret;
}

/*将chunks中最旧的n个chunk移到old chunks*/
int __wt_lsm_tree_retire_chunks(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, u_int start_chunk, u_int nchunks)
{
	u_int i;

	WT_ASSERT(session, start_chunk + nchunks <= lsm_tree->nchunks);

	/* Setup the array of obsolete chunks. */
	WT_RET(__wt_realloc_def(session, &lsm_tree->old_alloc, lsm_tree->nold_chunks + nchunks, &lsm_tree->old_chunks));

	/* Copy entries one at a time, so we can reuse gaps in the list. */
	for (i = 0; i < nchunks; i++)
		lsm_tree->old_chunks[lsm_tree->nold_chunks++] = lsm_tree->chunk[start_chunk + i];

	return 0;
}

/*drop一个lsm tree表*/
int __wt_lsm_tree_drop(WT_SESSION_IMPL* session, const char* name, const char* cfg[])
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	u_int i;
	int locked;

	locked = 0;

	/* Get the LSM tree. */
	WT_WITH_DHANDLE_LOCK(session, ret = __wt_lsm_tree_get(session, name, 1, &lsm_tree));
	WT_RET(ret);

	/*关闭lsm tree的工作状态,清除它队列中的工作任务*/
	WT_ERR(__lsm_tree_close(session, lsm_tree));
	/*获得lsm tree的写锁，防止其他任务重新打开*/
	WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));
	locked = 1;

	for(i = 0; i < lsm_tree->nchunks; i++){
		chunk = lsm_tree->chunk[i];
		/*在schema层删除掉对应的信息，包括 bloom filter信息*/
		WT_ERR(__wt_schema_drop(session, chunk->uri, cfg));
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			WT_ERR(__wt_schema_drop(session, chunk->bloom_uri, cfg));
	}
	
	/*删除掉所有的old chunks schema*/
	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		if ((chunk = lsm_tree->old_chunks[i]) == NULL)
			continue;
		WT_ERR(__wt_schema_drop(session, chunk->uri, cfg));
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			WT_ERR(__wt_schema_drop(session, chunk->bloom_uri, cfg));
	}

	locked = 0;
	WT_ERR(__wt_lsm_tree_writeunlock(session, lsm_tree));
	ret = __wt_metadata_remove(session, name);

err:
	if (locked)
		WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));
	/*释放树的chunks,并废弃lsm tree对象*/
	WT_WITH_DHANDLE_LOCK(session, WT_TRET(__lsm_tree_discard(session, lsm_tree, 0)));
	return (ret);
}

/*修改lsm tree的uri对应关系，并保存修改后的meta信息*/
int __wt_lsm_tree_rename(WT_SESSION_IMPL* session, const char* olduri, const char* newuri, const char** cfg[])
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	const char *old;
	u_int i;
	int locked;

	old = NULL;
	locked = 0;

	/*通过uri获取lsm tree对象*/
	WT_WITH_DHANDLE_LOCK(session, ret = __wt_lsm_tree_get(session, olduri, 1, &lsm_tree));
	WT_RET(ret);

	/*停止lsm 工作任务*/
	WT_ERR(__lsm_tree_close(session, lsm_tree));
	/*抢占写锁，防止其他任务打开*/
	WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));
	locked = 1;

	/* Set the new name. */
	WT_ERR(__lsm_tree_set_name(session, lsm_tree, newuri));

	/*更新各个chunk的meta信息和schema信息*/
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		old = chunk->uri;
		chunk->uri = NULL;

		WT_ERR(__wt_lsm_tree_chunk_name(session, lsm_tree, chunk->id, &chunk->uri));
		WT_ERR(__wt_schema_rename(session, old, chunk->uri, cfg));
		__wt_free(session, old);

		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM)) {
			old = chunk->bloom_uri;
			chunk->bloom_uri = NULL;
			WT_ERR(__wt_lsm_tree_bloom_name(session, lsm_tree, chunk->id, &chunk->bloom_uri));
			F_SET(chunk, WT_LSM_CHUNK_BLOOM);
			WT_ERR(__wt_schema_rename(session, old, chunk->uri, cfg));
			__wt_free(session, old);
		}
	}

	/*写入一个新的meta对象，删除旧的兑现*/
	WT_ERR(__wt_lsm_meta_write(session, lsm_tree));
	locked = 0;
	WT_ERR(__wt_lsm_tree_writeunlock(session, lsm_tree));
	WT_ERR(__wt_metadata_remove(session, olduri));

err:
	if(locked)
		WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));

	if(old != NULL)
		__wt_free(session, old);

	WT_WITH_DHANDLE_LOCK(session, WT_TRET(__lsm_tree_discard(session, lsm_tree, 0)));

	return ret;
}

/*获得lsm tree的s-lock*/
int __wt_lsm_tree_readlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_RET(__wt_readlock(session, lsm_tree->rwlock));
	/* 防止schema lock的死锁，这里为了避开它，设置了WT_SESSION_NO_CACHE_CHECK和WT_SESSION_NO_SCHEMA_LOCK两个选项
	 * Diagnostic: avoid deadlocks with the schema lock: if we need it for
	 * an operation, we should already have it.
	 */
	F_SET(session, WT_SESSION_NO_CACHE_CHECK | WT_SESSION_NO_SCHEMA_LOCK);
}
/*释放lsm tree的s-lock*/
int __wt_lsm_tree_readunlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_DECL_RET;

	F_CLR(session, WT_SESSION_NO_CACHE_CHECK | WT_SESSION_NO_SCHEMA_LOCK);

	if ((ret = __wt_readunlock(session, lsm_tree->rwlock)) != 0)
		WT_PANIC_RET(session, ret, "Unlocking an LSM tree");

	return 0;
}

/*获得lsm tree的x-lock*/
int __wt_lsm_tree_writelock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_RET(__wt_writelock(session, lsm_tree->rwlock));
	
	F_SET(session, WT_SESSION_NO_CACHE_CHECK | WT_SESSION_NO_SCHEMA_LOCK);
	return 0;
}
/*释放lsm tree的s-lock*/
int __wt_lsm_tree_writeunlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_DECL_RET;

	F_CLR(session, WT_SESSION_NO_CACHE_CHECK | WT_SESSION_NO_SCHEMA_LOCK);

	if ((ret = __wt_writeunlock(session, lsm_tree->rwlock)) != 0)
		WT_PANIC_RET(session, ret, "Unlocking an LSM tree");
	return 0;
}

#define	COMPACT_PARALLEL_MERGES	5

/*对lsm stree的compact操作，这个函数是由__wt_schema_worker*/
int __wt_lsm_compact(WT_SESSION_IMPL *session, const char *name, int *skip)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	time_t begin, end;
	uint64_t progress;
	int i, compacting, flushing, locked, ref;

	compacting = flushing = locked = ref = 0;
	chunk = NULL;

	if(!WT_PREFIX_MATCH(name, "lsm:"))
		return 0;

	/*将skip设置为1，表示已经开始compact,__wt_schema_worker会在compact前检查这个标示*/
	*skip = 1;

	WT_WITH_DHANDLE_LOCK(session,ret = __wt_lsm_tree_get(session, name, 0, &lsm_tree));
	WT_RET(ret);

	if (!F_ISSET(S2C(session), WT_CONN_LSM_MERGE))
		WT_ERR_MSG(session, EINVAL, "LSM compaction requires active merge threads");

	/*记录开始时间*/
	WT_ERR(__wt_seconds(session, &begin));

	WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));
	locked = 1;

	/*清除掉merge 阈值，防止同时merge操作*/
	lsm_tree->merge_throttle = 0;
	lsm_tree->merge_aggressiveness = 0;
	progress = lsm_tree->merge_progressing;

	/* If another thread started a compact on this tree, we're done. */
	if (F_ISSET(lsm_tree, WT_LSM_TREE_COMPACTING))
		goto err;

	/*判断最近的一个chunk是否在磁盘上，那么需要先设置个switch 事务ID*/
	if (lsm_tree->nchunks > 0 && (chunk = lsm_tree->chunk[lsm_tree->nchunks - 1]) != NULL) {
		if (chunk->switch_txn == WT_TXN_NONE)
			chunk->switch_txn = __wt_txn_new_id(session);
		/*
		* If we have a chunk, we want to look for it to be on-disk.
		* So we need to add a reference to keep it available.
		*/
		(void)WT_ATOMIC_ADD4(chunk->refcnt, 1);
		ref = 1;
	}

	locked = 0;
	WT_ERR(__wt_lsm_tree_writeunlock(session, lsm_tree));

	if(chunk != NULL){
		WT_ERR(__wt_verbose(session, WT_VERB_LSM,
			"Compact force flush %s flags 0x%" PRIx32
			" chunk %u flags 0x%" PRIx32, name, lsm_tree->flags, chunk->id, chunk->flags));
		flushing = 1;
		/*将chunk进行flush磁盘操作*/
		WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_FLUSH, WT_LSM_WORK_FORCE, lsm_tree));
	}
	else{
		compacting = 1;
		progress = lsm_tree->merge_progressing;
		F_SET(lsm_tree, WT_LSM_TREE_COMPACTING);
		WT_ERR(__wt_verbose(session, WT_VERB_LSM, "COMPACT: Start compacting %s", lsm_tree->name));
	}

	/*等待任务线程完成对应的任务*/
	while(F_ISSET(lsm_tree, WT_LSM_TREE_ACTIVE)){
		if(flushing){
			WT_ASSERT(session, chunk != NULL);
			if (F_ISSET(chunk, WT_LSM_CHUNK_ONDISK)) { /*chunk已经落盘了,可进行compacting了*/
				WT_ERR(__wt_verbose(session, WT_VERB_LSM,
					"Compact flush done %s chunk %u.  Start compacting progress %" PRIu64, name, chunk->id, lsm_tree->merge_progressing));

				(void)WT_ATOMIC_SUB4(chunk->refcnt, 1);
				flushing = ref = 0;
				compacting = 1;
				F_SET(lsm_tree, WT_LSM_TREE_COMPACTING);
				progress = lsm_tree->merge_progressing;
			}
			else{
				WT_ERR(__wt_verbose(session, WT_VERB_LSM, "Compact flush retry %s chunk %u", name, chunk->id));
				WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_FLUSH, WT_LSM_WORK_FORCE, lsm_tree));
			}
		}

		/*启动compacting状态*/
		if (compacting && !F_ISSET(lsm_tree, WT_LSM_TREE_COMPACTING)) {
			if (lsm_tree->merge_aggressiveness < 10 || (progress < lsm_tree->merge_progressing) || lsm_tree->merge_syncing) {
				progress = lsm_tree->merge_progressing;
				F_SET(lsm_tree, WT_LSM_TREE_COMPACTING);
				lsm_tree->merge_aggressiveness = 10;
			} 
			else
				break;
		}

		__wt_sleep(1, 0);
		WT_ERR(__wt_seconds(session, &end));
		/*进行持续时间判断，如果最大的compact时间，直接退出*/
		if (session->compact->max_time > 0 && session->compact->max_time < (uint64_t)(end - begin)) {
			WT_ERR(ETIMEDOUT);
		}
		
		/*每次发起5个merge操作*/
		if (compacting){
			for (i = lsm_tree->queue_ref; i < COMPACT_PARALLEL_MERGES; i++) {
				lsm_tree->merge_aggressiveness = 10;
				WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_MERGE, 0, lsm_tree));
			}
		}
	}

err:
	/*消除refcount*/
	if (ref)
		(void)WT_ATOMIC_SUB4(chunk->refcnt, 1);

	/*已经完成compact操作，进行状态清除*/
	if (compacting) {
		F_CLR(lsm_tree, WT_LSM_TREE_COMPACTING);
		lsm_tree->merge_aggressiveness = 0;
	}

	if (locked)
		WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));

	WT_TRET(__wt_verbose(session, WT_VERB_LSM, "Compact %s complete, return %d", name, ret));
	__wt_lsm_tree_release(session, lsm_tree);

	return ret;
}

/*在lsm tree的每一层运行一个schema worker,在schema_worker函数会调用这个函数,这两个函数是相互递归的，根据uri中关键词来判别操作*/
int __wt_lsm_tree_worker(WT_SESSION_IMPL *session, const char *uri,
	int (*file_func)(WT_SESSION_IMPL *, const char *[]),
	int (*name_func)(WT_SESSION_IMPL *, const char *, int *),
	const char *cfg[], uint32_t open_flags)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	u_int i;
	int exclusive, locked;

	locked = 0;
	exclusive = FLD_ISSET(open_flags, WT_DHANDLE_EXCLUSIVE) ? 1 : 0;
	WT_WITH_DHANDLE_LOCK(session, ret = __wt_lsm_tree_get(session, uri, exclusive, &lsm_tree));
	WT_RET(ret);

	/*获得操作是否是需要独占锁*/
	WT_ERR(exclusive ? __wt_lsm_tree_writelock(session, lsm_tree) : __wt_lsm_tree_readlock(session, lsm_tree));
	locked = 1;

	/*对每一个chunk进行操作扫描*/
	for(i = 0; i < lsm_tree->nchunks; i++){
		chunk = lsm_tree->chunk[i];
		if (file_func == __wt_checkpoint && F_ISSET(chunk, WT_LSM_CHUNK_ONDISK)) /*chunk已经落盘，无需checkpoint*/
			continue;
		/*执行schema worker函数来进行lsm_tree操作*/
		WT_ERR(__wt_schema_worker(session, chunk->uri, file_func, name_func, cfg, open_flags));

		if (name_func == __wt_backup_list_uri_append && F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			WT_ERR(__wt_schema_worker(session, chunk->bloom_uri, file_func, name_func, cfg, open_flags));
	}
err:
	if (locked)
		WT_TRET(exclusive ? __wt_lsm_tree_writeunlock(session, lsm_tree) : __wt_lsm_tree_readunlock(session, lsm_tree));
	__wt_lsm_tree_release(session, lsm_tree);
}



