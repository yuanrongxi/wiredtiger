/************************************************************************
*
************************************************************************/
#include "wt_internal.h"

static int __lsm_tree_cleanup_old(WT_SESSION_IMPL *, const char*);
static int __lsm_tree_open_check(WT_SESSION_IMPL *, WT_LSM_TREE *);
static int __lsm_tree_open(WT_SESSION_IMPL *, const char *, WT_LSM_TREE **);
static int __lsm_tree_set_name(WT_SESSION_IMPL *, WT_LSM_TREE *, const char *);


static int __lsm_tree_discard(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, int final)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	u_int i;

	WT_UNUSED(final);

	/**/
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
	for(i = 0; lsm_tree->refcnt> 1 || lsm_tree->queue_ref > 0; ++i){
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

/*撤销（先关闭，后释放）所有的lsm tree 对象*/
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

/*清除掉一个lsm chunk文件，*/
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
	FLD_SET(lsm_tree->bloom,
		(cval.val == 0 ? WT_LSM_BLOOM_OFF : WT_LSM_BLOOM_MERGED));
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




