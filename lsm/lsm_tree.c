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
	WT_ERR(__wt_buf_fmt(
		session, tmp, "file:%s-%06" PRIu32 ".lsm", lsm_tree->filename, id));
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



