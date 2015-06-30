/*****************************************************************************
*
*****************************************************************************/

#include "wt_internal.h"

static int __lsm_bloom_create(WT_SESSION_IMPL* , WT_LSM_TREE* , WT_LSM_CHUNK*, u_int);
static int __lsm_discard_handle(WT_SESSION_IMPL*, const char* , const char*);

/*将lsm tree中的chunks指针数组拷贝到cookie->chunk_array中*/
static int __lsm_copy_chunks(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, WT_LSM_WORKER_COOKIE* cookie, int old_chunks)
{
	WT_DECL_RET;
	u_int i, nchunks;
	size_t alloc;

	cookie->nchunks = 0;

	/*获得lsm tree的读锁权限，因为只是拷贝指针，可以使用spin read/write lock*/
	WT_RET(__wt_lsm_tree_readlock(session, lsm_tree));

	if (!F_ISSET(lsm_tree, WT_LSM_TREE_ACTIVE))
		return (__wt_lsm_tree_readunlock(session, lsm_tree));

	nchunks = old_chunks ? lsm_tree->nold_chunks : lsm_tree->nchunks;
	alloc = old_chunks ? lsm_tree->old_alloc : lsm_tree->chunk_alloc;

	if (cookie->chunk_alloc < alloc)
		WT_ERR(__wt_realloc(session, &cookie->chunk_alloc, alloc, &cookie->chunk_array));

	if (nchunks > 0)
		memcpy(cookie->chunk_array, old_chunks ? lsm_tree->old_chunks : lsm_tree->chunk, nchunks * sizeof(*cookie->chunk_array));

	/*为每一个chunk增加一个引用计数,在从array中删除的时候再递减*/
	for (i = 0; i < nchunks; i++)
		WT_ATOMIC_ADD4(cookie->chunk_array[i]->refcnt, 1);

err:
	WT_TRET(__wt_lsm_tree_readunlock(session, lsm_tree));

	if(ret == 0)
		cookie->nchunks = nchunks;

	return ret;
}

/*确定一个可以flush的chunk*/
int __wt_lsm_get_chunk_to_flush(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, int force, WT_LSM_CHUNK **chunkp)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk, *evict_chunk, *flush_chunk;
	u_int i;

	*chunkp = NULL;
	chunk = evict_chunk = flush_chunk = NULL;

	WT_ASSERT(session, lsm_tree->queue_ref > 0);
	WT_RET(__wt_lsm_tree_readlock(session, lsm_tree));

	if(!F_ISSET(lsm_tree, WT_LSM_TREE_ACTIVE) || lsm_tree->nchunks == 0)
		return __wt_lsm_tree_readunlock(session, lsm_tree);

	for(i = 0; i < lsm_tree->nchunks; i ++){
		chunk = lsm_tree->chunk[i];
		
		if(F_ISSET(chunk, WT_LSM_CHUNK_ONDISK)){
			/*
			 * Normally we don't want to force out the last chunk.
			 * But if we're doing a forced flush on behalf of a
			 * compact, then we want to include the final chunk.
			 */
			if (evict_chunk == NULL && !chunk->evicted && !F_ISSET(chunk, WT_LSM_CHUNK_STABLE))
				evict_chunk = chunk;
		}
		else if (flush_chunk == NULL && chunk->switch_txn != 0 && (force || i < lsm_tree->nchunks - 1)){
			flush_chunk = chunk;
		}
	}

	/*确定需要flush的chunk*/
	if (evict_chunk != NULL && flush_chunk != NULL) {
		chunk = (__wt_random(session->rnd) & 1) ? evict_chunk : flush_chunk;
		WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_FLUSH, 0, lsm_tree));
	} 
	else
		chunk = (evict_chunk != NULL) ? evict_chunk : flush_chunk;

	if (chunk != NULL) {
		WT_ERR(__wt_verbose(session, WT_VERB_LSM, "Flush%s: return chunk %u of %u: %s", force ? " w/ force" : "",
			i, lsm_tree->nchunks, chunk->uri));

		/*增加引用计数*/
		(void)WT_ATOMIC_ADD4(chunk->refcnt, 1);
	}

err:
	WT_RET(__wt_lsm_tree_readunlock(session, lsm_tree));
	*chunkp = chunk;
	return ret;
}

/*对cookie中的chunk全部去掉引用计数*/
static void __lsm_unpin_chunks(WT_SESSION_IMPL* session, WT_LSM_WORKER_COOKIE* cookie)
{
	u_int i;

	for (i = 0; i < cookie->nchunks; i++) {
		if (cookie->chunk_array[i] == NULL)
			continue;

		WT_ASSERT(session, cookie->chunk_array[i]->refcnt > 0);
		WT_ATOMIC_SUB4(cookie->chunk_array[i]->refcnt, 1);
	}

	/* Ensure subsequent calls don't double decrement. */
	cookie->nchunks = 0;
}

/*检查lsm tree是否需要进行chunk switch，如果需要，调用__wt_lsm_tree_switch进行switch，如果__wt_lsm_tree_switch
 * 忙状态，从新加入一个switch信号到lsm manager中*/
int __wt_lsm_work_switch(WT_SESSION_IMPL *session, WT_LSM_WORK_UNIT **entryp, int *ran)
{
	WT_DECL_RET;
	WT_LSM_WORK_UNIT *entry;

	entry = *entryp;
	*ran = 0;
	*entryp = NULL;

	if (F_ISSET(entry->lsm_tree, WT_LSM_TREE_NEED_SWITCH)) {
		WT_WITH_SCHEMA_LOCK(session, ret = __wt_lsm_tree_switch(session, entry->lsm_tree));
		/* Failing to complete the switch is fine */
		if (ret == EBUSY) {
			if (F_ISSET(entry->lsm_tree, WT_LSM_TREE_NEED_SWITCH))
				WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_SWITCH, 0, entry->lsm_tree));

			ret = 0;
		} 
		else
			*ran = 1;
	}

err:
	__wt_lsm_manager_free_work_unit(session, entry);
	return ret;
}

/*尝试在chunk中建立一个bloom filter数据，一般是在chunk落盘的时候调用*/
int __wt_lsm_work_bloom(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_WORKER_COOKIE cookie;
	u_int i, merge;

	WT_CLEAR(cookie);

	/*增加lsm_tree中的chunk引用计数*/
	WT_RET(__lsm_copy_chunks(session, lsm_tree, &cookie, 0));

	merge = 0;
	/*对所有的chunk建立bloom filter*/
	for(i = 0; i < cookie.nchunks; i ++){
		chunk = cookie.chunk_array[i];

		/*跳过正在处理的chunk*/
		if(!F_ISSET(chunk, WT_LSM_CHUNK_ONDISK) || F_ISSET(chunk, WT_LSM_CHUNK_BLOOM | WT_LSM_CHUNK_MERGING) 
			|| chunk->generation > 0 || chunk->count == 0)
			continue;

		if (WT_ATOMIC_CAS4(chunk->bloom_busy, 0, 1)) {
			/*建立bloom filter*/
			if (!F_ISSET(chunk, WT_LSM_CHUNK_BLOOM)) {
				ret = __lsm_bloom_create(session, lsm_tree, chunk, (u_int)i);
				if (ret == 0)
					merge = 1;
			}

			chunk->bloom_busy = 0;
			break;
		}
	}

	/*发起一个chunk merge操作,为什么会发起一个merge呢？？需要仔细分析*/
	if (merge)
		WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_MERGE, 0, lsm_tree));

err:
	/*删除chunks的引用计数*/
	__lsm_unpin_chunks(session, &cookie);
	__wt_free(session, cookie.chunk_array);

	return ret;
}

/*将chunk上的数据flush到磁盘上*/
int __wt_lsm_checkpoint_chunk(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, WT_LSM_CHUNK *chunk)
{
	WT_DECL_RET;
	WT_TXN_ISOLATION saved_isolation;

	/*这个chunk已经被建立了checkpoint，我们必须将其设置为被废弃状态*/
	if (F_ISSET(chunk, WT_LSM_CHUNK_ONDISK) && !F_ISSET(chunk, WT_LSM_CHUNK_STABLE) && !chunk->evicted) {
			if ((ret = __lsm_discard_handle(session, chunk->uri, NULL)) == 0)
				chunk->evicted = 1;
			else if (ret == EBUSY)
				ret = 0;
			else
				WT_RET_MSG(session, ret, "discard handle");
	}

	if(F_ISSET(chunk, WT_LSM_CHUNK_ONDISK)){
		WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker %s already on disk", chunk->uri));
		return 0;
	}

	/*假如有事务正在操作这个chunk，那么checkpoint需要暂停*/
	__wt_txn_update_oldest(session);

	if (chunk->switch_txn == WT_TXN_NONE || !__wt_txn_visible_all(session, chunk->switch_txn)) {
		WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker %s: running transaction, return", chunk->uri));
		return 0;
	}

	WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker flushing %s", chunk->uri));

	/*设置事务的隔离属性标识*/
	if ((ret = __wt_session_get_btree(session, chunk->uri, NULL, NULL, 0)) == 0) {
		saved_isolation = session->txn.isolation;
		session->txn.isolation = TXN_ISO_EVICTION;
		ret = __wt_cache_op(session, NULL, WT_SYNC_WRITE_LEAVES);
		session->txn.isolation = saved_isolation;
		WT_TRET(__wt_session_release_btree(session));
	}

	WT_RET(ret);

	WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker checkpointing %s", chunk->uri));

	WT_WITH_SCHEMA_LOCK(session,
		ret = __wt_schema_worker(session, chunk->uri, __wt_checkpoint, NULL, NULL, 0));

	if (ret != 0)
		WT_RET_MSG(session, ret, "LSM checkpoint");

	/* Now the file is written, get the chunk size. */
	WT_RET(__wt_lsm_tree_set_chunk_size(session, chunk));

	/* Update the flush timestamp to help track ongoing progress. */
	WT_RET(__wt_epoch(session, &lsm_tree->last_flush_ts));

	/* Lock the tree, mark the chunk as on disk and update the metadata. */
	WT_RET(__wt_lsm_tree_writelock(session, lsm_tree));
	F_SET(chunk, WT_LSM_CHUNK_ONDISK);

	ret = __wt_lsm_meta_write(session, lsm_tree);
	++lsm_tree->dsk_gen;

	/* Update the throttle time. */
	__wt_lsm_tree_throttle(session, lsm_tree, 1);
	WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));

	if (ret != 0)
		WT_RET_MSG(session, ret, "LSM metadata write");

	WT_RET(__wt_session_get_btree(session, chunk->uri, NULL, NULL, 0));
	__wt_btree_evictable(session, 1);
	WT_RET(__wt_session_release_btree(session));

	/* Make sure we aren't pinning a transaction ID. */
	__wt_txn_release_snapshot(session);

	WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker checkpointed %s", chunk->uri));

	/* Schedule a bloom filter create for our newly flushed chunk. */
	if (!FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_OFF))
		WT_RET(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_BLOOM, 0, lsm_tree));
	else
		WT_RET(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_MERGE, 0, lsm_tree));

	return 0;
}

/*为chunk创建一个bloom filter*/
static int __lsm_bloom_create(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, WT_LSM_CHUNK* chunk, u_int chunk_off)
{
	WT_BLOOM *bloom;
	WT_CURSOR *src;
	WT_DECL_RET;
	WT_ITEM key;
	uint64_t insert_count;

	WT_RET(__wt_lsm_tree_setup_bloom(session, lsm_tree, chunk));

	bloom = NULL;

	++lsm_tree->merge_progressing;

	/*创建并初始化一个bloom filter*/
	WT_RET(__wt_bloom_create(session, chunk->bloom_uri, lsm_tree->bloom_config, chunk->count, 
								lsm_tree->bloom_bit_count, lsm_tree->bloom_hash_count, &bloom));

	/*构建一个lsm tree cursor*/
	WT_ERR(__wt_open_cursor(session, lsm_tree->name, NULL, NULL, &src));
	F_SET(src, WT_CURSTD_RAW);
	WT_ERR(__wt_clsm_init_merge(src, chunk_off, chunk->id, 1));

	F_SET(session, WT_SESSION_NO_CACHE | WT_SESSION_NO_CACHE_CHECK);
	/*构建bloom filter数据*/
	for (insert_count = 0; (ret = src->next(src)) == 0; insert_count++) {
		WT_ERR(src->get_key(src, &key));
		WT_ERR(__wt_bloom_insert(bloom, &key));
	}

	WT_ERR_NOTFOUND_OK(ret);
	WT_TRET(src->close(src));

	/*将bloom filter写入到session对应的元数据中*/
	WT_TRET(__wt_bloom_finalize(bloom));
	WT_ERR(ret);

	F_CLR(session, WT_SESSION_NO_CACHE);

	/*校验新构建的bloom filter是否正确*/
	WT_CLEAR(key);
	WT_ERR_NOTFOUND_OK(__wt_bloom_get(bloom, &key));

	WT_ERR(__wt_verbose(session, WT_VERB_LSM,
		"LSM worker created bloom filter %s. Expected %" PRIu64 " items, got %" PRIu64, chunk->bloom_uri, chunk->count, insert_count));

	/*写入bloom filter数据*/
	WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));
	F_SET(chunk, WT_LSM_CHUNK_BLOOM);
	ret = __wt_lsm_meta_write(session, lsm_tree);
	++lsm_tree->dsk_gen;
	WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));

	if (ret != 0)
		WT_ERR_MSG(session, ret, "LSM bloom worker metadata write");

err:
	if (bloom != NULL)
		WT_TRET(__wt_bloom_close(bloom));

	F_CLR(session, WT_SESSION_NO_CACHE | WT_SESSION_NO_CACHE_CHECK);

	return ret;
}

/*尝试从cache中废弃一个handler对应的数据*/
static int __lsm_discard_handle(WT_SESSION_IMPL *session, const char *uri, const char *checkpoint)
{
	WT_RET(__wt_session_get_btree(session, uri, checkpoint, NULL, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_LOCK_ONLY));

	F_SET(session->dhandle, WT_DHANDLE_DISCARD);
	return __wt_session_release_btree(session);
}

/*删除掉lsm tree的一个文件*/
static int __lsm_drop_file(WT_SESSION_IMPL* session, const char* uri)
{
	WT_DECL_RET;
	const char *drop_cfg[] = {
		WT_CONFIG_BASE(session, session_drop), "remove_files=false", NULL
	};

	/*从cache中驱逐已经建立checkpoint的数据*/
	WT_RET(__lsm_discard_handle(session, uri, WT_CHECKPOINT));

	/*删除元数据*/
	WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_drop(session, uri, drop_cfg));

	/*文件删除*/
	if (ret == 0)
		ret = __wt_remove(session, uri + strlen("file:"));

	WT_RET(__wt_verbose(session, WT_VERB_LSM, "Dropped %s", uri));

	if (ret == EBUSY || ret == ENOENT)
		WT_RET(__wt_verbose(session, WT_VERB_LSM, "LSM worker drop of %s failed with %d", uri, ret));

	return ret;
}

/*释放掉old chunks中的空闲chunk*/
int __wt_lsm_free_chunks(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_WORKER_COOKIE cookie;
	u_int i, skipped;
	int flush_metadata, drop_ret;

	flush_metadata = 0;

	/*设置free状态*/
	if (!WT_ATOMIC_CAS4(lsm_tree->freeing_old_chunks, 0, 1))
		return 0;

	/*对old chunks的引用计数进行增加，做占用标识*/
	WT_CLEAR(cookie);
	WT_RET(__lsm_copy_chunks(session, lsm_tree, &cookie, 1));

	for(i = skipped = 0; i < cookie.nchunks; i ++){
		chunk = cookie.chunk_array[i];
		WT_ASSERT(session, chunk != NULL);
		/* Skip the chunk if another worker is using it. 还有其他模块在引用这个chunk，不做释放*/
		if (chunk->refcnt > 1) {
			++skipped;
			continue;
		}

		if (S2C(session)->hot_backup != 0)
			break;

		/*如果chunk是一个bloom filter数据，那么直接删除掉对应的bloom filter对应的文件,如果这个文件正在被引用，那么暂时不删除*/
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM)) {
			drop_ret = __lsm_drop_file(session, chunk->bloom_uri);
			if (drop_ret == EBUSY) {
				++skipped;
				continue;
			} 
			else if (drop_ret != ENOENT)
				WT_ERR(drop_ret);

			flush_metadata = 1;
			F_CLR(chunk, WT_LSM_CHUNK_BLOOM);
		}

		if (chunk->uri != NULL) {
			drop_ret = __lsm_drop_file(session, chunk->uri);
			if (drop_ret == EBUSY) {
				++skipped;
				continue;
			} 
			else if (drop_ret != ENOENT)
				WT_ERR(drop_ret);

			flush_metadata = 1;
		}

		/* Lock the tree to clear out the old chunk information. */
		WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));

		/*更新old_chunks的状态*/
		WT_ASSERT(session, lsm_tree->old_chunks[skipped] == chunk);
		__wt_free(session, chunk->bloom_uri);
		__wt_free(session, chunk->uri);
		__wt_free(session, lsm_tree->old_chunks[skipped]);

		/*删除掉chunk在old chunks中的关系*/
		if (--lsm_tree->nold_chunks > skipped) {
			memmove(lsm_tree->old_chunks + skipped, lsm_tree->old_chunks + skipped + 1, (lsm_tree->nold_chunks - skipped) * sizeof(WT_LSM_CHUNK *));
			lsm_tree->old_chunks[lsm_tree->nold_chunks] = NULL;
		}

		WT_ERR(__wt_lsm_tree_writeunlock(session, lsm_tree));

		cookie.chunk_array[i] = NULL;
	}
}





