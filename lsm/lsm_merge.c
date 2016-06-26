/*****************************************************************************
*lsm tree的合并操作
*****************************************************************************/
#include "wt_internal.h"

static int __lsm_merge_span(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, u_int, u_int* , u_int* , uint64_t*);

/*merge后调整chunk数组的布局*/
int __wt_lsm_merge_update_tree(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, u_int start_chunk, u_int nchunks, WT_LSM_CHUNK *chunk)
{
	size_t chunks_after_merge;
	/*将被merge的chunk先移入old chunks用于drop回收*/
	WT_RET(__wt_lsm_tree_retire_chunks(session, lsm_tree, start_chunk, nchunks));

	chunks_after_merge = lsm_tree->nchunks - (nchunks + start_chunk);
	/*将已经merge后的chunk覆盖掉*/
	memmove(lsm_tree->chunk + start_chunk + 1, lsm_tree->chunk + start_chunk + nchunks, chunks_after_merge * sizeof(*lsm_tree->chunk));

	/*合并后的chunk数量*/
	lsm_tree->nchunks -= nchunks - 1;
	/*将后面空余出来chunk指针职位NULL*/
	memset(lsm_tree->chunk + lsm_tree->nchunks, 0, (nchunks - 1) * sizeof(*lsm_tree->chunk));
	lsm_tree->chunk[start_chunk] = chunk;

	return 0;
}

static int __lsm_merge_span(WT_SESSION_IMPL* session, WT_LSM_TREE* lsm_tree, u_int id, u_int* start, u_int* end, uint64_t* records)
{
	WT_LSM_CHUNK *chunk, *previous, *youngest;
	uint32_t aggressive, max_gap, max_gen, max_level;
	uint64_t record_count, chunk_size;
	u_int end_chunk, i, merge_max, merge_min, nchunks, start_chunk;

	chunk_size = 0;
	nchunks = 0;
	record_count = 0;
	chunk = youngest = NULL;

	*start = 0;
	*end = 0;
	*records = 0;

	/*lsm仅仅是只读或者正在进行compact操作,所以需要制定一个更长的merge时间来完成chunk merge*/
	if(!lsm_tree->modified || F_ISSET(lsm_tree, WT_LSM_TREE_COMPACTING)){
		lsm_tree->merge_aggressiveness = 10;
	}

	/*确定合并的条件参数，主要是合并的最大chunk数、最小的chunk数*/
	aggressive = lsm_tree->merge_aggressiveness;
	merge_max = (aggressive > WT_LSM_AGGRESSIVE_THRESHOLD) ? 100 : lsm_tree->merge_min;
	merge_min = (aggressive > WT_LSM_AGGRESSIVE_THRESHOLD) ? 2 : lsm_tree->merge_min;
	max_gap = (aggressive + 4) / 5;
	max_level = (lsm_tree->merge_throttle > 0) ? 0 : id + aggressive;

	if (lsm_tree->nchunks < merge_min)
		return WT_NOTFOUND;

	/*
	 * Only include chunks that already have a Bloom filter or are the
	 * result of a merge and not involved in a merge.
	 * 确定merge最后一个chunk,这个chunk必须有bloom filter
	 */
	for(end_chunk = lsm_tree->nchunks - 1; end_chunk > 0; --end_chunk){
		chunk = lsm_tree->chunk[end_chunk];
		WT_ASSERT(session, chunk != NULL);

		if(!F_ISSET(chunk, WT_LSM_CHUNK_MERGING))
			continue;

		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM) || chunk->generation > 0)
			break;

		else if (FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_OFF) && F_ISSET(chunk, WT_LSM_CHUNK_ONDISK))
			break;
	}

	if(end_chunk < merge_min - 1)
		return WT_NOTFOUND;

	/*从end chunk开始，计算往前能进行chunk merge的chunk*/
	for (start_chunk = end_chunk + 1, record_count = 0; start_chunk > 0; ){
		chunk = lsm_tree->chunk[start_chunk - 1];
		youngest = lsm_tree->chunk[end_chunk];
		nchunks = (end_chunk + 1) - start_chunk;

		if (F_ISSET(chunk, WT_LSM_CHUNK_MERGING) || chunk->bloom_busy)
			break;

		if(chunk->generation > max_level)
			break;

		/*假如merge后的chunk size会大于lsm tree容许的最大chunk size,不进行下面的chunk的merge操作*/
		chunk_size += chunk->size;
		if (chunk_size > lsm_tree->chunk_max)
			if (nchunks < merge_min || (chunk->generation > youngest->generation &&chunk_size - youngest->size > lsm_tree->chunk_max))
				break;

		/*已经能merge的chunk个数达到了merge chunk最小个数要求，确定chunk merge的位置*/
		if (nchunks >= merge_min) {
			previous = lsm_tree->chunk[start_chunk];
			max_gen = youngest->generation + max_gap;
			if (previous->generation <= max_gen && chunk->generation > max_gen)
				break;
		}

		/*设置chunk 处于merging状态*/
		F_SET(chunk, WT_LSM_CHUNK_MERGING);
		record_count += chunk->count;
		--start_chunk;

		/*被merge的chunks数据太大，需要重新调整*/
		if (nchunks == merge_max || chunk_size > lsm_tree->chunk_max) {
			WT_ASSERT(session, F_ISSET(youngest, WT_LSM_CHUNK_MERGING));
			F_CLR(youngest, WT_LSM_CHUNK_MERGING);

			record_count -= youngest->count;
			chunk_size -= youngest->size;
			--end_chunk;
		}
	}

	nchunks = (end_chunk + 1) - start_chunk;
	
	WT_ASSERT(session, start_chunk + nchunks <= lsm_tree->nchunks);
	WT_ASSERT(session, nchunks == 0 || (chunk != NULL && youngest != NULL));

	/*没达到merge chunk要求，返回一个WT_NOFOUND*/
	if (nchunks < merge_min || lsm_tree->chunk[end_chunk]->generation > youngest->generation + max_gap) {
		for (i = 0; i < nchunks; i++) {
			chunk = lsm_tree->chunk[start_chunk + i];
			WT_ASSERT(session, F_ISSET(chunk, WT_LSM_CHUNK_MERGING));
			F_CLR(chunk, WT_LSM_CHUNK_MERGING);
		}
		return WT_NOTFOUND;
	}

	/*将计算得到merge的位置和参数返回，然后进行merge操作*/
	*records = record_count;
	*start = start_chunk;
	*end = end_chunk;

	return 0;
}

/*进行chunks merge操作*/
int __wt_lsm_merge(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, u_int id)
{
	WT_BLOOM *bloom;
	WT_CURSOR *dest, *src;
	WT_DECL_RET;
	WT_ITEM key, value;
	WT_LSM_CHUNK *chunk;
	uint32_t generation;
	uint64_t insert_count, record_count;
	u_int dest_id, end_chunk, i, nchunks, start_chunk, start_id;
	u_int created_chunk, verb;
	int create_bloom, locked, in_sync, tret;
	const char *cfg[3];
	const char *drop_cfg[] = { WT_CONFIG_BASE(session, session_drop), "force", NULL };

	bloom = NULL;
	chunk = NULL;
	create_bloom = 0;
	created_chunk = 0;
	dest = src = NULL;
	locked = 0;
	start_id = 0;
	in_sync = 0;

	/*lsm tree没达到merge要求*/
	if (lsm_tree->nchunks < lsm_tree->merge_min && lsm_tree->merge_aggressiveness < WT_LSM_AGGRESSIVE_THRESHOLD)
		return WT_NOTFOUND;

	/* 获得lsm tree的写锁，这里只能一个线程进行merge chunk的计算，防止其他线程重复计算，虽然在下面的
	 * 操作并没有修改lsm tree的结构，所以这里还是需要持有写锁，*/
	WT_RET(__wt_lsm_tree_writelock(session, lsm_tree));
	locked = 1;

	/*计算merge的chunk范围*/
	WT_ERR(__lsm_merge_span(session, lsm_tree, id, &start_chunk, &end_chunk, &record_count));
	nchunks = (end_chunk + 1) - start_chunk;

	/*获得开始merge*/
	WT_ASSERT(session, nchunks > 0);
	start_id = lsm_tree->chunk[start_chunk]->id;

	/* Find the merge generation. */
	for (generation = 0, i = 0; i < nchunks; i++)
		generation = WT_MAX(generation, lsm_tree->chunk[start_chunk + i]->generation + 1);

	WT_ERR(__wt_lsm_tree_writeunlock(session, lsm_tree));

	/* Allocate an ID for the merge. */
	dest_id = WT_ATOMIC_ADD4(lsm_tree->last, 1);

	if(WT_VERBOSE_ISSET(session, WT_VERB_LSM)){
		WT_ERR(__wt_verbose(session, WT_VERB_LSM, "Merging %s chunks %u-%u into %u (%" PRIu64 " records), generation %" PRIu32,
			lsm_tree->name, start_chunk, end_chunk, dest_id, record_count, generation));

		for (verb = start_chunk; verb <= end_chunk; verb++)
			WT_ERR(__wt_verbose(session, WT_VERB_LSM, "%s: Chunk[%u] id %u",
			lsm_tree->name, verb, lsm_tree->chunk[verb]->id));
	}

	/*分配一个chunk内存块*/
	WT_ERR(__wt_calloc_one(session, &chunk));
	created_chunk = 1;
	chunk->id = dest_id;

	/*merge的chunks中已经建立的bloom filter并且合并的chunks中的是有记录的,那么在merge过程中需要重新构建bloom filter*/
	if(FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_MERGED) && (FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_OLDEST) ||
		start_chunk > 0) && record_count > 0){
			create_bloom = 1;
	}
	/*确定merge开始的chunk位置和范围*/
	WT_ERR(__wt_open_cursor(session, lsm_tree->name, NULL, NULL, &src));
	F_SET(src, WT_CURSTD_RAW);
	WT_ERR(__wt_clsm_init_merge(src, start_chunk, start_id, nchunks));
	/*获取merge chunk的meta配置信息*/
	WT_WITH_SCHEMA_LOCK(session, ret = __wt_lsm_tree_setup_chunk(session, lsm_tree, chunk));

	WT_ERR(ret);
	/*建立一个bloom filter*/
	if(create_bloom){
		WT_ERR(__wt_lsm_tree_setup_bloom(session, lsm_tree, chunk));

		WT_ERR(__wt_bloom_create(session, chunk->bloom_uri, lsm_tree->bloom_config, record_count, lsm_tree->bloom_bit_count, 
			lsm_tree->bloom_hash_count, &bloom));
	}

	/* Discard pages we read as soon as we're done with them. */
	F_SET(session, WT_SESSION_NO_CACHE);

	cfg[0] = WT_CONFIG_BASE(session, session_open_cursor);
	cfg[1] = "bulk,raw,skip_sort_check";
	cfg[2] = NULL;
	WT_ERR(__wt_open_cursor(session, chunk->uri, NULL, cfg, &dest));

#define LSM_MERGE_CHECK_INTERVAL 1000
	/*merge过程*/
	for(insert_count = 0; (ret = src->next(src)) == 0; insert_count ++){
		if (insert_count % LSM_MERGE_CHECK_INTERVAL == 0) {
			if (!F_ISSET(lsm_tree, WT_LSM_TREE_ACTIVE))
				WT_ERR(EINTR);

			WT_STAT_FAST_CONN_INCRV(session, lsm_rows_merged, LSM_MERGE_CHECK_INTERVAL);
			++lsm_tree->merge_progressing;
		}

		/*记录转移*/
		WT_ERR(src->get_key(src, &key));
		dest->set_key(dest, &key);
		WT_ERR(src->get_value(src, &value));
		dest->set_value(dest, &value);
		WT_ERR(dest->insert(dest));

		/*构建bloom filter中的值*/
		if (create_bloom)
			WT_ERR(__wt_bloom_insert(bloom, &key));
	}

	WT_ERR_NOTFOUND_OK(ret);

	WT_STAT_FAST_CONN_INCRV(session, lsm_rows_merged, insert_count % LSM_MERGE_CHECK_INTERVAL);

	++lsm_tree->merge_progressing;
	WT_ERR(__wt_verbose(session, WT_VERB_LSM, "Bloom size for %" PRIu64 " has %" PRIu64 " items inserted.", record_count, insert_count));

	(void)WT_ATOMIC_ADD4(lsm_tree->merge_syncing, 1);
	in_sync = 1;

	WT_TRET(src->close(src));
	WT_TRET(dest->close(dest));
	src = dest = NULL;

	F_CLR(session, WT_SESSION_NO_CACHE);
	F_SET(session, WT_SESSION_NO_CACHE_CHECK);

	/*最后将bloom进行打包，并结束封装*/
	if(create_bloom){
		if (ret == 0)
			WT_TRET(__wt_bloom_finalize(bloom));

		if (ret == 0) {
			WT_CLEAR(key);
			WT_TRET_NOTFOUND_OK(__wt_bloom_get(bloom, &key));
		}

		WT_TRET(__wt_bloom_close(bloom));
		bloom = NULL;
	}

	WT_ERR(ret);

	/*进行转移后的目的chunk验证*/
	cfg[1] = "checkpoint=" WT_CHECKPOINT;
	WT_ERR(__wt_open_cursor(session, chunk->uri, NULL, cfg, &dest));
	WT_TRET(dest->close(dest));
	dest = NULL;

	++lsm_tree->merge_progressing;
	(void)WT_ATOMIC_SUB4(lsm_tree->merge_syncing, 1);
	in_sync = 0;
	WT_ERR_NOTFOUND_OK(ret);

	WT_ERR(__wt_lsm_tree_set_chunk_size(session, chunk));
	WT_ERR(__wt_lsm_tree_writelock(session, lsm_tree));
	locked = 1;

	if (start_chunk >= lsm_tree->nchunks || lsm_tree->chunk[start_chunk]->id != start_id){
		for (start_chunk = 0; start_chunk < lsm_tree->nchunks; start_chunk++){
			if (lsm_tree->chunk[start_chunk]->id == start_id)
				break;
		}
	}

	/*将merger后的无效chunks从lsm_tree->chunks数组中移除*/
	WT_ERR(__wt_lsm_merge_update_tree(session, lsm_tree, start_chunk, nchunks, chunk));

	if (create_bloom)
		F_SET(chunk, WT_LSM_CHUNK_BLOOM);

	chunk->count = insert_count;
	chunk->generation = generation;
	F_SET(chunk, WT_LSM_CHUNK_ONDISK);

	/*更新lsm_tree的元信息*/
	if ((ret = __wt_lsm_meta_write(session, lsm_tree)) != 0)
		WT_PANIC_ERR(session, ret, "Failed finalizing LSM merge");

	lsm_tree->dsk_gen++;

	__wt_lsm_tree_throttle(session, lsm_tree, 1);

	/*触发一个文件移除的信号*/
	WT_ERR(__wt_lsm_manager_push_entry(session, WT_LSM_WORK_DROP, 0, lsm_tree));

err:
	/*进行资源释放*/
	if (locked)
		WT_TRET(__wt_lsm_tree_writeunlock(session, lsm_tree));

	if (in_sync)
		(void)WT_ATOMIC_SUB4(lsm_tree->merge_syncing, 1);

	if (src != NULL)
		WT_TRET(src->close(src));

	if (dest != NULL)
		WT_TRET(dest->close(dest));

	if (bloom != NULL)
		WT_TRET(__wt_bloom_close(bloom));

	/*merge失败，进行新建chunk的释放*/
	if(ret != 0 && created_chunk){
		if (chunk->uri != NULL) {
			WT_WITH_SCHEMA_LOCK(session, tret = __wt_schema_drop(session, chunk->uri, drop_cfg));
			WT_TRET(tret);
		}

		if (create_bloom && chunk->bloom_uri != NULL) {
			WT_WITH_SCHEMA_LOCK(session, tret = __wt_schema_drop(session, chunk->bloom_uri, drop_cfg));
			WT_TRET(tret);
		}

		__wt_free(session, chunk->bloom_uri);
		__wt_free(session, chunk->uri);
		__wt_free(session, chunk);

		if (ret == EINTR)
			WT_TRET(__wt_verbose(session, WT_VERB_LSM, "Merge aborted due to close"));
		else
			WT_TRET(__wt_verbose(session, WT_VERB_LSM, "Merge failed with %s", __wt_strerror(session, ret, NULL, 0)));
	}

	F_CLR(session, WT_SESSION_NO_CACHE | WT_SESSION_NO_CACHE_CHECK);
	return ret;
}


