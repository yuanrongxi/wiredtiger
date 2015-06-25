/***************************************************************************
*LSM meta信息的读写实现
***************************************************************************/
#include "wt_internal.h"

/*读取lsm tree的元信息,这个过程是从lsmconfig中读取，lsm config是存在wiredtiger的metadata中*/
int __wt_lsm_meta_read(WT_SESSION_IMPL* sesion, WT_LSM_TREE* lsm_tree)
{
	WT_CONFIG cparser, lparser;
	WT_CONFIG_ITEM ck, cv, fileconf, lk, lv, metadata;
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	char *lsmconfig;
	u_int nchunks;

	chunk = NULL;

	/*lsm tree继承connection的LSM_MERGE标识*/
	if(F_ISSET(S2C(session), WT_CONN_LSM_MERGE))
		F_SET(lsm_tree, WT_LSM_TREE_MERGES);

	/*通过lsm tree的name，查找到对应的配置串*/
	WT_RET(__wt_metadata_search(session, lsm_tree->name, &lsmconfig));
	/*对lsm配置信息进行解析*/
	WT_ERR(__wt_config_init(session, &cparser, lsmconfig));

	/*对各项配置的解析*/
	while ((ret = __wt_config_next(&cparser, &ck, &cv)) == 0) {
		if (WT_STRING_MATCH("key_format", ck.str, ck.len)) {
			__wt_free(session, lsm_tree->key_format);
			WT_ERR(__wt_strndup(session, cv.str, cv.len, &lsm_tree->key_format));
		} 
		else if (WT_STRING_MATCH("value_format", ck.str, ck.len)) {
			__wt_free(session, lsm_tree->value_format);
			WT_ERR(__wt_strndup(session, cv.str, cv.len, &lsm_tree->value_format));
		} 
		else if (WT_STRING_MATCH("collator", ck.str, ck.len)) { /*比较器信息读取,并对比较器进行初始化*/
			if (cv.len == 0 || WT_STRING_MATCH("none", cv.str, cv.len))
				continue;
			/*
			 * Extract the application-supplied metadata (if any)
			 * from the file configuration.
			 */
			WT_ERR(__wt_config_getones(session, lsmconfig, "file_config", &fileconf));
			WT_CLEAR(metadata);
			WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, &fileconf, "app_metadata", &metadata));
			WT_ERR(__wt_collator_config(session, lsm_tree->name, &cv, &metadata, &lsm_tree->collator, &lsm_tree->collator_owned));
			WT_ERR(__wt_strndup(session, cv.str, cv.len, &lsm_tree->collator_name));
		} 
		else if (WT_STRING_MATCH("bloom_config", ck.str, ck.len)) {
			__wt_free(session, lsm_tree->bloom_config);
			/* Don't include the brackets. */
			WT_ERR(__wt_strndup(session, cv.str + 1, cv.len - 2, &lsm_tree->bloom_config));
		} 
		else if (WT_STRING_MATCH("file_config", ck.str, ck.len)) {
			__wt_free(session, lsm_tree->file_config);
			/* Don't include the brackets. */
			WT_ERR(__wt_strndup(session, cv.str + 1, cv.len - 2, &lsm_tree->file_config));
		} 
		else if (WT_STRING_MATCH("auto_throttle", ck.str, ck.len)) {
			if (cv.val)
				F_SET(lsm_tree, WT_LSM_TREE_THROTTLE);
			else
				F_CLR(lsm_tree, WT_LSM_TREE_THROTTLE);
		} 
		else if (WT_STRING_MATCH("bloom", ck.str, ck.len))
			lsm_tree->bloom = (uint32_t)cv.val;
		else if (WT_STRING_MATCH("bloom_bit_count", ck.str, ck.len))
			lsm_tree->bloom_bit_count = (uint32_t)cv.val;
		else if (WT_STRING_MATCH("bloom_hash_count", ck.str, ck.len))
			lsm_tree->bloom_hash_count = (uint32_t)cv.val;
		else if (WT_STRING_MATCH("chunk_count_limit", ck.str, ck.len)) {
			lsm_tree->chunk_count_limit = (uint32_t)cv.val;
			if (cv.val != 0)
				F_CLR(lsm_tree, WT_LSM_TREE_MERGES);
		} 
		else if (WT_STRING_MATCH("chunk_max", ck.str, ck.len))
			lsm_tree->chunk_max = (uint64_t)cv.val;
		else if (WT_STRING_MATCH("chunk_size", ck.str, ck.len))
			lsm_tree->chunk_size = (uint64_t)cv.val;
		else if (WT_STRING_MATCH("merge_max", ck.str, ck.len))
			lsm_tree->merge_max = (uint32_t)cv.val;
		else if (WT_STRING_MATCH("merge_min", ck.str, ck.len))
			lsm_tree->merge_min = (uint32_t)cv.val;
		else if (WT_STRING_MATCH("last", ck.str, ck.len))
			lsm_tree->last = (u_int)cv.val;
		else if (WT_STRING_MATCH("chunks", ck.str, ck.len)) { /*读取chunk的元信息*/
			WT_ERR(__wt_config_subinit(session, &lparser, &cv));
			for (nchunks = 0; (ret = __wt_config_next(&lparser, &lk, &lv)) == 0; ) {
				if (WT_STRING_MATCH("id", lk.str, lk.len)) {
					WT_ERR(__wt_realloc_def(session, &lsm_tree->chunk_alloc, nchunks + 1, &lsm_tree->chunk));
					WT_ERR(__wt_calloc_one(session, &chunk));

					lsm_tree->chunk[nchunks++] = chunk;
					chunk->id = (uint32_t)lv.val;
					WT_ERR(__wt_lsm_tree_chunk_name(session, lsm_tree, chunk->id, &chunk->uri));
					F_SET(chunk, WT_LSM_CHUNK_ONDISK | WT_LSM_CHUNK_STABLE);
				} 
				else if (WT_STRING_MATCH("bloom", lk.str, lk.len)) {
					WT_ERR(__wt_lsm_tree_bloom_name(session, lsm_tree, chunk->id, &chunk->bloom_uri));
					F_SET(chunk, WT_LSM_CHUNK_BLOOM);
					continue;
				} 
				else if (WT_STRING_MATCH("chunk_size", lk.str, lk.len)) {
					chunk->size = (uint64_t)lv.val;
					continue;
				} 
				else if (WT_STRING_MATCH("count", lk.str, lk.len)) {
					chunk->count = (uint64_t)lv.val;
					continue;
				} 
				else if (WT_STRING_MATCH("generation", lk.str, lk.len)) {
					chunk->generation = (uint32_t)lv.val;
					continue;
				}
			}
			WT_ERR_NOTFOUND_OK(ret);
			lsm_tree->nchunks = nchunks;
		}
		else if (WT_STRING_MATCH("old_chunks", ck.str, ck.len)) { /*读取历史chunk的元信息*/
			WT_ERR(__wt_config_subinit(session, &lparser, &cv));
			for (nchunks = 0; (ret = __wt_config_next(&lparser, &lk, &lv)) == 0; ) {
				if (WT_STRING_MATCH("bloom", lk.str, lk.len)) {
					WT_ERR(__wt_strndup(session, lv.str, lv.len, &chunk->bloom_uri));
					F_SET(chunk, WT_LSM_CHUNK_BLOOM);
					continue;
				}
				WT_ERR(__wt_realloc_def(session, &lsm_tree->old_alloc, nchunks + 1, &lsm_tree->old_chunks));
				WT_ERR(__wt_calloc_one(session, &chunk));
				lsm_tree->old_chunks[nchunks++] = chunk;
				WT_ERR(__wt_strndup(session, lk.str, lk.len, &chunk->uri));
				F_SET(chunk, WT_LSM_CHUNK_ONDISK);
			}

			WT_ERR_NOTFOUND_OK(ret);
			lsm_tree->nold_chunks = nchunks;
		}
		/*
		 * Ignore any other values: the metadata entry might have been
		 * created by a future release, with unknown options.
		 */
	}

	WT_ERR_NOTFOUND_OK(ret);

	/*设置最少多少个chunk一起合并*/
	if (lsm_tree->merge_min < 2)
		lsm_tree->merge_min = WT_MAX(2, lsm_tree->merge_max / 2);

err:
	/*释放lsmconfig开辟的内存空间*/
	__wt_free(session, lsmconfig);

	return ret;
}

/*将lsm tree的元信息更新保存到wiredtiger的metadata中*/
int __wt_lsm_meta_write(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	u_int i;
	int first;

	WT_RET(__wt_scr_alloc(session, 0, &buf));
	/*格式化key format的格式字符等*/
	WT_ERR(__wt_buf_fmt(session, buf, "key_format=%s,value_format=%s,bloom_config=(%s),file_config=(%s)",
		lsm_tree->key_format, lsm_tree->value_format, lsm_tree->bloom_config, lsm_tree->file_config));

	/*格式化比较器元信息*/
	if (lsm_tree->collator_name != NULL)
		WT_ERR(__wt_buf_catfmt(session, buf, ",collator=%s", lsm_tree->collator_name));

	/*格式化LSM基本的元信息*/
	WT_ERR(__wt_buf_catfmt(session, buf,
		",last=%" PRIu32
		",chunk_count_limit=%" PRIu32
		",chunk_max=%" PRIu64
		",chunk_size=%" PRIu64
		",auto_throttle=%" PRIu32
		",merge_max=%" PRIu32
		",merge_min=%" PRIu32
		",bloom=%" PRIu32
		",bloom_bit_count=%" PRIu32
		",bloom_hash_count=%" PRIu32,
		lsm_tree->last, lsm_tree->chunk_count_limit,
		lsm_tree->chunk_max, lsm_tree->chunk_size,
		F_ISSET(lsm_tree, WT_LSM_TREE_THROTTLE) ? 1 : 0,
		lsm_tree->merge_max, lsm_tree->merge_min, lsm_tree->bloom,
		lsm_tree->bloom_bit_count, lsm_tree->bloom_hash_count));

	WT_ERR(__wt_buf_catfmt(session, buf, ",chunks=["));

	/*格式化chunk的元信息*/
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		if (i > 0)
			WT_ERR(__wt_buf_catfmt(session, buf, ","));

		WT_ERR(__wt_buf_catfmt(session, buf, "id=%" PRIu32, chunk->id));
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			WT_ERR(__wt_buf_catfmt(session, buf, ",bloom"));

		if (chunk->size != 0)
			WT_ERR(__wt_buf_catfmt(session, buf, ",chunk_size=%" PRIu64, chunk->size));

		if (chunk->count != 0)
			WT_ERR(__wt_buf_catfmt(session, buf, ",count=%" PRIu64, chunk->count));

		WT_ERR(__wt_buf_catfmt(session, buf, ",generation=%" PRIu32, chunk->generation));
	}

	WT_ERR(__wt_buf_catfmt(session, buf, "]"));
	WT_ERR(__wt_buf_catfmt(session, buf, ",old_chunks=["));
	first = 1;
	
	/*格式化old chunk的元信息*/
	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		chunk = lsm_tree->old_chunks[i];
		WT_ASSERT(session, chunk != NULL);
		if (first)
			first = 0;
		else
			WT_ERR(__wt_buf_catfmt(session, buf, ","));

		WT_ERR(__wt_buf_catfmt(session, buf, "\"%s\"", chunk->uri));
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			WT_ERR(__wt_buf_catfmt(session, buf, ",bloom=\"%s\"", chunk->bloom_uri));
	}

	WT_ERR(__wt_buf_catfmt(session, buf, "]"));

	/*更新到wiredtiger的metadata当中*/
	ret = __wt_metadata_update(session, lsm_tree->name, buf->data);

	WT_ERR(ret);

err:
	__wt_scr_free(session, &buf);

	return ret;
}


