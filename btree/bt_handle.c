/*******************************************************************
*btree的基本的open close等操作实现，主要是构建内存中的btree的对象结构
*会涉及到BTREE的配置信息读取、meta信息读取
*******************************************************************/

#include "wt_internal.h"

static int __btree_conf(WT_SESSION_IMPL* sssion, WT_CKPT* ckpt);
static int __btree_get_last_recno(WT_SESSION_IMPL* session);
static int __btree_page_sizes(WT_SESSION_IMPL* session);
static int __btree_preload(WT_SESSION_IMPL* session);
static int __btree_tree_open_empty(WT_SESSION_IMPL* session, int creation);

/*创建或者打开一个btree*/
int __wt_btree_open(WT_SESSION_IMPL *session, const char *op_cfg[])
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CKPT ckpt;
	WT_CONFIG_ITEM cval;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	size_t root_addr_size;
	uint8_t root_addr[WT_BTREE_MAX_ADDR_COOKIE];
	int creation, forced_salvage, readonly;
	const char *filename;

	dhandle = session->dhandle;
	btree = S2BT(session);

	/*判断是否是只读属性，如果SESSION已经建立了CHECKPOINT，那么不能对这个BTREE文件进行修改*/
	readonly = (dhandle->checkpoint == NULL ? 0 : 1);
	/*获得checkpoint信息*/
	WT_CLEAR(ckpt);
	WT_RET(__wt_meta_checkpoint(session, dhandle->name, dhandle->checkpoint, &ckpt));

	/*判断是否是新建一个btree*/
	creation = (ckpt.raw.size == 0);
	if (!creation && F_ISSET(btree, WT_BTREE_BULK))
		WT_ERR_MSG(session, EINVAL, "bulk-load is only supported on newly created objects");

	/*判断是否需要对btree进行salvage修复*/
	forced_salvage = 0;
	if(F_ISSET(btree, WT_BTREE_SALVAGE)){
		WT_ERR(__wt_config_gets(session, op_cfg, "force", &cval));
		forced_salvage = (cval.val != 0);
	}
	/*初始化btree对象结构*/
	WT_ERR(__btree_conf(session, &ckpt));

	/*关联btree和对应的file block manager对象*/
	filename = dhandle->name;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		WT_ERR_MSG(session, EINVAL, "expected a 'file:' URI");
	/*为btree打开一个block manager*/
	WT_ERR(__wt_block_manager_open(session, filename, dhandle->cfg, forced_salvage, readonly, btree->allocsize, &btree->bm));
	bm = btree->bm;

	btree->block_header = bm->block_header(bm);

	/*打开一个指定的checkpoint位置的数据，是不需要进行校验修复等操作，因为自己建立的checkpoint是可靠的*/
	if(!F_ISSET(btree, WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY)){
		WT_ERR(bm->checkpoint_load(bm, session, ckpt.raw.data, ckpt.raw.size,
			root_addr, &root_addr_size, readonly));
		/*如果是建立一个空的btree只需要直接打开就行了，如果是有数据的btree需要根据数据组织来建立内存信息对象*/
		if(creation || root_addr_size == 0)
			WT_ERR(__btree_tree_open_empty(session, creation));
		else{
			WT_ERR(__wt_btree_tree_open(session, root_addr, root_addr_size));

			/*预热加载数据*/
			WT_WITH_PAGE_INDEX(session, ret = __btree_preload(session));
			WT_ERR(ret);

			/*获取列式存储最后的记录序号*/
			if(btree->type != BTREE_ROW)
				WT_ERR(__btree_get_last_recno(session));
		}
	}
	if(0){
err:	WT_TRET(__wt_btree_close(session));
	}

	__wt_meta_checkpoint_free(session, &ckpt);
	return ret;
}

/*关闭一个btree对象*/
int __wt_btree_close(WT_SESSION_IMPL* session)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	dhandle = session->dhandle;
	btree = S2BT(session);

	if((bm = btree->bm) != NULL){
		/*只有不是对btree进行校验修复的session才unload checkpoint操作*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) && !F_ISSET(btree, WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY))
			WT_TRET(bm->checkpoint_unload(bm, session));

		WT_TRET(bm->close(bm, session));
		btree->bm = NULL;
	}

	/*关闭huffman tree*/
	__wt_btree_huffman_close(session);

	/*释放掉latch、btree的kv内存对象、collator对象等*/
	WT_TRET(__wt_rwlock_destroy(session, &btree->ovfl_lock));
	__wt_spin_destroy(session, &btree->flush_lock);

	__wt_free(session, btree->key_format);
	__wt_free(session, btree->value_format);

	if (btree->collator_owned) {
		if (btree->collator->terminate != NULL)
			WT_TRET(btree->collator->terminate(btree->collator, &session->iface));
		btree->collator_owned = 0;
	}
	btree->collator = NULL;

	btree->bulk_load_ok = 0;

	return ret;
}

/*根据ckpt信息构建btree对应的结构对象并初始化*/
static int __btree_conf(WT_SESSION_IMPL* sssion, WT_CKPT* ckpt)
{
	WT_BTREE *btree;
	WT_CONFIG_ITEM cval, metadata;
	int64_t maj_version, min_version;
	uint32_t bitcnt;
	int fixed;
	const char **cfg;

	btree = S2BT(session);
	cfg = btree->dhandle->cfg;

	/*读取cfg配置信息中的版本信息*/
	if (WT_VERBOSE_ISSET(session, WT_VERB_VERSION)) {
		WT_RET(__wt_config_gets(session, cfg, "version.major", &cval));
		maj_version = cval.val;
		WT_RET(__wt_config_gets(session, cfg, "version.minor", &cval));
		min_version = cval.val;
		WT_RET(__wt_verbose(session, WT_VERB_VERSION, "%" PRIu64 ".%" PRIu64, maj_version, min_version));
	}

	/*获得文件ID*/
	WT_RET(__wt_config_gets(session, cfg, "id", &cval));
	btree->id = (uint32_t)cval.val;

	/*读取cfg中的key format格式*/
	WT_RET(__wt_config_gets(session, cfg, "key_format", &cval));
	WT_RET(__wt_struct_confchk(session, &cval));
	if (WT_STRING_MATCH("r", cval.str, cval.len))
		btree->type = BTREE_COL_VAR;
	else
		btree->type = BTREE_ROW;

	WT_RET(__wt_strndup(session, cval.str, cval.len, &btree->key_format));

	WT_RET(__wt_config_gets(session, cfg, "value_format", &cval));
	WT_RET(__wt_struct_confchk(session, &cval));
	WT_RET(__wt_strndup(session, cval.str, cval.len, &btree->value_format));

	/*构建btree的校对器collator*/
	if (btree->type == BTREE_ROW) {
		WT_RET(__wt_config_gets_none(session, cfg, "collator", &cval));
		if (cval.len != 0) {
			WT_RET(__wt_config_gets(session, cfg, "app_metadata", &metadata));
			WT_RET(__wt_collator_config(session, btree->dhandle->name, &cval, &metadata,
				&btree->collator, &btree->collator_owned));
		}

		WT_RET(__wt_config_gets(session, cfg, "key_gap", &cval));
		btree->key_gap = (uint32_t)cval.val;
	}

	/*列式存储需要检查fixed-size data并指定btree-type = FIX*/
	if (btree->type == BTREE_COL_VAR) {
		WT_RET(__wt_struct_check(session, cval.str, cval.len, &fixed, &bitcnt));
		if (fixed) {
			if (bitcnt == 0 || bitcnt > 8)
				WT_RET_MSG(session, EINVAL, "fixed-width field sizes must be greater than 0 and less than or equal to 8");
			btree->bitcnt = (uint8_t)bitcnt;
			btree->type = BTREE_COL_FIX;
		}
	}

	WT_RET(__btree_page_sizes(session));

	/*设置page驱逐选项信息，metadata是不会被驱逐的*/
	if(WT_IS_METADATA(btree->dhandle))
		F_SET(btree, WT_BTREE_NO_EVICTION | WT_BTREE_NO_HAZARD);
	else{
		WT_RET(__wt_config_gets(session, cfg, "cache_resident", &cval));
		if (cval.val)
			F_SET(btree, WT_BTREE_NO_EVICTION | WT_BTREE_NO_HAZARD);
		else
			F_CLR(btree, WT_BTREE_NO_EVICTION);
	}

	/*checksums校验信息设置*/
	WT_RET(__wt_config_gets(session, cfg, "checksum", &cval));
	if (WT_STRING_MATCH("on", cval.str, cval.len))
		btree->checksum = CKSUM_ON;
	else if (WT_STRING_MATCH("off", cval.str, cval.len))
		btree->checksum = CKSUM_OFF;
	else
		btree->checksum = CKSUM_UNCOMPRESSED;

	/*打开huffman编码对象*/
	WT_RET(__wt_btree_huffman_open(session));

	switch(btree->type){
	case BTREE_COL_FIX:
		break;

	case BTREE_ROW: /*行存储时指定数据压缩的方式*/
		WT_RET(__wt_config_gets(session, cfg, "internal_key_truncate", &cval));
		btree->internal_key_truncate = cval.val == 0 ? 0 : 1;

		WT_RET(__wt_config_gets(session, cfg, "prefix_compression", &cval));
		btree->prefix_compression = cval.val == 0 ? 0 : 1;

		WT_RET(__wt_config_gets(session, cfg, "prefix_compression_min", &cval));
		btree->prefix_compression_min = (u_int)cval.val;

	case BTREE_COL_VAR:
		WT_RET(__wt_config_gets(session, cfg, "dictionary", &cval));
		btree->dictionary = (u_int)cval.val;
		break;
	}
	/*创建并初始化压缩对象*/
	WT_RET(__wt_config_gets_none(session, cfg, "block_compressor", &cval));
	WT_RET(__wt_compressor_config(session, &cval, &btree->compressor));

	/*初始化latch*/
	WT_RET(__wt_rwlock_alloc(session, &btree->ovfl_lock, "btree overflow lock"));
	WT_RET(__wt_spin_init(session, &btree->flush_lock, "btree flush lock"));
	/*初始化统计信息*/
	__wt_stat_init_dsrc_stats(&btree->dhandle->stats);

	btree->write_gen = ckpt->write_gen;		/* Write generation */
	btree->modified = 0;					/* Clean */

	return 0;
}

/*初始化btree root的ref对象*/
void __wt_root_ref_init(WT_REF* root_ref, WT_PAGE* root, int is_recno)
{
	memset(root_ref, 0, sizeof(*root_ref));

	root_ref->page = root;
	root_ref->state = WT_REF_MEM;

	root_ref->key.recno = is_recno ? 1 : 0;

	root->pg_intl_parent_ref = root_ref;
}

/*从磁盘中读取btree的数据并初始化各个page*/
int __wt_btree_tree_open(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_ITEM dsk;
	WT_PAGE *page;

	btree = S2BT(session);

	WT_CLEAR(dsk);
	/*根据block addr读取对应的文件数据*/
	WT_ERR(__wt_bt_read(session, &dsk, addr, addr_size));
	/**/
	WT_ERR(__wt_page_inmem(session, NULL, dsk.data, dsk.memsize, WT_DATA_IN_ITEM(&dsk) ? WT_PAGE_DISK_ALLOC : WT_PAGE_DISK_MAPPED, &page));
	dsk.mem = NULL;
	
	__wt_root_ref_init(&btree->root, page, btree->type != BTREE_ROW);
err:
	__wt_buf_free(session, &dsk);
	return ret;
}

/*在内存中创建一个空的btree对象*/
static int __btree_tree_open_empty(WT_SESSION_IMPL* session, int creation)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *leaf, *root;
	WT_PAGE_INDEX *pindex;
	WT_REF *ref;

	btree = S2BT(session);
	root = leaf = NULL;
	ref = NULL;

	/*
	 * Newly created objects can be used for cursor inserts or for bulk
	 * loads; set a flag that's cleared when a row is inserted into the
	 * tree.   Objects being bulk-loaded cannot be evicted, we set it
	 * globally, there's no point in searching empty trees for eviction.
	 */
	if(creation){
		btree->bulk_load_ok = 1;
		__wt_btree_evictable(session, 0);
	}

	switch(btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		WT_ERR(__wt_page_alloc(session, WT_PAGE_COL_INT, 1, 1, 1, &root));
		root->pg_intl_parent_ref = &btree->root;

		pindex = WT_INTL_INDEX_GET_SAFE(root);
		ref = pindex->index[0];
		ref->home = root;
		ref->page = NULL;
		ref->addr = NULL;
		ref->state = WT_REF_DELETED;
		ref->key.recno = 1;
		break;

	case BTREE_ROW:
		WT_ERR(__wt_page_alloc(session, WT_PAGE_ROW_INT, 0, 1, 1, &root));
		root->pg_intl_parent_ref = &btree->root;

		pindex = WT_INTL_INDEX_GET_SAFE(root);
		ref = pindex->index[0];
		ref->home = root;
		ref->page = NULL;
		ref->addr = NULL;
		ref->state = WT_REF_DELETED;
		WT_ERR(__wt_row_ikey_incr(session, root, 0, "", 1, ref));
		break;

	WT_ILLEGAL_VALUE_ERR(session);
	}

	/*如果bulk load操作，需要提前新建一个叶子页来做协调存储*/
	if(F_ISSET(btree, WT_BTREE_BULK)){
		WT_ERR(__wt_btree_new_leaf_page(session, &leaf));
		/*设置modify选项*/
		ref->page = leaf;
		ref->state = WT_REF_MEM;
		WT_ERR(__wt_page_modify_init(session, leaf));
		__wt_page_only_modify_set(session, leaf);
	}

	__wt_root_ref_init(&btree->root, root, btree->type != BTREE_ROW);
	return 0;

	
err:/*如果错误，需要废弃掉root和分配的leaf page*/
	if (leaf != NULL)
		__wt_page_out(session, &leaf);
	if (root != NULL)
		__wt_page_out(session, &root);

	return ret;
}

/*创建一个空的leaf page*/
int __wt_btree_new_leaf_page(WT_SESSION_IMPL* session, WT_PAGE* pagep)
{
	WT_BTREE *btree;

	btree = S2BT(session);

	switch(btree->type){
	case BTREE_COL_FIX:
		WT_RET(__wt_page_alloc(session, WT_PAGE_COL_FIX, 1, 0, 0, pagep));
		break;

	case BTREE_COL_VAR:
		WT_RET(__wt_page_alloc(session, WT_PAGE_COL_VAR, 1, 0, 0, pagep));
		break;

	case BTREE_ROW:
		WT_RET(__wt_page_alloc(session, WT_PAGE_ROW_LEAF, 0, 0, 0, pagep));
		break;

	WT_ILLEGAL_VALUE(session);
	}

	return 0;
}

/*标示btree中的page是不能被cahce 驱逐的*/
void __wt_btree_evictable(WT_SESSION_IMPL* session, int on)
{
	WT_BTREE *btree;

	btree = S2BT(session);

	/* The metadata file is never evicted. */
	if (on && !WT_IS_METADATA(btree->dhandle))
		F_CLR(btree, WT_BTREE_NO_EVICTION);
	else
		F_SET(btree, WT_BTREE_NO_EVICTION);
}

/*预读internal page数据*/
static int __btree_preload(WT_SESSION_IMPL* session)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_REF *ref;
	size_t addr_size;
	const uint8_t *addr;

	btree = S2BT(session);
	bm = btree->bm;

	/* Pre-load the second-level internal pages. */
	WT_INTL_FOREACH_BEGIN(session, btree->root.page, ref) {
		WT_RET(__wt_ref_info(session, ref, &addr, &addr_size, NULL));
		if (addr != NULL)
			WT_RET(bm->preload(bm, session, addr, addr_size));
	} WT_INTL_FOREACH_END;

	return 0;
}

/*为列式存储设置最后的记录序号，这个会遍历整个的page??*/
static int __btree_get_last_recno(WT_SESSION_IMPL* session)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	WT_REF *next_walk;

	btree = S2BT(session);

	next_walk = NULL;
	WT_RET(__wt_tree_walk(session, &next_walk, NULL, WT_READ_PREV));
	if (next_walk == NULL)
		return (WT_NOTFOUND);

	page = next_walk->page;
	btree->last_recno = page->type == WT_PAGE_COL_VAR ? __col_var_last_recno(page) : __col_fix_last_recno(page);

	return (__wt_page_release(session, next_walk, 0));
}

/*
 * __btree_page_sizes --
 *	Verify the page sizes. Some of these sizes are automatically checked
 *	using limits defined in the API, don't duplicate the logic here.
 * 确定btree各个page size的大小规格
 */
static int __btree_page_sizes(WT_SESSION_IMPL* session)
{
	WT_BTREE *btree;
	WT_CONFIG_ITEM cval;
	uint64_t cache_size;
	uint32_t intl_split_size, leaf_split_size;
	const char **cfg;

	btree = S2BT(session);
	cfg = btree->dhandle->cfg;

	/*获得cfg配置信息中的allocation size, 如果这个size必须是2的N次方*/
	WT_RET(__wt_direct_io_size_check(session, cfg, "allocation_size", &btree->allocsize));
	if (!__wt_ispo2(btree->allocsize))
		WT_RET_MSG(session, EINVAL, "the allocation size must be a power of two");

	/*获得internal page的最大空间大小和leaf page的最大的page size*/
	WT_RET(__wt_direct_io_size_check(session, cfg, "internal_page_max", &btree->maxintlpage));
	WT_RET(__wt_direct_io_size_check(session, cfg, "leaf_page_max", &btree->maxleafpage));
	if (btree->maxintlpage < btree->allocsize || btree->maxintlpage % btree->allocsize != 0 ||
		btree->maxleafpage < btree->allocsize || btree->maxleafpage % btree->allocsize != 0)
		WT_RET_MSG(session, EINVAL, "page sizes must be a multiple of the page allocation size (%" PRIu32 "B)", btree->allocsize);

	/*确定最大的内存page的大小*/
	WT_RET(__wt_config_gets(session, cfg, "memory_page_max", &cval));
	btree->maxmempage = WT_MAX((uint64_t)cval.val, 50 * (uint64_t)btree->maxleafpage);
	cache_size = S2C(session)->cache_size;
	if (cache_size > 0)
		btree->maxmempage = WT_MIN(btree->maxmempage, cache_size / 4);

	/*确定split internal page和split leaf page的空间大小*/
	WT_RET(__wt_config_gets(session, cfg, "split_pct", &cval));
	btree->split_pct = (int)cval.val;
	intl_split_size = __wt_split_page_size(btree, btree->maxintlpage);
	leaf_split_size = __wt_split_page_size(btree, btree->maxleafpage);

	/*确定deep min/max child参数大小*/
	if (__wt_config_gets(session, cfg, "split_deepen_min_child", &cval) == WT_NOTFOUND || cval.val == 0)
		btree->split_deepen_min_child = WT_SPLIT_DEEPEN_MIN_CHILD_DEF;
	else
		btree->split_deepen_min_child = (u_int)cval.val;
	if (__wt_config_gets(session, cfg, "split_deepen_per_child", &cval) == WT_NOTFOUND || cval.val == 0)
		btree->split_deepen_per_child = WT_SPLIT_DEEPEN_PER_CHILD_DEF;
	else
		btree->split_deepen_per_child = (u_int)cval.val;

	/*确定internal page key/value max*/
	WT_RET(__wt_config_gets(session, cfg, "internal_key_max", &cval));
	btree->maxintlkey = (uint32_t)cval.val;
	if (btree->maxintlkey == 0) {
		WT_RET(__wt_config_gets(session, cfg, "internal_item_max", &cval));
		btree->maxintlkey = (uint32_t)cval.val;
	}

	/*确定leaf page key/value max*/
	WT_RET(__wt_config_gets(session, cfg, "leaf_key_max", &cval));
	btree->maxleafkey = (uint32_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "leaf_value_max", &cval));
	btree->maxleafvalue = (uint32_t)cval.val;

	if (btree->maxleafkey == 0 && btree->maxleafvalue == 0) {
		WT_RET(__wt_config_gets(session, cfg, "leaf_item_max", &cval));
		btree->maxleafkey = (uint32_t)cval.val;
		btree->maxleafvalue = (uint32_t)cval.val;
	}

	/*调整max internal/leaf key的大小*/
	if (btree->maxintlkey == 0 || btree->maxintlkey > intl_split_size / 10)
		btree->maxintlkey = intl_split_size / 10;
	if (btree->maxleafkey == 0)
		btree->maxleafkey = leaf_split_size / 10;
	if (btree->maxleafvalue == 0)
		btree->maxleafvalue = leaf_split_size / 2;

	return 0;
}


