/*************************************************************************
 * 内存中的btree的verify的过程
 ************************************************************************/

#include "wt_internal.h"

#define	WT_VRFY_DUMP(vs)	((vs)->dump_address || (vs)->dump_blocks || (vs)->dump_pages || (vs)->dump_shape)

typedef struct  
{
	uint64_t		record_total;
	WT_ITEM*		max_key;
	WT_ITEM*		max_addr;
	uint64_t		fcnt;

	int				dump_address;
	int				dump_blocks;
	int				dump_pages;
	int				dump_shape;

	u_int depth, depth_internal[100], depth_leaf[100];

	WT_ITEM*		tmp1;
	WT_ITEM*		tmp2;
} WT_VSTUFF;

static void __verify_checkpoint_reset(WT_VSTUFF *);
static int  __verify_overflow(WT_SESSION_IMPL *, const uint8_t *, size_t, WT_VSTUFF *);
static int  __verify_overflow_cell(WT_SESSION_IMPL *, WT_REF *, int *, WT_VSTUFF *);
static int  __verify_row_int_key_order(WT_SESSION_IMPL *, WT_PAGE *, WT_REF *, uint32_t, WT_VSTUFF *);
static int  __verify_row_leaf_key_order(WT_SESSION_IMPL *, WT_REF *, WT_VSTUFF *);
static int  __verify_tree(WT_SESSION_IMPL *, WT_REF *, WT_VSTUFF *);

/* 获取各个配置信息中verify dump信息 */
static int __verify_config(WT_SESSION_IMPL* session, const char* cfg[], WT_VSTUFF* vs)
{
	WT_CONFIG_ITEM cval;

	WT_RET(__wt_config_gets(session, cfg, "dump_address", &cval));
	vs->dump_address = cval.val != 0;

	WT_RET(__wt_config_gets(session, cfg, "dump_blocks", &cval));
	vs->dump_blocks = cval.val != 0;

	WT_RET(__wt_config_gets(session, cfg, "dump_pages", &cval));
	vs->dump_pages = cval.val != 0;

	WT_RET(__wt_config_gets(session, cfg, "dump_shape", &cval));
	vs->dump_shape = cval.val != 0;

#if !defined(HAVE_DIAGNOSTIC)
	if (vs->dump_blocks || vs->dump_pages)
		WT_RET_MSG(session, ENOTSUP, "the WiredTiger library was not built in diagnostic mode");
#endif
	return 0;
}

/*从配置信息中获取指定block的dump信息*/
static int __verify_config_offsets(WT_SESSION_IMPL* session, const char* cfg[], int* quitp)
{
	WT_CONFIG list;
	WT_CONFIG_ITEM cval, k, v;
	WT_DECL_RET;
	u_long offset;

	*quitp = 0;

	WT_RET(__wt_config_gets(session, cfg, "dump_offsets", &cval));
	WT_RET(__wt_config_subinit(session, &list, &cval));
	while ((ret = __wt_config_next(&list, &k, &v)) == 0) {
		/*
		 * Quit after dumping the requested blocks.  (That's hopefully
		 * what the user wanted, all of this stuff is just hooked into
		 * verify because that's where we "dump blocks" for debugging.)
		 */
		*quitp = 1;
		if (v.len != 0 || sscanf(k.str, "%lu", &offset) != 1)
			WT_RET_MSG(session, EINVAL, "unexpected dump offset format");
#if !defined(HAVE_DIAGNOSTIC)
		WT_RET_MSG(session, ENOTSUP, "the WiredTiger library was not built in diagnostic mode");
#else
		WT_TRET(__wt_debug_offset_blind(session, (wt_off_t)offset, NULL));
#endif
	}
	return (ret == WT_NOTFOUND ? 0 : ret);
}

/*dump btree的模型信息,主要是page的关系拓扑*/
static int __verify_tree_shape(WT_SESSION_IMPL* session, WT_VSTUFF* vs)
{
	size_t i;

	/*内部索引页的打印*/
	WT_RET(__wt_msg(session, "Internal page tree-depth:"));
	for (i = 0; i < WT_ELEMENTS(vs->depth_internal); ++i)
		if (vs->depth_internal[i] != 0)
			WT_RET(__wt_msg(session, "\t%03zu: %u", i, vs->depth_internal[i]));

	/*叶子数据页的打印*/
	WT_RET(__wt_msg(session, "Leaf page tree-depth:"));
	for (i = 0; i < WT_ELEMENTS(vs->depth_leaf); ++i)
		if (vs->depth_leaf[i] != 0)
			WT_RET(__wt_msg(session, "\t%03zu: %u", i, vs->depth_leaf[i]));

	return 0;
}

int __wt_verify(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CKPT *ckptbase, *ckpt;
	WT_DECL_RET;
	WT_VSTUFF *vs, _vstuff;
	size_t root_addr_size;
	uint8_t root_addr[WT_BTREE_MAX_ADDR_COOKIE];
	int bm_start, quit;

	btree = S2BT(session);
	bm = btree->bm;
	ckptbase = NULL;
	bm_start = 0;

	WT_CLEAR(_vstuff);
	vs = &_vstuff;

	WT_ERR(__wt_scr_alloc(session, 0, &vs->max_key));
	WT_ERR(__wt_scr_alloc(session, 0, &vs->max_addr));
	WT_ERR(__wt_scr_alloc(session, 0, &vs->tmp1));
	WT_ERR(__wt_scr_alloc(session, 0, &vs->tmp2));

	WT_ERR(__verify_config(session, cfg, vs));

	/* Optionally dump specific block offsets. */
	WT_ERR(__verify_config_offsets(session, cfg, &quit));
	if (quit)
		goto done;

		/* Get a list of the checkpoints for this file. */
	WT_ERR(__wt_meta_ckptlist_get(session, btree->dhandle->name, &ckptbase));

	/* Inform the underlying block manager we're verifying. */
	WT_ERR(bm->verify_start(bm, session, ckptbase));
	bm_start = 1;

	/* Loop through the file's checkpoints, verifying each one. */
	WT_CKPT_FOREACH(ckptbase, ckpt) {
		WT_ERR(__wt_verbose(session, WT_VERB_VERIFY, "%s: checkpoint %s", btree->dhandle->name, ckpt->name));

		/* Fake checkpoints require no work. */
		if (F_ISSET(ckpt, WT_CKPT_FAKE))
			continue;

		/* House-keeping between checkpoints. */
		__verify_checkpoint_reset(vs);

		if (WT_VRFY_DUMP(vs))
			WT_ERR(__wt_msg(session, "%s: checkpoint %s", btree->dhandle->name, ckpt->name));

		/* Load the checkpoint. */
		WT_ERR(bm->checkpoint_load(bm, session, ckpt->raw.data, ckpt->raw.size, root_addr, &root_addr_size, 1));

		/*
		 * Ignore trees with no root page.
		 * Verify, then discard the checkpoint from the cache.
		 */
		if (root_addr_size != 0 && (ret = __wt_btree_tree_open(session, root_addr, root_addr_size)) == 0) {
			if (WT_VRFY_DUMP(vs))
				WT_ERR(__wt_msg(session, "Root: %s %s",
				    __wt_addr_string(session, root_addr, root_addr_size, vs->tmp1), __wt_page_type_string(btree->root.page->type)));

			WT_WITH_PAGE_INDEX(session, ret = __verify_tree(session, &btree->root, vs));

			WT_TRET(__wt_cache_op(session, NULL, WT_SYNC_DISCARD));
		}

		/* Unload the checkpoint. */
		WT_TRET(bm->checkpoint_unload(bm, session));
		WT_ERR(ret);

		/* Display the tree shape. */
		if (vs->dump_shape)
			WT_ERR(__verify_tree_shape(session, vs));
	}

done:
err:	/* Inform the underlying block manager we're done. */
	if (bm_start)
		WT_TRET(bm->verify_end(bm, session));

	/* Discard the list of checkpoints. */
	if (ckptbase != NULL)
		__wt_meta_ckptlist_free(session, ckptbase);

	/* Wrap up reporting. */
	WT_TRET(__wt_progress(session, NULL, vs->fcnt));

	/* Free allocated memory. */
	__wt_scr_free(session, &vs->max_key);
	__wt_scr_free(session, &vs->max_addr);
	__wt_scr_free(session, &vs->tmp1);
	__wt_scr_free(session, &vs->tmp2);

	return (ret);
}

static void __verify_checkpoint_reset(WT_VSTUFF *vs)
{
	/*
	 * Key order is per checkpoint, reset the data length that serves as a
	 * flag value.
	 */
	vs->max_addr->size = 0;

	/* Record total is per checkpoint, reset the record count. */
	vs->record_total = 0;

	/* Tree depth. */
	vs->depth = 1;
}

/*进行btree的verify操作，通过逐层扫描来进行verify，主要是校验page和整个树的层次逻辑关系*/
static int __verify_tree(WT_SESSION_IMPL* session, WT_REF* ref, WT_VSTUFF* vs)
{
	WT_BM *bm;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_COL *cip;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_REF *child_ref;
	uint64_t recno;
	uint32_t entry, i;
	int found;

	bm = S2BT(session)->bm;
	page = ref->page;

	unpack = &_unpack;
	WT_CLEAR(*unpack);	/* -Wuninitialized */

	/* block address的dump信息 */
	WT_RET(__wt_verbose(session, WT_VERB_VERIFY, "%s %s", __wt_page_addr_string(session, ref, vs->tmp1), __wt_page_type_string(page->type)));

	/* Optionally dump the address. */
	if (vs->dump_address)
		WT_RET(__wt_msg(session, "%s %s", __wt_page_addr_string(session, ref, vs->tmp1), __wt_page_type_string(page->type)));

	/*索引page计数*/
	if(WT_PAGE_IS_INTERNAL(page))
		++vs->depth_internal[WT_MIN(vs->depth, WT_ELEMENTS(vs->depth_internal) - 1)];
	else /*叶子page计数*/
		++vs->depth_leaf[WT_MIN(vs->depth, WT_ELEMENTS(vs->depth_internal) - 1)];

	/*
	 * The page's physical structure was verified when it was read into
	 * memory by the read server thread, and then the in-memory version
	 * of the page was built.   Now we make sure the page and tree are
	 * logically consistent.
	 *
	 * !!!
	 * The problem: (1) the read server has to build the in-memory version
	 * of the page because the read server is the thread that flags when
	 * any thread can access the page in the tree; (2) we can't build the
	 * in-memory version of the page until the physical structure is known
	 * to be OK, so the read server has to verify at least the physical
	 * structure of the page; (3) doing complete page verification requires
	 * reading additional pages (for example, overflow keys imply reading
	 * overflow pages in order to test the key's order in the page); (4)
	 * the read server cannot read additional pages because it will hang
	 * waiting on itself.  For this reason, we split page verification
	 * into a physical verification, which allows the in-memory version
	 * of the page to be built, and then a subsequent logical verification
	 * which happens here.
	 *
	 * Report progress every 10 pages.
	 */
	if (++vs->fcnt % 10 == 0)
		WT_RET(__wt_progress(session, NULL, vs->fcnt));

	/*
	 * Column-store key order checks: check the page's record number and
	 * then update the total record count.
	 */
	switch(page->type){
	case WT_PAGE_COL_FIX:
		recno = page->pg_fix_recno;
		goto recno_chk;

	case WT_PAGE_COL_INT:
		recno = page->pg_intl_recno;
		goto recno_chk;

	case WT_PAGE_COL_VAR:
		recno = page->pg_var_recno;

recno_chk:	/*校验recno与vs->record_total是否匹配*/
		if (recno != vs->record_total + 1)
			WT_RET_MSG(session, WT_ERROR,
			"page at %s has a starting record of %" PRIu64
			" when the expected starting record is %" PRIu64,
			__wt_page_addr_string(session, ref, vs->tmp1), recno, vs->record_total + 1)
		break;
	}

	/*对vs->recno_total进行统计增加*/
	switch(page->type){
	case WT_PAGE_COL_FIX:
		vs->record_total += page->pg_fix_entries;
		break;
	case WT_PAGE_COL_VAR:
		recno = 0;
		WT_COL_FOREACH(page, cip, i){
			if ((cell = WT_COL_PTR(page, cip)) == NULL)
				++recno;
			else{
				__wt_cell_unpack(cell, unpack);
				recno += __wt_cell_rle(unpack);
			}
		}
		vs->record_total += recno;
		break;
	}

	/*对row store的校验*/
	switch (page->type) {
	case WT_PAGE_ROW_LEAF:
		WT_RET(__verify_row_leaf_key_order(session, ref, vs));
		break;
	}

	/* If it's not the root page, unpack the parent cell. */
	if (!__wt_ref_is_root(ref)) {
		__wt_cell_unpack(ref->addr, unpack);

		/* Compare the parent cell against the page type. */
		switch (page->type) {
		case WT_PAGE_COL_FIX:
			if (unpack->raw != WT_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case WT_PAGE_COL_VAR:
			if (unpack->raw != WT_CELL_ADDR_LEAF &&
				unpack->raw != WT_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case WT_PAGE_ROW_LEAF:
			if (unpack->raw != WT_CELL_ADDR_DEL &&
				unpack->raw != WT_CELL_ADDR_LEAF &&
				unpack->raw != WT_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case WT_PAGE_COL_INT:
		case WT_PAGE_ROW_INT:
			if (unpack->raw != WT_CELL_ADDR_INT)
celltype_err:			
			WT_RET_MSG(session, WT_ERROR,
						"page at %s, of type %s, is referenced in "
						"its parent by a cell of type %s",
						__wt_page_addr_string(session, ref, vs->tmp1),
						__wt_page_type_string(page->type),
						__wt_cell_type_string(unpack->raw));
			break;
		}
	}

	/*
	 * Check overflow pages.  We check overflow cells separately from other
	 * tests that walk the page as it's simpler, and I don't care much how
	 * fast table verify runs.
	 */
	switch (page->type) {
	case WT_PAGE_COL_VAR:
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		WT_RET(__verify_overflow_cell(session, ref, &found, vs));
		if (__wt_ref_is_root(ref) || page->type == WT_PAGE_ROW_INT)
			break;

		/*
		 * Object if a leaf-no-overflow address cell references a page
		 * with overflow keys, but don't object if a leaf address cell
		 * references a page without overflow keys.  Reconciliation
		 * doesn't guarantee every leaf page without overflow items will
		 * be a leaf-no-overflow type.
		 */
		if (found && unpack->raw == WT_CELL_ADDR_LEAF_NO)
			WT_RET_MSG(session, WT_ERROR,
			    "page at %s, of type %s and referenced in its "
			    "parent by a cell of type %s, contains overflow "
			    "items",
			    __wt_page_addr_string(session, ref, vs->tmp1),
			    __wt_page_type_string(page->type),
			    __wt_cell_type_string(WT_CELL_ADDR_LEAF_NO));
		break;
	}

	/* Check tree connections and recursively descend the tree. 整个过程是个递归过程的检查 */
	switch (page->type) {
	case WT_PAGE_COL_INT:
		/* For each entry in an internal page, verify the subtree. */
		entry = 0;
		WT_INTL_FOREACH_BEGIN(session, page, child_ref) {
			/*
			 * It's a depth-first traversal: this entry's starting
			 * record number should be 1 more than the total records
			 * reviewed to this point.
			 */
			++entry;
			if (child_ref->key.recno != vs->record_total + 1) {
				WT_RET_MSG(session, WT_ERROR,
				    "the starting record number in entry %"
				    PRIu32 " of the column internal page at "
				    "%s is %" PRIu64 " and the expected "
				    "starting record number is %" PRIu64,
				    entry,
				    __wt_page_addr_string(session, child_ref, vs->tmp1),
				    child_ref->key.recno, vs->record_total + 1);
			}

			/* Verify the subtree. */
			++vs->depth;
			WT_RET(__wt_page_in(session, child_ref, 0));
			ret = __verify_tree(session, child_ref, vs);
			WT_TRET(__wt_page_release(session, child_ref, 0));
			--vs->depth;
			WT_RET(ret);

			__wt_cell_unpack(child_ref->addr, unpack);
			WT_RET(bm->verify_addr(bm, session, unpack->data, unpack->size));
		} WT_INTL_FOREACH_END;
		break;

	case WT_PAGE_ROW_INT:
		/* For each entry in an internal page, verify the subtree. */
		entry = 0;
		WT_INTL_FOREACH_BEGIN(session, page, child_ref) {
			/*
			 * It's a depth-first traversal: this entry's starting
			 * key should be larger than the largest key previously
			 * reviewed.
			 *
			 * The 0th key of any internal page is magic, and we
			 * can't test against it.
			 */
			++entry;
			if (entry != 1)
				WT_RET(__verify_row_int_key_order(session, page, child_ref, entry, vs));

			/* Verify the subtree. */
			++vs->depth;
			WT_RET(__wt_page_in(session, child_ref, 0));
			ret = __verify_tree(session, child_ref, vs);
			WT_TRET(__wt_page_release(session, child_ref, 0));
			--vs->depth;
			WT_RET(ret);

			__wt_cell_unpack(child_ref->addr, unpack);
			WT_RET(bm->verify_addr(bm, session, unpack->data, unpack->size));
		} WT_INTL_FOREACH_END;
		break;
	}

	return 0;
}

/* 校验索引页中key的连续性和排序行（BTREE是按key从小到大排序的） */
static int __verify_row_int_key_order(WT_SESSION_IMPL *session, WT_PAGE *parent, WT_REF *ref, uint32_t entry, WT_VSTUFF *vs)
{
	WT_BTREE *btree;
	WT_ITEM item;
	int cmp;

	btree = S2BT(session);

	/* The maximum key is set, we updated it from a leaf page first. */
	WT_ASSERT(session, vs->max_addr->size != 0);

	/* Get the parent page's internal key. */
	__wt_ref_key(parent, ref, &item.data, &item.size);

	/* Compare the key against the largest key we've seen so far. */
	WT_RET(__wt_compare(session, btree->collator, &item, vs->max_key, &cmp));

	/*vs->max_key比当前internal key大，说明有更大的key在前面，这违反了btree从小到大的排序规则 */
	if(cmp <= 0)
		WT_RET_MSG(session, WT_ERROR,
		"the internal key in entry %" PRIu32 " on the page at %s "
		"sorts before the last key appearing on page %s, earlier in the tree",
		entry, __wt_page_addr_string(session, ref, vs->tmp1),(char *)vs->max_addr->data);

	/*将internal key作为vs->max_key*/
	WT_RET(__wt_buf_set(session, vs->max_key, item.data, item.size));
	(void)__wt_page_addr_string(session, ref, vs->max_addr);

	return 0;
}

/* 对leaf page中的key做连续性和排序性检查 */
static int __verify_row_leaf_key_order(WT_SESSION_IMPL* session, WT_REF* ref, WT_VSTUFF* vs)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	int cmp;

	btree = S2BT(session);
	page = ref->page;

	/*叶子节点没有数据，说明是一个刚建立的btree，直接返回即可*/
	if(page->pg_row_entries == 0)
		return 0;

	if(vs->max_addr->size != 0){
		/*将page中第一个key拷贝出来进行比较，如果第一个key是小于vs->max_key,btree树不是以从小到大排序的，存在错误*/
		WT_RET(__wt_row_leaf_key_copy(session, page, page->pg_row_d, vs->tmp1));

		WT_RET(__wt_compare(session, btree->collator, vs->tmp1, (WT_ITEM *)vs->max_key, &cmp));
		if(cmp < 0)
			WT_RET_MSG(session, WT_ERROR,
			"the first key on the page at %s sorts equal to or "
			"less than a key appearing on the page at %s, earlier in the tree",
			__wt_page_addr_string(session, ref, vs->tmp1), (char *)vs->max_addr->data);
	}

	/*将页的最后一个KV作为max_key*/
	WT_RET(__wt_row_leaf_key_copy(session, page, page->pg_row_d + (page->pg_row_entries - 1), vs->max_key));
	(void)__wt_page_addr_string(session, ref, vs->max_addr);

	return 0;
}

/*对overflow cell的校验*/
static int __verify_overflow_cell(WT_SESSION_IMPL *session, WT_REF *ref, int *found, WT_VSTUFF *vs)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_RET;
	const WT_PAGE_HEADER *dsk;
	uint32_t cell_num, i;

	btree = S2BT(session);
	unpack = &_unpack;
	*found = 0;

	/*page是空的，btree刚刚建立*/
	if((dsk = ref->page->dsk) == NULL)
		return 0;

	/* 扫描disk的page,校验所有的overflow cell */
	cell_num = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;
		__wt_cell_unpack(cell, unpack);

		switch (unpack->type) {
		case WT_CELL_KEY_OVFL:
		case WT_CELL_VALUE_OVFL:
			*found = 1;
			WT_ERR(__verify_overflow(session, unpack->data, unpack->size, vs));
			break;
		}
	}

	return 0;

err:
	WT_RET_MSG(session, ret,
		"cell %" PRIu32 " on page at %s references an overflow item at %s that failed verification",
		cell_num - 1,
		__wt_page_addr_string(session, ref, vs->tmp1),
		__wt_addr_string(session, unpack->data, unpack->size, vs->tmp2));
}

/* 从磁盘上读取一个overflow page到内存，并进行校验 */
static int __verify_overflow(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, WT_VSTUFF *vs)
{
	WT_BM *bm;
	const WT_PAGE_HEADER *dsk;

	bm = S2BT(session)->bm;

	/*读取overflow block到内存中*/
	WT_RET(__wt_bt_read(session, vs->tmp1, addr, addr_size));

	/*索引page已经校验过了，那么需要校验overflow对应的page是否是我们预期的数据,需要校验block各个占用标示位来确定合法性*/
	dsk = vs->tmp1->data;
	if (dsk->type != WT_PAGE_OVFL)
		WT_RET_MSG(session, WT_ERROR, "overflow referenced page at %s is not an overflow page",
		__wt_addr_string(session, addr, addr_size, vs->tmp1));

	WT_RET(bm->verify_addr(bm, session, addr, addr_size));
	return 0;
}

