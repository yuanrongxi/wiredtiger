/****************************************************************
* btree的状态统计
****************************************************************/
#include "wt_internal.h"

static int  __stat_page(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *cst);
static void __stat_page_col_var(WT_PAGE *page, WT_DSRC_STATS *cst);
static void __stat_page_row_int(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *cst);
static void __stat_page_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *cst);

/*初始化btree的统计状态信息*/
int __wt_btree_stat_init(WT_SESSION_IMPL *session, WT_CURSOR_STAT *cst)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_DSRC_STATS *stats;
	WT_REF *next_walk;

	btree = S2BT(session);
	bm = btree->bm;
	stats = &btree->dhandle->stats;

	WT_RET(bm->stat(bm, session, stats));

	WT_STAT_SET(stats, btree_fixed_len, btree->bitcnt);
	WT_STAT_SET(stats, btree_maximum_depth, btree->maximum_depth);
	WT_STAT_SET(stats, btree_maxintlpage, btree->maxintlpage);
	WT_STAT_SET(stats, btree_maxintlkey, btree->maxintlkey);
	WT_STAT_SET(stats, btree_maxleafpage, btree->maxleafpage);
	WT_STAT_SET(stats, btree_maxleafkey, btree->maxleafkey);
	WT_STAT_SET(stats, btree_maxleafvalue, btree->maxleafvalue);
	/*如果不需要统计全部的信息，到此为止即可*/
	if (!F_ISSET(cst, WT_CONN_STAT_ALL))
		return 0;

	/*统计计数器清零*/
	WT_STAT_SET(stats, btree_column_deleted, 0);
	WT_STAT_SET(stats, btree_column_fix, 0);
	WT_STAT_SET(stats, btree_column_internal, 0);
	WT_STAT_SET(stats, btree_column_variable, 0);
	WT_STAT_SET(stats, btree_entries, 0);
	WT_STAT_SET(stats, btree_overflow, 0);
	WT_STAT_SET(stats, btree_row_internal, 0);
	WT_STAT_SET(stats, btree_row_leaf, 0);

	/*遍历所有的page，并对page的信息做统计*/
	next_walk = NULL;
	while ((ret = __wt_tree_walk(session, &next_walk, NULL, 0)) == 0 && next_walk != NULL){
		WT_WITH_PAGE_INDEX(session, ret = __stat_page(session, next_walk->page, stats));
		WT_RET(ret);
	}

	return (ret == WT_NOTFOUND ? 0 : ret);
}

/*统计page中的信息，主要包括：行列数等*/
static int __stat_page(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	switch (page->type){
	case WT_PAGE_COL_FIX:
		WT_STAT_INCR(stats, btree_column_fix);
		WT_STAT_INCRV(stats, btree_entries, page->pg_fix_entries);
		break;

	case WT_PAGE_COL_INT:
		WT_STAT_INCR(stats, btree_column_internal);
		break;

	case WT_PAGE_COL_VAR:
		__stat_page_col_var(page, stats);
		break;

	case WT_PAGE_ROW_INT:
		__stat_page_row_int(session, page, stats);
		break;

	case WT_PAGE_ROW_LEAF:
		__stat_page_row_leaf(session, page, stats);
		break;

		WT_ILLEGAL_VALUE(session);
	}

	return 0;
}

/*计算一个WT_PAGE_COL_VAR page的统计信息,主要信息有：标记删除的k/v数,溢出page的k/v数，k/v总数*/
static void __stat_page_col_var(WT_PAGE* page, WT_DSRC_STATS* stats)
{
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_COL *cip;
	WT_INSERT *ins;
	WT_UPDATE *upd;
	uint64_t deleted_cnt, entry_cnt, ovfl_cnt;
	uint32_t i;
	int orig_deleted;

	unpack = &_unpack;
	deleted_cnt = entry_cnt = ovfl_cnt = 0;

	WT_STAT_INCR(stats, btree_column_variable);

	WT_COL_FOREACH(page, cip, i){
		if ((cell = WT_COL_PTR(page, cip)) == NULL){ /*cell被删除*/
			orig_deleted = 1;
			++deleted_cnt;
		}
		else{
			orig_deleted = 0;
			
			__wt_cell_unpack(cell, unpack);
			if (unpack->type == WT_CELL_ADDR_DEL)
				orig_deleted = 1;
			else
				entry_cnt += __wt_cell_rle(unpack);

			if (unpack->ovfl)
				++ovfl_cnt;
		}

		/*扫描insert list*/
		WT_SKIP_FOREACH(ins, WT_COL_UPDATE(page, cip)){
			upd = ins->upd;
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				if (!orig_deleted) { /*记录标记为删除，删除的计数器需要+1*/
					++deleted_cnt;
					--entry_cnt;
				}
			}
			else if (orig_deleted) {
				--deleted_cnt;
				++entry_cnt;
			}
		}
	}

	/*扫描最后一个insert list*/
	WT_SKIP_FOREACH(ins, WT_COL_APPEND(page))
		if (WT_UPDATE_DELETED_ISSET(ins->upd))
			++deleted_cnt;
		else
			++entry_cnt;

	/*将统计的信息填入stats中*/
	WT_STAT_INCRV(stats, btree_column_deleted, deleted_cnt);
	WT_STAT_INCRV(stats, btree_entries, entry_cnt);
	WT_STAT_INCRV(stats, btree_overflow, ovfl_cnt);
}

/*统计行存储的internal page的信息,主要是统计溢出类型*/
static void __stat_page_row_int(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	uint32_t i, ovfl_cnt;

	btree = S2BT(session);
	ovfl_cnt = 0;

	WT_STAT_INCR(stats, btree_row_internal);

	if (page->dsk != NULL){
		WT_CELL_FOREACH(btree, page->dsk, cell, &unpack, i) {
			__wt_cell_unpack(cell, &unpack);
			if (__wt_cell_type(cell) == WT_CELL_KEY_OVFL) /*key值属于溢出类型*/
				++ovfl_cnt;
		}

		WT_STAT_INCRV(stats, btree_overflow, ovfl_cnt);
	}
}

/*统计行存储的叶子页信息*/
static void __stat_page_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_INSERT *ins;
	WT_ROW *rip;
	WT_UPDATE *upd;
	uint32_t entry_cnt, i, ovfl_cnt;

	btree = S2BT(session);
	entry_cnt = ovfl_cnt = 0;

	WT_STAT_INCR(stats, btree_row_leaf);

	/*统计insert list中的有效行K/V*/
	WT_SKIP_FOREACH(ins, WT_ROW_INSERT_SMALLEST(page)){
		if (!WT_UPDATE_DELETED_ISSET(ins->upd))
			++entry_cnt;
	}

	/*遍历page中所有的k/v，并做计数统计*/
	WT_ROW_FOREACH(page, rip, i) {
		upd = WT_ROW_UPDATE(page, rip);
		if (upd == NULL || !WT_UPDATE_DELETED_ISSET(upd))
			++entry_cnt;

		if (upd == NULL && (cell = __wt_row_leaf_value_cell(page, rip, NULL)) != NULL && __wt_cell_type(cell) == WT_CELL_VALUE_OVFL)
			++ovfl_cnt;

		/* Walk K/V pairs inserted after the on-page K/V pair. */
		WT_SKIP_FOREACH(ins, WT_ROW_INSERT(page, rip))
			if (!WT_UPDATE_DELETED_ISSET(ins->upd))
				++entry_cnt;
	}

	/*直接去遍历所有的over flow类型的key是很困难的，我们可以扫描page->dsk的cell对象来计算他们*/
	if (page->dsk != NULL){
		WT_CELL_FOREACH(btree, page->dsk, cell, &unpack, i) {
			__wt_cell_unpack(cell, &unpack);
			if (__wt_cell_type(cell) == WT_CELL_KEY_OVFL)
				++ovfl_cnt;
		}
	}

	WT_STAT_INCRV(stats, btree_entries, entry_cnt);
	WT_STAT_INCRV(stats, btree_overflow, ovfl_cnt);
}



