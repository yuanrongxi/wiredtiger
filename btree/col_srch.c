/**********************************************************
 * 实现column store btree的检索操作
 *********************************************************/
#include "wt_internal.h"

/*column store btree 检索实现，通过recno对应的key来检索*/
int __wt_col_search(WT_SESSION_IMPL* session, uint64_t recno, WT_REF* leaf, WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_COL *cip;
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_INSERT_HEAD *ins_head;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *current, *descent;
	uint32_t base, indx, limit;
	int depth;

	btree = S2BT(session);

	__cursor_pos_clear(cbt);

	/*如果引擎正在eviction splits操作，只做leaf page中内容的检索*/
	if (leaf != NULL){
		current = leaf;
		goto leaf_only;
	}

	/*从root page开始对整个btree做检索*/
	current = &btree->root;
	for (depth = 2;; ++depth){
restart:
		page = current->page;
		if (page->type != WT_PAGE_COL_INT)
			break;

		WT_ASSERT(session, current->key.recno == page->pg_intl_recno);

		WT_INTL_INDEX_GET(session, page, pindex);
		base = pindex->entries;
		descent = pindex->index[base - 1];

		/*进行下一层级的检索*/
		if (recno >= descent->key.recno)
			goto descend;

		/*二分法在索引page中做查找,定位到下一级page*/
		for (base = 0, limit = pindex->entries - 1; limit != 0; limit >>= 1){
			indx = base + (limit >> 1);
			descent = pindex->index[indx];

			if (recno == descent->key.recno)
				break;
			if (recno < descent->key.recno)
				continue;
			base = indx + 1;
			--limit;
		}

	descend:
		/*没有完全匹配的记录，那么定位到当前记录的前面一条记录位置，因为是后面这个条记录KEY是大于要查找的KEY，那么要查找的位置是在这条记录的前面*/
		if (recno != descent->key.recno) {
			/*
			* We don't have to correct for base == 0 because the
			* only way for base to be 0 is if recno is the page's
			* starting recno.
			*/
			WT_ASSERT(session, base > 0);
			descent = pindex->index[base - 1];
		}

		/*将下一级的page读入到内存，与正在操作的page做swap操作*/
		switch (ret = __wt_page_swap(session, current, descent, 0)) {
		case 0:
			current = descent;
			break;
		case WT_RESTART:
			goto restart;
		default:
			return (ret);
		}
	}

	/* Track how deep the tree gets. */
	if (depth > btree->maximum_depth)
		btree->maximum_depth = depth;

leaf_only:
	page = current->page;
	cbt->ref = current;
	cbt->recno = recno;
	cbt->compare = 0;

	/*
	* Set the on-page slot to an impossible value larger than any possible
	* slot (it's used to interpret the search function's return after the
	* search returns an insert list for a page that has no entries).
	*/
	cbt->slot = UINT32_MAX;

	/*进行btree cursor信息更新*/
	if (page->type == WT_PAGE_COL_FIX) {
		if (recno >= page->pg_fix_recno + page->pg_fix_entries) {
			cbt->recno = page->pg_fix_recno + page->pg_fix_entries;
			goto past_end;
		}
		else
			ins_head = WT_COL_UPDATE_SINGLE(page);
	}
	else{
		if ((cip = __col_var_search(page, recno, NULL)) == NULL) {
			cbt->recno = __col_var_last_recno(page);
			goto past_end;
		}
		else {
			cbt->slot = WT_COL_SLOT(page, cip);
			ins_head = WT_COL_UPDATE_SLOT(page, cbt->slot);
		}
	}

	if ((ins = __col_insert_search(ins_head, cbt->ins_stack, cbt->next_stack, recno)) != NULL)
		if (recno == WT_INSERT_RECNO(ins)) {
			cbt->ins_head = ins_head;
			cbt->ins = ins;
		}

	return 0;

past_end:
	/*
	* A record past the end of the page's standard information.  Check the
	* append list; by definition, any record on the append list is closer
	* than the last record on the page, so it's a better choice for return.
	* This is a rarely used path: we normally find exact matches, because
	* column-store files are dense, but in this case the caller searched
	* past the end of the table.
	*
	* Don't bother searching if the caller is appending a new record where
	* we'll allocate the record number; we're not going to find a match by
	* definition, and we figure out the position when we do the work.
	*/
	cbt->ins_head = WT_COL_APPEND(page);
	if (recno == UINT64_MAX)
		cbt->ins = NULL;
	else
		cbt->ins = __col_insert_search(cbt->ins_head, cbt->ins_stack, cbt->next_stack, recno);

	if (cbt->ins == NULL)
		cbt->compare = -1;
	else {
		cbt->recno = WT_INSERT_RECNO(cbt->ins);
		if (recno == cbt->recno)
			cbt->compare = 0;
		else if (recno < cbt->recno)
			cbt->compare = 1;
		else
			cbt->compare = -1;
	}

	/*
	* Note if the record is past the maximum record in the tree, the cursor
	* search functions need to know for fixed-length column-stores because
	* appended records implicitly create any skipped records, and cursor
	* search functions have to handle that case.
	*/

	/*recno到了最大的限制，设置cursor的状态*/
	if(cbt->compare == -1)
		F_SET(cbt, WT_CBT_MAX_RECORD);

	return 0;
}




