/*************************************************************
* btree cursor向前移动
*************************************************************/
#include "wt_internal.h"

/*
* Walking backwards through skip lists.
*
* The skip list stack is an array of pointers set up by a search.  It points
* to the position a node should go in the skip list.  In other words, the skip
* list search stack always points *after* the search item (that is, into the
* search item's next array).
*
* Helper macros to go from a stack pointer at level i, pointing into a next
* array, back to the insert node containing that next array.
*/

/*获得的第i个insert队列的前一个*/
#undef PREV_ITEM
#define	PREV_ITEM(ins_head, insp, i)					\
	(((insp) == &(ins_head)->head[i] || (insp) == NULL) ? NULL : (WT_INSERT *)((char *)((insp) - (i)) - offsetof(WT_INSERT, next)))

#undef	PREV_INS
#define	PREV_INS(cbt, i)	PREV_ITEM((cbt)->ins_head, (cbt)->ins_stack[(i)], (i))

/*在skip list stack中向前移动一个位置*/
static inline int __cursor_skip_prev(WT_CURSOR_BTREE* cbt)
{
	WT_INSERT *current, *ins;
	WT_ITEM key;
	WT_SESSION_IMPL *session;
	int i;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

restart:
	while ((current = cbt->ins) != PREV_INS(cbt, 0)){
		if (cbt->btree->type == BTREE_ROW) {
			key.data = WT_INSERT_KEY(current);
			key.size = WT_INSERT_KEY_SIZE(current);
			WT_RET(__wt_search_insert(session, cbt, &key));
		}
		else
			cbt->ins = __col_insert_search(cbt->ins_head, cbt->ins_stack, cbt->next_stack, WT_INSERT_RECNO(current));
	}

	/*
	* Find the first node up the search stack that does not move.
	*
	* The depth of the current item must be at least this level, since we
	* see it in that many levels of the stack.
	*
	* !!! Watch these loops carefully: they all rely on the value of i,
	* and the exit conditions to end up with the right values are
	* non-trivial.
	*/

	ins = NULL;			/* -Wconditional-uninitialized */
	for (i = 0; i < WT_SKIP_MAXDEPTH - 1; i++)
		if ((ins = PREV_INS(cbt, i + 1)) != current)
			break;

	/*
	* Find a starting point for the new search.  That is either at the
	* non-moving node if we found a valid node, or the beginning of the
	* next list down that is not the current node.
	*
	* Since it is the beginning of a list, and we know the current node is
	* has a skip depth at least this high, any node we find must sort
	* before the current node.
	*/
	if (ins == NULL || ins == current){
		for (; i >= 0; i--) {
			cbt->ins_stack[i] = NULL;
			cbt->next_stack[i] = NULL;
			ins = cbt->ins_head->head[i];
			if (ins != NULL && ins != current)
				break;
		}
	}

	/* Walk any remaining levels until just before the current node. */
	while (i >= 0) {
		/*
		* If we get to the end of a list without finding the current
		* item, we must have raced with an insert.  Restart the search.
		*/
		if (ins == NULL) {
			cbt->ins_stack[0] = NULL;
			cbt->next_stack[0] = NULL;
			goto restart;
		}
		if (ins->next[i] != current)		/* Stay at this level */
			ins = ins->next[i];
		else {					/* Drop down a level */
			cbt->ins_stack[i] = &ins->next[i];
			cbt->next_stack[i] = ins->next[i];
			--i;
		}
	}

	/* If we found a previous node, the next one must be current. */
	if (cbt->ins_stack[0] != NULL && *cbt->ins_stack[0] != current)
		goto restart;

	/*btree cursor指向前一个*/
	cbt->ins = PREV_INS(cbt, 0);

	return 0;
}

/*返回append list中的前一个fix length entry*/
static inline int __cursor_fix_append_prev(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_ITEM *val;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;
	
	if (newpage){
		if ((cbt->ins = WT_SKIP_LAST(cbt->ins_head)) == NULL)
			return WT_NOTFOUND;
	}
	else{
		/*
		* Handle the special case of leading implicit records, that is,
		* there aren't any records in the tree not on the append list,
		* and the first record on the append list isn't record 1.
		*
		* The "right" place to handle this is probably in our caller.
		* The high-level cursor-previous routine would:
		*    -- call this routine to walk the append list
		*    -- call the routine to walk the standard page items
		*    -- call the tree walk routine looking for a previous page
		* Each of them returns WT_NOTFOUND, at which point our caller
		* checks the cursor record number, and if it's larger than 1,
		* returns the implicit records.  Instead, I'm trying to detect
		* the case here, mostly because I don't want to put that code
		* into our caller.  Anyway, if this code breaks for any reason,
		* that's the way I'd go.
		*
		* If we're not pointing to a WT_INSERT entry, or we can't find
		* a WT_INSERT record that precedes our record name-space, check
		* if there are any records on the page.  If there aren't, then
		* we're in the magic zone, keep going until we get to a record
		* number of 1.
		*/

		if (cbt->ins != NULL && cbt->recno <= WT_INSERT_RECNO(cbt->ins))
			WT_RET(__cursor_skip_prev(cbt));
		/*已经到page的最前面了，返回没有找到*/
		if (cbt->ins == NULL && (cbt->recno == 1 || __col_fix_last_recno(page) != 0))
			return WT_NOTFOUND;
	}

	/*记录序号向前递减*/
	if (newpage)
		__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
	else
		__cursor_set_recno(cbt, cbt->recno - 1);

	/*获取记录值(v)*/
	if (cbt->ins == NULL || cbt->recno > WT_INSERT_RECNO(cbt->ins) || (upd = __wt_txn_read(session, cbt->ins->upd)) == NULL) {
		cbt->v = 0;
		val->data = &cbt->v;
	}
	else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;

	return 0;
}

/*btree cursor的fix column store方式向前移动一个记录*/
static inline int __cursor_fix_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_BTREE *btree;
	WT_ITEM *val;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	btree = S2BT(session);
	val = &cbt->iface.value;

	/* Initialize for each new page. 如果是新页, 将cursor移动到page的末尾最大记录序号上 */
	if (newpage) {
		cbt->last_standard_recno = __col_fix_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	if (cbt->recno == page->pg_fix_recno)
		return WT_NOTFOUND;
	__cursor_set_recno(cbt, cbt->recno - 1);

new_page:
	cbt->ins_head = WT_COL_UPDATE_SINGLE(page);
	cbt->ins = __col_insert_search(cbt->ins_head, cbt->ins_stack, cbt->next_stack, cbt->recno);
	if (cbt->ins != NULL && cbt->recno != WT_INSERT_RECNO(cbt->ins))
		cbt->ins = NULL;

	/*获得修改记录的版本对象*/
	upd = cbt->ins == NULL ? NULL : __wt_txn_read(session, cbt->ins->upd);
	if (upd == NULL) {
		cbt->v = __bit_getv_recno(page, cbt->recno, btree->bitcnt);
		val->data = &cbt->v;
	}
	else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;

	return 0;
}

/*将btree cursor在variable-length类型的btree上向前移动一个记录序号，并且返回记录的entry*/
static inline int __cursor_var_append_prev(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage){
		cbt->ins = WT_SKIP_LAST(cbt->ins_head);
		goto new_page;
	}

	for (;;){
		/*向前移动*/
		WT_RET(__cursor_skip_prev(cbt));
new_page:
		if (cbt->ins == NULL)
			return WT_NOTFOUND;

		/*设置btree cursor当前的记录序号*/
		__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
		/*读取记录版本对象*/
		if ((upd = __wt_txn_read(session, cbt->ins->upd)) == NULL)
			continue;
		/*删除判断*/
		if (WT_UPDATE_DELETED_ISSET(upd)) {
			++cbt->page_deleted_count;
			continue;
		}
		val->data = WT_UPDATE_DATA(upd);
		val->size = upd->size;

		return 0;
	}
}

/*将btree cursor在variable length column store方式的tree上前移一个记录项*/
static inline int __cursor_var_prev(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_COL *cip;
	WT_INSERT *ins;
	WT_ITEM *val;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	uint64_t rle_start;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;

	rle_start = 0;

	if (newpage){
		cbt->last_standard_recno = __col_var_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return WT_NOTFOUND;

		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	for (;;){
		__cursor_set_recno(cbt, cbt->recno - 1);

new_page:
		if (cbt->recno < page->pg_var_recno)
			return (WT_NOTFOUND);

		if ((cip = __col_var_search(page, cbt->recno, &rle_start)) == NULL)
			return WT_NOTFOUND;

		cbt->slot = WT_COL_SLOT(page, cip);
		/*对insert list进行检查*/
		cbt->ins_head = WT_COL_UPDATE_SLOT(page, cbt->slot);
		cbt->ins = __col_insert_search_match(cbt->ins_head, cbt->recno);
		upd = cbt->ins == NULL ? NULL : __wt_txn_read(session, cbt->ins->upd);
		if (upd != NULL) {
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				++cbt->page_deleted_count;
				continue;
			}

			val->data = WT_UPDATE_DATA(upd);
			val->size = upd->size;
			return (0);
		}
		/*
		* If we're at the same slot as the last reference and there's
		* no matching insert list item, re-use the return information
		* (so encoded items with large repeat counts aren't repeatedly
		* decoded).  Otherwise, unpack the cell and build the return
		* information.
		*/
		if (cip != cbt->cip_saved){
			/*获得cip对应的value所在page内存空间的位置*/
			if ((cell = WT_COL_PTR(page, cip)) == NULL)
				continue;

			/*进行cell解析*/
			__wt_cell_unpack(cell, &unpack);
			if (unpack.type == WT_CELL_DEL) {
				if (__wt_cell_rle(&unpack) == 1)
					continue;

				/*前向查找*/
				ins = __col_insert_search_lt(cbt->ins_head, cbt->recno);

				/*recno前移*/
				cbt->recno = rle_start - 1;
				if (ins != NULL && WT_INSERT_RECNO(ins) > cbt->recno)
					cbt->recno = WT_INSERT_RECNO(ins);

				/* Adjust for the outer loop decrement. */
				++cbt->recno;
				continue;
			}

			WT_RET(__wt_page_cell_data_ref(session, page, &unpack, &cbt->tmp));
			cbt->cip_saved = cip;
		}
		val->data = cbt->tmp.data;
		val->size = cbt->tmp.size;
		return 0;
	}
}

/*btree cursor在row store中前移*/
static inline int __cursor_row_prev(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_INSERT *ins;
	WT_ITEM *key, *val;
	WT_PAGE *page;
	WT_ROW *rip;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	key = &cbt->iface.key;
	val = &cbt->iface.value;

	if (newpage){
		if (!F_ISSET_ATOMIC(page, WT_PAGE_BUILD_KEYS))
			WT_RET(__wt_row_leaf_keys(session, page));

		/*
		* For row-store pages, we need a single item that tells us the part
		* of the page we're walking (otherwise switching from next to prev
		* and vice-versa is just too complicated), so we map the WT_ROW and
		* WT_INSERT_HEAD insert array slots into a single name space: slot 1
		* is the "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is
		* WT_INSERT_HEAD[0], and so on.  This means WT_INSERT lists are
		* odd-numbered slots, and WT_ROW array slots are even-numbered slots.
		*
		* New page configuration.
		*/

		/*确定ins head位置，让ins从末尾向前移动*/
		if (page->pg_row_entries == 0)
			cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
		else
			cbt->ins_head = WT_ROW_INSERT_SLOT(page, page->pg_row_entries - 1);

		cbt->ins = WT_SKIP_LAST(cbt->ins_head);
		cbt->row_iteration_slot = page->pg_row_entries * 2 + 1;
		goto new_insert;
	}

	/* Move to the previous entry and return the item. */
	for (;;){
		if (cbt->ins != NULL)
			WT_RET(__cursor_skip_prev(cbt));

new_insert:
		if ((ins = cbt->ins) != NULL){
			upd = __wt_txn_read(session, ins->upd);
			if (upd == NULL)
				continue;

			key->data = WT_INSERT_KEY(ins);
			key->size = WT_INSERT_KEY_SIZE(ins);
			val->data = WT_UPDATE_DATA(upd);
			val->size = upd->size;

			return 0;
		}

		if (cbt->row_iteration_slot == 1)
			return WT_NOTFOUND;

		--cbt->row_iteration_slot;

		/*
		* Odd-numbered slots configure as WT_INSERT_HEAD entries,
		* even-numbered slots configure as WT_ROW entries.
		*/
		if (cbt->row_iteration_slot & 0x01){
			cbt->ins_head = cbt->row_iteration_slot == 1 ?
				WT_ROW_INSERT_SMALLEST(page) : WT_ROW_INSERT_SLOT(page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = WT_SKIP_LAST(cbt->ins_head);
			goto new_insert;
		}

		cbt->ins_head = NULL;
		cbt->ins = NULL;

		/*在row list中查找*/
		cbt->slot = cbt->row_iteration_slot / 2 - 1;
		rip = &page->pg_row_d[cbt->slot];
		upd = __wt_txn_read(session, WT_ROW_UPDATE(page, rip));
		if (upd != NULL && WT_UPDATE_DELETED_SET(upd)){
			++cbt->page_deleted_count;
			continue;
		}

		return __cursor_row_slot_return(cbt, rip, upd);
	}
}

/* btree cursor 向前移动 */
int __wt_btcur_prev(WT_CURSOR_BTREE *cbt, int truncating)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	uint32_t flags;
	int newpage;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_STAT_FAST_CONN_INCR(session, cursor_prev);
	WT_STAT_FAST_DATA_INCR(session, cursor_prev);

	flags = WT_READ_PREV | WT_READ_SKIP_INTL;	/* Tree walk flags. */
	if (truncating)
		LF_SET(WT_READ_TRUNCATE);

	WT_RET(__cursor_func_init(cbt, 0));

	if (!F_ISSET(cbt, WT_CBT_ITERATE_PREV))
		__wt_btcur_iterate_setup(cbt, 0);

	for (newpage = 0;; newpage = 1){
		page = cbt->ref == NULL ? NULL : cbt->ref->page;
		WT_ASSERT(session, page == NULL || !WT_PAGE_IS_INTERNAL(page));

		if (newpage && page != NULL && page->type != WT_PAGE_ROW_LEAF && (cbt->ins_head = WT_COL_APPEND(page)) != NULL)
			F_SET(cbt, WT_CBT_ITERATE_APPEND);

		/*在append list中向前*/
		if (F_ISSET(cbt, WT_CBT_ITERATE_APPEND)) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_append_prev(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_append_prev(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}

			if (ret == 0)
				break;

			F_CLR(cbt, WT_CBT_ITERATE_APPEND);
			if (ret != WT_NOTFOUND)
				break;

			newpage = 1;
		}
		/*在通用存储空间中移动*/
		if (page != NULL) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_prev(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_prev(cbt, newpage);
				break;
			case WT_PAGE_ROW_LEAF:
				ret = __cursor_row_prev(cbt, newpage);
				break;
				WT_ILLEGAL_VALUE_ERR(session);
			}
			if (ret != WT_NOTFOUND)
				break;
		}

		/*page的填充因子太小，进行page重组*/
		if (page != NULL && (cbt->page_deleted_count > WT_BTREE_DELETE_THRESHOLD || (newpage && cbt->page_deleted_count > 0)))
			__wt_page_evict_soon(page);
		cbt->page_deleted_count = 0;

		WT_ERR(__wt_tree_walk(session, &cbt->ref, NULL, flags));
		WT_ERR_TEST(cbt->ref == NULL, WT_NOTFOUND);
	}

err:
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));

	return ret;
}





