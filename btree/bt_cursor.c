/********************************************************
* btree cursor实现
********************************************************/

#include "wt_internal.h"

/*检查插入的k/v的value是否太大,如果太大抛出一个错误信息*/
static inline int __cursor_size_chk(WT_SESSION_IMPL* session, WT_ITEM* kv)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_RET;
	size_t size;

	btree = S2BT(session);
	bm = btree->bm;

	/*列式存储的定长模型必须V是单字节*/
	if (btree->type == BTREE_COL_FIX){
		if (kv->size != 1)
			WT_RET_MSG(session, EINVAL, "item size of %" WT_SIZET_FMT " does not match fixed-length file requirement of 1 byte", kv->size);
		return 0;
	}

	/*1G以下的长度默认是合法的，最大可以到4G*/
	if (kv->size < WT_GIGABYTE)
		return 0;

	/*超过4G，返回异常*/
	if (kv->size > WT_BTREE_MAX_OBJECT_SIZE)
		ret = EINVAL;
	else {
		/*进行block write尝试，并返回写入后的size,看是否能正常写入*/
		size = kv->size;
		ret = bm->write_size(bm, session, &size);
	}

	if (ret != 0)
		WT_RET_MSG(session, ret, "item size of %" WT_SIZET_FMT " exceeds the maximum supported size", kv->size);

	return 0;
}

/*
 * __cursor_fix_implicit --
 *	Return if search went past the end of the tree.
 */
static inline int __cursor_fix_implicit(WT_BTREE* btree, WT_CURSOR_BTREE* cbt)
{
	return (btree->type == BTREE_COL_FIX && !F_ISSET(cbt, WT_CBT_MAX_RECORD) ? 1 : 0);
}

/*
* __cursor_valid --
*	Return if the cursor references an valid key/value pair.
*/
static inline int __cursor_valid(WT_CURSOR_BTREE* cbt, WT_UPDATE** updp)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_COL *cip;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	btree = cbt->btree;
	page = cbt->ref->page;
	session = (WT_SESSION_IMPL *)cbt->iface.session;
	if (updp != NULL)
		*updp = NULL;

	/*
	* We may be pointing to an insert object, and we may have a page with
	* existing entries.  Insert objects always have associated update
	* objects (the value).  Any update object may be deleted, or invisible
	* to us.  In the case of an on-page entry, there is by definition a
	* value that is visible to us, the original page cell.
	*
	* If we find a visible update structure, return our caller a reference
	* to it because we don't want to repeatedly search for the update, it
	* might suddenly become invisible (imagine a read-uncommitted session
	* with another session's aborted insert), and we don't want to handle
	* that potential error every time we look at the value.
	*
	* Unfortunately, the objects we might have and their relationships are
	* different for the underlying page types.
	*
	* In the case of row-store, an insert object implies ignoring any page
	* objects, no insert object can have the same key as an on-page object.
	* For row-store:
	*	if there's an insert object:
	*		if there's a visible update:
	*			exact match
	*		else
	*			no exact match
	*	else
	*		use the on-page object (which may have an associated
	*		update object that may or may not be visible to us).
	*
	* Column-store is more complicated because an insert object can have
	* the same key as an on-page object: updates to column-store rows
	* are insert/object pairs, and an invisible update isn't the end as
	* there may be an on-page object that is visible.  This changes the
	* logic to:
	*	if there's an insert object:
	*		if there's a visible update:
	*			exact match
	*		else if the on-page object's key matches the insert key
	*			use the on-page object
	*	else
	*		use the on-page object
	*
	* First, check for an insert object with a visible update (a visible
	* update that's been deleted is not a valid key/value pair).
	*/
	if (cbt->ins != NULL && (upd = __wt_txn_read(session, cbt->ins->upd)) != NULL) {
		if (WT_UPDATE_DELETED_ISSET(upd))
			return 0;
		if (updp != NULL)
			*updp = upd;
		return 1;
	}
	/*
	* If we don't have an insert object, or in the case of column-store,
	* there's an insert object but no update was visible to us and the key
	* on the page is the same as the insert object's key, and the slot as
	* set by the search function is valid, we can use the original page
	* information.
	*/
	switch (btree->type){
	case BTREE_COL_FIX:
		/*
		* If search returned an insert object, there may or may not be
		* a matching on-page object, we have to check.  Fixed-length
		* column-store pages don't have slots, but map one-to-one to
		* keys, check for retrieval past the end of the page.
		*/
		if (cbt->recno >= page->pg_fix_recno + page->pg_fix_entries)
			return 0;

		break;
	case BTREE_COL_VAR:
		/*
		* If search returned an insert object, there may or may not be
		* a matching on-page object, we have to check.  Variable-length
		* column-store pages don't map one-to-one to keys, but have
		* "slots", check if search returned a valid slot.
		*/
		if (cbt->slot >= page->pg_var_entries)
			return 0;

		/*
		* Updates aren't stored on the page, an update would have
		* appeared as an "insert" object; however, variable-length
		* column store deletes are written into the backing store,
		* check the cell for a record already deleted when read.
		*/
		cip = &page->pg_var_d[cbt->slot];
		if ((cell = WT_COL_PTR(page, cip)) == NULL ||
			__wt_cell_type(cell) == WT_CELL_DEL)
			return 0;
		break;

	case BTREE_ROW:
		/*
		* See above: for row-store, no insert object can have the same
		* key as an on-page object, we're done.
		*/
		if (cbt->ins != NULL)
			return 0;

		/*
		* Check if searched returned a valid slot (the failure mode is
		* an empty page, the search function doesn't check, and so the
		* more exact test is "page->pg_row_entries == 0", but this test
		* mirrors the column-store test).
		*/
		if (cbt->slot >= page->pg_row_entries)
			return 0;

		/* Updates are stored on the page, check for a delete. */
		if (page->pg_row_upd != NULL && (upd = __wt_txn_read(session, page->pg_row_upd[cbt->slot])) != NULL) {
			if (WT_UPDATE_DELETED_ISSET(upd))
				return 0;
			if (updp != NULL)
				*updp = upd;
		}
		break;
	}

	return 1;
}

static inline int __cursor_col_search(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt)
{
	WT_DECL_RET;

	WT_WITH_PAGE_INDEX(session, ret = __wt_col_search(session, cbt->iface.recno, NULL, cbt));
	return (ret);
}



