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

	/*1G以下的key长度默认是合法的，最大可以到4G*/
	if (kv->size < WT_GIGABYTE)
		return 0;

	/*超过4G，返回异常*/
	if (kv->size > WT_BTREE_MAX_OBJECT_SIZE)
		ret = EINVAL;
	else {
		/*进行block write长度校验，并返回写入后的size,看是否能正常写入*/
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
		if ((cell = WT_COL_PTR(page, cip)) == NULL || __wt_cell_type(cell) == WT_CELL_DEL)
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

/*用btree cursor进行列式检索*/
static inline int __cursor_col_search(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt)
{
	WT_DECL_RET;

	WT_WITH_PAGE_INDEX(session, ret = __wt_col_search(session, cbt->iface.recno, NULL, cbt));
	return (ret);
}
/*用btree cursor进行行式检索*/
static inline int __cursor_row_search(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, int insert)
{
	WT_DECL_RET;

	WT_WITH_PAGE_INDEX(session, ret = __wt_row_search(session, &cbt->iface.key, NULL, cbt, insert));
	return ret;
}

/*Column store 修改操作（包括:删除、插入、update）*/
static inline int __cursor_col_modify(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, int is_remove)
{
	return (__wt_col_modify(session, cbt, cbt->iface.recno, &cbt->iface.value, NULL, is_remove));
}

/*row store 修改操作（包括:删除、插入、update）*/
static inline int __cursor_row_modify(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, int is_remove)
{
	return (__wt_row_modify(session,cbt, &cbt->iface.key, &cbt->iface.value, NULL, is_remove));
}

/*重置btree cursor*/
int _wt_btcur_reset(WT_CURSOR_BTREE* cbt)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_STAT_FAST_CONN_INCR(session, cursor_reset);
	WT_STAT_FAST_DATA_INCR(session, cursor_reset);

	return __cursor_reset(cbt);
}

/* 在btree中进行记录匹配检索 */
int __wt_btcur_search(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	/*检索统计数据的更新*/
	WT_STAT_FAST_CONN_INCR(session, cursor_search);
	WT_STAT_FAST_DATA_INCR(session, cursor_search);

	/*如果btree是row store存储，需要检查key的值大小，如果太大，抛出异常，行记录存储时不能KEY值不能超过block的大小限制*/
	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));

	/*初始化btree cursor*/
	WT_RET(__cursor_func_init(cbt, 1));

	/*进行记录定位*/
	WT_ERR(btree->type == BTREE_ROW ? __cursor_row_search(session, cbt, 0) : __cursor_col_search(session, cbt));
	if (cbt->compare == 0 && __cursor_valid(cbt, &upd)) /*记录找到了，进行value返回*/
		ret = __wt_kv_return(session, cbt, upd);
	else if (__cursor_fix_implicit(btree, cbt)){
		/*
		* Creating a record past the end of the tree in a fixed-length
		* column-store implicitly fills the gap with empty records.
		*/
		cbt->recno = cursor->recno;
		cbt->v = 0;
		cursor->value.data = &cbt->v;
		cursor->value.size = 1;
	}
	else
		ret = WT_NOTFOUND;

err:
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));

	return ret;
}

/*进行key value查找检索，如果定位的记录不在btree上，那么使cursor指向Key位置的前一条或者后一条记录位置*/
int __wt_btcur_search_near(WT_CURSOR_BTREE* cbt, int* exactp)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	int exact;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;
	exact = 0;

	WT_STAT_FAST_CONN_INCR(session, cursor_search_near);
	WT_STAT_FAST_DATA_INCR(session, cursor_search_near);

	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));

	WT_RET(__cursor_func_init(cbt, 1));

	WT_ERR(btree->type == BTREE_ROW ? __cursor_row_search(session, cbt, 1) : __cursor_col_search(session, cbt));

	/*
	* If we find an valid key, return it.
	*
	* Else, creating a record past the end of the tree in a fixed-length
	* column-store implicitly fills the gap with empty records.  In this
	* case, we instantiate the empty record, it's an exact match.
	*
	* Else, move to the next key in the tree (bias for prefix searches).
	* Cursor next skips invalid rows, so we don't have to test for them
	* again.
	*
	* Else, redo the search and move to the previous key in the tree.
	* Cursor previous skips invalid rows, so we don't have to test for
	* them again.
	*
	* If that fails, quit, there's no record to return.
	*/

	/*exact = 0 表示精确定位到key所在的位置，=1表示定位KEY位置的后一条记录， = -1表示定位到KEY所在位置的前一条记录*/
	if (__cursor_valid(cbt, &upd)){ /*key对应的kv对是合法正确的，直接返回*/
		exact = cbt->compare;
		ret = __wt_kv_return(session, cbt, upd);
	}
	else if (__cursor_fix_implicit(btree, cbt)){
		cbt->recno = cursor->recno;
		cbt->v = 0;
		cursor->value.data = &cbt->v;
		cursor->value.size = 1;
		exact = 0;
	}
	else if ((ret = __wt_btcur_next(cbt, 0)) != WT_NOTFOUND){ /*先后移动一个记录，使得cursor定位到后一条记录*/
		exact = 1;
	}
	else{
		/*再次定位到查询的位置，前一个条记录，因为后面的记录在btree是不存在的，那只能指向前一条记录*/
		WT_ERR(btree->type == BTREE_ROW ? __cursor_row_search(session, cbt, 1) : __cursor_col_search(session, cbt));
		if (__cursor_valid(cbt, &upd)) {
			exact = cbt->compare;
			ret = __wt_kv_return(session, cbt, upd);
		}
		else if ((ret = __wt_btcur_prev(cbt, 0)) != WT_NOTFOUND)
			exact = -1;
	}

err:
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));
	if (exactp != NULL && (ret == 0 || ret == WT_NOTFOUND))
		*exactp = exact;

	return ret;
}

/*插入一条记录到btree上*/
int __wt_btcur_insert(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_FAST_CONN_INCR(session, cursor_insert);
	WT_STAT_FAST_DATA_INCR(session, cursor_insert);
	WT_STAT_FAST_DATA_INCRV(session, cursor_insert_bytes, cursor->key.size + cursor->value.size);

	/*对kv的值大小做校验过滤*/
	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));
	WT_RET(__cursor_size_chk(session, &cursor->value));

	/*设置btree上的page是可以驱逐的,防止bulk方式的插入记录无法完全插入到btree上*/
	if (btree->bulk_load_ok){
		btree->bulk_load_ok = 0;
		__wt_btree_evictable(session, 1);
	}

retry:
	WT_RET(__cursor_func_init(cbt, 1));
	switch (btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		/*
		* If WT_CURSTD_APPEND is set, insert a new record (ignoring
		* the application's record number).  First we search for the
		* maximum possible record number so the search ends on the
		* last page.  The real record number is assigned by the
		* serialized append operation.
		*/
		if (F_ISSET(cursor, WT_CURSTD_APPEND))
			cbt->iface.recno = UINT64_MAX;
		WT_ERR(__cursor_col_search(session, cbt));
		if (F_ISSET(cursor, WT_CURSTD_APPEND))
			cbt->iface.recno = 0;

		/*如果是append insert,需要判断键值重复*/
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
			((cbt->compare == 0 && __cursor_valid(cbt, NULL)) || (cbt->compare != 0 && __cursor_fix_implicit(btree, cbt))))
			WT_ERR(WT_DUPLICATE_KEY);

		WT_ERR(__cursor_col_modify(session, cbt, 0));
		if (F_ISSET(cursor, WT_CURSTD_APPEND)) /*将cbt face的位置指向刚刚加入行的序号位置*/
			cbt->iface.recno = cbt->recno;
		break;

	case BTREE_ROW:
		WT_ERR(__cursor_row_search(session, cbt, 1));
		/*键值重复判断*/
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) && cbt->compare == 0 && __cursor_valid(cbt, NULL))
			WT_ERR(WT_DUPLICATE_KEY);

		ret = __cursor_row_modify(session, cbt, 0);
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}

err:
	if (ret == WT_RESTART)
		goto retry;
	if (ret == 0)
		WT_TRET(__curfile_leave(cbt));
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));

	return ret;
}

/*
* __curfile_update_check --
*	Check whether an update would conflict.
*
*	This function expects the cursor to already be positioned.  It should
*	be called before deciding whether to skip an update operation based on
*	existence of a visible update for a key -- even if there is no value
*	visible to the transaction, an update could still conflict.
*	检查更新冲突,会涉及到事务隔离
*/
static int __curfile_update_check(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	session = (WT_SESSION_IMPL *)cbt->iface.session;

	if (cbt->compare != 0)
		return 0;

	if (cbt->ins != NULL)
		return (__wt_txn_update_check(session, cbt->ins->upd));
	if (btree->type == BTREE_ROW && cbt->ref->page->pg_row_upd != NULL)
		return (__wt_txn_update_check(session, cbt->ref->page->pg_row_upd[cbt->slot]));

	return 0;
}
/*
* __wt_btcur_update_check --
*	Check whether an update would conflict.
*
*	This can be used to replace WT_CURSOR::insert or WT_CURSOR::update, so
*	they only check for conflicts without updating the tree.  It is used to
*	maintain snapshot isolation for transactions that span multiple chunks
*	in an LSM tree.
*   检查更新冲突*/
int __wt_btcur_update_check(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	cursor = &cbt->iface;
	btree = cbt->btree;
	session = (WT_SESSION_IMPL *)cursor->session;

retry:
	WT_RET(__cursor_func_init(cbt, 1));
	switch (btree->type){
	case BTREE_ROW:
		WT_ERR(__cursor_row_search(session, cbt, 1));
		ret = __curfile_update_check(cbt);
		break;

	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
	WT_ILLEGAL_VALUE_ERR(session);
	}

err:
	if (ret == WT_RESTART)
		goto retry;
	WT_TRET(__curfile_leave(cbt));
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));

	return ret;
}

/* btree cursor 删除操作实现 */
int __wt_btcur_remove(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_FAST_CONN_INCR(session, cursor_remove);
	WT_STAT_FAST_DATA_INCR(session, cursor_remove);
	WT_STAT_FAST_DATA_INCRV(session, cursor_remove_bytes, cursor->key.size);

	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));
retry:
	WT_RET(__cursor_func_init(cbt, 1));

	switch (btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		WT_ERR(__cursor_col_search(session, cbt));
		/*如果找到了匹配的记录，需要在调用__cursor_valid之前检查更新冲突*/
		WT_ERR(__curfile_update_check(cbt));
		if (cbt->compare != 0 || !__cursor_valid(cbt, NULL)){ /*记录未找到或者不合法*/
			if (!__cursor_fix_implicit(btree, cbt))
				WT_ERR(WT_NOTFOUND);
			cbt->recno = cursor->recno;
		}
		else
			ret = __cursor_col_modify(session, cbt, 1);
		break;

	case BTREE_ROW:
		WT_ERR(__cursor_row_search(session, cbt, 0));
		WT_ERR(__curfile_update_check(cbt));

		if (cbt->compare != 0 || !__cursor_valid(cbt, NULL))
			WT_ERR(WT_NOTFOUND);

		ret = __cursor_row_modify(session, cbt, 1);
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}
err:
	if (ret == WT_RESTART)
		goto retry;

	if (F_ISSET(cursor, WT_CURSTD_OVERWRITE) && ret == WT_NOTFOUND)
		ret = 0;

	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));

	return ret;
}

/*btree的update操作实现*/
int __wt_btcur_update(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_FAST_CONN_INCR(session, cursor_update);
	WT_STAT_FAST_DATA_INCR(session, cursor_update);
	WT_STAT_FAST_DATA_INCRV(session, cursor_update_bytes, cursor->value.size);

	/*对插入的KV长度进行检查*/
	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));
	WT_RET(__cursor_size_chk(session, &cursor->value));

	if (btree->bulk_load_ok){
		btree->bulk_load_ok = 0;
		__wt_btree_evictable(session, 1); /*标记驱逐*/
	}

retry:
	WT_RET(__cursor_func_init(cbt, 1));
	switch (btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		/*检索记录*/
		WT_ERR(__cursor_col_search(session, cbt));
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)){
			WT_ERR(__curfile_update_check(cbt));
			if ((cbt->compare != 0 || !__cursor_valid(cbt, NULL)) && __cursor_fix_implicit(btree, cbt))
				return WT_NOTFOUND;
		}
		/*记录修改*/
		ret = __cursor_col_modify(session, cbt, 0);
		break;

	case BTREE_ROW:
		WT_ERR(__cursor_row_search(session, cbt, 1));
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)){
			WT_ERR(__curfile_update_check(cbt));
			if (cbt->compare != 0 || !__cursor_valid(cbt, NULL))
				WT_ERR(WT_NOTFOUND);
		}
		/*记录修改*/
		ret = __cursor_row_modify(session, cbt, 0);
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}
err:
	if (ret == WT_RESTART)
		goto retry;

	if (ret == 0)
		WT_TRET(__wt_kv_return(session, cbt, cbt->modify_update));

	if (ret != 0)
		WT_TRET(__cursor_reset(ret));

	return ret;
}

/*随机位置移动*/
int __wt_btcur_next_random(WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL*)cbt->iface.session;

	/*只支持row store存储方式*/
	if (btree->type != BTREE_ROW)
		WT_RET(ENOTSUP);

	WT_STAT_FAST_CONN_INCR(session, cursor_next);
	WT_STAT_FAST_DATA_INCR(session, cursor_next);

	WT_RET(__cursor_func_init(cbt, 1));

	WT_WITH_PAGE_INDEX(session, ret = __wt_row_random(session, cbt));
	WT_ERR(ret);
	if (__cursor_valid(cbt, &upd))
		WT_ERR(__wt_kv_return(session, cbt, upd));
	else
		WT_ERR(__wt_btcur_search_near(cbt, 0));

err:
	if (ret != 0)
		WT_TRET(__cursor_reset(cbt));
	return ret;
}

/*比较两个cursor的前后（KEY大小）关系，返回比较结果*/
int __wt_btcur_compare(WT_CURSOR_BTREE* a_arg, WT_CURSOR_BTREE* b_arg, int* cmpp)
{
	WT_CURSOR *a, *b;
	WT_SESSION_IMPL *session;

	a = (WT_CURSOR *)a_arg;
	b = (WT_CURSOR *)b_arg;
	session = (WT_SESSION_IMPL *)a->session;

	if (a_arg->btree != b_arg->btree)
		WT_RET_MSG(session, EINVAL, "Cursors must reference the same object");

	switch (a_arg->btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		if (a->recno < b->recno)
			*cmpp = -1;
		else if (a->recno == b->recno)
			*cmpp = 0;
		else
			*cmpp = 1;
		break;

	case BTREE_ROW:
		/*直接KEY值比较就可以*/
		WT_RET(__wt_compare(session, a_arg->btree->collator, &a->key, &b->key, cmpp));
		break;
		WT_ILLEGAL_VALUE(session);
	}

	return 0;
}

/*判断两个cursor是否指向相同的行*/
static inline int __cursor_equals(WT_CURSOR_BTREE *a, WT_CURSOR_BTREE *b)
{
	switch (a->btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		if (((WT_CURSOR *)a)->recno == ((WT_CURSOR *)b)->recno)
			return 1;
		break;

	case BTREE_ROW:
		if (a->ref != b->ref)
			return 0;
		if (a->ins != NULL || b->ins != NULL) {
			if (a->ins == b->ins)
				return 1;
			break;
		}
		if (a->slot == b->slot)
			return 1;
		break;
	}
	return 0;
}

/*比较两个cursor是否相等,返回值存在equalp, = 1表示相等，=0表示不等*/
int __wt_btcur_equals(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *equalp)
{
	WT_CURSOR *a, *b;
	WT_SESSION_IMPL *session;
	int cmp;

	a = (WT_CURSOR *)a_arg;
	b = (WT_CURSOR *)b_arg;
	session = (WT_SESSION_IMPL *)a->session;

	if (a_arg->btree != b_arg->btree)
		WT_RET_MSG(session, EINVAL, "Cursors must reference the same object");
	/*
	* The reason for an equals method is because we can avoid doing
	* a full key comparison in some cases. If both cursors point into the
	* tree, take the fast path, otherwise fall back to the slower compare
	* method; in both cases, return 1 if the cursors are equal, 0 if they
	* are not.
	*/
	if (F_ISSET(a, WT_CURSTD_KEY_INT) && F_ISSET(b, WT_CURSTD_KEY_INT))
		*equalp = __cursor_equals(a_arg, b_arg);
	else{
		WT_RET(__wt_btcur_compare(a_arg, b_arg, &cmp));
		*equalp = (cmp == 0) ? 1 : 0;
	}

	return 0;
}

/*将btree树上指定范围的记录废弃*/
static int __cursor_truncate(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *start,
	WT_CURSOR_BTREE *stop, int(*rmfunc)(WT_SESSION_IMPL *, WT_CURSOR_BTREE *, int))
{
	WT_DECL_RET;

	/*将start 到 stop之间的记录全部删除*/
	do{
		WT_RET(__wt_btcur_remove(start));
		for (ret = 0;;) {
			if (stop != NULL && __cursor_equals(start, stop))
				break;
			if ((ret = __wt_btcur_next(start, 1)) != 0)
				break;
			start->compare = 0;	/* Exact match */
			if ((ret = rmfunc(session, start, 1)) != 0)
				break;
		}
	} while (ret == WT_RESTART);

	WT_RET_NOTFOUND_OK(ret);
	return 0;
}

static int __cursor_truncate_fix(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop,
	int(*rmfunc)(WT_SESSION_IMPL *, WT_CURSOR_BTREE *, int))
{
	WT_DECL_RET;
	uint8_t* value;

	do{
		WT_RET(__wt_btcur_remove(start));
		for (ret = 0;;) {
			if (stop != NULL && __cursor_equals(start, stop))
				break;
			if ((ret = __wt_btcur_next(start, 1)) != 0)
				break;
			start->compare = 0;	/* Exact match */
			value = (uint8_t *)start->iface.value.data;
			if (*value != 0 && (ret = rmfunc(session, start, 1)) != 0)
				break;
		}
	} while (ret == WT_RESTART);
	WT_RET_NOTFOUND_OK(ret);

	return ret;
}

/*btree上进行范围删除*/
int __wt_btcur_range_truncate(WT_CURSOR_BTREE* start, WT_CURSOR_BTREE* stop)
{
	WT_BTREE *btree;
	WT_CURSOR_BTREE *cbt;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	cbt = (start != NULL) ? start : stop;
	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = cbt->btree;

	WT_ASSERT(session, start != NULL);

	/*
	* For recovery, log the start and stop keys for a truncate operation,
	* not the individual records removed.  On the other hand, for rollback
	* we need to keep track of all the in-memory operations.
	*
	* We deal with this here by logging the truncate range first, then (in
	* the logging code) disabling writing of the in-memory remove records
	* to disk.
	*/
	if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
		WT_RET(__wt_txn_truncate_log(session, start, stop));

	switch (btree->type){
	case BTREE_COL_FIX:
		WT_ERR(__cursor_truncate_fix(session, start, stop, __cursor_col_modify));
		break;

	case BTREE_COL_VAR:
		WT_ERR(__cursor_truncate(session, start, stop, __cursor_col_modify));
		break;

	case BTREE_ROW:
		/*
		* The underlying cursor comparison routine requires cursors be
		* fully instantiated when truncating row-store objects because
		* it's comparing page and/or skiplist positions, not keys. (Key
		* comparison would work, it's only that a key comparison would
		* be relatively expensive, especially with custom collators.
		* Column-store objects have record number keys, so the key
		* comparison is cheap.)  The session truncate code did cursor
		* searches when setting up the truncate so we're good to go: if
		* that ever changes, we'd need to do something here to ensure a
		* fully instantiated cursor.
		*/
		WT_ERR(__cursor_truncate(session, start, stop, __cursor_row_modify));
		break;
	}

err:
	if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
		WT_TRET(__wt_txn_truncate_end(session));

	return ret;
}

/*关闭btree cursor*/
int __wt_btcur_close(WT_CURSOR_BTREE* cbt)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	ret = __curfile_leave(cbt);
	__wt_buf_free(session, &cbt->search_key);
	__wt_buf_free(session, &cbt->tmp);

	return ret;
}










