/**************************************************
 * overflow item 的磁盘读写操作实现
 *************************************************/

#include "wt_internal.h"

/* 从磁盘中读取一个overflow item到内存中 */
static int __ovfl_read(WT_SESSION_IMPL* session, const uint8_t* addr, size_t addr_size, WT_ITEM* store)
{
	WT_BTREE* btree;
	const WT_PAGE_HEADER* dsk;

	btree = S2BT(session);

	/* overflow item是同步从磁盘读取到内中，这个过程可能需要一些时间，但wiredtiger是支持512M大小的page页的，也就是说overflow存在的概率较低 */
	WT_RET(__wt_bt_read(session, store, addr, addr_size));
	/*将block去掉页头数据，直接将store指向数据其实位置*/
	dsk = store->data;
	store->data = WT_PAGE_HEADER_BYTE(btree, dsk);
	store->size = dsk->u.datalen;

	WT_STAT_FAST_DATA_INCR(session, cache_read_overflow);

	return 0;
}

/* 将一个overflow item 载入内存 */
int __wt_ovfl_read(WT_SESSION_IMPL* session, WT_PAGE* page, WT_CELL_UNPACK* unpack, WT_ITEM* store)
{
	WT_DECL_RET;

	/*没有指定overflow item的索引存储的page,直接从unpack中的block addr指定的文件中读取数据*/
	if (page == NULL)
		return  __ovfl_read(session, unpack->data, unpack->size, store);

	/*如果unpack是WT_CELL_VALUE_OVFL_RM类型，表示记录数据被标记为删除，这个时候需要读取到标记删除之前的版本数据*/

	WT_RET(__wt_readlock(session, S2BT(session)->ovfl_lock));
	ret = __wt_cell_type_raw(unpack->cell) == WT_CELL_VALUE_OVFL_RM ?
		__wt_ovfl_txnc_search(page, unpack->data, unpack->size, store) : __ovfl_read(session, unpack->data, unpack->size, store);
	WT_TRET(__wt_readunlock(session, S2BT(session)->ovfl_lock));

	return ret;
}

/* 检查column store 的全局事务可见更新属性 */
static int __ovfl_cache_col_visible(WT_SESSION_IMPL* session, WT_UPDATE* upd, WT_CELL_UNPACK* unpack)
{
	/*
	* Column-store is harder than row_store: we're here because there's a
	* reader in the system that might read the original version of an
	* overflow record, which might match a number of records.  For example,
	* the original overflow value was for records 100-200, we've replaced
	* each of those records individually, but there exists a reader that
	* might read any one of those records, and all of those records have
	* different update entries with different transaction IDs.  Since it's
	* infeasible to determine if there's a globally visible update for each
	* reader for each record, we test the simple case where a single record
	* has a single, globally visible update.  If that's not the case, cache
	* the value.
	*/
	if (__wt_cell_rle(unpack) == 1 && upd != NULL & __wt_txn_visible_all(session, upd->txnid))
		return 1;
	return 0;
}

/* 检查row store 的全局事务可见更新属性 */
static int __ovfl_cache_row_visible(WT_SESSION_IMPL* session, WT_PAGE* page, WT_ROW* rip)
{
	WT_UPDATE *upd;

	/* Check to see if there's a globally visible update. */
	for (upd = WT_ROW_UPDATE(page, rip); upd != NULL; upd = upd->next)
		if (__wt_txn_visible_all(session, upd->txnid))
			return 1;

	return 0;
}

/* cache 一个标记删除的记录值 */
static int __ovfl_cache(WT_SESSION_IMPL* session, WT_PAGE* page, WT_CELL_UNPACK* unpack)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	size_t addr_size;
	const uint8_t *addr;

	addr = unpack->data;
	addr_size = unpack->size;

	WT_RET(__wt_scr_alloc(session, 1024, &tmp));

	WT_ERR(__ovfl_read(session, addr, addr_size, tmp));
	WT_ERR(__wt_ovfl_txnc_add(session, page, addr, addr_size, tmp->data, tmp->size));

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*
* __wt_ovfl_cache --
*	Handle deletion of an overflow value.
*/
int __wt_ovfl_cache(WT_SESSION_IMPL* session, WT_PAGE* page, void* cookie, WT_CELL_UNPACK* vpack)
{
	int visible;

   /*
	* This function solves a problem in reconciliation. The scenario is:
	*     - reconciling a leaf page that references an overflow item
	*     - the item is updated and the update committed
	*     - a checkpoint runs, freeing the backing overflow blocks
	*     - a snapshot transaction wants the original version of the item
	*
	* In summary, we may need the original version of an overflow item for
	* a snapshot transaction after the item was deleted from a page that's
	* subsequently been checkpointed, where the checkpoint must know about
	* the freed blocks.  We don't have any way to delay a free of the
	* underlying blocks until a particular set of transactions exit (and
	* this shouldn't be a common scenario), so cache the overflow value in
	* memory.
	*
	* This gets hard because the snapshot transaction reader might:
	*     - search the WT_UPDATE list and not find an useful entry
	*     - read the overflow value's address from the on-page cell
	*     - go to sleep
	*     - checkpoint runs, caches the overflow value, frees the blocks
	*     - another thread allocates and overwrites the blocks
	*     - the reader wakes up and reads the wrong value
	*
	* Use a read/write lock and the on-page cell to fix the problem: hold
	* a write lock when changing the cell type from WT_CELL_VALUE_OVFL to
	* WT_CELL_VALUE_OVFL_RM and hold a read lock when reading an overflow
	* item.
	*
	* The read/write lock is per btree, but it could be per page or even
	* per overflow item.  We don't do any of that because overflow values
	* are supposed to be rare and we shouldn't see contention for the lock.
	*
	* Check for a globally visible update.  If there is a globally visible
	* update, we don't need to cache the item because it's not possible for
	* a running thread to have moved past it.
	*/

	switch (page->type) {
	case WT_PAGE_COL_VAR:
		visible = __ovfl_cache_col_visible(session, cookie, vpack);
		break;
	case WT_PAGE_ROW_LEAF:
		visible = __ovfl_cache_row_visible(session, page, cookie);
		break;
		WT_ILLEGAL_VALUE(session);
	}

	/*如果新的修改对读事务是不可见的，那么需要返回修改前的值给读事务，并cache之*/
	if (!visible){
		WT_RET(__ovfl_cache(session, page, vpack));
		WT_STAT_FAST_DATA_INCR(session, cache_overflow_value);
	}

	/*
	* Queue the on-page cell to be set to WT_CELL_VALUE_OVFL_RM and the
	* underlying overflow value's blocks to be freed when reconciliation
	* completes.
	*/
	return __wt_ovfl_discard_add(session, page, vpack->cell);
}

/* 废弃一个overflow类型的值，并且重置cell */
int __wt_ovfl_discard(WT_SESSION_IMPL* session, WT_CELL* cell)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_RET;

	btree = S2BT(session);
	bm = btree->bm;
	unpack = &_unpack;

	__wt_cell_unpack(cell, unpack);

	/*
	* Finally remove overflow key/value objects, called when reconciliation
	* finishes after successfully writing a page.
	*
	* Keys must have already been instantiated and value objects must have
	* already been cached (if they might potentially still be read by any
	* running transaction).
	*
	* Acquire the overflow lock to avoid racing with a thread reading the
	* backing overflow blocks.
	*/

	WT_RET(__wt_writelock(session, btree->ovfl_lock));

	/*重置page中对应cell的值，表示废弃了overflow item*/
	switch (unpack->raw){
	case WT_CELL_KEY_OVFL:
		__wt_cell_type_reset(session, unpack->cell, WT_CELL_KEY_OVFL, WT_CELL_KEY_OVFL_RM);
		break;

	case WT_CELL_VALUE_OVFL:
		__wt_cell_type_reset(session, unpack->cell, WT_CELL_VALUE_OVFL, WT_CELL_VALUE_OVFL_RM);
		break;

		WT_ILLEGAL_VALUE(session);
	}

	WT_TRET(__wt_writeunlock(session, btree->ovfl_lock));

	/*废弃overflow对应block文件*/
	WT_TRET(bm->free(bm, session, unpack->data, unpack->size));

	return ret;
}





