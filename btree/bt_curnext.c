/*******************************************************
* btree cursor移动到下一条记录上
*******************************************************/

#include "wt_internal.h"

/*btree cursor移向下一个记录，仅仅在append list上移动*/
static inline int __cursor_fix_append_next(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	/*新载入的page,判断ins_head是否为空，如果为空表示没有append的记录*/
	if (newpage){
		if ((cbt->ins = WT_SKIP_FIRST(cbt->ins_head)) == NULL)
			return (WT_NOTFOUND);
	}
	else{
		/*已经到append list的最后一条记录了，后面没有记录*/
		if (cbt->recno >= WT_INSERT_RECNO(cbt->ins) && (cbt->ins = WT_SKIP_NEXT(cbt->ins)) == NULL)
			return (WT_NOTFOUND);
	}
	/*
	* This code looks different from the cursor-previous code.  The append
	* list appears on the last page of the tree, but it may be preceded by
	* other rows, which means the cursor's recno will be set to a value and
	* we simply want to increment it.  If the cursor's recno is NOT set,
	* we're starting our iteration in a tree that has only appended items.
	* In that case, recno will be 0 and happily enough the increment will
	* set it to 1, which is correct.
	*/
	__cursor_set_recno(cbt, cbt->recno + 1);

	/*
	* Fixed-width column store appends are inherently non-transactional.
	* Even a non-visible update by a concurrent or aborted transaction
	* changes the effective end of the data.  The effect is subtle because
	* of the blurring between deleted and empty values, but ideally we
	* would skip all uncommitted changes at the end of the data.  This
	* doesn't apply to variable-width column stores because the implicitly
	* created records written by reconciliation are deleted and so can be
	* never seen by a read.
	*
	* The problem is that we don't know at this point whether there may be
	* multiple uncommitted changes at the end of the data, and it would be
	* expensive to check every time we hit an aborted update.  If an
	* insert is aborted, we simply return zero (empty), regardless of
	* whether we are at the end of the data.
	*/
	if (cbt->recno < WT_INSERT_RECNO(cbt->ins) || (upd = __wt_txn_read(session, cbt->ins->upd)) == NULL){ /*没有可见的记录值，直接返回0*/
		cbt->v = 0;
		val->data = &cbt->v;
	}
	else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;
	return 0;
}

/*btree cursor移向下一个记录(fix col方式存储)，在btree树空间上移动*/
static inline int __cursor_fix_next(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_BTREE *btree;
	WT_ITEM *val;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = S2BT(session);
	page = cbt->ref->page;
	val = &cbt->iface.value;

	/*切换到新的page上做next操作*/
	if (newpage){
		cbt->last_standard_recno = __col_fix_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);

		__cursor_set_recno(cbt, page->pg_fix_recno);
		goto new_page;
	}

	/*记录序号超出最后一个序号，到末尾了*/
	if (cbt->recno >= cbt->last_standard_recno)
		return WT_NOTFOUND;
	__cursor_set_recno(cbt, cbt->recno + 1);

new_page:
	/*获得第一个修改列表*/
	cbt->ins_head = WT_COL_UPDATE_SINGLE(page);
	/*定位recno所在的修改条目*/
	cbt->ins = __col_insert_search(cbt->ins_head, cbt->ins_stack, cbt->next_stack, cbt->recno);
	if (cbt->ins != NULL && cbt->recno != WT_INSERT_RECNO(cbt->ins)) /*不正确的修改定位，将ins设置为NULL表示定位失败*/
		cbt->ins = NULL;
	
	/*做事务隔离读取记录版本*/
	upd = cbt->ins == NULL ? NULL : __wt_txn_read(session, cbt->ins->upd);
	if (upd == NULL){
		cbt->v = __bit_getv_recno(page, cbt->recno, btree->bitcnt); /*V赋值*/
		val->data = &cbt->v;
	}
	else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;

	return 0;
}

/*在append list上移动variable-length类型的btree cursor*/
static inline int __cursor_var_append_next(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage){
		cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
		goto new_page;
	}

	for (;;){
		cbt->ins = WT_SKIP_NEXT(cbt->ins);
new_page:
		if (cbt->ins == NULL)
			return (WT_NOTFOUND);

		__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
		/*事务隔离读，对本事务不可见，继续向前*/
		if ((upd = __wt_txn_read(session, cbt->ins->upd)) == NULL)
			continue;
		/*删除集合，不做指向这条记录，继续向下移动*/
		if (WT_UPDATE_DELETED_ISSET(upd)) {
			++cbt->page_deleted_count;
			continue;
		}
		/*赋值value*/
		val->data = WT_UPDATE_DATA(upd);
		val->size = upd->size;

		return 0;
	}
}

/*移向下条variable-length column-store 记录*/
static inline int __cursor_var_next(WT_CURSOR_BTREE* cbt, int newpage)
{
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_COL *cip;
	WT_ITEM *val;
	WT_INSERT *ins;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	uint64_t rle, rle_start;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;

	rle_start = 0;			/* -Werror=maybe-uninitialized */

	if (newpage){
		cbt->last_standard_recno = __col_var_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, page->pg_var_recno);
		goto new_page;
	}

	for (;;){
		if (cbt->recno >= cbt->last_standard_recno)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->recno + 1);

	new_page:
		/*定位到recno对应的WT_COL slot*/
		if ((cip = __col_var_search(page, cbt->recno, &rle_start)) == NULL)
			return (WT_NOTFOUND);
		cbt->slot = WT_COL_SLOT(page, cip);

		/*读取内容值*/
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
		* upd == NULL, 记录可能被删除放入到了insert列表中，slot可能被重用了，那么需要进行cell unpack取值
		*/

		if (cbt->cip_saved != cip) {
			if ((cell = WT_COL_PTR(page, cip)) == NULL)
				continue;
			__wt_cell_unpack(cell, &unpack);
			if (unpack.type == WT_CELL_DEL) {
				if ((rle = __wt_cell_rle(&unpack)) == 1)
					continue;

				/*定位到修改列表中的记录*/
				ins = __col_insert_search_gt(cbt->ins_head, cbt->recno);
				cbt->recno = rle_start + rle;
				if (ins != NULL && WT_INSERT_RECNO(ins) < cbt->recno)
					cbt->recno = WT_INSERT_RECNO(ins);

				/* Adjust for the outer loop increment. */
				--cbt->recno;
				continue;
			}

			/*取出cell中的值到tmp中*/
			WT_RET(__wt_page_cell_data_ref(session, page, &unpack, &cbt->tmp));
			cbt->cip_saved = cip;
		}
		val->data = cbt->tmp.data;
		val->size = cbt->tmp.size;
		return 0;
	}
}

/*移向行存储的下一个行对象*/
static inline int __cursor_row_next(WT_CURSOR_BTREE* cbt, int newpage)
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

	/*假如是newpage，定位到insert修改队列的头位置*/
	if (newpage){
		cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
		cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
		cbt->row_iteration_slot = 1;
		goto new_insert;
	}

	for (;;){
		if (cbt->ins != NULL)
			cbt->ins = WT_SKIP_NEXT(cbt->ins);

new_insert:
		if ((ins = cbt->ins) != NULL) {
			/*事务可见数据读取*/
			if ((upd = __wt_txn_read(session, ins->upd)) == NULL)
				continue;
			/*判断是否删除，如果删除，跳过被删除的对象*/
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				++cbt->page_deleted_count;
				continue;
			}

			key->data = WT_INSERT_KEY(ins);
			key->size = WT_INSERT_KEY_SIZE(ins);
			val->data = WT_UPDATE_DATA(upd);
			val->size = upd->size;
			return 0;
		}

		/*检索page row entires数组， 到了page的末尾*/
		if (cbt->row_iteration_slot >= page->pg_row_entries * 2 + 1)
			return (WT_NOTFOUND);
		++cbt->row_iteration_slot;

		/*
		* Odd-numbered slots configure as WT_INSERT_HEAD entries,
		* even-numbered slots configure as WT_ROW entries.
		*/
		if (cbt->row_iteration_slot & 0x01) {
			cbt->ins_head = WT_ROW_INSERT_SLOT(page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
			goto new_insert;
		}

		cbt->ins_head = NULL;
		cbt->ins = NULL;

		/*计算定位slot*/
		cbt->slot = cbt->row_iteration_slot / 2 - 1;
		rip = &page->pg_row_d[cbt->slot];

		upd = __wt_txn_read(session, WT_ROW_UPDATE(page, rip));
		if (upd != NULL && WT_UPDATE_DELETED_ISSET(upd)) {
			++cbt->page_deleted_count;
			continue;
		}

		return __cursor_row_slot_return(cbt, rip, upd);
	}
}

/*为一个检索操作初始化一个查询的cursor*/
void __wt_btcur_iterate_setup(WT_CURSOR_BTREE* cbt, int next)
{
	WT_PAGE *page;
	WT_UNUSED(next);

	/*
	* We don't currently have to do any setup when we switch between next
	* and prev calls, but I'm sure we will someday -- I'm leaving support
	* here for both flags for that reason.
	*/
	F_SET(cbt, WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV);

	/*初始化del count计数器，在检索的过程需要统计del count*/
	cbt->page_deleted_count = 0;

	if (cbt->ref == NULL)
		return;

	page = cbt->ref->page;

	/*行存储叶子节点*/
	if (page->type == WT_PAGE_ROW_LEAF){
		/*指定iteration slot*/
		/*
		* For row-store pages, we need a single item that tells us the
		* part of the page we're walking (otherwise switching from next
		* to prev and vice-versa is just too complicated), so we map
		* the WT_ROW and WT_INSERT_HEAD insert array slots into a
		* single name space: slot 1 is the "smallest key insert list",
		* slot 2 is WT_ROW[0], slot 3 is WT_INSERT_HEAD[0], and so on.
		* This means WT_INSERT lists are odd-numbered slots, and WT_ROW
		* array slots are even-numbered slots.
		*/
		cbt->row_iteration_slot = (cbt->slot + 1) * 2;
		if (cbt->ins_head != NULL){
			if (cbt->ins_head == WT_ROW_INSERT_SMALLEST(page))
				cbt->row_iteration_slot = 1;
			else
				cbt->row_iteration_slot += 1;
		}
	}
	else{ /*column store page, 计算这个page中最大的记录序号*/
		cbt->last_standard_recno = page->type == WT_PAGE_COL_VAR ? __col_var_last_recno(page) : __col_fix_last_recno(page);
		if (cbt->ins_head != NULL && cbt->ins_head == WT_COL_APPEND(page))
			F_SET(cbt, WT_CBT_ITERATE_APPEND);
	}
}

/*将btree cursor移动到下一个记录*/
int __wt_btcur_next(WT_CURSOR_BTREE *cbt, int truncating)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	uint32_t flags;
	int newpage;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_STAT_FAST_CONN_INCR(session, cursor_next);
	WT_STAT_FAST_DATA_INCR(session, cursor_next);

	/*btree 扫描标示*/
	flags = WT_READ_SKIP_INTL;
	if (truncating)
		LF_SET(WT_READ_TRUNCATE);

	/*激活一个btree cursor*/
	WT_RET(__cursor_func_init(cbt, 0));

	/*初始化cursor*/
	if (!F_ISSET(cbt, WT_CBT_ITERATE_NEXT))
		__wt_btcur_iterate_setup(cbt, 1);

	/*对btree的扫描*/
	for (;;){
		page = cbt->ref == NULL ? NULL : cbt->ref->page;
		WT_ASSERT(session, page == NULL || !WT_PAGE_IS_INTERNAL(page));

		/*column store append方式，在insert header上做扫描*/
		if (F_ISSET(cbt, WT_CBT_ITERATE_APPEND)){
			switch (page->type){
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_append_next(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_append_next(cbt, newpage);
				break;
				WT_ILLEGAL_VALUE_ERR(session);
			}

			if (ret == 0)
				break;

			/*清除掉column store的标记*/
			F_CLR(cbt, WT_CBT_ITERATE_APPEND);
			if (ret != WT_NOTFOUND)
				break;
		}
		else if (page != NULL){
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_next(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_next(cbt, newpage);
				break;
			case WT_PAGE_ROW_LEAF:
				ret = __cursor_row_next(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}

			/*找到对应的记录了，直接返回*/
			if (ret != WT_NOTFOUND)
				break;

			/*假如是column store方式，检查是否要扫描insert header list*/
			if (page->type != WT_PAGE_ROW_LEAF && (cbt->ins_head = WT_COL_APPEND(page)) != NULL) {
				F_SET(cbt, WT_CBT_ITERATE_APPEND);
				continue;
			}
		}

		/*删除的记录太多，对page进行重组，增大page的填充因子*/
		if (page != NULL && (cbt->page_deleted_count > WT_BTREE_DELETE_THRESHOLD || (newpage && cbt->page_deleted_count > 0))){
			__wt_page_evict_soon(page);
		}

		cbt->page_deleted_count = 0;

		/*btree cursor跳转到下一个page上*/
		WT_ERR(__wt_tree_walk(session, &cbt->ref, NULL, flags));
		WT_ERR_TEST(cbt->ref == NULL, WT_NOTFOUND);
	}

err:
	if (ret != 0) /*失败了，恢复cursor的状态*/
		WT_TRET(__cursor_reset(cbt));

	return ret;
}





