/*****************************************************
 * 实现column store的记录修改操作，包括：新增、更新和删除
 ****************************************************/
#include "wt_internal.h"

static int __col_insert_alloc(WT_SESSION_IMPL* session, uint64_t recno, u_int skipdepth, WT_INSERT** insp, size_t* ins_sizep);

/* 实现column store的新增、更新和删除 */
int __wt_col_modify(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, uint64_t recno, WT_ITEM* value, WT_UPDATE* upd, int is_remove)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_INSERT_HEAD *ins_head, **ins_headp;
	WT_ITEM _value;
	WT_PAGE *page;
	WT_UPDATE *old_upd;
	size_t ins_size, upd_size;
	u_int i, skipdepth;
	int append, logged;

	btree = cbt->btree;
	ins = NULL;
	page = cbt->ref->page;
	append = logged = 0;

	/* 如果是删除操作，那么value一定是NULL */
	if (is_remove){
		if (btree->type == BTREE_COL_FIX){
			value = &_value;
			value->data = "";
			value->size = 1;
		}
		else
			value = NULL;
	}
	else{
		/* 表示是新增追加方式, 直接操作append list */
		if (recno == 0 || recno > (btree->type == BTREE_COL_VAR ? __col_var_last_recno(page) : __col_fix_last_recno(page)))
			append = 1;
	}

	/* 如果page的modify没有创建，进行对象创建 */
	WT_RET(__wt_page_modify_init(session, page));

	/* 对前面已经存在的记录进行修改操作 */
	if (cbt->compare == 0 && cbt->ins != NULL){
		WT_ASSERT(session, upd == NULL);
		/* Make sure the update can proceed. */
		WT_ERR(__wt_txn_update_check(session, old_upd = cbt->ins->upd));

		/*新建一个WT_UPDATE结构对象*/
		WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
		WT_ERR(__wt_txn_modify(session, upd));
		logged = 1;

		cbt->modify_update = upd;

		upd->next = old_upd;

		/* Serialize the update. */
		WT_ERR(__wt_update_serial(session, page, &cbt->ins->upd, &upd, upd_size));
	}
	else{
		/* 追加更新 */
		if (append) {
			WT_PAGE_ALLOC_AND_SWAP(session, page, page->modify->mod_append, ins_headp, 1);
			ins_headp = &page->modify->mod_append[0];
		}
		else if (page->type == WT_PAGE_COL_FIX) {
			WT_PAGE_ALLOC_AND_SWAP(session,page, page->modify->mod_update, ins_headp, 1);
			ins_headp = &page->modify->mod_update[0];
		}
		else {
			WT_PAGE_ALLOC_AND_SWAP(session, page, page->modify->mod_update, ins_headp, page->pg_var_entries);
			ins_headp = &page->modify->mod_update[cbt->slot];
		}

		/* Allocate the WT_INSERT_HEAD structure as necessary. */
		WT_PAGE_ALLOC_AND_SWAP(session, page, *ins_headp, ins_head, 1);
		ins_head = *ins_headp;

		/* Choose a skiplist depth for this insert. entry是个跳表结构*/
		skipdepth = __wt_skip_choose_depth(session);

		/*新建一个WT_INSERT结构对象*/
		WT_ERR(__col_insert_alloc(session, recno, skipdepth, &ins, &ins_size));
		cbt->ins_head = ins_head;
		cbt->ins = ins;

		if (upd == NULL) {
			WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
			WT_ERR(__wt_txn_modify(session, upd));
			logged = 1;

			/* Avoid a data copy in WT_CURSOR.update. */
			cbt->modify_update = upd;
		}
		else
			upd_size = __wt_update_list_memsize(upd);

		ins->upd = upd;
		ins_size += upd_size;

		/*
		* If there was no insert list during the search, or there was
		* no search because the record number has not been allocated
		* yet, the cursor's information cannot be correct, search
		* couldn't have initialized it.
		*
		* Otherwise, point the new WT_INSERT item's skiplist to the
		* next elements in the insert list (which we will check are
		* still valid inside the serialization function).
		*
		* The serial mutex acts as our memory barrier to flush these
		* writes before inserting them into the list.
		*/
		/*构建跳表结构*/
		if (WT_SKIP_FIRST(ins_head) == NULL || recno == 0)
			for (i = 0; i < skipdepth; i++) {
				cbt->ins_stack[i] = &ins_head->head[i];
				ins->next[i] = cbt->next_stack[i] = NULL;
			}
		else
			for (i = 0; i < skipdepth; i++)
				ins->next[i] = cbt->next_stack[i];

		/* Append or insert the WT_INSERT structure. */
		if (append)
			WT_ERR(__wt_col_append_serial(session, page, cbt->ins_head, cbt->ins_stack, &ins, ins_size, &cbt->recno, skipdepth));
		else
			WT_ERR(__wt_insert_serial(session, page, cbt->ins_head, cbt->ins_stack, &ins, ins_size, skipdepth));
	}

	/* If the update was successful, add it to the in-memory log. */
	if (logged)
		WT_ERR(__wt_txn_log_op(session, cbt));

	if (0) {
err:		
		/*
		 * Remove the update from the current transaction, so we don't
		 * try to modify it on rollback.
		 */
		if (logged)
			__wt_txn_unmodify(session);
		__wt_free(session, ins);
		__wt_free(session, upd);
	}

	return ret;
}

/*分配一个WT_INSERT结构对象，并设置为column方式存储*/
static int __col_insert_alloc(WT_SESSION_IMPL *session,
	uint64_t recno, u_int skipdepth, WT_INSERT **insp, size_t *ins_sizep)
{
	WT_INSERT* ins;
	size_t ins_size;

	ins_size = sizeof(WT_INSERT) + skipdepth * sizeof(WT_INSERT*);
	WT_RET(__wt_calloc(session, 1, ins_size, &ins));

	WT_INSERT_RECNO(ins) = recno;

	*insp = ins;
	*ins_sizep = ins_size;

	return 0;
}






