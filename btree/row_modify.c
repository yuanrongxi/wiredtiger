/**********************************************************************
 *实现row store方式的btree的新增、修改和删除方式
 **********************************************************************/

#include "wt_internal.h"

/* 在内存中分配一个page的修改对象 */
int __wt_page_modify_alloc(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_CONNECTION_IMPL *conn;
	WT_PAGE_MODIFY *modify;

	conn = S2C(session);

	WT_RET(__wt_calloc_one(session, &modify));

	/* 为modify对象选择一个spin lock,用于多线程竞争并发控制 */
	modify->page_lock = ++conn->page_lock_cnt % WT_PAGE_LOCKS(conn);

	/* 有可能多个线程在进行分配，只有一个线程会创建成功,其他线程创建的对象先要释放掉 */
	if(WT_ATOMIC_CAS8(page->modify, NULL, modify))
		__wt_cache_page_inmem_incr(session, page, sizeof(*modify));
	else
		__wt_free(session, modify);

	return 0;
}

/*row store btree的修改实现，包括:insert, update和delete */
int __wt_row_modify(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, WT_ITEM* key, WT_ITEM* value, WT_UPDATE* upd, int is_remove)
{
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_INSERT_HEAD *ins_head, **ins_headp;
	WT_PAGE *page;
	WT_UPDATE *old_upd, **upd_entry;
	size_t ins_size, upd_size;
	uint32_t ins_slot;
	u_int i, skipdepth;
	int logged;

	ins = NULL;
	page = cbt->ref->page;
	logged = 0;

	if(is_remove)
		value = NULL;

	/*分配并初始化一个page modify对象*/
	WT_RET(__wt_page_modify_init(session, page));

	/* 修改操作， */
	if(cbt->compare == 0){
		if (cbt->ins == NULL){ /*是在update list当中找到修改的记录，直接获取udpate list当中对应的update对象*/
			/* 为cbt游标分配一个update对象数组,并设置update数组槽位，原子操作性的分配*/
			WT_PAGE_ALLOC_AND_SWAP(session, page, page->pg_row_upd, upd_entry, page->pg_row_entries);
			upd_entry = &page->pg_row_upd[cbt->slot];
		}
		else /* 获取一个upd_entry */
			upd_entry = &cbt->ins->upd;

		if(upd == NULL){
			/*确定是否可以进行更新操作*/
			WT_ERR(__wt_txn_update_check(session, old_upd = *upd_entry));

			WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
			WT_ERR(__wt_txn_modify(session, upd));
			logged = 1;

			/* Avoid WT_CURSOR.update data copy. */
			cbt->modify_update = upd;
		}
		else{
			upd_size = __wt_update_list_memsize(upd);

			WT_ASSERT(session, *upd_entry == NULL);
			old_upd = *upd_entry = upd->next;
		}
		/*
		 * Point the new WT_UPDATE item to the next element in the list.
		 * If we get it right, the serialization function lock acts as
		 * our memory barrier to flush this write.
		 */
		upd->next = old_upd;

		/*进行串行更新操作*/
		WT_ERR(__wt_update_serial(session, page, upd_entry, &upd, upd_size));
	}
	else{ /*没有定位到具体的记录，那么相当于插入一个新的记录行*/
		WT_PAGE_ALLOC_AND_SWAP(session, page, page->pg_row_ins, ins_headp, page->pg_row_entries + 1);
		ins_slot = F_ISSET(cbt, WT_CBT_SEARCH_SMALLEST) ? page->pg_row_entries : cbt->slot;
		ins_headp = &page->pg_row_ins[ins_slot];

		/*分配一个insert head数组*/
		WT_PAGE_ALLOC_AND_SWAP(session, page, *ins_headp, ins_head, 1);
		ins_head = *ins_headp;

		skipdepth = __wt_skip_choose_depth(session);

		/*
		 * Allocate a WT_INSERT/WT_UPDATE pair and transaction ID, and
		 * update the cursor to reference it (the WT_INSERT_HEAD might
		 * be allocated, the WT_INSERT was allocated).
		 */
		WT_ERR(__wt_row_insert_alloc(session, key, skipdepth, &ins, &ins_size));
		cbt->ins_head = ins_head;
		cbt->ins = ins;

		/*通过value构建upd对象*/
		if(upd == NULL){
			WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
			WT_ERR(__wt_txn_modify(session, upd));
			logged = 1;

			/* Avoid WT_CURSOR.update data copy. */
			cbt->modify_update = upd;
		}
		else 
			upd_size = __wt_update_list_memsize(upd);

		ins->upd = upd;
		ins_size += upd_size;

		if (WT_SKIP_FIRST(ins_head) == NULL)
			for (i = 0; i < skipdepth; i++) {
				cbt->ins_stack[i] = &ins_head->head[i];
				ins->next[i] = cbt->next_stack[i] = NULL;
			}
		else
			for (i = 0; i < skipdepth; i++)
				ins->next[i] = cbt->next_stack[i];

		/* Insert the WT_INSERT structure. */
		WT_ERR(__wt_insert_serial(session, page, cbt->ins_head, cbt->ins_stack, &ins, ins_size, skipdepth));
	}
	
	if(logged)
		WT_ERR(__wt_txn_log_op(session, cbt));

	if(0){
err:
		if (logged)
			__wt_txn_unmodify(session);

		__wt_free(session, ins);
		cbt->ins = NULL;
		__wt_free(session, upd);
	}

	return ret;
}

/* 分配一个row insert的WT_INSERT对象 */
int __wt_row_insert_alloc(WT_SESSION_IMPL* session, WT_ITEM* key, u_int skipdepth, WT_INSERT** insp, size_t* ins_sizep)
{
	WT_INSERT *ins;
	size_t ins_size;

	ins_size = sizeof(WT_INSERT) + skipdepth * sizeof(WT_INSERT *) + key->size;
	WT_RET(__wt_calloc(session, 1, ins_size, &ins));

	/*确定key的起始偏移位置*/
	ins->u.key.offset = WT_STORE_SIZE(ins_size - key->size);
	/*设置key的长度*/
	WT_INSERT_KEY_SIZE(ins) = WT_STORE_SIZE(key->size);
	/*key值的拷贝*/
	memcpy(WT_INSERT_KEY(ins), key->data, key->size);

	insp = ins;
	if(ins_sizep != NULL)
		*ins_sizep = ins_size;

	return 0;
}

/*分配一个row update的WT_UPDATE对象, value = NULL表示delete操作*/
int __wt_update_alloc(WT_SESSION_IMPL* session, WT_ITEM* value, WT_UPDATE** updp, size_t* sizep)
{
	WT_UPDATE *upd;
	size_t size;

	size = (value == NULL ? 0 : value->size);
	WT_RET(__wt_calloc(session, 1, sizeof(WT_UPDATE) + size, &upd));
	if (value == NULL)
		WT_UPDATE_DELETED_SET(upd);
	else {
		upd->size = WT_STORE_SIZE(size);
		memcpy(WT_UPDATE_DATA(upd), value->data, size);
	}

	*updp = upd;
	*sizep = WT_UPDATE_MEMSIZE(upd);
	return 0;
}

/* 检查过期废弃的update，只有下最近一个session能看到的update版本，前面处于rollback的版本作为废弃数据 */
WT_UPDATE* __wt_update_obsolete_check(WT_SESSION_IMPL *session, WT_UPDATE *upd)
{
	WT_UPDATE *first, *next;


	for(first = NULL; upd != NULL; upd = upd->next){
		if(__wt_txn_visible_all(session, upd->txnid)){
			if(first == NULL)
				first = upd;
		}
		else if(upd->txnid != WT_TXN_ABORTED)
			first = NULL;
	}

	/*截掉所有最后一个不为空的upd并且TXNID = WT_TXN_ABORTED，而且这个upd对所有事务不可见*/
	if(first != NULL &&& (next = first->next) != NULL && WT_ATOMIC_CAS8(first->next, next, NULL))
		return next;

	return NULL;
}

/*释放upd list中的对象,这些对象确认是被过期废弃的*/
void __wt_update_obsolete_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd)
{
	WT_UPDATE *next;
	size_t size;

	/* Free a WT_UPDATE list. */
	for (size = 0; upd != NULL; upd = next) {
		next = upd->next;
		size += WT_UPDATE_MEMSIZE(upd);
		__wt_free(session, upd);
	}

	if (size != 0)
		__wt_cache_page_inmem_decr(session, page, size);
}






