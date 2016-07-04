/***********************************************************************
* btree page discard实现
*
*
***********************************************************************/
#include "wt_internal.h"


static void __free_page_modify(WT_SESSION_IMPL *, WT_PAGE *);
static void __free_page_col_var(WT_SESSION_IMPL *, WT_PAGE *);
static void __free_page_int(WT_SESSION_IMPL *, WT_PAGE *);
static void __free_page_row_leaf(WT_SESSION_IMPL *, WT_PAGE *);
static void __free_skip_array(WT_SESSION_IMPL *, WT_INSERT_HEAD **, uint32_t);
static void __free_skip_list(WT_SESSION_IMPL *, WT_INSERT *);
static void __free_update(WT_SESSION_IMPL *, WT_UPDATE **, uint32_t);
static void __free_update_list(WT_SESSION_IMPL *, WT_UPDATE *);

/*废弃一个内存中的btree page对象，释放与之有管理的内存*/
void __wt_ref_out(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_ASSERT(session, S2BT(session)->evict_ref != ref);
	__wt_page_out(session, &ref->page);
}

/*废弃btree page内存中的对象*/
void __wt_page_out(WT_SESSION_IMPL *session, WT_PAGE **pagep)
{
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	WT_PAGE_MODIFY *mod;

	/*先将pagep的值置为NULL，防止其他地方在释放的时候使用*/
	page = *pagep;
	*pagep = NULL;

	/*合法性判断，被释放的page不能是脏页，不能是正在split的页*/
	WT_ASSERT(session, !__wt_page_is_modified(page));
	WT_ASSERT(session, !F_ISSET_ATOMIC(page, WT_PAGE_EVICT_LRU));
	WT_ASSERT(session, !F_ISSET_ATOMIC(page, WT_PAGE_SPLITTING));

	switch(page->type){
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		mod = page->modify;
		if(mod != NULL && mod->mod_root_split != NULL) /*如果root page是split过的，可能有更多的page与要废弃的page相连，需要全部找出并废弃它*/
			__wt_page_out(session, &mod->mod_root_split);
		break;
	}

	/*将page驱逐出内存cache*/
	__wt_cache_page_evict(session, page);

	/*假如discarded page是不释放的，那么直接返回，这样会造成内存泄露*/
	if(F_ISSET(S2C(session), WT_CONN_LEAK_MEMORY))
		return ;

	switch(page->type){
	case WT_PAGE_COL_FIX:
		break;

	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		__free_page_int(session, page);
		break;

	case WT_PAGE_COL_VAR:
		__free_page_col_var(session, page);
		break;

	case WT_PAGE_ROW_LEAF:
		__free_page_row_leaf(session, page);
		break;
	}

	dsk = (WT_PAGE_HEADER *)page->dsk;
	/*已经分配了dsk空间对象，需要进行释放*/
	if(F_ISSET_ATOMIC(page, WT_PAGE_DISK_ALLOC))
		__wt_overwrite_and_free_len(session, dsk, dsk->mem_size);
	
	/*page已经在磁盘上隐射了存储空间，进行空间释放*/
	if(F_ISSET_ATOMIC(page, WT_PAGE_DISK_MAPPED))
		__wt_mmap_discard(session, dsk, dsk->mem_size);

	__wt_overwrite_and_free(session, page);
}

/*释放废弃page的相关的修改的数据对象*/
static void __free_page_modify(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_INSERT_HEAD *append;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	/*记录有修改,需要释放对应的修改对象*/
	switch(F_ISSET(mod, WT_PM_REC_MASK)){
	case WT_PM_REC_MULTIBLOCK:
		/*释放modify的entry对象*/
		for(multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i){
			switch(page->type){
				/*如果是行存储格式，需要先释放key值*/
			case WT_PAGE_ROW_INT:
			case WT_PAGE_ROW_LEAF:
				__wt_free(session, multi->key.ikey);
				break;
			}

			__wt_free(session, multi->skip);
			__wt_free(session, multi->skip_dsk);
			__wt_free(session, multi->addr.addr);
		}
		__wt_free(session, mod->mod_multi);
		break;

		/*记录对象替换模型,直接释放即可*/
	case WT_PM_REC_REPLACE:
		__wt_free(session, mod->mod_replace.addr);
		break;
	}

	switch(page->type){
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_VAR:
		/*释放append数组*/
		if((append == WT_COL_APPEND(page)) != NULL){
			__free_skip_list(session, WT_SKIP_FIRST(append));
			__wt_free(session, append);
			__wt_free(session, mod->mod_append);
		}

		/*释放掉insert/update对象数组*/
		if(mod->mod_update != NULL){
			__free_skip_array(session, mod->mod_update, page->type == WT_PAGE_COL_FIX ? 1 : page->pg_var_entries);
		}

		break;
	}

	/*释放溢出page的结构对象*/
	__wt_ovfl_reuse_free(session, page);
	__wt_ovfl_txnc_free(session, page);
	__wt_ovfl_discard_free(session, page);
	__wt_free(session, page->modify->ovfl_track);

	__wt_free(session, page->modify);
}

/*释放一个内部索引page, 类型为：WT_PAGE_COL_INT或者WT_PAGE_ROW_INT*/
static void __free_page_int(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	__wt_free_ref_index(session, page, WT_INTL_INDEX_GET_SAFE(page), 0);
}

/*废弃一个page对应的ref对象*/
void __wt_free_ref(WT_SESSION_IMPL* session, WT_PAGE* page, WT_REF* ref, int free_pages)
{
	WT_IKEY *ikey;

	if (ref == NULL)
		return;

	/*确定需要释放掉对应的page,那么先改变对应的cache的胀数据长度，然后将page对象释放*/
	if(free_pages && ref->page != NULL){
		if(ref->page->modify != NULL){
			ref->page->modify->write_gen = 0;
			__wt_cache_dirty_decr(session, ref->page);
		}

		__wt_page_out(session, &ref->page);
	}

	/*释放key*/
	switch(page->type){
		/*行存储，释放ikey对象*/
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		if ((ikey = __wt_ref_key_instantiated(ref)) != NULL)
			__wt_free(session, ikey);
		break;
	}

	/*如果ref->addr不在page空间上，是单独开辟的内存空间，需要单独释放*/
	if(ref->addr != NULL && __wt_off_page(page, ref->addr)){
		__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
		__wt_free(session, ref->addr);
	}

	if(ref->page_del != NULL){
		__wt_free(session, ref->page_del->update_list);
		__wt_free(session, ref->page_del);
	}

	__wt_overwrite_and_free(session, ref);
}

/*释放index entry对象*/
void __wt_free_ref_index(WT_SESSION_IMPL* session, WT_PAGE* page, WT_PAGE_INDEX* pindex, int free_pages)
{
	uint32_t i;

	if (pindex == NULL)
		return;

	for (i = 0; i < pindex->entries; ++i)
		__wt_free_ref(session, page, pindex->index[i], free_pages);
	__wt_free(session, pindex);
}

/*废弃一个WT_PAGE_COL_VAR page对象*/
static void __free_page_col_var(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	__wt_free(session, page->pg_var_repeats);
}

/*废弃一个行存储的leaf page*/
static void __free_page_row_leaf(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_IKEY *ikey;
	WT_ROW *rip;
	uint32_t i;
	void *copy;

	WT_ROW_FOREACH(page, rip, i){
		copy = WT_ROW_KEY_COPY(rip);
		(void)__wt_row_leaf_key_info(page, copy, &ikey, NULL, NULL, NULL);
		if (ikey != NULL)
			__wt_free(session, ikey);
	}

	if (page->pg_row_ins != NULL)
		__free_skip_array(session, page->pg_row_ins, page->pg_row_entries + 1);

	if (page->pg_row_upd != NULL)
		__free_update(session, page->pg_row_upd, page->pg_row_entries);

}

/*废弃header的skip array*/
static void __free_skip_array(WT_SESSION_IMPL* session, WT_INSERT_HEAD** head_arg, uint32_t entries)
{
	WT_INSERT_HEAD **head;

	for (head = head_arg; entries > 0; --entries, ++head){
		if (*head != NULL) {
			__free_skip_list(session, WT_SKIP_FIRST(*head));
			__wt_free(session, *head);
		}
	}

	__wt_free(session, head_arg);
}

/*废弃insert list对象*/
static void __free_skip_list(WT_SESSION_IMPL* session, WT_INSERT* ins)
{
	WT_INSERT *next;
	for (; ins != NULL; ins = next) {
		__free_update_list(session, ins->upd);
		next = WT_SKIP_NEXT(ins);
		__wt_free(session, ins);
	}
}

/*废弃一个update list*/
static void __free_update(WT_SESSION_IMPL *session, WT_UPDATE **update_head, uint32_t entries)
{
	WT_UPDATE **updp;
	for (updp = update_head; entries > 0; --entries, ++updp)
		if (*updp != NULL)
			__free_update_list(session, *updp);

	__wt_free(session, update_head);
}

static void __free_update_list(WT_SESSION_IMPL* session, WT_UPDATE* upd)
{
	WT_UPDATE *next;

	for (; upd != NULL; upd = next) {
		/* Everything we free should be visible to everyone. */
		WT_ASSERT(session, F_ISSET(session, WT_SESSION_DISCARD_FORCE) ||
			upd->txnid == WT_TXN_ABORTED || __wt_txn_visible_all(session, upd->txnid));

		next = upd->next;
		__wt_free(session, upd);
	}
}

