/***********************************************************************
* 这个文件主要是实现btree page的标记删除功能（fast-deleted），主要是在删除
* 的时候把page->state标记为WT_REF_DELETED,并设置page walk是skip这个page
*
***********************************************************************/
#include "wt_internal.h"

/*尝试删除一个page*/
int __wt_delete_page(WT_SESSION_IMPL* session, WT_REF* ref, int* skipp)
{
	WT_DECL_RET;
	WT_PAGE *parent;

	*skipp = 0;

	/*假如这个page已经被实例化到内存中，我们尝试从cache中驱逐它，先需要将ref设置为locked状态，防止其他事务进行操作*/
	if(ref->state == WT_REF_MEM && WT_ATOMIC_CAS4(ref->state, WT_REF_MEM, WT_REF_LOCKED)){
		if (__wt_page_is_modified(ref->page)) {
			WT_PUBLISH(ref->state, WT_REF_MEM);
			return (0);
		}

		/*进行page驱逐*/
		WT_ATOMIC_ADD4(S2BT(session)->evict_busy, 1);
		ret = __wt_evict_page(session, ref);
		WT_ATOMIC_SUB4(S2BT(session)->evict_busy, 1);

		WT_RET_BUSY_OK(ret);
	}

	/*被驱逐的page对应ref->state一定是WT_REF_DISK,如果不是，说明其实的事务还在使用它，不能进行删除标记*/
	if(ref->state != WT_REF_DISK || !WT_ATOMIC_CAS4(ref->state, WT_REF_DISK, WT_REF_LOCKED))
		return 0;

	/*如果page中有overflow的KV对，不能进行fast delete它，只能通过page discard来删除,这里涉及的东西比较复杂，暂时没太弄明白*/
	parent = ref->home;
	if(__wt_off_page(parent, ref->addr) || __wt_cell_type_raw(ref->addr) != WT_CELL_ADDR_LEAF_NO)
		goto err;

	/*因为子节点要标记为删除，所以它的父节点需要标记为脏页*/
	WT_ERR(__wt_page_parent_modify_set(session, ref, 0));

	/*在ref上产生一个page_del结构来表示ref对应的page删除了*/
	WT_ERR(__wt_calloc_one(session, &ref->page_del));
	ref->page_del->txnid = session->txn.id;

	WT_ERR(__wt_txn_modify_ref(session, ref));

	*skipp = 1;
	/*在page ref上标记为删除状态*/
	WT_PUBLISH(ref->state, WT_REF_DELETED);
	return 0;

err:
	__wt_free(session, ref->page_del);
	/*在ref上标记为WT_REF_DISK，因为这个时候page被驱逐出cache中*/
	WT_PUBLISH(ref->state, WT_REF_DISK);
	return ret;
}

/*事务rollback时，将实例化的page删除*/
void __wt_delete_page_rollback(WT_SESSION_IMPL* session, WT_REF* ref)
{
	WT_UPDATE **upd;

	for(;; __wt_yield()){
		switch(ref->state){
		case WT_REF_DISK:
		case WT_REF_READING:
			WT_ASSERT(session, 0);
			break;

		case WT_REF_DELETED:
			/*page已经删除，我们需要回收它，以便重复使用？？*/
			if(WT_ATOMIC_ADD4(ref->state, WT_REF_DELETED, WT_REF_DISK))
				return ;
			break;

		case WT_REF_LOCKED: /*page正在实例化可能是这个状态，因为构建page的block可能是异步的*/
			break;

		case WT_REF_MEM:
		case WT_REF_SPLIT:
			/*
			 * We can't use the normal read path to get a copy of
			 * the page because the session may have closed the
			 * cursor, we no longer have the reference to the tree
			 * required for a hazard pointer.  We're safe because
			 * with unresolved transactions, the page isn't going
			 * anywhere.
			 *
			 * The page is in an in-memory state, walk the list of
			 * update structures and abort them.
			 * 大致的意思是说将内存中的page设置一个放弃的TXN ID,来标记
			 * 这个page已经处于放弃状态，防止其他地方的引用。
			 */
			for (upd =ref->page_del->update_list; *upd != NULL; ++upd)
				(*upd)->txnid = WT_TXN_ABORTED;

			/*释放掉存储upd的list和page_del*/
			__wt_free(session, ref->page_del->update_list);
			__wt_free(session, ref->page_del);

			return ;
		}
	}
}

/*
 * __wt_delete_page_skip --
 *	If iterating a cursor, skip deleted pages that are visible to us.
 * 在btree cursor操作btree时，需要判断page是否是删除，如果删除了，
 * 需要skip 它
 */
int __wt_delete_page_skip(WT_SESSION_IMPL *session, WT_REF *ref)
{
	int skip; 

	/*page已经删除，可以skip*/
	if(ref->page_del == NULL)
		return 1;

	/*不处于deleted状态,不能skip它*/
	if (!WT_ATOMIC_CAS4(ref->state, WT_REF_DELETED, WT_REF_LOCKED))
		return 0;

	/*page del是null,表示已经没有其他地方引用他，如果操作事务是对这个page可见，那么应该skip*/
	skip = (ref->page_del == NULL ||
		__wt_txn_visible(session, ref->page_del->txnid));

	WT_PUBLISH(ref->state, WT_REF_DELETED);

	return skip;
}

/*实例化一个deleted row leaf page*/
int __wt_delete_page_instantiate(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_DELETED *page_del;
	WT_UPDATE **upd_array, *upd;
	size_t size;
	uint32_t i;

	btree = S2BT(session);
	page = ref->page;
	page_del = ref->page_del;

	/*如果btree标记为脏而且我们又要对page进行写，那么标记这个page为脏页*/
	if(btree->modified){
		WT_RET(__wt_page_modify_init(session, page));
		__wt_page_modify_set(session, page);
	}

	/*page已经标记为deleted,那么需要为page标记一个版本用来内存实例化*/
	if(page_del != NULL){
		WT_RET(__wt_calloc_def(session, page->pg_row_entries + 1, &page_del->update_list));
	}

	WT_ERR(__wt_calloc_def(session, page->pg_row_entries, &upd_array));
	page->pg_row_upd = upd_array;

	/*设置upd的txnid，并将这些行实例全部放入update_list中*/
	for (i = 0, size = 0; i < page->pg_row_entries; ++i) {
		WT_ERR(__wt_calloc_one(session, &upd));
		WT_UPDATE_DELETED_SET(upd);

		if (page_del == NULL) /*如果page已物理上删除，那么标记所有的记录对所有的事务可见*/
			upd->txnid = WT_TXN_NONE;	/* Globally visible */
		else {
			upd->txnid = page_del->txnid;
			page_del->update_list[i] = upd;
		}

		upd->next = upd_array[i];
		upd_array[i] = upd;

		size += sizeof(WT_UPDATE *) + WT_UPDATE_MEMSIZE(upd);
	}

	__wt_cache_page_inmem_incr(session, page, size);
}





