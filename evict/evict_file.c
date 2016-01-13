
#include "wt_internal.h"

/*对session对应的btree树上所有的page进行evict操作*/
int __wt_evict_file(WT_SESSION_IMPL* session, int syncop)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_REF *next_ref, *ref;
	int evict_reset;

	/*独占式对btree上的page进行evict操作,因为对整个btree的page进行evict,必须是线程独占式的*/
	WT_RET(__wt_evict_file_exclusive_on(session, &evict_reset));

	/* Make sure the oldest transaction ID is up-to-date. */
	__wt_txn_update_oldest(session);

	next_ref = NULL;
	WT_ERR(__wt_tree_walk(session, &next_ref, NULL, WT_READ_CACHE | WT_READ_NO_EVICT));
	
	while ((ref == next_ref) != NULL){
		page = ref->page;

		/*
		* Eviction can fail when a page in the evicted page's subtree
		* switches state.  For example, if we don't evict a page marked
		* empty, because we expect it to be merged into its parent, it
		* might no longer be empty after it's reconciled, in which case
		* eviction of its parent would fail.  We can either walk the
		* tree multiple times (until it's finally empty), or reconcile
		* each page to get it to its final state before considering if
		* it's an eviction target or will be merged into its parent.
		*
		* Don't limit this test to any particular page type, that tends
		* to introduce bugs when the reconciliation of other page types
		* changes, and there's no advantage to doing so.
		*
		* Eviction can also fail because an update cannot be written.
		* If sessions have disjoint sets of files open, updates in a
		* no-longer-referenced file may not yet be globally visible,
		* and the write will fail with EBUSY.  Our caller handles that
		* error, retrying later.
		*/
		if (syncop == WT_SYNC_CLOSE && __wt_page_is_modified(page)) /*有脏数据，进行reconcile到磁盘*/
			WT_ERR(__wt_reconcile(session, ref, NULL, WT_EVICTING));

		WT_ERR(__wt_tree_walk(session, &next_ref, NULL, WT_READ_CACHE | WT_READ_NO_EVICT));
		switch (syncop){
		case WT_SYNC_CLOSE: /*同步关闭，直接进行evict*/
			WT_ERR(__wt_evict(session, ref, 1));
			break;

		case WT_SYNC_DISCARD:
			WT_ASSERT(session, __wt_page_can_evict(session, page, 0));
			__wt_evict_page_clean_update(session, ref);
			break;

		case WT_SYNC_DISCARD_FORCE:
			/*
			* Forced discard of the page, whether clean or dirty.
			* If we see a dirty page in a forced discard, clean
			* the page, both to keep statistics correct, and to
			* let the page-discard function assert no dirty page
			* is ever discarded.
			*/
			if (__wt_page_is_modified(page)) {
				page->modify->write_gen = 0;
				__wt_cache_dirty_decr(session, page);
			}

			F_SET(session, WT_SESSION_DISCARD_FORCE);
			__wt_evict_page_clean_update(session, ref);
			F_CLR(session, WT_SESSION_DISCARD_FORCE);
			break;

		WT_ILLEGAL_VALUE_ERR(session);
		}
	}

	if (0){
err:
		/* On error, clear any left-over tree walk. */
		if (next_ref != NULL)
			WT_TRET(__wt_page_release(session, next_ref, WT_READ_NO_EVICT));
	}

	/*释放独占evict方式*/
	if (evict_reset)
		__wt_evict_file_exclusive_off(session);

	return ret;
}



