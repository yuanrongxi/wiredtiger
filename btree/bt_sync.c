/***********************************************************
 * flush page实现，将page的数据落盘固化,并建立CHECKPOINT
 **********************************************************/

#include "wt_internal.h"

static int __sync_file(WT_SESSION_IMPL* session, int syncop)
{
	struct timespec end, start;
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	WT_REF *walk;
	WT_TXN *txn;
	uint64_t internal_bytes, leaf_bytes;
	uint64_t internal_pages, leaf_pages;
	uint32_t flags;
	int evict_reset;

	btree = S2BT(session);

	flags = WT_READ_CACHE | WT_READ_NO_GEN;
	walk = NULL;
	txn = &session->txn;

	internal_bytes = leaf_bytes = 0;
	internal_pages = leaf_pages = 0;

	if (WT_VERBOSE_ISSET(session, WT_VERB_CHECKPOINT))
		WT_RET(__wt_epoch(session, &start));

	switch (syncop){
	case WT_SYNC_WRITE_LEAVES:
		/* 将所有可以写入磁盘的脏页落盘,只刷入leaf page的数据入盘 */
		if (!btree->modified)
			return 0;
		__wt_spin_lock(session, &btree->flush_lock);
		if (!btree->modified){
			__wt_spin_unlock(session, &btree->flush_lock);
			return 0;
		}

		flags |= WT_READ_NO_WAIT | WT_READ_SKIP_INTL;
		for (walk = NULL;;){
			/*遍历所有无操作的page*/
			WT_ERR(__wt_tree_walk(session, &walk, NULL, flags));
			if (walk == NULL)
				break;

			/* 将无操作的脏写入磁盘，如果有的更新在刷盘事务之后产生，那么这个更新的page不做刷盘操作*/
			if (__wt_page_is_modified(page) && __wt_txn_visible_all(session, page->modify->update_txn)){
				if (txn->isolation == TXN_ISO_READ_COMMITTED)
					__wt_txn_refresh(session, 1);

				leaf_bytes += page->memory_footprint;
				++leaf_pages;
				WT_ERR(__wt_reconcile(session, walk, NULL, 0));
			}
		}
		break;

	case WT_SYNC_CHECKPOINT:
		__wt_spin_lock(session, &btree->flush_lock);

		/*这里使用内存屏障是为了checkpointing立即生效*/
		btree->checkpointing = 1;
		WT_FULL_BARRIER();
		/*暂停evict server的工作，让其跳过或者随眠*/
		WT_ERR(__wt_evict_file_exclusive_on(session, &evict_reset));
		if (evict_reset)
			__wt_evict_file_exclusive_off(session);

		/*对脏页进行落盘*/
		flags |= WT_READ_NO_EVICT;
		for (walk = NULL;;){
			/*
			* If we have a page, and it was ever modified, track
			* the highest transaction ID in the tree.  We do this
			* here because we want the value after reconciling
			* dirty pages.
			* 设置btree上最大生效的最大事务ID
			*/
			if (walk != NULL && walk->page != NULL &&
				(mod = walk->page->modify) != NULL && TXNID_LT(btree->rec_max_txn, mod->rec_max_txn))
				btree->rec_max_txn = mod->rec_max_txn;

			WT_ERR(__wt_tree_walk(session, &walk, NULL, flags));
			if (walk == NULL)
				break;

			page = walk->page;
			mod = page->modify;

			/*脏页判断，如果不是脏页，跳过*/
			if (!__wt_page_is_modified(page))
				continue;

			/*
			* Write dirty pages, unless we can be sure they only
			* became dirty after the checkpoint started.
			*
			* We can skip dirty pages if:
			* (1) they are leaf pages;
			* (2) there is a snapshot transaction active (which
			*     is the case in ordinary application checkpoints
			*     but not all internal cases); and
			* (3) the first dirty update on the page is
			*     sufficiently recent that the checkpoint
			*     transaction would skip them.
			*
			* Mark the tree dirty: the checkpoint marked it clean
			* and we can't skip future checkpoints until this page
			* is written.
			* 跳过后于checkpoint事务提交的事务修改,因为是SNAPSHOT隔离
			*/

			if (!WT_PAGE_IS_INTERNAL(page) && F_ISSET(txn, TXN_HAS_SNAPSHOT) && TXNID_LT(txn->snap_max, mod->first_dirty_txn)) {
				__wt_page_modify_set(session, page);
				continue;
			}

			if (WT_PAGE_IS_INTERNAL(page)){
				internal_bytes += page->memory_footprint;
				++internal_pages;
			}
			else{
				leaf_bytes += page->memory_footprint;
				++leaf_pages;
			}
			WT_ERR(__wt_reconcile(session, walk, NULL, 0));
		}
		break;
	}
	
	if (WT_VERBOSE_ISSET(session, WT_VERB_CHECKPOINT)) {
		WT_ERR(__wt_epoch(session, &end));
		WT_ERR(__wt_verbose(session, WT_VERB_CHECKPOINT,
			"__sync_file WT_SYNC_%s wrote:\n\t %" PRIu64
			" bytes, %" PRIu64 " pages of leaves\n\t %" PRIu64
			" bytes, %" PRIu64 " pages of internal\n\t"
			"Took: %" PRIu64 "ms",
			syncop == WT_SYNC_WRITE_LEAVES ?
			"WRITE_LEAVES" : "CHECKPOINT",
			leaf_bytes, leaf_pages, internal_bytes, internal_pages,
			WT_TIMEDIFF(end, start) / WT_MILLION));
	}

err:
	/*释放最后walk page, 防止落盘的page在内存中*/
	if (walk != NULL)
		WT_TRET(__wt_page_release(session, walk, flags));

	if (txn->isolation == TXN_ISO_READ_COMMITTED && session->ncursors == 0)
		__wt_txn_release_snapshot(session);

	if (btree->checkpointing){
		btree->checkpointing = 0;
		WT_FULL_BARRIER();

		btree->evict_walk_period = 0;
		/*
		* Wake the eviction server, in case application threads have
		* stalled while the eviction server decided it couldn't make
		* progress.Without this, application threads will be stalled
		* until the eviction server next wakes.
		*/
		WT_TRET(__wt_evict_server_wake(session));
	}

	__wt_spin_unlock(session, &btree->flush_lock);

	/*异步刷盘，因为sync_write_leaves选项是checkpoint之前的脏数据落盘，这里采用了异步刷盘不阻塞checkpoint线程*/
	if (ret == 0 && syncop == WT_SYNC_WRITE_LEAVES)
		WT_RET(btree->bm->sync(btree->bm, session, 1));

	return ret;
}

/* cache 磁盘相关的操作实现 */
int __wt_cache_op(WT_SESSION_IMPL* session, WT_CKPT* ckptbase, int op)
{
	WT_DECL_RET;
	WT_BTREE *btree;

	btree = S2BT(session);

	switch (op){
	case WT_SYNC_CHECKPOINT:
	case WT_SYNC_CLOSE:
		/*
		* Set the checkpoint reference for reconciliation; it's ugly,
		* but drilling a function parameter path from our callers to
		* the reconciliation of the tree's root page is going to be
		* worse.
		*/
		WT_ASSERT(session, btree->ckpt == NULL);
		btree->ckpt = ckptbase;
		break;
	}

	switch (op) {
	case WT_SYNC_CHECKPOINT:
	case WT_SYNC_WRITE_LEAVES:
		WT_ERR(__sync_file(session, op));
		break;
	case WT_SYNC_CLOSE:
	case WT_SYNC_DISCARD:
	case WT_SYNC_DISCARD_FORCE:
		WT_ERR(__wt_evict_file(session, op));
		break;
		WT_ILLEGAL_VALUE_ERR(session);
	}

err:
	switch (op) {
	case WT_SYNC_CHECKPOINT:
	case WT_SYNC_CLOSE:
		btree->ckpt = NULL;
		break;
	}
}


