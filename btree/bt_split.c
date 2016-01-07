/************************************************************************
 * btree split实现
 ***********************************************************************/

#include "wt_internal.h"

/*内存计数交换*/
#define	WT_MEM_TRANSFER(from_decr, to_incr, len) do {			\
	size_t __len = (len);										\
	from_decr += __len;											\
	to_incr += __len;											\
} while (0)

/* 获得connection对应最早active状态的split generation */
static uint64_t __split_oldest_gen(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *s;
	uint64_t gen, oldest;
	u_int i, session_cnt;

	conn = S2C(session);
	/* 在这用内存屏障是防止读乱序 */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = 0, s = conn->sessions, oldest = conn->split_gen + 1; i < session_cnt; i++, s++){
		if (((gen = s->split_gen) != 0) && gen < oldest)
			oldest = gen;
	}

	return oldest;
}

/* 向session的split stash list中增加一个新的entry对象 */
static int __split_stash_add(WT_SESSION_IMPL* session, uint64_t split_gen, void* p, size_t len)
{
	WT_SPLIT_STASH *stash;

	WT_ASSERT(session, p != NULL);

	/*扩大split_stash_alloc*/
	WT_RET(__wt_realloc_def(session, &session->split_stash_alloc, session->split_stash_cnt + 1, &session->split_stash));

	stash = session->split_stash + session->split_stash_cnt++;
	stash->split_gen = split_gen;
	stash->p = p;
	stash->len = len;

	WT_STAT_FAST_CONN_ATOMIC_INCRV(session, rec_split_stashed_bytes, len);
	WT_STAT_FAST_CONN_ATOMIC_INCR(session, rec_split_stashed_objects);

	/*尝试释放split stash list中可以释放*/
	if(session->split_stash_cnt > 1)
		__wt_split_stash_discard(session);

	return 0;
}

/* 废弃释放掉不是active状态的split stash单元 */
void __wt_split_stash_discard(WT_SESSION_IMPL* session)
{
	WT_SPLIT_STASH *stash;
	uint64_t oldest;
	size_t i;

	/* 获得最老的split generation */
	oldest = __split_oldest_gen(session);

	for(i = 0, stash = session->split_stash; i < session->split_stash_cnt; ++i, ++stash){
		if (stash->p == NULL)
			continue;
		else if(stash->split_gen >= oldest) /*找到最早active状态的split stash*/
			break;

		/* 释放掉已经断开的 split stash */
		WT_STAT_FAST_CONN_ATOMIC_DECRV(session, rec_split_stashed_bytes, stash->len);
		WT_STAT_FAST_CONN_ATOMIC_DECR(session, rec_split_stashed_objects);

		__wt_overwrite_and_free_len(session, stash->p, stash->len);
	}

	/*进行空间收缩*/
	if (i > 100 || i == session->split_stash_cnt){
		if ((session->split_stash_cnt -= i) > 0)
			memmove(session->split_stash, stash, session->split_stash_cnt * sizeof(*stash));
	}
}

/*释放所有session中的split stash*/
void __wt_split_stash_discard_all(WT_SESSION_IMPL *session_safe, WT_SESSION_IMPL *session)
{
	WT_SPLIT_STASH *stash;
	size_t i;

	/*
	 * This function is called during WT_CONNECTION.close to discard any
	 * memory that remains.  For that reason, we take two WT_SESSION_IMPL
	 * arguments: session_safe is still linked to the WT_CONNECTION and
	 * can be safely used for calls to other WiredTiger functions, while
	 * session is the WT_SESSION_IMPL we're cleaning up.
	 */
	for(i = 0, stash = session->split_stash; i < session->split_stash_cnt; ++i, ++stash){
		if(stash->p != NULL)
			__wt_free(session_safe, stash->p);
	}

	__wt_free(session_safe, session->split_stash);
	session->split_stash_cnt = session->split_stash_alloc = 0;
}

/*当p没有线程对其操作或者访问时，释放它*/
static int __split_safe_free(WT_SESSION_IMPL* session, uint64_t split_gen, int exclusive, void* p, size_t s)
{
	WT_ASSERT(session, session->split_gen != split_gen);

	/*没有其他线程在操作，因为oldest gen是表示最早并且还active的操作线程*/
	if (!exclusive && __split_oldest_gen(session) > split_gen)
		exclusive = 1;

	if (exclusive){
		__wt_free(session, p);
		return 0;
	}

	/*不能释放，加入到session split stash list当中进行等待释放*/
	return __split_stash_add(session, split_gen, p, s);
}

/*检查btree是否可以deepen（加深层次）*/
static int __split_should_deepen(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t *childrenp)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;

	*childrenp = 0;

	btree = S2BT(session);
	page = ref->page;

	/* 在调用这个函数时，已经获得了page的父亲节点的lock来办证split操作是单线程的，所以这里可以直接获取 entries indexs*/
	pindex = WT_INTL_INDEX_GET_SAFE(page);

	/*在page deepen时，这个page的footprint应该是大于btree->maxmempage, 这样在访问时会强制进行page淘汰，那么就必须对这个page做split*/
	if (page->memory_footprint < btree->maxmempage)
		return 0;

	/*在page split操作时，page中应该有足够的entries数量来支持split操作*/
	if (pindex->entries > btree->split_deepen_min_child){
		*childrenp = pindex->entries / btree->split_deepen_per_child; /*确定split后的孩子节点个数*/
		return 1;
	}

	/*
	* Don't allow a single page to put pressure on cache usage. The root
	* page cannot be evicted, so if it's larger than the maximum, or if
	* and page has a quarter of the cache, let it split, a deep tree is
	* better than making no progress at all. Sanity check for 100 on-page
	* keys, nothing helps in the case of large keys and a too-small cache.
	* 如果page的entry数量 > 100并且是root page进行split操作
	* 如果是普通page且他的footprint占用的内存大于cache_size的1/4，考虑split,因为page太大
	*/
	if (pindex->entries >= 100 &&
		(__wt_ref_is_root(ref) || page->memory_footprint >= S2C(session)->cache_size / 4)) {
		*childrenp = pindex->entries / 10;
		return (1);
	}

	return 0;
}

/* clean row store overflow keys */
static int __split_ovfl_key_cleanup(WT_SESSION_IMPL* session, WT_PAGE* page, WT_REF* ref)
{
	WT_CELL *cell;
	WT_CELL_UNPACK kpack;
	WT_IKEY *ikey;
	uint32_t cell_offset;

	/*
	* A key being discarded (page split) or moved to a different page (page
	* deepening) may be an on-page overflow key.  Clear any reference to an
	* underlying disk image, and, if the key hasn't been deleted, delete it
	* along with any backing blocks.
	*/
	if ((ikey = __wt_ref_key_instantiated(ref)) == NULL)
		return (0);
	if ((cell_offset = ikey->cell_offset) == 0)
		return (0);

	/* Leak blocks rather than try this twice. */
	ikey->cell_offset = 0;

	/*删除keys值空间*/
	cell = WT_PAGE_REF_OFFSET(page, cell_offset);
	__wt_cell_unpack(cell, &kpack);
	if (kpack.ovfl && kpack.raw != WT_CELL_KEY_OVFL_RM)
		WT_RET(__wt_ovfl_discard(session, cell));

	return 0;
}

/*
* __split_ref_deepen_move --
*	Move a WT_REF from a parent to a child in service of a split to deepen
* the tree, including updating the accounting information.
*/
static int __split_ref_deepen_move(WT_SESSION_IMPL* session, WT_PAGE* parent, WT_REF* ref, size_t* parent_decrp, size_t* child_incrp)
{
	WT_ADDR *addr;
	WT_CELL_UNPACK unpack;
	WT_DECL_RET;
	WT_IKEY *ikey;
	size_t size;
	void *key;

	/*
	* Instantiate row-store keys, and column- and row-store addresses in
	* the WT_REF structures referenced by a page that's being split (and
	* deepening the tree).  The WT_REF structures aren't moving, but the
	* index references are moving from the page we're splitting to a set
	* of child pages, and so we can no longer reference the block image
	* that remains with the page being split.
	*
	* No locking is required to update the WT_REF structure because we're
	* the only thread splitting the parent page, and there's no way for
	* readers to race with our updates of single pointers.  The changes
	* have to be written before the page goes away, of course, our caller
	* owns that problem.
	*
	* Row-store keys, first.
	*/

	/*确定key值的长度和存储的位置*/
	if (parent->type == WT_PAGE_ROW_INT){
		if ((ikey = __wt_ref_key_instantiated(ref)) == NULL) {
			__wt_ref_key(parent, ref, &key, &size);
			WT_RET(__wt_row_ikey(session, 0, key, size, ref));
			ikey = ref->key.ikey;
		}
		else {
			WT_RET(__split_ovfl_key_cleanup(session, parent, ref));
			*parent_decrp += sizeof(WT_IKEY) + ikey->size;
		}
		*child_incrp += sizeof(WT_IKEY) + ikey->size;
	}

	addr = ref->addr;
	if (addr != NULL && !__wt_off_page(parent, addr)) {
		__wt_cell_unpack((WT_CELL *)ref->addr, &unpack);
		WT_RET(__wt_calloc_one(session, &addr));
		if ((ret = __wt_strndup(session, unpack.data, unpack.size, &addr->addr)) != 0) {
			__wt_free(session, addr);
			return (ret);
		}
		addr->size = (uint8_t)unpack.size;
		addr->type = unpack.raw == WT_CELL_ADDR_INT ? WT_ADDR_INT : WT_ADDR_LEAF;
		ref->addr = addr;
	}

	/*And finally, the WT_REF itself. */
	WT_MEM_TRANSFER(*parent_decrp, *child_incrp, sizeof(WT_REF));
	return 0;
}

#undef	SPLIT_CORRECT_1
#define	SPLIT_CORRECT_1	1		/* First page correction */
#undef	SPLIT_CORRECT_2
#define	SPLIT_CORRECT_2	2		/* First/last page correction */

/*对一个internal内部索引page做split*/
static int __split_deepen(WT_SESSION_IMPL *session, WT_PAGE *parent, uint32_t children)
{
	WT_DECL_RET;
	WT_PAGE *child;
	WT_PAGE_INDEX *alloc_index, *child_pindex, *pindex;
	WT_REF **alloc_refp;
	WT_REF *child_ref, **child_refp, *parent_ref, **parent_refp, *ref;
	size_t child_incr, parent_decr, parent_incr, size;
	uint64_t split_gen;
	uint32_t chunk, i, j, remain, slots;
	int panic;
	void *p;

	alloc_index = NULL;
	parent_incr = parent_decr = 0;
	panic = 0;

	/*在调用这个函数时，已经获得了page的父亲节点的lock来办证split操作是单线程的，所以这里可以直接获取 entries indexs*/
	pindex = WT_INTL_INDEX_GET_SAFE(parent);

	WT_STAT_FAST_CONN_INCR(session, cache_eviction_deepen);
	WT_STAT_FAST_DATA_INCR(session, cache_eviction_deepen);
	WT_ERR(__wt_verbose(session, WT_VERB_SPLIT, "%p: %" PRIu32 " elements, splitting into %" PRIu32 " children", parent, pindex->entries, children));

	/*
	* If the workload is prepending/appending to the tree, we could deepen
	* without bound.  Don't let that happen, keep the first/last pages of
	* the tree at their current level.
	*
	* XXX
	* To improve this, we could track which pages were last merged into
	* this page by eviction, and leave those pages alone, to prevent any
	* sustained insert into the tree from deepening a single location.
	*/

	/*
	* Allocate a new WT_PAGE_INDEX and set of WT_REF objects.  Initialize
	* the first/last slots of the allocated WT_PAGE_INDEX to point to the
	* first/last pages we're keeping at the current level, and the rest of
	* the slots to point to new WT_REF objects.
	* 分配entries slots空间和各个entries的ref空间
	*/

	size = sizeof(WT_PAGE_INDEX) + (children + SPLIT_CORRECT_2) * sizeof(WT_REF *);
	WT_ERR(__wt_calloc(session, 1, size, &alloc_index));
	parent_incr += size;
	alloc_index->index = (WT_REF **)(alloc_index + 1);
	alloc_index->entries = children + SPLIT_CORRECT_2;
	alloc_index->index[0] = pindex->index[0];
	alloc_index->index[alloc_index->entries - 1] = pindex->index[pindex->entries - 1];

	for (alloc_refp = alloc_index->index + SPLIT_CORRECT_1, i = 0; i < children; ++alloc_refp, ++i) {
		WT_ERR(__wt_calloc_one(session, alloc_refp));
		parent_incr += sizeof(WT_REF);
	}

	/*根据分配的entries数量来分配孩子page的空间*/
	chunk = (pindex->entries - SPLIT_CORRECT_2) / children;
	remain = (pindex->entries - SPLIT_CORRECT_2) - chunk * (children - 1);
	for (parent_refp = pindex->index + SPLIT_CORRECT_1, alloc_refp = alloc_index->index + SPLIT_CORRECT_1, i = 0; i < children; ++i){
		slots = ((i == children - 1) ? remain : chunk);
		WT_ERR(__wt_page_alloc(session, parent->type, 0, slots, 0, &child));

		/*对ref进行赋值*/
		ref = *alloc_refp++;
		ref->home = parent;
		ref->page = child;
		ref->addr = NULL;
		if (parent->type == WT_PAGE_ROW_INT){
			__wt_ref_key(parent, *parent_refp, &p, &size);
			WT_ERR(__wt_row_ikey(session, 0, p, size, ref));
			parent_incr += sizeof(WT_IKEY) + size;
		}
		else 
			ref->key.recno = (*parent_refp)->key.recno;
		ref->state = WT_REF_MEM;

		/*初始化child page内容*/
		if (parent->type == WT_PAGE_COL_INT)
			child->pg_intl_recno = (*parent_refp)->key.recno;
		/*初始化孩子的父亲ref*/
		child->pg_intl_parent_ref = ref;

		/*将孩子标记为脏页*/
		WT_ERR(__wt_page_modify_init(session, child));
		__wt_page_modify_set(session, child);

		/*
		* Once the split goes live, the newly created internal pages
		* might be evicted and their WT_REF structures freed.  If those
		* pages are evicted before threads exit the previous page index
		* array, a thread might see a freed WT_REF.  Set the eviction
		* transaction requirement for the newly created internal pages.
		*/
		child->modify->mod_split_txn = __wt_txn_new_id(session);

		child_incr = 0;
		child_pindex = WT_INTL_INDEX_GET_SAFE(child);
		for (child_refp = child_pindex->index, j = 0; j < slots; ++j) {
			/*索引key值从parent page移到child page上*/
			WT_ERR(__split_ref_deepen_move(session, parent, *parent_refp, &parent_decr, &child_incr));
			*child_refp++ = *parent_refp++;
		}

		__wt_cache_page_inmem_incr(session, child, child_incr);
	}

	/*数据安全检查，防止在split过程发生数据变化*/
	WT_ASSERT(session, alloc_retp - alloc_index->index == alloc_index->entries - SPLIT_CORRECT_1);
	WT_ASSERT(session,parent_refp - pindex->index == pindex->entries - SPLIT_CORRECT_1);
	WT_ASSERT(session, WT_INTL_INDEX_GET_SAFE(parent) == pindex);

	WT_INTL_INDEX_SET(parent, alloc_index);
	/*对split gen做自加*/
	split_gen = WT_ATOMIC_ADD8(S2C(session)->split_gen, 1);
	panic = 1;

	/*
	* The moved reference structures now reference the wrong parent page,
	* and we have to fix that up.  The problem is revealed when a thread
	* of control searches for a page's reference structure slot, and fails
	* to find it because the page it's searching no longer references it.
	* When that failure happens, the thread waits for the reference's home
	* page to be updated, which we do here: walk the children and fix them
	* up.
	*
	* We're not acquiring hazard pointers on these pages, they cannot be
	* evicted because of the eviction transaction value set above.
	*/
	for (parent_refp = alloc_index->index, i = alloc_index->entries; i > 0; ++parent_refp, --i){
		parent_ref = *parent_refp;
		WT_ASSERT(session, parent_ref->home == parent);
		if (parent_ref->state != WT_REF_MEM)
			continue;

		/*
		 * We left the first/last children of the parent at the current
		 * level to avoid bad split patterns, they might be leaf pages;
		 * check the page type before we continue.
		 */
		child = parent_ref->page;
		if (!WT_PAGE_IS_INTERNAL(child)) /*leaf page*/
			continue;

		WT_ENTER_PAGE_INDEX(session);
		WT_INTL_FOREACH_BEGIN(session, child, child_ref) {
			/*
			* The page's parent reference may not be wrong, as we
			* opened up access from the top of the tree already,
			* pages may have been read in since then.  Check and
			* only update pages that reference the original page,
			* they must be wrong.
			*/
			if (child_ref->home == parent) {
				child_ref->home = child;
				child_ref->ref_hint = 0;
			}
		}WT_INTL_FOREACH_END;
		WT_LEAVE_PAGE_INDEX(session);
	}

	/*
	* Push out the changes: not required for correctness, but don't let
	* threads spin on incorrect page references longer than necessary.
	*/
	WT_FULL_BARRIER();
	alloc_index = NULL;
	/*
	* We can't free the previous parent's index, there may be threads using
	* it.  Add to the session's discard list, to be freed once we know no
	* threads can still be using it.
	*
	* This change requires care with error handling: we have already
	* updated the page with a new index.  Even if stashing the old value
	* fails, we don't roll back that change, because threads may already
	* be using the new index.
	*/
	size = sizeof(WT_PAGE_INDEX) + pindex->entries * sizeof(WT_REF *);
	WT_ERR(__split_safe_free(session, split_gen, 0, pindex, size));
	parent_decr += size;

	/*更新内存统计的状态值*/
	__wt_cache_page_inmem_incr(session, parent, parent_incr);
	__wt_cache_page_inmem_decr(session, parent, parent_decr);

	if (0){
err:
		__wt_free_ref_index(session, parent, alloc_index, 1);
		/*
		* If panic is set, we saw an error after opening up the tree
		* to descent through the parent page's new index.  There is
		* nothing we can do, the tree is inconsistent and there are
		* threads potentially active in both versions of the tree.
		*/
		if (panic)
			ret = __wt_panic(session);
	}

	return ret;
}

/*当处btree于不可以更新状态,需要实例化一个临时的page来做更新保存，等可以写的时候，再同步到btree上？*/
static int __split_multi_inmem(WT_SESSION_IMPL* session, WT_PAGE* orig, WT_REF* ref, WT_MULTI* multi)
{
	WT_CURSOR_BTREE cbt;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_UPDATE *upd;
	WT_UPD_SKIPPED *skip;
	uint64_t recno;
	uint32_t i, slot;

	WT_CLEAR(cbt);
	cbt.iface.session = &session->iface;
	cbt.btree = S2BT(session);

   /*
	* We can find unresolved updates when attempting to evict a page, which
	* can't be written. This code re-creates the in-memory page and applies
	* the unresolved updates to that page.
	*
	* Clear the disk image and link the page into the passed-in WT_REF to
	* simplify error handling: our caller will not discard the disk image
	* when discarding the original page, and our caller will discard the
	* allocated page on error, when discarding the allocated WT_REF.
	*/
	/*将skip_dsk中的数据实例化成一个page结构*/
	WT_RET(__wt_page_inmem(session, ref, multi->skip_dsk, ((WT_PAGE_HEADER *)multi->skip_dsk)->mem_size, WT_PAGE_DISK_ALLOC, &page));
	multi->skip_dsk = NULL;

	/*对象复用方式开辟的item,用于构建KEY*/
	if (orig->type == WT_PAGE_ROW_LEAF)
		WT_RET(__wt_scr_alloc(session, 0, &key));

	/* Re-create each modification we couldn't write. */
	for (i = 0, skip = multi->skip; i < multi->skip_entries; ++i, ++skip){
		switch (orig->type){
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			/* Build a key. */
			upd = skip->ins->upd;
			skip->ins->upd = NULL;
			recno = WT_INSERT_RECNO(skip->ins);

			/* Search the page. */
			WT_ERR(__wt_col_search(session, recno, ref, &cbt));

			/* Apply the modification. */
			WT_ERR(__wt_col_modify(session, &cbt, recno, NULL, upd, 0));
			break;

		case WT_PAGE_ROW_LEAF:
			/* Build a key. */
			if (skip->ins == NULL) {
				slot = WT_ROW_SLOT(orig, skip->rip);
				upd = orig->pg_row_upd[slot];
				orig->pg_row_upd[slot] = NULL;

				WT_ERR(__wt_row_leaf_key(session, orig, skip->rip, key, 0));
			}
			else {
				upd = skip->ins->upd;
				skip->ins->upd = NULL;

				key->data = WT_INSERT_KEY(skip->ins);
				key->size = WT_INSERT_KEY_SIZE(skip->ins);
			}

			/* Search the page. */
			WT_ERR(__wt_row_search(session, key, ref, &cbt, 1));

			/* Apply the modification. */
			WT_ERR(__wt_row_modify(session, &cbt, key, NULL, upd, 0));
			break;

		WT_ILLEGAL_VALUE_ERR(session);
		}
	}

	/*
	* We modified the page above, which will have set the first dirty
	* transaction to the last transaction current running.  However, the
	* updates we installed may be older than that.  Set the first dirty
	* transaction to an impossibly old value so this page is never skipped
	* in a checkpoint.
	*/
	page->modify->first_dirty_txn = WT_TXN_FIRST;

err:
	WT_TRET(__wt_btcur_close(&cbt));
	__wt_scr_free(session, &key);

	return ret;
}



