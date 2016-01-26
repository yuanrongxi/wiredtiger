
#include "wt_internal.h"

/*
* Estimated memory cost for a structure on the overflow lists, the size of
* the structure plus two pointers (assume the average skip list depth is 2).
*/
#define WT_OVFL_SIZE(p, s)	(sizeof(s) + 2 * sizeof(void *) + (p)->addr_size + (p)->value_size)

/*为modify分配一个overflow track对象*/
static int __ovfl_track_init(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	return __wt_calloc_one(session, &page->modify->ovfl_track);
}

/*对一个废弃的overflow类型记录进行dump输出*/
static int __ovfl_discard_verbose(WT_SESSION_IMPL* session, WT_PAGE* page, WT_CELL* cell, const char* tag)
{
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 512, &tmp));

	unpack = &_unpack;
	__wt_cell_unpack(cell, unpack);
	/*dump 信息输出*/
	WT_ERR(__wt_verbose(session, WT_VERB_OVERFLOW, "discard: %s%s%p %s",
		tag == NULL ? "" : tag,
		tag == NULL ? "" : ": ",
		page, __wt_addr_string(session, unpack->data, unpack->size, tmp)));

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*释放掉page的track中的废弃的overflow cells*/
static int __ovfl_discard_wrapup(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_CELL **cellp;
	WT_DECL_RET;
	WT_OVFL_TRACK *track;
	uint32_t i;

	/*将track对象中的废弃的overflow 记录(cell)对象全部释放*/
	track = page->modify->ovfl_track;
	for (i = 0, cellp = track->discard; i < track->discard_entries; ++i, ++cellp){
		if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
			WT_RET(__ovfl_discard_verbose(session, page, *cellp, "free"));

		WT_RET(__wt_ovfl_discard(session, *cellp));
	}

	/*释放掉track对象中的discard数组*/
	__wt_free(session, track->discard);
	track->discard_entries = track->discard_allocated = 0;

	return ret;
}

/*
 *	Resolve the page's overflow discard list after an error occurs.
 */
static int __ovfl_discard_wrapup_err(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_OVFL_TRACK* track;

	track = page->modify->ovfl_track;
	__wt_free(session, track->discard);
	track->discard_entries = track->discard_allocated = 0;
}

/*增加一个discard ovfl cell到track对象中*/
int __wt_ovfl_discard_add(WT_SESSION_IMPL* session, WT_PAGE* page, WT_CELL* cell)
{
	WT_OVFL_TRACK* track;

	/*page的overflow track*/
	if (page->modify->ovfl_track == NULL)
		WT_RET(__ovfl_track_init(session, page));

	/*保证track->discard数组能存入新加入的discard ovfl cell*/
	track = page->modify->ovfl_track;
	WT_RET(__wt_realloc_def(session, &track->discard_allocated, track->discard_entries + 1, &track->discard));
	track->discard[track->discard_entries++] = cell;

	if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
		WT_RET(__ovfl_discard_verbose(session, page, cell, "add"));

	return 0;
}

/*释放page的track中的discard数组*/
void __wt_ovfl_discard_free(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_OVFL_TRACK* track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return;

	track = page->modify->ovfl_track;

	__wt_free(session, track->discard);
	track->discard_allocated = track->discard_entries = 0;
}

/*对ovfl reuse对象进行dump输出*/
static int __ovfl_reuse_verbose(WT_SESSION_IMPL *session, WT_PAGE *page, WT_OVFL_REUSE *reuse, const char *tag)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 64, &tmp));

	WT_ERR(__wt_verbose(session, WT_VERB_OVERFLOW,
		"reuse: %s%s%p %s (%s%s%s) {%.*s}",
		tag == NULL ? "" : tag,
		tag == NULL ? "" : ": ",
		page,
		__wt_addr_string(
		session, WT_OVFL_REUSE_ADDR(reuse), reuse->addr_size, tmp),
		F_ISSET(reuse, WT_OVFL_REUSE_INUSE) ? "inuse" : "",
		F_ISSET(reuse, WT_OVFL_REUSE_INUSE) &&
		F_ISSET(reuse, WT_OVFL_REUSE_JUST_ADDED) ? ", " : "",
		F_ISSET(reuse, WT_OVFL_REUSE_JUST_ADDED) ? "just-added" : "",
		WT_MIN(reuse->value_size, 40), (char *)WT_OVFL_REUSE_VALUE(reuse)));

err:	
	__wt_scr_free(session, &tmp);
	return ret;
}

/*在ovfl reuse的跳表中进行value匹配查找，如果找到相同的返回对应的节点指针,如果没有找到，返回NULL*/
static WT_OVFL_REUSE* __ovfl_reuse_skip_search(WT_OVFL_REUSE* head, const void* value, size_t value_size)
{
	WT_OVFL_REUSE **e, *next;
	size_t len;
	int cmp, i;

	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;){
		if (*e == NULL){ /*这一层是空的，向底下一层进行检索*/
			--i;
			--e;
			continue;
		}

		len = WT_MIN((*e)->value_size, value_size);
		cmp = memcmp(WT_OVFL_REUSE_VALUE(*e), value, len);
		if (cmp == 0 && (*e)->value_size == value_size){ /*这一层匹配到了结果，进行结果判断，如果这一层是最底层，直接返回，如果不是最底层，进行下一层判断，直到最低一层*/
			if (i == 0)
				return (F_ISSET(*e, WT_OVFL_REUSE_INUSE) ? NULL : *e);
			if ((next = (*e)->next[i]) == NULL || !F_ISSET(next, WT_OVFL_REUSE_INUSE) ||
				next->value_size != len || memcmp(WT_OVFL_REUSE_VALUE(next), value, len) != 0) {
				--i;		/* Drop down a level */
				--e;
			}
			else			/* Keep going at this level */
				e = &(*e)->next[i];
			continue;
		}

		if (cmp > 0 || (cmp == 0 && (*e)->value_size > value_size)){ /*value匹配的位置在这个节点管辖的范围，直接深入下一层*/
			--i;
			--e;
		}
		else /*value不在本层节点管理范围之内，继续向本层的后一个节点移动查找*/
			e = &(*e)->next[i];
	}

	return NULL;
}

/*
* __ovfl_reuse_skip_search_stack --
*	 Search an overflow reuse skiplist, returning an insert/remove stack.
*/
static void __ovfl_reuse_skip_search_stack(WT_OVFL_REUSE **head, WT_OVFL_REUSE ***stack, const void *value, size_t value_size)
{
	WT_OVFL_REUSE **e;
	size_t len;
	int cmp, i;

	/*
	* Start at the highest skip level, then go as far as possible at each
	* level before stepping down to the next.
	*/
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			stack[i--] = e--;
			continue;
		}

		/*
		* If the skiplist value is larger than the search value, or
		* they compare equally and the skiplist value is longer than
		* the search value, drop down a level, otherwise continue on
		* this level.
		*/
		len = WT_MIN((*e)->value_size, value_size);
		cmp = memcmp(WT_OVFL_REUSE_VALUE(*e), value, len);
		if (cmp > 0 || (cmp == 0 && (*e)->value_size > value_size))
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
	}
}


/*回收所有废弃的ovfl page对应的block空间*/
static int __ovfl_reuse_wrapup(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_BM *bm;
	WT_OVFL_REUSE **e, **head, *reuse;
	size_t decr;
	int i;

	bm = S2BT(session)->bm;

	/*
	* Discard any overflow records that aren't in-use, freeing underlying blocks.
	* First, walk the overflow reuse lists (except for the lowest one),
	* fixing up skiplist links.
	* 先删除1 ~ WT_SKIP_MAXDEPTH所有符合删除节点的对应关系
	*/
	for (i = WT_SKIP_MAXDEPTH - 1; i > 0; --i){
		for (e = &head[i]; (reuse = *e) != NULL;){
			if (F_ISSET(reuse, WT_OVFL_REUSE_INUSE)) {
				e = &reuse->next[i];
				continue;
			}
			*e = reuse->next[i];
		}
	}

	decr = 0;
	for (e = &head[0]; (reuse = *e) != NULL;) {
		if (F_ISSET(reuse, WT_OVFL_REUSE_INUSE)) { /*跳过reuse-inuse的block,将这个block标记去掉WT_OVFL_REUSE_INUSE | WT_OVFL_REUSE_JUST_ADDED*/
			F_CLR(reuse, WT_OVFL_REUSE_INUSE | WT_OVFL_REUSE_JUST_ADDED);
			e = &reuse->next[0];
			continue;
		}
		*e = reuse->next[0];

		WT_ASSERT(session, !F_ISSET(reuse, WT_OVFL_REUSE_JUST_ADDED));

		if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
			WT_RET(__ovfl_reuse_verbose(session, page, reuse, "free"));
		/*free overflow block*/
		WT_RET(bm->free(bm, session, WT_OVFL_REUSE_ADDR(reuse), reuse->addr_size));
		decr += WT_OVFL_SIZE(reuse, WT_OVFL_REUSE);
		__wt_free(session, reuse);
	}

	/*更新内存统计*/
	if (decr != 0)
		__wt_cache_page_inmem_decr(session, page, decr);

	return 0;
}

static int __ovfl_reuse_wrapup_err(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BM *bm;
	WT_DECL_RET;
	WT_OVFL_REUSE **e, **head, *reuse;
	size_t decr;
	int i;

	bm = S2BT(session)->bm;
	head = page->modify->ovfl_track->ovfl_reuse;

	/*
	* Discard any overflow records that were just added, freeing underlying blocks.
	*
	* First, walk the overflow reuse lists (except for the lowest one),
	* fixing up skiplist links.
	*/
	for (i = WT_SKIP_MAXDEPTH - 1; i > 0; --i)
		for (e = &head[i]; (reuse = *e) != NULL;) {
			if (!F_ISSET(reuse, WT_OVFL_REUSE_JUST_ADDED)) {
				e = &reuse->next[i];
				continue;
			}
			*e = reuse->next[i];
		}

	/*
	* Second, discard any overflow record with a just-added flag, clear the
	* flags for the next run.
	*/
	decr = 0;
	for (e = &head[0]; (reuse = *e) != NULL;) {
		if (!F_ISSET(reuse, WT_OVFL_REUSE_JUST_ADDED)) {
			F_CLR(reuse, WT_OVFL_REUSE_INUSE);
			e = &reuse->next[0];
			continue;
		}
		*e = reuse->next[0];

		if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
			WT_RET(__ovfl_reuse_verbose(session, page, reuse, "free"));

		WT_TRET(bm->free(bm, session, WT_OVFL_REUSE_ADDR(reuse), reuse->addr_size));
		decr += WT_OVFL_SIZE(reuse, WT_OVFL_REUSE);
		__wt_free(session, reuse);
	}

	if (decr != 0)
		__wt_cache_page_inmem_decr(session, page, decr);
	return (0);
}

/*用value作为匹配查找条件，在reuse blocks跳表中找到匹配的block*/
int __wt_ovfl_reuse_search(WT_SESSION_IMPL* session, WT_PAGE* page, uint8_t** addrp, size_t*addr_sizep, const void* value, size_t value_size)
{
	WT_OVFL_REUSE **head, *reuse;

	*addrp = NULL;
	*addr_sizep = 0;

	if (page->modify->ovfl_track == NULL)
		return 0;

	/*获取跳表头*/
	head = page->modify->ovfl_track->ovfl_reuse;
	/*匹配定位*/
	if ((reuse = __ovfl_reuse_skip_search(head, value, value_size)) == NULL)
		return 0;
	/*复制匹配的block cookie*/
	*addrp = WT_OVFL_REUSE_ADDR(reuse);
	*addr_sizep = reuse->addr_size;
	F_SET(reuse, WT_OVFL_REUSE_INUSE);

	if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
		WT_RET(__ovfl_reuse_verbose(session, page, reuse, "reclaim"));

	return 1;
}

/*向track的reuse列表中增加一个reuse block*/
int __wt_ovfl_reuse_add(WT_SESSION_IMPL* session, WT_PAGE* page, const uint8_t* addr, size_t addr_size, 
						const void* value, size_t value_size)
{
	WT_OVFL_REUSE **head, *reuse, **stack[WT_SKIP_MAXDEPTH];
	size_t size;
	u_int i, skipdepth;
	uint8_t *p;

	if (page->modify->ovfl_track == NULL)
		WT_RET(__ovfl_track_init(session, page));

	head = page->modify->ovfl_track->ovfl_reuse;

	/* Choose a skiplist depth for this insert. */
	skipdepth = __wt_skip_choose_depth(session);

	/*分配一个reuse 跳表单元*/
	size = sizeof(WT_OVFL_REUSE) + skipdepth * sizeof(WT_OVFL_REUSE *) + addr_size + value_size;
	WT_RET(__wt_calloc(session, 1, size, &reuse));
	p = (uint8_t *)reuse + sizeof(WT_OVFL_REUSE) + skipdepth * sizeof(WT_OVFL_REUSE *);
	/*设置单元的值*/
	reuse->addr_offset = (uint8_t)WT_PTRDIFF(p, reuse);
	reuse->addr_size = (uint8_t)addr_size;
	memcpy(p, addr, addr_size);
	p += addr_size;
	reuse->value_offset = WT_PTRDIFF32(p, reuse);
	reuse->value_size = WT_STORE_SIZE(value_size);
	memcpy(p, value, value_size);
	F_SET(reuse, WT_OVFL_REUSE_INUSE | WT_OVFL_REUSE_JUST_ADDED);

	__wt_cache_page_inmem_incr(session, page, WT_OVFL_SIZE(reuse, WT_OVFL_REUSE));

	/* Insert the new entry into the skiplist. */
	__ovfl_reuse_skip_search_stack(head, stack, value, value_size);
	for (i = 0; i < skipdepth; ++i) {
		reuse->next[i] = *stack[i];
		*stack[i] = reuse;
	}

	if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
		WT_RET(__ovfl_reuse_verbose(session, page, reuse, "add"));

	return 0;
}

/*释放track中的reuse对象*/
static int __wt_ovfl_reuse_free(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_OVFL_REUSE *reuse;
	WT_PAGE_MODIFY *mod;
	void *next;

	mod = page->modify;
	if (mod == NULL || mod->ovfl_track == NULL)
		return;

	for (reuse = mod->ovfl_track->ovfl_reuse[0];
		reuse != NULL; reuse = next) {
		next = reuse->next[0];
		__wt_free(session, reuse);
	}
}

/*对缓冲事务的overflow record进行dump输出*/
static int __ovfl_txnc_verbose(WT_SESSION_IMPL* session, WT_PAGE* page, WT_OVFL_TXNC* txnc, const char* tag)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 64, &tmp));

	WT_ERR(__wt_verbose(session, WT_VERB_OVERFLOW,
		"txn-cache: %s%s%p %s %" PRIu64 " {%.*s}",
		tag == NULL ? "" : tag,
		tag == NULL ? "" : ": ",
		page,
		__wt_addr_string(
		session, WT_OVFL_TXNC_ADDR(txnc), txnc->addr_size, tmp),
		txnc->current,
		WT_MIN(txnc->value_size, 40), (char *)WT_OVFL_TXNC_VALUE(txnc)));

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*通过addr匹配查找到对应的OVFL_TXNC对象*/
static WT_OVFL_TXNC* __ovfl_txnc_skip_search(WT_OVFL_TXNC** head, const void* addr, size_t addr_size)
{
	WT_OVFL_TXNC **e;
	size_t len;
	int cmp, i;

	/*
	* Start at the highest skip level, then go as far as possible at each
	* level before stepping down to the next.
	*/
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			--i;
			--e;
			continue;
		}

		/*
		* Return any exact matches: we don't care in what search level
		* we found a match.
		*/
		len = WT_MIN((*e)->addr_size, addr_size);
		cmp = memcmp(WT_OVFL_TXNC_ADDR(*e), addr, len);
		if (cmp == 0 && (*e)->addr_size == addr_size)
			return (*e);

		/*
		* If the skiplist address is larger than the search address, or
		* they compare equally and the skiplist address is longer than
		* the search address, drop down a level, otherwise continue on
		* this level.
		*/
		if (cmp > 0 || (cmp == 0 && (*e)->addr_size > addr_size)) {
			--i;			/* Drop down a level */
			--e;
		}
		else				/* Keep going at this level */
			e = &(*e)->next[i];
	}

	return NULL;
}

/*
* Search an overflow transaction-cache skiplist, returning an insert/remove stack.
*/
static void __ovfl_txnc_skip_search_stack(WT_OVFL_TXNC **head, WT_OVFL_TXNC ***stack, const void *addr, size_t addr_size)
{
	WT_OVFL_TXNC **e;
	size_t len;
	int cmp, i;

	/*
	* Start at the highest skip level, then go as far as possible at each
	* level before stepping down to the next.
	*/
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			stack[i--] = e--;
			continue;
		}

		/*
		* If the skiplist addr is larger than the search addr, or
		* they compare equally and the skiplist addr is longer than
		* the search addr, drop down a level, otherwise continue on
		* this level.
		*/
		len = WT_MIN((*e)->addr_size, addr_size);
		cmp = memcmp(WT_OVFL_TXNC_ADDR(*e), addr, len);
		if (cmp > 0 || (cmp == 0 && (*e)->addr_size > addr_size))
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
	}
}

/*重整transaction-cache skiplist*/
static int __ovfl_txnc_wrapup(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_OVFL_TXNC **e, **head, *txnc;
	uint64_t oldest_txn;
	size_t decr;
	int i;

	head = page->modify->ovfl_track->ovfl_txnc;

	/*
	* Take a snapshot of the oldest transaction ID we need to keep alive.
	* Since we do two passes through entries in the structure, the normal
	* visibility check could give different results as the global ID moves
	* forward.
	*/
	oldest_txn = S2C(session)->txn_global.oldest_id;

	/*剔除所有小于oldest_txn的事务, 并删除跳表节点对应关系*/
	for (i = WT_SKIP_MAXDEPTH - 1; i > 0; --i){
		for (e = &head[i]; (txnc = *e) != NULL;) {
			if (TXNID_LE(oldest_txn, txnc->current)) {
				e = &txnc->next[i];
				continue;
			}
			*e = txnc->next[i];
		}
	}

	/*从最底层删除OVFL_TXNC对象*/
	decr = 0;
	for (e = &head[0]; (txnc = *e) != NULL;) {
		if (TXNID_LE(oldest_txn, txnc->current)) {
			e = &txnc->next[0];
			continue;
		}
		*e = txnc->next[0];

		if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
			WT_RET(__ovfl_txnc_verbose(session, page, txnc, "free"));

		/*释放掉OVFL_TXNC对象*/
		decr += WT_OVFL_SIZE(txnc, WT_OVFL_TXNC);
		__wt_free(session, txnc);
	}

	if (decr != 0)
		__wt_cache_page_inmem_decr(session, page, decr);

		return 0;
}

/*通过addr的值在track中的ovfl_txnc中进行查找定位，并通过store将值返回*/
int __wt_ovfl_txnc_search(WT_PAGE* page, const uint8_t* addr, size_t addr_size, WT_ITEM* store)
{
	WT_OVFL_TXNC** head, *txnc;

	if (page->modify->ovfl_track == NULL)
		return WT_NOTFOUND;

	head = page->modify->ovfl_track->ovfl_txnc;

	if ((txnc = __ovfl_txnc_skip_search(head, addr, addr_size)) == NULL)
		return WT_NOTFOUND;

	store->data = WT_OVFL_TXNC_VALUE(txnc);
	store->size = txnc->value_size;

	return 0;
}

/*向stack中添加一个OVFL_TXNC对象，用addr作为跳表查找条件*/
int __wt_ovfl_txnc_add(WT_SESSION_IMPL *session, WT_PAGE *page, const uint8_t *addr, size_t addr_size, const void *value, size_t value_size)
{
	WT_OVFL_TXNC **head, **stack[WT_SKIP_MAXDEPTH], *txnc;
	size_t size;
	u_int i, skipdepth;
	uint8_t *p;

	if (page->modify->ovfl_track == NULL)
		WT_RET(__ovfl_track_init(session, page));

	head = page->modify->ovfl_track->ovfl_txnc;

	/* Choose a skiplist depth for this insert. */
	skipdepth = __wt_skip_choose_depth(session);

	/*
	* Allocate the WT_OVFL_TXNC structure, next pointers for the skip
	* list, room for the address and value, then copy everything into
	* place.
	*
	* To minimize the WT_OVFL_TXNC structure size, the address offset
	* and size are single bytes: that's safe because the address follows
	* the structure (which can't be more than about 100B), and address
	* cookies are limited to 255B.
	*/
	size = sizeof(WT_OVFL_TXNC) + skipdepth * sizeof(WT_OVFL_TXNC *) + addr_size + value_size;
	WT_RET(__wt_calloc(session, 1, size, &txnc));
	p = (uint8_t *)txnc + sizeof(WT_OVFL_TXNC) + skipdepth * sizeof(WT_OVFL_TXNC *);
	txnc->addr_offset = (uint8_t)WT_PTRDIFF(p, txnc);
	txnc->addr_size = (uint8_t)addr_size;
	memcpy(p, addr, addr_size);
	p += addr_size;
	txnc->value_offset = WT_PTRDIFF32(p, txnc);
	txnc->value_size = WT_STORE_SIZE(value_size);
	memcpy(p, value, value_size);
	txnc->current = __wt_txn_new_id(session);

	__wt_cache_page_inmem_incr(session, page, WT_OVFL_SIZE(txnc, WT_OVFL_TXNC));

	/* Insert the new entry into the skiplist. */
	__ovfl_txnc_skip_search_stack(head, stack, addr, addr_size);
	for (i = 0; i < skipdepth; ++i) {
		txnc->next[i] = *stack[i];
		*stack[i] = txnc;
	}

	if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
		WT_RET(__ovfl_txnc_verbose(session, page, txnc, "add"));

	return 0;
}

/*释放page track对应的ovfl_txnc跳表中的单元数据,其实就是释放ovfl_txnc跳表*/
void __wt_ovfl_txnc_free(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_OVFL_TXNC *txnc;
	WT_PAGE_MODIFY *mod;
	void *next;

	mod = page->modify;
	if (mod == NULL || mod->ovfl_track == NULL)
		return;

	for (txnc = mod->ovfl_track->ovfl_txnc[0];
		txnc != NULL; txnc = next) {
		next = txnc->next[0];
		__wt_free(session, txnc);
	}
}

/* 对整个track对象进行重整，包括:dicard数组、reuse skip list/ovfl_txnc skip list */
int __wt_ovfl_track_wrapup(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_DECL_RET;
	WT_OVFL_TRACK *track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return (0);

	track = page->modify->ovfl_track;
	if (track->discard != NULL)
		WT_RET(__ovfl_discard_wrapup(session, page));

	if (track->ovfl_reuse[0] != NULL)
		WT_RET(__ovfl_reuse_wrapup(session, page));

	if (track->ovfl_txnc[0] != NULL) {
		WT_RET(__wt_writelock(session, S2BT(session)->ovfl_lock));
		ret = __ovfl_txnc_wrapup(session, page);
		WT_TRET(__wt_writeunlock(session, S2BT(session)->ovfl_lock));
	}
	return (0);
}

/*
* __wt_ovfl_track_wrapup_err --
*	Resolve the page's overflow tracking on reconciliation error.
*/
int __wt_ovfl_track_wrapup_err(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_DECL_RET;
	WT_OVFL_TRACK *track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return (0);

	track = page->modify->ovfl_track;
	if (track->discard != NULL)
		WT_RET(__ovfl_discard_wrapup_err(session, page));

	if (track->ovfl_reuse[0] != NULL)
		WT_RET(__ovfl_reuse_wrapup_err(session, page));

	if (track->ovfl_txnc[0] != NULL) {
		WT_RET(__wt_writelock(session, S2BT(session)->ovfl_lock));
		ret = __ovfl_txnc_wrapup(session, page);
		WT_TRET(__wt_writeunlock(session, S2BT(session)->ovfl_lock));
	}
	return 0;
}


