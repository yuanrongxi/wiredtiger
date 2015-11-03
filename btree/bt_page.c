#include "wt_internal.h"

static void __inmem_col_fix(WT_SESSION_IMPL *, WT_PAGE *);
static void __inmem_col_int(WT_SESSION_IMPL *, WT_PAGE *);
static int  __inmem_col_var(WT_SESSION_IMPL *, WT_PAGE *, size_t *);
static int  __inmem_row_int(WT_SESSION_IMPL *, WT_PAGE *, size_t *);
static int  __inmem_row_leaf(WT_SESSION_IMPL *, WT_PAGE *);
static int  __inmem_row_leaf_entries(WT_SESSION_IMPL *, const WT_PAGE_HEADER *, uint32_t *);

/*检查一个page是否可以强制驱逐（淘汰）出内存*/
static int __evict_force_check(WT_SESSION_IMPL* session, WT_PAGE* page, uint32_t flags)
{
	WT_BTREE* btree;
	btree = S2BT(session);

	/*page的内存没有达到驱逐的阈值，可以不用被驱逐出内存*/
	if (page->memory_footprint < btree->maxmempage)
		return 0;

	/*索引page暂不驱逐，只驱逐leaf page*/
	if (WT_PAGE_IS_INTERNAL(page))
		return 0;

	/*驱逐被关闭,不做驱逐动作*/
	if (LF_ISSET(WT_READ_NO_EVICT) || F_ISSET(btree, WT_BTREE_NO_EVICTION))
		return 0;

	/*没太弄明白为什么没有修改的page不做驱逐*/
	if (page->modify == NULL)
		return 0;

	/*触发驱逐page动作,设置page的驱逐状态*/
	__wt_page_evict_soon(page);

	/*判断是否可以直接驱逐*/
	return __wt_page_can_evict(session, page, 1);
}

/*帮助ref对应的page获得一个hazard pointer,如果page是在磁盘上，将page读入memory中*/
int __wt_page_in_func(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
{
	WT_DECL_RET;
	WT_PAGE *page;
	u_int sleep_cnt, wait_cnt;
	int busy, force_attempts, oldgen;

	for (force_attempts = oldgen = 0, wait_cnt = 0;;){
		switch (ref->state){
		case WT_REF_DISK:
		case WT_REF_DELETED:
			/*page已经做了cache read，无需read*/
			if (LF_ISSET(WT_READ_CACHE))
				return WT_NOTFOUND;

			WT_RET(__wt_cache_full_check(session));
			WT_RET(__wt_cache_read(session, ref));
			oldgen = LF_ISSET(WT_READ_WONT_NEED) || F_ISSET(session, WT_SESSION_NO_CACHE);
			continue;

		case WT_REF_READING:
			if (LF_ISSET(WT_READ_CACHE))
				return WT_NOTFOUND;
			if (LF_ISSET(WT_READ_NO_WAIT))
				return WT_NOTFOUND;
			WT_STAT_FAST_CONN_INCR(session, page_read_blocked);
			break;

		case WT_REF_LOCKED:
			if (LF_ISSET(WT_READ_NO_WAIT))
				return WT_NOTFOUND;
			WT_STAT_FAST_CONN_INCR(session, page_locked_blocked);
			break;

		case WT_REF_SPLIT:
			return WT_RESTART;

			/*已经导入内存中了*/
		case WT_REF_MEM:
			WT_RET(__wt_hazard_set(session, ref, &busy)); /*如果page导入memory，需要占用一个hazard pointer，如果占用hazard pointer时返回busy，表示这个page可能已经正在被驱逐,需要重试*/
			if (busy){
				WT_STAT_FAST_CONN_INCR(session, page_busy_blocked);
				break;
			}

			page = ref->page;
			WT_ASSERT(session, page != NULL);

			/*检查page是否可以驱逐，如果可以进行强制驱逐ref对应的page*/
			if (force_attempts < 10 && __evict_force_check(session, page, flags)){
				++force_attempts;
				ret = __wt_page_release_evict(session, ref);
				/*evict失败,退出循环重试*/
				if (ret == EBUSY) {
					ret = 0;
					wait_cnt += 1000;
					WT_STAT_FAST_CONN_INCR(session, page_forcible_evict_blocked);
					break;
				}
				else
					WT_RET(ret);

				continue;
			}

			/* Check if we need an autocommit transaction. */
			if ((ret = __wt_txn_autocommit_check(session)) != 0) {
				WT_TRET(__wt_hazard_clear(session, page)); /*清除hazard pointer占用*/
				return (ret);
			}

			if (oldgen && page->read_gen == WT_READGEN_NOTSET)
				__wt_page_evict_soon(page);
			else if (!LF_ISSET(WT_READ_NO_GEN) && page->read_gen != WT_READGEN_OLDEST && page->read_gen < __wt_cache_read_gen(session))
				page->read_gen = __wt_cache_read_gen_set(session);

			return 0;
			WT_ILLEGAL_VALUE(session);
		}

		/*spin waiting*/
		if (++wait_cnt < 1000)
			__wt_yield();
		else {
			sleep_cnt = WT_MIN(wait_cnt, 10000);
			wait_cnt *= 2;
			WT_STAT_FAST_CONN_INCRV(session, page_sleep, sleep_cnt);
			__wt_sleep(0, sleep_cnt);
		}
	}
}

/*创建并读取一个PAGE内容到cache中*/
int __wt_page_alloc(WT_SESSION_IMPL* session, uint8_t type, uint64_t recno, uint32_t alloc_entries, int alloc_refs, WT_PAGE** pagep)
{
	WT_CACHE *cache;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	size_t size;
	uint32_t i;
	void *p;

	*pagep = NULL;

	cache = S2C(session)->cache;
	page = NULL;

	size = sizeof(WT_PAGE);
	switch (type){
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		break;

	case WT_PAGE_COL_VAR:
		/*计算page的内存空间占用*/
		size += alloc_entries * sizeof(WT_COL);
		break;

	case WT_PAGE_ROW_LEAF:
		size += alloc_entries * sizeof(WT_ROW);
		break;

	WT_ILLEGAL_VALUE(session);
	}

	/*分配page对象空间并进行page类型设置*/
	WT_RET(__wt_calloc(session, 1, size, &page));
	page->type = type;
	page->read_gen = WT_READGEN_NOTSET;

	switch (type){
	case WT_PAGE_COL_FIX:
		page->pg_fix_recno = recno;
		page->pg_fix_entries = alloc_entries;
		break;

	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		page->pg_intl_recno = recno;

		WT_ERR(__wt_calloc(session, 1, sizeof(WT_PAGE_INDEX)+alloc_entries * sizeof(WT_REF *), &p));
		size += sizeof(WT_PAGE_INDEX)+alloc_entries * sizeof(WT_REF *);
		pindex = p;
		pindex->index = (WT_REF **)((WT_PAGE_INDEX *)p + 1);
		pindex->entries = alloc_entries;
		WT_INTL_INDEX_SET(page, pindex);
		if (alloc_refs){
			for (i = 0; i < pindex->entries; ++i) {
				WT_ERR(__wt_calloc_one(session, &pindex->index[i]));
				size += sizeof(WT_REF);
			}
		}
		if (0){
err:
			if ((pindex = WT_INTL_INDEX_GET_SAFE(page)) != NULL) {
				for (i = 0; i < pindex->entries; ++i)
					__wt_free(session, pindex->index[i]);
				__wt_free(session, pindex);
			}
			__wt_free(session, page);
			return (ret);
		}

	case WT_PAGE_COL_VAR:
		page->pg_var_recno = recno;
		page->pg_var_d = (WT_COL *)((uint8_t *)page + sizeof(WT_PAGE));
		page->pg_var_entries = alloc_entries;
		break;

	case WT_PAGE_ROW_LEAF:
		page->pg_row_d = (WT_ROW *)((uint8_t *)page + sizeof(WT_PAGE));
		page->pg_row_entries = alloc_entries;
		break;

		WT_ILLEGAL_VALUE(session);
	}
	/*更新cache的状态信息*/
	__wt_cache_page_inmem_incr(session, page, size);
	WT_ATOMIC_ADD8(cache->bytes_read, size);
	WT_ATOMIC_ADD8(cache->pages_inmem, 1);

	*pagep = page;
	return 0;
}



