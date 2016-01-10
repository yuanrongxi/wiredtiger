/***********************************************************************
* btree修复操作实现
***********************************************************************/
#include "wt_internal.h"


struct __wt_stuff;	  
typedef struct __wt_stuff WT_STUFF;
struct __wt_track;	  
typedef struct __wt_track WT_TRACK;
struct __wt_track_shared; 
typedef struct __wt_track_shared WT_TRACK_SHARED;

struct __wt_stuff
{
	WT_SESSION_IMPL*			session;			/* Salvage session */

	WT_TRACK**					pages;				/* Pages */
	uint32_t					pages_next;			/* Next empty slot */
	size_t						pages_allocated;	/* bytes allocated */

	WT_TRACK**					ovfl;				/* Overflow pages */
	uint32_t					ovfl_next;			/* Next empty slot */
	size_t						ovfl_alloctated;	/* Bytes allocated */

	WT_REF						root_ref;			/* root page ref */

	uint8_t						page_type;			/* Page type */

	int							merge_free;
	WT_ITEM*					tmp1;				/* Verbose print buffer */
	WT_ITEM*					tmp2;				/* Verbose print buffer */

	uint64_t					fcnt;				/* Progress counter */		
};

struct __wt_track_shared
{
	uint32_t					ref;				/* Reference count */
	
	WT_ADDR						addr;				/* Page address */
	uint32_t					size;				/* Page size */
	uint64_t					gen;				/* Page generation */

	WT_ADDR*					ovfl_addr;			/* Overflow pages by address */
	uint32_t*					ovfl_slot;			/* Overflow pages by slot */
	uint32_t					ovfl_cnt;			/* Overflow reference count */
};

struct __wt_track
{
	WT_TRACK_SHARED*			shared;				/* Shared information */
	WT_STUFF*					ss;					/* Enclosing stuff */
	union{
		/*row store 描述*/
		struct{;
			WT_ITEM					_row_start;		/* Row-store start range */
			WT_ITEM					_row_stop;		/* Row-store stop range */
		}row;

		/*column store 描述*/
		struct{
			uint64_t				_col_start;		/* Col-store start range */
			uint64_t				_col_stop;		/* Col-store stop range */
			uint64_t				_col_missing;	/* Col-store missing range */
		}col;
	}u;

	u_int	flags;
};

/*快捷宏定义*/
#define	trk_addr		shared->addr.addr
#define	trk_addr_size	shared->addr.size
#define	trk_gen			shared->gen
#define	trk_ovfl_addr	shared->ovfl_addr
#define	trk_ovfl_cnt	shared->ovfl_cnt
#define	trk_ovfl_slot	shared->ovfl_slot
#define	trk_size		shared->size

#undef	row_start
#define	row_start		u.row._row_start

#undef	row_stop
#define	row_stop		u.row._row_stop

#undef	col_start
#define	col_start		u.col._col_start

#undef	col_stop
#define	col_stop		u.col._col_stop

#undef	col_missing
#define	col_missing		u.col._col_missing

#define	WT_TRACK_CHECK_START	0x01		/* Row: initial key updated */
#define	WT_TRACK_CHECK_STOP		0x02		/* Row: last key updated */
#define	WT_TRACK_MERGE			0x04		/* Page requires merging */
#define	WT_TRACK_OVFL_REFD		0x08		/* Overflow page referenced */

/*内部函数*/
static int  __slvg_cleanup(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_col_build_internal(WT_SESSION_IMPL *, uint32_t, WT_STUFF *);
static int  __slvg_col_build_leaf(WT_SESSION_IMPL *, WT_TRACK *, WT_REF *);
static int  __slvg_col_ovfl(WT_SESSION_IMPL *, WT_TRACK *, WT_PAGE *, uint64_t, uint64_t);
static int  __slvg_col_range(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_col_range_missing(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_col_range_overlap(WT_SESSION_IMPL *, uint32_t, uint32_t, WT_STUFF *);
static void __slvg_col_trk_update_start(uint32_t, WT_STUFF *);
static int  __slvg_merge_block_free(WT_SESSION_IMPL *, WT_STUFF *);
static int WT_CDECL __slvg_ovfl_compare(const void *, const void *);
static int  __slvg_ovfl_discard(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_ovfl_reconcile(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_ovfl_ref(WT_SESSION_IMPL *, WT_TRACK *, int);
static int  __slvg_ovfl_ref_all(WT_SESSION_IMPL *, WT_TRACK *);
static int  __slvg_read(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_row_build_internal(WT_SESSION_IMPL *, uint32_t, WT_STUFF *);
static int  __slvg_row_build_leaf(WT_SESSION_IMPL *, WT_TRACK *, WT_REF *, WT_STUFF *);
static int  __slvg_row_ovfl(WT_SESSION_IMPL *, WT_TRACK *, WT_PAGE *, uint32_t, uint32_t);
static int  __slvg_row_range(WT_SESSION_IMPL *, WT_STUFF *);
static int  __slvg_row_range_overlap(WT_SESSION_IMPL *, uint32_t, uint32_t, WT_STUFF *);
static int  __slvg_row_trk_update_start(WT_SESSION_IMPL *, WT_ITEM *, uint32_t, WT_STUFF *);
static int  WT_CDECL __slvg_trk_compare_addr(const void *, const void *);
static int  WT_CDECL __slvg_trk_compare_gen(const void *, const void *);
static int  WT_CDECL __slvg_trk_compare_key(const void *, const void *);
static int  __slvg_trk_free(WT_SESSION_IMPL *, WT_TRACK **, int);
static void __slvg_trk_free_addr(WT_SESSION_IMPL *, WT_TRACK *);
static int  __slvg_trk_init(WT_SESSION_IMPL *, uint8_t *, size_t, uint32_t, uint64_t, WT_STUFF *, WT_TRACK **);
static int  __slvg_trk_leaf(WT_SESSION_IMPL *,const WT_PAGE_HEADER *, uint8_t *, size_t, WT_STUFF *);
static int  __slvg_trk_leaf_ovfl(WT_SESSION_IMPL *, const WT_PAGE_HEADER *, WT_TRACK *);
static int  __slvg_trk_ovfl(WT_SESSION_IMPL *,const WT_PAGE_HEADER *, uint8_t *, size_t, WT_STUFF *);
static int  __slvg_trk_split(WT_SESSION_IMPL *, WT_TRACK *, WT_TRACK **);

/*修复btree函数实现*/
int __wt_bt_salvage(WT_SESSION_IMPL* session, WT_CKPT* ckptbase, const char* cfg[])
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_STUFF *ss, stuff;
	uint32_t i, leaf_cnt;

	WT_UNUSED(cfg);

	btree = S2BT(session);
	bm = btree->bm;

	WT_CLEAR(stuff);
	ss = &stuff;
	ss->session = session;
	ss->page_type = WT_PAGE_INVALID;

	/*分配verbose print buffer*/
	WT_ERR(__wt_scr_alloc(session, 0, &ss->tmp1));
	WT_ERR(__wt_scr_alloc(session, 0, &ss->tmp2));

	/*第一步：告诉block manager要开始对btree对应的block做修复操作*/
	WT_ERR(bm->salvage_start(bm, session));

	/*第二步：读取对应文件，并将文件转换成对应内存中的leaf page或者overflow page结构*/
	F_SET(session, WT_SESSION_SALVAGE_CORRUPT_OK);
	ret = __slvg_read(session, ss);
	F_CLR(session, WT_SESSION_SALVAGE_CORRUPT_OK);
	WT_ERR(ret);

	/*
	 * Step 3:
	 * Discard any page referencing a non-existent overflow page.  We do
	 * this before checking overlapping key ranges on the grounds that a
	 * bad key range we can use is better than a terrific key range that
	 * references pages we don't have. On the other hand, we subsequently
	 * discard key ranges where there are better overlapping ranges, and
	 * it would be better if we let the availability of an overflow value
	 * inform our choices as to the key ranges we select, ideally on a
	 * per-key basis.
	 *
	 * A complicating problem is found in variable-length column-store
	 * objects, where we potentially split key ranges within RLE units.
	 * For example, if there's a page with rows 15-20 and we later find
	 * row 17 with a larger LSN, the range splits into 3 chunks, 15-16,
	 * 17, and 18-20.  If rows 15-20 were originally a single value (an
	 * RLE of 6), and that record is an overflow record, we end up with
	 * two chunks, both of which want to reference the same overflow value.
	 *
	 * Instead of the approach just described, we're first discarding any
	 * pages referencing non-existent overflow pages, then we're reviewing
	 * our key ranges and discarding any that overlap.  We're doing it that
	 * way for a few reasons: absent corruption, missing overflow items are
	 * strong arguments the page was replaced (on the other hand, some kind
	 * of file corruption is probably why we're here); it's a significant
	 * amount of additional complexity to simultaneously juggle overlapping
	 * ranges and missing overflow items; finally, real-world applications
	 * usually don't have a lot of overflow items, as WiredTiger supports
	 * very large page sizes, overflow items shouldn't be common.
	 *
	 * Step 4:
	 * Add unreferenced overflow page blocks to the free list so they are
	 * reused immediately.
	 */

	WT_ERR(__slvg_ovfl_reconcile(session, ss));
	WT_ERR(__slvg_ovfl_discard(session, ss));

	/*第5步:对pages list按KEY/LSN做排序*/
	qsort(ss->pages, (size_t)ss->pages_next, sizeof(WT_TRACK *), __slvg_trk_compare_key);
	if(ss->page_type == WT_PAGE_ROW_LEAF)
		WT_ERR(__slvg_row_range(session, ss));
	else
		WT_ERR(__slvg_col_range(session, ss));

	/*
	 * Step 6:
	 * We may have lost key ranges in column-store databases, that is, some
	 * part of the record number space is gone.   Look for missing ranges.
	 * 查询到所有column-store的missing ranges
	 */
	switch (ss->page_type) {
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_VAR:
		WT_ERR(__slvg_col_range_missing(session, ss));
		break;
	case WT_PAGE_ROW_LEAF:
		break;
	}

	/*
	 * Step 7:
	 * Build an internal page that references all of the leaf pages,
	 * and write it, as well as any merged pages, to the file.
	 *
	 * Count how many leaf pages we have (we could track this during the
	 * array shuffling/splitting, but that's a lot harder).
	 */
	for(leaf_cnt = i = 0; i < ss->pages_next; ++i){
		if (ss->pages[i] != NULL)
			++leaf_cnt;
	}
	if(leaf_cnt != 0){
		switch(ss->page_type){
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			WT_WITH_PAGE_INDEX(session, ret = __slvg_col_build_internal(session, leaf_cnt, ss));
			WT_ERR(ret);
			break;

		case WT_PAGE_ROW_LEAF:
			WT_WITH_PAGE_INDEX(session, ret = __slvg_row_build_internal(session, leaf_cnt, ss));
			WT_ERR(ret);
			break;
		}
	}

	/*
	 * Step 8:
	 * If we had to merge key ranges, we have to do a final pass through
	 * the leaf page array and discard file pages used during key merges.
	 * We can't do it earlier: if we free'd the leaf pages we're merging as
	 * we merged them, the write of subsequent leaf pages or the internal
	 * page might allocate those free'd file blocks, and if the salvage run
	 * subsequently fails, we'd have overwritten pages used to construct the
	 * final key range.  In other words, if the salvage run fails, we don't
	 * want to overwrite data the next salvage run might need.
	 */
	if(ss->merge_free)
		WT_ERR(__slvg_merge_block_free(session, ss));

	/*
	 * Step 9:
	 * Evict the newly created root page, creating a checkpoint.
	 */
	if(ss->root_ref.page != NULL){
		btree->ckpt = ckptbase;
		ret = __wt_evict(session, &ss->root_ref, 1);
		ss->root_ref.page = NULL;
		btree->ckpt = NULL;
	}

err:
	/*第十步：通告block manager修复btree操作结束*/
	WT_TRET(bm->salvage_end(bm, session));

	/* Discard any root page we created. */
	if (ss->root_ref.page != NULL)
		__wt_ref_out(session, &ss->root_ref);

	__wt_scr_free(session, &ss->tmp1);
	__wt_scr_free(session, &ss->tmp2);

	/* Wrap up reporting. */
	WT_TRET(__wt_progress(session, NULL, ss->fcnt));

	return ret;
}

/*读取指定文件中的数据，并构建一个WT_STUFF的page结构表*/
static int __slvg_read(WT_SESSION_IMPL* session, WT_STUFF* ss)
{
	WT_BM *bm;
	WT_DECL_ITEM(as);
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	const WT_PAGE_HEADER *dsk;
	size_t addr_size;
	uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];
	int eof, valid;

	bm = S2BT(session)->bm;
	WT_ERR(__wt_scr_alloc(session, 0, &as));
	WT_ERR(__wt_scr_alloc(session, 0, &buf));

	for(;;){
		/**/
		WT_ERR(bm->salvage_next(bm, session, addr, &addr_size, &eof));
		if(eof)
			break;

		/* Report progress every 10 chunks. */
		if (++ss->fcnt % 10 == 0)
			WT_ERR(__wt_progress(session, NULL, ss->fcnt));
		/*进行page读取,如果失败，立即返回*/
		ret = __wt_bt_read(session, buf, addr, addr_size);
		if(ret == 0)
			valid = 1;
		else{
			valid = 0;
			if(ret == WT_ERROR)
				ret = 0;

			WT_ERR(ret);
		}

		/*进行物理block校验*/
		WT_ERR(bm->salvage_valid(bm, session, addr, addr_size, valid));
		if (!valid)
			continue;

		/* Create a printable version of the address. */
		WT_ERR(bm->addr_string(bm, session, as, addr, addr_size));

		/*应该为leaf page或者overflow page,内部索引page不做salvage*/
		dsk = buf->data;
		switch(dsk->type){
		case WT_PAGE_BLOCK_MANAGER:
		case WT_PAGE_COL_INT:
		case WT_PAGE_ROW_INT:
			WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
				"%s page ignored %s",__wt_page_type_string(dsk->type), (const char *)as->data));
			continue;
		}

		/*
		 * Verify the page.  It's unlikely a page could have a valid
		 * checksum and still be broken, but paranoia is healthy in
		 * salvage.  Regardless, verify does return failure because
		 * it detects failures we'd expect to see in a corrupted file,
		 * like overflow references past the end of the file or
		 * overflow references to non-existent pages, might as well
		 * discard these pages now.
		 * 对btree pagge中的数据逻辑构建做校验
		 */
		if (__wt_verify_dsk(session, as->data, buf) != 0) {
			WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
			    "%s page failed verify %s", __wt_page_type_string(dsk->type), (const char *)as->data));
			WT_ERR(bm->free(bm, session, addr, addr_size));
			continue;
		}

		WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
			"tracking %s page, generation %" PRIu64 " %s",
			__wt_page_type_string(dsk->type), dsk->write_gen,
			(const char *)as->data));

		switch(dsk->type){
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
		case WT_PAGE_ROW_LEAF:
			if(ss->page_type == WT_PAGE_INVALID)
				ss->page_type = dsk->type;
			if(ss->page_type != dsk->type)
				WT_ERR_MSG(session, WT_ERROR,
				"file contains multiple file formats (both %s and %s), and cannot be salvaged",
				__wt_page_type_string(ss->page_type), __wt_page_type_string(dsk->type));

			WT_ERR(__slvg_trk_leaf(session, dsk, addr, addr_size, ss));
			break;

			/*overflow page的track*/
		case WT_PAGE_OVFL:
			WT_ERR(__slvg_trk_ovfl(session, dsk, addr, addr_size, ss));
			break;
		}
	}

err:
	__wt_scr_free(session, &as);
	__wt_scr_free(session, &buf);

	return ret;
}

/*为一个page构建一个响应的WT_TRACK对象*/
static int __slvg_trk_init(WT_SESSION_IMPL* session, uint8_t* addr, size_t addr_size, uint32_t size, 
			uint64_t gen, WT_STUFF* ss, WT_TRACK** retp)
{
	WT_DECL_RET;
	WT_TRACK *trk;

	WT_RET(__wt_calloc_one(session, &trk));
	WT_ERR(__wt_calloc_one(session, &trk->shared));
	trk->shared->ref = 1;
	trk->ss = ss;

	WT_ERR(__wt_strndup(session, addr, addr_size, &trk->trk_addr));
	trk->trk_addr_size = (uint8_t)addr_size;
	trk->trk_size = size;
	trk->trk_gen = gen;

	*retp = trk;
	return 0;
}

/*创建一个WT_TRACK对象，并且进行初始化赋值,这个对象和orig共享一个SS*/
static int __slvg_trk_split(WT_SESSION_IMPL* session, WT_TRACK* orig, WT_TRACK** newp)
{
	WT_TRACK* trk;

	WT_RET(__wt_calloc_one(session, &trk));
	trk->shared = orig->shared;
	trk->ss = orig->ss;

	++orig->shared->ref;

	*newp = trk;
	return 0;
}

/*通过page信息，构建一个page对应的track信息*/
static int __slvg_trk_leaf(WT_SESSION_IMPL* session, const WT_PAGE_HEADER* dsk, uint8_t* addr, size_t addr_size, WT_STUFF* ss)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_TRACK *trk;
	uint64_t stop_recno;
	uint32_t i;

	btree = S2BT(session);
	unpack = &_unpack;
	page = NULL;
	trk = NULL;

	/*调整pages数组的大小*/
	WT_RET(__wt_realloc_def(session, &ss->pages_allocated, ss->pages_next + 1, &ss->pages));
	/*为dsk对应的page分配一个WT_TRACK对象*/
	WT_RET(__slvg_trk_init(session, addr, addr_size, dsk->mem_size, dsk->write_gen, ss, &trk));

	switch(dsk->type){
	case WT_PAGE_COL_FIX:
		trk->col_start = dsk->recno;
		trk->col_stop = dsk->recno + (dsk->u.entries - 1);
		WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE, "%s records %" PRIu64 "-%" PRIu64,
			__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, ss->tmp1), trk->col_start, trk->col_stop));
		break;

	case WT_PAGE_COL_VAR:
		stop_recno = dsk->recno;
		/*计算最后的recno,对整个page的cell做walk*/
		WT_CELL_FOREACH(btree, dsk, cell, unpack, i){
			__wt_cell_unpack(cell, unpack);
			stop_recno += __wt_cell_rle(unpack);
		}

		trk->col_start = dsk->recno;
		trk->col_stop = stop_recno - 1;

		WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
			"%s records %" PRIu64 "-%" PRIu64,
			__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, ss->tmp1),
			trk->col_start, trk->col_stop));

		/* Column-store pages can contain overflow items. */
		WT_ERR(__slvg_trk_leaf_ovfl(session, dsk, trk));
		break;

	case WT_PAGE_ROW_LEAF:
		/*
		 * Row-store format: copy the first and last keys on the page.
		 * Keys are prefix-compressed, the simplest and slowest thing
		 * to do is instantiate the in-memory page, then instantiate
		 * and copy the full keys, then free the page.   We do this
		 * on every leaf page, and if you need to speed up the salvage,
		 * it's probably a great place to start.
		 */
		WT_ERR(__wt_page_inmem(session, NULL, dsk, 0, 0, &page));
		WT_ERR(__wt_row_leaf_key_copy(session, page, &page->pg_row_d[0], &trk->row_start)); /*第一行的key作为start key*/
		WT_ERR(__wt_row_leaf_key_copy(session, page, &page->pg_row_d[page->pg_row_entries - 1], &trk->row_stop)); /*最后一行的key作为stop key*/
		if (WT_VERBOSE_ISSET(session, WT_VERB_SALVAGE)) {
			WT_ERR(__wt_buf_set_printable(session, ss->tmp1, trk->row_start.data, trk->row_start.size));
			WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE, "%s start key %.*s",
				__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, ss->tmp2),
				(int)ss->tmp1->size, (char *)ss->tmp1->data));
			WT_ERR(__wt_buf_set_printable(session, ss->tmp1, trk->row_stop.data, trk->row_stop.size));
			WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE, "%s stop key %.*s",
				__wt_addr_string(session,trk->trk_addr, trk->trk_addr_size, ss->tmp2),
				(int)ss->tmp1->size, (char *)ss->tmp1->data));
		}

		/* Row-store pages can contain overflow items. */
		WT_ERR(__slvg_trk_leaf_ovfl(session, dsk, trk));
		break;
	}

	ss->pages[ss->pages_next++] = trk;

	if(0){
err:
		__wt_free(session, trk);
	}

	if(page != NULL){
		__wt_page_out(session, &page);
	}
	return ret;
}

/*为overflow page创建一个WT_TRACK对象*/
static int __slvg_trk_ovfl(WT_SESSION_IMPL* session, const WT_PAGE_HEADER* dsk, uint8_t* addr, size_t addr_size, WT_STUFF* ss)
{
	WT_TRACK* trk;

	WT_RET(__wt_realloc_def(session, &ss->ovfl_alloctated, ss->ovfl_next + 1, &ss->ovfl));

	WT_RET(__slvg_trk_init(session, addr, addr_size, dsk->mem_size, dsk->write_gen, ss, &trk));
	ss->ovfl[ss->ovfl_next++] = trk;

	return 0;
}

/*扫描整个page,把所有over flow item的block cookies addr记录到WT_TRACK中*/
static int __slvg_trk_leaf_ovfl(WT_SESSION_IMPL* session, const WT_PAGE_HEADER* dsk, WT_TRACK* trk)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	uint32_t i, ovfl_cnt;

	btree = S2BT(session);
	unpack = &_unpack;

	/*计算dsk对应的page存储overflow item的个数*/
	ovfl_cnt = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i){
		__wt_cell_unpack(cell, unpack);
		if (unpack->ovfl)
			++ovfl_cnt;
	}

	if(ovfl_cnt == 0)
		return 0;

	/*记录olverflow page的block cookies*/
	WT_RET(__wt_calloc_def(session, ovfl_cnt, &trk->trk_ovfl_addr));
	trk->trk_ovfl_cnt = ovfl_cnt;

	/*重新扫描这个page，记录overflow page的block cookies*/
	ovfl_cnt = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);
		if (unpack->ovfl) {
			WT_RET(__wt_strndup(session, unpack->data, unpack->size, &trk->trk_ovfl_addr[ovfl_cnt].addr));
			trk->trk_ovfl_addr[ovfl_cnt].size = (uint8_t)unpack->size;

			WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
				"%s overflow reference %s",
				__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1),
				__wt_addr_string(session,unpack->data, unpack->size, trk->ss->tmp2)));

			if (++ovfl_cnt == trk->trk_ovfl_cnt)
				break;
		}
	}

	return 0;
}

/*
 * __slvg_col_range --
 *	Figure out the leaf pages we need and free the leaf pages we don't.
 *
 * When pages split, the key range is split across multiple pages.  If not all
 * of the old versions of the page are overwritten, or not all of the new pages
 * are written, or some of the pages are corrupted, salvage will read different
 * pages with overlapping key ranges, at different LSNs.
 *
 * We salvage all of the key ranges we find, at the latest LSN value: this means
 * we may resurrect pages of deleted items, as page deletion doesn't write leaf
 * pages and salvage will read and instantiate the contents of an old version of
 * the deleted page.
 *
 * The leaf page array is sorted in key order, and secondarily on LSN: what this
 * means is that for each new key range, the first page we find is the best page
 * for that key.   The process is to walk forward from each page until we reach
 * a page with a starting key after the current page's stopping key.
 *
 * For each of page, check to see if they overlap the current page's key range.
 * If they do, resolve the overlap.  Because WiredTiger rarely splits pages,
 * overlap resolution usually means discarding a page because the key ranges
 * are the same, and one of the pages is simply an old version of the other.
 *
 * However, it's possible more complex resolution is necessary.  For example,
 * here's an improbably complex list of page ranges and LSNs:
 *
 *	Page	Range	LSN
 *	 30	 A-G	 3
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *	 35	 F-M	 8
 *	 36	 H-O	 9
 *
 * We walk forward from each page reviewing all other pages in the array that
 * overlap the range.  For each overlap, the current or the overlapping
 * page is updated so the page with the most recent information for any range
 * "owns" that range.  Here's an example for page 30.
 *
 * Review page 31: because page 31 has the range C-D and a higher LSN than page
 * 30, page 30 would "split" into two ranges, A-C and E-G, conceding the C-D
 * range to page 31.  The new track element would be inserted into array with
 * the following result:
 *
 *	Page	Range	LSN
 *	 30	 A-C	 3		<< Changed WT_TRACK element
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *	 30	 E-G	 3		<< New WT_TRACK element
 *	 35	 F-M	 8
 *	 36	 H-O	 9
 *
 * Continue the review of the first element, using its new values.
 *
 * Review page 32: because page 31 has the range B-C and a higher LSN than page
 * 30, page 30's A-C range would be truncated, conceding the B-C range to page
 * 32.
 *	 30	 A-B	 3
 *		 E-G	 3
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *
 * Review page 33: because page 33 has a starting key (C) past page 30's ending
 * key (B), we stop evaluating page 30's A-B range, as there can be no further
 * overlaps.
 *
 * This process is repeated for each page in the array.
 *
 * When page 33 is processed, we'd discover that page 33's C-F range overlaps
 * page 30's E-G range, and page 30's E-G range would be updated, conceding the
 * E-F range to page 33.
 *
 * This is not computationally expensive because we don't walk far forward in
 * the leaf array because it's sorted by starting key, and because WiredTiger
 * splits are rare, the chance of finding the kind of range overlap requiring
 * re-sorting the array is small.
 */

/*对SS中各个page重叠部分的调整，*/
static int __slvg_col_range(WT_SESSION_IMPL* session, WT_STUFF* ss)
{
	WT_TRACK *jtrk;
	uint32_t i, j;

	for(i = 0; i < ss->pages_next; ++i){
		if(ss->pages[i] == NULL)
			continue;

		/* Check for pages that overlap our page. */
		for (j = i + 1; j < ss->pages_next; ++j) {
			if (ss->pages[j] == NULL)
				continue;

			/*没有重叠部分，直接进行下一个page的重叠部分判断*/
			if(ss->pages[j]->col_start > ss->pages[i]->col_stop)
				break;

			/*解决重叠部分的归属问题*/
			jtrk = ss->pages[j];
			WT_RET(__slvg_col_range_overlap(session, i, j, ss));

			/*如果上面的__slvg_col_range_overlap调整了page的start key，可能发生了范围变化，那么需要判断前退一个page单元来做重新判断*/
			if (ss->pages[j] != NULL && jtrk != ss->pages[j])
				--j;
		}
	}

	return 0;
}

/*解决两个column page key ranges重叠的问题*/
static int __slvg_col_range_overlap(WT_SESSION_IMPL* session, uint32_t a_slot, uint32_t b_slot, WT_STUFF* ss)
{
	WT_TRACK *a_trk, *b_trk, *new;
	uint32_t i;

	a_trk = ss->pages[a_slot];
	b_trk = ss->pages[b_slot];

	WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
		"%s and %s range overlap",
		__wt_addr_string(session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
		__wt_addr_string(session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));

	/*
	 * The key ranges of two WT_TRACK pages in the array overlap -- choose
	 * the ranges we're going to take from each.
	 *
	 * We can think of the overlap possibilities as 11 different cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		pages are the same
	 * #2	BBBBBBBBBBBBB				overlaps the beginning
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #8			AAAAAAAAAAAAAAAA	same as #2
	 * #9		AAAAA				A is a prefix of B
	 * #10			AAAAAA			A is middle of B
	 * #11			AAAAAAAAAA		A is a suffix of B
	 *
	 * Note the leaf page array was sorted by key and a_trk appears earlier
	 * in the array than b_trk, so cases #2/8, #10 and #11 are impossible.
	 *
	 * Finally, there's one additional complicating factor -- final ranges
	 * are assigned based on the page's LSN.
	 */
	/* Case #2/8, #10, #11,这是不可能出现的，因为pages是按照KEY和LSN的重大到小排序的，不会出现b_trk->start 比a_trk->start小的情况 */
	if(a_trk->col_start > b_trk->col_start)
		WT_PANIC_RET(session, EINVAL, "unexpected merge array sort order");

	if(a_trk->col_start == b_trk->col_start){/* Case #1, #4 and #9 */
		/*
		 * The secondary sort of the leaf page array was the page's LSN,
		 * in high-to-low order, which means a_trk has a higher LSN, and
		 * is more desirable, than b_trk.  In cases #1 and #4 and #9,
		 * where the start of the range is the same for the two pages,
		 * this simplifies things, it guarantees a_trk has a higher LSN
		 * than b_trk.
		 */
		if(a_trk->col_stop >= b_trk->col_stop)
			goto delete_b;

		/*剩下一种情况是就A是B的前面一部分*/
		b_trk->col_start = a_trk->col_stop + 1;
		__slvg_col_trk_update_start(b_slot, ss);
		F_SET(b_trk, WT_TRACK_MERGE);
		goto merge;
	}

	if(a_trk->col_stop == b_trk->col_stop){ /*B是A的后面一部分*/
		if (a_trk->trk_gen > b_trk->trk_gen) /*A的版本号比B大，说明B是较早产生的，直接删除*/
			goto delete_b;

		/*如果B是后于A产生，那么A去掉与B重复的部分*/
		a_trk->col_stop = b_trk->col_start - 1;
		F_SET(a_trk, WT_TRACK_MERGE);
		goto merge;
	}

	/*B是A的子集合*/
	if(a_trk->trk_gen > b_trk->trk_gen){ /*删除B*/
		delete_b:	/*
		 * After page and overflow reconciliation, one (and only one)
		 * page can reference an overflow record.  But, if we split a
		 * page into multiple chunks, any of the chunks might own any
		 * of the backing overflow records, so overflow records won't
		 * normally be discarded until after the merge phase completes.
		 * (The merge phase is where the final pages are written, and
		 * we figure out which overflow records are actually used.)
		 * If freeing a chunk and there are no other references to the
		 * underlying shared information, the overflow records must be
		 * useless, discard them to keep the final file size small.
		 */
		if (b_trk->shared->ref == 1)
			for (i = 0; i < b_trk->trk_ovfl_cnt; ++i)
				WT_RET(__slvg_trk_free(session, &ss->ovfl[b_trk->trk_ovfl_slot[i]], 1));
		return (__slvg_trk_free(session, &ss->pages[b_slot], 1));
	}

	/*
	 * Case #5: b_trk is more desirable and is a middle chunk of a_trk.
	 * Split a_trk into two parts, the key range before b_trk and the
	 * key range after b_trk.
	 * 如果B是A的子集合，而且B有效，那就要对A做split操作
	 */
	WT_RET(__slvg_trk_split(session, a_trk, &new));

	/*将new ss插入在A之后*/
	WT_RET(__wt_realloc_def(session, &ss->pages_allocated, ss->pages_next + 1, &ss->pages));
	memmove(ss->pages + a_slot + 1, ss->pages + a_slot, (ss->pages_next - a_slot) * sizeof(*ss->pages));
	ss->pages[a_slot + 1] = new;
	++ss->pages_next;

	/*下一次会再次进行new和B的判断，那么就相当于没有重叠,这种情况就是A一分为3*/
	new->col_start = b_trk->col_stop + 1;
	new->col_stop = a_trk->col_stop;
	/*将new添加到ss->pages中*/
	__slvg_col_trk_update_start(a_slot + 1, ss);

	a_trk->col_stop = b_trk->col_start - 1;

	F_SET(new, WT_TRACK_MERGE);
	F_SET(a_trk, WT_TRACK_MERGE);

merge:
	WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
		"%s and %s require merge",
		__wt_addr_string(session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
		__wt_addr_string(session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));

	return 0;
}

static void __slvg_col_trk_update_start(uint32_t slot, WT_STUFF* ss)
{
	WT_TRACK *trk;
	uint32_t i;

	trk = ss->pages[slot];
	/*确定排序的末尾位置*/
	for (i = slot + 1; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;
		if (ss->pages[i]->col_start > trk->col_stop)
			break;
	}

	/*重新排序*/
	i -= slot;
	if(i > 1)
		qsort(ss->pages + slot, i, sizeof(WT_TRACK *), __slvg_trk_compare_key);
}

/*检查column store 文件中的missing ranges*/
static int __slvg_col_range_missing(WT_SESSION_IMPL *session, WT_STUFF *ss)
{
	WT_TRACK *trk;
	uint64_t r;
	uint32_t i;

	for(i = 0, r = 0; i < ss->pages_next; ++i){
		trk = ss->pages[i];
		if(trk == NULL)
			continue;

		/*中间有空隙？*/
		if(trk->col_start != r + 1){
			WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
				"%s column-store missing range from %"
				PRIu64 " to %" PRIu64 " inclusive",
				__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, ss->tmp1), r + 1, trk->col_start - 1));

			trk->col_missing = r + 1;
			F_SET(trk, WT_TRACK_MERGE);
		}

		r = trk->col_stop;
	}

	return 0;
}

/*初始化salvage page的modify信息,设置page为脏页*/
static int __slvg_modify_init(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_RET(__wt_page_modify_init(session, page));
	__wt_page_modify_set(session, page);

	return 0;
}

/*为column store的leaf page构建一个内存中的internal page*/
static int __slvg_col_build_internal(WT_SESSION_IMPL* session, uint32_t leaf_cnt, WT_STUFF* ss)
{
	WT_ADDR *addr;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *ref, **refp;
	WT_TRACK *trk;
	uint32_t i;

	addr = NULL;

	WT_RET(__wt_page_alloc(session, WT_PAGE_COL_INT, 1, leaf_cnt, 1, &page));
	WT_ERR(__slvg_modify_init(session, page));

	pindex = WT_INTL_INDEX_GET_SAFE(page);
	for(refp = pindex->index, i = 0; i < ss->pages_next; ++i){
		trk = ss->pages[i];
		if(trk = NULL)
			continue;

		/*设置ref的值*/
		ref = *refp++;
		ref->home = page;
		ref->page = NULL;

		WT_ERR(__wt_calloc_one(session, &addr));
		WT_ERR(__wt_strndup(session, trk->trk_addr, trk->trk_addr_size, &addr->addr));
		addr->size = trk->trk_addr_size;
		addr->type = trk->trk_ovfl_cnt == 0 ? WT_ADDR_LEAF_NO : WT_ADDR_LEAF;
		ref->addr = addr;
		addr = NULL;

		ref->key.recno = trk->col_start;
		ref->state = WT_REF_DISK;

		if(F_ISSET(trk, WT_TRACK_MERGE)){
			ss->merge_free = 1;
			WT_ERR(__slvg_col_build_leaf(session, trk, ref));
		}
		else
			WT_ERR(__slvg_ovfl_ref_all(session, trk));
		++ref;
	}

	__wt_root_ref_init(&ss->root_ref, page, 1);

	if(0){
err:
		if(addr != NULL)
			__wt_free(session, addr);
		__wt_page_out(session, &page);
	}
}

/*为merged page构建一个column store leaf page页*/
static int __slvg_col_build_leaf(WT_SESSION_IMPL* session, WT_TRACK* trk, WT_REF* ref)
{
	WT_COL *save_col_var;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SALVAGE_COOKIE *cookie, _cookie;
	uint64_t skip, take;
	uint32_t *entriesp, save_entries;

	cookie = &_cookie;
	WT_CLEAR(*cookie);

	/*将page在读入内存中*/
	WT_RET(__wt_page_in(session, ref, 0));
	page = ref->page;

	entriesp = page->type == WT_PAGE_COL_VAR ? &page->pg_var_entries : &page->pg_fix_entries;
	save_col_var = page->pg_var_d;
	save_entries = *entriesp;
	
	/*
	 * Calculate the number of K/V entries we are going to skip, and
	 * the total number of K/V entries we'll take from this page.
	 */
	cookie->skip = skip = trk->col_start - page->pg_var_recno;
	cookie->take = take = (trk->col_stop - trk->col_start) + 1;
	WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
		"%s merge discarding first %" PRIu64 " records, "
		"then taking %" PRIu64 " records",
		__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1), skip, take));

	if(page->type == WT_PAGE_COL_VAR && trk->trk_ovfl_cnt != 0)
		WT_ERR(__slvg_col_ovfl(session, trk, page, skip, take));

	/*中间没有空隙区，那么pg_var_recno应该和col_start相等*/
	if(trk->col_missing == 0)
		page->pg_var_recno = trk->col_start;
	else{
		page->pg_var_recno = trk->col_missing;
		cookie->missing = trk->col_start - trk->col_missing;
		WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
			"%s merge inserting %" PRIu64 " missing records",
			__wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1), cookie->missing));
	}
	ref->key.recno = page->pg_var_recno;

	__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
	__wt_free(session, ref->addr);
	ref->addr = NULL;

	/* Write the new version of the leaf page to disk. */
	WT_ERR(__slvg_modify_init(session, page));
	WT_ERR(__wt_reconcile(session, ref, cookie, WT_SKIP_UPDATE_ERR));

	page->pg_var_d = save_col_var;
	*entriesp = save_entries;

	/*完成了对应的salvg*/
	ret = __wt_page_release(session, ref, 0);
	if(ret == 0)
		ret = __wt_evict(session, ref, 1);

	if(0){
err:
		WT_TRET(__wt_page_release(session, ref, 0));
	}

	return ret;
}

/*对unpack对应的overflow addr进行校验,并标示关联ref关系*/
static int __slvg_col_ovfl_single(WT_SESSION_IMPL* session, WT_TRACK* trk, WT_CELL_UNPACK* unpack)
{
	WT_TRACK *ovfl;
	uint32_t i;

	for(i = 0; i < trk->trk_ovfl_cnt; ++i){
		ovfl = trk->ss->ovfl[trk->trk_ovfl_slot[i]];
		if (unpack->size == ovfl->trk_addr_size && memcmp(unpack->data, ovfl->trk_addr, unpack->size) == 0)
			return (__slvg_ovfl_ref(session, ovfl, 0));
	}

	WT_PANIC_RET(session, EINVAL, "overflow record at column-store page merge not found");
}

static int __slvg_col_ovfl(WT_SESSION_IMPL* session, WT_TRACK* trk, WT_PAGE* page, uint64_t skip, uint64_t take)
{
	WT_CELL_UNPACK unpack;
	WT_CELL *cell;
	WT_COL *cip;
	WT_DECL_RET;
	uint64_t recno, start, stop;
	uint32_t i;

	recno = page->pg_var_recno;
	start = recno + skip;
	stop = (recno + skip + take) - 1;

	WT_COL_FOREACH(page, cip, i){
		cell = WT_COL_PTR(page, cip);
		__wt_cell_unpack(cell, &unpack);
		recno += __wt_cell_rle(&unpack);

		/*
		 * I keep getting this calculation wrong, so here's the logic.
		 * Start is the first record we want, stop is the last record
		 * we want. The record number has already been incremented one
		 * past the maximum record number for this page entry, that is,
		 * it's set to the first record number for the next page entry.
		 * The test of start should be greater-than (not greater-than-
		 * or-equal), because of that increment, if the record number
		 * equals start, we want the next record, not this one.  The
		 * test against stop is greater-than, not greater-than-or-equal
		 * because stop is the last record wanted, if the record number
		 * equals stop, we want the next record.
		 */
		if (recno > start && unpack.type == WT_CELL_VALUE_OVFL) {
			ret = __slvg_col_ovfl_single(session, trk, &unpack);
			/*
			 * When handling overlapping ranges on variable-length
			 * column-store leaf pages, we split ranges without
			 * considering if we were splitting RLE units.  (See
			 * note at the beginning of this file for explanation
			 * of the overall process.) If the RLE unit was on-page,
			 * we can simply write it again. If the RLE unit was an
			 * overflow value that's already been used by another
			 * row (from some other page created by a range split),
			 * there's not much to do, this row can't reference an
			 * overflow record we don't have: delete the row.
			 */
			if (ret == EBUSY) {
				__wt_cell_type_reset(session, cell, WT_CELL_VALUE_OVFL, WT_CELL_DEL);
				ret = 0;
			}
			WT_RET(ret);
		}
		if(recno > stop)
			break;
	}

	return 0;
}

/*row store的ranges去重叠区操作*/
static int __slvg_row_range(WT_SESSION_IMPL* session, WT_STUFF* ss)
{
	WT_TRACK* jtrk;
	WT_BTREE* btree;
	uint32_t i, j;
	int cmp; 

	btree = S2BT(session);

	for(i = 0; i < ss->pages_next; ++i){
		if (ss->pages[i] == NULL)
			continue;

		for(j = i + 1; j < ss->pages_next; ++j){
			if (ss->pages[j] == NULL)
				continue;

			/*stop 一定是大于start*/
			WT_RET(__wt_compare(session, btree->collator, &ss->pages[j]->row_start, &ss->pages[i]->row_stop, &cmp));
			if(cmp > 0)
				break;

			/*去重叠操作*/
			jtrk = ss->pages[j];
			WT_RET(__slvg_row_range_overlap(session, i, j, ss));

			if (ss->pages[j] != NULL && jtrk != ss->pages[j])
				--j;
		}
	}

	return 0;
}

/*
 * __slvg_row_range_overlap --
 *	Two row-store key ranges overlap, deal with it.
 */
static int __slvg_row_range_overlap(WT_SESSION_IMPL *session, uint32_t a_slot, uint32_t b_slot, WT_STUFF *ss)
{
	WT_BTREE *btree;
	WT_TRACK *a_trk, *b_trk, *new;
	uint32_t i;
	int start_cmp, stop_cmp;

	/*
	 * DO NOT MODIFY THIS CODE WITHOUT REVIEWING THE CORRESPONDING ROW- OR
	 * COLUMN-STORE CODE: THEY ARE IDENTICAL OTHER THAN THE PAGES THAT ARE
	 * BEING HANDLED.
	 */
	btree = S2BT(session);

	a_trk = ss->pages[a_slot];
	b_trk = ss->pages[b_slot];

	WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
	    "%s and %s range overlap",
	    __wt_addr_string(session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __wt_addr_string(session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));

	/*
	 * The key ranges of two WT_TRACK pages in the array overlap -- choose
	 * the ranges we're going to take from each.
	 *
	 * We can think of the overlap possibilities as 11 different cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		pages are the same
	 * #2	BBBBBBBBBBBBB				overlaps the beginning
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #8			AAAAAAAAAAAAAAAA	same as #2
	 * #9		AAAAA				A is a prefix of B
	 * #10			AAAAAA			A is middle of B
	 * #11			AAAAAAAAAA		A is a suffix of B
	 *
	 * Note the leaf page array was sorted by key and a_trk appears earlier
	 * in the array than b_trk, so cases #2/8, #10 and #11 are impossible.
	 *
	 * Finally, there's one additional complicating factor -- final ranges
	 * are assigned based on the page's LSN.
	 */
#define	A_TRK_START	(&a_trk->row_start)
#define	A_TRK_STOP	(&a_trk->row_stop)
#define	B_TRK_START	(&b_trk->row_start)
#define	B_TRK_STOP	(&b_trk->row_stop)
#define	SLOT_START(i)	(&ss->pages[i]->row_start)
#define	__slvg_key_copy(session, dst, src)				\
	__wt_buf_set(session, dst, (src)->data, (src)->size)

	WT_RET(__wt_compare(session, btree->collator, A_TRK_START, B_TRK_START, &start_cmp));
	WT_RET(__wt_compare(session, btree->collator, A_TRK_STOP, B_TRK_STOP, &stop_cmp));

	if (start_cmp > 0)			/* Case #2/8, #10, #11 */
		WT_PANIC_RET(session, EINVAL, "unexpected merge array sort order");

	if (start_cmp == 0) {				/* Case #1, #4, #9 */
		/*
		 * The secondary sort of the leaf page array was the page's LSN,
		 * in high-to-low order, which means a_trk has a higher LSN, and
		 * is more desirable, than b_trk.  In cases #1 and #4 and #9,
		 * where the start of the range is the same for the two pages,
		 * this simplifies things, it guarantees a_trk has a higher LSN
		 * than b_trk.
		 */
		if (stop_cmp >= 0)
			/*
			 * Case #1, #4: a_trk is a superset of b_trk, and a_trk
			 * is more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #9: b_trk is a superset of a_trk, but a_trk is more
		 * desirable: keep both but delete a_trk's key range from
		 * b_trk.
		 */
		WT_RET(__slvg_row_trk_update_start(session, A_TRK_STOP, b_slot, ss));
		F_SET(b_trk, WT_TRACK_CHECK_START | WT_TRACK_MERGE);
		goto merge;
	}

	if (stop_cmp == 0) {				/* Case #6 */
		if (a_trk->trk_gen > b_trk->trk_gen)
			/*
			 * Case #6: a_trk is a superset of b_trk and a_trk is
			 * more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #6: a_trk is a superset of b_trk, but b_trk is more
		 * desirable: keep both but delete b_trk's key range from a_trk.
		 */
		WT_RET(__slvg_key_copy(session, A_TRK_STOP, B_TRK_START));
		F_SET(a_trk, WT_TRACK_CHECK_STOP | WT_TRACK_MERGE);
		goto merge;
	}

	if (stop_cmp < 0) {				/* Case #3/7 */
		if (a_trk->trk_gen > b_trk->trk_gen) {
			/*
			 * Case #3/7: a_trk is more desirable, delete a_trk's
			 * key range from b_trk;
			 */
			WT_RET(__slvg_row_trk_update_start(session, A_TRK_STOP, b_slot, ss));
			F_SET(b_trk, WT_TRACK_CHECK_START | WT_TRACK_MERGE);
		} else {
			/*
			 * Case #3/7: b_trk is more desirable, delete b_trk's
			 * key range from a_trk;
			 */
			WT_RET(__slvg_key_copy(session, A_TRK_STOP, B_TRK_START));
			F_SET(a_trk, WT_TRACK_CHECK_STOP | WT_TRACK_MERGE);
		}
		goto merge;
	}

	/*
	 * Case #5: a_trk is a superset of b_trk and a_trk is more desirable --
	 * discard b_trk.
	 */
	if (a_trk->trk_gen > b_trk->trk_gen) {
delete_b:	/*
		 * After page and overflow reconciliation, one (and only one)
		 * page can reference an overflow record.  But, if we split a
		 * page into multiple chunks, any of the chunks might own any
		 * of the backing overflow records, so overflow records won't
		 * normally be discarded until after the merge phase completes.
		 * (The merge phase is where the final pages are written, and
		 * we figure out which overflow records are actually used.)
		 * If freeing a chunk and there are no other references to the
		 * underlying shared information, the overflow records must be
		 * useless, discard them to keep the final file size small.
		 */
		if (b_trk->shared->ref == 1)
			for (i = 0; i < b_trk->trk_ovfl_cnt; ++i)
				WT_RET(__slvg_trk_free(session,&ss->ovfl[b_trk->trk_ovfl_slot[i]], 1));
		return (__slvg_trk_free(session, &ss->pages[b_slot], 1));
	}

	/*
	 * Case #5: b_trk is more desirable and is a middle chunk of a_trk.
	 * Split a_trk into two parts, the key range before b_trk and the
	 * key range after b_trk.
	 */
	WT_RET(__slvg_trk_split(session, a_trk, &new));

	/*
	 * Second, reallocate the array of pages if necessary, and then insert
	 * the new element into the array after the existing element (that's
	 * probably wrong, but we'll fix it up in a second).
	 */
	WT_RET(__wt_realloc_def(session, &ss->pages_allocated, ss->pages_next + 1, &ss->pages));
	memmove(ss->pages + a_slot + 1, ss->pages + a_slot, (ss->pages_next - a_slot) * sizeof(*ss->pages));
	ss->pages[a_slot + 1] = new;
	++ss->pages_next;

	/*
	 * Third, set its its stop key to be the stop key of the original chunk,
	 * and call __slvg_row_trk_update_start.   That function will both set
	 * the start key to be the first key after the stop key of the middle
	 * chunk (that's b_trk), and re-sort the WT_TRACK array as necessary to
	 * move our new entry into the right sorted location.
	 */
	WT_RET(__slvg_key_copy(session, &new->row_stop, A_TRK_STOP));
	WT_RET(__slvg_row_trk_update_start(session, B_TRK_STOP, a_slot + 1, ss));

	/*
	 * Fourth, set the original WT_TRACK information to reference only
	 * the initial key space in the page, that is, everything up to the
	 * starting key of the middle chunk (that's b_trk).
	 */
	WT_RET(__slvg_key_copy(session, A_TRK_STOP, B_TRK_START));
	F_SET(new, WT_TRACK_CHECK_START);
	F_SET(a_trk, WT_TRACK_CHECK_STOP);

	F_SET(new, WT_TRACK_MERGE);
	F_SET(a_trk, WT_TRACK_MERGE);

merge:	WT_RET(__wt_verbose(session, WT_VERB_SALVAGE,
	    "%s and %s require merge",
	    __wt_addr_string(session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __wt_addr_string(session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));
	return (0);
}

/*更改B集合的start key，并对ss->pages做跳帧后的排序*/
static int __slvg_row_trk_update_start(WT_SESSION_IMPL *session, WT_ITEM *stop, uint32_t slot, WT_STUFF *ss)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(dsk);
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_ROW *rip;
	WT_TRACK *trk;
	uint32_t i;
	int cmp, found;

	btree = S2BT(session);
	page = NULL;
	found = 0;

	trk = ss->pages[slot];

	/*
	 * If we deleted an initial piece of the WT_TRACK name space, it may no
	 * longer be in the right location.
	 *
	 * For example, imagine page #1 has the key range 30-50, it split, and
	 * we wrote page #2 with key range 30-40, and page #3 key range with
	 * 40-50, where pages #2 and #3 have larger LSNs than page #1.  When the
	 * key ranges were sorted, page #2 came first, then page #1 (because of
	 * their earlier start keys than page #3), and page #2 came before page
	 * #1 because of its LSN.  When we resolve the overlap between page #2
	 * and page #1, we truncate the initial key range of page #1, and it now
	 * sorts after page #3, because it has the same starting key of 40, and
	 * a lower LSN.
	 *
	 * First, update the WT_TRACK start key based on the specified stop key.
	 *
	 * Read and instantiate the WT_TRACK page (we don't have to verify the
	 * page, nor do we have to be quiet on error, we've already read this
	 * page successfully).
	 */
	WT_RET(__wt_scr_alloc(session, trk->trk_size, &dsk));
	WT_ERR(__wt_bt_read(session, dsk, trk->trk_addr, trk->trk_addr_size));
	WT_ERR(__wt_page_inmem(session, NULL, dsk->mem, 0, 0, &page));

	/*
	 * Walk the page, looking for a key sorting greater than the specified
	 * stop key -- that's our new start key.
	 */
	WT_ERR(__wt_scr_alloc(session, 0, &key));
	WT_ROW_FOREACH(page, rip, i) {
		WT_ERR(__wt_row_leaf_key(session, page, rip, key, 0));
		WT_ERR(__wt_compare(session, btree->collator, key, stop, &cmp));
		if (cmp > 0) {
			found = 1;
			break;
		}
	}

	/*
	 * We know that at least one key on the page sorts after the specified
	 * stop key, otherwise the page would have entirely overlapped and we
	 * would have discarded it, we wouldn't be here.  Therefore, this test
	 * is safe.  (But, it never hurts to check.)
	 */
	WT_ERR_TEST(!found, WT_ERROR);
	WT_ERR(__slvg_key_copy(session, &trk->row_start, key));

	/*
	 * We may need to re-sort some number of elements in the list.  Walk
	 * forward in the list until reaching an entry which cannot overlap
	 * the adjusted entry.  If it's more than a single slot, re-sort the
	 * entries.
	 */
	for (i = slot + 1; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;
		WT_ERR(__wt_compare(session, btree->collator, SLOT_START(i), &trk->row_stop, &cmp));
		if (cmp > 0)
			break;
	}

	i -= slot;
	if (i > 1)
		qsort(ss->pages + slot, (size_t)i, sizeof(WT_TRACK *), __slvg_trk_compare_key);

err:	if (page != NULL)
		__wt_page_out(session, &page);
	__wt_scr_free(session, &dsk);
	__wt_scr_free(session, &key);

	return ret;
}

/*
 * __slvg_row_build_internal --
 *	Build a row-store in-memory page that references all of the leaf
 *	pages we've found.
 */
static int __slvg_row_build_internal(WT_SESSION_IMPL *session, uint32_t leaf_cnt,  WT_STUFF *ss)
{
	WT_ADDR *addr;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *ref, **refp;
	WT_TRACK *trk;
	uint32_t i;

	addr = NULL;

	/* Allocate a row-store root (internal) page and fill it in.其实就是一个root page来管理ss中的pages */
	WT_RET(
	    __wt_page_alloc(session, WT_PAGE_ROW_INT, 0, leaf_cnt, 1, &page));
	WT_ERR(__slvg_modify_init(session, page));

	pindex = WT_INTL_INDEX_GET_SAFE(page);
	for (refp = pindex->index, i = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL)
			continue;

		ref = *refp++;
		ref->home = page;
		ref->page = NULL;

		WT_ERR(__wt_calloc_one(session, &addr));
		WT_ERR(__wt_strndup(session, trk->trk_addr, trk->trk_addr_size, &addr->addr));
		addr->size = trk->trk_addr_size;
		addr->type = trk->trk_ovfl_cnt == 0 ? WT_ADDR_LEAF_NO : WT_ADDR_LEAF;
		ref->addr = addr;
		addr = NULL;

		__wt_ref_key_clear(ref);
		ref->state = WT_REF_DISK;

		/*
		 * If the page's key range is unmodified from when we read it
		 * (in other words, we didn't merge part of this page with
		 * another page), we can use the page without change, and the
		 * only thing we need to do is mark all overflow records the
		 * page references as in-use.
		 *
		 * If we did merge with another page, we have to build a page
		 * reflecting the updated key range.  Note, that requires an
		 * additional pass to free the merge page's backing blocks.
		 */
		if (F_ISSET(trk, WT_TRACK_MERGE)) {
			ss->merge_free = 1;
			WT_ERR(__slvg_row_build_leaf(session, trk, ref, ss));
		}
		else {
			WT_ERR(__wt_row_ikey_incr(session, page, 0, trk->row_start.data, trk->row_start.size, ref));
			WT_ERR(__slvg_ovfl_ref_all(session, trk));
		}
		++ref;
	}

	__wt_root_ref_init(&ss->root_ref, page, 0);

	if (0) {
err:		
		if (addr != NULL) __wt_free(session, addr);
		__wt_page_out(session, &page);
	}
	return (ret);
}

/*
 * __slvg_row_build_leaf --
 *	Build a row-store leaf page for a merged page.
 */
static int __slvg_row_build_leaf(WT_SESSION_IMPL *session, WT_TRACK *trk, WT_REF *ref, WT_STUFF *ss)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_ROW *rip;
	WT_SALVAGE_COOKIE *cookie, _cookie;
	uint32_t i, skip_start, skip_stop;
	int cmp;

	btree = S2BT(session);
	page = NULL;

	cookie = &_cookie;
	WT_CLEAR(*cookie);

	/* Allocate temporary space in which to instantiate the keys. */
	WT_RET(__wt_scr_alloc(session, 0, &key));

	/* Get the original page, including the full in-memory setup. */
	WT_ERR(__wt_page_in(session, ref, 0));
	page = ref->page;

	/*
	 * Figure out how many page keys we want to take and how many we want
	 * to skip.
	 *
	 * If checking the starting range key, the key we're searching for will
	 * be equal to the starting range key.  This is because we figured out
	 * the true merged-page start key as part of discarding initial keys
	 * from the page (see the __slvg_row_range_overlap function, and its
	 * calls to __slvg_row_trk_update_start for more information).
	 *
	 * If checking the stopping range key, we want the keys on the page that
	 * are less-than the stopping range key.  This is because we copied a
	 * key from another page to define this page's stop range: that page is
	 * the page that owns the "equal to" range space.
	 */
	skip_start = skip_stop = 0;
	if (F_ISSET(trk, WT_TRACK_CHECK_START))
		WT_ROW_FOREACH(page, rip, i) {
			WT_ERR(__wt_row_leaf_key(session, page, rip, key, 0));

			/*
			 * >= is correct: see the comment above.
			 */
			WT_ERR(__wt_compare(session, btree->collator, key, &trk->row_start, &cmp));
			if (cmp >= 0)
				break;
			if (WT_VERBOSE_ISSET(session, WT_VERB_SALVAGE)) {
				WT_ERR(__wt_buf_set_printable(session, ss->tmp1, key->data, key->size));
				WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
				    "%s merge discarding leading key %.*s",
				    __wt_addr_string(session, trk->trk_addr, trk->trk_addr_size,ss->tmp2), (int)ss->tmp1->size, (char *)ss->tmp1->data));
			}
			++skip_start;
		}
	if (F_ISSET(trk, WT_TRACK_CHECK_STOP))
		WT_ROW_FOREACH_REVERSE(page, rip, i) {
			WT_ERR(__wt_row_leaf_key(session, page, rip, key, 0));

			/*
			 * < is correct: see the comment above.
			 */
			WT_ERR(__wt_compare(session, btree->collator, key, &trk->row_stop, &cmp));
			if (cmp < 0)
				break;
			if (WT_VERBOSE_ISSET(session, WT_VERB_SALVAGE)) {
				WT_ERR(__wt_buf_set_printable(session, ss->tmp1, key->data, key->size));
				WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE,
				    "%s merge discarding trailing key %.*s",
				    __wt_addr_string(session, trk->trk_addr, trk->trk_addr_size, ss->tmp2), 
					(int)ss->tmp1->size, (char *)ss->tmp1->data));
			}
			++skip_stop;
		}

	/* We should have selected some entries, but not the entire page. */
	WT_ASSERT(session, skip_start + skip_stop > 0 && skip_start + skip_stop < page->pg_row_entries);

	/*
	 * Take a copy of this page's first key to define the start of
	 * its range.  The key may require processing, otherwise, it's
	 * a copy from the page.
	 */
	rip = page->pg_row_d + skip_start;
	WT_ERR(__wt_row_leaf_key(session, page, rip, key, 0));
	WT_ERR(__wt_row_ikey_incr(session, ref->home, 0, key->data, key->size, ref));

	/* Set the referenced flag on overflow pages we're using. */
	if (trk->trk_ovfl_cnt != 0)
		WT_ERR(__slvg_row_ovfl(session, trk, page, skip_start, page->pg_row_entries - skip_stop));

	/*
	 * Change the page to reflect the correct record count: there is no
	 * need to copy anything on the page itself, the entries value limits
	 * the number of page items.
	 */
	page->pg_row_entries -= skip_stop;
	cookie->skip = skip_start;

	/*
	 * We can't discard the original blocks associated with this page now.
	 * (The problem is we don't want to overwrite any original information
	 * until the salvage run succeeds -- if we free the blocks now, the next
	 * merge page we write might allocate those blocks and overwrite them,
	 * and should the salvage run eventually fail, the original information
	 * would have been lost.)  Clear the reference addr so eviction doesn't
	 * free the underlying blocks.
	 */
	__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
	__wt_free(session, ref->addr);
	ref->addr = NULL;

	/* Write the new version of the leaf page to disk. */
	WT_ERR(__slvg_modify_init(session, page));
	WT_ERR(__wt_reconcile(session, ref, cookie, WT_SKIP_UPDATE_ERR));

	/* Reset the page. */
	page->pg_row_entries += skip_stop;

	/*
	 * Discard our hazard pointer and evict the page, updating the
	 * parent's reference.
	 */
	ret = __wt_page_release(session, ref, 0);
	if (ret == 0)
		ret = __wt_evict(session, ref, 1);

	if (0) {
err:		WT_TRET(__wt_page_release(session, ref, 0));
	}
	__wt_scr_free(session, &key);

	return (ret);
}

static int __slvg_row_ovfl_single(WT_SESSION_IMPL *session, WT_TRACK *trk, WT_CELL *cell)
{
	WT_CELL_UNPACK unpack;
	WT_TRACK *ovfl;
	uint32_t i;

	/* Unpack the cell, and check if it's an overflow record. */
	__wt_cell_unpack(cell, &unpack);
	if (unpack.type != WT_CELL_KEY_OVFL && unpack.type != WT_CELL_VALUE_OVFL)
		return (0);

	/*
	 * Search the list of overflow records for this page -- we should find
	 * exactly one match, and we mark it as referenced.
	 */
	for (i = 0; i < trk->trk_ovfl_cnt; ++i) {
		ovfl = trk->ss->ovfl[trk->trk_ovfl_slot[i]];
		if (unpack.size == ovfl->trk_addr_size && memcmp(unpack.data, ovfl->trk_addr, unpack.size) == 0)
			return (__slvg_ovfl_ref(session, ovfl, 1));
	}

	WT_PANIC_RET(session, EINVAL, "overflow record at row-store page merge not found");
}

static int __slvg_row_ovfl(WT_SESSION_IMPL *session, WT_TRACK *trk, WT_PAGE *page, uint32_t start, uint32_t stop)
{
	WT_CELL *cell;
	WT_ROW *rip;
	void *copy;

	/*
	 * We're merging a row-store page, and we took some number of records,
	 * figure out which (if any) overflow records we used.
	 */
	for (rip = page->pg_row_d + start; start < stop; ++start, ++rip) {
		copy = WT_ROW_KEY_COPY(rip);
		(void)__wt_row_leaf_key_info(page, copy, NULL, &cell, NULL, NULL);
		if (cell != NULL)
			WT_RET(__slvg_row_ovfl_single(session, trk, cell));

		cell = __wt_row_leaf_value_cell(page, rip, NULL);
		if (cell != NULL)
			WT_RET(__slvg_row_ovfl_single(session, trk, cell));
	}
	return 0;
}


