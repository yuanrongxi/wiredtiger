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

	*newp = ss;
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

	}
}
