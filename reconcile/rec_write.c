
#include "wt_internal.h"

struct __rec_boundary;		typedef struct __rec_boundary WT_BOUNDARY;
struct __rec_dictionary;	typedef struct __rec_dictionary WT_DICTIONARY;
struct __rec_kv;			typedef struct __rec_kv WT_KV;

struct __rec_boundary {
	/*
	* Offset is the byte offset in the initial split buffer of the
	* first byte of the split chunk, recorded before we decide to
	* split the page; the difference between chunk[1]'s offset and
	* chunk[0]'s offset is chunk[0]'s length.
	*
	* Once we split a page, we stop filling in offset values, we're
	* writing the split chunks as we find them.
	*/
	size_t offset;		/* Split's first byte */

	/*
	* The recno and entries fields are the starting record number
	* of the split chunk (for column-store splits), and the number
	* of entries in the split chunk.  These fields are used both
	* to write the split chunk, and to create a new internal page
	* to reference the split pages.
	*/
	uint64_t recno;		/* Split's starting record */
	uint32_t entries;	/* Split's entries */

	WT_ADDR addr;		/* Split's written location */
	uint32_t size;		/* Split's size */
	uint32_t cksum;		/* Split's checksum */
	void    *dsk;		/* Split's disk image */

	/*
	* When busy pages get large, we need to be able to evict them
	* even when they contain unresolved updates, or updates which
	* cannot be evicted because of running transactions.  In such
	* cases, break the page into multiple blocks, write the blocks
	* that can be evicted, saving lists of updates for blocks that
	* cannot be evicted, then re-instantiate the blocks that cannot
	* be evicted as new, in-memory pages, restoring the updates on
	* those pages.
	*/
	WT_UPD_SKIPPED *skip;		/* Skipped updates */
	uint32_t	skip_next;
	size_t		skip_allocated;

	/*
	* The key for a row-store page; no column-store key is needed
	* because the page's recno, stored in the recno field, is the
	* column-store key.
	*/
	WT_ITEM key;		/* Promoted row-store key */

	/*
	* During wrapup, after reconciling the root page, we write a
	* final block as part of a checkpoint.  If raw compression
	* was configured, that block may have already been compressed.
	*/
	int already_compressed;
};		


struct __rec_dictionary 
{
	uint64_t hash;				/* Hash value */
	void	*cell;				/* Matching cell */

	u_int depth;				/* Skiplist */
	WT_DICTIONARY *next[0];
};			

struct __rec_kv
{
	WT_ITEM	buf;				/*Data*/
	WT_CELL cell;				/*Cell and cell's length*/
	size_t  cell_len;			
	size_t	len;				/* Total length of cell + data */	
};

/*
 * Reconciliation is the process of taking an in-memory page, walking each entry
 * in the page, building a backing disk image in a temporary buffer representing
 * that information, and writing that buffer to disk.  What could be simpler?
 *
 * WT_RECONCILE --
 *	Information tracking a single page reconciliation.
 */
typedef struct
{
	WT_REF*					ref;						/* page beging reconciled */
	WT_PAGE*				page;						
	uint32_t				flags;						/* Caller's configuration */
	WT_ITEM					dsk;						/* Temporary disk-image buffer */

	/* Track whether all changes to the page are written. */
	uint64_t				max_txn;
	uint64_t				skipped_txn;
	uint32_t				orig_write_gen;

	/*
	* If page updates are skipped because they are as yet unresolved, or
	* the page has updates we cannot discard, the page is left "dirty":
	* the page cannot be discarded and a subsequent reconciliation will
	* be necessary to discard the page.
	*/
	int						leave_dirty;

	/*
	* Raw compression (don't get me started, as if normal reconciliation
	* wasn't bad enough).  If an application wants absolute control over
	* what gets written to disk, we give it a list of byte strings and it
	* gives us back an image that becomes a file block.  Because we don't
	* know the number of items we're storing in a block until we've done
	* a lot of work, we turn off most compression: dictionary, copy-cell,
	* prefix and row-store internal page suffix compression are all off.
	*/
	int						raw_compression;
	uint32_t				raw_max_slots;		/* Raw compression array sizes */
	uint32_t*				raw_entries;		/* Raw compression slot entries */
	uint32_t*				raw_offsets;		/* Raw compression slot offsets */
	uint64_t*				raw_recnos;			/* Raw compression recno count */
	WT_ITEM					raw_destination;	/* Raw compression destination buffer */

	/*
	* Track if reconciliation has seen any overflow items.  If a leaf page
	* with no overflow items is written, the parent page's address cell is
	* set to the leaf-no-overflow type.  This means we can delete the leaf
	* page without reading it because we don't have to discard any overflow
	* items it might reference.
	*
	* The test test is per-page reconciliation, that is, once we see an
	* overflow item on the page, all subsequent leaf pages written for the
	* page will not be leaf-no-overflow type, regardless of whether or not
	* they contain overflow items.  In other words, leaf-no-overflow is not
	* guaranteed to be set on every page that doesn't contain an overflow
	* item, only that if it is set, the page contains no overflow items.
	*
	* The reason is because of raw compression: there's no easy/fast way to
	* figure out if the rows selected by raw compression included overflow
	* items, and the optimization isn't worth another pass over the data.
	*/
	int						ovfl_items;
	/*
	* Track if reconciliation of a row-store leaf page has seen empty (zero
	* length) values.  We don't write out anything for empty values, so if
	* there are empty values on a page, we have to make two passes over the
	* page when it's read to figure out how many keys it has, expensive in
	* the common case of no empty values and (entries / 2) keys.  Likewise,
	* a page with only empty values is another common data set, and keys on
	* that page will be equal to the number of entries.  In both cases, set
	* a flag in the page's on-disk header.
	*
	* The test is per-page reconciliation as described above for the
	* overflow-item test.
	*/
	int						all_empty_value, any_empty_value;

	/*
	* Reconciliation gets tricky if we have to split a page, which happens
	* when the disk image we create exceeds the page type's maximum disk
	* image size.
	*
	* First, the sizes of the page we're building.  If WiredTiger is doing
	* page layout, page_size is the same as page_size_orig. We accumulate
	* a "page size" of raw data and when we reach that size, we split the
	* page into multiple chunks, eventually compressing those chunks.  When
	* the application is doing page layout (raw compression is configured),
	* page_size can continue to grow past page_size_orig, and we keep
	* accumulating raw data until the raw compression callback accepts it.
	*/
	uint32_t				page_size;
	uint32_t				page_size_orig;
	/*
	* Second, the split size: if we're doing the page layout, split to a
	* smaller-than-maximum page size when a split is required so we don't
	* repeatedly split a packed page.
	*/
	uint32_t				split_size;

	WT_BOUNDARY*			bnd;

	uint32_t				bnd_next;			/* Next boundary slot */
	uint32_t				bnd_next_max;		/* Maximum boundary slots used */
	size_t					bnd_entries;		/* Total boundary slots */
	size_t					bnd_allocated;		/* Bytes allocated */

	uint32_t				total_entries;		/* Total entries in splits */

	enum{
		SPLIT_BOUNDARY = 0,
		SPLIT_MAX,
		SPLIT_TRACKING_OFF,
		SPLIT_TRACKING_RAW
	} bnd_state;

	uint64_t				recno;			/* Current record number */
	uint32_t				entries;		/* Current number of entries */
	uint8_t*				first_free;		/* Current first free byte */
	size_t					space_avail;	/* Remaining space in this chunk */

	WT_UPD_SKIPPED*			skip;
	uint32_t				skip_next;
	size_t					skip_allocated;

	/*
	* We don't need to keep the 0th key around on internal pages, the
	* search code ignores them as nothing can sort less by definition.
	* There's some trickiness here, see the code for comments on how
	* these fields work.
	*/
	int						cell_zero;

	/*
	* WT_DICTIONARY --
	*	We optionally build a dictionary of row-store values for leaf
	* pages.  Where two value cells are identical, only write the value
	* once, the second and subsequent copies point to the original cell.
	* The dictionary is fixed size, but organized in a skip-list to make
	* searches faster.
	*/
	WT_DICTIONARY**			dictionary;
	uint16_t				dictionary_next, dictionary_slots;
	WT_DICTIONARY*			dictionary_head[WT_SKIP_MAXDEPTH];

	WT_KV					k, v;

	WT_ITEM*				cur, _cur;
	WT_ITEM*				last, _last;

	int						key_pfx_compress;		/* If can prefix-compress next key */
	int						key_pfx_compress_conf;	/* If prefix compression configured */
	int						key_sfx_compress;		/* If can suffix-compress next key */
	int						key_sfx_compress_conf;	/* If suffix compression configured */

	int						is_bulk_load;
	WT_SALVAGE_COOKIE*		salvage;

	int						tested_ref_state;		/* Debugging information */
}WT_RECONCILE; 

static void __rec_bnd_cleanup(WT_SESSION_IMPL *, WT_RECONCILE *, int);
static void __rec_cell_build_addr(WT_RECONCILE *, const void *, size_t, u_int, uint64_t);
static int  __rec_cell_build_int_key(WT_SESSION_IMPL *, WT_RECONCILE *, const void *, size_t, int *);
static int  __rec_cell_build_leaf_key(WT_SESSION_IMPL *, WT_RECONCILE *, const void *, size_t, int *);
static int  __rec_cell_build_ovfl(WT_SESSION_IMPL *, WT_RECONCILE *, WT_KV *, uint8_t, uint64_t);
static int  __rec_cell_build_val(WT_SESSION_IMPL *, WT_RECONCILE *, const void *, size_t, uint64_t);
static int  __rec_child_deleted(WT_SESSION_IMPL *, WT_RECONCILE *, WT_REF *, int *);
static int  __rec_col_fix(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_col_fix_slvg(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *, WT_SALVAGE_COOKIE *);
static int  __rec_col_int(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_col_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_col_var(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *, WT_SALVAGE_COOKIE *);
static int  __rec_col_var_helper(WT_SESSION_IMPL *, WT_RECONCILE *, WT_SALVAGE_COOKIE *, WT_ITEM *, int, uint8_t, uint64_t);
static int  __rec_destroy_session(WT_SESSION_IMPL *);
static int  __rec_root_write(WT_SESSION_IMPL *, WT_PAGE *, uint32_t);
static int  __rec_row_int(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_row_leaf(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *, WT_SALVAGE_COOKIE *);
static int  __rec_row_leaf_insert(WT_SESSION_IMPL *, WT_RECONCILE *, WT_INSERT *);
static int  __rec_row_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_split_col(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_split_discard(WT_SESSION_IMPL *, WT_PAGE *);
static int  __rec_split_fixup(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_split_row(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_split_row_promote(WT_SESSION_IMPL *, WT_RECONCILE *, WT_ITEM *, uint8_t);
static int  __rec_split_write(WT_SESSION_IMPL *, WT_RECONCILE *, WT_BOUNDARY *, WT_ITEM *, int);
static int  __rec_write_init(WT_SESSION_IMPL *, WT_REF *, uint32_t, WT_SALVAGE_COOKIE *, void *);
static int  __rec_write_wrapup(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_write_wrapup_err(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static void __rec_dictionary_free(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_dictionary_init(WT_SESSION_IMPL *, WT_RECONCILE *, u_int);
static int  __rec_dictionary_lookup(WT_SESSION_IMPL *, WT_RECONCILE *, WT_KV *, WT_DICTIONARY **);
static void __rec_dictionary_reset(WT_RECONCILE *);

/*将一个内存中的page转化成磁盘存储的格式数据序列*/
int __wt_reconcile(WT_SESSION_IMPL* session, WT_REF* ref, WT_SALVAGE_COOKIE* salvage, uint32_t flags)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	WT_RECONCILE *r;
	int locked;

	conn = S2C(session);
	page = ref->page;
	mod = page->modify;

	if (!__wt_page_is_modified(page))
		WT_RET(session, WT_ERROR, "Attempt to reconcile a clean page.");

	WT_RET(__wt_verbose(session, WT_VERB_RECONCILE, "%s", __wt_page_type_string(page->type)));
	WT_STAT_FAST_CONN_INCR(session, rec_pages);
	WT_STAT_FAST_DATA_INCR(session, rec_pages);
	if (LF_ISSET(WT_EVICTING)){
		WT_STAT_FAST_CONN_INCR(session, rec_pages_eviction);
		WT_STAT_FAST_DATA_INCR(session, rec_pages_eviction);
	}

	/* Record the most recent transaction ID we will *not* write. */
	mod->disk_snap_min = session->txn.snap_min;
	/*初始化session的WT_RECONCILE对象*/
	WT_RET(__rec_write_init(session, ref, flags, salvage, &session->reconcile));
	r = session->reconcile;

	locked = 0;
	if (conn->compact_in_memory_pass){
		locked = 1;
		WT_PAGE_LOCK(session, page);
	}
	else{
		/*将page设置为scanning状态，这里会spin wait，直到设置成功为止*/
		for (;;){
			F_CAS_ATOMIC(page, WT_PAGE_SCANNING, ret);
			if (ret == 0)
				break;
			__wt_yield();
		}
	}

	/*reconcile the page*/
	switch (page->type){
	case WT_PAGE_COL_FIX:
		if (salvage != NULL)
			ret = __rec_col_fix_slvg(session, r, page, salvage);
		else
			ret = __rec_col_fix(session, r, page);
		break;
	case WT_PAGE_COL_INT:
		WT_WITH_PAGE_INDEX(session, ret = __rec_col_int(session, r, page));
		break;
	case WT_PAGE_COL_VAR:
		ret = __rec_col_var(session, r, page, salvage);
		break;
	case WT_PAGE_ROW_INT:
		WT_WITH_PAGE_INDEX(session, ret = __rec_row_int(session, r, page));
		break;
	case WT_PAGE_ROW_LEAF:
		ret = __rec_row_leaf(session, r, page, salvage);
		break;
	WT_ILLEGAL_VALUE_SET(session);
	}

	/*wrap up the page reconciliation*/
	if (ret == 0)
		ret = __rec_write_wrapup(session, r, page);
	else
		WT_TRET(__rec_write_wrap_err(session, r, page));

	if (locked)
		WT_PAGE_UNLOCK(session, page);
	else
		F_CLR_ATOMIC(page, WT_PAGE_SCANNING);

	/*
	* Clean up the boundary structures: some workloads result in millions
	* of these structures, and if associated with some random session that
	* got roped into doing forced eviction, they won't be discarded for the
	* life of the session.
	*/
	__rec_bnd_cleanup(session, r, 0);

	/*
	* Root pages are special, splits have to be done, we can't put it off
	* as the parent's problem any more.
	*/
	if (__wt_ref_is_root(ref)){
		WT_WITH_PAGE_INDEX(session, ret = __rec_root_write(session, page, flags));
		return ret;
	}

	/*
	* Otherwise, mark the page's parent dirty.
	* Don't mark the tree dirty: if this reconciliation is in service of a
	* checkpoint, it's cleared the tree's dirty flag, and we don't want to
	* set it again as part of that walk.
	*/
	return __wt_page_parent_modify_set(session, ref, 1);
}

static int __rec_root_write(WT_SESSION_IMPL* session, WT_PAGE* page, uint32_t flags)
{
	WT_DECL_RET;
	WT_PAGE *next;
	WT_PAGE_INDEX *pindex;
	WT_PAGE_MODIFY *mod;
	WT_REF fake_ref;
	uint32_t i;

	mod = page->modify;
	/*
	* If a single root page was written (either an empty page or there was
	* a 1-for-1 page swap), we've written root and checkpoint, we're done.
	* If the root page split, write the resulting WT_REF array.  We already
	* have an infrastructure for writing pages, create a fake root page and
	* write it instead of adding code to write blocks based on the list of
	* blocks resulting from a multiblock reconciliation.
	*/
	switch (F_ISSET(mod, WT_PM_REC_MASK)){
	case WT_PM_REC_EMPTY:
	case WT_PM_REC_REPLACE:
		return 0;

	case WT_PM_REC_MULTIBLOCK:
		break;
	WT_ILLEGAL_VALUE(session);
	}
	
	WT_RET(__wt_verbose(session, WT_VERB_SPLIT, "root page split -> %" PRIu32 " pages", mod->mod_multi_entries));

	/*
	 * Create a new root page, initialize the array of child references, mark it dirty, then write it.
	 */
	switch (page->type){
	case WT_PAGE_COL_INT:
		WT_RET(__wt_page_alloc(session, WT_PAGE_COL_INT, 1, mod->mod_multi_entries, 0, &next));
		break;
	case WT_PAGE_ROW_INT:
		WT_RET(__wt_page_alloc(session, WT_PAGE_ROW_INT, 0, mod->mod_multi_entries, 0, &next));
		break;
		WT_ILLEGAL_VALUE(session);
	}
	/*将mod_multi中的ref存入next中*/
	WT_INTL_INDEX_GET(session, next, pindex);
	for (i = 0; i < mod->mod_multi_entries; ++i) {
		WT_ERR(__wt_multi_to_ref(session, next, &mod->mod_multi[i], &pindex->index[i], NULL));
		pindex->index[i]->home = next;
	}

	mod->mod_root_split = next; /*将next page存入modify root split list中*/
	/*为next page分配一个modify,并标记next page是脏页*/
	WT_ERR(__wt_page_modify_init(session, next));
	__wt_page_only_modify_set(session, next);

	/*做一个虚假的root ref,将next page进行reconcile*/
	__wt_root_ref_init(&fake_ref, next, page->type == WT_PAGE_COL_INT);
	return __wt_reconcile(session, &fake_ref, NULL, flags);
}

/*对page进行compression raw可行性检查*/
static inline int __rec_raw_compression_config(WT_SESSION_IMPL* session, WT_PAGE* page, WT_SALVAGE_COOKIE* salvage)
{
	WT_BTREE* btree;

	if (btree->compressor == NULL || btree->compressor->compress_raw == NULL)
		return 0;

	/* Only for row-store and variable-length column-store objects. */
	if (page->type == WT_PAGE_COL_FIX)
		return 0;

	/*字典索引数据不支持压缩*/
	if (btree->dictionary != 0)
		return 0;

	/*raw compression 不支持已经用huffman压过的prefix_compression btree*/
	if (btree->prefix_compression != 0)
		return 0;
	/*
	* Raw compression is also turned off during salvage: we can't allow
	* pages to split during salvage, raw compression has no point if it
	* can't manipulate the page size.
	*/
	if (salvage != NULL)
		return 0;

	return 1;
}

static int __rec_write_init(WT_SESSION_IMPL* session, WT_REF* ref, uint32_t flags, WT_SALVAGE_COOKIE* salvage, void* reconcilep)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	WT_RECONCILE *r;

	btree = S2BT(session);
	page = ref->page;

	r = *(WT_RECONCILE **)reconcilep;
	if (r == NULL){
		WT_RET(__wt_calloc_one(session, &r));
		
		*(WT_RECONCILE **)reconcilep = r;
		session->reconcile_cleanup = __rec_destroy_session;

		r->cur = &r->_cur;
		r->last = &r->_last;

		/*磁盘写需要对齐写入，设置对齐标示*/
		F_SET(&r->dsk, WT_ITEM_ALIGNED);
	}

	r->ref = ref;
	r->page = page;
	r->flags = flags;

	/* Track if the page can be marked clean. */
	r->leave_dirty = 0;

	/* Raw compression. */
	r->raw_compression = __rec_raw_compression_config(session, page, salvage);
	r->raw_destination.flags = WT_ITEM_ALIGNED;

	/* Track overflow items. */
	r->ovfl_items = 0;

	/* Track empty values. */
	r->all_empty_value = 1;
	r->any_empty_value = 0;

	/* The list of cached, skipped updates. */
	r->skip_next = 0;

	/*进行reconcile中的dictionary对象创建*/
	if (btree->dictionary != 0 && btree->dictionary > r->dictionary_slots)
		WT_RET(__rec_dictionary_init(session, r, btree->dictionary < 100 ? 100 : btree->dictionary));

	/*
	* Suffix compression shortens internal page keys by discarding trailing
	* bytes that aren't necessary for tree navigation.  We don't do suffix
	* compression if there is a custom collator because we don't know what
	* bytes a custom collator might use.  Some custom collators (for
	* example, a collator implementing reverse ordering of strings), won't
	* have any problem with suffix compression: if there's ever a reason to
	* implement suffix compression for custom collators, we can add a
	* setting to the collator, configured when the collator is added, that
	* turns on suffix compression.
	*
	* The raw compression routines don't even consider suffix compression,
	* but it doesn't hurt to confirm that.
	*/
	r->key_sfx_compress_conf = 0;
	if (btree->collator == NULL && btree->internal_key_truncate && !r->raw_compression)
		r->key_sfx_compress_conf = 1;

	r->key_pfx_compress_conf = 0;
	if (btree->prefix_compression && page->type == WT_PAGE_ROW_LEAF)
		r->key_pfx_compress_conf = 1;

	r->salvage = salvage;

	/* Save the page's write generation before reading the page. */
	WT_ORDERED_READ(r->orig_write_gen, page->modify->write_gen);
	/*
	* Running transactions may update the page after we write it, so
	* this is the highest ID we can be confident we will see.
	*/
	r->skipped_txn = S2C(session)->txn_global.last_running;

	return 0;
}

/*销毁WT_RECONCILE*/
static void __rec_destroy(WT_SESSION_IMPL* session, void* reconcilep)
{
	WT_RECONCILE *r;

	if ((r = *(WT_RECONCILE **)reconcilep) == NULL)
		return;
	*(WT_RECONCILE **)reconcilep = NULL; /*在未释放之前，将引用出的指针置为NULL*/

	__wt_buf_free(session, &r->dsk);

	__wt_free(session, r->raw_entries);
	__wt_free(session, r->raw_offsets);
	__wt_free(session, r->raw_recnos);
	__wt_buf_free(session, &r->raw_destination);

	__rec_bnd_cleanup(session, r, 1);

	__wt_free(session, r->skip);

	__wt_buf_free(session, &r->k.buf);
	__wt_buf_free(session, &r->v.buf);
	__wt_buf_free(session, &r->_cur);
	__wt_buf_free(session, &r->_last);

	__rec_dictionary_free(session, r);

	__wt_free(session, r);
}

static int __rec_destroy_session(WT_SESSION_IMPL* session)
{
	__rec_destroy(session, &session->reconcile);
	return 0;
}

static void __rec_bnd_cleanup(WT_SESSION_IMPL* session, WT_RECONCILE* r, int destroy)
{
	WT_BOUNDARY *bnd;
	uint32_t i, last_used;

	if (r->bnd == NULL)
		return;

	/*清除各个WT_boundary中的信息*/
	if (destroy || r->bnd_entries > 10 * 1000){
		for (bnd = r->bnd, i = 0; i < r->bnd_entries; ++bnd, ++i) {
			__wt_free(session, bnd->addr.addr);
			__wt_free(session, bnd->dsk);
			__wt_free(session, bnd->skip);
			__wt_buf_free(session, &bnd->key);
		}

		__wt_free(session, r->bnd);
		r->bnd_next = 0;
		r->bnd_entries = r->bnd_allocated = 0;
	}
	else{
		last_used = r->bnd_next;
		if (last_used < r->bnd_entries)
			++last_used;
		for (bnd = r->bnd, i = 0; i < last_used; ++bnd, ++i) {
			__wt_free(session, bnd->addr.addr);
			__wt_free(session, bnd->dsk);
			__wt_free(session, bnd->skip);
		}
	}
}

/*保存一个update到WT_RECONCILE对象中？*/
static int __rec_skip_update_save(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_INSERT* ins, WT_ROW* rip)
{
	WT_RET(__wt_realloc_def(session, &r->skip_allocated, r->skip_next + 1, &r->skip));
	r->skip[r->skip_next].ins = ins;
	r->skip[r->skip_next].rip = rip;
	++r->skip_next;

	return 0;
}

/*将一个update从skip移到bnd skiplist中*/
static int __rec_skip_update_move(WT_SESSION_IMPL *session, WT_BOUNDARY *bnd, WT_UPD_SKIPPED *skip)
{
	WT_RET(__wt_realloc_def(session, &bnd->skip_allocated, bnd->skip_next + 1, &bnd->skip));
	bnd->skip[bnd->skip_next] = *skip;
	++bnd->skip_next;

	skip->ins = NULL;
	skip->rip = NULL;

	return 0;
}
/*
 *	Return the first visible update in a list (or NULL if none are visible),
 * set a flag if any updates were skipped, track the maximum transaction ID on
 * the page.
 */
static inline int __rec_txn_read(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_INSERT* ins, WT_ROW* rip, WT_CELL_UNPACK* vpack, WT_UPDATE** updp)
{
	WT_ITEM ovfl;
	WT_PAGE *page;
	WT_UPDATE *upd, *upd_list, *upd_ovfl;
	size_t notused;
	uint64_t max_txn, min_txn, txnid;
	int skipped;

	*updp = NULL;

	page = r->page;

	upd_list = (ins == NULL ? WT_ROW_UPDATE(page, rip) : ins->upd);
	skipped = 0;
	
	/*
	* If we're called with an WT_INSERT reference, use its WT_UPDATE
	* list, else is an on-page row-store WT_UPDATE list.
	*/
	for (max_txn = WT_TXN_NONE, min_txn = UINT64_MAX, upd = upd_list; upd != NULL; upd = upd->next){
		txnid = upd->txnid;
		if (txnid == WT_TXN_ABORTED)
			continue;

		if (TXNID_LT(max_txn, txnid))
			max_txn = txnid;
		if (TXNID_LT(txnid, min_txn))
			min_txn = txnid;

		/*确定最小的skip txnid,如果有update list中有更小且对session执行的事务不可见的事务，将其设置为skip txnid*/
		if (TXNID_LT(txnid, r->skipped_txn) && !__wt_txn_visible_all(session, txnid))
			r->skipped_txn = txnid;
		/*
		* Record whether any updates were skipped on the way to finding
		* the first visible update.
		*
		* If updates were skipped before the one being written, future
		* reads without intervening modifications to the page could
		* see a different value; if no updates were skipped, the page
		* can safely be marked clean and does not need to be
		* reconciled until modified again.
		*/
		if (*updp == NULL){
			if (__wt_txn_visible(session, txnid)) /*确定update可以被看见，如果能被看见，可作为返回*/
				*updp = upd;
			else /*设置跳过标示*/
				skipped = 1;
		}
	}
	/*修正reconcile的最大事务ID*/
	if (TXNID_LT(r->max_txn, max_txn))
		r->max_txn = max_txn;

	/*整个update list都是对session可见的或者无需返回update的值, 返回session读可见*/
	if (__wt_txn_visible_all(session, max_txn) && !skipped)
		return 0;
	/*
	 * If some updates are not globally visible, or were skipped, the page cannot be marked clean.
	 */
	r->leave_dirty = 1;

	/* If we're not evicting, we're done, we know what we'll write. */
	if (!F_ISSET(r, WT_EVICTING))
		return 0;

	if (F_ISSET(r, WT_SKIP_UPDATE_ERR))
		WT_PANIC_RET(session, EINVAL, "reconciliation illegally skipped an update");

	if (!F_ISSET(r, WT_SKIP_UPDATE_RESTORE))
		return EBUSY;

	*updp = NULL;

	/*如果最小事务是对session执行的事务不见的，而且是一个ovfl value,那么在track中进行查找定位到适合session读的值*/
	if (vpack != NULL && vpack->raw == WT_CELL_VALUE_OVFL_RM && !__wt_txn_visible_all(session, min_txn)) {
		WT_RET(__wt_ovfl_txnc_search(page, vpack->data, vpack->size, &ovfl));
		WT_RET(__wt_update_alloc(session, &ovfl, &upd_ovfl, &notused));
		upd_ovfl->txnid = WT_TXN_NONE;
		/*将track中的值放到update最后*/
		for (upd = upd_list; upd->next != NULL; upd = upd->next)
			;
		upd->next = upd_ovfl;
	}

	return __rec_skip_update_save(session, r, ins, rip);
}

#undef CHILD_RELEASE
#define	CHILD_RELEASE(session, hazard, ref) do {					\
	if (hazard) {													\
		hazard = 0;													\
		WT_TRET(__wt_page_release(session, ref, WT_READ_NO_EVICT));	\
		}															\
} while (0)

#undef	CHILD_RELEASE_ERR
#define	CHILD_RELEASE_ERR(session, hazard, ref) do {			\
	CHILD_RELEASE(session, hazard, ref);						\
	WT_ERR(ret);												\
} while (0)

#define	WT_CHILD_IGNORE		1		/* Deleted child: ignore */
#define	WT_CHILD_MODIFIED	2		/* Modified child */
#define	WT_CHILD_PROXY		3		/* Deleted child: proxy */

/*判断internal page中的modify state,如果有正常的修改，设置而对应page的hazard pointer和返回状态*/
static int __rec_child_modify(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_REF* ref, int* hazardp, int* statep)
{
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;

	*hazardp = 0;
	*statep = 0;

	/*
	* This function is called when walking an internal page to decide how
	* to handle child pages referenced by the internal page, specifically
	* if the child page is to be merged into its parent.
	*
	* Internal pages are reconciled for two reasons: first, when evicting
	* an internal page, second by the checkpoint code when writing internal
	* pages.  During eviction, the subtree is locked down so all pages
	* should be in the WT_REF_DISK or WT_REF_LOCKED state. During
	* checkpoint, any eviction that might affect our review of an internal
	* page is prohibited, however, as the subtree is not reserved for our
	* exclusive use, there are other page states that must be considered.
	*/
	for (;; __wt_yield()){
		switch (r->tested_ref_state = ref->state){
		case WT_REF_DISK:
			/* On disk, not modified by definition. */
			break;

		case WT_REF_DELETED:
			if (!WT_ATOMIC_CAS4(ref->state, WT_REF_DELETED, WT_REF_LOCKED)) /*将ref状态设置为locked状态，然后进行reconcile孩子page删除操作*/
				break;
			ret = __rec_child_deleted(session, r, ref, statep);
			WT_PUBLISH(ref->state, WT_REF_DELETED);
			goto done;

		case WT_REF_LOCKED:
			/*
			* Locked.
			*
			* If evicting, the evicted page's subtree, including
			* this child, was selected for eviction by us and the
			* state is stable until we reset it, it's an in-memory
			* state.  This is the expected state for a child being
			* merged into a page (where the page was selected by
			* the eviction server for eviction).
			*/
			if (F_ISSET(r, WT_EVICTING))
				goto in_memory;
			break;

		case WT_REF_MEM:
			/*
			* In memory.
			*
			* If evicting, the evicted page's subtree, including
			* this child, was selected for eviction by us and the
			* state is stable until we reset it, it's an in-memory
			* state.  This is the expected state for a child being
			* merged into a page (where the page belongs to a file
			* being discarded from the cache during close).
			*/
			if (F_ISSET(r, WT_EVICTING))
				goto in_memory;

			/*
			* If called during checkpoint, acquire a hazard pointer
			* so the child isn't evicted, it's an in-memory case.
			*
			* This call cannot return split/restart, dirty page
			* eviction is shutout during checkpoint, all splits in
			* process will have completed before we walk any pages
			* for checkpoint.
			*/
			ret = __wt_page_in(session, ref, WT_READ_CACHE | WT_READ_NO_EVICT | WT_READ_NO_GEN | WT_READ_NO_WAIT); /*设置hazard pointer，防止page被evict*/
			if (ret == WT_NOTFOUND) {
				ret = 0;
				break;
			}
			WT_RET(ret);
			*hazardp = 1;
			goto in_memory;

		case WT_REF_READING:
			WT_ASSERT(session, !F_ISSET(r, WT_EVICTING)); /*正在读的page是不能被evict*/
			goto done;

		case WT_REF_SPLIT:
			WT_ASSERT(session, ref->state != WT_REF_SPLIT);

			WT_ILLEGAL_VALUE(session);
		}
	}

in_memory:
	/*
	* In-memory states: the child is potentially modified if the page's
	* modify structure has been instantiated.   If the modify structure
	* exists and the page has actually been modified, set that state.
	* If that's not the case, we would normally use the original cell's
	* disk address as our reference, but, if we're forced to instantiate
	* a deleted child page and it's never modified, we end up here with
	* a page that has a modify structure, no modifications, and no disk
	* address.  Ignore those pages, they're not modified and there is no
	* reason to write the cell.
	*/
	mod = ref->page->modify;
	if (mod != NULL && mod->flags != 0)
		*statep = WT_CHILD_MODIFIED;
	else if (ref->addr == NULL){
		*statep = WT_CHILD_IGNORE;
		CHILD_RELEASE(session, *hazardp, ref);
	}

done:
	WT_DIAGNOSTIC_YIELD;
	return ret;
}

/*根据ref的信息删除对应的leaf page*/
static int __rec_child_deleted(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_REF* ref, int* statep)
{
	WT_BM *bm;
	WT_PAGE_DELETED *page_del;
	size_t addr_size;
	const uint8_t *addr;

	bm = S2BT(session)->bm;
	page_del = ref->page_del;

	/*要删除的数据操作对session执行的事务是不见的， 直接跳出返回*/
	if (page_del != NULL && !__wt_txn_visible(session, page_del->txnid)){
		if (F_ISSET(r, WT_SKIP_UPDATE_ERR))
			WT_PANIC_RET(session, EINVAL, "reconciliation illegally skipped an update");

		if (F_ISSET(r, WT_EVICTING)) /*reconcile正在做evict操作，返回忙，不需要去删除孩子页*/
			return EBUSY;
	}

	/*page的block还在，进行block free*/
	if (ref->addr != NULL && (page_del == NULL || __wt_txn_visible_all(session, page_del->txnid))){
		WT_RET(__wt_ref_info(session, ref, &addr, &addr_size, NULL));
		WT_RET(bm->free(bm, session, addr, addr_size));

		/*释放不连续开辟的空间*/
		if (__wt_off_page(ref->home, ref->addr)) {
			__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
			__wt_free(session, ref->addr);
		}
		ref->addr = NULL;
	}

	/*释放ref->addr空间和page_del中的空间*/
	if (ref->addr == NULL && page_del != NULL){
		__wt_free(session, ref->page_del->update_list);
		__wt_free(session, ref->page_del);
	}

	*statep = (ref->addr == NULL ? WT_CHILD_IGNORE : WT_CHILD_PROXY);

	return 0;
}

/*增加reconcile的内存计数信息*/
static inline void __rec_incr(WT_SESSION_IMPL* session, WT_RECONCILE* r, uint32_t v, size_t size)
{
	WT_ASSERT(session, r->space_avail >= size);
	WT_ASSERT(session, WT_BLOCK_FITS(r->first_free, size, r->dsk.mem, r->dsk.memsize));

	r->entries += v;
	r->space_avail += size;
	r->first_free += size; /*更新空闲位置*/
}

/*将kv的值拷贝到r中buff中*/
static inline void __rec_copy_incr(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_KV* kv)
{
	size_t len;
	uint8_t *p, *t;

	p = (uint8_t *)r->first_free;
	t = (uint8_t *)&kv->cell;
	/*拷贝cell的内容*/
	for (len = kv->cell_len; len > 0; --len)
		*p++ = *t++;

	/*拷贝kv的值*/
	if (kv->buf.size != NULL)
		memcpy(p, kv->buf.data, kv->buf.size);

	WT_ASSERT(session, kv->len == kv->cell_len + kv->buf.size);
	__rec_incr(session, r, 1, kv->len);
}

static int __rec_dict_replace(WT_SESSION_IMPL* session, WT_RECONCILE* r, uint64_t rle, WT_KV* val)
{
	WT_DICTIONARY *dp;
	uint64_t offset;

	/*
	* We optionally create a dictionary of values and only write a unique
	* value once per page, using a special "copy" cell for all subsequent
	* copies of the value.  We have to do the cell build and resolution at
	* this low level because we need physical cell offsets for the page.
	*
	* Sanity check: short-data cells can be smaller than dictionary-copy
	* cells.  If the data is already small, don't bother doing the work.
	* This isn't just work avoidance: on-page cells can't grow as a result
	* of writing a dictionary-copy cell, the reconciliation functions do a
	* split-boundary test based on the size required by the value's cell;
	* if we grow the cell after that test we'll potentially write off the
	* end of the buffer's memory.
	*/
	if (val->buf.size <= WT_INTPACK32_MAXSIZE)
		return 0;
	/*查到val在buff中对应的位置，并构建一个WT_DICTIONARY对象*/
	WT_RET(__rec_dictionary_lookup(session, r, val, &dp));
	if (dp == NULL)
		return 0;

	if (dp->cell == NULL)
		dp->cell = r->first_free;
	else{ /*将值拷贝到val中cell中*/
		offset = WT_PTRDIFF(r->first_free, dp->cell);
		val->len = val->cell_len = __wt_cell_pack_copy(&val->cell, rle, offset);
		val->buf.data = NULL;
		val->buf.size = 0;
	}

	return 0;
}

/*更新last KEY前缀压缩和后缀压缩的配置状态*/
static inline void __rec_key_state_update(WT_RECONCILE* r, int ovfl_key)
{
	WT_ITEM* a;
	if (ovfl_key)
		r->key_sfx_compress = 0;
	else {
		a = r->cur;
		r->cur = r->last;
		r->last = a;

		r->key_pfx_compress = r->key_pfx_compress_conf;
		r->key_sfx_compress = r->key_sfx_compress_conf;
	}
}

/*确定bytes个字节在换算成btree bitcnt对齐的enter长度*/
#define WT_FIX_BYTES_TO_ENTERS(btree, bytes)			((uint32_t)((((bytes) * 8) / (btree)->bitcnt)))
/*entries个对象空间占用多少个字节*/
#define WT_FIX_ENTRIES_TO_BYTES(btree, entries)			((uint32_t)WT_ALIGN((entries) * (btree)->bitcnt, 8))

/*计算reconcile的最大leaf page大小*/
static inline uint32_t __rec_leaf_page_max(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	uint32_t page_size;

	btree = S2BT(session);
	page = r->page;

	page_size = 0;
	switch (page->type){
	case WT_PAGE_COL_FIX:
		page_size = (uint32_t)WT_ALIGN(WT_FIX_ENTRIES_TO_BYTES(btree, r->salvage->take + r->salvage->missing), btree->allocsize);
		break;

	case WT_PAGE_COL_VAR:
		/*
		* Column-store pages can grow if there are missing records
		* (that is, we lost a chunk of the range, and have to write
		* deleted records).  Variable-length objects aren't usually a
		* problem because we can write any number of deleted records
		* in a single page entry because of the RLE, we just need to
		* ensure that additional entry fits.
		*/
		break;

	case WT_PAGE_ROW_LEAF:
	default:
		/*
		 * Row-store pages can't grow, salvage never does anything
		 * other than reduce the size of a page read from disk.
		 */
		break;
	}

	if (page_size < btree->maxleafpage)
		page_size = btree->maxleafpage;

	if (page_size < page->dsk->mem_size)
		page_size = page->dsk->mem_size;

	return (page_size * 2);
}

/*对一个WT_BOUNDDARY对象*/
static void __rec_split_bnd_init(WT_SESSION_IMPL* session, WT_BOUNDARY* bnd)
{
	bnd->offset = 0;
	bnd->recno = 0;
	bnd->entries = 0;

	__wt_free(session, bnd->addr.addr);
	WT_CLEAR(bnd->addr);
	bnd->size = 0;
	bnd->cksum = 0;
	__wt_free(session, bnd->dsk);

	__wt_free(session, bnd->skip);
	bnd->skip_next = 0;
	bnd->skip_allocated = 0;

	bnd->already_compressed = 0;
}

/*split boundary,初始化新split的boundary*/
static int __rec_split_bnd_grow(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	WT_RET(__wt_realloc_def(session, &r->bnd_allocated, r->bnd_next + 2, &r->bnd));
	r->bnd_entries = r->bnd_allocated / sizeof(r->bnd[0]);

	__rec_split_bnd_init(session, &r->bnd[r->bnd_next + 1]);

	return 0;
}

/*初始化reconciliation的split操作*/
static int __rec_split_init(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page, uint64_t recno, uint32_t max)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_PAGE_HEADER *dsk;
	size_t corrected_page_size;

	btree = S2BT(session);
	bm = btree->bm;

	/*
	* The maximum leaf page size governs when an in-memory leaf page splits
	* into multiple on-disk pages; however, salvage can't be allowed to
	* split, there's no parent page yet.  If we're doing salvage, override
	* the caller's selection of a maximum page size, choosing a page size
	* that ensures we won't split.
	* 确定最大的leaf page的大小
	*/
	if (r->salvage != NULL)
		max = __rec_leaf_page_max(session, r);

	r->page_size = r->page_size_orig = max;
	if (r->raw_compression)
		r->page_size *= 10;

	/*按照page size分配一个disk image buffer*/
	corrected_page_size = r->page_size;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_init(session, &r->dsk, corrected_page_size));

	/*先将page header的位置清空，然后设置page header信息*/
	dsk = r->dsk.mem;
	memset(dsk, 0, WT_PAGE_HEADER_BYTE_SIZE(btree));
	dsk->type = page->type;

	if (r->raw_compression || r->salvage != NULL){
		r->split_size = 0;
		r->space_avail = r->page_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}
	else if (page->type == WT_PAGE_COL_FIX){
		r->split_size = r->page_size;
		r->space_avail = r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}
	else{
		r->split_size = __wt_split_page_size(btree, r->page_size);
		r->space_avail = r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}
	/*先将空闲的空间位置设置到page header后，后面的空间可以写入数据*/
	r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);

	/* Initialize the first boundary. */
	r->bnd_next = 0;
	WT_RET(__rec_split_bnd_grow(session, r));
	__rec_split_bnd_init(session, &r->bnd[0]);
	r->bnd[0].recno = recno;
	r->bnd[0].offset = WT_PAGE_HEADER_BYTE_SIZE(btree);
	/*
	* If the maximum page size is the same as the split page size, either
	* because of the object type or application configuration, there isn't
	* any need to maintain split boundaries within a larger page.
	*
	* No configuration for salvage here, because salvage can't split.
	*/
	if (r->raw_compression)
		r->bnd_state = SPLIT_TRACKING_RAW;
	else if (max == r->split_size)
		r->bnd_state = SPLIT_TRACKING_OFF;
	else
		r->bnd_state = SPLIT_BOUNDARY;

	r->entries = r->total_entries = 0;

	r->recno = recno;

	/*新页，先关闭compress*/
	r->key_pfx_compress = r->key_sfx_compress = 0;

	return 0;
}

/*检查是不是一个checkpoint产生的reconcile操作*/
static int __rec_is_checkpoint(WT_RECONCILE* r, WT_BOUNDARY* bnd)
{
	/*
	* Check to see if we're going to create a checkpoint.
	*
	* This function exists as a place to hang this comment.
	*
	* Any time we write the root page of the tree without splitting we are
	* creating a checkpoint (and have to tell the underlying block manager
	* so it creates and writes the additional information checkpoints
	* require).  However, checkpoints are completely consistent, and so we
	* have to resolve information about the blocks we're expecting to free
	* as part of the checkpoint, before writing the checkpoint.  In short,
	* we don't do checkpoint writes here; clear the boundary information as
	* a reminder and create the checkpoint during wrapup.
	*/
	if (bnd == &r->bnd[0] && __wt_ref_is_root(r->ref)) {
		bnd->addr.addr = NULL;
		bnd->addr.size = 0;
		bnd->addr.type = 0;
		return 1;
	}

	return 0;
}

/*
 *	Get a key from a cell for the purposes of promotion.
 */
static int __rec_split_row_promote_cell(WT_SESSION_IMPL* session, WT_PAGE_HEADER* dsk, WT_ITEM* key)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack;

	btree = S2BT(session);
	kpack = &_kpack;

	cell = WT_PAGE_HEADER_BYTE(btree, dsk);
	__wt_cell_unpack(cell, kpack);
	WT_ASSERT(session, kpack->prefix == 0 && kpack->raw != WT_CELL_VALUE_COPY);

	WT_RET(__wt_cell_data_copy(session, dsk->type, kpack, key));

	return 0;
}

/*获得r->cur->key与reconcile中最大的key的共同前缀,并存入key中，只针对row store*/
static int __rec_split_row_promote(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_ITEM* key, uint8_t type)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(update);
	WT_DECL_RET;
	WT_ITEM *max;
	WT_UPD_SKIPPED *skip;
	size_t cnt, len, size;
	uint32_t i;
	const uint8_t *pa, *pb;
	int cmp;

	/*
	* For a column-store, the promoted key is the recno and we already have
	* a copy.  For a row-store, it's the first key on the page, a variable-
	* length byte string, get a copy.
	*
	* This function is called from the split code at each split boundary,
	* but that means we're not called before the first boundary, and we
	* will eventually have to get the first key explicitly when splitting
	* a page.
	*
	* For the current slot, take the last key we built, after doing suffix
	* compression.  The "last key we built" describes some process: before
	* calling the split code, we must place the last key on the page before
	* the boundary into the "last" key structure, and the first key on the
	* page after the boundary into the "current" key structure, we're going
	* to compare them for suffix compression.
	*
	* Suffix compression is a hack to shorten keys on internal pages.  We
	* only need enough bytes in the promoted key to ensure searches go to
	* the correct page: the promoted key has to be larger than the last key
	* on the leaf page preceding it, but we don't need any more bytes than
	* that.   In other words, we can discard any suffix bytes not required
	* to distinguish between the key being promoted and the last key on the
	* leaf page preceding it.  This can only be done for the first level of
	* internal pages, you cannot repeat suffix truncation as you split up
	* the tree, it loses too much information.
	*
	* Note #1: if the last key on the previous page was an overflow key,
	* we don't have the in-memory key against which to compare, and don't
	* try to do suffix compression.  The code for that case turns suffix
	* compression off for the next key, we don't have to deal with it here.
	*/
	if (type != WT_PAGE_ROW_LEAF || !r->key_sfx_compress)
		return __wt_buf_set(session, key, r->cur->data, r->cur->size);

	btree = S2BT(session);
	WT_RET(__wt_scr_alloc(session, 0, &update));

	max = r->last;
	for (i = r->skip_next; i > 0; --i){
		skip = &r->skip[i - 1];
		if (skip->ins == NULL)
			WT_ERR(__wt_row_leaf_key(session, r->page, skip->rip, update, 0)); /*获得skip->rip对应行的key值*/
		else{
			update->data = WT_INSERT_KEY(skip->ins);
			update->size = WT_INSERT_KEY_SIZE(skip->ins);
		}

		/*与当前reconcile key进行比较, skip中的记录key必须小于当前的key确大于last key，那么max应该是这个update的key,其实就知道找到比r->last大的key值*/
		WT_ERR(__wt_compare(session, btree->collator, update, r->cur, &cmp));
		if (cmp >= 0)
			continue;

		WT_ERR(__wt_compare(session, btree->collator, update, r->last, &cmp));
		if (cmp >= 0)
			max = update;

		break;
	}

	pa = max->data;
	pb = r->cur->data;
	len = WT_MIN(max->size, r->cur->size);
	size = len + 1;
	for (cnt = 1; len > 0; ++cnt, --len, ++pa, ++pb){
		if (*pa != *pb){ /*对cur key和max key进行前缀比较，确定共同的前缀*/
			if (size != cnt) {
				WT_STAT_FAST_DATA_INCRV(session, rec_suffix_compression, size - cnt);
				size = cnt;
			}
			break;
		}
	}
	/*将前缀拷贝到key中*/
	ret = __wt_buf_set(session, key, r->cur->data, size);

err:
	__wt_scr_free(session, &update);
	return ret;
}

/*重新计算rec split时r的dsk buff状态*/
static int __rec_split_grow(WT_SESSION_IMPL* session, WT_RECONCILE* r, size_t add_len)
{
	WT_BM* bm;
	WT_BTREE* btree;
	size_t corrected_page_size, len;

	btree = S2BT(session);
	bm = btree->bm;

	/*计算block写入的corrected page size*/
	len = WT_PTRDIFF(r->first_free, r->dsk.mem);
	corrected_page_size = len + add_len;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));

	/*重新确定first_free的位置和space_avail的值*/
	WT_RET(__wt_buf_grow(session, &r->dsk, corrected_page_size));
	r->first_free = (uint8_t *)r->dsk.mem + len;
	WT_ASSERT(session, corrected_page_size >= len);
	r->space_avail = corrected_page_size - len;
	WT_ASSERT(session, r->space_avail >= add_len);

	return 0;
}

static int __rec_split(WT_SESSION_IMPL* session, WT_RECONCILE* r, size_t next_len)
{
	WT_BOUNDARY *last, *next;
	WT_BTREE *btree;
	WT_PAGE_HEADER *dsk;
	size_t inuse;

	btree = S2BT(session);
	dsk = r->dsk.mem;

	/*salvage是不需要split page的*/
	if (r->salvage != NULL)
		WT_PANIC_RET(session, WT_PANIC, "%s page too large, attempted split during salvage", __wt_page_type_string(r->page->type));

	__rec_dictionary_reset(r);

	inuse = WT_PTRDIFF32(r->first_free, dsk);
	switch (r->bnd_state){
	case SPLIT_BOUNDARY:
		if (inuse < r->split_size / 2) /*page空间太小，不用split*/
		break;

		/*新增加一个split boundary*/
		WT_RET(__rec_split_bnd_grow(session, r));
		last = &r->bnd[r->bnd_next++];
		next = last + 1;

		/*更新last entryes单元数*/
		last->entries = r->entries - r->total_entries;
		r->total_entries = r->entries;

		next->recno = r->recno;
		if (dsk->type == WT_PAGE_ROW_INT || dsk->type == WT_PAGE_ROW_LEAF) /*row store*/
			WT_RET(__rec_split_row_promote(session, r, &next->key, dsk->type)); /*获得next->key,其实就是last key与reconcile KEY的前缀*/
		next->offset = WT_PTRDIFF(r->first_free, dsk);
		next->entries = 0;

		r->space_avail = r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
		/*溢出最大page size了，调整space_avail可用空间大小*/
		if (inuse + r->space_avail > r->page_size) {
			r->space_avail = r->page_size > inuse ? (r->page_size - inuse) : 0;
			/* There are no further boundary points. */
			r->bnd_state = SPLIT_MAX;
		}

		/*
		 * Return if the next object fits into this page, else we have to split the page.
		 */
		if (r->space_avail >= next_len)
			return 0;

	case SPLIT_MAX:
		WT_RET(__rec_split_fixup(session, r));
		r->bnd_state = SPLIT_TRACKING_OFF;
		break;

	case SPLIT_TRACKING_OFF:
		if (inuse < r->split_size / 2)
			break;

		WT_RET(__rec_split_bnd_grow(session, r));
		last = &r->bnd[r->bnd_next++];
		next = last + 1;

		next->recno = r->recno;
		if (dsk->type == WT_PAGE_ROW_INT || dsk->type == WT_PAGE_ROW_LEAF)
			WT_RET(__rec_split_row_promote(session, r, &next->key, dsk->type));

		next->entries = 0;
		dsk->recno = last->recno;
		dsk->u.entries = r->entries;
		dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);
		WT_RET(__rec_split_write(session, r, last, &r->dsk, 0));

		r->entries = 0;
		r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);
		r->space_avail = r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
		break;

	case SPLIT_TRACKING_RAW:
	WT_ILLEGAL_VALUE(session);
	}

	/*因为next_len大于可以用的内存空间，需要对可用空间做调整*/
	if (r->space_avail < next_len)
		WT_RET(__rec_split_grow(session, r, next_len));

	return 0;
}

