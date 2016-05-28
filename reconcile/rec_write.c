
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

	WT_KV					k, v;					/*当前reconcile的k/v缓存空间对象*/

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
		WT_RET_MSG(session, WT_ERROR, "Attempt to reconcile a clean page.");

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
		WT_PAGE_LOCK(session, page); /*这个锁是为了防止page中有新的数据添加，如果在reconcile过称重需要进行内存的compact，那么必须防止新的数据添加*/
	}
	else{
		/*将page设置为scanning状态，这里会spin wait，直到设置成功为止,防止在recocile过程出现对update list进行垃圾回收*/
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
		WT_TRET(__rec_write_wrapup_err(session, r, page));

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

/*分配一个WT_RECONCILE对象，并进行初始化*/
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

		/*dsk buffer需要对齐写入，设置对齐标示*/
		F_SET(&r->dsk, WT_ITEM_ALIGNED);
	}

	r->ref = ref;
	r->page = page;
	r->flags = flags;

	/* Track if the page can be marked clean. */
	r->leave_dirty = 0;

	/* Raw compression. */
	r->raw_compression = __rec_raw_compression_config(session, page, salvage);
	r->raw_destination.flags = WT_ITEM_ALIGNED; /*压缩后的buffer必须是磁盘写入对其的长度,因为压缩后的数据要落盘*/

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
	* 保存当前正在执行的事务中最早启动的事务ID，防止这个事务后面的事务修改的数据被recocile的磁盘上
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
			goto done;

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

/*根据ref的信息删除对应的leaf page,并返回操作状态*/
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
	r->space_avail -= size;
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

/*检查是否有值重复，如果有，做引用关联*/
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
	else{ /*将值拷贝到val的cell中*/
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
#define WT_FIX_BYTES_TO_ENTRIES(btree, bytes)			((uint32_t)((((bytes) * 8) / (btree)->bitcnt)))
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

/*计算行存储时page分裂的大小*/
uint32_t __wt_split_page_size(WT_BTREE* btree, uint32_t maxpagesize)
{
	uintmax_t a;
	uint32_t split_size;

	a = maxpagesize;
	split_size = (uint32_t)WT_ALIGN((a * (u_int)btree->split_pct) / 100, btree->allocsize); /*btree->split_pct是page分裂阈值的百分比，每次分裂的时候都分裂成最大size的这个百分数大小*/
	if (split_size == btree->allocsize)
		split_size = (uint32_t)((a * (u_int)btree->split_pct) / 100);

	return split_size;
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

	/*计算page分裂后的大小*/
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

/*  */
static int __rec_split_raw_worker(WT_SESSION_IMPL* session, WT_RECONCILE* r, size_t next_len, int no_more_rows)
{
	WT_BM *bm;
	WT_BOUNDARY *last, *next;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_COMPRESSOR *compressor;
	WT_DECL_RET;
	WT_ITEM *dst, *write_ref;
	WT_PAGE_HEADER *dsk, *dsk_dst;
	WT_SESSION *wt_session;
	size_t corrected_page_size, len, result_len;
	uint64_t recno;
	uint32_t entry, i, result_slots, slots;
	int last_block;
	uint8_t *dsk_start;

	wt_session = (WT_SESSION *)session;
	btree = S2BT(session);
	bm = btree->bm;

	unpack = &_unpack;
	compressor = btree->compressor;
	dst = &r->raw_destination;
	dsk = r->dsk.mem;

	/*尝试扩大boundary素组，因为要占用一个新的boundary单元*/
	WT_RET(__rec_split_bnd_grow(session, r));
	last = &r->bnd[r->bnd_next];
	next = last + 1;

	/*第一个kv对还没有确定，。。。。*/
	if (r->entries == 0)
		goto split_grow;

	/*
	* Build arrays of offsets and cumulative counts of cells and rows in
	* the page: the offset is the byte offset to the possible split-point
	* (adjusted for an initial chunk that cannot be compressed), entries
	* is the cumulative page entries covered by the byte offset, recnos is
	* the cumulative rows covered by the byte offset.
	*/
	if (r->entries >= r->raw_max_slots){
		/*为什么是重新释放，再alloc??*/
		__wt_free(session, r->raw_entries);
		__wt_free(session, r->raw_offsets);
		__wt_free(session, r->raw_recnos);
		r->raw_max_slots = 0;

		/*扩大100个raw entires数组单元*/
		i = r->entries + 100;
		WT_RET(__wt_calloc_def(session, i, &r->raw_entries));
		WT_RET(__wt_calloc_def(session, i, &r->raw_offsets));

		if (dsk->type == WT_PAGE_COL_INT || dsk->type == WT_PAGE_COL_VAR)
			WT_RET(__wt_calloc_def(session, i, &r->raw_recnos));
		r->raw_max_slots = i;
	}
	/*更新disk page header中的entries值*/
	dsk->u.entries = r->entries;

	/*We track the record number at each column-store split point, set an initial value.*/
	recno = 0;
	if (dsk->type == WT_PAGE_COL_VAR)
		recno = last->recno;

	/*遍历所有的btree disk buffer中所有的cell，进行split操作*/
	entry = slots = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i){
		++entry;

		/*对cell进行unpack*/
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type){
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
		case WT_CELL_KEY_SHORT:
			break;
		case WT_CELL_ADDR_DEL:
		case WT_CELL_ADDR_INT:
		case WT_CELL_ADDR_LEAF:
		case WT_CELL_ADDR_LEAF_NO:
		case WT_CELL_DEL:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
		case WT_CELL_VALUE_SHORT:
			if (dsk->type == WT_PAGE_COL_INT) {
				recno = unpack->v;
				break;
			}
			if (dsk->type == WT_PAGE_COL_VAR) {
				recno += __wt_cell_rle(unpack);
				break;
			}
			r->raw_entries[slots] = entry;
			continue;
			WT_ILLEGAL_VALUE(session);
		}

		/*dsk->type == WT_PAGE_COL_INT | WT_PAGE_COL_VAR */
		if ((len = WT_PTRDIFF(cell, dsk)) > btree->allocsize)
			r->raw_offsets[++slots] = WT_STORE_SIZE(len - WT_BLOCK_COMPRESS_SKIP);

		if (dsk->type == WT_PAGE_COL_INT || dsk->type == WT_PAGE_COL_VAR)
			r->raw_recnos[slots] = recno;
		r->raw_entries[slots] = entry;
	}

	/*
	* If we haven't managed to find at least one split point, we're done,
	* don't bother calling the underlying compression function.
	*/
	if (slots == 0){
		result_len = 0;
		result_slots = 0;
		goto no_slots;
	}

	r->raw_offsets[++slots] = WT_STORE_SIZE(WT_PTRDIFF(cell, dsk) - WT_BLOCK_COMPRESS_SKIP);

	/*compress处理，计算compress后的数据大小，并作为block size*/
	if (compressor->pre_size == NULL)
		result_len = (size_t)r->raw_offsets[slots];
	else
		WT_RET(compressor->pre_size(compressor, wt_session, (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP, (size_t)r->raw_offsets[slots], &result_len));
	corrected_page_size = result_len + WT_BLOCK_COMPRESS_SKIP;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_init(session, dst, corrected_page_size));

	/*数据压缩转移*/
	memcpy(dst->mem, dsk, WT_BLOCK_COMPRESS_SKIP);
	ret = compressor->compress_raw(compressor, wt_session,
		r->page_size_orig, btree->split_pct,
		WT_BLOCK_COMPRESS_SKIP, (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP,
		r->raw_offsets, slots, (uint8_t *)dst->mem + WT_BLOCK_COMPRESS_SKIP,
		result_len, no_more_rows, &result_len, &result_slots);

	switch (ret){
	case EAGAIN:
		/*
		* The compression function wants more rows; accumulate and
		* retry.
		*
		* Reset the resulting slots count, just in case the compression
		* function modified it before giving up.
		*/
		result_slots = 0;
		break;

	case 0:
		if (result_slots == 0){ /*result_slots = 0，直接用raw data写入block file*/
			WT_STAT_FAST_DATA_INCR(session, compress_raw_fail);
			if (no_more_rows)
				break;

			result_slots = slots - 1;
			result_len = r->raw_offsets[result_slots];
			WT_RET(__wt_buf_grow(session, dst, result_len + WT_BLOCK_COMPRESS_SKIP));
			memcpy((uint8_t *)dst->mem + WT_BLOCK_COMPRESS_SKIP, (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP, result_len);

			last->already_compressed = 0;
		}
		else{ /*数据压缩一个slot单元成功*/
			WT_STAT_FAST_DATA_INCR(session, compress_raw_ok);

			if (result_slots == slots && !no_more_rows)
				result_slots = 0;
			else
				last->already_compressed = 1;
		}
		break;

	default:
		return ret;
	}

no_slots:
	last_block = no_more_rows && (result_slots == 0 || result_slots == slots);
	if (result_slots != 0){
		/*
		* We have a block, finalize the header information.
		*/
		dst->size = result_len + WT_BLOCK_COMPRESS_SKIP;
		dsk_dst = dst->mem;
		dsk_dst->recno = last->recno;
		dsk_dst->mem_size = r->raw_offsets[result_slots] + WT_BLOCK_COMPRESS_SKIP;
		dsk_dst->u.entries = r->raw_entries[result_slots - 1];

		len = WT_PTRDIFF(r->first_free, (uint8_t *)dsk + dsk_dst->mem_size);
		dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
		(void)memmove(dsk_start, (uint8_t *)r->first_free - len, len);

		r->entries -= r->raw_entries[result_slots - 1];
		r->first_free = dsk_start + len;
		r->space_avail += r->raw_offsets[result_slots];
		WT_ASSERT(session, r->first_free + r->space_avail <= (uint8_t *)r->dsk.mem + r->dsk.memsize);

		/*设置boundary边界*/
		switch (dsk->type) {
		case WT_PAGE_COL_INT:
			next->recno = r->raw_recnos[result_slots];
			break;
		case WT_PAGE_COL_VAR:
			next->recno = r->raw_recnos[result_slots - 1];
			break;
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			next->recno = 0;
			if (!last_block) {
				/*
				* Confirm there was uncompressed data remaining
				* in the buffer, we're about to read it for the
				* next chunk's initial key.
				*/
				WT_ASSERT(session, len > 0);
				WT_RET(__rec_split_row_promote_cell(session, dsk, &next->key)); /*设置next->key*/
			}
			break;
		}
		write_ref = dst;
	}
	else if(no_more_rows){
		/*
		* Compression failed and there are no more rows to accumulate,
		* write the original buffer instead.
		*/
		WT_STAT_FAST_DATA_INCR(session, compress_raw_fail);

		dsk->recno = last->recno;
		dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);
		dsk->u.entries = r->entries;

		r->entries = 0;
		r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);
		r->space_avail = r->page_size - WT_PAGE_HEADER_BYTE_SIZE(btree);

		write_ref = &r->dsk;
		last->already_compressed = 0;
	}
	else{
		/*
		* Compression failed, there are more rows to accumulate and the
		* compression function wants to try again; increase the size of
		* the "page" and try again after we accumulate some more rows.
		*/
		WT_STAT_FAST_DATA_INCR(session, compress_raw_fail_temporary);
		goto split_grow;
	}

	++r->bnd_next;

	/*
	* If we are writing the whole page in our first/only attempt, it might
	* be a checkpoint (checkpoints are only a single page, by definition).
	* Further, checkpoints aren't written here, the wrapup functions do the
	* write, and they do the write from the original buffer location.  If
	* it's a checkpoint and the block isn't in the right buffer, copy it.
	*
	* If it's not a checkpoint, write the block.
	*/
	if (r->bnd_next == 1 && last_block && __rec_is_checkpoint(r, last)){
		if (write_ref == dst)
			WT_RET(__wt_buf_set(session, &r->dsk, dst->mem, dst->size));
	}
	else
		WT_RET(__rec_split_write(session, r, last, write_ref, last_block));

	if (r->space_avail < next_len){
split_grow:
		r->page_size *= 2;
		return (__rec_split_grow(session, r, r->page_size + next_len));
	}

	return 0;
}

/*进行压缩数据的解压*/
static int __rec_raw_decompress(WT_SESSION_IMPL* session, const void* image, size_t size, void* retp)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE_HEADER const *dsk;
	size_t result_len;

	btree = S2BT(session);
	dsk = image;

	/*进行数据解压缩*/
	WT_RET(__wt_alloc(session, dsk->mem_size, &tmp));
	memcpy(tmp->mem, image, WT_BLOCK_COMPRESS_SKIP);
	WT_ERR(btree->compressor->decompress(btree->compressor, 
		&session->iface, 
		(uint8_t*)image + WT_BLOCK_COMPRESS_SKIP, size - WT_BLOCK_COMPRESS_SKIP,
		(uint8_t *)tmp->mem + WT_BLOCK_COMPRESS_SKIP, dsk->mem_size - WT_BLOCK_COMPRESS_SKIP, 
		&result_len));

	/*解压缩后长度的判断，如果长度不匹配，说明是解压缩失败*/
	if (result_len != dsk->mem_size - WT_BLOCK_COMPRESS_SKIP)
		WT_ERR(__wt_illegal_value(session, btree->dhandle->name));
	
	WT_ERR(__wt_strdup(session, tmp->data, dsk->mem_size, retp));

	/*block错误检查*/
	WT_ASSERT(session, __wt_verify_dsk_image(session, "[raw evict split]", tmp->data, dsk->mem_size, 0) == 0);
err:
	__wt_src_free(session, &tmp);
	return ret;
}

/* raw compression split routine */
static inline int __rec_split_raw(WT_SESSION_IMPL* session, WT_RECONCILE* r, size_t next_len)
{
	return __rec_split_raw_worker(session, r, next_len, 0);
}

/*
*	Finish processing a page, standard version.
*/
static int __rec_split_finish_std(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	WT_BOUNDARY *bnd;
	WT_PAGE_HEADER *dsk;

	switch (r->bnd_state){
	case SPLIT_BOUNDARY:
	case SPLIT_MAX:
		/*
		* We never split, the reconciled page fit into a maximum page
		* size.  Change the first boundary slot to represent the full
		* page (the first boundary slot is largely correct, just update
		* the number of entries).
		*/
		r->bnd_next = 0;
		break;

	case SPLIT_TRACKING_OFF:
		/*
		* If we have already split, or aren't tracking boundaries, put
		* the remaining data in the next boundary slot.
		*/
		WT_RET(__rec_split_bnd_grow(session, r));
		break;

	case SPLIT_TRACKING_RAW:
		/*We were configured for raw compression, but never actually wrote anything.*/
		break;

	WT_ILLEGAL_VALUE(session);
	}

	/*
	* We only arrive here with no entries to write if the page was entirely
	* empty, and if the page is empty, we merge it into its parent during
	* the parent's reconciliation.  A page with skipped updates isn't truly
	* empty, continue on.
	*/
	if (r->entries == 0 && r->skip_next == 0)
		return 0;

	/*设置新的boundary数据*/
	bnd = &r->bnd[r->bnd_next++];
	bnd->entries = r->entries;

	dsk = r->dsk.mem;
	dsk->recno = bnd->recno;
	dsk->u.entries = r->entries;
	dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);

	/* If this is a checkpoint, we're done, otherwise write the page. */
	return __rec_is_checkpoint(r, bnd) ? 0 : __rec_split_write(session, r, bnd, &r->dsk, 1);
}

/*结束page的reconcile操作*/
static int __rec_split_finish(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	if (r->raw_compression && r->entries != 0) {
		while (r->entries != 0) /*进行compress split*/
			WT_RET(__rec_split_raw_worker(session, r, 0, 1));
	}
	else /*直接将page写入盘里面*/
		WT_RET(__rec_split_finish_std(session, r));

	return 0;
}

/*将已经split的boundary数据写入block中*/
static int __rec_split_fixup(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	WT_BOUNDARY *bnd;
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE_HEADER *dsk;
	size_t i, len;
	uint8_t *dsk_start, *p;

	btree = S2BT(session);

	/*分配一个临时的缓冲区，并把dsk.mem的页头信息拷贝到缓冲区开始的位置*/
	WT_RET(__wt_scr_alloc(session, r->dsk.memsize, &tmp));
	dsk = tmp->mem;
	memcpy(dsk, r->dsk.mem, WT_PAGE_HEADER_SIZE);

	/*计算数据开始存储的位置*/
	dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
	for (i = 0, bnd = r->bnd; i < r->bnd_next; ++i, ++bnd){
		/*拷贝boundary区间数据*/
		len = (bnd + 1)->offset - bnd->offset;
		memcpy(dsk_start, (uint8_t *)r->dsk.mem + bnd->offset, len);

		dsk->recno = bnd->recno;
		dsk->u.entries = bnd->entries;
		dsk->mem_size = tmp->size = WT_PAGE_HEADER_BYTE_SIZE(btree) + len;
		/*将boundary的数据写入block中*/
		WT_ERR(__rec_split_write(session, r, bnd, tmp, 0));
	}

	p = (uint8_t *)r->dsk.mem + bnd->offset;
	len = WT_PTRDIFF(r->first_free, p); /*计算最后一个剩余boundary的长度*/
	if (len >= r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree))
		WT_PANIC_ERR(session, EINVAL, "Reconciliation remnant too large for the split buffer");

	/*将写入的数据从r中移除掉*/
	dsk = r->dsk.mem;
	dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
	(void)memmove(dsk_start, p, len);
	/*调整r中的状态信息*/
	r->entries -= r->total_entries;
	r->first_free = dsk_start + len;
	WT_ASSERT(session, r->page_size >= (WT_PAGE_HEADER_BYTE_SIZE(btree) + len));
	r->space_avail = r->split_size - (WT_PAGE_HEADER_BYTE_SIZE(btree) + len);

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*
*	Write a disk block out for the split helper functions.
*/
static int __rec_split_write(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_BOUNDARY* bnd, WT_ITEM* buf, int last_block)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_MULTI *multi;
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	WT_PAGE_MODIFY *mod;
	WT_UPD_SKIPPED *skip;
	size_t addr_size;
	uint32_t bnd_slot, i, j;
	int cmp;
	uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];

	btree = S2BT(session);
	dsk = buf->mem;
	page = r->page;
	mod = page->modify;

	WT_RET(__wt_scr_alloc(session, 0, &key));

	/* Set the zero-length value flag in the page header. */
	if (dsk->type == WT_PAGE_ROW_LEAF){
		F_CLR(dsk, WT_PAGE_EMPTY_V_ALL | WT_PAGE_EMPTY_V_NONE);

		if (r->entries != 0 && r->all_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_ALL);
		if (r->entries != 0 && !r->any_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_NONE);
	}

	switch (dsk->type){
	case WT_PAGE_COL_FIX:
		bnd->addr.type = WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_VAR:
	case WT_PAGE_ROW_LEAF:
		bnd->addr.type = r->ovfl_items ? WT_ADDR_LEAF : WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		bnd->addr.type = WT_ADDR_INT;
		break;
		WT_ILLEGAL_VALUE_ERR(session);
	}

	bnd->size = (uint32_t)buf->size;
	bnd->cksum = 0;

	/*将update skip list中的update插入到对应的boundary中*/
	for (i = 0, skip = r->skip; i < r->skip_next; i++, ++skip){
		/*最后一个block，将剩余的update全部存入boundary中*/
		if (last_block){
			WT_ERR(__rec_skip_update_move(session, bnd, skip));
			continue;
		}

		switch (page->type){
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			if (WT_INSERT_RECNO(skip->ins) >= (bnd + 1)->recno)/*skip update list中剩余的update超出了这个boundary的管辖范围*/
				goto skip_check_complete;
			break;

		case WT_PAGE_ROW_LEAF:
			if (skip->ins == NULL)
				WT_ERR(__wt_row_leaf_key(session, page, skip->rip, key, 0));
			else {
				key->data = WT_INSERT_KEY(skip->ins);
				key->size = WT_INSERT_KEY_SIZE(skip->ins);
			}
			WT_ERR(__wt_compare(session, btree->collator, key, &(bnd + 1)->key, &cmp)); /*skip update list中剩余的update超出了这个boundary的管辖范围*/
			if (cmp >= 0)
				goto skip_check_complete;
			break;

		WT_ILLEGAL_VALUE_ERR(session);
		}

		WT_ERR(__rec_skip_update_move(session, bnd, skip));
	}

skip_check_complete:
	/*把移到boundary的update从skip array中移除*/
	for (j = 0; i < r->skip_next; ++j, ++i)
		r->skip[j] = r->skip[i];
	r->skip_next = j;

	if (bnd->skip != NULL){
		if (bnd->already_compressed)
			WT_ERR(__rec_raw_decompress(session, buf->data, buf->size, &bnd->dsk));
		else {
			WT_ERR(__wt_strndup(session, buf->data, buf->size, &bnd->dsk));
			WT_ASSERT(session, __wt_verify_dsk_image(session, "[evict split]", buf->data, buf->size, 1) == 0);
		}
		goto done;
	}

	/*
	* If we wrote this block before, re-use it.  Pages get written in the
	* same block order every time, only check the appropriate slot.  The
	* expensive part of this test is the checksum, only do that work when
	* there has been or will be a reconciliation of this page involving
	* split pages.  This test isn't perfect: we're doing a checksum if a
	* previous reconciliation of the page split or if we will split this
	* time, but that test won't calculate a checksum on the first block
	* the first time the page splits.
	*/
	bnd_slot = (uint32_t)(bnd - r->bnd);
	if (bnd_slot > 1 || (F_ISSET(mod, WT_PM_REC_MULTIBLOCK) && mod->mod_multi != NULL)){
		dsk->write_gen = 0;
		memset(WT_BLOCK_HEADER_REF(dsk), 0, btree->block_header);
		bnd->cksum = __wt_cksum(buf->data, buf->size);

		if (F_ISSET(mod, WT_PM_REC_MULTIBLOCK) && mod->mod_multi_entries > bnd_slot) {
			multi = &mod->mod_multi[bnd_slot];
			if (multi->size == bnd->size && multi->cksum == bnd->cksum) {
				multi->addr.reuse = 1;
				bnd->addr = multi->addr;

				WT_STAT_FAST_DATA_INCR(session, rec_page_match);
				goto done;
			}
		}
	}
	/*将数据写入到block中,并获得block addr cookie*/
	WT_ERR(__wt_bt_write(session,buf, addr, &addr_size, 0, bnd->already_compressed));
	WT_ERR(__wt_strndup(session, addr, addr_size, &bnd->addr.addr));
	bnd->addr.size = (uint8_t)addr_size;

done:
err :
	__wt_scr_free(session, &key);
	return ret;
}

int __wt_bulk_init(WT_SESSION_IMPL* session, WT_CURSOR_BULK* cbulk)
{
	WT_BTREE* btree;
	WT_PAGE_INDEX* pindex;
	WT_RECONCILE* r;
	uint64_t recno;

	btree = S2BT(session);

	/*
	* Bulk-load is only permitted on newly created files, not any empty file -- see the checkpoint code for a discussion.
	*/
	if (!btree->bulk_load_ok)
		WT_RET_MSG(session, EINVAL, "bulk-load is only possible for newly created trees");

	pindex = WT_INTL_INDEX_GET_SAFE(btree->root.page);
	cbulk->ref = pindex->index[0];
	cbulk->leaf = cbulk->ref->page;

	/*初始化bulk reconcile对象*/
	WT_RET(__rec_write_init(session, cbulk->ref, 0, NULL, &cbulk->reconcile));
	r = cbulk->reconcile;
	r->is_bulk_load = 1;

	switch (btree->type){
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		recno = 1;
		break;

	case BTREE_ROW:
		recno = 0;
		break;

		WT_ILLEGAL_VALUE(session);
	}

	return __rec_split_init(session, r, cbulk->ref, recno, btree->maxleafpage);
}

int __wt_bulk_wrapup(WT_SESSION_IMPL* session, WT_CURSOR_BULK* cbulk)
{
	WT_BTREE *btree;
	WT_PAGE *parent;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);

	switch (btree->type){
	case BTREE_COL_FIX:
		if (cbulk->entry != 0)
			__rec_incr(session, r, cbulk->entry, __bitstr_size((size_t)cbulk->entry * btree->bitcnt));
		break;

	case BTREE_COL_VAR:
		if (cbulk->rle != 0)
			WT_RET(__wt_bulk_insert_var(session, cbulk));
		break;

	case BTREE_ROW:
		break;

	WT_ILLEGAL_VALUE(session);
	}

	WT_RET(__rec_split_finish(session, r));
	WT_RET(__rec_write_wrapup(session, r, r->page));

	parent = r->ref->home;
	WT_RET(__wt_page_modify_init(session, parent));
	__wt_page_modify_set(session, parent);

	__rec_destroy(session, &cbulk->reconcile);

	return 0;
}

/*bulk 方式的row插入操作*/
int __wt_bulk_insert_row(WT_SESSION_IMPL* session, WT_CURSOR_BULK* cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_KV *key, *val;
	WT_RECONCILE *r;
	int ovfl_key;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	key = &r->k;
	val = &r->v;
	WT_RET(__rec_cell_build_leaf_key(session, r, cursor->key.data, cursor->key.size, &ovfl_key));
	WT_RET(__rec_cell_build_val(session, r, cursor->value.data, cursor->value.size, (uint64_t)0));

	/*确定boundary范围,reconile 的剩余的空间无法存下k/v*/
	if (key->len + val->len > r->space_avail){
		if (r->raw_compression)
			WT_RET(__rec_split_raw(session, r, key->len + val->len));
		else {
			/*
			* Turn off prefix compression until a full key written
			* to the new page, and (unless already working with an
			* overflow key), rebuild the key without compression.
			*/
			if (r->key_pfx_compress_conf) {
				r->key_pfx_compress = 0;
				if (!ovfl_key)
					WT_RET(__rec_cell_build_leaf_key(session, r, NULL, 0, &ovfl_key));
			}

			WT_RET(__rec_split(session, r, key->len + val->len));
		}
	}

	/*将kv拷贝到r缓冲区中*/
	__rec_copy_incr(session, r, key);
	if (val->len == 0)
		r->any_empty_value = 1;
	else{
		r->all_empty_value = 0;
		if (btree->dictionary)
			WT_RET(__rec_dict_replace(session, r, 0, val));
		__rec_copy_incr(session, r, val);
	}
	/* Update compression state. */
	__rec_key_state_update(r, ovfl_key);

	return 0;
}

/*Check if a bulk-loaded fixed-length column store page needs to split.*/
static inline int __rec_col_fix_bulk_insert_split_check(WT_CURSOR_BULK* cbulk)
{
	WT_BTREE *btree;
	WT_RECONCILE *r;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL*)cbulk->cbt.iface.session;
	r = cbulk->reconcile;
	btree = S2BT(session);

	if (cbulk->entry == cbulk->nrecs){
		if (cbulk->entry != 0){
			__rec_incr(session, r, cbulk->entry, __bitstr_size((size_t)cbulk->entry * btree->bitcnt));
			/*对reconcile中的buf进行split操作*/
			WT_RET(__rec_split(session, r, 0));
		}
		cbulk->entry = 0;
		cbulk->nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
	}

	return 0;
}

/*Fix-length的column方式的bulk(批量插入)插入操作实现*/
int __wt_bulk_insert_fix(WT_SESSION_IMPL* session, WT_CURSOR_BULK* cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_RECONCILE *r;
	uint32_t entries, offset, page_entries, page_size;
	const uint8_t *data;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	if (cbulk->bitmap){
		if (((r->recno - 1) * btree->bitcnt) & 0x7)
			WT_RET_MSG(session, EINVAL, "Bulk bitmap load not aligned on a byte boundary");
		data = cursor->value.data;
		for (entries = (uint32_t)cursor->value.size; entries > 0; entries -= page_entries, data += page_size){
			WT_RET(__rec_col_fix_bulk_insert_split_check(cbulk));

			page_entries = WT_MIN(entries, cbulk->nrecs - cbulk->entry);
			page_size = __bitstr_size(page_entries * btree->bitcnt); /*计算page_entries字节长度*/
			offset = __bitstr_size(cbulk->entry * btree->bitcnt); /*计算当前cbulk对应的entry的指针位置*/
			memcpy(r->first_free + offset, data, page_size);

			cbulk->entry += page_entries;
			r->recno += page_entries;
		}
		return 0;
	}

	WT_RET(__rec_col_fix_bulk_insert_split_check(cbulk));

	__bit_setv(r->first_free, cbulk->entry, btree->bitcnt, ((uint8_t *)cursor->value.data)[0]);
	++cbulk->entry;
	++r->recno;

	return 0;
}

/*Variable-length column-store bulk insert*/
int __wt_bulk_insert_var(WT_SESSION_IMPL* session, WT_CURSOR_BULK* cbulk)
{
	WT_BTREE *btree;
	WT_KV *val;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);

	/*
	* Store the bulk cursor's last buffer, not the current value, we're
	* creating a duplicate count, which means we want the previous value
	* seen, not the current value.
	*/
	val = &r->v;
	WT_RET(__rec_cell_build_val(session, r, cbulk->last.data, cbulk->last.size, cbulk->rle));

	/*boundary split*/
	if (val->len > r->space_avail)
		WT_RET(r->raw_compression ? __rec_split_raw(session, r, val->len) : __rec_split(session, r, val->len));

	if (btree->dictionary)
		WT_RET(__rec_dict_replace(session, r, cbulk->rle, val));
	__rec_copy_incr(session, r, val);

	r->recno += cbulk->rle;

	return 0;
}

/*获得reconcile addr的类型值*/
static inline u_int __rec_vtype(WT_ADDR* addr)
{
	if (addr->type == WT_ADDR_INT)
		return (WT_CELL_ADDR_INT);
	if (addr->type == WT_ADDR_LEAF)
		return (WT_CELL_ADDR_LEAF);
	return (WT_CELL_ADDR_LEAF_NO);
}

/* 将split page中的 mod_multi中数据合并到reconcile buf中*/
static int __rec_col_merge(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_ADDR *addr;
	WT_KV *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;
	val = &r->v;

	for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i){
		r->recno = multi->key.recno;

		addr = &multi->addr;
		__rec_cell_build_addr(r, addr->addr, addr->size, __rec_vtype(addr), r->recno);

		if (val->len > r->space_avail)
			WT_RET(r->raw_compression ? __rec_split_raw(session, r, val->len) : __rec_split(session, r, val->len));

		__rec_copy_incr(session, r, val);
	}

	return 0;
}

/*对一个column store的internal page做reconcile*/
static int __rec_col_int(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_DECL_RET;
	WT_KV *val;
	WT_PAGE *child;
	WT_REF *ref;
	int hazard, state;

	btree = S2BT(session);
	child = NULL;
	hazard = 0;

	val = &r->v;
	vpack = &_vpack;

	WT_RET(__rec_split_init(session, r, page, page->pg_intl_recno, btree->maxleafpage));

	WT_INTL_FOREACH_BEGIN(session, page, ref) {
		r->recno = ref->key.recno;

		WT_ERR(__rec_child_modify(session, r, ref, &hazard, &state));
		addr = NULL;
		child = ref->page;

		/*对应的孩子page被删除了，不需要进行reconcile这个ref记录*/
		if (state == WT_CHILD_IGNORE){
			CHILD_RELEASE_ERR(session, hazard, ref);
			continue;
		}

		if (state == WT_CHILD_MODIFIED){
			switch (F_ISSET(child->modify, WT_PM_REC_MASK)){
			case WT_PM_REC_EMPTY:  /*空页*/
				CHILD_RELEASE_ERR(session, hazard, ref);
				continue;

			case WT_PM_REC_MULTIBLOCK:
				WT_ERR(__rec_col_merge(session, r, child)); /*将split中的孩子节点修改数据合并到这一层*/
				CHILD_RELEASE_ERR(session, hazard, ref);
				continue;

			case WT_PM_REC_REPLACE:
				addr = &child->modify->mod_replace; /*替换覆盖??*/
				break;

				WT_ILLEGAL_VALUE_ERR(session);
			}
		}
		else
			WT_ASSERT(session, state == 0);

		if (addr == NULL && __wt_off_page(page, ref->addr))
			addr = ref->addr;
		if (addr == NULL){ /*从ref中获取block addr作为val写入到reconcile buf中*/
			__wt_cell_unpack(ref->addr, vpack);
			val->buf.data = ref->addr;
			val->buf.size = __wt_cell_total_len(vpack);
			val->cell_len = 0;
			val->len = val->buf.size;
		}
		else
			__rec_cell_build_addr(r, addr->addr, addr->size, __rec_vtype(addr), ref->key.recno);

		CHILD_RELEASE_ERR(session, hazard, ref);

		if (val->len > r->space_avail)
			WT_ERR(r->raw_compression ? __rec_split_raw(session, r, val->len) : __rec_split(session, r, val->len));

		__rec_copy_incr(session, r, val);
	}WT_INTL_FOREACH_END;

	return __rec_split_finish(session, r);

err:
	CHILD_RELEASE(session, hazard, ref);
	return ret;
}

/*定长column store的叶子page的reconcile操作*/
static int __rec_col_fix(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_BTREE *btree;
	WT_INSERT *ins;
	WT_UPDATE *upd;
	uint64_t recno;
	uint32_t entry, nrecs;

	btree = S2BT(session);

	WT_RET(__rec_split_init(session, r, page, page->pg_fix_recno, btree->maxleafpage));
	/*获得session对应事务可见的修改数据*/
	WT_SKIP_FOREACH(ins, WT_COL_UPDATE_SINGLE(page)) {
		WT_RET(__rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd != NULL)
			__bit_setv_recno(page, WT_INSERT_RECNO(ins), btree->bitcnt, ((uint8_t *)WT_UPDATE_DATA(upd))[0]);
	}

	/*将数据拷贝到reconcile的buf中*/
	memcpy(r->first_free, page->pg_fix_bitf, __bitstr_size((size_t)page->pg_fix_entries * btree->bitcnt));

	entry = page->pg_fix_entries;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail) - page->pg_fix_entries;
	r->recno += entry;

	/* Walk any append list. */
	WT_SKIP_FOREACH(ins, WT_COL_APPEND(page)) {
		WT_RET(__rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL)
			continue;
		for (;;) {
			/*
			* The application may have inserted records which left
			* gaps in the name space.
			*/
			for (recno = WT_INSERT_RECNO(ins); nrecs > 0 && r->recno < recno; --nrecs, ++entry, ++r->recno)
				__bit_setv(r->first_free, entry, btree->bitcnt, 0);

			if (nrecs > 0) {
				__bit_setv(r->first_free, entry, btree->bitcnt, ((uint8_t *)WT_UPDATE_DATA(upd))[0]);
				--nrecs;
				++entry;
				++r->recno;
				break;
			}

			/* If everything didn't fit, update the counters and split.
			 * Boundary: split or write the page.
			 */
			__rec_incr(session, r, entry, __bitstr_size((size_t)entry * btree->bitcnt));
			WT_RET(__rec_split(session, r, 0));

			entry = 0;
			nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
		}
	}

	__rec_incr(session, r, entry, __bitstr_size((size_t)entry * btree->bitcnt));

	return __rec_split_finish(session, r);
}

/*对修复的fixed-width 列式存储page做reconcile操作*/
static int __rec_col_fix_slvg(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page, WT_SALVAGE_COOKIE* salvage)
{
	WT_BTREE *btree;
	uint64_t page_start, page_take;
	uint32_t entry, nrecs;

	btree = S2BT(session);

	WT_RET(__rec_split_init(session, r, page, page->pg_fix_recno, btree->maxleafpage));

	page_take = salvage->take == 0 ? page->pg_fix_entries : salvage->take;
	page_start = salvage->skip == 0 ? 0 : salvage->skip;

	/* Calculate the number of entries per page. */
	entry = 0;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
	/*跳过错误的数据位置*/
	for (; nrecs > 0 && salvage->missing > 0; --nrecs, --salvage->missing, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt, 0);
	/*将有效的修正后的数据写入到对应的位置*/
	for (; nrecs > 0 && page_take > 0; --nrecs, --page_take, ++page_start, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt, __bit_getv(page->pg_fix_bitf, (uint32_t)page_start, btree->bitcnt));

	r->recno += entry;
	__rec_incr(session, r, entry, __bitstr_size((size_t)entry * btree->bitcnt));

	/*
	* We can't split during salvage -- if everything didn't fit, it's all gone wrong.
	*/
	if (salvage->missing != 0 || page_take != 0)
		WT_PANIC_RET(session, WT_PANIC, "%s page too large, attempted split during salvage", __wt_page_type_string(page->type));

	/* Write the page. */
	return __rec_split_finish(session, r);
}

/**/
static int __rec_col_var_helper(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_SALVAGE_COOKIE* salvage, 
					WT_ITEM* value, int deleted, uint8_t overflow_type, uint64_t rle)
{
	WT_BTREE *btree;
	WT_KV *val;

	btree = S2BT(session);

	val = &r->v;

	/*
	 * Occasionally, salvage needs to discard records from the beginning or
	 * end of the page, and because the items may be part of a RLE cell, do
	 * the adjustments here.   It's not a mistake we don't bother telling
	 * our caller we've handled all the records from the page we care about,
	 * and can quit processing the page: salvage is a rare operation and I
	 * don't want to complicate our caller's loop.
	 */
	if(salvage != NULL){
		if(salvage->done)
			return 0;

		if(salvage->skip != 0){
			if (rle <= salvage->skip) {
				salvage->skip -= rle;
				return 0;
			}
			rle -= salvage->skip;
			salvage->skip = 0;
		}

		if(salvage->take != 0){
			if(rle <= salvage->take)
				salvage->take -= rle;
			else{
				rle = salvage->take;
				salvage->take = 0;
			}

			if(salvage->take == 0)
				salvage->done = 1;
		}
	}

	if(deleted){
		val->cell_len = __wt_cell_pack_del(&val->cell, rle);
		val->buf.data = NULL;
		val->buf.size = 0;
		val->len = val->cell_len;
	}
	else if(overflow_type){
		val->cell_len = __wt_cell_pack_ovfl(&val->cell, overflow_type, rle, value->size);
		val->buf.data = value->data;
		val->buf.size = value->size;
		val->len = val->cell_len + value->size;
	}
	else
		WT_RET(__rec_cell_build_val(session, r, value->data, value->size, rle));

	/* Boundary: split or write the page. */
	if (val->len > r->space_avail)
		WT_RET(r->raw_compression ? __rec_split_raw(session, r, val->len) : __rec_split(session, r, val->len));

	/* Copy the value onto the page. */
	if (!deleted && !overflow_type && btree->dictionary)
		WT_RET(__rec_dict_replace(session, r, rle, val));
	__rec_copy_incr(session, r, val);

	/* Update the starting record number in case we split. */
	r->recno += rle;

	return 0;
}

/*变长列式存储的叶子page进行reconcile操作*/
static int __rec_col_var(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page, WT_SALVAGE_COOKIE* salvage)
{
	enum { OVFL_IGNORE, OVFL_UNUSED, OVFL_USED } ovfl_state;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_COL *cip;
	WT_DECL_ITEM(orig);
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_ITEM *last;
	WT_UPDATE *upd;
	uint64_t n, nrepeat, repeat_count, rle, skip, src_recno;
	uint32_t i, size;
	int deleted, last_deleted, orig_deleted, update_no_copy;
	const void *data;

	btree = S2BT(session);
	last = r->last;
	vpack = &_vpack;

	WT_RET(__wt_scr_alloc(session, 0, &orig));
	data = NULL;
	size = 0;
	upd = NULL;

	WT_RET(__rec_split_init(session, r, page, page->pg_var_recno, btree->maxleafpage));

	rle = 0;
	last_deleted = 0;

	/*
	* The salvage code may be calling us to reconcile a page where there
	* were missing records in the column-store name space.  If taking the
	* first record from on the page, it might be a deleted record, so we
	* have to give the RLE code a chance to figure that out.  Else, if
	* not taking the first record from the page, write a single element
	* representing the missing records onto a new page.  (Don't pass the
	* salvage cookie to our helper function in this case, we're handling
	* one of the salvage cookie fields on our own, and we don't need the
	* helper function's assistance.)
	*/
	if (salvage != NULL && salvage->missing != 0) {
		if (salvage->skip == 0) {
			rle = salvage->missing;
			last_deleted = 1;

			/* Correct the number of records we're going to "take", pretending the missing records were on the page.*/
			salvage->take += salvage->missing;
		}
		else
			WT_ERR(__rec_col_var_helper(session, r, NULL, NULL, 1, 0, salvage->missing));
	}

	src_recno = r->recno + rle;

	WT_COL_FOREACH(page, cip, i){
		ovfl_state = OVFL_IGNORE;
		if ((cell = WT_COL_PTR(page, cip)) == NULL) {
			nrepeat = 1;
			ins = NULL;
			orig_deleted = 1;
		}
		else {
			__wt_cell_unpack(cell, vpack);
			nrepeat = __wt_cell_rle(vpack);
			ins = WT_SKIP_FIRST(WT_COL_UPDATE(page, cip));

			/*
			* If the original value is "deleted", there's no value
			* to compare, we're done.
			*/
			orig_deleted = vpack->type == WT_CELL_DEL ? 1 : 0;
			if (orig_deleted)
				goto record_loop;

			/*
			* Overflow items are tricky: we don't know until we're
			* finished processing the set of values if we need the
			* overflow value or not.  If we don't use the overflow
			* item at all, we have to discard it from the backing
			* file, otherwise we'll leak blocks on the checkpoint.
			* That's safe because if the backing overflow value is
			* still needed by any running transaction, we'll cache
			* a copy in the reconciliation tracking structures.
			*
			* Regardless, we avoid copying in overflow records: if
			* there's a WT_INSERT entry that modifies a reference
			* counted overflow record, we may have to write copies
			* of the overflow record, and in that case we'll do the
			* comparisons, but we don't read overflow items just to
			* see if they match records on either side.
			*/
			if (vpack->ovfl) {
				ovfl_state = OVFL_UNUSED;
				goto record_loop;
			}

			/*
			* If data is Huffman encoded, we have to decode it in
			* order to compare it with the last item we saw, which
			* may have been an update string.  This guarantees we
			* find every single pair of objects we can RLE encode,
			* including applications updating an existing record
			* where the new value happens (?) to match a Huffman-
			* encoded value in a previous or next record.
			*/
			WT_ERR(__wt_dsk_cell_data_ref(session, WT_PAGE_COL_VAR, vpack, orig));
		}

record_loop:
		for (n = 0; n < nrepeat; n += repeat_count, src_recno += repeat_count) {
			upd = NULL;
			if (ins != NULL && WT_INSERT_RECNO(ins) == src_recno) {
				WT_ERR(__rec_txn_read(session, r, ins, NULL, vpack, &upd));
				ins = WT_SKIP_NEXT(ins);
			}
			if (upd != NULL) {
				update_no_copy = 1;	/* No data copy */
				repeat_count = 1;	/* Single record */

				deleted = WT_UPDATE_DELETED_ISSET(upd);
				if (!deleted) {
					data = WT_UPDATE_DATA(upd);
					size = upd->size;
				}
			}
			else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				update_no_copy = 1;	/* No data copy */
				repeat_count = 1;	/* Single record */

				deleted = 0;

				/*
				* If doing update save and restore, there's an
				* update that's not globally visible, and the
				* underlying value is a removed overflow value,
				* we end up here.
				*
				* When the update save/restore code noticed the
				* removed overflow value, it appended a copy of
				* the cached, original overflow value to the
				* update list being saved (ensuring the on-page
				* item will never be accessed after the page is
				* re-instantiated), then returned a NULL update
				* to us.
				*
				* Assert the case: if we remove an underlying
				* overflow object, checkpoint reconciliation
				* should never see it again, there should be a
				* visible update in the way.
				*
				* Write a placeholder.
				*/
				WT_ASSERT(session, F_ISSET(r, WT_SKIP_UPDATE_RESTORE));

				data = "@";
				size = 1;
			}
			else {
				update_no_copy = 0;	/* Maybe data copy */

				/*
				* The repeat count is the number of records up
				* to the next WT_INSERT record, or up to the
				* end of the entry if we have no more WT_INSERT
				* records.
				*/
				if (ins == NULL)
					repeat_count = nrepeat - n;
				else
					repeat_count = WT_INSERT_RECNO(ins) - src_recno;

				deleted = orig_deleted;
				if (deleted)
					goto compare;

				/*
				* If we are handling overflow items, use the
				* overflow item itself exactly once, after
				* which we have to copy it into a buffer and
				* from then on use a complete copy because we
				* are re-creating a new overflow record each
				* time.
				*/
				switch (ovfl_state) {
				case OVFL_UNUSED:
					/*
					* An as-yet-unused overflow item.
					*
					* We're going to copy the on-page cell,
					* write out any record we're tracking.
					*/
					if (rle != 0) {
						WT_ERR(__rec_col_var_helper(session, r, salvage, last, last_deleted, 0, rle));
						rle = 0;
					}

					last->data = vpack->data;
					last->size = vpack->size;
					WT_ERR(__rec_col_var_helper(session, r, salvage, last, 0, WT_CELL_VALUE_OVFL, repeat_count));

					/* Track if page has overflow items. */
					r->ovfl_items = 1;

					ovfl_state = OVFL_USED;
					continue;
				case OVFL_USED:
					/*
					* Original is an overflow item; we used
					* it for a key and now we need another
					* copy; read it into memory.
					*/
					WT_ERR(__wt_dsk_cell_data_ref(session, WT_PAGE_COL_VAR, vpack, orig));

					ovfl_state = OVFL_IGNORE;
					/* FALLTHROUGH */
				case OVFL_IGNORE:
					/*
					* Original is an overflow item and we
					* were forced to copy it into memory,
					* or the original wasn't an overflow
					* item; use the data copied into orig.
					*/
					data = orig->data;
					size = (uint32_t)orig->size;
					break;
				}
			}
compare:
			   /* If we have a record against which to compare, and
				* the records compare equal, increment the rle counter
				* and continue.If the records don't compare equal,
				* output the last record and swap the last and current
				* buffers : do NOT update the starting record number,
				*we've been doing that all along.
				*/
				if (rle != 0) {
					if ((deleted && last_deleted) || (!last_deleted && !deleted && last->size == size && memcmp(last->data, data, size) == 0)) {
						rle += repeat_count;
						continue;
					}
					WT_ERR(__rec_col_var_helper(session, r, salvage, last, last_deleted, 0, rle));
				}

			/*
			* Swap the current/last state.
			*
			* Reset RLE counter and turn on comparisons.
			*/
			if (!deleted) {
				/*
				* We can't simply assign the data values into
				* the last buffer because they may have come
				* from a copy built from an encoded/overflow
				* cell and creating the next record is going
				* to overwrite that memory.  Check, because
				* encoded/overflow cells aren't that common
				* and we'd like to avoid the copy.  If data
				* was taken from the current unpack structure
				* (which points into the page), or was taken
				* from an update structure, we can just use
				* the pointers, they're not moving.
				*/
				if (data == vpack->data || update_no_copy) {
					last->data = data;
					last->size = size;
				}
				else
					WT_ERR(__wt_buf_set(session, last, data, size));
			}
			last_deleted = deleted;
			rle = repeat_count;
		}

		/*
		* If we had a reference to an overflow record we never used,
		* discard the underlying blocks, they're no longer useful.
		*
		* One complication: we must cache a copy before discarding the
		* on-disk version if there's a transaction in the system that
		* might read the original value.
		*/
		if (ovfl_state == OVFL_UNUSED && vpack->raw != WT_CELL_VALUE_OVFL_RM)
			WT_ERR(__wt_ovfl_cache(session, page, upd, vpack));
	}

	WT_SKIP_FOREACH(ins, WT_COL_APPEND(page)){
		WT_ERR(__rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL)
			continue;

		for (n = WT_INSERT_RECNO(ins); src_recno <= n; ++src_recno){
			if (src_recno < n){
				deleted = 1;
				if (last_deleted){
					skip = (n - src_recno) - 1;
					rle += skip;
					src_recno += skip;
				}
			}
			else{
				deleted = WT_UPDATE_DELETED_ISSET(upd);
				if (!deleted) {
					data = WT_UPDATE_DATA(upd);
					size = upd->size;
				}
			}

			if (rle != 0) {
				if ((deleted && last_deleted) || (!last_deleted && !deleted && last->size == size && memcmp(last->data, data, size) == 0)) {
					++rle;
					continue;
				}
				WT_ERR(__rec_col_var_helper(session, r, salvage, last, last_deleted, 0, rle));
			}

			if (!deleted) {
				last->data = data;
				last->size = size;
			}
			last_deleted = deleted;
			rle = 1;
		}
	}

	if (rle != 0)
		WT_ERR(__rec_col_val_helper(session, r, salvage, last, last_deleted, 0, rle));

	ret = __rec_split_finish(session, r);

err:
	__wt_scr_free(session, &orig);
}

/*row store的internal page的reconcile操作*/
static int __rec_row_int(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_KV *key, *val;
	WT_PAGE *child;
	WT_REF *ref;
	size_t size;
	u_int vtype;
	int hazard, key_onpage_ovfl, ovfl_key, state;
	const void *p;

	btree = S2BT(session);
	child = NULL;
	hazard = 0;

	key = &r->k;
	kpack = &_kpack;
	WT_CLEAR(*kpack);	/* -Wuninitialized */

	val = &r->v;
	vpack = &_vpack;
	WT_CLEAR(*vpack);	/* -Wuninitialized */

	WT_RET(__rec_split_init(session, r, page, 0, btree->maxintlpage));

	r->cell_zero = 1;
	WT_INTL_FOREACH_BEGIN(session, page, ref){
		ikey = __wt_ref_key_instantiated(ref);
		if (ikey == NULL || ikey->cell_offset == 0){
			cell = NULL;
			key_onpage_ovfl = 0;
		}
		else{/*确定key是否是overflow方式存储*/
			cell = WT_PAGE_REF_OFFSET(page, ikey->cell_offset);
			__wt_cell_unpack(cell, kpack);
			key_onpage_ovfl = (kpack->ovfl && kpack->raw != WT_CELL_KEY_OVFL_RM);
		}
		/*将孩子节点修改为脏page,并获取其hazard pointer防止被evict出内存*/
		WT_ERR(__rec_child_modify(session, r, ref, &hazard, &state));
		addr = ref->addr;
		child = ref->page;
		/*孩子被标记为deleted，不需要对这个ref记录做reconcile操作*/
		if (state == WT_CHILD_IGNORE){
			if (key_onpage_ovfl)
				WT_ERR(__wt_ovfl_discard_add(session, page, kpack->cell));
			CHILD_RELEASE_ERR(session, hazard, ref);
			continue;
		}

		/*
		* Modified child.  Empty pages are merged into the parent and discarded.
		*/
		if (state == WT_CHILD_MODIFIED)
			switch (F_ISSET(child->modify, WT_PM_REC_MASK)) {
			case WT_PM_REC_EMPTY:
				/*
				* Overflow keys referencing empty pages are no
				* longer useful, schedule them for discard.
				* Don't worry about instantiation, internal
				* page keys are always instantiated.  Don't
				* worry about reuse, reusing this key in this
				* reconciliation is unlikely.
				*/
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(session, page, kpack->cell));
				CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_MULTIBLOCK:
				/*
				* Overflow keys referencing split pages are no
				* longer useful (the split page's key is the
				* interesting key); schedule them for discard.
				* Don't worry about instantiation, internal
				* page keys are always instantiated.  Don't
				* worry about reuse, reusing this key in this
				* reconciliation is unlikely.
				*/
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(session, page, kpack->cell));

				WT_ERR(__rec_row_merge(session, r, child));
				CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_REPLACE:
				/*
				* If the page is replaced, the page's modify
				* structure has the page's address.
				*/
				addr = &child->modify->mod_replace;
				break;
			WT_ILLEGAL_VALUE_ERR(session);
		}

		/*addr是在page存储空间之外分配的内存,直接赋值就可以*/
		if (__wt_off_page(page, addr)){
			p = addr->addr;
			size = addr->size;
			vtype = (state == WT_CHILD_PROXY ? WT_CELL_ADDR_DEL : __rec_vtype(addr));
		}
		else{ /*连续内存中分配的addr,进行cell upack*/
			__wt_cell_unpack(ref->addr, vpack);
			p = vpack->data;
			size = vpack->size;
			vtype = (state == WT_CHILD_PROXY ? WT_CELL_ADDR_DEL : (u_int)(vpack->raw));
		}

		__rec_cell_build_addr(r, p, size, vtype, 0);
		CHILD_RELEASE_ERR(session, hazard, ref);

		/*
		* Build key cell.
		* Truncate any 0th key, internal pages don't need 0th keys.
		*/
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = 1;
		}
		else {
			__wt_ref_key(page, ref, &p, &size);
			WT_ERR(__rec_cell_build_int_key(session, r, p, r->cell_zero ? 1 : size, &ovfl_key));
		}
		r->cell_zero = 0;

		/*将key/value写入到reconcile buf中*/
		/* Boundary: split or write the page. */
		if (key->len + val->len > r->space_avail) {
			if (r->raw_compression)
				WT_ERR(__rec_split_raw(session, r, key->len + val->len));
			else {
				/*
				* In one path above, we copied address blocks
				* from the page rather than building the actual
				* key.  In that case, we have to build the key
				* now because we are about to promote it.
				*/
				if (key_onpage_ovfl) {
					WT_ERR(__wt_buf_set(session, r->cur, WT_IKEY_DATA(ikey), ikey->size));
					key_onpage_ovfl = 0;
				}
				WT_ERR(__rec_split(session, r, key->len + val->len));
			}
		}

		/* Copy the key and value onto the page. */
		__rec_copy_incr(session, r, key);
		__rec_copy_incr(session, r, val);

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	}WT_INTL_FOREACH_END;

	return __rec_split_finish(session, r);
err:
	CHILD_RELEASE(session, hazard, ref);
	return ret;
}

/*合并内存中split page 的修改合并到reconcile buf中*/
static int __rec_row_merge(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_ADDR *addr;
	WT_KV *key, *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;
	int ovfl_key;

	mod = page->modify;

	key = &r->k;
	val = &r->v;

	/*将孩子page分裂后的key和block addr作为k/v reconcile到internal page block当中*/
	for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i){
		WT_RET(__rec_cell_build_int_key(session, r, WT_IKEY_DATA(multi->key.ikey), r->cell_zero ? 1 : multi->key.ikey->size, &ovfl_key));
		r->cell_zero = 0;

		addr = &multi->addr;

		__rec_cell_build_addr(r, addr->addr, addr->size, __rec_vtype(addr), 0);

		/* Boundary: split or write the page. */
		if (key->len + val->len > r->space_avail)
			WT_RET(r->raw_compression ? __rec_split_raw(session, r, key->len + val->len) : __rec_split(session, r, key->len + val->len));

		/* Copy the key and value onto the page. */
		__rec_copy_incr(session, r, key);
		__rec_copy_incr(session, r, val);

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	}

	return 0;
}

/*行存储方式leaf page的reconcile操作，内部没看懂*/
static int __rec_row_leaf(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;
	WT_CELL *cell, *val_cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_DECL_ITEM(tmpkey);
	WT_DECL_ITEM(tmpval);
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_INSERT *ins;
	WT_KV *key, *val;
	WT_ROW *rip;
	WT_UPDATE *upd;
	size_t size;
	uint64_t slvg_skip;
	uint32_t i;
	int dictionary, key_onpage_ovfl, ovfl_key;
	const void *p;
	void *copy;

	btree = S2BT(session);
	slvg_skip = salvage == NULL ? 0 : salvage->skip;

	key = &r->k;
	val = &r->v;

	WT_RET(__rec_split_init(session, r, page, 0ULL, btree->maxleafpage));

	/*
	* Write any K/V pairs inserted into the page before the first from-disk key on the page.
	*/
	if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SMALLEST(page))) != NULL)
		WT_RET(__rec_row_leaf_insert(session, r, ins));

	/*
	* Temporary buffers in which to instantiate any uninstantiated keys
	* or value items we need.
	*/
	WT_RET(__wt_scr_alloc(session, 0, &tmpkey));
	WT_RET(__wt_scr_alloc(session, 0, &tmpval));

	/* For each entry in the page... */
	WT_ROW_FOREACH(page, rip, i) {
		/*
		* The salvage code, on some rare occasions, wants to reconcile
		* a page but skip some leading records on the page.  Because
		* the row-store leaf reconciliation function copies keys from
		* the original disk page, this is non-trivial -- just changing
		* the in-memory pointers isn't sufficient, we have to change
		* the WT_CELL structures on the disk page, too.  It's ugly, but
		* we pass in a value that tells us how many records to skip in
		* this case.
		*/
		if (slvg_skip != 0) {
			--slvg_skip;
			continue;
		}

		/*
		* Figure out the key: set any cell reference (and unpack it),
		* set any instantiated key reference.
		*/
		copy = WT_ROW_KEY_COPY(rip);
		(void)__wt_row_leaf_key_info(page, copy, &ikey, &cell, NULL, NULL);
		if (cell == NULL)
			kpack = NULL;
		else {
			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
		}

		/* Unpack the on-page value cell, and look for an update. */
		if ((val_cell = __wt_row_leaf_value_cell(page, rip, NULL)) == NULL)
			vpack = NULL;
		else {
			vpack = &_vpack;
			__wt_cell_unpack(val_cell, vpack);
		}
		WT_ERR(__rec_txn_read(session, r, NULL, rip, vpack, &upd));

		/* Build value cell. */
		dictionary = 0;
		if (upd == NULL) {
			/*
			* When the page was read into memory, there may not
			* have been a value item.
			*
			* If there was a value item, check if it's a dictionary
			* cell (a copy of another item on the page).  If it's a
			* copy, we have to create a new value item as the old
			* item might have been discarded from the page.
			*/
			if (vpack == NULL) {
				val->buf.data = NULL;
				val->cell_len = val->len = val->buf.size = 0;
			}
			else if (vpack->raw == WT_CELL_VALUE_COPY) {
				/* If the item is Huffman encoded, decode it. */
				if (btree->huffman_value == NULL) {
					p = vpack->data;
					size = vpack->size;
				}
				else {
					WT_ERR(__wt_huffman_decode(session, btree->huffman_value, vpack->data, vpack->size, tmpval));
					p = tmpval->data;
					size = tmpval->size;
				}
				WT_ERR(__rec_cell_build_val(session, r, p, size, (uint64_t)0));
				dictionary = 1;
			}
			else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				/*
				* If doing update save and restore in service
				* of eviction, there's an update that's not
				* globally visible, and the underlying value
				* is a removed overflow value, we end up here.
				*
				* When the update save/restore code noticed the
				* removed overflow value, it appended a copy of
				* the cached, original overflow value to the
				* update list being saved (ensuring any on-page
				* item will never be accessed after the page is
				* re-instantiated), then returned a NULL update
				* to us.
				*
				* Assert the case.
				*/
				WT_ASSERT(session, F_ISSET(r, WT_SKIP_UPDATE_RESTORE));

				/*
				* If the key is also a removed overflow item,
				* don't write anything at all.
				*
				* We don't have to write anything because the
				* code re-instantiating the page gets the key
				* to match the saved list of updates from the
				* original page.  By not putting the key on
				* the page, we'll move the key/value set from
				* a row-store leaf page slot to an insert list,
				* but that shouldn't matter.
				*
				* The reason we bother with the test is because
				* overflows are expensive to write.  It's hard
				* to imagine a real workload where this test is
				* worth the effort, but it's a simple test.
				*/
				if (kpack != NULL && kpack->raw == WT_CELL_KEY_OVFL_RM)
					goto leaf_insert;

				/*
				* The on-page value will never be accessed,
				* write a placeholder record.
				*/
				WT_ERR(__rec_cell_build_val(session, r, "@", 1, (uint64_t)0));
			}
			else {
				val->buf.data = val_cell;
				val->buf.size = __wt_cell_total_len(vpack);
				val->cell_len = 0;
				val->len = val->buf.size;

				/* Track if page has overflow items. */
				if (vpack->ovfl)
					r->ovfl_items = 1;
			}
		}
		else {
			/*
			* If the original value was an overflow and we've not
			* already done so, discard it.  One complication: we
			* must cache a copy before discarding the on-disk
			* version if there's a transaction in the system that
			* might read the original value.
			*/
			if (vpack != NULL && vpack->ovfl && vpack->raw != WT_CELL_VALUE_OVFL_RM)
				WT_ERR(__wt_ovfl_cache(session, page, rip, vpack));

			/* If this key/value pair was deleted, we're done. */
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				/*
				* Overflow keys referencing discarded values
				* are no longer useful, discard the backing
				* blocks.  Don't worry about reuse, reusing
				* keys from a row-store page reconciliation
				* seems unlikely enough to ignore.
				*/
				if (kpack != NULL && kpack->ovfl && kpack->raw != WT_CELL_KEY_OVFL_RM) {
					/*
					* Keys are part of the name-space, we
					* can't remove them from the in-memory
					* tree; if an overflow key was deleted
					* without being instantiated (for
					* example, cursor-based truncation, do
					* it now.
					*/
					if (ikey == NULL)
						WT_ERR(__wt_row_leaf_key(session, page, rip, tmpkey, 1));

					WT_ERR(__wt_ovfl_discard_add(session, page, kpack->cell));
				}

				/*
				* We aren't actually creating the key so we
				* can't use bytes from this key to provide
				* prefix information for a subsequent key.
				*/
				tmpkey->size = 0;

				/* Proceed with appended key/value pairs. */
				goto leaf_insert;
			}

			/*
			* If no value, nothing needs to be copied.  Otherwise,
			* build the value's WT_CELL chunk from the most recent
			* update value.
			*/
			if (upd->size == 0) {
				val->buf.data = NULL;
				val->cell_len = val->len = val->buf.size = 0;
			}
			else {
				WT_ERR(__rec_cell_build_val(session, r, WT_UPDATE_DATA(upd), upd->size, (uint64_t)0));
				dictionary = 1;
			}
		}

		/*
		* Build key cell.
		*
		* If the key is an overflow key that hasn't been removed, use
		* the original backing blocks.
		*/
		key_onpage_ovfl = kpack != NULL && kpack->ovfl && kpack->raw != WT_CELL_KEY_OVFL_RM;
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = 1;

			/*
			* We aren't creating a key so we can't use this key as
			* a prefix for a subsequent key.
			*/
			tmpkey->size = 0;

			/* Track if page has overflow items. */
			r->ovfl_items = 1;
		}
		else {
			/*
			* Get the key from the page or an instantiated key, or
			* inline building the key from a previous key (it's a
			* fast path for simple, prefix-compressed keys), or by
			* by building the key from scratch.
			*/
			if (__wt_row_leaf_key_info(page, copy, NULL, &cell, &tmpkey->data, &tmpkey->size))
				goto build;

			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
			if (btree->huffman_key == NULL && kpack->type == WT_CELL_KEY && tmpkey->size >= kpack->prefix) {
				/*
				* The previous clause checked for a prefix of
				* zero, which means the temporary buffer must
				* have a non-zero size, and it references a
				* valid key.
				*/
				WT_ASSERT(session, tmpkey->size != 0);

				/*
				* Grow the buffer as necessary, ensuring data
				* data has been copied into local buffer space,
				* then append the suffix to the prefix already
				* in the buffer.
				*
				* Don't grow the buffer unnecessarily or copy
				* data we don't need, truncate the item's data
				* length to the prefix bytes.
				*/
				tmpkey->size = kpack->prefix;
				WT_ERR(__wt_buf_grow(session, tmpkey, tmpkey->size + kpack->size));
				memcpy((uint8_t *)tmpkey->mem + tmpkey->size, kpack->data, kpack->size);
				tmpkey->size += kpack->size;
			}
			else
				WT_ERR(__wt_row_leaf_key_copy(session, page, rip, tmpkey));
build:
			WT_ERR(__rec_cell_build_leaf_key(session, r, tmpkey->data, tmpkey->size, &ovfl_key));
		}

		/* Boundary: split or write the page. */
		if (key->len + val->len > r->space_avail) {
			if (r->raw_compression)
				WT_ERR(__rec_split_raw(session, r, key->len + val->len));
			else {
				/*
				* In one path above, we copied address blocks
				* from the page rather than building the actual
				* key.  In that case, we have to build the key
				* now because we are about to promote it.
				*/
				if (key_onpage_ovfl) {
					WT_ERR(__wt_dsk_cell_data_ref(session, WT_PAGE_ROW_LEAF, kpack, r->cur));
					key_onpage_ovfl = 0;
				}

				/*
				* Turn off prefix compression until a full key
				* written to the new page, and (unless already
				* working with an overflow key), rebuild the
				* key without compression.
				*/
				if (r->key_pfx_compress_conf) {
					r->key_pfx_compress = 0;
					if (!ovfl_key)
						WT_ERR(__rec_cell_build_leaf_key(session, r, NULL, 0, &ovfl_key));
				}

				WT_ERR(__rec_split(session, r, key->len + val->len));
			}
		}

		/* Copy the key/value pair onto the page. */
		__rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = 1;
		else {
			r->all_empty_value = 0;
			if (dictionary && btree->dictionary)
				WT_ERR(__rec_dict_replace(session, r, 0, val));
			__rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);

leaf_insert:	/* Write any K/V pairs inserted into the page after this key. */
		if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT(page, rip))) != NULL)
			WT_ERR(__rec_row_leaf_insert(session, r, ins));
	}

	/* Write the remnant page. */
	ret = __rec_split_finish(session, r);

err:	
	__wt_scr_free(session, &tmpkey);
	__wt_scr_free(session, &tmpval);
	return ret;
}

/*对对应的记录的update多版本reconcile操作*/
static int __rec_row_leaf_insert(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_INSERT* ins)
{
	WT_BTREE *btree;
	WT_KV *key, *val;
	WT_UPDATE *upd;
	int ovfl_key;

	btree = S2BT(session);

	key = &r->k;
	val = &r->v;

	for (; ins != NULL; ins = WT_SKIP_NEXT(ins)){
		/*找到本事务可见的update*/
		WT_RET(__rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL || WT_UPDATE_DELETED_ISSET(upd))
			continue;

		/*构建val值，并设置给r->v*/
		if (upd->size == 0)
			val->len = 0;
		else
			WT_RET(__rec_cell_build_val(session, r, WT_UPDATE_DATA(upd), upd->size, (uint64_t)0));

		/*构建key值，并设置给r->k*/
		WT_RET(__rec_cell_build_leaf_key(session, r, WT_INSERT_KEY(ins), WT_INSERT_KEY_SIZE(ins), &ovfl_key));

		/*boundary:split or write the page*/
		if (key->len + val->len > r->space_avail) {
			if (r->raw_compression)
				WT_RET(__rec_split_raw(session, r, key->len + val->len));
			else {
				/*
				* Turn off prefix compression until a full key
				* written to the new page, and (unless already
				* working with an overflow key), rebuild the
				* key without compression.
				*/
				if (r->key_pfx_compress_conf) {
					r->key_pfx_compress = 0;
					if (!ovfl_key)
						WT_RET(__rec_cell_build_leaf_key(session, r, NULL, 0, &ovfl_key));
				}

				WT_RET(__rec_split(session, r, key->len + val->len));
			}
		}
		/*将k/v写入到reconcile buf中*/
		__rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = 1;
		else {
			r->all_empty_value = 0;
			if (btree->dictionary)
				WT_RET(__rec_dict_replace(session, r, 0, val));
			__rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	}

	return 0;
}

/*废弃已经被split page*/
static int __rec_split_discard(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_BM *bm;
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;
	WT_MULTI *multi;
	uint32_t i;

	bm = S2BT(session)->bm;
	mod = page->modify;

	/*
	* A page that split is being reconciled for the second, or subsequent
	* time; discard underlying block space used in the last reconciliation
	* that is not being reused for this reconciliation.
	*/
	for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i){
		switch (page->type){
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			__wt_free(session, multi->key.ikey);
			break;
		}

		if (multi->skip == NULL){
			if (multi->addr.reuse)
				multi->addr.addr = NULL;
			else {
				WT_RET(bm->free(bm, session, multi->addr.addr, multi->addr.size));
				__wt_free(session, multi->addr.addr);
			}
		}
		else {
			__wt_free(session, multi->skip);
			__wt_free(session, multi->skip_dsk);
		}
	}
	__wt_free(session, mod->mod_multi);
	mod->mod_multi_entries = 0;

	switch (page->type){
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		if (mod->mod_root_split == NULL)
			break;
		WT_RET(__rec_split_discard(session, mod->mod_root_split));
		/*删除掉对应的ovfl track数据*/
		WT_RET(__wt_ovfl_track_wrapup(session, mod->mod_root_split));
		__wt_page_out(session, &mod->mod_root_split);
		break;
	}

	return ret;
}

/* Finish the reconciliation.*/
static int __rec_write_wrapup(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BM *bm;
	WT_BOUNDARY *bnd;
	WT_BTREE *btree;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	WT_REF *ref;
	size_t addr_size;
	const uint8_t *addr;

	btree = S2BT(session);
	bm = btree->bm;
	mod = page->modify;
	ref = r->ref;

	/*
	* This page may have previously been reconciled, and that information
	* is now about to be replaced.  Make sure it's discarded at some point,
	* and clear the underlying modification information, we're creating a
	* new reality.
	*/
	switch (F_ISSET(mod, WT_PM_REC_MASK)) {
	case 0:	/*
			* The page has never been reconciled before, free the original
			* address blocks (if any).  The "if any" is for empty trees
			* created when a new tree is opened or previously deleted pages
			* instantiated in memory.
			*
			* The exception is root pages are never tracked or free'd, they
			* are checkpoints, and must be explicitly dropped.
			*/
		if (__wt_ref_is_root(ref))
			break;
		if (ref->addr != NULL) {
			/*
			* Free the page and clear the address (so we don't free it twice).
			*/
			WT_RET(__wt_ref_info(session, ref, &addr, &addr_size, NULL));
			WT_RET(bm->free(bm, session, addr, addr_size));
			if (__wt_off_page(ref->home, ref->addr)) {
				__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
				__wt_free(session, ref->addr);
			}
			ref->addr = NULL;
		}
		break;
	case WT_PM_REC_EMPTY:				/* Page deleted */
		break;
	case WT_PM_REC_MULTIBLOCK:			/* Multiple blocks */
		/*
		* Discard the multiple replacement blocks.
		*/
		WT_RET(__rec_split_discard(session, page));
		break;
	case WT_PM_REC_REPLACE:				/* 1-for-1 page swap */
		/*
		* Discard the replacement leaf page's blocks.
		*
		* The exception is root pages are never tracked or free'd, they
		* are checkpoints, and must be explicitly dropped.
		*/
		if (!__wt_ref_is_root(ref))
			WT_RET(bm->free(bm, session, mod->mod_replace.addr, mod->mod_replace.size));

		/* Discard the replacement page's address. */
		__wt_free(session, mod->mod_replace.addr);
		mod->mod_replace.size = 0;
		break;

	WT_ILLEGAL_VALUE(session);
	}
	F_CLR(mod, WT_PM_REC_MASK);

	/*
	* Wrap up overflow tracking.  If we are about to create a checkpoint,
	* the system must be entirely consistent at that point (the underlying
	* block manager is presumably going to do some action to resolve the
	* list of allocated/free/whatever blocks that are associated with the
	* checkpoint).
	*/
	WT_RET(__wt_ovfl_track_wrapup(session, page));

	switch (r->bnd_next) {
	case 0:						/* Page delete */
		WT_RET(__wt_verbose(session, WT_VERB_RECONCILE, "page %p empty", page));
		WT_STAT_FAST_DATA_INCR(session, rec_page_delete);

		/* If this is the root page, we need to create a sync point. */
		ref = r->ref;
		if (__wt_ref_is_root(ref))
			WT_RET(bm->checkpoint(bm, session, NULL, btree->ckpt, 0));

		/*
		* If the page was empty, we want to discard it from the tree
		* by discarding the parent's key when evicting the parent.
		* Mark the page as deleted, then return success, leaving the
		* page in memory.  If the page is subsequently modified, that
		* is OK, we'll just reconcile it again.
		*/
		F_SET(mod, WT_PM_REC_EMPTY);
		break;
	case 1:						/* 1-for-1 page swap */
		/*
		* Because WiredTiger's pages grow without splitting, we're
		* replacing a single page with another single page most of
		* the time.
		*/
		bnd = &r->bnd[0];

		/*
		* If we're saving/restoring changes for this page, there's
		* nothing to write. Allocate, then initialize the array of
		* replacement blocks.
		*/
		if (bnd->skip != NULL) {
			WT_RET(__wt_calloc_def(session, r->bnd_next, &mod->mod_multi));
			multi = mod->mod_multi;
			multi->skip = bnd->skip;
			multi->skip_entries = bnd->skip_next;
			bnd->skip = NULL;
			multi->skip_dsk = bnd->dsk;
			bnd->dsk = NULL;
			mod->mod_multi_entries = 1;

			F_SET(mod, WT_PM_REC_MULTIBLOCK);
			break;
		}

		/*
		* If this is a root page, then we don't have an address and we
		* have to create a sync point.  The address was cleared when
		* we were about to write the buffer so we know what to do here.
		*/
		if (bnd->addr.addr == NULL)
			WT_RET(__wt_bt_write(session, &r->dsk, NULL, NULL, 1, bnd->already_compressed));
		else {
			mod->mod_replace = bnd->addr;
			bnd->addr.addr = NULL;
		}

		F_SET(mod, WT_PM_REC_REPLACE);
		break;
	default:					/* Page split */
		WT_RET(__wt_verbose(session, WT_VERB_RECONCILE, "page %p reconciled into %" PRIu32 " pages", page, r->bnd_next));

		switch (page->type) {
		case WT_PAGE_COL_INT:
		case WT_PAGE_ROW_INT:
			WT_STAT_FAST_DATA_INCR(session, rec_multiblock_internal);
			break;
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
		case WT_PAGE_ROW_LEAF:
			WT_STAT_FAST_DATA_INCR(session, rec_multiblock_leaf);
			break;
			WT_ILLEGAL_VALUE(session);
		}

		/* Display the actual split keys. */
		if (WT_VERBOSE_ISSET(session, WT_VERB_SPLIT)) {
			WT_DECL_ITEM(tkey);
			WT_DECL_RET;
			uint32_t i;

			if (page->type == WT_PAGE_ROW_INT || page->type == WT_PAGE_ROW_LEAF)
				WT_RET(__wt_scr_alloc(session, 0, &tkey));
			for (bnd = r->bnd, i = 0; i < r->bnd_next; ++bnd, ++i)
				switch (page->type) {
				case WT_PAGE_ROW_INT:
				case WT_PAGE_ROW_LEAF:
					WT_ERR(__wt_buf_set_printable(session, tkey, bnd->key.data, bnd->key.size));
					WT_ERR(__wt_verbose(session, WT_VERB_SPLIT, "split: starting key %.*s", (int)tkey->size, (const char *)tkey->data));
					break;
				case WT_PAGE_COL_FIX:
				case WT_PAGE_COL_INT:
				case WT_PAGE_COL_VAR:
					WT_ERR(__wt_verbose(session, WT_VERB_SPLIT,"split: starting recno %" PRIu64, bnd->recno));
					break;
				WT_ILLEGAL_VALUE_ERR(session);
			}
		err:			
			__wt_scr_free(session, &tkey);
			WT_RET(ret);
		}

		if (r->bnd_next > r->bnd_next_max) {
			r->bnd_next_max = r->bnd_next;
			WT_STAT_FAST_DATA_SET(session, rec_multiblock_max, r->bnd_next_max);
		}

		switch (page->type) {
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			WT_RET(__rec_split_row(session, r, page));
			break;
		case WT_PAGE_COL_INT:
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			WT_RET(__rec_split_col(session, r, page));
			break;
			WT_ILLEGAL_VALUE(session);
		}
		F_SET(mod, WT_PM_REC_MULTIBLOCK);
		break;
	}

	/*
	* If updates were skipped, the tree isn't clean.  The checkpoint call
	* cleared the tree's modified value before calling the eviction thread,
	* so we must explicitly reset the tree's modified flag.  We insert a
	* barrier after the change for clarity (the requirement is the value
	* be set before a subsequent checkpoint reads it, and because the
	* current checkpoint is waiting on this reconciliation to complete,
	* there's no risk of that happening).
	*
	* Otherwise, if no updates were skipped, we have a new maximum
	* transaction written for the page (used to decide if a clean page can
	* be evicted).  The page only might be clean; if the write generation
	* is unchanged since reconciliation started, clear it and update cache
	* dirty statistics, if the write generation changed, then the page has
	* been written since we started reconciliation, it cannot be
	* discarded.
	*/
	if (r->leave_dirty) {
		mod->first_dirty_txn = r->skipped_txn;

		btree->modified = 1;
		WT_FULL_BARRIER();
	}
	else {
		mod->rec_max_txn = r->max_txn;

		if (WT_ATOMIC_CAS4(mod->write_gen, r->orig_write_gen, 0))
			__wt_cache_dirty_decr(session, page);
	}

	return 0;
}

/*Finish the reconciliation on error.*/
static int __rec_write_wrapup_err(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BM *bm;
	WT_BOUNDARY *bnd;
	WT_DECL_RET;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	bm = S2BT(session)->bm;
	mod = page->modify;

	/*
	 * Clear the address-reused flag from the multiblock reconciliation
	 * information (otherwise we might think the backing block is being
	 * reused on a subsequent reconciliation where we want to free it).
	 */
	if(F_ISSET(mod, WT_PM_REC_MASK()) == WT_PM_REC_MULTIBLOCK){
		for(multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i)
			multi->addr.reuse = 0;
	}

	/*
	 * On error, discard blocks we've written, they're unreferenced by the
	 * tree.  This is not a question of correctness, we're avoiding block
	 * leaks.
	 *
	 * Don't discard backing blocks marked for reuse, they remain part of
	 * a previous reconciliation.
	 */
	WT_RET(__wt_ovfl_track_wrapup_err(session, page));
	for(bnd = r->bnd, i = 0; i < r->bnd_next; ++bnd, ++i){
		if (bnd->addr.addr != NULL) {
			if (bnd->addr.reuse)
				bnd->addr.addr = NULL;
			else {
				WT_TRET(bm->free(bm, session, bnd->addr.addr, bnd->addr.size));
				__wt_free(session, bnd->addr.addr);
			}
		}
	}

	return ret;
}

/*row store方式根据reconcile boundary进行split block确定*/
static int __rec_split_row(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_BOUNDARY *bnd;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	WT_REF *ref;
	uint32_t i;
	size_t size;
	void *p;

	mod = page->modify;

	ref = r->ref;
	/*计算boundary key*/
	if(__wt_ref_is_root(ref))
		WT_RET(__wt_buf_set(session, &r->bnd[0].key, "", 1));
	else{
		__wt_ref_key(ref->home, ref, &p, &size);
		WT_RET(__wt_buf_set(session, &r->bnd[0].key, p, size));
	}

	/* Allocate, then initialize the array of replacement blocks. */
	WT_RET(__wt_calloc_def(session, r->bnd_next, &mod->mod_multi));
	/*将boundary作为split边界，进行split block设置*/
	for(multi = mod->mod_multi, bnd = r->bnd, i = 0; i < r->bnd_next; ++multi, ++bnd, ++i){
		WT_RET(__wt_row_ikey_alloc(session, 0, bnd->key.data, bnd->key.size, &multi->key.ikey));

		if (bnd->skip == NULL) {
			multi->addr = bnd->addr;
			multi->addr.reuse = 0;
			multi->size = bnd->size;
			multi->cksum = bnd->cksum;
			bnd->addr.addr = NULL;
		} 
		else {
			multi->skip = bnd->skip;
			multi->skip_entries = bnd->skip_next;
			bnd->skip = NULL;
			multi->skip_dsk = bnd->dsk;
			bnd->dsk = NULL;
		}
	}

	mod->mod_multi_entries = r->bnd_next;

	return 0;
}

/*列式存储方式根据reconcile boundary进行split block确定*/
static int __rec_split_col(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_PAGE* page)
{
	WT_BOUNDARY *bnd;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	/* Allocate, then initialize the array of replacement blocks. */
	WT_RET(__wt_calloc_def(session, r->bnd_next, &mod->mod_multi));
	multi = mod->mod_multi;
	bnd = r->bnd;
	for(i = 0; i < r->bnd_next; ++multi, ++bnd, ++i){
		multi->key.recno = bnd->recno;
		if (bnd->skip == NULL) {
			multi->addr = bnd->addr;
			multi->addr.reuse = 0;
			multi->size = bnd->size;
			multi->cksum = bnd->cksum;
			bnd->addr.addr = NULL;
		} else {
			multi->skip = bnd->skip;
			multi->skip_entries = bnd->skip_next;
			bnd->skip = NULL;
			multi->skip_dsk = bnd->dsk;
			bnd->dsk = NULL;
		}
	}

	mod->mod_multi_entries = r->bnd_next;
	return 0;
}

/*通过data构建一个用于reconcile的internal key值*/
static int __rec_cell_build_int_key(WT_SESSION_IMPL* session, WT_RECONCILE* r, const void* data, size_t size, int* is_ovflp)
{
	WT_BTREE* btree;
	WT_KV* key;

	*is_ovflp = 0;
	btree = S2BT(session);

	key = &r->k;
	
	/* Copy the bytes into the "current" and key buffers. */
	WT_RET(__wt_buf_set(session, r->cur, data, size));
	WT_RET(__wt_buf_set(session, &key->buf, data, size));
	/*ovfl key，需要用另外的内存空间（cell）来做存储*/
	if(size > btree->maxintlkey){
		WT_STAT_FAST_DATA_INCR(session, rec_overflow_key_internal);

		*is_ovflp = 1;
		return (__rec_cell_build_ovfl(session, r, key, WT_CELL_KEY_OVFL, (uint64_t)0));
	}

	key->cell_len = __wt_cell_pack_int_key(&key->cell, key->buf.size);
	key->len = key->cell_len + key->buf.size;

	return 0;
}

/*通过data构建一个用于reconcile的leaf key值*/
static int __rec_cell_build_leaf_key(WT_SESSION_IMPL* session, WT_RECONCILE* r, const void* data, size_t size, int* is_ovflp)
{
	WT_BTREE *btree;
	WT_KV *key;
	size_t pfx_max;
	uint8_t pfx;
	const uint8_t *a, *b;

	*is_ovflp = 0;

	btree = S2BT(session);

	key = &r->k;

	pfx = 0;
	if(data == NULL){
		WT_RET(__wt_buf_set(session, &key->buf, r->cur->data, r->cur->size));
	}
	else{ /*进行key的前缀压缩*/
		WT_RET(__wt_buf_set(session, r->cur, data, size));
		/*
		 * Do prefix compression on the key.  We know by definition the
		 * previous key sorts before the current key, which means the
		 * keys must differ and we just need to compare up to the
		 * shorter of the two keys.
		 */
		if (r->key_pfx_compress) {
			/*
			 * We can't compress out more than 256 bytes, limit the comparison to that.
			 */
			pfx_max = UINT8_MAX;
			if (size < pfx_max)
				pfx_max = size;
			if (r->last->size < pfx_max)
				pfx_max = r->last->size;

			for (a = data, b = r->last->data; pfx < pfx_max; ++pfx)
				if (*a++ != *b++)
					break;

			/*
			 * Prefix compression may cost us CPU and memory when
			 * the page is re-loaded, don't do it unless there's
			 * reasonable gain.
			 */
			if (pfx < btree->prefix_compression_min)
				pfx = 0;
			else
				WT_STAT_FAST_DATA_INCRV(session, rec_prefix_compression, pfx);
		}
		/* Copy the non-prefix bytes into the key buffer. */
		WT_RET(__wt_buf_set(session, &key->buf, (uint8_t *)data + pfx, size - pfx));
	}

	/*huffman后缀压缩*/
	if(btree->huffman_key != NULL)
		WT_RET(__wt_huffman_encode(session, btree->huffman_key, key->buf.data, (uint32_t)key->buf.size, &key->buf));

	/* Create an overflow object if the data won't fit. */
	if (key->buf.size > btree->maxleafkey) {
		/*
		 * Overflow objects aren't prefix compressed -- rebuild any object that was prefix compressed.
		 */
		if (pfx == 0) {
			WT_STAT_FAST_DATA_INCR(session, rec_overflow_key_leaf);

			*is_ovflp = 1;
			return (__rec_cell_build_ovfl(session, r, key, WT_CELL_KEY_OVFL, (uint64_t)0));
		}
		return __rec_cell_build_leaf_key(session, r, NULL, 0, is_ovflp);
	}
}

/*构建一个reconcile的block addr的值*/
static void __rec_cell_build_addr(WT_RECONCILE* r, const void* addr, size_t size, u_int cell_type, uint64_t recno)
{
	WT_KV *val;
	val = &r->v;

	val->buf.data = addr;
	val->buf.size = size;
	val->cell_len = __wt_cell_pack_addr(&val->cell, cell_type, recno, val->buf.size);
	val->len = val->cell_len + val->buf.size;
}

/*通过data构建一个reconcile的value值*/
static int __rec_cell_build_val(WT_SESSION_IMPL* session, WT_RECONCILE* r, const void* data, size_t size, uint64_t rle)
{
	WT_BTREE *btree;
	WT_KV *val;

	btree = S2BT(session);

	val = &r->v;

	val->buf.data = data;
	val->buf.size = size;

	if(size != 0){
		/* Optionally compress the data using the Huffman engine. */
		if (btree->huffman_value != NULL)
			WT_RET(__wt_huffman_encode(ession, btree->huffman_value, val->buf.data, (uint32_t)val->buf.size, &val->buf));

		/* Create an overflow object if the data won't fit. */
		if (val->buf.size > btree->maxleafvalue) {
			WT_STAT_FAST_DATA_INCR(session, rec_overflow_value);
			return __rec_cell_build_ovfl(session, r, val, WT_CELL_VALUE_OVFL, rle);
		}
	}

	val->cell_len = __wt_cell_pack_data(&val->cell, rle, val->buf.size);
	val->len = val->cell_len + val->buf.size;

	return 0;
}

/*为ovfl的kv数据分配一个ovfl block并将block addr更新到kv对应关系中，以便reconcile过程建立关联关系*/
static int __rec_cell_build_ovfl(WT_SESSION_IMPL* session, WT_RECONCILE* r, WT_KV* kv, uint8_t type, uint64_t rle)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	size_t size;
	uint8_t *addr, buf[WT_BTREE_MAX_ADDR_COOKIE];

	btree = S2BT(session);
	bm = btree->bm;
	page = r->page;

	r->ovfl_items = 1;

	/*
	 * See if this overflow record has already been written and reuse it if
	 * possible.  Else, write a new overflow record.
	 */
	if(!__wt_ovfl_reuse_search(session, page, &addr, &size, kv->buf.data, kv->buf.size)){ /*没有可以复用的block,进行新建一个*/
		/* Allocate a buffer big enough to write the overflow record. */
		size = kv->buf.size;
		WT_RET(bm->write_size(bm, session, &size));
		WT_RET(__wt_scr_alloc(session, size, &tmp));

		/* Initialize the buffer: disk header and overflow record. */
		dsk = tmp->mem;
		memset(dsk, 0, WT_PAGE_HEADER_SIZE);
		dsk->type = WT_PAGE_OVFL;
		dsk->u.datalen = (uint32_t)kv->buf.size;
		memcpy(WT_PAGE_HEADER_BYTE(btree, dsk), kv->buf.data, kv->buf.size);
		dsk->mem_size = tmp->size = WT_PAGE_HEADER_BYTE_SIZE(btree) + (uint32_t)kv->buf.size;

		/* Write the buffer. */
		addr = buf;
		WT_ERR(__wt_bt_write(session, tmp, addr, &size, 0, 0));

		/*
		 * Track the overflow record (unless it's a bulk load, which
		 * by definition won't ever reuse a record.
		 */
		if (!r->is_bulk_load)
			WT_ERR(__wt_ovfl_reuse_add(session, page, addr, size, kv->buf.data, kv->buf.size));
	}

	/* Set the callers K/V to reference the overflow record's address. */
	WT_ERR(__wt_buf_set(session, &kv->buf, addr, size));

	/* Build the cell and return. */
	kv->cell_len = __wt_cell_pack_ovfl(&kv->cell, type, rle, kv->buf.size);
	kv->len = kv->cell_len + kv->buf.size;

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*对dirctionary调表做检索查找，dirctionary是用来做多个KEY关联同样的值去重用了，防止存储空间浪费*/
static WT_DICTIONARY* __rec_dictionary_skip_search(WT_DICTIONARY **head, uint64_t hash)
{
	WT_DICTIONARY **e;
	int i;

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
		 * Return any exact matches: we don't care in what search level we found a match.
		 */
		if ((*e)->hash == hash)		/* Exact match */
			return (*e);
		if ((*e)->hash > hash) {	/* Drop down a level */
			--i;
			--e;
		} else				/* Keep going at this level */
			e = &(*e)->next[i];
	}
	return NULL;
}

/*为insert/remove操作定位skiplist的位置*/
static void __rec_dictionary_skip_search_stack(WT_DICTIONARY** head, WT_DICTIONARY ***stack, uint64_t hash)
{
	WT_DICTIONARY **e;
	int i;

	/*
	 * Start at the highest skip level, then go as far as possible at each level before stepping down to the next.
	 */
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;){
		if (*e == NULL || (*e)->hash > hash)
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
	}
}

/*插入一个dictionary实例到skiplist中*/
static void __rec_dictionary_skip_insert(WT_DICTIONARY** head, WT_DICTIONARY* e, uint64_t hash)
{
	WT_DICTIONARY **stack[WT_SKIP_MAXDEPTH];
	u_int i;

	/* Insert the new entry into the skiplist. */
	__rec_dictionary_skip_search_stack(head, stack, hash);
	for (i = 0; i < e->depth; ++i) {
		e->next[i] = *stack[i];
		*stack[i] = e;
	}
}

/*初始化reconcile中的dictionary管理器*/
static int __rec_dictionary_init(WT_SESSION_IMPL *session, WT_RECONCILE *r, u_int slots)
{
	u_int depth, i;

	/* Free any previous dictionary. */
	__rec_dictionary_free(session, r);

	r->dictionary_slots = slots;
	WT_RET(__wt_calloc(session, r->dictionary_slots, sizeof(WT_DICTIONARY *), &r->dictionary));
	for (i = 0; i < r->dictionary_slots; ++i) {
		depth = __wt_skip_choose_depth(session);
		WT_RET(__wt_calloc(session, 1, sizeof(WT_DICTIONARY) + depth * sizeof(WT_DICTIONARY *), &r->dictionary[i]));
		r->dictionary[i]->depth = depth;
	}
	return 0;
}

/*撤销reconcile中的dictionary管理器*/
static void __rec_dictionary_free(WT_SESSION_IMPL* session, WT_RECONCILE* r)
{
	u_int i;

	if (r->dictionary == NULL)
		return;

	/*
	 * We don't correct dictionary_slots when we fail during allocation,
	 * but that's OK, the value is either NULL or a memory reference to
	 * be free'd.
	 */
	for (i = 0; i < r->dictionary_slots; ++i)
		__wt_free(session, r->dictionary[i]);
	__wt_free(session, r->dictionary);
}

static void __rec_dictionary_reset(WT_RECONCILE* r)
{
	if (r->dictionary_slots) {
		r->dictionary_next = 0;
		memset(r->dictionary_head, 0, sizeof(r->dictionary_head));
	}
}

/*根据val值在dictionary中查找是否有相同的值已经被reconcile了，如果已经进行了这个操作，那么只要需要保存一个关联引用即可*/
static int __rec_dictionary_lookup(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_KV *val, WT_DICTIONARY **dpp)
{
	WT_DICTIONARY *dp, *next;
	uint64_t hash;
	int match;

	*dpp = NULL;

	/* Search the dictionary, and return any match we find. */
	hash = __wt_hash_fnv64(val->buf.data, val->buf.size);
	for (dp = __rec_dictionary_skip_search(r->dictionary_head, hash); dp != NULL && dp->hash == hash; dp = dp->next[0]) {
		WT_RET(__wt_cell_pack_data_match(dp->cell, &val->cell, val->buf.data, &match));
		if (match) {
			WT_STAT_FAST_DATA_INCR(session, rec_dictionary);
			*dpp = dp;
			return (0);
		}
	}

	/*
	 * We're not doing value replacement in the dictionary.  We stop adding
	 * new entries if we run out of empty dictionary slots (but continue to
	 * use the existing entries).  I can't think of any reason a leaf page
	 * value is more likely to be seen because it was seen more recently
	 * than some other value: if we find working sets where that's not the
	 * case, it shouldn't be too difficult to maintain a pointer which is
	 * the next dictionary slot to re-use.
	 */
	if (r->dictionary_next >= r->dictionary_slots)
		return 0;

	/*
	 * Set the hash value, we'll add this entry into the dictionary when we
	 * write it into the page's disk image buffer (because that's when we
	 * know where on the page it will be written).
	 */
	next = r->dictionary[r->dictionary_next++];
	next->cell = NULL;		/* Not necessary, just cautious. */
	next->hash = hash;
	__rec_dictionary_skip_insert(r->dictionary_head, next, hash);
	*dpp = next;

	return 0;
}









