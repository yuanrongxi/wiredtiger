/***********************************************************************
* 对block dsk中数据的错误检查实现
***********************************************************************/
#include "wt_internal.h"


#define	WT_ERR_VRFY(session, ...) do {							\
	if (!(F_ISSET(session, WT_SESSION_SALVAGE_CORRUPT_OK)))		\
	__wt_errx(session, __VA_ARGS__);							\
	goto err;													\
} while (0)

#define	WT_RET_VRFY(session, ...) do {							\
	if (!(F_ISSET(session, WT_SESSION_SALVAGE_CORRUPT_OK)))		\
	__wt_errx(session, __VA_ARGS__);							\
	return (WT_ERROR);											\
} while (0)

static int __err_cell_corrupted(WT_SESSION_IMPL *, uint32_t, const char *);
static int __err_cell_type(WT_SESSION_IMPL *, uint32_t, const char *, uint8_t, uint8_t);
static int __err_eof(WT_SESSION_IMPL *, uint32_t, const char *);
static int __verify_dsk_chunk(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *, uint32_t);
static int __verify_dsk_col_fix(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *);
static int __verify_dsk_col_int(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *);
static int __verify_dsk_col_var(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *);
static int __verify_dsk_memsize(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *, WT_CELL *);
static int __verify_dsk_row(WT_SESSION_IMPL *, const char *, const WT_PAGE_HEADER *);



/*对单个block的错误校验*/
int __wt_verify_dsk_image(WT_SESSION_IMPL *session, const char *addr, const WT_PAGE_HEADER *dsk, size_t size, int empty_page_ok)
{
	const uint8_t *p, *end;
	u_int i;
	uint8_t flags;

	/*校验page header中的page type*/
	switch(dsk->type){
		case WT_PAGE_BLOCK_MANAGER:
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_INT:
		case WT_PAGE_COL_VAR:
		case WT_PAGE_OVFL:
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			break;

		case WT_PAGE_INVALID:
		default:
			WT_RET_VRFY(session, "page at %s has an invalid type of %" PRIu32, addr, dsk->type);
	}

	/*校验记录数,只有column才会有recno不为零的情况*/
	switch(dsk->type){
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_INT:
	case WT_PAGE_COL_VAR:
		if (dsk->recno != 0)
			break;
		WT_RET_VRFY(session, "%s page at %s has a record number of zero", __wt_page_type_string(dsk->type), addr);

	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_OVFL:
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		if (dsk->recno == 0)
			break;
		WT_RET_VRFY(session, "%s page at %s has a non-zero record number", __wt_page_type_string(dsk->type), addr);
	}

	/* 检查page flags,将所有固定的disk page属性全部注销掉,如果不为0，说明有其他内存属性，page应该是错误的*/
	flags = dsk->flags;
	if (LF_ISSET(WT_PAGE_COMPRESSED))
		LF_CLR(WT_PAGE_COMPRESSED);

	if (dsk->type == WT_PAGE_ROW_LEAF) {
		if (LF_ISSET(WT_PAGE_EMPTY_V_ALL) &&LF_ISSET(WT_PAGE_EMPTY_V_NONE))
			WT_RET_VRFY(session, "page at %s has invalid flags combination: 0x%" PRIx8, addr, dsk->flags);

		if (LF_ISSET(WT_PAGE_EMPTY_V_ALL))
			LF_CLR(WT_PAGE_EMPTY_V_ALL);

		if (LF_ISSET(WT_PAGE_EMPTY_V_NONE))
			LF_CLR(WT_PAGE_EMPTY_V_NONE);
	}

	if (flags != 0)
		WT_RET_VRFY(session,"page at %s has invalid flags set: 0x%" PRIx8, addr, flags);

	/*对disk unused bytes的校验*/
	for (p = dsk->unused, i = sizeof(dsk->unused); i > 0; --i){
		if (*p != '\0')
			WT_RET_VRFY(session,"page at %s has non-zero unused page header bytes", addr);
	}

	/*
	 * Any bytes after the data chunk should be nul bytes; ignore if the
	 * size is 0, that allows easy checking of disk images where we don't
	 * have the size.
	 */
	if (size != 0) {
		p = (uint8_t *)dsk + dsk->mem_size;
		end = (uint8_t *)dsk + size;
		for (; p < end; ++p){
			if (*p != '\0')
				WT_RET_VRFY(session, "%s page at %s has non-zero trailing bytes", __wt_page_type_string(dsk->type), addr);
		}
	}

	/*空页校验*/
	switch (dsk->type) {
	case WT_PAGE_COL_INT:
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_VAR:
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		if (!empty_page_ok && dsk->u.entries == 0) /*传入的标示不为空页，但是entry个数为0*/
			WT_RET_VRFY(session, "%s page at %s has no entries", __wt_page_type_string(dsk->type), addr);
		break;

	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_OVFL:
		if (dsk->u.datalen == 0) /*overflow page一定是有数据的*/
			WT_RET_VRFY(session, "%s page at %s has no data", __wt_page_type_string(dsk->type), addr);
		break;
	}

	switch (dsk->type) {
	case WT_PAGE_COL_INT:
		return (__verify_dsk_col_int(session, addr, dsk));
	case WT_PAGE_COL_FIX:
		return (__verify_dsk_col_fix(session, addr, dsk));
	case WT_PAGE_COL_VAR:
		return (__verify_dsk_col_var(session, addr, dsk));
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		return (__verify_dsk_row(session, addr, dsk));
	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_OVFL:
		return (__verify_dsk_chunk(session, addr, dsk, dsk->u.datalen));
		WT_ILLEGAL_VALUE(session);
	}
}

/*校验从磁盘上读取page时间校验btree上的page*/
int __wt_verify_dsk(WT_SESSION_IMPL* session, const char* addr, WT_ITEM* buf)
{
	return __wt_verify_dsk_image(session, addr, buf->data, buf->size, 0);
}

/*扫描row store方式btree的所有pages,并进行page中记录的错误校验*/
static int __verify_dsk_row(WT_SESSION_IMPL *session, const char *addr, const WT_PAGE_HEADER *dsk)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_ITEM(current);
	WT_DECL_ITEM(last_ovfl);
	WT_DECL_ITEM(last_pfx);
	WT_DECL_RET;
	WT_ITEM *last;
	enum { FIRST, WAS_KEY, WAS_VALUE } last_cell_type;
	void *huffman;
	uint32_t cell_num, cell_type, i, key_cnt, prefix;
	uint8_t *end;
	int cmp;

	btree = S2BT(session);
	bm = btree->bm;
	unpack = &_unpack;
	huffman = dsk->type == WT_PAGE_ROW_INT ? NULL : btree->huffman_key;

	WT_ERR(__wt_scr_alloc(session, 0, &current));
	WT_ERR(__wt_scr_alloc(session, 0, &last_pfx));
	WT_ERR(__wt_scr_alloc(session, 0, &last_ovfl));

	last = last_ovfl;

	end = (uint8_t*)dsk + dsk->mem_size;
	last_cell_type = FIRST;
	cell_num = 0;
	key_cnt = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i){
		++ cell_num;
		/*对cell做错误检查,如果发现cell错误，报告错误*/
		if(__wt_cell_unpack_safe(cell, unpack, end) != 0){
			ret = __err_cell_corrupted(session, cell_num, addr);
			goto err;
		}

		/* 对cell type错误检查. */
		WT_ERR(__err_cell_type(session, cell_num, addr, unpack->raw, dsk->type));
		WT_ERR(__err_cell_type(session, cell_num, addr, unpack->type, dsk->type));
		cell_type = unpack->type;

		switch (cell_type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
			++key_cnt;
			
			switch (last_cell_type) {
			case FIRST:
			case WAS_VALUE:
				break;
			case WAS_KEY:
				if (dsk->type == WT_PAGE_ROW_LEAF)
					break;
				WT_ERR_VRFY(session, "cell %" PRIu32 " on page at %s is the first of two adjacent keys", cell_num - 1, addr);
			}
			last_cell_type = WAS_KEY;
			break;

		case WT_CELL_ADDR_DEL:
		case WT_CELL_ADDR_INT:
		case WT_CELL_ADDR_LEAF:
		case WT_CELL_ADDR_LEAF_NO:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
			switch (last_cell_type) {
			case FIRST:
				WT_ERR_VRFY(session, "page at %s begins with a value", addr);
			case WAS_KEY:
				break;
			case WAS_VALUE:
				WT_ERR_VRFY(session, "cell %" PRIu32 " on page at %s is the first of two adjacent values", cell_num - 1, addr);
			}
			last_cell_type = WAS_VALUE;
			break;
		}

		/* Check if any referenced item has a valid address. */
		switch (cell_type) {
		case WT_CELL_ADDR_DEL:
		case WT_CELL_ADDR_INT:
		case WT_CELL_ADDR_LEAF:
		case WT_CELL_ADDR_LEAF_NO:
		case WT_CELL_KEY_OVFL:
		case WT_CELL_VALUE_OVFL:
			if (!bm->addr_valid(bm, session, unpack->data, unpack->size))
				goto eof;
			break;
		}

		/*
		 * Remaining checks are for key order and prefix compression.
		 * If this cell isn't a key, we're done, move to the next cell.
		 * If this cell is an overflow item, instantiate the key and
		 * compare it with the last key.   Otherwise, we have to deal
		 * with prefix compression.
		 */
		switch (cell_type) {
		case WT_CELL_KEY:
			break;
		case WT_CELL_KEY_OVFL:
			WT_ERR(__wt_dsk_cell_data_ref(session, dsk->type, unpack, current));
			goto key_compare;
		default:
			/* Not a key -- continue with the next cell. */
			continue;
		}

		/*
		 * Prefix compression checks.
		 *
		 * Confirm the first non-overflow key on a page has a zero
		 * prefix compression count.
		 */
		prefix = unpack->prefix;
		if (last_pfx->size == 0 && prefix != 0)
			WT_ERR_VRFY(session,
			    "the %" PRIu32 " key on page at %s is the first "
			    "non-overflow key on the page and has a non-zero "
			    "prefix compression value",
			    cell_num, addr);

		/* Confirm the prefix compression count is possible. */
		if (cell_num > 1 && prefix > last->size)
			WT_ERR_VRFY(session,
			    "key %" PRIu32 " on page at %s has a prefix "
			    "compression count of %" PRIu32 ", larger than "
			    "the length of the previous key, %" WT_SIZET_FMT,
			    cell_num, addr, prefix, last->size);

		/*
		 * If Huffman decoding required, unpack the cell to build the
		 * key, then resolve the prefix.  Else, we can do it faster
		 * internally because we don't have to shuffle memory around as
		 * much.
		 */
		if (huffman != NULL) {
			WT_ERR(__wt_dsk_cell_data_ref(session, dsk->type, unpack, current));

			/*
			 * If there's a prefix, make sure there's enough buffer
			 * space, then shift the decoded data past the prefix
			 * and copy the prefix into place.  Take care with the
			 * pointers: current->data may be pointing inside the
			 * buffer.
			 */
			if (prefix != 0) {
				WT_ERR(__wt_buf_grow(session, current, prefix + current->size));
				memmove((uint8_t *)current->mem + prefix, current->data, current->size);
				memcpy(current->mem, last->data, prefix);
				current->data = current->mem;
				current->size += prefix;
			}
		} 
		else {
			/*
			 * Get the cell's data/length and make sure we have
			 * enough buffer space.
			 */
			WT_ERR(__wt_buf_init(session, current, prefix + unpack->size));

			/* Copy the prefix then the data into place. */
			if (prefix != 0)
				memcpy(current->mem, last->data, prefix);
			memcpy((uint8_t *)current->mem + prefix, unpack->data, unpack->size);
			current->size = prefix + unpack->size;
		}

key_compare:	
		/*
		 * Compare the current key against the last key.
		 *
		 * Be careful about the 0th key on internal pages: we only store
		 * the first byte and custom collators may not be able to handle
		 * truncated keys.
		 */
		if ((dsk->type == WT_PAGE_ROW_INT && cell_num > 3) || (dsk->type != WT_PAGE_ROW_INT && cell_num > 1)) {
			WT_ERR(__wt_compare(session, btree->collator, last, current, &cmp));
			if (cmp >= 0)
				WT_ERR_VRFY(session, "the %" PRIu32 " and %" PRIu32 " keys on page at %s are incorrectly sorted", cell_num - 2, cell_num, addr);
		}

		/*
		 * Swap the buffers: last always references the last key entry,
		 * last_pfx and last_ovfl reference the last prefix-compressed
		 * and last overflow key entries.  Current gets pointed to the
		 * buffer we're not using this time around, which is where the
		 * next key goes.
		 */
		last = current;
		if (cell_type == WT_CELL_KEY) {
			current = last_pfx;
			last_pfx = last;
		} 
		else {
			current = last_ovfl;
			last_ovfl = last;
		}
		WT_ASSERT(session, last != current);
	}

	WT_ERR(__verify_dsk_memsize(session, addr, dsk, cell));

	if (dsk->type == WT_PAGE_ROW_INT && key_cnt * 2 != dsk->u.entries)
		WT_ERR_VRFY(session,
		"%s page at %s has a key count of %" PRIu32 " and a "
		"physical entry count of %" PRIu32,
		__wt_page_type_string(dsk->type),
		addr, key_cnt, dsk->u.entries);

	if (dsk->type == WT_PAGE_ROW_LEAF &&
		F_ISSET(dsk, WT_PAGE_EMPTY_V_ALL) &&
		key_cnt != dsk->u.entries)
		WT_ERR_VRFY(session,
		"%s page at %s with the 'all empty values' flag set has a "
		"key count of %" PRIu32 " and a physical entry count of %"
		PRIu32,
		__wt_page_type_string(dsk->type),
		addr, key_cnt, dsk->u.entries);

	if (dsk->type == WT_PAGE_ROW_LEAF &&
		F_ISSET(dsk, WT_PAGE_EMPTY_V_NONE) &&
		key_cnt * 2 != dsk->u.entries)
		WT_ERR_VRFY(session,
		"%s page at %s with the 'no empty values' flag set has a "
		"key count of %" PRIu32 " and a physical entry count of %"
		PRIu32,
		__wt_page_type_string(dsk->type),
		addr, key_cnt, dsk->u.entries);

	if(0){
eof:
		ret = __err_eof(session, cell_num, addr);
	}

	if(0){
err:
		if(ret == 0) ret = WT_ERROR;
	}

	__wt_scr_free(session, &current);
	__wt_scr_free(session, &last_pfx);
	__wt_scr_free(session, &last_ovfl);
	return (ret);
}

/*对一个WT_PAGE_COL_INT类型的BLOCK disk进行校验*/
static int __verify_dsk_col_int(WT_SESSION_IMPL* session, const char* addr, const WT_PAGE_HEADER* dsk)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	uint32_t cell_num, i;
	uint8_t *end;

	btree = S2BT(session);
	bm = btree->bm;
	unpack = &_unpack;
	end = (uint8_t *)dsk + dsk->mem_size;

	cell_num = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i){
		++cell_num;

		/* Carefully unpack the cell. */
		if (__wt_cell_unpack_safe(cell, unpack, end) != 0)
			return (__err_cell_corrupted(session, cell_num, addr));

		/* Check the raw and collapsed cell types. */
		WT_RET(__err_cell_type(session, cell_num, addr, unpack->raw, dsk->type));
		WT_RET(__err_cell_type(session, cell_num, addr, unpack->type, dsk->type));

		/* Check if any referenced item is entirely in the file. */
		if (!bm->addr_valid(bm, session, unpack->data, unpack->size))
			return (__err_eof(session, cell_num, addr));
	}

	WT_RET(__verify_dsk_memsize(session, addr, dsk, cell));
	return 0;
}

/*对WT_PAGE_COL_FIX类型的block做错误校验*/
static int __verify_dsk_col_fix(WT_SESSION_IMPL* session, const char* addr, const WT_PAGE_HEADER* dsk)
{
	WT_BTREE *btree;
	uint32_t datalen;

	btree = S2BT(session);

	datalen = __bitstr_size(btree->bitcnt * dsk->u.entries);
	return (__verify_dsk_chunk(session, addr, dsk, datalen));
}

/*对WT_PAGE_COL_VAR类型的block做错误校验,会进行整个page扫描*/
static int __verify_dsk_col_var(WT_SESSION_IMPL *session, const char *addr, const WT_PAGE_HEADER *dsk)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	size_t last_size;
	uint32_t cell_num, cell_type, i;
	int last_deleted;
	const uint8_t *last_data;
	uint8_t *end;

	btree = S2BT(session);
	bm = btree->bm;
	unpack = &_unpack;
	end = (uint8_t *)dsk + dsk->mem_size;

	last_data = NULL;
	last_size = 0;
	last_deleted = 0;

	cell_num = 0;

	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;

		/* Carefully unpack the cell. */
		if (__wt_cell_unpack_safe(cell, unpack, end) != 0)
			return (__err_cell_corrupted(session, cell_num, addr));

		/* Check the raw and collapsed cell types. */
		WT_RET(__err_cell_type(session, cell_num, addr, unpack->raw, dsk->type));
		WT_RET(__err_cell_type(session, cell_num, addr, unpack->type, dsk->type));
		cell_type = unpack->type;

		/* Check if any referenced item is entirely in the file. */
		if (cell_type == WT_CELL_VALUE_OVFL &&!bm->addr_valid(bm, session, unpack->data, unpack->size))
			return (__err_eof(session, cell_num, addr));

		/*
		 * Compare the last two items and see if reconciliation missed
		 * a chance for RLE encoding.  We don't have to care about data
		 * encoding or anything else, a byte comparison is enough.
		 */
		if (last_deleted == 1) {
			if (cell_type == WT_CELL_DEL)
				goto match_err;
		}
		else{
			if (cell_type == WT_CELL_VALUE && last_data != NULL && last_size == unpack->size && memcmp(last_data, unpack->data, last_size) == 0)
match_err:			WT_RET_VRFY(session,
					"data entries %" PRIu32 " and %" PRIu32
					" on page at %s are identical and should "
					"have been run-length encoded",
					cell_num - 1, cell_num, addr);

			switch (cell_type) {
			case WT_CELL_DEL:
				last_deleted = 1;
				last_data = NULL;
				break;
			case WT_CELL_VALUE_OVFL:
				last_deleted = 0;
				last_data = NULL;
				break;
			case WT_CELL_VALUE:
				last_deleted = 0;
				last_data = unpack->data;
				last_size = unpack->size;
				break;
			}
		}
	}

	WT_RET(__verify_dsk_memsize(session, addr, dsk, cell));
	return 0;
}

/*校验最后一个cell与dsk之间的内存关系，也就是说最后一个cell一定是和dsk->memszie的偏移是重合的，否则dsk是有问题的*/
static int __verify_dsk_memsize(WT_SESSION_IMPL* session, const char* addr, const WT_PAGE_HEADER* dsk, WT_CELL* cell)
{
	size_t len;

	/*
	 * We use the fact that cells exactly fill a page to detect the case of
	 * a row-store leaf page where the last cell is a key (that is, there's
	 * no subsequent value cell).  Check for any page type containing cells.
	 */
	len = WT_PTRDIFF((uint8_t *)dsk + dsk->mem_size, cell);
	if(len == 0)
		return 0;

	WT_RET_VRFY(session,"%s page at %s has %" WT_SIZET_FMT " unexpected bytes of data after the last cell", 
				__wt_page_type_string(dsk->type), addr, len);
}

/* 对btree page对应的chunk数据做错误校验 */
static int __verify_dsk_chunk(WT_SESSION_IMPL *session, const char *addr, const WT_PAGE_HEADER *dsk, uint32_t datalen)
{
	WT_BTREE *btree;
	uint8_t *p, *end;

	btree = S2BT(session);
	end = (uint8_t *)dsk + dsk->mem_size;

	/*校验有效的数据范围空间*/
	p = WT_PAGE_HEADER_BYTE(btree, dsk);
	if(p + datalen > end)
		WT_RET_VRFY(session, "data on page at %s extends past the end of the page", addr);

	/*未占用的空间一定是0*/
	for(p += datalen; p < end; ++p){
		if (*p != '\0')
			WT_RET_VRFY(session, "%s page at %s has non-zero trailing bytes", __wt_page_type_string(dsk->type), addr);
	}

	return 0;
}

static int __err_cell_corrupted(WT_SESSION_IMPL* session, uint32_t entry_num, const char* addr)
{
	/*抛出一个cell错误消息*/
	WT_RET_VRFY(session, "item %" PRIu32 " on page at %s is a corrupted cell", entry_num, addr);
}

/* cell type 对应的cell与cell所处在的page type的匹配错误检查*/
static int __err_cell_type(WT_SESSION_IMPL *session, uint32_t entry_num, const char *addr, uint8_t cell_type, uint8_t dsk_type)
{
	switch (cell_type) {
	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_INT:
	case WT_CELL_ADDR_LEAF:
	case WT_CELL_ADDR_LEAF_NO:
		if (dsk_type == WT_PAGE_COL_INT || dsk_type == WT_PAGE_ROW_INT)
			return (0);
		break;
	case WT_CELL_DEL:
		if (dsk_type == WT_PAGE_COL_VAR)
			return (0);
		break;
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_SHORT:
		if (dsk_type == WT_PAGE_ROW_INT || dsk_type == WT_PAGE_ROW_LEAF)
			return (0);
		break;
	case WT_CELL_KEY_PFX:
	case WT_CELL_KEY_SHORT_PFX:
		if (dsk_type == WT_PAGE_ROW_LEAF)
			return (0);
		break;
	case WT_CELL_KEY_OVFL_RM:
	case WT_CELL_VALUE_OVFL_RM:
		/*
		 * Removed overflow cells are in-memory only, it's an error to
		 * ever see one on a disk page.
		 */
		break;
	case WT_CELL_VALUE:
	case WT_CELL_VALUE_COPY:
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_SHORT:
		if (dsk_type == WT_PAGE_COL_VAR || dsk_type == WT_PAGE_ROW_LEAF)
			return (0);
		break;
	default:
		break;
	}

	WT_RET_VRFY(session,
	    "illegal cell and page type combination: cell %" PRIu32
	    " on page at %s is a %s cell on a %s page",
	    entry_num, addr, __wt_cell_type_string(cell_type), __wt_page_type_string(dsk_type));
}

/* 抛出一个block file文件错误的消息 */
static int __err_eof(WT_SESSION_IMPL* session, uint32_t entry_num, const char* addr)
{
	WT_RET_VRFY(session,
		"off-page item %" PRIu32
		" on page at %s references non-existent file pages",
		entry_num, addr);
}


