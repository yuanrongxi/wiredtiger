/*
 * WT_CELL --
 *	Variable-length cell type.
 *
 * Pages containing variable-length keys or values data (the WT_PAGE_ROW_INT,
 * WT_PAGE_ROW_LEAF, WT_PAGE_COL_INT and WT_PAGE_COL_VAR page types), have
 * cells after the page header.
 *
 * There are 4 basic cell types: keys and data (each of which has an overflow
 * form), deleted cells and off-page references.  The cell is usually followed
 * by additional data, varying by type: a key or data cell is followed by a set
 * of bytes, an address cookie follows overflow or off-page cells.
 *
 * Deleted cells are place-holders for column-store files, where entries cannot
 * be removed in order to preserve the record count.
 *
 * Here's the cell use by page type:
 *
 * WT_PAGE_ROW_INT (row-store internal page):
 *	Keys and offpage-reference pairs (a WT_CELL_KEY or WT_CELL_KEY_OVFL
 * cell followed by a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_ROW_LEAF (row-store leaf page):
 *	Keys with optional data cells (a WT_CELL_KEY or WT_CELL_KEY_OVFL cell,
 *	normally followed by a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell).
 *
 *	WT_PAGE_ROW_LEAF pages optionally prefix-compress keys, using a single
 *	byte count immediately following the cell.
 *
 * WT_PAGE_COL_INT (Column-store internal page):
 *	Off-page references (a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_COL_VAR (Column-store leaf page storing variable-length cells):
 *	Data cells (a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell), or deleted
 * cells (a WT_CELL_DEL cell).
 *
 * Each cell starts with a descriptor byte:
 *
 * Bits 1 and 2 are reserved for "short" key and value cells (that is, a cell
 * carrying data less than 64B, where we can store the data length in the cell
 * descriptor byte):
 *	0x00	Not a short key/data cell
 *	0x01	Short key cell
 *	0x10	Short key cell, with a following prefix-compression byte
 *	0x11	Short value cell
 * In these cases, the other 6 bits of the descriptor byte are the data length.
 *
 * Bit 3 marks an 8B packed, uint64_t value following the cell description byte.
 * (A run-length counter or a record number for variable-length column store.)
 *
 * Bit 4 is unused.
 *
 * Bits 5-8 are cell "types".
 */

#define WT_CELL_KEY_SHORT				0x01
#define WT_CELL_KEY_SHORT_PFX			0x02
#define WT_CELL_VALUE_SHORT				0x03
#define WT_CELL_SHORT_TYPE(v)			((v) & 0x03U)

#define WT_CELL_SHORT_MAX				63
#define WT_CELL_SHORT_SHIFT				2
#define WT_CELL_64V						0x04

#define WT_CELL_UNUSED_BIT4				0x08

#define	WT_CELL_ADDR_DEL				(0)		/* Address: deleted */
#define	WT_CELL_ADDR_INT				(1 << 4)	/* Address: internal  */
#define	WT_CELL_ADDR_LEAF				(2 << 4)	/* Address: leaf */
#define	WT_CELL_ADDR_LEAF_NO			(3 << 4)	/* Address: leaf no overflow */
#define	WT_CELL_DEL						(4 << 4)	/* Deleted value */
#define	WT_CELL_KEY						(5 << 4)	/* Key */
#define	WT_CELL_KEY_OVFL				(6 << 4)	/* Overflow key */
#define	WT_CELL_KEY_OVFL_RM				(12 << 4)	/* Overflow key (removed) */
#define	WT_CELL_KEY_PFX					(7 << 4)	/* Key with prefix byte */
#define	WT_CELL_VALUE					(8 << 4)	/* Value */
#define	WT_CELL_VALUE_COPY				(9 << 4)	/* Value copy */
#define	WT_CELL_VALUE_OVFL				(10 << 4)	/* Overflow value */
#define	WT_CELL_VALUE_OVFL_RM			(11 << 4)	/* Overflow value (removed) */

#define	WT_CELL_TYPE_MASK				(0x0fU << 4)	/* Maximum 16 cell types */
#define	WT_CELL_TYPE(v)					((v) & WT_CELL_TYPE_MASK)

#define WT_CELL_SIZE_ADJUST				64

struct __wt_cell
{
	/*
	 *第一个字节					cell类型描述
	 *第二个字节					prefix compression count
	 *第3 ~ 11个字节				associated 64-bit value	(uint64_t encoding, max 9 bytes)
	 *第12 ~ 16个字节				数据长度(5字节)
	 */
	uint8_t __chunk[1 + 1 + WT_INTPACK64_MAXSIZE + WT_INTPACK32_MAXSIZE];
};

struct __wt_cell_unpack
{
	WT_CELL*		cell;		/*cell的磁盘位置信息*/
	uint64_t		v;			/*RLE count或者行号*/
	
	const void*		data;		/*数据*/
	uint32_t		size;		/*数据长度*/

	uint32_t		__len;		/*Cell + data length (usually)*/
	uint8_t			prefix;		/*Cell prefix length*/
	uint8_t			raw;		/*row cell type*/
	uint8_t			type;		/*Cell type*/
	uint8_t			ovfl;		/*cell是否溢出*/
};

/*遍历page上所有的cell,主要是在entries中的cell,在update和append上的记录不会被遍历*/
#define WT_CELL_FOREACH(btree, dsk, cell, unpack, i)		\
	for ((cell) = WT_PAGE_HEADER_BYTE(btree, dsk), (i) = (dsk)->u.entries;	\
	(i) > 0;						\
	(cell) = (WT_CELL *)((uint8_t *)(cell) + (unpack)->__len), --(i))

/*对cell address进行pack操作*/
static inline size_t __wt_cell_pack_addr(WT_CELL* cell, u_int cell_type, uint64_t recno, size_t size)
{
	uint8_t* p;
	p = cell->__chunk + 1;

	if(recno == 0)
		cell->__chunk[0] = cell_type;
	else{ 
		cell->__chunk[0] = cell_type | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, recno); /*将行号编入pack中*/
	}

	(void)__wt_vpack_uint(&p, 0, (uint64_t)size); /*打包size*/
	return (WT_PTRDIFF(p, cell));
}

/*设置CELL的内容时，需要改变cell->chunk中的类型值和长度*/
static inline size_t __wt_cell_pack_data(WT_CELL* cell, uint64_t rle, size_t size)
{
	uint8_t byte, *p;

	/*高6位表示长度（size < 64）,低2位表示数据类型*/
	if(rle < 2 && size <= WT_CELL_SHORT_MAX){
		byte = (uint8_t)size;
		cell->__chunk[0] = (byte << WT_CELL_SHORT_SHIFT) | WT_CELL_VALUE_SHORT;
		return 1;
	}

	p = cell->__chunk + 1;
	if(rle < 2){
		size -= WT_CELL_SIZE_ADJUST;
		cell->__chunk[0] = WT_CELL_VALUE;
	}
	else{
		cell->__chunk[0] = WT_CELL_VALUE | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle); /*编码rle*/
	}

	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);
	return (WT_PTRDIFF(p, cell));
}

/*假如page_cell和val_cell是相同的值，那么他们的值与val_data进行比较大小,如果相同，返回1，不相同返回0*/
static inline int __wt_cell_pack_data_match(WT_CELL *page_cell, WT_CELL *val_cell, const uint8_t *val_data, int *matchp)
{
	const uint8_t *a, *b;
	uint64_t av, bv;
	int rle;

	*matchp = 0;

	a = (uint8_t *)page_cell;
	b = (uint8_t *)val_cell;

	if (WT_CELL_SHORT_TYPE(a[0]) == WT_CELL_VALUE_SHORT) {
		av = a[0] >> WT_CELL_SHORT_SHIFT;
		++a;
	} 
	else if (WT_CELL_TYPE(a[0]) == WT_CELL_VALUE) {
		rle = a[0] & WT_CELL_64V ? 1 : 0;	/* Skip any RLE */
		++a;
		if (rle)
			WT_RET(__wt_vunpack_uint(&a, 0, &av));
		WT_RET(__wt_vunpack_uint(&a, 0, &av));	/* Length */
	} 
	else
		return 0;

	if (WT_CELL_SHORT_TYPE(b[0]) == WT_CELL_VALUE_SHORT) {
		bv = b[0] >> WT_CELL_SHORT_SHIFT;
		++b;
	} else if (WT_CELL_TYPE(b[0]) == WT_CELL_VALUE) {
		rle = b[0] & WT_CELL_64V ? 1 : 0;	/* Skip any RLE */
		++b;
		if (rle)
			WT_RET(__wt_vunpack_uint(&b, 0, &bv));
		WT_RET(__wt_vunpack_uint(&b, 0, &bv));	/* Length */
	} 
	else
		return 0;
	
	if (av == bv)
		*matchp = memcmp(a, val_data, av) == 0 ? 1 : 0;
	return 0;
}

/*pack一个copy data类型到cell中*/
static inline size_t __wt_cell_pack_copy(WT_CELL* cell, uint64_t rle, uint64_t v)
{
	uint8_t *p;

	p = cell->__chunk + 1;

	if(rle < 2)
		cell->__chunk[0] = WT_CELL_VALUE_COPY;
	else{
		cell->__chunk[0] = WT_CELL_VALUE_COPY | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);	
	}

	(void)__wt_vpack_uint(&p, 0, v);		/* Copy offset */
	return (WT_PTRDIFF(p, cell));
}

/*删除cell中的数据，标记删除*/
static inline size_t __wt_cell_pack_del(WT_CELL* cell, uint64_t rle)
{
	uint8_t* p;

	p = cell->__chunk + 1;
	if(rle < 2){
		cell->__chunk[0] = WT_CELL_DEL;
		return 1;
	}

	cell->__chunk[0] = WT_CELL_DEL | WT_CELL_64V;
	(void)__wt_vpack_uint(&p, 0, rle);
	return WT_PTRDIFF(p, cell);
}

/*设置一个行存储的key到cell中*/
static inline size_t __wt_cell_pack_int_key(WT_CELL* cell, size_t size)
{
	uint8_t byte, *p;

	if(size <= WT_CELL_SHORT_MAX){
		byte = (uint8_t)size;
		cell->__chunk[0] = (byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT;
		return 1;
	}

	cell->__chunk[0] = WT_CELL_KEY;
	p = cell->__chunk + 1;

	size -= WT_CELL_SIZE_ADJUST;
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);

	return WT_PTRDIFF(p, cell);
}

/*设置一个叶子节点KEY到CELL中*/
static inline size_t __wt_cell_pack_leaf_key(WT_CELL* cell, uint8_t prefix, size_t size)
{
	uint8_t byte, *p;

	if(size <= WT_CELL_SHORT_MAX){
		if(prefix == 0){
			byte = (uint8_t)size;
			cell->__chunk[0] = (byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT;
			return 1;
		}
		else{
			byte = (uint8_t)size;		/* Type + length */
			cell->__chunk[0] = (byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT_PFX;
			cell->__chunk[1] = prefix;	/* Prefix */
			return 2;
		}
	}

	if(prefix == 0){
		cell->__chunk[0] = WT_CELL_KEY;		/* Type */
		p = cell->__chunk + 1;
	}
	else{
		cell->__chunk[0] = WT_CELL_KEY_PFX;	/* Type */
		cell->__chunk[1] = prefix;			/* Prefix */
		p = cell->__chunk + 2;
	}

	size -= WT_CELL_SIZE_ADJUST;
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);

	return WT_PTRDIFF(p, cell);
}

/*pack一个overflow cell*/
static inline size_t __wt_cell_pack_ovfl(WT_CELL* cell, uint8_t type, uint64_t rle, size_t size)
{
	uint8_t* p;

	p = cell->__chunk + 1;
	if(rle < 2){
		cell->__chunk[0] = type;
	}
	else{
		cell->__chunk[0] = type | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);
	}

	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);
	return WT_PTRDIFF(p, cell);
}

/*获取cell的rle的值*/
static inline uint64_t __wt_cell_rle(WT_CELL_UNPACK* unpack)
{
	return (unpack->v < 2 ? 1 : unpack->v);
}

/*获得cell的长度*/
static inline size_t __wt_cell_total_len(WT_CELL_UNPACK* unpack)
{
	return unpack->__len;
}

/*获得cell的类型*/
static inline u_int __wt_cell_type(WT_CELL* cell)
{
	u_int type;

	switch (WT_CELL_SHORT_TYPE(cell->__chunk[0])) {
	case WT_CELL_KEY_SHORT:
	case WT_CELL_KEY_SHORT_PFX:
		return (WT_CELL_KEY);
	case WT_CELL_VALUE_SHORT:
		return (WT_CELL_VALUE);
	}

	switch (type = WT_CELL_TYPE(cell->__chunk[0])) {
	case WT_CELL_KEY_PFX:
		return (WT_CELL_KEY);
	case WT_CELL_KEY_OVFL_RM:
		return (WT_CELL_KEY_OVFL);
	case WT_CELL_VALUE_OVFL_RM:
		return (WT_CELL_VALUE_OVFL);
	}

	return type;
}

/*从cell中获得cell的type*/
static inline u_int __wt_cell_type_raw(WT_CELL* cell)
{
	return (WT_CELL_SHORT_TYPE(cell->__chunk[0]) == 0 ?
		WT_CELL_TYPE(cell->__chunk[0]) : WT_CELL_SHORT_TYPE(cell->__chunk[0]));
}

/*修改cell中的type*/
static inline void __wt_cell_type_reset(WT_SESSION_IMPL* session, WT_CELL* cell, u_int old_type, u_int new_type)
{
	WT_ASSERT(session, old_type == 0 || old_type == __wt_cell_type(cell));
	WT_UNUSED(old_type);

	cell->__chunk[0] = (cell->__chunk[0] & ~WT_CELL_TYPE_MASK) | WT_CELL_TYPE(new_type);
}

/*判断cell是否是行存储时page(在page的分配连续空间中)中的内容(value)，如果不是返回NULL*/
static inline WT_CELL* __wt_cell_leaf_value_parse(WT_PAGE* page, WT_CELL* cell)
{
	if (cell >= (WT_CELL *)((uint8_t *)page->dsk + page->dsk->mem_size))
		return (NULL);

	switch (__wt_cell_type_raw(cell)) {
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_OVFL_RM:
	case WT_CELL_KEY_PFX:
	case WT_CELL_KEY_SHORT:
	case WT_CELL_KEY_SHORT_PFX:
		return (NULL);
	default:
		return (cell);
	}
}

#define	WT_CELL_LEN_CHK(p, len) do {						\
	if (end != NULL && (((uint8_t *)p) + (len)) > end)		\
	return (WT_ERROR);										\
} while (0)

/*解码一个cell到WT_CELL_UNPACK中，并进行合法性判断*/
static inline int __wt_cell_unpack_safe(WT_CELL* cell, WT_CELL_UNPACK* unpack, uint8_t* end)
{
	struct {
		uint64_t len;
		uint32_t v;
	} copy;

	uint64_t v;
	const uint8_t *p;

	copy.len = 0;
	copy.v = 0;			/* -Werror=maybe-uninitialized */

restart:
	WT_CELL_LEN_CHK(cell, 0);
	unpack->cell = cell;
	unpack->v = 0;
	unpack->raw = __wt_cell_type_raw(cell);
	unpack->type = __wt_cell_type(cell);
	unpack->ovfl = 0;

	switch(unpack->raw){
	case WT_CELL_KEY_SHORT_PFX:
		WT_CELL_LEN_CHK(cell, 1);		/* skip prefix */
		unpack->prefix = cell->__chunk[1];
		unpack->data = cell->__chunk + 2;
		unpack->size = cell->__chunk[0] >> WT_CELL_SHORT_SHIFT;
		unpack->__len = 2 + unpack->size;
		goto done;

	case WT_CELL_KEY_SHORT:
	case WT_CELL_VALUE_SHORT:
		unpack->prefix = 0;
		unpack->data = cell->__chunk + 1;
		unpack->size = cell->__chunk[0] >> WT_CELL_SHORT_SHIFT;
		unpack->__len = 1 + unpack->size;
		goto done;
	}

	unpack->prefix = 0;
	unpack->data = NULL;
	unpack->size = 0;
	unpack->__len = 0;

	p = (uint8_t *)cell + 1;			/* skip cell */

	if(unpack->raw == WT_CELL_KEY_PFX){
		++ p;
		WT_CELL_LEN_CHK(p, 0);
		unpack->prefix = cell->__chunk[1];
	}

	if(cell->__chunk[0] & WT_CELL_64V)
		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 : (size_t)(end - p), &unpack->v));

	switch(unpack->raw){
	case WT_CELL_VALUE_COPY:
		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 : (size_t)(end - p), &v));
		copy.len = WT_PTRDIFF32(p, cell);
		copy.v = unpack->v;
		cell = (WT_CELL *)((uint8_t *)cell - v);
		goto restart;

	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_OVFL_RM:
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_OVFL_RM:
		unpack->ovfl = 1;

	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_INT:
	case WT_CELL_ADDR_LEAF:
	case WT_CELL_ADDR_LEAF_NO:
	case WT_CELL_KEY:
	case WT_CELL_KEY_PFX:
	case WT_CELL_VALUE:
		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 : (size_t)(end - p), &v));

		if (unpack->raw == WT_CELL_KEY || unpack->raw == WT_CELL_KEY_PFX || (unpack->raw == WT_CELL_VALUE && unpack->v == 0))
			v += WT_CELL_SIZE_ADJUST;

		unpack->data = p;
		unpack->size = (uint32_t)v;
		unpack->__len = WT_PTRDIFF32(p + unpack->size, cell);
		break;

	case WT_CELL_DEL:
		unpack->__len = WT_PTRDIFF32(p, cell);
		break;

	default:
		return WT_ERROR;
	}

done:	
	WT_CELL_LEN_CHK(cell, unpack->__len);
	if (copy.len != 0) {
		unpack->raw = WT_CELL_VALUE_COPY;
		unpack->__len = copy.len;
		unpack->v = copy.v;
	}

	return 0;
}

/*解析cell到unpack结构中*/
static inline void __wt_cell_unpack(WT_CELL* cell, WT_CELL_UNPACK* unpack)
{
	(void)__wt_cell_unpack_safe(cell, unpack, NULL);
}

/*将unpack中的data设置到一个缓冲区中*/
static inline int __cell_data_ref(WT_SESSION_IMPL* session, WT_PAGE* page, int page_type, WT_CELL_UNPACK* unpack, WT_ITEM* store)
{
	WT_BTREE *btree;
	void *huffman;

	btree = S2BT(session);

	switch(unpack->type){
	case WT_CELL_KEY:
		store->data = unpack->data;
		store->size = unpack->size;
		if (page_type == WT_PAGE_ROW_INT)
			return (0);

		huffman = btree->huffman_key;
		break;

	case WT_CELL_VALUE:
		store->data = unpack->data;
		store->size = unpack->size;
		huffman = btree->huffman_value;
		break;

	case WT_CELL_KEY_OVFL:
		WT_RET(__wt_ovfl_read(session, page, unpack, store));
		if (page_type == WT_PAGE_ROW_INT)
			return (0);

		huffman = btree->huffman_key;
		break;

	case WT_CELL_VALUE_OVFL:
		WT_RET(__wt_ovfl_read(session, page, unpack, store));
		huffman = btree->huffman_value;
		break;

	WT_ILLEGAL_VALUE(session);
	}
	/*霍夫曼解码*/
	return (huffman == NULL ? 0 : __wt_huffman_decode(session, huffman, store->data, store->size, store));
}

static inline int __wt_dsk_cell_data_ref(WT_SESSION_IMPL* session, int page_type, WT_CELL_UNPACK* unpack, WT_ITEM* store)
{
	WT_ASSERT(session, __wt_cell_type_raw(unpack->cell) != WT_CELL_VALUE_OVFL_RM);
	return (__cell_data_ref(session, NULL, page_type, unpack, store));
}

static inline int __wt_page_cell_data_ref(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL_UNPACK *unpack, WT_ITEM *store)
{
	return (__cell_data_ref(session, page, page->type, unpack, store));
}

/*从unpack中将data拷贝到store缓冲区中*/
static inline int __wt_cell_data_copy(WT_SESSION_IMPL* session, int page_type, WT_CELL_UNPACK* unpack, WT_ITEM* store)
{
	WT_RET(__wt_dsk_cell_data_ref(session, page_type, unpack, store));
	if (!WT_DATA_IN_ITEM(store))
		WT_RET(__wt_buf_set(session, store, store->data, store->size));
	return (0);
}




