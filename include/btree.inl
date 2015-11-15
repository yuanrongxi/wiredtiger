/************************************************************************
*btree的一些基本函数，主要包括：cache信息状态改变，脏页状态，BTREE中的KEY
*VALUE格式构建等函数。
************************************************************************/

/*判断ref对应的page是否是root page*/
static inline int __wt_ref_is_root(WT_REF* ref)
{
	return (ref->home == NULL ? 1 : 0);
}

/*判断page是否一个空页*/
static inline int __wt_page_is_empty(WT_PAGE* page)
{
	return (page->modify != NULL && F_ISSET(page->modify, WT_PM_REC_MASK) == WT_PM_REC_EMPTY : 1 : 0);
}

/*判断page是否存在内存中的修改，也就是脏页判断*/
static inline int __wt_page_is_modified(WT_PAGE* page)
{
	return (page->modify != NULL && page->modify->write_gen != 0 ? 1 : 0);
}

/*在cache增加对page的内存信息统计*/
static inline void __wt_cache_page_inmem_incr(WT_SESSION_IMPL *session, WT_PAGE *page, size_t size)
{
	WT_CACHE* cache;

	WT_ASSERT(session, size < WT_EXABYTE);

	cache = S2C(session)->cache;

	/*对cache中个项数据的增加*/
	(void)WT_ATOMIC_ADD8(cache->bytes_inmem, size);
	(void)WT_ATOMIC_ADD8(page->memory_footprint, size);
	if (__wt_page_is_modified(page)) {
		(void)WT_ATOMIC_ADD8(cache->bytes_dirty, size);
		(void)WT_ATOMIC_ADD8(page->modify->bytes_dirty, size);
	}
	/*对溢出page的统计信息更改*/
	if (WT_PAGE_IS_INTERNAL(page))
		(void)WT_ATOMIC_ADD8(cache->bytes_internal, size);
	else if (page->type == WT_PAGE_OVFL)
		(void)WT_ATOMIC_ADD8(cache->bytes_overflow, size);
}

/*这里的if判断是为了防止负值溢出*/
#define WT_CACHE_DECR(s, f, sz) do{					\
	if(WT_ATOMIC_SUB8(f, sz) > WT_EXABYTE)			\
		(void)WT_ATOMIC_ADD8(f, sz);				\
}while(0)

/*脏页数据递减，一般在脏页刷盘后从内存中淘汰时会调用*/
static inline void __wt_cache_page_byte_dirty_decr(WT_SESSION_IMPL* session, WT_PAGE* page, size_t size)
{
	WT_CACHE* cache;
	size_t decr, orig;
	int i;

	cache = S2C(session)->cache;
	for(i = 0; i < 5; i++){
		orig = page->modify->bytes_dirty;
		decr = WT_MIN(size, orig);
		/*首先修改page的modify，如果成功了，才进行cache的修改，这是个并发先后的问题*/
		if (WT_ATOMIC_CAS8(page->modify->bytes_dirty, orig, orig - decr)) {
			WT_CACHE_DECR(session, cache->bytes_dirty, decr);
			break;
		}
	}
}

/*page从内存中淘汰，cache需要修改对应的内存统计信息*/
static inline void __wt_cache_page_inmem_decr(WT_SESSION_IMPL* session, WT_PAGE* page, size_t size)
{
	WT_CACHE *cache;

	cache = S2C(session)->cache;

	WT_ASSERT(session, size < WT_EXABYTE);

	WT_CACHE_DECR(session, cache->bytes_inmem, size);
	WT_CACHE_DECR(session, page->memory_footprint, size);
	/*page是个脏页，需要进行脏页数据统计信息更改*/
	if (__wt_page_is_modified(page))
		__wt_cache_page_byte_dirty_decr(session, page, size);

	if (WT_PAGE_IS_INTERNAL(page))
		WT_CACHE_DECR(session, cache->bytes_internal, size);
	else if (page->type == WT_PAGE_OVFL)
		WT_CACHE_DECR(session, cache->bytes_overflow, size);
}

/*page switch动作，造成cache dirty page的字节统计增加*/
static inline void __wt_cache_dirty_incr(WT_SESSION_IMPL* session, WT_PAGE* page)
{
WT_CACHE *cache;
	size_t size;

	cache = S2C(session)->cache;
	/*增加脏页数量*/
	(void)WT_ATOMIC_ADD8(cache->pages_dirty, 1);

	/*增加脏数据总量*/
	size = page->memory_footprint;
	(void)WT_ATOMIC_ADD8(cache->bytes_dirty, size);
	(void)WT_ATOMIC_ADD8(page->modify->bytes_dirty, size);
}

/*page switch动作，造成cache dirty page的字节统计递减*/
static inline void __wt_cache_dirty_decr(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_CACHE *cache;
	WT_PAGE_MODIFY *modify;

	cache = S2C(session)->cache;

	if (cache->pages_dirty < 1) {
		__wt_errx(session, "cache eviction dirty-page decrement failed: dirty page count went negative");
		cache->pages_dirty = 0;
	} 
	else
		(void)WT_ATOMIC_SUB8(cache->pages_dirty, 1);

	modify = page->modify;
	if (modify != NULL && modify->bytes_dirty != 0)
		__wt_cache_page_byte_dirty_decr(session, page, modify->bytes_dirty);
}

/*page从cache中淘汰，需要对 cache的状态统计进行修改*/
static inline void __wt_cache_page_evict(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_CACHE *cache;
	WT_PAGE_MODIFY *modify;

	cache = S2C(session)->cache;
	modify = page->modify;

	/*首先修改cache的内存数据总数*/
	WT_CACHE_DECR(session, cache->bytes_inmem, page->memory_footprint);

	/*修改脏数据统计*/
	if (modify != NULL && modify->bytes_dirty != 0) {
		if (cache->bytes_dirty < modify->bytes_dirty) {
			__wt_errx(session, "cache eviction dirty-bytes decrement failed: dirty byte count went negative");
			cache->bytes_dirty = 0;
		} 
		else
			WT_CACHE_DECR(session, cache->bytes_dirty, modify->bytes_dirty);
	}

	/* Update pages and bytes evicted. */
	(void)WT_ATOMIC_ADD8(cache->bytes_evict, page->memory_footprint);
	(void)WT_ATOMIC_ADD8(cache->pages_evict, 1);
}

/*计算upd在内存中修改的数据总数*/
static inline size_t __wt_update_list_memsize(WT_UPDATE* upd)
{
	size_t upd_size;
	for (upd_size = 0; upd != NULL; upd = upd->next)
		upd_size += WT_UPDATE_MEMSIZE(upd);

	return upd_size;
}

/*将page置为淘汰状态*/
static inline void __wt_page_evict_soon(WT_PAGE* page)
{
	page->read_gen = WT_READGEN_OLDEST;
}

/*查找ref对应page在pindex中对应的槽位ID*/
static inline void __wt_page_refp(WT_SESSION_IMPL* session, WT_REF* ref, WT_PAGE_INDEX** pindexp, uint32_t* slotp)
{
	WT_PAGE_INDEX *pindex;
	uint32_t i;

retry:
	WT_INTL_INDEX_GET(session, ref->home, pindex);

	/*先在指定的起始位置进行槽位查询定位*/
	for(i = ref->ref_hint; i < pindex->entries; ++i){
		if (pindex->index[i]->page == ref->page) {
			*pindexp = pindex;
			*slotp = ref->ref_hint = i;
			return;
		}
	}

	/*在指定槽位起始位置没找到，从0位置开始全槽位扫描定位*/
	for (i = 0; i < pindex->entries; ++i){
		if (pindex->index[i]->page == ref->page) {
			*pindexp = pindex;
			*slotp = ref->ref_hint = i;
			return;
		}
	}

	/*没有找到，将线程执行权重新归还给操作系统进行调度*/
	__wt_yield();
	goto retry;
}

/*为page分配一个modify结构，并初始化它*/
static inline int __wt_page_modify_init(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	return (page->modify == NULL ? __wt_page_modify_alloc(session, page) : 0);
}

/*在page由未修改状态变为修改状态，标记此page为脏页*/
static inline void __wt_page_only_modify_set(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	uint64_t last_running = 0;

	if (page->modify->write_gen == 0)
		last_running = S2C(session)->txn_global.last_running;

	/*设置write gen计数器，如果是从0变为1，表示是由未修改状态转变为修改状态，设置脏页状态*/
	if(WT_ATOMIC_ADD4(page->modify->write_gen, 1) == 1){
		__wt_cache_dirty_incr(session, page);

		/*设置snapshot的落盘位置*/
		if (F_ISSET(&session->txn, TXN_HAS_SNAPSHOT))
			page->modify->disk_snap_min = session->txn.snap_min;

		if (last_running != 0)
			page->modify->first_dirty_txn = last_running;
	}

	/*更新最新的txn id*/
	if(TXNID_LT(page->modify->update_txn, session->txn.id))
		page->modify->update_txn = session->txn.id;
}

/*标记page为btree上的脏页,并标记树上有脏页*/
static inline void __wt_page_modify_set(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	if (S2BT(session)->modified == 0) {
		S2BT(session)->modified = 1;
		WT_FULL_BARRIER();
	}

	__wt_page_only_modify_set(session, page);
}

/*标记ref对应的父节点page为脏页*/
static inline int __wt_page_parent_modify_set(WT_SESSION_IMPL* session, WT_REF* ref, int page_only)
{
	WT_PAGE *parent;

	parent = ref->home;
	WT_RET(__wt_page_modify_init(session, parent));
	if (page_only)
		__wt_page_only_modify_set(session, parent);
	else
		__wt_page_modify_set(session, parent);

	return 0;
}

/*判断p的位置是否在page的磁盘部分数据上*/
static inline int __wt_off_page(WT_PAGE* page, const void* p)
{
	return (page->dsk == NULL || p < (void *)page->dsk ||p >= (void *)((uint8_t *)page->dsk + page->dsk->mem_size));
}

/*对btree上key/value值计算*/
#define	WT_IK_FLAG					0x01
#define	WT_IK_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 32)
#define	WT_IK_DECODE_KEY_LEN(v)		((v) >> 32)
#define	WT_IK_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 1)
#define	WT_IK_DECODE_KEY_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 1)

/*获取行存储时ref对应page中的内部key值*/
static inline void __wt_ref_key(WT_PAGE *page, WT_REF *ref, void *keyp, size_t *sizep)
{
	v = (uintptr_t)ref->key.ikey;
	/*key的值与key.ikey内存关系是不连续的，keyp指向key值开始位置,通过v的值计算偏移*/
	if (v & WT_IK_FLAG){
		*(void **)keyp = WT_PAGE_REF_OFFSET(page, WT_IK_DECODE_KEY_OFFSET(v));
		*sizep = WT_IK_DECODE_KEY_LEN(v);
	} 
	else {
		*(void **)keyp = WT_IKEY_DATA(ref->key.ikey);
		*sizep = ((WT_IKEY *)ref->key.ikey)->size;
	}
}

/*将unpack数据作为key内容设置WT_REF的KEY,不连续内存设置*/
static inline void __wt_ref_key_onpage_set(WT_PAGE* page, WT_REF* ref, WT_CELL_UNPACK* unpack)
{
	uintptr_t v;
	v = WT_IK_ENCODE_KEY_LEN(unpack->size) | WT_IK_ENCODE_KEY_OFFSET(WT_PAGE_DISK_OFFSET(page, unpack->data)) | WT_IK_FLAG;
	ref->key.ikey = (void *)v;
}

/*判断ref对应的key的值空间是否是连续的*/
static inline WT_IKEY* __wt_ref_key_instantiated(WT_REF* ref)
{
	uintptr_t v;
	v = (uintptr_t)ref->key.ikey;
	return (v & WT_IK_FLAG ? NULL : ref->key.ikey);
}

/*清除WT_REF的key*/
static inline void __wt_ref_key_clear(WT_REF* ref)
{
	ref->key.recno = 0;
}

/*cell 的标示和cell的偏移编码*/
#define	WT_CELL_FLAG				0x01
#define	WT_CELL_ENCODE_OFFSET(v)	((uintptr_t)(v) << 2)
#define	WT_CELL_DECODE_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 2)

/*KEY的标示和编码，高32位是KEY的长度，低2为是标示值，2 ~ 31位为偏移值*/
#define	WT_K_FLAG					0x02
#define	WT_K_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 32)
#define	WT_K_DECODE_KEY_LEN(v)		((v) >> 32)
#define	WT_K_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 2)
#define	WT_K_DECODE_KEY_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 2)

/*KV的标示和编码
 *0 ~ 1			标示位
 *2 ~ 21		value的偏移
 *22 ~ 41		key的偏移
 *42 ~ 54		value的长度
 *55 ~ 63		key的长度
 */
#define	WT_KV_FLAG					0x03
#define	WT_KV_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 55)
#define	WT_KV_DECODE_KEY_LEN(v)		((v) >> 55)
#define	WT_KV_MAX_KEY_LEN			(0x200 - 1)
#define	WT_KV_ENCODE_VALUE_LEN(v)	((uintptr_t)(v) << 42)
#define	WT_KV_DECODE_VALUE_LEN(v)	(((v) & 0x007FFC0000000000) >> 42)
#define	WT_KV_MAX_VALUE_LEN			(0x2000 - 1)
#define	WT_KV_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 22)
#define	WT_KV_DECODE_KEY_OFFSET(v)	(((v) & 0x000003FFFFC00000) >> 22)
#define	WT_KV_MAX_KEY_OFFSET		(0x100000 - 1)
#define	WT_KV_ENCODE_VALUE_OFFSET(v)	((uintptr_t)(v) << 2)
#define	WT_KV_DECODE_VALUE_OFFSET(v)	(((v) & 0x00000000003FFFFC) >> 2)
#define	WT_KV_MAX_VALUE_OFFSET		(0x100000 - 1)

/*解析copy的值，通过解析得到page的key的指针偏移位置和对应数据长度*/
static inline int __wt_row_leaf_key_info(WT_PAGE* page, void* copy, WT_IKEY** ikeyp, WT_CELL** cellp, void* datap, size_t *sizep)
{
	WT_IKEY* ikey;
	uintptr_t v;

	v = (uintptr_t)copy;

	switch(v & 0x03){
	case WT_CELL_FLAG: /*cell方式解析*/
		if (ikeyp != NULL)
			*ikeyp = NULL;
		if (cellp != NULL)
			*cellp = WT_PAGE_REF_OFFSET(page, WT_CELL_DECODE_OFFSET(v));
		return 0;

	case WT_K_FLAG: /*key方式解析*/
		if(cellp != NULL)
			*cellp = NULL;
		if(ikeyp != NULL)
			*ikeyp = NULL;
		if(datap != NULL){
			*(void **)datap = WT_PAGE_REF_OFFSET(page, WT_K_DECODE_KEY_OFFSET(v));
			*sizep = WT_K_DECODE_KEY_LEN(v);
			return 1;
		}
		return 0;

	case WT_KV_FLAG: /*key/value方式解析*/
		if (cellp != NULL)
			*cellp = NULL;
		if (ikeyp != NULL)
			*ikeyp = NULL;
		if (datap != NULL) {
			*(void **)datap = WT_PAGE_REF_OFFSET(page, WT_KV_DECODE_KEY_OFFSET(v));
			*sizep = WT_KV_DECODE_KEY_LEN(v);
			return 1;
		}
		return 0;
	}

	/*默认连续的key值存储方式*/
	ikey = copy;
	if (ikeyp != NULL)
		*ikeyp = copy;
	if (cellp != NULL)
		*cellp = WT_PAGE_REF_OFFSET(page, ikey->cell_offset);
	if (datap != NULL) {
		*(void **)datap = WT_IKEY_DATA(ikey);
		*sizep = ikey->size;
		return 1;
	}
	return 0;
}

/*设置行存储时叶子节点的reference key的值*/
static inline void __wt_row_leaf_key_set(WT_PAGE* page, WT_ROW* rip, WT_CELL_UNPACK* unpack)
{
	uintptr_t v;

	/*编码存储的值*/
	v = WT_K_ENCODE_KEY_LEN(unpack->size) |
		WT_K_ENCODE_KEY_OFFSET(WT_PAGE_DISK_OFFSET(page, unpack->data)) | WT_K_FLAG;

	WT_ROW_KEY_SET(rip, v);
}

/*设置行存储时叶子节点的reference cell的值*/
static inline void __wt_row_leaf_key_set_cell(WT_PAGE* page, WT_ROW* rip, WT_CELL* cell)
{
	uintptr_t v;
	v = WT_CELL_ENCODE_OFFSET(WT_PAGE_DISK_OFFSET(page, cell)) | WT_CELL_FLAG;
	WT_ROW_KEY_SET(rip, v);
}

/*设置行存储是也只几点的reference k/v的值*/
static inline void __wt_row_leaf_value_set(WT_PAGE* page, WT_ROW* rip, WT_CELL_UNPACK* unpack)
{
	uintptr_t key_len, key_offset, value_offset, v;

	v = (uintptr_t)WT_ROW_KEY_COPY(rip);
	if(!(v & WT_K_FLAG)) /*没有先设置KEY的值，不能设置VALUE的值*/
		return ;

	key_len = WT_K_DECODE_KEY_LEN(v);	/* Key length */
	if (key_len > WT_KV_MAX_KEY_LEN)
		return;

	if (unpack->size > WT_KV_MAX_VALUE_LEN)	/* Value length */
		return;

	key_offset = WT_K_DECODE_KEY_OFFSET(v);	/* Page offsets */
	if (key_offset > WT_KV_MAX_KEY_OFFSET)
		return;

	value_offset = WT_PAGE_DISK_OFFSET(page, unpack->data);
	if (value_offset > WT_KV_MAX_VALUE_OFFSET)
		return;

	/*对k/v方式进行魔法字值的更新*/
	v = WT_KV_ENCODE_KEY_LEN(key_len) |
		WT_KV_ENCODE_VALUE_LEN(unpack->size) |
		WT_KV_ENCODE_KEY_OFFSET(key_offset) |
		WT_KV_ENCODE_VALUE_OFFSET(value_offset) | WT_KV_FLAG;

	WT_ROW_KEY_SET(rip, v);
}

/*获取row store的叶子节点参考KEY值*/
static inline int __wt_row_leaf_key(WT_SESSION_IMPL* session, WT_PAGE* page, WT_ROW* rip, WT_ITEM* key, int instantiate)
{
	void* copy;

	copy = WT_ROW_KEY_COPY(rip);
	/*设置key的魔法字*/
	if(__wt_row_leaf_key_info(page, copy, NULL, NULL, &key->data, &key->size))
		return 0;

	return __wt_row_leaf_key_work(session, page, rip, key, instantiate);
}

/*获取btree cursor设置叶子节点的key值*/
static inline int __wt_cursor_row_leaf_key(WT_CURSOR_BTREE* cbt, WT_ITEM* key)
{
	WT_PAGE *page;
	WT_ROW *rip;
	WT_SESSION_IMPL *session;

	if (cbt->ins == NULL) {
		session = (WT_SESSION_IMPL *)cbt->iface.session;
		page = cbt->ref->page;
		rip = &page->u.row.d[cbt->slot];
		WT_RET(__wt_row_leaf_key(session, page, rip, key, 0));
	} 
	else { /*直接获得最后一个insert操作的key即可*/
		key->data = WT_INSERT_KEY(cbt->ins);
		key->size = WT_INSERT_KEY_SIZE(cbt->ins);
	}

	return 0;
}

/*获得行存储时的叶子节点的cell值*/
static inline WT_CELL* __wt_row_leaf_value_cell(WT_PAGE* page, WT_ROW* rip, WT_CELL_UNPACK* kpack)
{
	WT_CELL *kcell, *vcell;
	WT_CELL_UNPACK unpack;
	void *copy, *key;
	size_t size;

	/*从kpack的中直接获的cell数据指针*/
	if (kpack != NULL)
		vcell = (WT_CELL *)((uint8_t *)kpack->cell + __wt_cell_total_len(kpack));
	else{
		copy = WT_ROW_KEY_COPY(rip);

		/*获得cell的存储信息，主要是偏移量和长度,并通过key和size得到cell指针*/
		if (__wt_row_leaf_key_info(page, copy, NULL, &kcell, &key, &size) && kcell == NULL)
			vcell = (WT_CELL *)((uint8_t *)key + size);
		else { /*通过unpack直接解析获得cell数据指针*/
			__wt_cell_unpack(kcell, &unpack);
			vcell = (WT_CELL *)((uint8_t *)unpack.cell + __wt_cell_total_len(&unpack));
		}
	}

	/*对cell的解析，并返回对象指针*/
	return __wt_cell_leaf_value_parse(page, vcell);
}

/*获得行存储时k/v的魔法字中存储的信息（偏移量 + 长度）*/
static inline int __wt_row_leaf_value(WT_PAGE* page, WT_ROW* rip, WT_ITEM* value)
{
	uintptr_t v;

	v = (uintptr_t)WT_ROW_KEY_COPY(rip);

	/*如果不是WT_KV_FLAG模式，无法获得K/V的信息*/
	if((v & 0x03) == WT_KV_FLAG){
		value->data = WT_PAGE_REF_OFFSET(page, WT_KV_DECODE_VALUE_OFFSET(v));
		value->size = WT_KV_DECODE_VALUE_LEN(v);

		return 1;
	}

	return 0;
}

/*通过ref信息，返回一个对应的的block的(addr/size/type)对，这个block地址对可以从磁盘上读取对应的信息*/
static inline int __wt_ref_info(WT_SESSION_IMPL* session, WT_REF* ref, const uint8_t** addrp, size_t* sizep, u_int* typep)
{
	WT_ADDR *addr;
	WT_CELL_UNPACK *unpack, _unpack;

	addr = ref->addr;
	unpack = &_unpack;

	if(addr == NULL){
		*addrp = NULL;
		*sizep = NULL;
		if(typep != NULL)
			*typep = 0;
	}
	else if (__wt_off_page(ref->home, addr)){ /*addr存储部分在磁盘上，需要进行磁盘读取*/
		*addrp = addr->addr;
		*sizep = addr->size;
		if (typep != NULL)
			switch (addr->type) { /*确定type值*/
			case WT_ADDR_INT:
				*typep = WT_CELL_ADDR_INT;
				break;
			case WT_ADDR_LEAF:
				*typep = WT_CELL_ADDR_LEAF;
				break;
			case WT_ADDR_LEAF_NO:
				*typep = WT_CELL_ADDR_LEAF_NO;
				break;

			WT_ILLEGAL_VALUE(session);
		}
	}
	else{ /*从unpack中解析*/
		__wt_cell_unpack((WT_CELL *)addr, unpack);
		*addrp = unpack->data;
		*sizep = unpack->size;
		if (typep != NULL)
			*typep = unpack->type;
	}

	return 0;
}

/*判断page是否可以进行LRU淘汰*/
static inline int __wt_page_can_evict(WT_SESSION_IMPL* session, WT_PAGE* page, int check_splits)
{
	WT_BTREE *btree;
	WT_PAGE_MODIFY *mod;

	btree = S2BT(session);
	mod = page->modify;

	/*page没有进行过修改，可以直接进行淘汰，不需要数据落盘操作*/
	if(mod == NULL)
		return 1;

	/*page已经正在进行淘汰,不需要重复*/
	if(F_ISSET_ATOMIC(page, WT_PAGE_EVICT_LRU))
		return 0;

	/*page正在分裂,不能进行淘汰*/
	if(check_splits && WT_PAGE_IS_INTERNAL(page) && !__wt_txn_visible_all(session, mod->mod_split_txn))
		return 0;

	/*btree正在进行checkpoint，而且这个page是被修改过的，不能进行淘汰*/
	if (btree->checkpointing && (__wt_page_is_modified(page) || F_ISSET(mod, WT_PM_REC_MULTIBLOCK))) {
		WT_STAT_FAST_CONN_INCR(session, cache_eviction_checkpoint);
		WT_STAT_FAST_DATA_INCR(session, cache_eviction_checkpoint);
		return 0;
	}

	/*page不是cache中最早有读操作的页，而且还有正在执行的事务依赖于这个page,不能进行淘汰*/
	if (page->read_gen != WT_READGEN_OLDEST && !__wt_txn_visible_all(session, __wt_page_is_modified(page) ? mod->update_txn : mod->rec_max_txn))
		return 0;

	/*这个page刚才发生了split操作，不能立即进行淘汰，而是通过viction thread进行淘汰slipt*/
	if (check_splits && !__wt_txn_visible_all(session, mod->inmem_split_txn))
		return 0;

	return 1;
}

/*尝试淘汰并释放一个内存中的page*/
static inline int  __wt_page_release_evict(WT_SESSION_IMPL* session, WT_REF* ref)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	int locked, too_big;

	btree = S2BT(session);
	page = ref->page;
	too_big = (page->memory_footprint > btree->maxmempage) ? 1 : 0;

	/*先将state从ref mem设置为LOCKED,防止其他事务操作这个页*/
	locked = WT_ATOMIC_CAS4(ref->state, WT_REF_MEM, WT_REF_LOCKED);
	WT_TRET(__wt_hazard_clear(session, page));
	if(!locked){
		WT_TRET(EBUSY);
		return ret;
	}

	/*将btree社会为正在淘汰page状态并增加淘汰计数器,并进行页淘汰*/
	(void)WT_ATOMIC_CAS4(btree->evict_busy, 1);
	if((ret = __wt_evict_page(session, ref)) == 0){
		if (too_big)
			WT_STAT_FAST_CONN_INCR(session, cache_eviction_force);
		else
			WT_STAT_FAST_CONN_INCR(session, cache_eviction_force_delete);
	}
	else
		WT_STAT_FAST_CONN_INCR(session, cache_eviction_force_fail);

	/*淘汰完毕，btree淘汰计数器递减*/
	(void)WT_ATOMIC_SUB4(btree_evict_busy, 1);

	return ret;
}

/*释放一个ref对应的page*/
static inline int __wt_page_release(WT_SESSION_IMPL* session, WT_REF* ref, uint32_t flags)
{
	WT_BTREE *btree;
	WT_PAGE *page;

	btree = S2BT(session);

	/*ref为NULL或者REF对应的page是root page,不能进行释放*/
	if(ref == NULL || __wt_ref_is_root(ref)){
		return 0;
	}

	page = ref->page;

	/*判断是否能淘汰page, 释放的一定是最早发生读操作的page*/
	if(page->read_gen != WT_READGEN_OLDEST || LF_ISSET(WT_READ_NO_EVICT) || F_ISSET(btree, WT_BTREE_NO_EVICTION)
		!__wt_page_can_evict(session, page, 1)){
			return __wt_hazard_clear(session, page);
	}

	WT_RET_BUSY_OK(__wt_page_release_evict(session, ref));

	return 0;
}

/*交换held和want的page指针，相当于淘汰held,从磁盘中读取want来顶替held的位置*/
static inline int __wt_page_swap_func(WT_SESSION_IMPL* session, WT_REF* held, WT_REF* want, uint32_t flags)
{
	WT_DECL_RET;
	int acquired;

	if(held == want)
		return 0;
	/*获取ref对应page的指针，如果他不在内存中，会从磁盘中读取导入到内存中*/
	ret = __wt_page_in_func(session, want, flags);
	/*want读取失败或者操作失败*/
	if (LF_ISSET(WT_READ_CACHE) && ret == WT_NOTFOUND)
		return (WT_NOTFOUND);

	if (ret == WT_RESTART)
		return (WT_RESTART);

	acquired = ret == 0;
	/*释放掉held对应的page*/
	WT_TRET(__wt_page_release(session, held, flags));
	
	/*加入释放held失败，无法交换指针，应该将want从内从中释放*/
	if (acquired && ret != 0)
		WT_TRET(__wt_page_release(session, want, flags));

	return ret;
}

/*检查page是否是一个hazard pointer,如果是返回其对应hazard对象,这里涉及到了无锁操作，用了内存屏障来保证并发*/
static inline WT_HAZARD* __wt_page_hazard_check(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CONNECTION_IMPL *conn;
	WT_HAZARD *hp;
	WT_SESSION_IMPL *s;
	uint32_t i, hazard_size, session_cnt;

	conn = S2C(session);

	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for(s == conn->sessions, i = 0; i < session_cnt; ++s, ++i){
		if(!s->active)
			continue;

		WT_ORDERED_READ(hazard_size, s->hazard_size);

		for (hp = s->hazard; hp < s->hazard + hazard_size; ++hp){
			if (hp->page == page)
				return hp;
		}
	}

	return NULL;
}

/*随机一个层来做skiplist的insert操作*/
static inline u_int __wt_skip_choose_depth(WT_SESSION_IMPL* session)
{
	u_int d;
	for(d = 1; d < WT_SKIP_MAXDEPTH && __wt_random(session->rnd) < WT_SKIP_PROBABILITY; d++)
		;

	return d;
}

/*
 * __wt_btree_lsm_size --
 *	Return if the size of an in-memory tree with a single leaf page is over
 * a specified maximum.  If called on anything other than a simple tree with a
 * single leaf page, returns true so our LSM caller will switch to a new tree.
 */
static inline __wt_btree_lsm_size(WT_SESSION_IMPL* session, uint64_t maxsize)
{
	WT_BTREE *btree;
	WT_PAGE *child, *root;
	WT_PAGE_INDEX *pindex;
	WT_REF *first;

	btree = S2BT(session);
	root = btree->root.page;

	if(root == NULL)
		return 0;

	if(!F_ISSET(btree, WT_BTREE_NO_EVICTION))
		return 1;

	/* 检查btree是否只有一个叶子节点 */
	WT_INTL_INDEX_GET(session, root, pindex);
	if (pindex->entries != 1)		
		return 1;

	first = pindex->index[0];
	if (first->state != WT_REF_MEM)		
		return (0);

	child = first->page;
	if(child->type != WT_PAGE_ROW_LEAF) /*第一个孩子也不是单个叶子节点*/
		return 1;

	return child->memory_footprint > maxsize;
}




