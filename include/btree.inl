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
static inline void __wt_cache_inmem_incr(WT_SESSION_IMPL *session, WT_PAGE *page, size_t size)
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



