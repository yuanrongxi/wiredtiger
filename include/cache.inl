
/*对session的cache read generation做自加*/
static inline void __wt_cache_read_gen_incr(WT_SESSION_IMPL* session)
{
	++S2C(session)->cache->read_gen;
}

static inline uint64_t __wt_cache_read_gen(WT_SESSION_IMPL* session)
{
	return (S2C(session)->cache->read_gen);
}

/*为page获得一个read gen,这个gen + WT_READGEN_STEP是防止区间冲突*/
static inline uint64_t __wt_cache_read_gen_set(WT_SESSION_IMPL* session)
{
	/*
	 * We return read-generations from the future (where "the future" is
	 * measured by increments of the global read generation).  The reason
	 * is because when acquiring a new hazard pointer for a page, we can
	 * check its read generation, and if the read generation isn't less
	 * than the current global generation, we don't bother updating the
	 * page.  In other words, the goal is to avoid some number of updates
	 * immediately after each update we have to make.
	 */
	return (__wt_cache_read_gen(session) + WT_READGEN_STEP);
}

/*返回正在使用的page数*/
static inline uint64_t __wt_cache_pages_inuse(WT_CACHE* cache)
{
	return cache->pages_inmem - cache->pages_evict; 
}

/*返回cache中占用的字节数*/
static inline uint64_t __wt_cache_bytes_inuse(WT_CACHE* cache)
{
	uint64_t bytes_inuse;

	/* Adjust the cache size to take allocation overhead into account. */
	bytes_inuse = cache->bytes_inmem;
	if (cache->overhead_pct != 0) /*如果需要计算百分比空间占用，那么需要通过overhead_pct来得到*/
		bytes_inuse += (bytes_inuse * (uint64_t)cache->overhead_pct) / 100;

	return (bytes_inuse);
}

/*计算脏数据字节数*/
static inline uint64_t __wt_cache_dirty_inuse(WT_CACHE* cache)
{
	uint64_t dirty_inuse;

	dirty_inuse = cache->bytes_dirty;
	if (cache->overhead_pct != 0)
		dirty_inuse += (dirty_inuse * (uint64_t)cache->overhead_pct) / 100;

	return dirty_inuse;
}

/*判断空间的占用率，如果超过设定的阈值，唤醒evict thread进行evict page操作*/
static inline int __wt_eviction_check(WT_SESSION_IMPL* session, int* fullp, int wake)
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	uint64_t bytes_inuse, bytes_max, dirty_inuse;

	conn = S2C(session);
	cache = conn->cache;

	bytes_inuse = __wt_cache_bytes_inuse(cache);
	dirty_inuse = __wt_cache_dirty_inuse(cache);
	bytes_max = conn->cache_size + 1;

	*fullp = (int)((100 * bytes_inuse) / bytes_max); /*获得填充率，按百分比计算，100表示全部填充*/

	/*如果占用空间满足evict阈值，唤醒evict 服务线程进行evict page操作*/
	if(wake && (bytes_inuse > (cache->eviction_trigger * bytes_max) / 100 ||
		dirty_inuse > (cache->eviction_dirty_target * bytes_max) / 100))
		WT_RET(__wt_evict_server_wake(session));

	return 0;
}

/*判断一个session是否要进行慢操作等待*/
static inline int __wt_session_can_wait(WT_SESSION_IMPL* session)
{
	/*
	 * Return if a session available for a potentially slow operation;
	 * for example, used by the block manager in the case of flushing
	 * the system cache.
	 */
	if (!F_ISSET(session, WT_SESSION_CAN_WAIT))
		return (0);

	/*
	 * LSM sets the no-cache-check flag when holding the LSM tree lock,
	 * in that case, or when holding the schema lock, we don't want to
	 * highjack the thread for eviction.
	 */
	if (F_ISSET(session, WT_SESSION_NO_CACHE_CHECK | WT_SESSION_SCHEMA_LOCKED))
		return (0);

	return 1;
}




