/*************************************************************************************
 * lru cache的头文件定义
 ************************************************************************************/

/*
 * Tuning constants: I hesitate to call this tuning, but we want to review some
 * number of pages from each file's in-memory tree for each page we evict.
 */

#define WT_EVICT_INT_SKEW		(1 << 20)	/*1M, Prefer leaf pages over internal pages by this many increments of the read generation.*/

#define WT_EVICT_WALK_PER_FILE	10			/* Pages to queue per file */
#define WT_EVICT_MAX_PER_FILE	100			/* Max pages to visit per file */
#define WT_EVICT_WALK_BASE		300			/* Pages tracked across file visits */
#define WT_EVICT_WALK_INCR		100			/* Pages added each walk */

#define	WT_EVICT_PASS_AGGRESSIVE	0x01
#define	WT_EVICT_PASS_ALL			0x02	/*清除所有的evict entry*/
#define	WT_EVICT_PASS_DIRTY			0x04	/*清除所有有脏数据的page的evict entry*/
#define	WT_EVICT_PASS_WOULD_BLOCK	0x08	/*清除最小read_gen的evict entry*/

/*对一个evition对象的封装*/
struct __wt_evict_entry
{
	WT_BTREE* btree;
	WT_REF*	ref;
};

#define	WT_EVICT_WORKER_RUN	0x01

/*evition thread的封装*/
struct __wt_evict_worker
{
	WT_SESSION_IMPL* session;
	u_int id;
	wt_thread_t tid;
	uint32_t flags;
};

/*cache结构定义*/
struct __wt_cache 
{
	uint64_t bytes_inmem;					/* Bytes/pages in memory */
	uint64_t pages_inmem;
	uint64_t bytes_internal;				/* Bytes of internal pages */
	uint64_t bytes_overflow;				/* Bytes of overflow pages */
	uint64_t bytes_evict;					/* Bytes/pages discarded by eviction */
	uint64_t pages_evict;
	uint64_t bytes_dirty;					/* Bytes/pages currently dirty */
	uint64_t pages_dirty;
	uint64_t bytes_read;					/* Bytes read into memory */

	uint64_t evict_max_page_size;			/* Largest page seen at eviction */

	uint64_t   read_gen;					/* Page read generation (LRU) */
	WT_CONDVAR *evict_cond;					/* Eviction server condition */
	WT_SPINLOCK evict_lock;					/* Eviction LRU queue */
	WT_SPINLOCK evict_walk_lock;			/* Eviction walk location */

	WT_CONDVAR *evict_waiter_cond;			/* Condition signalled when the eviction server populates the queue */

	u_int eviction_trigger;					/* Percent to trigger eviction */
	u_int eviction_target;					/* Percent to end eviction */
	u_int eviction_dirty_target;			/* Percent to allow dirty */

	u_int overhead_pct;						/* Cache percent adjustment */

	/*
	* LRU eviction list information.
	*/
	WT_EVICT_ENTRY *evict;					/* LRU pages being tracked */
	WT_EVICT_ENTRY *evict_current;			/* LRU current page to be evicted */
	uint32_t evict_candidates;				/* LRU list pages to evict */
	uint32_t evict_entries;					/* LRU entries in the queue */
	volatile uint32_t evict_max;			/* LRU maximum eviction slot used */
	uint32_t evict_slots;					/* LRU list eviction slots */
	WT_DATA_HANDLE *evict_file_next;		/* LRU next file to search */

	volatile uint64_t sync_request;			/* File sync requests */
	volatile uint64_t sync_complete;		/* File sync requests completed */

	/*
	* Cache pool information.
	*/
	uint64_t cp_saved_read;					/* Read count from last pass */
	uint64_t cp_current_read;				/* Read count from current pass */
	uint32_t cp_skip_count;					/* Post change stabilization */
	uint64_t cp_reserved;					/* Base size for this cache */
	WT_SESSION_IMPL *cp_session;			/* May be used for cache management */
	wt_thread_t cp_tid;						/* Thread ID for cache pool manager */

	uint32_t flags;
};

/*cache's flags type*/
#define	WT_CACHE_POOL_MANAGER	0x01	/* The active cache pool manager */
#define	WT_CACHE_POOL_RUN	0x02		/* Cache pool thread running */
#define	WT_CACHE_CLEAR_WALKS	0x04	/* Clear eviction walks */
#define	WT_CACHE_STUCK		0x08		/* Eviction server is stuck */
#define	WT_CACHE_WALK_REVERSE	0x10	/* Scan backwards for candidates */
#define	WT_CACHE_WOULD_BLOCK	0x20	/* Pages that would block apps */

/*cache pool's flags type*/
#define	WT_CACHE_POOL_MANAGED	0x01	/* Cache pool has a manager thread */
#define	WT_CACHE_POOL_ACTIVE	0x02	/* Cache pool is active */

struct __wt_cache_pool
{
	WT_SPINLOCK cache_pool_lock;
	WT_CONDVAR *cache_pool_cond;
	const char *name;
	uint64_t size;
	uint64_t chunk;
	uint64_t currently_used;
	uint32_t refs;						/* Reference count for structure. */
	
	/* Locked: List of connections participating in the cache pool. */
	TAILQ_HEAD(__wt_cache_pool_qh, __wt_connection_impl) cache_pool_qh;

	uint8_t flags_atomic;
};







