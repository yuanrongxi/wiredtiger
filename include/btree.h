/***********************************************************************
*BTREE的数据结构定义
***********************************************************************/

/*btree的版本号支持范围*/
#define	WT_BTREE_MAJOR_VERSION_MIN	1	/* Oldest version supported */
#define	WT_BTREE_MINOR_VERSION_MIN	1

#define	WT_BTREE_MAJOR_VERSION_MAX	1	/* Newest version supported */
#define	WT_BTREE_MINOR_VERSION_MAX	

/*btree的节点最大空间大小512M，或者说是1M个磁盘扇区*/
#define WT_BTREE_PAGE_SIZE_MAX		(512 * WT_MEGABYTE)

/*列式存储时数据最大的空间占用大小，4G - 1KB*/
#define	WT_BTREE_MAX_OBJECT_SIZE	(UINT32_MAX - 1024)

#define	WT_BTREE_MAX_ADDR_COOKIE	255	/* Maximum address cookie */

/*页的驱逐条件，如果页内有1000个以上不连贯的删除记录，那么需要对页做重组？*/
#define WT_BTREE_DELETE_THRESHOLD	1000

#define	WT_SPLIT_DEEPEN_MIN_CHILD_DEF	10000

#define	WT_SPLIT_DEEPEN_PER_CHILD_DEF	100

/* Flags values up to 0xff are reserved for WT_DHANDLE_* */
#define	WT_BTREE_BULK			0x00100	/* Bulk-load handle */
#define	WT_BTREE_NO_EVICTION	0x00200	/* Disable eviction */
#define	WT_BTREE_NO_HAZARD		0x00400	/* Disable hazard pointers */
#define	WT_BTREE_SALVAGE		0x00800	/* Handle is for salvage */
#define	WT_BTREE_UPGRADE		0x01000	/* Handle is for upgrade */
#define	WT_BTREE_VERIFY			0x02000	/* Handle is for verify */

#define WT_BTREE_SPECIAL_FLAGS	(WT_BTREE_BULK | WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY)

/*btree结构*/
struct __wt_btree
{
	WT_DATA_HANDLE*			dhandle;

	WT_CKPT*				ckpt;				/*checkpoint信息结构指针*/

	enum{
		BTREE_COL_FIX = 1,						/*列式定长存储*/
		BTREE_COL_VAR = 2,						/*列式边长存储*/
		BTREE_ROW	  = 3,						/*行式存储*/
	} type;

	const char*				key_format;			/*key格式串*/
	const char*				value_format;		/*value格式串*/

	uint8_t					bitcnt;				/*定长field的长度*/
	WT_COLLATOR*			collator;			/*行存储时的比较器*/
	int						collator_owned;		/*如果这个值为1，表示比较器需要进行free*/

	uint32_t				id;					/*log file ID*/

	uint32_t				key_gap;			/*行存储时的key前缀范围长度*/

	uint32_t				allocsize;			/* Allocation size */
	uint32_t				maxintlpage;		/* Internal page max size */
	uint32_t				maxintlkey;			/* Internal page max key size */
	uint32_t				maxleafpage;		/* Leaf page max size */
	uint32_t				maxleafkey;			/* Leaf page max key size */
	uint32_t				maxleafvalue;		/* Leaf page max value size */
	uint64_t				maxmempage;			/* In memory page max size */

	void*					huffman_key;		/*key值的霍夫曼编码*/
	void*					huffman_value;		/*value值的霍夫曼编码*/

	enum{
		CKSUM_ON			= 1,		
		CKSUM_OFF			= 2,
		CKSUM_UNCOMPRESSED	= 3,
	} checksum;									/*checksum开关*/

	u_int					dictionary;			/*slots字典*/
	int						internal_key_truncate;
	int						maximum_depth;		/*树的最大层数*/
	int						prefix_compression; /*前缀压缩开关*/
	u_int					prefix_compression_min;

	u_int					split_deepen_min_child;/*页split时最少的entry个数*/
	u_int					split_deepen_per_child;/*页slpit时btree层增加的平均entry个数*/
	int						split_pct;
	WT_COMPRESSOR*			compressor;			/*页数据压缩器*/

	WT_RWLOCK*				ovfl_lock;

	uint64_t				last_recno;			/*列式存储时最后的记录序号*/
	WT_REF					root;				/*btree root的根节点句柄*/
	int						modified;			/*btree修改标示*/
	int						bulk_load_ok;		/*是否允许btree占用空间增大*/

	WT_BM*					bm;					/*block manager句柄*/
	u_int					block_header;		/*block头长度，=WT_PAGE_HEADER_BYTE_SIZE*/

	uint64_t				checkpoint_gen;
	uint64_t				rec_max_txn;		/*最大可见事务ID*/
	uint64_t				write_gen;

	WT_REF*					evict_ref;			
	uint64_t				evict_priority;		/*页从内存中淘汰的优先级*/
	u_int					evict_walk_period;	/* Skip this many LRU walks */
	u_int					evict_walk_skips;	/* Number of walks skipped */
	volatile uint32_t		evict_busy;			/* Count of threads in eviction */

	int						checkpointing;		/*是否正在checkpoint*/

	WT_SPINLOCK				flush_lock;

	uint32_t				flags;
};

/*数据恢复的cookie结构*/
struct __wt_salvage_cookie
{
	uint64_t				missing;
	uint64_t				skip;
	uint64_t				take;
	int						done;
};

/**********************************************************************/

