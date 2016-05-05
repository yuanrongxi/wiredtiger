/**************************************************************************
*定义wiredtiger的block(表空间)管理接口
**************************************************************************/
#define WT_BLOCK_INVALID_OFFSET			0


struct __wt_extlist
{
	char*					name;	

	uint64_t				bytes;					/*字节数*/
	uint32_t				entries;				/*实例个数，可以看着是记录条数*/

	wt_off_t				offset;					/*跳表在文件中的起始位置*/
	uint32_t				cksum;					/*extlist数据的checksum*/
	uint32_t				size;					/*extlist的size总和*/
	int						track_size;				/*skip list维护的ext个数，如果read extlist block时是avail采用merge方式，如果是其他的，采用append方式*/

	WT_EXT*					last;					/*off跳表中最后一个ext对象*/

	WT_EXT*					off[WT_SKIP_MAXDEPTH];	/*ext skip list对象*/
	WT_SIZE*				sz[WT_SKIP_MAXDEPTH];	/*size skip list的对象*/
};

/*ext跳表的单元定义*/
struct __wt_ext
{
	wt_off_t				off;					/*extent的文件文件对应的偏移量*/
	wt_off_t				size;					/*extent的大小*/
	uint8_t					depth;					/*skip list的深度*/

	WT_EXT*					next[0];				/* Offset, size skiplists */
};

/*一个block size的跳表单元*/
struct __wt_size
{
	wt_off_t				size;
	uint8_t					depth;
	WT_EXT*					off[WT_SKIP_MAXDEPTH];
	WT_SIZE*				next[WT_SKIP_MAXDEPTH];
};

/*跳表在最低层循环*/
#define	WT_EXT_FOREACH(skip, head)							\
	for ((skip) = (head)[0];								\
	(skip) != NULL; (skip) = (skip)->next[0])

/*调表在depth层循环*/
#define	WT_EXT_FOREACH_OFF(skip, head)						\
	for ((skip) = (head)[0];								\
	(skip) != NULL; (skip) = (skip)->next[(skip)->depth])

#define	WT_BM_CHECKPOINT_VERSION	1					/* Checkpoint format version */
#define	WT_BLOCK_EXTLIST_MAGIC		71002				/* Identify a list */

/*一个checkpoint信息对象*/
struct __wt_block_ckpt
{
	uint8_t					version;		/*block checkpoint的系统版本号*/
	wt_off_t				root_offset;	/*block checkpoint信息起始位置偏移*/
	uint32_t				root_cksum;		/*block checkpoint信息的checksum*/
	uint32_t				root_size;		/*block checkpoint信息的大小*/

	WT_EXTLIST				alloc;			/* Extents allocated */
	WT_EXTLIST				avail;			/* Extents available */
	WT_EXTLIST				discard;		/* Extents discarded */
	wt_off_t				file_size;		/*checkpoint file size*/

	uint64_t				ckpt_size;		/* Checkpoint byte count*/
	WT_EXTLIST				ckpt_avail;		/* Checkpoint free'd extents */
	WT_EXTLIST				ckpt_alloc;		/* Checkpoint archive */
	WT_EXTLIST				ckpt_discard;	/* Checkpoint archive */
};

/*block manger定义*/
struct __wt_bm {
						/* Methods */
	int (*addr_string)(WT_BM *, WT_SESSION_IMPL *, WT_ITEM *, const uint8_t *, size_t);
	int (*addr_valid)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t);
	u_int (*block_header)(WT_BM *);
	int (*checkpoint)(WT_BM *, WT_SESSION_IMPL *, WT_ITEM *, WT_CKPT *, int);
	int (*checkpoint_load)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t, uint8_t *, size_t *, int);
	int (*checkpoint_resolve)(WT_BM *, WT_SESSION_IMPL *);
	int (*checkpoint_unload)(WT_BM *, WT_SESSION_IMPL *);
	int (*close)(WT_BM *, WT_SESSION_IMPL *);
	int (*compact_end)(WT_BM *, WT_SESSION_IMPL *);
	int (*compact_page_skip)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t, int *);
	int (*compact_skip)(WT_BM *, WT_SESSION_IMPL *, int *);
	int (*compact_start)(WT_BM *, WT_SESSION_IMPL *);
	int (*free)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t);
	int (*preload)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t);
	int (*read)(WT_BM *, WT_SESSION_IMPL *, WT_ITEM *, const uint8_t *, size_t);
	int (*salvage_end)(WT_BM *, WT_SESSION_IMPL *);
	int (*salvage_next)(WT_BM *, WT_SESSION_IMPL *, uint8_t *, size_t *, int *);
	int (*salvage_start)(WT_BM *, WT_SESSION_IMPL *);
	int (*salvage_valid)(WT_BM *, WT_SESSION_IMPL *, uint8_t *, size_t, int);
	int (*stat)(WT_BM *, WT_SESSION_IMPL *, WT_DSRC_STATS *stats);
	int (*sync)(WT_BM *, WT_SESSION_IMPL *, int);
	int (*verify_addr)(WT_BM *, WT_SESSION_IMPL *, const uint8_t *, size_t);
	int (*verify_end)(WT_BM *, WT_SESSION_IMPL *);
	int (*verify_start)(WT_BM *, WT_SESSION_IMPL *, WT_CKPT *);
	int (*write) (WT_BM *,WT_SESSION_IMPL *, WT_ITEM *, uint8_t *, size_t *, int);
	int (*write_size)(WT_BM *, WT_SESSION_IMPL *, size_t *);

	WT_BLOCK*	block;			/* Underlying file */

	void*		map;					/* Mapped region */
	size_t		maplen;
	void*		mappingcookie;
	int			is_live;				/* The live system */
};

/*__wt_block块定义*/
struct __wt_block
{
	const char*				name;				/*block对应的文件名*/
	uint64_t				name_hash;			/*文件名hash*/

	uint32_t				ref;				/*对象引用计数*/
	WT_FH*					fh;					/*block文件的handler*/
	SLIST_ENTRY(__wt_block) l;
	SLIST_ENTRY(__wt_block) hashl;

	uint32_t				allocfirst;			/*从文件开始处进行写的标识*/		
	uint32_t				allocsize;			/*文件写入对齐的长度*/
	size_t					os_cache;			/*当前block中在os page cache中的数据字节数*/
	size_t					os_cache_max;		/*操作系统对文件最大的page cache的字节数*/
	size_t					os_cache_dirty;		/*当前脏数据的字节数*/
	size_t					os_cache_dirty_max;	/*允许最大的脏数据字节数*/

	u_int					block_header;		/*block header的长度*/
	WT_SPINLOCK				live_lock;			/*对live的保护锁*/
	WT_BLOCK_CKPT			live;				/*checkpoint的详细信息*/

	int						ckpt_inprogress;	/*是否正在进行checkpoint*/
	int						compact_pct_tenths;

	wt_off_t				slvg_off;

	int						verify;
	wt_off_t				verify_size;	/* Checkpoint's file size */
	WT_EXTLIST				verify_alloc;	/* Verification allocation list */
	uint64_t				frags;			/* Maximum frags in the file */
	uint8_t*				fragfile;		/* Per-file frag tracking list */
	uint8_t*				fragckpt;		/* Per-checkpoint frag tracking list */
};

#define WT_BLOCK_MAGIC				120897
#define WT_BLOCK_MAJOR_VERSION		1
#define WT_BLOCK_MINOR_VERSION		0

#define	WT_BLOCK_DESC_SIZE			16
/*WT BLOCK的文件描述*/
struct __wt_block_desc
{
	uint32_t				magic;			/*魔法校验值*/
	uint16_t				majorv;			/*大版本*/
	uint16_t				minorv;			/*小版本*/

	uint32_t				cksum;			/*blocks的checksum*/
	uint32_t				unused;			
};

#define WT_BLOCK_DATA_CKSUM			0x01
#define	WT_BLOCK_HEADER_SIZE		12
/*磁盘block header结构*/
struct __wt_block_header
{
	uint32_t				disk_size;		/*在磁盘上的page大小*/
	uint32_t				cksum;			/*checksum*/
	uint8_t					flags;	
	uint8_t					unused[3];		/*没有用，仅仅是为了填充对齐*/
};

/*定位block的数据的开始位置*/
#define	WT_BLOCK_HEADER_BYTE_SIZE					(WT_PAGE_HEADER_SIZE + WT_BLOCK_HEADER_SIZE)
#define	WT_BLOCK_HEADER_BYTE(dsk)					((void *)((uint8_t *)(dsk) + WT_BLOCK_HEADER_BYTE_SIZE))

#define	WT_BLOCK_COMPRESS_SKIP	64

