/*************************************************************************
*定义LOG的结构(LOG Sequence Number)和宏
*************************************************************************/

#define	WT_LOG_FILENAME	"WiredTigerLog"				/* Log的文件名*/
#define	WT_LOG_PREPNAME	"WiredTigerPreplog"			/* Log预分配的文件名*/
#define	WT_LOG_TMPNAME	"WiredTigerTmplog"			/* Log临时文件名*/


#define LOG_ALIGN						128
#define WT_LOG_SLOT_BUF_INIT_SIZE		(64 * 1024)

/*初始化LSN*/
#define WT_INIT_LSN(l) do{				\
	(l)->file = 1;						\
	(l)->offset = 0;					\
}while(0)

/*将lsn设置为最大值*/
#define WT_MAX_LSN(l) do{				\
	(l)->file = UINT32_MAX;				\
	(l)->offset = UINT64_MAX;			\
}while(0)

/*将lsn清空为0*/
#define	WT_ZERO_LSN(l)	do {			\
	(l)->file = 0;						\
	(l)->offset = 0;					\
}while(0)

/*判断LSN是否为起始位置*/
#define	WT_IS_INIT_LSN(l)				((l)->file == 1 && (l)->offset == 0)
/*判断LSN是否为最大值位置*/
#define	WT_IS_MAX_LSN(l)				((l)->file == UINT32_MAX && (l)->offset == INT64_MAX)

#define	LOGC_KEY_FORMAT					WT_UNCHECKED_STRING(IqI)
#define	LOGC_VALUE_FORMAT				WT_UNCHECKED_STRING(qIIIuu)

/*跳过WT_LOG_RECORD的头偏移*/
#define	LOG_SKIP_HEADER(data)			((const uint8_t *)(data) + offsetof(WT_LOG_RECORD, record))
/*获得LOG_REC的数据长度*/
#define	LOG_REC_SIZE(size)				((size) - offsetof(WT_LOG_RECORD, record))

/* LSN之间的大小比较宏
 * lsn1 > lsn2,返回1
 * lsn1 == lsn2,返回0
 * lsn1 < lsn2,返回-1
 */
#define	LOG_CMP(lsn1, lsn2)								\
	((lsn1)->file != (lsn2)->file ?						\
	((lsn1)->file < (lsn2)->file ? -1 : 1) :			\
	((lsn1)->offset != (lsn2)->offset ?					\
	((lsn1)->offset < (lsn2)->offset ? -1 : 1) : 0))

/* LOG SLOT的状态
 * Possible values for the consolidation array slot states:
 * (NOTE: Any new states must be > WT_LOG_SLOT_DONE and < WT_LOG_SLOT_READY.)
 *
 * < WT_LOG_SLOT_DONE	- threads are actively writing to the log.
 * WT_LOG_SLOT_DONE		- all activity on this slot is complete.
 * WT_LOG_SLOT_FREE		- slot is available for allocation.
 * WT_LOG_SLOT_PENDING	- slot is transitioning from ready to active.
 * WT_LOG_SLOT_WRITTEN	- slot is written and should be processed by worker.
 * WT_LOG_SLOT_READY	- slot is ready for threads to join.
 * > WT_LOG_SLOT_READY	- threads are actively consolidating on this slot.
 */
#define	WT_LOG_SLOT_DONE	0
#define	WT_LOG_SLOT_FREE	1
#define	WT_LOG_SLOT_PENDING	2
#define	WT_LOG_SLOT_WRITTEN	3
#define	WT_LOG_SLOT_READY	4

/*slot flags的值标示*/
#define	SLOT_BUF_GROW	0x01			/* Grow buffer on release */
#define	SLOT_BUFFERED	0x02			/* Buffer writes */
#define	SLOT_CLOSEFH	0x04			/* Close old fh on release */
#define	SLOT_SYNC		0x08			/* Needs sync on release */
#define	SLOT_SYNC_DIR	0x10			/* Directory sync on release */

#define	SLOT_INIT_FLAGS	(SLOT_BUFFERED)

/*非法的slot index值，相当于-1*/
#define	SLOT_INVALID_INDEX	0xffffffff

#define LOG_FIRST_RECORD	log->allocsize

#define	SLOT_ACTIVE			1
#define	SLOT_POOL			16

#define	WT_LOG_FORCE_CONSOLIDATE	0x01	/* Disable direct writes */

#define	WT_LOG_RECORD_COMPRESSED	0x01	/* Compressed except hdr */

typedef /*WT_COMPILER_TYPE_ALIGN(WT_CACHE_LINE_ALIGNMENT)*/ struct 
{
	int64_t				slot_state;					/*slot状态*/
	uint64_t			slot_group_size;			/*slot group size*/
	int32_t				slot_error;					/*slot的错误码值*/

	uint32_t			slot_index;					/* Active slot index */
	wt_off_t			slot_start_offset;			/* Starting file offset */

	WT_LSN				slot_release_lsn;			/* Slot 释放的LSN值 */
	WT_LSN				slot_start_lsn;				/* Slot 开始的LSN值 */
	WT_LSN				slot_end_lsn;				/* Slot 结束的LSN值 */
	WT_FH*				slot_fh;					/* slot对应的文件handler */
	WT_ITEM				slot_buf;					/* Buffer for grouped writes */
	int32_t				slot_churn;					/* Active slots are scarce. */

	uint32_t			flags;						/* slot flags,主要是对slot的一些调整操作标识*/	
} WT_LOGSLOT;

/*WT_MYSLOT结构*/
typedef struct 
{
	WT_LOGSLOT*			slot;
	wt_off_t			offset;
} WT_MYSLOT;

/*定义WT_LOG结构*/
typedef struct  
{
	uint32_t			allocsize;					/*logrec最小对齐单元长度*/
	wt_off_t			log_written;

	/*log文件相关变量*/
	uint32_t			fileid;
	uint32_t			prep_fileid;
	uint32_t			prep_missed;

	WT_FH*				log_fh;						/*正在使用的log文件handler*/
	WT_FH*				log_close_fh;				/*上一个被关闭的log文件handler*/
	WT_FH*				log_dir_fh;					/*log目录索引文件的handler*/

	/*系统的LSN定义*/
	WT_LSN				alloc_lsn;					/* Next LSN for allocation */
	WT_LSN				ckpt_lsn;					/* 最后一次建立ckeckpoint的LSN位置 */
	WT_LSN				first_lsn;					/* 日志起始的LSN */
	WT_LSN				sync_dir_lsn;				/* 最后一次sync dir的LSN位置 */
	WT_LSN				sync_lsn;					/* 日志文件最后一次sync LSN位置*/
	WT_LSN				trunc_lsn;					/* 在恢复过程中，如果有日志数据损坏，那么需要截掉这个位置后的所有日志文件，表示开始截掉数据的LSN*/
	WT_LSN				write_lsn;					/* 最后一次写日志的LSN位置 */

	/*log对象的线程同步latch*/
	WT_SPINLOCK			log_lock;					/* Locked: Logging fields */
	WT_SPINLOCK			log_slot_lock;				/* Locked: Consolidation array */
	WT_SPINLOCK			log_sync_lock;				/* Locked: Single-thread fsync */

	WT_RWLOCK*			log_archive_lock;			/* Archive and log cursors */

	/* Notify any waiting threads when sync_lsn is updated. */
	WT_CONDVAR*			log_sync_cond;
	/* Notify any waiting threads when write_lsn is updated. */
	WT_CONDVAR*			log_write_cond;

	uint32_t			pool_index;
	WT_LOGSLOT*			slot_array[SLOT_ACTIVE];	/*已就绪的log slots*/
	WT_LOGSLOT			slot_pool[SLOT_POOL];		/*log slots池，所有的log slot都在里面*/

	uint32_t			flags;						/*当前日志对象操作的标识：例如：WT_LOG_DSYNC,WT_LOGSCAN_ONE等*/
} WT_LOG;

/*wt record log的定义*/
typedef struct  
{
	uint32_t			len;
	uint32_t			checksum;

	uint16_t			flags;
	uint8_t				unused[2];
	uint32_t			mem_len;
	uint8_t				record[0];		/*logrec body*/
} WT_LOG_RECORD;

#define	WT_LOG_MAGIC			0x101064
#define	WT_LOG_MAJOR_VERSION	1
#define WT_LOG_MINOR_VERSION	0

struct __wt_log_desc
{
	uint32_t			log_magic;			/*魔法校验字*/
	uint16_t			majorv;
	uint16_t			minorv;
	uint64_t			log_size;
};

struct __wt_log_rec_desc
{
	const char*			fmt;
	int					(*print)(WT_SESSION_IMPL* session, uint8_t** p, uint8_t* end);
};

struct __wt_log_op_desc
{
	const char*			fmt;
	int					(*print)(WT_SESSION_IMPL* session, uint8_t** p, uint8_t* end);
};




