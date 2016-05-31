/************************************************************
 * wiredtiger事务模型的实现
 ***********************************************************/
#define	WT_TXN_NONE		0			/* No txn running in a session. */
#define	WT_TXN_FIRST	1			/* First transaction to run. */
#define	WT_TXN_ABORTED	UINT64_MAX	/* Update rolled back, ignore. */


#define TXNID_LE(t1, t2)	((t1) <= (t2))
#define TXNID_LT(t1, t2)	((t1) != (t2) && TXNID_LE(t1, t2))  /*为什么不是((t1) < (t2))*/

/*session的事务状态*/
#define WT_SESSION_TXN_STATE(s) (&S2C(s)->txn_global_states[(s)->id])

struct WT_COMPILER_TYPE_ALIGN(WT_CACHE_LINE_ALIGNMENT) __wt_txn_state
{
	volatile uint64_t id;			/*执行事务的事务ID*/
	volatile uint64_t snap_min;		/*执行事务在建立snapshot时最早产生的且未提交的事务ID*/
};

/*全局事务管理对象*/
struct __wt_txn_global
{
	volatile uint64_t	current;	/* Current transaction ID. */

	/* The oldest running transaction ID (may race). */
	uint64_t			last_running;

	/*
	* The oldest transaction ID that is not yet visible to some
	* transaction in the system.
	* oldest_id对系统中所有的事务不一定都可见
	*/
	volatile uint64_t	oldest_id;

	/* The oldest session found in the last scan. */
	uint32_t			oldest_session;

	/* Count of scanning threads, or -1 for exclusive access. */
	volatile int32_t	scan_count;

	/*
	* Track information about the running checkpoint. The transaction IDs
	* used when checkpointing are special. Checkpoints can run for a long
	* time so we keep them out of regular visibility checks. Eviction and
	* checkpoint operations know when they need to be aware of
	* checkpoint IDs.
	*/
	volatile uint64_t	checkpoint_gen;
	volatile uint64_t	checkpoint_id;
	volatile uint64_t	checkpoint_snap_min;

	WT_TXN_STATE*		states;		/* Per-session transaction states */
};

/* wiredtiger 事务隔离类型 */
typedef enum __wt_txn_isolation
{
	TXN_ISO_EVICTION,			/* Internal: eviction context */
	TXN_ISO_READ_UNCOMMITTED,	/*事务修改及可见*/
	TXN_ISO_READ_COMMITTED,		/*事务提交可见*/
	TXN_ISO_SNAPSHOT
}WT_TXN_ISOLATION;

/*
* WT_TXN_OP --
*	A transactional operation.  Each transaction builds an in-memory array
*	of these operations as it runs, then uses the array to either write log
*	records during commit or undo the operations during rollback.
*/
struct __wt_txn_op
{
	uint32_t fileid;
	enum {
		TXN_OP_BASIC,
		TXN_OP_INMEM,
		TXN_OP_REF,
		TXN_OP_TRUNCATE_COL,
		TXN_OP_TRUNCATE_ROW
	} type;

	union {
		/* TXN_OP_BASIC, TXN_OP_INMEM */
		WT_UPDATE *upd;
		/* TXN_OP_REF */
		WT_REF *ref;
		/* TXN_OP_TRUNCATE_COL */
		struct {
			uint64_t start, stop;
		} truncate_col;
		/* TXN_OP_TRUNCATE_ROW */
		struct {
			WT_ITEM start, stop;
			enum {
				TXN_TRUNC_ALL,
				TXN_TRUNC_BOTH,
				TXN_TRUNC_START,
				TXN_TRUNC_STOP
			} mode;
		} truncate_row;
	} u;
};

/*
* WT_TXN --
*	Per-session transaction context.
*/
struct __wt_txn
{
	uint64_t				id;					/*事务ID*/
	WT_TXN_ISOLATION		isolation;			/*隔离级别*/

   /*
	* Snapshot data:
	*	ids < snap_min are visible,
	*	ids > snap_max are invisible,
	*	everything else is visible unless it is in the snapshot.
	*/
	uint64_t			snap_min, snap_max;
	uint64_t*			snapshot;
	uint32_t			snapshot_count;
	uint32_t			txn_logsync;			/* Log sync configuration */

	WT_TXN_OP*			mod;					/*事务进行的操作对象数组*/
	size_t				mod_alloc;
	u_int				mod_count;

	WT_ITEM*			logrec;					/* Scratch buffer for in-memory log records. */

	WT_TXN_NOTIFY*		notify;

	/* Checkpoint status. */
	WT_LSN				ckpt_lsn;
	int					full_ckpt;
	uint32_t			ckpt_nsnapshot;
	WT_ITEM*			ckpt_snapshot;

	uint32_t			flags;
};

/* txn flags的值类型 */
#define	TXN_AUTOCOMMIT		0x01
#define	TXN_ERROR			0x02
#define	TXN_HAS_ID	        0x04
#define	TXN_HAS_SNAPSHOT	0x08
#define	TXN_RUNNING			0x10



