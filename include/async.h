/*************************************************************************
*外部异步IO(FILE FLUSH) API实现
*************************************************************************/

typedef enum {
	WT_ASYNCOP_ENQUEUED,		/* Placed on the work queue */
	WT_ASYNCOP_FREE,			/* Able to be allocated to user */
	WT_ASYNCOP_READY,			/* Allocated and ready for user to use */
	WT_ASYNCOP_WORKING			/* Operation in progress by worker */
} WT_ASYNC_STATE;

typedef enum{
	WT_ASYNC_FLUSH_NONE=0,		/* No flush in progress */
	WT_ASYNC_FLUSH_COMPLETE,	/* Notify flush caller it's done */
	WT_ASYNC_FLUSH_IN_PROGRESS,	/* Prevent other callers */
	WT_ASYNC_FLUSHING			/* Notify workers */
}WT_ASYNC_FLUSH_STATE;

#define	MAX_ASYNC_SLEEP_USECS	100000	/* Maximum sleep waiting for work */
#define	MAX_ASYNC_YIELD			200		/* Maximum number of yields for work */

/*快速得到conn和session的宏*/
#define	O2C(op)	((WT_CONNECTION_IMPL *)(op)->iface.connection)
#define	O2S(op)	(((WT_CONNECTION_IMPL *)(op)->iface.connection)->default_session)

struct __wt_async_format 
{
	STAILQ_ENTRY(__wt_async_format) q;
	const char	*config;
	uint64_t	cfg_hash;		/* Config hash */
	const char	*uri;
	uint64_t	uri_hash;		/* URI hash */
	const char	*key_format;
	const char	*value_format;
};

struct __wt_async_op_impl {
	WT_ASYNC_OP	iface;

	WT_ASYNC_CALLBACK	*cb;	/*回调函数，用于完成flush后的通告*/

	uint32_t	internal_id;	/* Array position id. */
	uint64_t	unique_id;		/* Unique identifier. */

	WT_ASYNC_FORMAT *format;	/* Format structure */
	WT_ASYNC_STATE	state;		/* Op state */
	WT_ASYNC_OPTYPE	optype;		/* Operation type */
};

#define	OPS_INVALID_INDEX		0xffffffff
#define	WT_ASYNC_MAX_WORKERS	20

/*定义async子模块*/
struct __wt_async
{
	WT_SPINLOCK			ops_lock;
	WT_ASYNC_OP_IMPL*	async_ops;
	uint32_t			ops_index;
	uint64_t			op_id;
	WT_ASYNC_OP_IMPL**	async_queue;
	uint32_t			async_qsize;

	uint64_t			alloc_head;	/* Next slot to enqueue */
	uint64_t			head;		/* Head visible to worker */
	uint64_t			alloc_tail;	/* Next slot to dequeue */
	uint64_t			tail_slot;	/* Worker slot consumed */

	STAILQ_HEAD(__wt_async_format_qh, __wt_async_format) formatqh;

	int					cur_queue;
	int					max_queue;
	
	WT_ASYNC_FLUSH_STATE flush_state;
	WT_CONDVAR*			flush_cond;
	WT_ASYNC_OP_IMPL	flush_op;
	uint32_t			flush_count;
	uint64_t			flush_gen;

	WT_SESSION_IMPL*	worker_sessions[WT_ASYNC_MAX_WORKERS];
	wt_thread_t			worker_tids[WT_ASYNC_MAX_WORKERS];

	uint32_t			flags;
};

struct __wt_async_cursor
{
	STAILQ_ENTRY(__wt_async_cursor)	q;
	uint64_t			cfg_hash;
	uint64_t			uri_hash;
	WT_CURSOR*			c;
};

/*工作线程的状态结构*/
struct __wt_async_worker_state
{
	uint32_t			id;
	STAILQ_HEAD(__wt_cursor_qh, __wt_async_cursor)	cursorqh;
	uint32_t			num_cursors;
};

