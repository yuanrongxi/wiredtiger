/***************************************************************************
*定义cursor游标的数据结构
***************************************************************************/

/*初始化cursor 对象n*/
#define	WT_CURSOR_STATIC_INIT(n,		\
	get_key,							\
	get_value,							\
	set_key,							\
	set_value,							\
	compare,							\
	equals,								\
	next,								\
	prev,								\
	reset,								\
	search,								\
	search_near,						\
	insert,								\
	update,								\
	remove,								\
	reconfigure,						\
	close)								\
	static const WT_CURSOR n = {					\
	NULL,								/* session */			\
	NULL,								/* uri */			\
	NULL,								/* key_format */		\
	NULL,								/* value_format */		\
	(int (*)(WT_CURSOR *, ...))(get_key),				\
	(int (*)(WT_CURSOR *, ...))(get_value),				\
	(void (*)(WT_CURSOR *, ...))(set_key),				\
	(void (*)(WT_CURSOR *, ...))(set_value),			\
	(int (*)(WT_CURSOR *, WT_CURSOR *, int *))(compare),		\
	(int (*)(WT_CURSOR *, WT_CURSOR *, int *))(equals),		\
	next,								\
	prev,								\
	reset,								\
	search,								\
	(int (*)(WT_CURSOR *, int *))(search_near),			\
	insert,								\
	update,								\
	remove,								\
	close,								\
	(int (*)(WT_CURSOR *, const char *))(reconfigure),		\
	{ NULL, NULL },					/* TAILQ_ENTRY q */		\
	0,								/* recno key */			\
	{ 0 },							/* recno raw buffer */	\
	NULL,							/* json_private */		\
	NULL,							/* lang_private */		\
	{ NULL, 0, 0, NULL, 0 },		/* WT_ITEM key */		\
	{ NULL, 0, 0, NULL, 0 },		/* WT_ITEM value */		\
	0,								/* int saved_err */		\
	NULL,							/* internal_uri */		\
	0								/* uint32_t flags */	\
}

#define	WT_CBT_ACTIVE			0x01	/* Active in the tree */
#define	WT_CBT_ITERATE_APPEND	0x02	/* Col-store: iterating append list */
#define	WT_CBT_ITERATE_NEXT		0x04	/* Next iteration configuration */
#define	WT_CBT_ITERATE_PREV		0x08	/* Prev iteration configuration */
#define	WT_CBT_MAX_RECORD		0x10	/* Col-store: past end-of-table */
#define	WT_CBT_SEARCH_SMALLEST	0x20	/* Row-store: small-key insert list */

struct __wt_cursor_backup_entry
{
	char*			name;			/*file name*/
	WT_DATA_HANDLE* handle;			/*Handle*/
};

struct __wt_cursor_backup {
	WT_CURSOR iface;

	size_t			next;					/* Cursor position */
	FILE*			bfp;						/* Backup file */
	uint32_t		maxid;					/* Maximum log file ID seen */

	WT_CURSOR_BACKUP_ENTRY *list;			/* List of files to be copied. */
	size_t			list_allocated;
	size_t			list_next;
};

#define	WT_CURSOR_BACKUP_ID(cursor)	(((WT_CURSOR_BACKUP *)cursor)->maxid)

/*btree cursor结构*/
struct __wt_cursor_btree
{
	WT_CURSOR		iface;
	WT_BTREE*		btree;
	WT_REF*			ref;							/*当前page的参考信息*/

	uint32_t		slot;							/*WT_COL/WT_ROW 0-based slot*/

	WT_INSERT_HEAD* ins_head;						/*insert列表头单元*/
	WT_INSERT*		ins;							/*当前cursor指向的列表单元*/
	WT_INSERT**		ins_stack[WT_SKIP_MAXDEPTH];	/*检索stack*/
	WT_INSERT*		next_stack[WT_SKIP_MAXDEPTH];	/*在检索期间的下一个item存放空间*/
	
	uint32_t		page_deleted_count;
	uint64_t		recno;							/*当前记录序号*/

	/*
	* The search function sets compare to:
	*	< 0 if the found key is less than the specified key
	*	  0 if the found key matches the specified key
	*	> 0 if the found key is larger than the specified key
	*/
	int				compare;

	WT_ITEM			search_key;
	
	/*最后一条记录的序号，因为计算variable-length 类型的列存储最后一个序号是非常麻烦的，一般是只计算一次并且存放在cache中*/
	uint64_t		last_standard_recno;
	/**/
	uint32_t		row_iteration_slot;
	
	WT_COL*			cip_saved;

	WT_ROW*			rip_saved;

	WT_ITEM			tmp;

	WT_UPDATE*		modify_update;

	uint8_t			v;
	uint8_t			append_tree;

	uint8_t			flags;
};

struct __wt_cursor_bulk
{
	WT_CURSOR_BTREE cbt;
	WT_REF*			ref;
	WT_PAGE*		leaf;
	WT_ITEM			last;
	uint64_t		rle;
	uint32_t		entry;
	uint32_t		nrecs;
	int				bitmap;
	void*			reconcile;
};

struct __wt_cursor_config 
{
	WT_CURSOR		iface;
};

struct __wt_cursor_data_source
{
	WT_CURSOR		iface;
	WT_COLLATOR*	collator;
	int				collator_owned;
	WT_CURSOR*		source;
};

struct __wt_cursor_dump 
{
	WT_CURSOR iface;
	WT_CURSOR *child;
};

struct __wt_cursor_index 
{
	WT_CURSOR iface;

	WT_TABLE *table;
	WT_INDEX *index;
	const char *key_plan, *value_plan;

	WT_CURSOR *child;
	WT_CURSOR **cg_cursors;
};

struct __wt_cursor_json 
{
	char	*key_buf;				/* JSON formatted string */
	char	*value_buf;				/* JSON formatted string */
	WT_CONFIG_ITEM key_names;		/* Names of key columns */
	WT_CONFIG_ITEM value_names;		/* Names of value columns */
};

struct __wt_cursor_log 
{
	WT_CURSOR iface;

	WT_LSN		*cur_lsn;			/* LSN of current record */
	WT_LSN		*next_lsn;			/* LSN of next record */
	WT_ITEM		*logrec;			/* Copy of record for cursor */
	WT_ITEM		*opkey, *opvalue;	/* Op key/value copy */
	const uint8_t	*stepp, *stepp_end;	/* Pointer within record */
	uint8_t		*packed_key;		/* Packed key for 'raw' interface */
	uint8_t		*packed_value;		/* Packed value for 'raw' interface */
	uint32_t	step_count;			/* Intra-record count */
	uint32_t	rectype;			/* Record type */
	uint64_t	txnid;				/* Record txnid */
	uint32_t	flags;
};

#define	WT_MDC_POSITIONED	0x01
#define	WT_MDC_ONMETADATA	0x02

struct __wt_cursor_metadata 
{
	WT_CURSOR	iface;
	WT_CURSOR*	file_cursor;		/* Queries of regular metadata */

	uint32_t	flags;
};

struct __wt_cursor_stat {
	WT_CURSOR iface;

	int	notinitialized;				/* Cursor not initialized */
	int	notpositioned;				/* Cursor not positioned */

	WT_STATS *stats;				/* Stats owned by the cursor */
	WT_STATS *stats_first;			/* First stats reference */
	int	  stats_base;				/* Base statistics value */
	int	  stats_count;				/* Count of stats elements */

	union {							/* Copies of the statistics */
		WT_DSRC_STATS dsrc_stats;
		WT_CONNECTION_STATS conn_stats;
	} u;

	const char **cfg;				/* Original cursor configuration */

	int	 key;						/* Current stats key */
	uint64_t v;						/* Current stats value */
	WT_ITEM	 pv;					/* Current stats value (string) */

	uint32_t flags;					/* Uses the same values as WT_CONNECTION::stat_flags field */
};

#define WT_CURSOR_STATS(cursor)		(((WT_CURSOR_STAT *)cursor)->stats_first)

struct __wt_cursor_table 
{
	WT_CURSOR iface;

	WT_TABLE *table;
	const char *plan;

	const char **cfg;		/* Saved configuration string */

	WT_CURSOR **cg_cursors;
	WT_ITEM *cg_valcopy;		/*
								* Copies of column group values, for
								* overlapping set_value calls.
								*/
	WT_CURSOR **idx_cursors;
};

#define	WT_CURSOR_PRIMARY(cursor)		(((WT_CURSOR_TABLE *)cursor)->cg_cursors[0])

#define	WT_CURSOR_RECNO(cursor)			WT_STREQ((cursor)->key_format, "r")

#define	WT_CURSOR_CHECKKEY(cursor) do {					\
	if (!F_ISSET(cursor, WT_CURSTD_KEY_SET))			\
		WT_ERR(__wt_cursor_kv_not_set(cursor, 1));		\
} while(0)

#define	WT_CURSOR_CHECKVALUE(cursor) do {				\
	if (!F_ISSET(cursor, WT_CURSTD_VALUE_SET))			\
		WT_ERR(__wt_cursor_kv_not_set(cursor, 0));		\
} while(0)

/*将key.data中的数据移到key缓冲区的头位置*/
#define	WT_CURSOR_NEEDKEY(cursor) do {					\
	if (F_ISSET(cursor, WT_CURSTD_KEY_INT)) {			\
	if (!WT_DATA_IN_ITEM(&(cursor)->key))				\
	WT_ERR(__wt_buf_set(								\
	(WT_SESSION_IMPL *)(cursor)->session,				\
	&(cursor)->key,										\
	(cursor)->key.data, (cursor)->key.size));			\
	F_CLR(cursor, WT_CURSTD_KEY_INT);					\
	F_SET(cursor, WT_CURSTD_KEY_EXT);					\
	}													\
	WT_CURSOR_CHECKKEY(cursor);							\
} while (0)

/*将value.data的数据移到value缓冲区的头位置*/
#define	WT_CURSOR_NEEDVALUE(cursor) do {				\
	if (F_ISSET(cursor, WT_CURSTD_VALUE_INT)) {			\
	if (!WT_DATA_IN_ITEM(&(cursor)->value))				\
	WT_ERR(__wt_buf_set(								\
	(WT_SESSION_IMPL *)(cursor)->session,				\
	&(cursor)->value,									\
	(cursor)->value.data, (cursor)->value.size));		\
	F_CLR(cursor, WT_CURSTD_VALUE_INT);					\
	F_SET(cursor, WT_CURSTD_VALUE_EXT);					\
	}													\
	WT_CURSOR_CHECKVALUE(cursor);						\
} while (0)

#define	WT_CURSOR_NOVALUE(cursor) do {					\
	F_CLR(cursor, WT_CURSTD_VALUE_INT);					\
} while (0)

#define	WT_CURSOR_RAW_OK			(WT_CURSTD_DUMP_HEX | WT_CURSTD_DUMP_PRINT | WT_CURSTD_RAW)