#ifndef __WIREDTIGER_H_
#define __WIREDTIGER_H_

#if defined(__cplusplus)
extern "C" {
#endif

/*************************************
*wiredtiger版本信息(wiredtiger 2.5.3)
*************************************/
#define WIREDTIGER_VERSION_MAJOR	2
#define WIREDTIGER_VERSION_MINOR	5
#define WIREDTIGER_VERSION_PATCH	3
#define	WIREDTIGER_VERSION_STRING	"WiredTiger 2.5.3: (March 26, 2015)"

/*************************************
*标准C头文件
*************************************/
#include <sys/types.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

typedef off_t	wt_off_t;

/*函数指针定义*/
#define __F(func)	(*func)

#ifdef SWIG
%{
#include <wiredtiger.h>
%}
#endif

#define WT_HANDLE_NULLABLE(typename)	typename
#define	WT_HANDLE_CLOSED(typename)		typename

/*******************************
*公共结构的申明
*******************************/
struct __wt_async_callback;
typedef struct __wt_async_callback WT_ASYNC_CALLBACK;
struct __wt_async_op;	    typedef struct __wt_async_op WT_ASYNC_OP;
struct __wt_collator;	    typedef struct __wt_collator WT_COLLATOR;
struct __wt_compressor;	    typedef struct __wt_compressor WT_COMPRESSOR;
struct __wt_config_item;    typedef struct __wt_config_item WT_CONFIG_ITEM;

struct __wt_config_parser;
typedef struct __wt_config_parser WT_CONFIG_PARSER;
struct __wt_connection;	    typedef struct __wt_connection WT_CONNECTION;
struct __wt_cursor;	    typedef struct __wt_cursor WT_CURSOR;
struct __wt_data_source;    typedef struct __wt_data_source WT_DATA_SOURCE;
struct __wt_event_handler;  typedef struct __wt_event_handler WT_EVENT_HANDLER;
struct __wt_extension_api;  typedef struct __wt_extension_api WT_EXTENSION_API;
struct __wt_extractor;	    typedef struct __wt_extractor WT_EXTRACTOR;
struct __wt_item;	    typedef struct __wt_item WT_ITEM;
struct __wt_lsn;	    typedef struct __wt_lsn WT_LSN;
struct __wt_session;	    typedef struct __wt_session WT_SESSION;

/***************************************************
*__wt_item的定义,在使用之前不需要clean
***************************************************/
#define	WT_ITEM_ALIGNED					0x00000001
#define	WT_ITEM_INUSE					0x00000002

struct __wt_item
{
	const void*		data;		/*item的数据缓冲区指针*/
	size_t			size;		/*data的长度*/

	uint32_t		flags;
	void*			mem;		/*memory chunk,内部使用*/
	size_t			memsize;	/*memory chunk size*/
};

/*************************************************
日志序号结构定义
*************************************************/
struct __wt_lsn
{
	uint32_t		file;		/*日志文件序号*/
	wt_off_t		offset;		/*日志文件偏移位置*/
};

/*64为int在packed时候的最大长度*/
#define	WT_INTPACK64_MAXSIZE	((int)sizeof (int64_t) + 1)
#define	WT_INTPACK32_MAXSIZE	((int)sizeof (int32_t) + 1)

/************************************************
*__wt_cursor结构定义
************************************************/
struct __wt_cursor
{
	WT_SESSION*				session;
	const char*				uri;
	const char*				key_format;
	const char*				value_format;

	/*获取cursor指向记录的KEY*/
	int						__F(get_key)(WT_CURSOR *cursor, ...);
	/*获取cursor对应记录的value*/
	int						__F(get_value)(WT_CURSOR *cursor, ...);
	/*为cursor下一步操作设置对应的KEY*/
	void					__F(set_key)(WT_CURSOR *cursor, ...);
	/*为cursor下一步操作设置对应的value*/
	void					__F(set_value)(WT_CURSOR *cursor, ...);
	/*cursor匹配函数,返回值
						< 0表示，cursor在other前面，=0，表示两者相同，>0,表示在other之后*/
	int						__F(compare)(WT_CURSOR *cursor, WT_CURSOR *other, int *comparep);
	/*判断cursor是否与other相同，如果相同返回0*/
	int						__F(equals)(WT_CURSOR *cursor, WT_CURSOR *other, int *equalp);
	/*cursor向后移*/
	int						__F(next)(WT_CURSOR *cursor);
	/*cursor向后移*/
	int						__F(prev)(WT_CURSOR *cursor);
	/*对cursor重置*/
	int						__F(reset)(WT_CURSOR *cursor);
	/*根据cursor的KEY进行查找，KEY必须是刚设置的*/
	int						__F(search)(WT_CURSOR *cursor);

	int						__F(search_near)(WT_CURSOR *cursor, int *exactp);

	int						__F(insert)(WT_CURSOR *cursor);

	int						__F(update)(WT_CURSOR *cursor);

	int						__F(remove)(WT_CURSOR *cursor);

	int						__F(close)(WT_HANDLE_CLOSED(WT_CURSOR) *cursor);

	int						__F(reconfigure)(WT_CURSOR *cursor, const char *config);

	struct
	{
		WT_CURSOR*			tqe_next;
		WT_CURSOR **		tqe_prev;
	} q;		

	uint64_t				recno;			/*record number, normal and raw mode*/
	uint8_t					raw_recno_buf[WT_INTPACK64_MAXSIZE];

	void*					json_private;		/* JSON specific storage */
	void*					lang_private;		/* Language specific private storage */

	WT_ITEM					key;
	WT_ITEM					value;

	int						saved_err;
	const char*				internal_uri;

	uint32_t					flags;
};

/*cursor flag标识值*/
#define	WT_CURSTD_APPEND		0x0001
#define	WT_CURSTD_BULK			0x0002
#define	WT_CURSTD_DUMP_HEX		0x0004
#define	WT_CURSTD_DUMP_JSON		0x0008
#define	WT_CURSTD_DUMP_PRINT	0x0010
#define	WT_CURSTD_KEY_EXT		0x0020	/* Key points out of the tree. */
#define	WT_CURSTD_KEY_INT		0x0040	/* Key points into the tree. */
#define	WT_CURSTD_KEY_SET		(WT_CURSTD_KEY_EXT | WT_CURSTD_KEY_INT)
#define	WT_CURSTD_OPEN			0x0080
#define	WT_CURSTD_OVERWRITE		0x0100
#define	WT_CURSTD_RAW			0x0200
#define	WT_CURSTD_VALUE_EXT		0x0400	/* Value points out of the tree. */
#define	WT_CURSTD_VALUE_INT		0x0800	/* Value points into the tree. */
#define	WT_CURSTD_VALUE_SET		(WT_CURSTD_VALUE_EXT | WT_CURSTD_VALUE_INT)

/*异步操作类型定义*/
typedef enum{
	WT_AOP_NONE			= 0,
	WT_AOP_COMPACT,
	WT_AOP_INSERT,
	WT_AOP_REMOVE,
	WT_AOP_SEARCH,
	WT_AOP_UPDATE
}WT_ASYNC_OPTYPE;

/*定义异步操作对象*/
struct __wt_async_op{
	WT_CONNECTION*				connection;

	const char*					key_format;
	const char*					value_format;
	void*						app_private;

	/*操作cursor*/
	WT_CURSOR					c;

	/*操作函数*/
	int							__F(get_key)(WT_ASYNC_OP *op, ...);
	int							__F(get_value)(WT_ASYNC_OP *op, ...);
	void						__F(set_key)(WT_ASYNC_OP *op, ...);
	void						__F(set_value)(WT_ASYNC_OP *op, ...);
	int							__F(search)(WT_ASYNC_OP *op);
	int							__F(insert)(WT_ASYNC_OP *op);
	int							__F(update)(WT_ASYNC_OP *op);
	int							__F(remove)(WT_ASYNC_OP *op);
	int							__F(compact)(WT_ASYNC_OP *op);
	uint64_t					__F(get_id)(WT_ASYNC_OP *op);
	WT_ASYNC_OPTYPE				__F(get_type)(WT_ASYNC_OP *op);

};

/*定义wt session*/
struct __wt_session
{
	WT_CONNECTION*				connection;
	void*						app_private;

	int							__F(close)(WT_HANDLE_CLOSED(WT_SESSION) *session, const char *config);
	int							__F(reconfigure)(WT_SESSION *session, const char *config);
	const char*					__F(strerror)(WT_SESSION *session, int error);
	int							__F(open_cursor)(WT_SESSION *session, const char *uri, WT_HANDLE_NULLABLE(WT_CURSOR) *to_dup,
												const char *config, WT_CURSOR **cursorp);
	int							__F(create)(WT_SESSION *session, const char *name, const char *config);
	int							__F(compact)(WT_SESSION *session, const char *name, const char *config);
	int							__F(drop)(WT_SESSION *session, const char *name, const char *config);
	int							__F(log_printf)(WT_SESSION *session, const char *fmt, ...);
	int							__F(rename)(WT_SESSION *session, const char *uri, const char *newuri, const char *config);
	int							__F(salvage)(WT_SESSION *session, const char *name, const char *config);
	int							__F(truncate)(WT_SESSION *session,const char *name, WT_HANDLE_NULLABLE(WT_CURSOR) *start, 
												WT_HANDLE_NULLABLE(WT_CURSOR) *stop,const char *config);

	int							__F(upgrade)(WT_SESSION *session, const char *name, const char *config);
	int							__F(verify)(WT_SESSION *session, const char *name, const char *config);
	int							__F(begin_transaction)(WT_SESSION *session, const char *config);
	int							__F(commit_transaction)(WT_SESSION *session, const char *config);
	int							__F(rollback_transaction)(WT_SESSION *session, const char *config);
	int							__F(checkpoint)(WT_SESSION *session, const char *config);
	int							__F(transaction_pinned_range)(WT_SESSION* session, uint64_t *range);
};

/*定义wt connection,connection是对应一个database实例*/
struct __wt_connection
{
	int							__F(async_flush)(WT_CONNECTION* connection);
	int							__F(async_new_op)(WT_CONNECTION *connection, const char *uri, const char *config, WT_ASYNC_CALLBACK *callback, WT_ASYNC_OP **asyncopp);
	int							__F(close)(WT_HANDLE_CLOSED(WT_CONNECTION) *connection, const char *config);
	int							__F(reconfigure)(WT_CONNECTION *connection, const char *config);
	const char*					__F(get_home)(WT_CONNECTION *connection);						
	int							__F(configure_method)(WT_CONNECTION *connection, const char *method, const char *uri,
											const char *config, const char *type, const char *check);
	int							__F(is_new)(WT_CONNECTION *connection);
	int							__F(open_session)(WT_CONNECTION *connection, WT_EVENT_HANDLER *errhandler, const char *config, WT_SESSION **sessionp);
	int							__F(load_extension)(WT_CONNECTION *connection, const char *path, const char *config);
	int							__F(add_data_source)(WT_CONNECTION *connection, const char *prefix, WT_DATA_SOURCE *data_source, const char *config);
	int							__F(add_collator)(WT_CONNECTION *connection, const char *name, WT_COLLATOR *collator, const char *config);
	int							__F(add_compressor)(WT_CONNECTION *connection, const char *name, WT_COMPRESSOR *compressor, const char *config);
	int							__F(add_extractor)(WT_CONNECTION *connection, const char *name, WT_EXTRACTOR *extractor, const char *config);
	WT_EXTENSION_API*			__F(get_extension_api)(WT_CONNECTION *wt_conn);
};

/*WiredTiger数据库API*/
int								wiredtiger_open(const char* home, WT_EVENT_HANDLER* errhandler, const char* config, WT_CONNECTION* connectionp);

const char*						wiredtiger_strerror(int error);

/*回调事件对象定义*/
struct __wt_async_callback
{
	int							(*notify)(WT_ASYNC_CALLBACK *cb, WT_ASYNC_OP *op, int op_ret, uint32_t flags);
};

/*wiredtiger的事件处理对象*/
struct __wt_event_handler
{
	int							(*handle_error)(WT_EVENT_HANDLER *handler, WT_SESSION *session, int error, const char *message);
	int							(*handle_message)(WT_EVENT_HANDLER *handler, WT_SESSION *session, const char *message);
	int							(*handle_progress)(WT_EVENT_HANDLER *handler, WT_SESSION *session, const char *operation, uint64_t progress);
	int							(*handle_close)(WT_EVENT_HANDLER *handler, WT_SESSION *session, WT_CURSOR *cursor);
};

struct __wt_pack_stream;
typedef struct __wt_pack_stream WT_PACK_STREAM;

/*将一个结构对象pack到buffer中*/
int								wiredtiger_struct_pack(WT_SESSION *session, void *buffer, size_t size, const char *format, ...);
/*计算一个结构对象pack占用的空间字节数*/
int								wiredtiger_struct_size(WT_SESSION *session, size_t *sizep, const char *format, ...);

int								wiredtiger_struct_unpack(WT_SESSION *session, const void *buffer, size_t size, const char *format, ...);

int								wiredtiger_pack_start(WT_SESSION *session, const char *format, void *buffer, size_t size, WT_PACK_STREAM **psp);

int								wiredtiger_unpack_start(WT_SESSION *session, const char *format, const void *buffer, size_t size, WT_PACK_STREAM **psp);

int								wiredtiger_pack_close(WT_PACK_STREAM *ps, size_t *usedp);

int								wiredtiger_pack_item(WT_PACK_STREAM *ps, WT_ITEM *item);

int								wiredtiger_pack_int(WT_PACK_STREAM *ps, int64_t i);

int								wiredtiger_pack_str(WT_PACK_STREAM *ps, const char *s);

int								wiredtiger_pack_uint(WT_PACK_STREAM *ps, uint64_t u);

int								wiredtiger_unpack_item(WT_PACK_STREAM *ps, WT_ITEM *item);

int								wiredtiger_unpack_int(WT_PACK_STREAM *ps, int64_t *ip);

int								wiredtiger_unpack_str(WT_PACK_STREAM *ps, const char **sp);

int								wiredtiger_unpack_uint(WT_PACK_STREAM *ps, uint64_t *up);

/*定义config item结构*/
struct __wt_config_item
{
	const char*					str;
	size_t						len;
	
	int64_t						val;

	/*值的类型*/
	enum{
		WT_CONFIG_ITEM_STRING,
		WT_CONFIG_ITEM_BOOL,
		WT_CONFIG_ITEM_ID,
		WT_CONFIG_ITEM_NUM,
		WT_CONFIG_ITEM_STRUCT
	} type;
};
/*打开一个wiredtiger的配置文件*/
int wiredtiger_config_parser_open(WT_SESSION *session, const char *config, size_t len, WT_CONFIG_PARSER **config_parserp);

/*定义wiredtiger的配置解析器*/
struct __wt_config_parser
{
	int __F(close)(WT_CONFIG_PARSER *config_parser);
	int __F(next)(WT_CONFIG_PARSER *config_parser, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value);
	int __F(get)(WT_CONFIG_PARSER *config_parser, const char *key, WT_CONFIG_ITEM *value);
};

const char *wiredtiger_version(int *majorp, int *minorp, int *patchp);

/*错误码定义*/
#define	WT_ROLLBACK					-31800
#define	WT_DUPLICATE_KEY			-31801
#define	WT_ERROR					-31802
#define	WT_NOTFOUND					-31803
#define	WT_PANIC					-31804
#define	WT_RESTART					-31805
#define	WT_RUN_RECOVERY				-31806
#define	WT_DEADLOCK					WT_ROLLBACK

struct __wt_config_arg;     typedef struct __wt_config_arg WT_CONFIG_ARG;

struct __wt_collator
{
	int (*compare)(WT_COLLATOR *collator, WT_SESSION *session, const WT_ITEM *key1, const WT_ITEM *key2, int *cmp);
	int (*customize)(WT_COLLATOR *collator, WT_SESSION *session, const char *uri, WT_CONFIG_ITEM *appcfg, WT_COLLATOR **customp);
	int (*terminate)(WT_COLLATOR *collator, WT_SESSION *session);
};

struct __wt_compressor
{
	int (*compress)(WT_COMPRESSOR *compressor, WT_SESSION *session, uint8_t *src, size_t src_len, 
					uint8_t *dst, size_t dst_len, size_t *result_lenp, int *compression_failed);

	int (*compress_raw)(WT_COMPRESSOR *compressor, WT_SESSION *session,
						size_t page_max, int split_pct, size_t extra,
						uint8_t *src, uint32_t *offsets, uint32_t slots,
						uint8_t *dst, size_t dst_len,
						int final, size_t *result_lenp, uint32_t *result_slotsp);

	int (*decompress)(WT_COMPRESSOR *compressor, WT_SESSION *session,
		uint8_t *src, size_t src_len,
		uint8_t *dst, size_t dst_len, size_t *result_lenp);

	int (*pre_size)(WT_COMPRESSOR *compressor, WT_SESSION *session, uint8_t *src, size_t src_len, size_t *result_lenp);

	int (*terminate)(WT_COMPRESSOR *compressor, WT_SESSION *session);
};

struct __wt_data_source 
{
	int (*create)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*compact)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*drop)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*open_cursor)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);
	int (*rename)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, const char *newuri, WT_CONFIG_ARG *config);
	int (*salvage)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*truncate)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*range_truncate)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, WT_CURSOR *start, WT_CURSOR *stop);
	int (*verify)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	int (*checkpoint)(WT_DATA_SOURCE *dsrc, WT_SESSION *session, WT_CONFIG_ARG *config);
	int (*terminate)(WT_DATA_SOURCE *dsrc, WT_SESSION *session);
};

struct __wt_extractor
{
	int (*extract)(WT_EXTRACTOR *extractor, WT_SESSION *session, const WT_ITEM *key, const WT_ITEM *value, WT_CURSOR *result_cursor);
	int (*customize)(WT_EXTRACTOR *extractor, WT_SESSION *session, const char *uri, WT_CONFIG_ITEM *appcfg, WT_EXTRACTOR **customp);
	int (*terminate)(WT_EXTRACTOR *extractor, WT_SESSION *session);
};

extern int wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config);
extern int wiredtiger_extension_terminate(WT_CONNECTION *connection);

/*wt connection的统计项*/
/*! async: number of allocation state races */
#define	WT_STAT_CONN_ASYNC_ALLOC_RACE			1000
/*! async: number of operation slots viewed for allocation */
#define	WT_STAT_CONN_ASYNC_ALLOC_VIEW			1001
/*! async: current work queue length */
#define	WT_STAT_CONN_ASYNC_CUR_QUEUE			1002
/*! async: number of flush calls */
#define	WT_STAT_CONN_ASYNC_FLUSH			1003
/*! async: number of times operation allocation failed */
#define	WT_STAT_CONN_ASYNC_FULL				1004
/*! async: maximum work queue length */
#define	WT_STAT_CONN_ASYNC_MAX_QUEUE			1005
/*! async: number of times worker found no work */
#define	WT_STAT_CONN_ASYNC_NOWORK			1006
/*! async: total allocations */
#define	WT_STAT_CONN_ASYNC_OP_ALLOC			1007
/*! async: total compact calls */
#define	WT_STAT_CONN_ASYNC_OP_COMPACT			1008
/*! async: total insert calls */
#define	WT_STAT_CONN_ASYNC_OP_INSERT			1009
/*! async: total remove calls */
#define	WT_STAT_CONN_ASYNC_OP_REMOVE			1010
/*! async: total search calls */
#define	WT_STAT_CONN_ASYNC_OP_SEARCH			1011
/*! async: total update calls */
#define	WT_STAT_CONN_ASYNC_OP_UPDATE			1012
/*! block-manager: mapped bytes read */
#define	WT_STAT_CONN_BLOCK_BYTE_MAP_READ		1013
/*! block-manager: bytes read */
#define	WT_STAT_CONN_BLOCK_BYTE_READ			1014
/*! block-manager: bytes written */
#define	WT_STAT_CONN_BLOCK_BYTE_WRITE			1015
/*! block-manager: mapped blocks read */
#define	WT_STAT_CONN_BLOCK_MAP_READ			1016
/*! block-manager: blocks pre-loaded */
#define	WT_STAT_CONN_BLOCK_PRELOAD			1017
/*! block-manager: blocks read */
#define	WT_STAT_CONN_BLOCK_READ				1018
/*! block-manager: blocks written */
#define	WT_STAT_CONN_BLOCK_WRITE			1019
/*! cache: tracked dirty bytes in the cache */
#define	WT_STAT_CONN_CACHE_BYTES_DIRTY			1020
/*! cache: tracked bytes belonging to internal pages in the cache */
#define	WT_STAT_CONN_CACHE_BYTES_INTERNAL		1021
/*! cache: bytes currently in the cache */
#define	WT_STAT_CONN_CACHE_BYTES_INUSE			1022
/*! cache: tracked bytes belonging to leaf pages in the cache */
#define	WT_STAT_CONN_CACHE_BYTES_LEAF			1023
/*! cache: maximum bytes configured */
#define	WT_STAT_CONN_CACHE_BYTES_MAX			1024
/*! cache: tracked bytes belonging to overflow pages in the cache */
#define	WT_STAT_CONN_CACHE_BYTES_OVERFLOW		1025
/*! cache: bytes read into cache */
#define	WT_STAT_CONN_CACHE_BYTES_READ			1026
/*! cache: bytes written from cache */
#define	WT_STAT_CONN_CACHE_BYTES_WRITE			1027
/*! cache: pages evicted by application threads */
#define	WT_STAT_CONN_CACHE_EVICTION_APP			1028
/*! cache: checkpoint blocked page eviction */
#define	WT_STAT_CONN_CACHE_EVICTION_CHECKPOINT		1029
/*! cache: unmodified pages evicted */
#define	WT_STAT_CONN_CACHE_EVICTION_CLEAN		1030
/*! cache: page split during eviction deepened the tree */
#define	WT_STAT_CONN_CACHE_EVICTION_DEEPEN		1031
/*! cache: modified pages evicted */
#define	WT_STAT_CONN_CACHE_EVICTION_DIRTY		1032
/*! cache: pages selected for eviction unable to be evicted */
#define	WT_STAT_CONN_CACHE_EVICTION_FAIL		1033
/*! cache: pages evicted because they exceeded the in-memory maximum */
#define	WT_STAT_CONN_CACHE_EVICTION_FORCE		1034
/*! cache: pages evicted because they had chains of deleted items */
#define	WT_STAT_CONN_CACHE_EVICTION_FORCE_DELETE	1035
/*! cache: failed eviction of pages that exceeded the in-memory maximum */
#define	WT_STAT_CONN_CACHE_EVICTION_FORCE_FAIL		1036
/*! cache: hazard pointer blocked page eviction */
#define	WT_STAT_CONN_CACHE_EVICTION_HAZARD		1037
/*! cache: internal pages evicted */
#define	WT_STAT_CONN_CACHE_EVICTION_INTERNAL		1038
/*! cache: maximum page size at eviction */
#define	WT_STAT_CONN_CACHE_EVICTION_MAXIMUM_PAGE_SIZE	1039
/*! cache: eviction server candidate queue empty when topping up */
#define	WT_STAT_CONN_CACHE_EVICTION_QUEUE_EMPTY		1040
/*! cache: eviction server candidate queue not empty when topping up */
#define	WT_STAT_CONN_CACHE_EVICTION_QUEUE_NOT_EMPTY	1041
/*! cache: eviction server evicting pages */
#define	WT_STAT_CONN_CACHE_EVICTION_SERVER_EVICTING	1042
/*! cache: eviction server populating queue, but not evicting pages */
#define	WT_STAT_CONN_CACHE_EVICTION_SERVER_NOT_EVICTING	1043
/*! cache: eviction server unable to reach eviction goal */
#define	WT_STAT_CONN_CACHE_EVICTION_SLOW		1044
/*! cache: pages split during eviction */
#define	WT_STAT_CONN_CACHE_EVICTION_SPLIT		1045
/*! cache: pages walked for eviction */
#define	WT_STAT_CONN_CACHE_EVICTION_WALK		1046
/*! cache: eviction worker thread evicting pages */
#define	WT_STAT_CONN_CACHE_EVICTION_WORKER_EVICTING	1047
/*! cache: in-memory page splits */
#define	WT_STAT_CONN_CACHE_INMEM_SPLIT			1048
/*! cache: percentage overhead */
#define	WT_STAT_CONN_CACHE_OVERHEAD			1049
/*! cache: tracked dirty pages in the cache */
#define	WT_STAT_CONN_CACHE_PAGES_DIRTY			1050
/*! cache: pages currently held in the cache */
#define	WT_STAT_CONN_CACHE_PAGES_INUSE			1051
/*! cache: pages read into cache */
#define	WT_STAT_CONN_CACHE_READ				1052
/*! cache: pages written from cache */
#define	WT_STAT_CONN_CACHE_WRITE			1053
/*! connection: pthread mutex condition wait calls */
#define	WT_STAT_CONN_COND_WAIT				1054
/*! cursor: cursor create calls */
#define	WT_STAT_CONN_CURSOR_CREATE			1055
/*! cursor: cursor insert calls */
#define	WT_STAT_CONN_CURSOR_INSERT			1056
/*! cursor: cursor next calls */
#define	WT_STAT_CONN_CURSOR_NEXT			1057
/*! cursor: cursor prev calls */
#define	WT_STAT_CONN_CURSOR_PREV			1058
/*! cursor: cursor remove calls */
#define	WT_STAT_CONN_CURSOR_REMOVE			1059
/*! cursor: cursor reset calls */
#define	WT_STAT_CONN_CURSOR_RESET			1060
/*! cursor: cursor search calls */
#define	WT_STAT_CONN_CURSOR_SEARCH			1061
/*! cursor: cursor search near calls */
#define	WT_STAT_CONN_CURSOR_SEARCH_NEAR			1062
/*! cursor: cursor update calls */
#define	WT_STAT_CONN_CURSOR_UPDATE			1063
/*! data-handle: connection dhandles swept */
#define	WT_STAT_CONN_DH_CONN_HANDLES			1064
/*! data-handle: connection candidate referenced */
#define	WT_STAT_CONN_DH_CONN_REF			1065
/*! data-handle: connection sweeps */
#define	WT_STAT_CONN_DH_CONN_SWEEPS			1066
/*! data-handle: connection time-of-death sets */
#define	WT_STAT_CONN_DH_CONN_TOD			1067
/*! data-handle: session dhandles swept */
#define	WT_STAT_CONN_DH_SESSION_HANDLES			1068
/*! data-handle: session sweep attempts */
#define	WT_STAT_CONN_DH_SESSION_SWEEPS			1069
/*! connection: files currently open */
#define	WT_STAT_CONN_FILE_OPEN				1070
/*! log: log buffer size increases */
#define	WT_STAT_CONN_LOG_BUFFER_GROW			1071
/*! log: total log buffer size */
#define	WT_STAT_CONN_LOG_BUFFER_SIZE			1072
/*! log: log bytes of payload data */
#define	WT_STAT_CONN_LOG_BYTES_PAYLOAD			1073
/*! log: log bytes written */
#define	WT_STAT_CONN_LOG_BYTES_WRITTEN			1074
/*! log: yields waiting for previous log file close */
#define	WT_STAT_CONN_LOG_CLOSE_YIELDS			1075
/*! log: total size of compressed records */
#define	WT_STAT_CONN_LOG_COMPRESS_LEN			1076
/*! log: total in-memory size of compressed records */
#define	WT_STAT_CONN_LOG_COMPRESS_MEM			1077
/*! log: log records too small to compress */
#define	WT_STAT_CONN_LOG_COMPRESS_SMALL			1078
/*! log: log records not compressed */
#define	WT_STAT_CONN_LOG_COMPRESS_WRITE_FAILS		1079
/*! log: log records compressed */
#define	WT_STAT_CONN_LOG_COMPRESS_WRITES		1080
/*! log: maximum log file size */
#define	WT_STAT_CONN_LOG_MAX_FILESIZE			1081
/*! log: pre-allocated log files prepared */
#define	WT_STAT_CONN_LOG_PREALLOC_FILES			1082
/*! log: number of pre-allocated log files to create */
#define	WT_STAT_CONN_LOG_PREALLOC_MAX			1083
/*! log: pre-allocated log files used */
#define	WT_STAT_CONN_LOG_PREALLOC_USED			1084
/*! log: log read operations */
#define	WT_STAT_CONN_LOG_READS				1085
/*! log: log release advances write LSN */
#define	WT_STAT_CONN_LOG_RELEASE_WRITE_LSN		1086
/*! log: records processed by log scan */
#define	WT_STAT_CONN_LOG_SCAN_RECORDS			1087
/*! log: log scan records requiring two reads */
#define	WT_STAT_CONN_LOG_SCAN_REREADS			1088
/*! log: log scan operations */
#define	WT_STAT_CONN_LOG_SCANS				1089
/*! log: consolidated slot closures */
#define	WT_STAT_CONN_LOG_SLOT_CLOSES			1090
/*! log: logging bytes consolidated */
#define	WT_STAT_CONN_LOG_SLOT_CONSOLIDATED		1091
/*! log: consolidated slot joins */
#define	WT_STAT_CONN_LOG_SLOT_JOINS			1092
/*! log: consolidated slot join races */
#define	WT_STAT_CONN_LOG_SLOT_RACES			1093
/*! log: slots selected for switching that were unavailable */
#define	WT_STAT_CONN_LOG_SLOT_SWITCH_FAILS		1094
/*! log: record size exceeded maximum */
#define	WT_STAT_CONN_LOG_SLOT_TOOBIG			1095
/*! log: failed to find a slot large enough for record */
#define	WT_STAT_CONN_LOG_SLOT_TOOSMALL			1096
/*! log: consolidated slot join transitions */
#define	WT_STAT_CONN_LOG_SLOT_TRANSITIONS		1097
/*! log: log sync operations */
#define	WT_STAT_CONN_LOG_SYNC				1098
/*! log: log sync_dir operations */
#define	WT_STAT_CONN_LOG_SYNC_DIR			1099
/*! log: log server thread advances write LSN */
#define	WT_STAT_CONN_LOG_WRITE_LSN			1100
/*! log: log write operations */
#define	WT_STAT_CONN_LOG_WRITES				1101
/*! LSM: sleep for LSM checkpoint throttle */
#define	WT_STAT_CONN_LSM_CHECKPOINT_THROTTLE		1102
/*! LSM: sleep for LSM merge throttle */
#define	WT_STAT_CONN_LSM_MERGE_THROTTLE			1103
/*! LSM: rows merged in an LSM tree */
#define	WT_STAT_CONN_LSM_ROWS_MERGED			1104
/*! LSM: application work units currently queued */
#define	WT_STAT_CONN_LSM_WORK_QUEUE_APP			1105
/*! LSM: merge work units currently queued */
#define	WT_STAT_CONN_LSM_WORK_QUEUE_MANAGER		1106
/*! LSM: tree queue hit maximum */
#define	WT_STAT_CONN_LSM_WORK_QUEUE_MAX			1107
/*! LSM: switch work units currently queued */
#define	WT_STAT_CONN_LSM_WORK_QUEUE_SWITCH		1108
/*! LSM: tree maintenance operations scheduled */
#define	WT_STAT_CONN_LSM_WORK_UNITS_CREATED		1109
/*! LSM: tree maintenance operations discarded */
#define	WT_STAT_CONN_LSM_WORK_UNITS_DISCARDED		1110
/*! LSM: tree maintenance operations executed */
#define	WT_STAT_CONN_LSM_WORK_UNITS_DONE		1111
/*! connection: memory allocations */
#define	WT_STAT_CONN_MEMORY_ALLOCATION			1112
/*! connection: memory frees */
#define	WT_STAT_CONN_MEMORY_FREE			1113
/*! connection: memory re-allocations */
#define	WT_STAT_CONN_MEMORY_GROW			1114
/*! thread-yield: page acquire busy blocked */
#define	WT_STAT_CONN_PAGE_BUSY_BLOCKED			1115
/*! thread-yield: page acquire eviction blocked */
#define	WT_STAT_CONN_PAGE_FORCIBLE_EVICT_BLOCKED	1116
/*! thread-yield: page acquire locked blocked */
#define	WT_STAT_CONN_PAGE_LOCKED_BLOCKED		1117
/*! thread-yield: page acquire read blocked */
#define	WT_STAT_CONN_PAGE_READ_BLOCKED			1118
/*! thread-yield: page acquire time sleeping (usecs) */
#define	WT_STAT_CONN_PAGE_SLEEP				1119
/*! connection: total read I/Os */
#define	WT_STAT_CONN_READ_IO				1120
/*! reconciliation: page reconciliation calls */
#define	WT_STAT_CONN_REC_PAGES				1121
/*! reconciliation: page reconciliation calls for eviction */
#define	WT_STAT_CONN_REC_PAGES_EVICTION			1122
/*! reconciliation: split bytes currently awaiting free */
#define	WT_STAT_CONN_REC_SPLIT_STASHED_BYTES		1123
/*! reconciliation: split objects currently awaiting free */
#define	WT_STAT_CONN_REC_SPLIT_STASHED_OBJECTS		1124
/*! connection: pthread mutex shared lock read-lock calls */
#define	WT_STAT_CONN_RWLOCK_READ			1125
/*! connection: pthread mutex shared lock write-lock calls */
#define	WT_STAT_CONN_RWLOCK_WRITE			1126
/*! session: open cursor count */
#define	WT_STAT_CONN_SESSION_CURSOR_OPEN		1127
/*! session: open session count */
#define	WT_STAT_CONN_SESSION_OPEN			1128
/*! transaction: transaction begins */
#define	WT_STAT_CONN_TXN_BEGIN				1129
/*! transaction: transaction checkpoints */
#define	WT_STAT_CONN_TXN_CHECKPOINT			1130
/*! transaction: transaction checkpoint generation */
#define	WT_STAT_CONN_TXN_CHECKPOINT_GENERATION		1131
/*! transaction: transaction checkpoint currently running */
#define	WT_STAT_CONN_TXN_CHECKPOINT_RUNNING		1132
/*! transaction: transaction checkpoint max time (msecs) */
#define	WT_STAT_CONN_TXN_CHECKPOINT_TIME_MAX		1133
/*! transaction: transaction checkpoint min time (msecs) */
#define	WT_STAT_CONN_TXN_CHECKPOINT_TIME_MIN		1134
/*! transaction: transaction checkpoint most recent time (msecs) */
#define	WT_STAT_CONN_TXN_CHECKPOINT_TIME_RECENT		1135
/*! transaction: transaction checkpoint total time (msecs) */
#define	WT_STAT_CONN_TXN_CHECKPOINT_TIME_TOTAL		1136
/*! transaction: transactions committed */
#define	WT_STAT_CONN_TXN_COMMIT				1137
/*! transaction: transaction failures due to cache overflow */
#define	WT_STAT_CONN_TXN_FAIL_CACHE			1138
/*! transaction: transaction range of IDs currently pinned by a checkpoint */
#define	WT_STAT_CONN_TXN_PINNED_CHECKPOINT_RANGE	1139
/*! transaction: transaction range of IDs currently pinned */
#define	WT_STAT_CONN_TXN_PINNED_RANGE			1140
/*! transaction: transactions rolled back */
#define	WT_STAT_CONN_TXN_ROLLBACK			1141
/*! connection: total write I/Os */
#define	WT_STAT_CONN_WRITE_IO				1142

/*data sources的统计项*/
/*! block-manager: file allocation unit size */
#define	WT_STAT_DSRC_ALLOCATION_SIZE			2000
/*! block-manager: blocks allocated */
#define	WT_STAT_DSRC_BLOCK_ALLOC			2001
/*! block-manager: checkpoint size */
#define	WT_STAT_DSRC_BLOCK_CHECKPOINT_SIZE		2002
/*! block-manager: allocations requiring file extension */
#define	WT_STAT_DSRC_BLOCK_EXTENSION			2003
/*! block-manager: blocks freed */
#define	WT_STAT_DSRC_BLOCK_FREE				2004
/*! block-manager: file magic number */
#define	WT_STAT_DSRC_BLOCK_MAGIC			2005
/*! block-manager: file major version number */
#define	WT_STAT_DSRC_BLOCK_MAJOR			2006
/*! block-manager: minor version number */
#define	WT_STAT_DSRC_BLOCK_MINOR			2007
/*! block-manager: file bytes available for reuse */
#define	WT_STAT_DSRC_BLOCK_REUSE_BYTES			2008
/*! block-manager: file size in bytes */
#define	WT_STAT_DSRC_BLOCK_SIZE				2009
/*! LSM: bloom filters in the LSM tree */
#define	WT_STAT_DSRC_BLOOM_COUNT			2010
/*! LSM: bloom filter false positives */
#define	WT_STAT_DSRC_BLOOM_FALSE_POSITIVE		2011
/*! LSM: bloom filter hits */
#define	WT_STAT_DSRC_BLOOM_HIT				2012
/*! LSM: bloom filter misses */
#define	WT_STAT_DSRC_BLOOM_MISS				2013
/*! LSM: bloom filter pages evicted from cache */
#define	WT_STAT_DSRC_BLOOM_PAGE_EVICT			2014
/*! LSM: bloom filter pages read into cache */
#define	WT_STAT_DSRC_BLOOM_PAGE_READ			2015
/*! LSM: total size of bloom filters */
#define	WT_STAT_DSRC_BLOOM_SIZE				2016
/*! btree: btree checkpoint generation */
#define	WT_STAT_DSRC_BTREE_CHECKPOINT_GENERATION	2017
/*! btree: column-store variable-size deleted values */
#define	WT_STAT_DSRC_BTREE_COLUMN_DELETED		2018
/*! btree: column-store fixed-size leaf pages */
#define	WT_STAT_DSRC_BTREE_COLUMN_FIX			2019
/*! btree: column-store internal pages */
#define	WT_STAT_DSRC_BTREE_COLUMN_INTERNAL		2020
/*! btree: column-store variable-size leaf pages */
#define	WT_STAT_DSRC_BTREE_COLUMN_VARIABLE		2021
/*! btree: pages rewritten by compaction */
#define	WT_STAT_DSRC_BTREE_COMPACT_REWRITE		2022
/*! btree: number of key/value pairs */
#define	WT_STAT_DSRC_BTREE_ENTRIES			2023
/*! btree: fixed-record size */
#define	WT_STAT_DSRC_BTREE_FIXED_LEN			2024
/*! btree: maximum tree depth */
#define	WT_STAT_DSRC_BTREE_MAXIMUM_DEPTH		2025
/*! btree: maximum internal page key size */
#define	WT_STAT_DSRC_BTREE_MAXINTLKEY			2026
/*! btree: maximum internal page size */
#define	WT_STAT_DSRC_BTREE_MAXINTLPAGE			2027
/*! btree: maximum leaf page key size */
#define	WT_STAT_DSRC_BTREE_MAXLEAFKEY			2028
/*! btree: maximum leaf page size */
#define	WT_STAT_DSRC_BTREE_MAXLEAFPAGE			2029
/*! btree: maximum leaf page value size */
#define	WT_STAT_DSRC_BTREE_MAXLEAFVALUE			2030
/*! btree: overflow pages */
#define	WT_STAT_DSRC_BTREE_OVERFLOW			2031
/*! btree: row-store internal pages */
#define	WT_STAT_DSRC_BTREE_ROW_INTERNAL			2032
/*! btree: row-store leaf pages */
#define	WT_STAT_DSRC_BTREE_ROW_LEAF			2033
/*! cache: bytes read into cache */
#define	WT_STAT_DSRC_CACHE_BYTES_READ			2034
/*! cache: bytes written from cache */
#define	WT_STAT_DSRC_CACHE_BYTES_WRITE			2035
/*! cache: checkpoint blocked page eviction */
#define	WT_STAT_DSRC_CACHE_EVICTION_CHECKPOINT		2036
/*! cache: unmodified pages evicted */
#define	WT_STAT_DSRC_CACHE_EVICTION_CLEAN		2037
/*! cache: page split during eviction deepened the tree */
#define	WT_STAT_DSRC_CACHE_EVICTION_DEEPEN		2038
/*! cache: modified pages evicted */
#define	WT_STAT_DSRC_CACHE_EVICTION_DIRTY		2039
/*! cache: data source pages selected for eviction unable to be evicted */
#define	WT_STAT_DSRC_CACHE_EVICTION_FAIL		2040
/*! cache: hazard pointer blocked page eviction */
#define	WT_STAT_DSRC_CACHE_EVICTION_HAZARD		2041
/*! cache: internal pages evicted */
#define	WT_STAT_DSRC_CACHE_EVICTION_INTERNAL		2042
/*! cache: pages split during eviction */
#define	WT_STAT_DSRC_CACHE_EVICTION_SPLIT		2043
/*! cache: in-memory page splits */
#define	WT_STAT_DSRC_CACHE_INMEM_SPLIT			2044
/*! cache: overflow values cached in memory */
#define	WT_STAT_DSRC_CACHE_OVERFLOW_VALUE		2045
/*! cache: pages read into cache */
#define	WT_STAT_DSRC_CACHE_READ				2046
/*! cache: overflow pages read into cache */
#define	WT_STAT_DSRC_CACHE_READ_OVERFLOW		2047
/*! cache: pages written from cache */
#define	WT_STAT_DSRC_CACHE_WRITE			2048
/*! compression: raw compression call failed, no additional data available */
#define	WT_STAT_DSRC_COMPRESS_RAW_FAIL			2049
/*! compression: raw compression call failed, additional data available */
#define	WT_STAT_DSRC_COMPRESS_RAW_FAIL_TEMPORARY	2050
/*! compression: raw compression call succeeded */
#define	WT_STAT_DSRC_COMPRESS_RAW_OK			2051
/*! compression: compressed pages read */
#define	WT_STAT_DSRC_COMPRESS_READ			2052
/*! compression: compressed pages written */
#define	WT_STAT_DSRC_COMPRESS_WRITE			2053
/*! compression: page written failed to compress */
#define	WT_STAT_DSRC_COMPRESS_WRITE_FAIL		2054
/*! compression: page written was too small to compress */
#define	WT_STAT_DSRC_COMPRESS_WRITE_TOO_SMALL		2055
/*! cursor: create calls */
#define	WT_STAT_DSRC_CURSOR_CREATE			2056
/*! cursor: insert calls */
#define	WT_STAT_DSRC_CURSOR_INSERT			2057
/*! cursor: bulk-loaded cursor-insert calls */
#define	WT_STAT_DSRC_CURSOR_INSERT_BULK			2058
/*! cursor: cursor-insert key and value bytes inserted */
#define	WT_STAT_DSRC_CURSOR_INSERT_BYTES		2059
/*! cursor: next calls */
#define	WT_STAT_DSRC_CURSOR_NEXT			2060
/*! cursor: prev calls */
#define	WT_STAT_DSRC_CURSOR_PREV			2061
/*! cursor: remove calls */
#define	WT_STAT_DSRC_CURSOR_REMOVE			2062
/*! cursor: cursor-remove key bytes removed */
#define	WT_STAT_DSRC_CURSOR_REMOVE_BYTES		2063
/*! cursor: reset calls */
#define	WT_STAT_DSRC_CURSOR_RESET			2064
/*! cursor: search calls */
#define	WT_STAT_DSRC_CURSOR_SEARCH			2065
/*! cursor: search near calls */
#define	WT_STAT_DSRC_CURSOR_SEARCH_NEAR			2066
/*! cursor: update calls */
#define	WT_STAT_DSRC_CURSOR_UPDATE			2067
/*! cursor: cursor-update value bytes updated */
#define	WT_STAT_DSRC_CURSOR_UPDATE_BYTES		2068
/*! LSM: sleep for LSM checkpoint throttle */
#define	WT_STAT_DSRC_LSM_CHECKPOINT_THROTTLE		2069
/*! LSM: chunks in the LSM tree */
#define	WT_STAT_DSRC_LSM_CHUNK_COUNT			2070
/*! LSM: highest merge generation in the LSM tree */
#define	WT_STAT_DSRC_LSM_GENERATION_MAX			2071
/*! LSM: queries that could have benefited from a Bloom filter that did
 * not exist */
#define	WT_STAT_DSRC_LSM_LOOKUP_NO_BLOOM		2072
/*! LSM: sleep for LSM merge throttle */
#define	WT_STAT_DSRC_LSM_MERGE_THROTTLE			2073
/*! reconciliation: dictionary matches */
#define	WT_STAT_DSRC_REC_DICTIONARY			2074
/*! reconciliation: internal page multi-block writes */
#define	WT_STAT_DSRC_REC_MULTIBLOCK_INTERNAL		2075
/*! reconciliation: leaf page multi-block writes */
#define	WT_STAT_DSRC_REC_MULTIBLOCK_LEAF		2076
/*! reconciliation: maximum blocks required for a page */
#define	WT_STAT_DSRC_REC_MULTIBLOCK_MAX			2077
/*! reconciliation: internal-page overflow keys */
#define	WT_STAT_DSRC_REC_OVERFLOW_KEY_INTERNAL		2078
/*! reconciliation: leaf-page overflow keys */
#define	WT_STAT_DSRC_REC_OVERFLOW_KEY_LEAF		2079
/*! reconciliation: overflow values written */
#define	WT_STAT_DSRC_REC_OVERFLOW_VALUE			2080
/*! reconciliation: pages deleted */
#define	WT_STAT_DSRC_REC_PAGE_DELETE			2081
/*! reconciliation: page checksum matches */
#define	WT_STAT_DSRC_REC_PAGE_MATCH			2082
/*! reconciliation: page reconciliation calls */
#define	WT_STAT_DSRC_REC_PAGES				2083
/*! reconciliation: page reconciliation calls for eviction */
#define	WT_STAT_DSRC_REC_PAGES_EVICTION			2084
/*! reconciliation: leaf page key bytes discarded using prefix compression */
#define	WT_STAT_DSRC_REC_PREFIX_COMPRESSION		2085
/*! reconciliation: internal page key bytes discarded using suffix
 * compression */
#define	WT_STAT_DSRC_REC_SUFFIX_COMPRESSION		2086
/*! session: object compaction */
#define	WT_STAT_DSRC_SESSION_COMPACT			2087
/*! session: open cursor count */
#define	WT_STAT_DSRC_SESSION_CURSOR_OPEN		2088
/*! transaction: update conflicts */
#define	WT_STAT_DSRC_TXN_UPDATE_CONFLICT		2089

/*section 统计项*/
/*! invalid operation */
#define	WT_LOGOP_INVALID	0
/*! checkpoint */
#define	WT_LOGREC_CHECKPOINT	0
/*! transaction commit */
#define	WT_LOGREC_COMMIT	1
/*! file sync */
#define	WT_LOGREC_FILE_SYNC	2
/*! message */
#define	WT_LOGREC_MESSAGE	3
/*! column put */
#define	WT_LOGOP_COL_PUT	1
/*! column remove */
#define	WT_LOGOP_COL_REMOVE	2
/*! column truncate */
#define	WT_LOGOP_COL_TRUNCATE	3
/*! row put */
#define	WT_LOGOP_ROW_PUT	4
/*! row remove */
#define	WT_LOGOP_ROW_REMOVE	5
/*! row truncate */
#define	WT_LOGOP_ROW_TRUNCATE	6

#undef __F

#if defined(__cplusplus)
}
#endif

#endif



