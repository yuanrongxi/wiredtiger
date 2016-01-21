/************************************************************
 * data source cursor实现
 ***********************************************************/

#include "wt_internal.h"

/*开始事务操作,会根据隔离级别做snapshot操作*/
static int __curds_txn_enter(WT_SESSION_IMPL* session)
{
	session->ncursors++;
	__wt_txn_cursor_op(session);

	return 0;
}

/*结束事务操作，释放掉执行事务的snapshot*/
static void __curds_txn_leave(WT_SESSION_IMPL* session)
{
	if (--session->ncursors == 0)
		__wt_txn_read_last(session);
}

/*
*	Set the key for the data-source.
*/
static int __curds_key_set(WT_CURSOR* cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	WT_CURSOR_NEEDKEY(cursor);

	source->recno = cursor->recno;
	source->key.data = cursor->key.data;
	source->key.size = cursor->key.size;

err:
	return ret;
}

/*
*	Set the value for the data-source.
*/
static int __curds_value_set(WT_CURSOR* cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	WT_CURSOR_NEEDVALUE(cursor);

	source->value.data = cursor->value.data;
	source->value.size = cursor->value.size;

err:
	return ret;
}

/*
*	Resolve cursor operation.
*/
static int __curds_cursor_resolve(WT_CURSOR* cursor, int ret)
{
	WT_CURSOR *source;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;
	/*
	* Update the cursor's key, value and flags.  (We use the _INT flags in
	* the same way as file objects: there's some chance the underlying data
	* source is passing us a reference to data only pinned per operation,
	* might as well be safe.)
	*
	* There's also a requirement the underlying data-source never returns
	* with the cursor/source key referencing application memory: it'd be
	* great to do a copy as necessary here so the data-source doesn't have
	* to worry about copying the key, but we don't have enough information
	* to know if a cursor is pointing at application or data-source memory.
	*/
	if (ret == 0){
		/*从data source中读取k/v内容设置到cursor的key/value中*/
		cursor->key.data = source->key.data;
		cursor->key.size = source->key.size;
		cursor->value.data = source->value.data;
		cursor->value.size = source->value.size;
		cursor->recno = source->recno;

		F_CLR(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
		F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
	}
	else{
		if (ret == WT_NOTFOUND)
			F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
		else
			F_CLR(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

		/*
		* Cursor operation failure implies a lost cursor position and
		* a subsequent next/prev starting at the beginning/end of the
		* table.  We simplify underlying data source implementations
		* by resetting the cursor explicitly here.
		*/
		WT_TRET(source->reset(source));
	}

	return ret;
}

/*WT_CURSOR大小的比较,主要是KEY比较*/
static int __curds_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
	WT_COLLATOR *collator;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	CURSOR_API_CALL(a, session, compare, NULL);

	if (strcmp(a->internal_uri, b->internal_uri) != 0)
		WT_ERR_MSG(session, EINVAL, "Cursors must reference the same object");

	WT_CURSOR_NEEDKEY(a);
	WT_CURSOR_NEEDKEY(b);

	/*column store */
	if (WT_CURSOR_RECNO(a)) { 
		if (a->recno < b->recno)
			*cmpp = -1;
		else if (a->recno == b->recno)
			*cmpp = 0;
		else
			*cmpp = 1;
	} 
	else{ /*row store, 需要用指定的比较器来进行比较*/
		collator = ((WT_CURSOR_DATA_SOURCE *)a)->collator;
		WT_ERR(__wt_compare(session, collator, &a->key, &b->key, cmpp));
	}

err:
	API_END_RET(session, ret);
}

/*cursor的k/v指向datasource中下一个k/v*/
static int __curds_next(WT_CURSOR *cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, next, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_next);
	WT_STAT_FAST_DATA_INCR(session, cursor_next);

	WT_ERR(__curds_txn_enter(session));

	F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
	ret = __curds_cursor_resolve(cursor, source->next(source));

err: __curds_txn_leave(session);
	API_END_RET(session, ret);
}

/*cursor的k/v指向datasource中上一个k/v*/
static int __curds_prev(WT_CURSOR *cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, prev, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_prev);
	WT_STAT_FAST_DATA_INCR(session, cursor_prev);

	WT_ERR(__curds_txn_enter(session));

	F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
	ret = __curds_cursor_resolve(cursor, source->prev(source));

err:	__curds_txn_leave(session);
	API_END_RET(session, ret);
}

/*WT_CURSOR.reset method for the data-source cursor type. */
static int __curds_reset(WT_CURSOR* cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, reset, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_reset);
	WT_STAT_FAST_DATA_INCR(session, cursor_reset);

	WT_ERR(source->reset(source));

	F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*data source 的检索函数*/
static int __curds_search(WT_CURSOR* cursor)
{
	WT_CURSOR* source;
	WT_DECL_RET;
	WT_SESSION_IMPL* session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, search, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_search);
	WT_STAT_FAST_DATA_INCR(session, cursor_search);

	WT_ERR(__curds_tnx_enter(session));

	WT_ERR(__curds_key_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->search(source));

err: __curds_txn_leave(session);
	API_END_RET(session, ret);
}

/*data source近似检索, 定位到key对应位置的附近*/
static int __curds_search_near(WT_CURSOR* cursor, int* exact)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, search_near, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_search_near);
	WT_STAT_FAST_DATA_INCR(session, cursor_search_near);

	WT_ERR(__curds_txn_enter(session));

	WT_ERR(__curds_key_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->search_near(source, exact));

err: __curds_txn_leave(session);
	API_END_RET(session, ret);
}

/*data source的insert函数*/
static int __curds_insert(WT_CURSOR* cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_UPDATE_API_CALL(cursor, session, insert, NULL);

	WT_ERR(__curds_txn_enter(session));

	WT_STAT_FAST_CONN_INCR(session, cursor_insert);
	WT_STAT_FAST_DATA_INCR(session, cursor_insert);
	WT_STAT_FAST_DATA_INCRV(session, cursor_insert_bytes, cursor->key.size + cursor->value.size);

	if (!F_ISSET(cursor, WT_CURSTD_APPEND))
		WT_ERR(__curds_key_set(cursor));
	WT_ERR(__curds_value_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->insert(source));

err: __curds_txn_leave(session);
	CURSOR_UPDATE_API_END(session, ret);

	return ret;
}

/* data source的update更新操作 */
static int __curds_update(WT_CURSOR* cursor)
{
	WT_CURSOR *source;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_UPDATE_API_CALL(cursor, session, update, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_update);
	WT_STAT_FAST_DATA_INCR(session, cursor_update);
	WT_STAT_FAST_DATA_INCRV(session, cursor_update_bytes, cursor->value.size);

	WT_ERR(__curds_txn_enter(session));

	WT_ERR(__curds_key_set(cursor));
	WT_ERR(__curds_value_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->update(source));

err:
	__curds_txn_leave(session);
	CURSOR_UPDATE_API_END(session, ret);

	return ret;
}

/*data source的remove操作*/
static int __curds_remove(WT_CURSOR* cursor)
{
	WT_CURSOR* source;
	WT_DECL_RET;
	WT_SESSION_IMPL* session;

	source = ((WT_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_UPDATE_API_CALL(cursor, session, remove, NULL);

	WT_STAT_FAST_CONN_INCR(session, cursor_remove);
	WT_STAT_FAST_DATA_INCR(session, cursor_remove);
	WT_STAT_FAST_DATA_INCRV(session, cursor_remove_bytes, cursor->key.size);

	WT_ERR(__curds_txn_enter(session));

	WT_ERR(__curds_key_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->remove(source));
err:
	__curds_txn_leave(cursor);
	CURSOR_UPDATE_API_END(session, ret);

	return ret;
}

/* 关闭data source CURSOR */
static int __curds_close(WT_CURSOR* cursor)
{
	WT_CURSOR_DATA_SOURCE *cds;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	cds = (WT_CURSOR_DATA_SOURCE *)cursor;

	CURSOR_API_CALL(cursor, session, close, NULL);

	if (cds->source != NULL)
		ret = cds->source->close(cds->source);

	if (cds->collator_owned){
		if (cds->collator->terminate != NULL)
			WT_TRET(cds->collator->terminate(cds->collator, &session->iface));
		cds->collator_owned = 0;
	}
	cds->collator = NULL;

	__wt_free(session, cursor->key_format);
	__wt_free(session, cursor->value_format);

	WT_TRET(__wt_cursor_close(cursor));

err:
	API_END_RET(session, ret);
}

/*初始化data source cursor对象*/
int __wt_curds_open(WT_SESSION_IMPL* session, const char* uri, WT_CURSOR* owner, const char* cfg[], WT_DATA_SOURCE* dsrc, WT_CURSOR** cursorp)
{
	WT_CURSOR_STATIC_INIT(iface,
		__wt_cursor_get_key,	/* get-key */
		__wt_cursor_get_value,	/* get-value */
		__wt_cursor_set_key,	/* set-key */
		__wt_cursor_set_value,	/* set-value */
		__curds_compare,		/* compare */
		__wt_cursor_equals,		/* equals */
		__curds_next,		/* next */
		__curds_prev,		/* prev */
		__curds_reset,		/* reset */
		__curds_search,		/* search */
		__curds_search_near,	/* search-near */
		__curds_insert,		/* insert */
		__curds_update,		/* update */
		__curds_remove,		/* remove */
		__wt_cursor_notsup,		/* reconfigure */
		__curds_close);		/* close */

	WT_CONFIG_ITEM cval, metadata;
	WT_CURSOR *cursor, *source;
	WT_CURSOR_DATA_SOURCE *data_source;
	WT_DECL_RET;
	char *metaconf;

	WT_STATIC_ASSERT(offsetof(WT_CURSOR_DATA_SOURCE, iface) == 0);

	data_source = NULL;
	metaconf = NULL;

	WT_RET(__wt_calloc_one(session, &data_source)); /*todo:data source在什么地方释放？？*/
	cursor = &data_source->iface;
	*cursor = iface;
	cursor->session = &session->iface;

	/*
	* XXX
	* The underlying data-source may require the object's key and value
	* formats.  This isn't a particularly elegant way of getting that
	* information to the data-source, this feels like a layering problem
	* to me.
	*/
	WT_ERR(__wt_metadata_search(session, uri, &metaconf));
	WT_ERR(__wt_config_getones(session, metaconf, "key_format", &cval));
	WT_ERR(__wt_strndup(session, cval.str, cval.len, &cursor->key_format));
	WT_ERR(__wt_config_getones(session, metaconf, "value_format", &cval));
	WT_ERR(__wt_strndup(session, cval.str, cval.len, &cursor->value_format));

	WT_ERR(__wt_cursor_init(cursor, uri, owner, cfg, cursorp));

	WT_ERR(__wt_config_getones(session, metaconf, "app_metadata", &metadata));
	/*获得比较器函数实现*/
	WT_ERR(__wt_config_get_none(session, cfg, "collator", &cval));
	if (cval.len != 0)
		WT_ERR(__wt_collator_config(session, uri, &cval, &metadata, &data_source->collator, &data_source->collator_owned));

	WT_ERR(dsrc->open_cursor(dsrc, &session->iface, uri, (WT_CONFIG_ARG *)cfg, &data_source->source));

	source = data_source->source;
	source->session = (WT_SESSION *)session;
	memset(&source->q, 0, sizeof(source->q));
	source->recno = 0;
	memset(source->raw_recno_buf, 0, sizeof(source->raw_recno_buf));
	memset(&source->key, 0, sizeof(source->key));
	memset(&source->value, 0, sizeof(source->value));
	source->saved_err = 0;
	source->flags = 0;

	if (0){
err:
		if (F_ISSET(cursor, WT_CURSTD_OPEN))
			WT_TRET(cursor->close(cursor));
		else
			__wt_free(session, data_source);
		*cursorp = NULL;
	}

	__wt_free(session, metaconf);
	return ret;
}



