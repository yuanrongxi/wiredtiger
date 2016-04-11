
#include "wt_internal.h"

static int __session_checkpoint(WT_SESSION*, const char*);
static int __session_rollback_transaction(WT_SESSION* session, const char*);

/*重置session对应的cursor*/
int __wt_session_reset_cursors(WT_SESSION_IMPL* session)
{
	WT_CURSOR* cursor;
	WT_DECL_RET;

	/*将session管理的cursor进行reset*/
	TAILQ_FOREACH(cursor, &session->cursors, q){
		if (session->ncursors == 0)
			break;
		WT_TRET(cursor->reset(cursor));
	}
	return ret;
}

/*将session中所有的cursor的value值拷贝到value的开始位置*/
int __wt_session_copy_values(WT_SESSION_IMPL* session)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	TAILQ_FOREACH(cursor, &session->cursors, q)
		if (F_ISSET(cursor, WT_CURSTD_VALUE_INT)) {
			F_CLR(cursor, WT_CURSTD_VALUE_INT);
			WT_RET(__wt_buf_set(session, &cursor->value, cursor->value.data, cursor->value.size));
			F_SET(cursor, WT_CURSTD_VALUE_EXT);
		}

	return (ret);
}

/*清空session对应的数据，主要是hazard pointer*/
static void __session_clear(WT_SESSION_IMPL* session)
{
	/*
	* There's no serialization support around the review of the hazard
	* array, which means threads checking for hazard pointers first check
	* the active field (which may be 0) and then use the hazard pointer
	* (which cannot be NULL).
	*
	* Additionally, the session structure can include information that
	* persists past the session's end-of-life, stored as part of page
	* splits.
	*
	* For these reasons, be careful when clearing the session structure.
	*/
	memset(session, 0, WT_SESSION_CLEAR_SIZE(session));
	session->hazard_size = 0;
	session->nhazard = 0;
}

/*关闭session对象*/
static int __session_close(WT_SESSION* wt_session, const char* config)
{
	WT_CONNECTION_IMPL *conn;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	conn = (WT_CONNECTION_IMPL *)wt_session->connection;
	session = (WT_SESSION_IMPL *)wt_session;

	SESSION_API_CALL(session, close, config, cfg);
	WT_UNUSED(cfg);

	/*回滚当前正在执行的事务*/
	if (F_ISSET(&session->txn, TXN_RUNNING))
		WT_TRET(__session_rollback_transaction(wt_session, NULL));

	if (conn->txn_global.states != NULL)
		__wt_txn_release_snapshot(session);

	/*关闭session所有关联的cursor*/
	while ((cursor = TAILQ_FIRST(&session->cursors)) != NULL){
		if (session->event_handler->handle_close != NULL)
			WT_TRET(session->event_handler->handle_close(session->event_handler, wt_session, cursor));
		WT_TRET(cursor->close(cursor));
	}

	WT_ASSERT(session, session->cursors == 0);

	/*关闭session相关的dhandle cache*/
	__wt_session_close_cache(session);

	WT_TRET(__wt_schema_close_tables(session));
	/*释放sesson对应的meta track轨迹数据*/
	__wt_meta_track_discard(session);

	/* Discard scratch buffers, error memory. */
	__wt_scr_discard(session);
	__wt_buf_free(session, &session->err);
	/*销毁session对应的事务对象*/
	__wt_txn_destroy(session);
	/*校验并释放hazard pointer list*/
	__wt_hazard_close(session);

	/* Cleanup */
	if (session->block_manager_cleanup != NULL)
		WT_TRET(session->block_manager_cleanup(session));
	if (session->reconcile_cleanup != NULL)
		WT_TRET(session->reconcile_cleanup(session));

	/* Destroy the thread's mutex. */
	WT_TRET(__wt_cond_destroy(session, &session->cond));

	/* The API lock protects opening and closing of sessions. */
	__wt_spin_lock(session, &conn->api_lock);

	/* Decrement the count of open sessions. */
	WT_STAT_FAST_CONN_DECR(session, session_open);

   /*
	* Sessions are re-used, clear the structure: the clear sets the active
	* field to 0, which will exclude the hazard array from review by the
	* eviction thread.   Because some session fields are accessed by other
	* threads, the structure must be cleared carefully.
	*
	* We don't need to publish here, because regardless of the active field
	* being non-zero, the hazard pointer is always valid.
	*/
	__session_clear(session);
	session = conn->default_session;
   /*
	* Decrement the count of active sessions if that's possible: a session
	* being closed may or may not be at the end of the array, step toward
	* the beginning of the array until we reach an active session.
	*/
	while (conn->sessions[conn->session_cnt - 1].active == 0)
		if (--conn->session_cnt == 0)
			break;

	__wt_spin_unlock(session, &conn->api_lock);

err:
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*重新对session进行配置隔离级别*/
static int __session_reconfigure(WT_SESSION* wt_session, const char* config)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, reconfigure, config, cfg);

	/*如果session的事务正在执行，不能修改事务的隔离级别，抛出一个错误*/
	if (F_ISSET(&session->txn, TXN_RUNNING))
		WT_ERR_MSG(session, EINVAL, "transaction in progress");

	WT_TRET(__wt_session_reset_cursors(session));

	/*确定事务隔离级别*/
	WT_ERR(__wt_config_gets_def(session, cfg, "isolation", 0, &cval));
	if (cval.len != 0)
		session->isolation = session->txn.isolation = WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
	TXN_ISO_SNAPSHOT : (WT_STRING_MATCH("read-uncommitted", cval.str, cval.len) ? TXN_ISO_READ_UNCOMMITTED : TXN_ISO_READ_COMMITTED);

err:	
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*根据uri打开一个对应的cursor对象*/
int __wt_open_cursor(WT_SESSION_IMPL* session, const char* uri, WT_CURSOR* owner, const char* cfg[], WT_CURSOR** cursorp)
{
	WT_COLGROUP *colgroup;
	WT_DATA_SOURCE *dsrc;
	WT_DECL_RET;

	*cursorp = NULL;

	switch (uri[0]){
	case 't': /*table cursor*/
		if (WT_PREFIX_MATCH(uri, "table"))
			WT_RET(__WT_RET(__wt_curtable_open(session, uri, cfg, cursorp)));
		break;

	case 'c': /*colgroup cursor*/
		if (WT_PREFIX_MATCH(uri, "colgroup:")) {
			/*
			* Column groups are a special case: open a cursor on the underlying data source.
			*/
			WT_RET(__wt_schema_get_colgroup(session, uri, 0, NULL, &colgroup));
			WT_RET(__wt_open_cursor(session, colgroup->source, owner, cfg, cursorp));
		}
		else if (WT_PREFIX_MATCH(uri, "config:"))
			WT_RET(__wt_curconfig_open(session, uri, cfg, cursorp));
		break;

	case 'i': /*inde cursor*/
		if (WT_PREFIX_MATCH(uri, "index:"))
			WT_RET(__wt_curindex_open(session, uri, owner, cfg, cursorp));
		break;

	case 'l': /*lsm tree cursor*/
		if (WT_PREFIX_MATCH(uri, "lsm:"))
			WT_RET(__wt_clsm_open(session, uri, owner, cfg, cursorp));
		else if (WT_PREFIX_MATCH(uri, "log:"))
			WT_RET(__wt_curlog_open(session, uri, cfg, cursorp));
		break;

		/*
		* Less common cursor types.
		*/
	case 'f': /*btree file cursor*/
		if (WT_PREFIX_MATCH(uri, "file:"))
			WT_RET(__wt_curfile_open(session, uri, owner, cfg, cursorp));
		break;
	case 'm': /*meta cursor*/
		if (WT_PREFIX_MATCH(uri, WT_METADATA_URI))
			WT_RET(__wt_curmetadata_open(
			session, uri, owner, cfg, cursorp));
		break;
	case 'b': /*backup cursor*/
		if (WT_PREFIX_MATCH(uri, "backup:"))
			WT_RET(__wt_curbackup_open(
			session, uri, cfg, cursorp));
		break;
	case 's':/*stat cursor*/
		if (WT_PREFIX_MATCH(uri, "statistics:"))
			WT_RET(__wt_curstat_open(session, uri, cfg, cursorp));
		break;

	default:
		break;
	}

	/*获得data source并打开cursor*/
	if (*cursorp == NULL && (dsrc = __wt_schema_get_source(session, uri)) != NULL)
		WT_RET(dsrc->open_cursor == NULL ? __wt_object_unsupported(session, uri) : __wt_curds_open(session, uri, owner, cfg, dsrc, cursorp));

	if (*cursorp == NULL)
		return __wt_bad_object_type(session, uri);
	/*
	* When opening simple tables, the table code calls this function on the
	* underlying data source, in which case the application's URI has been copied.
	* uri不匹配，直接关闭打开的cursor，并返回错误
	*/
	if ((*cursorp)->uri == NULL && (ret = __wt_strdup(session, uri, &(*cursorp)->uri)) != 0)
		WT_TRET((*cursorp)->close(*cursorp));

	return ret;
}

/*打开一个cursor的外部调用方法*/
static int __session_open_cursor(WT_SESSION* wt_session, const char* uri, WT_CURSOR* to_dup, const char* config, WT_CURSOR** cursorp)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	cursor = *cursorp = NULL;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, open_cursor, config, cfg);

	if ((to_dup == NULL && uri == NULL) || (to_dup != NULL && uri != NULL))
		WT_ERR_MSG(session, EINVAL, "should be passed either a URI or a cursor to duplicate, but not both");

	if (to_dup != NULL){
		uri = to_dup->uri;
		/*对uri的检验*/
		if (!WT_PREFIX_MATCH(uri, "colgroup:") &&
			!WT_PREFIX_MATCH(uri, "index:") &&
			!WT_PREFIX_MATCH(uri, "file:") &&
			!WT_PREFIX_MATCH(uri, "lsm:") &&
			!WT_PREFIX_MATCH(uri, WT_METADATA_URI) &&
			!WT_PREFIX_MATCH(uri, "table:") &&
			__wt_schema_get_source(session, uri) == NULL)
			WT_ERR(__wt_bad_object_type(session, uri));
	}

	/*根据uri打开cursor*/
	WT_ERR(__wt_open_cursor(session, uri, NULL, cfg, &cursor));
	if (to_dup != NULL)
		WT_ERR(__wt_cursor_dup_position(to_dup, cursor));

	*cursorp = cursor;

	if (0){
err:
		if (cursor != NULL)
			WT_TRET(cursor->close(cursor));
	}

	/*
	* Opening a cursor on a non-existent data source will set ret to
	* either of ENOENT or WT_NOTFOUND at this point.  However,
	* applications may reasonably do this inside a transaction to check
	* for the existence of a table or index.
	*
	* Prefer WT_NOTFOUND here: that does not force running transactions to
	* roll back.  It will be mapped back to ENOENT.
	*/
	if (ret == ENOENT)
		ret = WT_NOTFOUND;

	API_END_RET_NOTFOUND_MAP(session, ret);
}

int __wt_session_create_strip(WT_SESSION* wt_session, const char* v1, const char* v2, char** value_ret)
{
	WT_SESSION_IMPL* session = (WT_SESSION_IMPL*)wt_session;
	const char* cfg[] = { WT_CONFIG_BASE(session, session_create), v1, v2, NULL };
	return __wt_config_collapse(session, cfg, value_ret);
}

/*根据uri和config信息创建一个对应的schema对象*/
static int __session_create(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, create, config, cfg);
	WT_UNUSED(cfg);

	/* Disallow objects in the WiredTiger name space. */
	WT_ERR(__wt_str_name_check(session, uri));

	/*
	* Type configuration only applies to tables, column groups and indexes.
	* We don't want applications to attempt to layer LSM on top of their
	* extended data-sources, and the fact we allow LSM as a valid URI is an
	* invitation to that mistake: nip it in the bud.
	*/
	if (!WT_PREFIX_MATCH(uri, "colgroup:") && !WT_PREFIX_MATCH(uri, "index:") && !WT_PREFIX_MATCH(uri, "table:")){
		/*
		* We can't disallow type entirely, a configuration string might
		* innocently include it, for example, a dump/load pair.  If the
		* URI type prefix and the type are the same, let it go.
		*/
		if ((ret = __wt_config_getones(session, config, "type", &cval)) == 0 && (strncmp(uri, cval.str, cval.len) != 0 || uri[cval.len] != ':'))
			WT_ERR_MSG(session, EINVAL, "%s: unsupported type configuration", uri);
		WT_ERR_NOTFOUND_OK(ret);
	}

	/*创建schema*/
	WT_WITH_SCHEMA_LOCK(session, WT_WITH_TABLE_LOCK(session, ret = __wt_schema_create(session, uri, config)));

err:
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*为外部提供的写日志的接口*/
static int __session_log_printf(WT_SESSION *wt_session, const char *fmt, ...)
{
	WT_SESSION_IMPL* session;
	WT_DECL_RET;
	va_list ap;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, log_printf);

	va_start(ap, fmt);
	ret = __wt_log_vprintf(session, fmt, ap);
	va_end(ap);

err:
	API_END_RET(session, ret);
}

/*修改uri对应对象的schema名字，其实就是更改uri*/
static int __session_rename(WT_SESSION* wt_session, const char* uri, const char* newuri, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, rename, config, cfg);

	WT_ERR(__wt_str_name_check(session, uri));
	WT_ERR(__wt_str_name_check(session, newuri));

	WT_WITH_SCHEMA_LOCK(session, WT_WITH_TABLE_LOCK(session, ret = __wt_schema_rename(session, uri, newuri, cfg)));

err:
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*session的compact接口*/
static int __session_compact(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	/* Disallow objects in the WiredTiger name space. */
	WT_RET(__wt_str_name_check(session, uri));

	if (!WT_PREFIX_MATCH(uri, "colgroup:") && !WT_PREFIX_MATCH(uri, "file:") &&
		!WT_PREFIX_MATCH(uri, "index:") && !WT_PREFIX_MATCH(uri, "lsm:") && !WT_PREFIX_MATCH(uri, "table:"))
		return __wt_bad_object_type(session, uri);

	return __wt_session_compact(wt_session, uri, config);
}

/*session的drop接口，是对uri对应的schema对象进行drop*/
static int __session_drop(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, drop, config, cfg);

	/* Disallow objects in the WiredTiger name space. */
	WT_ERR(__wt_str_name_check(session, uri));

	WT_WITH_SCHEMA_LOCK(session, WT_WITH_TABLE_LOCK(session, ret = __wt_schema_drop(session, uri, cfg)));

err:	
	/* Note: drop operations cannot be unrolled (yet?). */
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*session的salvage接口*/
static int __session_salvage(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	SESSION_API_CALL(session, salvage, config, cfg);

	/* Block out checkpoints to avoid spurious EBUSY errors. */
	__wt_spin_lock(session, &S2C(session)->checkpoint_lock);
	WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_worker(session, uri, __wt_salvage, NULL, cfg, WT_DHANDLE_EXCLUSIVE | WT_BTREE_SALVAGE));
	__wt_spin_unlock(session, &S2C(session)->checkpoint_lock);

err:
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*范围删除接口*/
static int __session_truncate(WT_SESSION *wt_session, const char *uri, WT_CURSOR *start, WT_CURSOR *stop, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_CURSOR *cursor;
	int cmp, local_start;

	local_start = 0;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_TXN_API_CALL(session, truncate, config, cfg);

	/*uri如果没有指定，那么start和stop必须指定其中之一，如果uri指定，start和stop不必指定，否则无法操作*/
	if ((uri == NULL && start == NULL && stop == NULL) || 
		(uri != NULL && !WT_PREFIX_MATCH(uri, "log:") && (start != NULL || stop != NULL)))
		WT_ERR_MSG(session, EINVAL, "the truncate method should be passed either a URI or start/stop cursors, but not both");

	if (uri != NULL) {
		/* Disallow objects in the WiredTiger name space. */
		WT_ERR(__wt_str_name_check(session, uri));

		if (WT_PREFIX_MATCH(uri, "log:")) {
			/*
			* Verify the user only gave the URI prefix and not
			* a specific target name after that.
			*/
			if (!WT_STREQ(uri, "log:"))
				WT_ERR_MSG(session, EINVAL, "the truncate method should not specify any target after the log: URI prefix.");
			ret = __wt_log_truncate_files(session, start, cfg);
		}
		else {
			/* Wait for checkpoints to avoid EBUSY errors. */
			__wt_spin_lock(session, &S2C(session)->checkpoint_lock);
			WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_truncate(session, uri, cfg));
			__wt_spin_unlock(session, &S2C(session)->checkpoint_lock);
		}
		goto done;
	}

	/*
	* Cursor truncate is only supported for some objects, check for the
	* supporting methods we need, range_truncate and compare.
	*/
	cursor = start == NULL ? stop : start;
	if (cursor->compare == NULL)
		WT_ERR(__wt_bad_object_type(session, cursor->uri));

	/*
	* If both cursors set, check they're correctly ordered with respect to
	* each other.  We have to test this before any search, the search can
	* change the initial cursor position.
	*
	* Rather happily, the compare routine will also confirm the cursors
	* reference the same object and the keys are set.
	*/
	if (start != NULL && stop != NULL) {
		WT_ERR(start->compare(start, stop, &cmp));
		if (cmp > 0)
			WT_ERR_MSG(session, EINVAL, "the start cursor position is after the stop cursor position");
	}

	/*
	* Truncate does not require keys actually exist so that applications
	* can discard parts of the object's name space without knowing exactly
	* what records currently appear in the object.  For this reason, do a
	* search-near, rather than a search.  Additionally, we have to correct
	* after calling search-near, to position the start/stop cursors on the
	* next record greater than/less than the original key.
	*/
	if (start != NULL) {
		WT_ERR(start->search_near(start, &cmp));
		if (cmp < 0 && (ret = start->next(start)) != 0) {
			WT_ERR_NOTFOUND_OK(ret);
			goto done;
		}
	}
	if (stop != NULL) {
		WT_ERR(stop->search_near(stop, &cmp));
		if (cmp > 0 && (ret = stop->prev(stop)) != 0) {
			WT_ERR_NOTFOUND_OK(ret);
			goto done;
		}
	}

	/*
	* We always truncate in the forward direction because the underlying
	* data structures can move through pages faster forward than backward.
	* If we don't have a start cursor, create one and position it at the
	* first record.
	*/
	if (start == NULL) {
		WT_ERR(__session_open_cursor(wt_session, stop->uri, NULL, NULL, &start));
		local_start = 1;
		WT_ERR(start->next(start));
	}

	/*
	* If the start/stop keys cross, we're done, the range must be empty.
	*/
	if (stop != NULL) {
		WT_ERR(start->compare(start, stop, &cmp));
		if (cmp > 0)
			goto done;
	}

	WT_ERR(__wt_schema_range_truncate(session, start, stop));

done:
err:
	TXN_API_END_RETRY(session, ret, 0);

   if (local_start)
	   WT_TRET(start->close(start));

   /* Only map WT_NOTFOUND to ENOENT if a URI was specified. */
   return (ret == WT_NOTFOUND && uri != NULL ? ENOENT : ret);
}

/*session的upgrade接口*/
static int __session_upgrade(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	SESSION_API_CALL(session, upgrade, config, cfg);
	/* Block out checkpoints to avoid spurious EBUSY errors. */
	__wt_spin_lock(session, &S2C(session)->checkpoint_lock);
	WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_worker(session, uri, __wt_upgrade, NULL, cfg, WT_DHANDLE_EXCLUSIVE | WT_BTREE_UPGRADE));
	__wt_spin_unlock(session, &S2C(session)->checkpoint_lock);

err:	
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*session的verify接口*/
static int __session_verify(WT_SESSION* wt_session, const char* uri, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	SESSION_API_CALL(session, verify, config, cfg);
	/* Block out checkpoints to avoid spurious EBUSY errors. */
	__wt_spin_lock(session, &S2C(session)->checkpoint_lock);
	WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_worker(session, uri, __wt_verify, NULL, cfg, WT_DHANDLE_EXCLUSIVE | WT_BTREE_VERIFY));
	__wt_spin_unlock(session, &S2C(session)->checkpoint_lock);

err:	
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*session开始一个事务调用*/
static int __session_begin_transaction(WT_SESSION* wt_session, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL* session;
	session = (WT_SESSION_IMPL*)wt_session;
	SESSION_API_CALL(session, begin_transaction, config, cfg);
	WT_STAT_FAST_CONN_INCR(session, txn_begin);

	if (F_ISSET(&session->txn, TXN_RUNNING))
		WT_ERR_MSG(session, EINVAL, "Transaction already running");

	ret = __wt_txn_begin(session, cfg);
err:
	API_END_RET(session, ret);
}

/*session提交一个事务*/
static int __session_commit_transaction(WT_SESSION *wt_session, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_TXN *txn;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, commit_transaction, config, cfg);
	WT_STAT_FAST_CONN_INCR(session, txn_commit);

	txn = &session->txn;
	if (F_ISSET(txn, TXN_ERROR)) {
		__wt_errx(session, "failed transaction requires rollback");
		ret = EINVAL;
	}

	if (ret == 0)
		ret = __wt_txn_commit(session, cfg);
	else {
		WT_TRET(__wt_session_reset_cursors(session));
		WT_TRET(__wt_txn_rollback(session, cfg));
	}

err:	
	API_END_RET(session, ret);
}

/*session回滚一个事务*/
static int __session_rollback_transaction(WT_SESSION* wt_session, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL* session;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, rollback_transaction, config, cfg);
	WT_STAT_FAST_CONN_INCR(session, txn_rollback);

	WT_TRET(__wt_session_reset_cursors(session));

	WT_TRET(__wt_txn_rollback(session, cfg));
err:
	API_END_RET(session, ret);
}

/*计算当前事务ID与最小可见事务ID之间的间隔*/
static int __session_transaction_pinned_range(WT_SESSION* wt_session, uint64_t* prange)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_TXN_STATE *txn_state;
	uint64_t pinned;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, pinned_range);

	txn_state = WT_SESSION_TXN_STATE(session);

	/*确定当前事务能见的最小事务*/
	if (txn_state->id != WT_TXN_NONE && TXNID_LT(txn_state->id, txn_state->snap_min))
		pinned = txn_state->id;
	else
		pinned = txn_state->snap_min;

	/*计算当前事务ID与最小可见事务ID之间的间隔*/
	if (pinned == WT_TXN_NONE)
		*prange = 0;
	else
		*prange = S2C(session)->txn_global.current - pinned;

err:
	API_END_RET(session, ret);
}

/*session的checkpoint的接口调用*/
static int __session_checkpoint(WT_SESSION* wt_session, const char* config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_TXN *txn;

	session = (WT_SESSION_IMPL *)wt_session;

	txn = &session->txn;

	WT_STAT_FAST_CONN_INCR(session, txn_checkpoint);
	SESSION_API_CALL(session, checkpoint, config, cfg);
	/*
	* Checkpoints require a snapshot to write a transactionally consistent
	* snapshot of the data.
	*
	* We can't use an application's transaction: if it has uncommitted
	* changes, they will be written in the checkpoint and may appear after
	* a crash.
	*
	* Use a real snapshot transaction: we don't want any chance of the
	* snapshot being updated during the checkpoint.  Eviction is prevented
	* from evicting anything newer than this because we track the oldest
	* transaction ID in the system that is not visible to all readers.
	*
	* 当前session有正在执行的事务，不能进行checkpoint
	*/
	if (F_ISSET(txn, TXN_RUNNING))
		WT_ERR_MSG(session, EINVAL, "Checkpoint not permitted in a transaction");
	/*
	* Reset open cursors.  Do this explicitly, even though it will happen
	* implicitly in the call to begin_transaction for the checkpoint, the
	* checkpoint code will acquire the schema lock before we do that, and
	* some implementation of WT_CURSOR::reset might need the schema lock.
	*/
	WT_ERR(__wt_session_reset_cursors(session));

	F_SET(session, WT_SESSION_CAN_WAIT | WT_SESSION_NO_CACHE_CHECK);
	/*
	* Only one checkpoint can be active at a time, and checkpoints must run
	* in the same order as they update the metadata.  It's probably a bad
	* idea to run checkpoints out of multiple threads, but serialize them
	* here to ensure we don't get into trouble.
	*/
	WT_STAT_FAST_CONN_SET(session, txn_checkpoint_running, 1);
	__wt_spin_lock(session, &S2C(session)->checkpoint_lock);

	ret = __wt_txn_checkpoint(session, cfg);

	WT_STAT_FAST_CONN_SET(session, txn_checkpoint_running, 0);
	__wt_spin_unlock(session, &S2C(session)->checkpoint_lock);

err:
	F_CLR(session, WT_SESSION_CAN_WAIT | WT_SESSION_NO_CACHE_CHECK);
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*sesson的获取错误信息接口*/
static const char* __session_strerror(WT_SESSION *wt_session, int error)
{
	WT_SESSION_IMPL *session;
	session = (WT_SESSION_IMPL *)wt_session;

	return (__wt_strerror(session, error, NULL, 0));
}

/*创建并打开一个wiretiger内部使用的session对象*/
int __wt_open_internal_session(WT_CONNECTION_IMPL* conn, const char* name, int uses_dhandles, int open_metadata, WT_SESSION_IMPL **sessionp)
{
	WT_SESSION_IMPL *session;

	*sessionp = NULL;

	WT_RET(__wt_open_session(conn, NULL, NULL, &session));
	session->name = name;

	/*
	* Public sessions are automatically closed during WT_CONNECTION->close.
	* If the session handles for internal threads were to go on the public
	* list, there would be complex ordering issues during close.  Set a
	* flag to avoid this: internal sessions are not closed automatically.
	*/
	F_SET(session, WT_SESSION_INTERNAL);

	/*
	* Some internal threads must keep running after we close all data
	* handles.  Make sure these threads don't open their own handles.
	*/
	if (!uses_dhandles)
		F_SET(session, WT_SESSION_NO_DATA_HANDLES);

	/*
	* Acquiring the metadata handle requires the schema lock; we've seen
	* problems in the past where a worker thread has acquired the schema
	* lock unexpectedly, relatively late in the run, and deadlocked. Be
	* defensive, get it now.  The metadata file may not exist when the
	* connection first creates its default session or the shared cache
	* pool creates its sessions, let our caller decline this work.
	*/
	if (open_metadata) {
		WT_ASSERT(session, !F_ISSET(session, WT_SESSION_SCHEMA_LOCKED));
		WT_RET(__wt_metadata_open(session));
	}

	*sessionp = session;
	return 0;
}

/*创建并打开一个外部使用的session对象*/
int __wt_open_session(WT_CONNECTION_IMPL* conn, WT_EVENT_HANDLER* event_handler, const char* config, WT_SESSION_IMPL** sessionp)
{
	static const WT_SESSION stds = {
		NULL,
		NULL,
		__session_close,
		__session_reconfigure,
		__session_strerror,
		__session_open_cursor,
		__session_create,
		__session_compact,
		__session_drop,
		__session_log_printf,
		__session_rename,
		__session_salvage,
		__session_truncate,
		__session_upgrade,
		__session_verify,
		__session_begin_transaction,
		__session_commit_transaction,
		__session_rollback_transaction,
		__session_checkpoint,
		__session_transaction_pinned_range
	};
	WT_DECL_RET;
	WT_SESSION_IMPL *session, *session_ret;
	uint32_t i;

	*sessionp = NULL;

	session = conn->default_session;
	session_ret = NULL;

	__wt_spin_lock(session, &conn->api_lock);
	
	/*
	* Make sure we don't try to open a new session after the application
	* closes the connection.  This is particularly intended to catch
	* cases where server threads open sessions.
	*/
	WT_ASSERT(session, F_ISSET(conn, WT_CONN_SERVER_RUN));

	/* Find the first inactive session slot. 找到可用的session slot*/
	for (session_ret = conn->sessions, i = 0; i < conn->session_size; ++session_ret, ++i)
		if (!session_ret->active)
			break;
	if (i == conn->session_size) /*超出connection的sessions slot范围,提示错误*/
		WT_ERR_MSG(session, ENOMEM, "only configured to support %" PRIu32 " sessions (including %" PRIu32 " internal)", 
					conn->session_size, WT_NUM_INTERNAL_SESSIONS);

	/*
	* If the active session count is increasing, update it.  We don't worry
	* about correcting the session count on error, as long as we don't mark
	* this session as active, we'll clean it up on close.
	*/
	if (i >= conn->session_cnt)	/* Defend against off-by-one errors. */
		conn->session_cnt = i + 1;

	session_ret->id = i;
	session_ret->iface = stds;
	session_ret->iface.connection = &conn->iface;

	/*分配一个session对象*/
	WT_ERR(__wt_cond_alloc(session, "session", 0, &session_ret->cond));
	if (WT_SESSION_FIRST_USE(session_ret))
		__wt_random_init(session_ret->rnd);

	TAILQ_INIT(&session_ret->cursors);
	SLIST_INIT(&session_ret->dhandles);
	/*
	* If we don't have one, allocate the dhandle hash array.
	* Allocate the table hash array as well.
	*/
	if (session_ret->dhhash == NULL)
		WT_ERR(__wt_calloc(session_ret, WT_HASH_ARRAY_SIZE, sizeof(struct __dhandles_hash), &session_ret->dhhash));
	if (session_ret->tablehash == NULL)
		WT_ERR(__wt_calloc(session_ret, WT_HASH_ARRAY_SIZE, sizeof(struct __tables_hash), &session_ret->tablehash));
	for (i = 0; i < WT_HASH_ARRAY_SIZE; i++) {
		SLIST_INIT(&session_ret->dhhash[i]);
		SLIST_INIT(&session_ret->tablehash[i]);
	}

	/* Initialize transaction support: default to read-committed. */
	session_ret->isolation = TXN_ISO_READ_COMMITTED;
	WT_ERR(__wt_txn_init(session_ret));

	if (WT_SESSION_FIRST_USE(session_ret))
		WT_ERR(__wt_calloc_def(session, conn->hazard_max, &session_ret->hazard)); /*如果session是初次使用,需要为其分配一个hazard array*/

	/*
	* Set an initial size for the hazard array. It will be grown as
	* required up to hazard_max. The hazard_size is reset on close, since
	* __wt_hazard_close ensures the array is cleared - so it is safe to
	* reset the starting size on each open.
	*/
	session_ret->hazard_size = WT_HAZARD_INCR;

	if (config != NULL)
		WT_ERR(__session_reconfigure((WT_SESSION*)session_ret, config));

	session_ret->name = NULL;

	/*
	* Publish: make the entry visible to server threads.  There must be a
	* barrier for two reasons, to ensure structure fields are set before
	* any other thread will consider the session, and to push the session
	* count to ensure the eviction thread can't review too few slots.
	*/
	WT_PUBLISH(session_ret->active, 1);

	WT_STATIC_ASSERT(offsetof(WT_SESSION_IMPL, iface) == 0);
	*sessionp = session_ret;

	WT_STAT_FAST_CONN_INCR(session, session_open);

err:
	__wt_spin_unlock(session, &conn->api_lock);
	return ret;
}

