
#include "wt_internal.h"


/*判断指定的checkpoint name是否合法*/
int __wt_checkpoint_name_ok(WT_SESSION_IMPL *session, const char *name, size_t len)
{
	/* Check for characters we don't want to see in a metadata file. */
	WT_RET(__wt_name_check(session, name, len));

	/*前缀一定是WiredTigerCheckpoint*/
	if(len < strlen(WT_CHECKPOINT))
		return 0;

	if(!WT_PREFIX_MATCH(name, WT_CHECKPOINT))
		return 0;

	WT_RET_MSG(session, EINVAL, "the checkpoint name \"%s\" is reserved", WT_CHECKPOINT);
}

/*检查uri是否能对checkpoint命名？？*/
static int __checkpoint_name_check(WT_SESSION_IMPL* session, const char* uri)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	const char *fail;

	cursor = NULL;
	fail = NULL;

	/*
	 * This function exists as a place for this comment: named checkpoints
	 * are only supported on file objects, and not on LSM trees or Helium
	 * devices.  If a target list is configured for the checkpoint, this
	 * function is called with each target list entry; check the entry to
	 * make sure it's backed by a file.  If no target list is configured,
	 * confirm the metadata file contains no non-file objects.
	 */

	if (uri == NULL) {
		WT_ERR(__wt_metadata_cursor(session, NULL, &cursor));
		while ((ret = cursor->next(cursor)) == 0) {
			WT_ERR(cursor->get_key(cursor, &uri));
			if (!WT_PREFIX_MATCH(uri, "colgroup:") && !WT_PREFIX_MATCH(uri, "file:") &&
				!WT_PREFIX_MATCH(uri, "index:") && !WT_PREFIX_MATCH(uri, "table:")) {
					fail = uri;
					break;
			}
		}
		WT_ERR_NOTFOUND_OK(ret);
	} 
	else
		if (!WT_PREFIX_MATCH(uri, "colgroup:") && !WT_PREFIX_MATCH(uri, "file:") &&
			!WT_PREFIX_MATCH(uri, "index:") && !WT_PREFIX_MATCH(uri, "table:"))
			fail = uri;

	if (fail != NULL)
		WT_ERR_MSG(session, EINVAL, "%s object does not support named checkpoints", fail);

err:
	if(cursor != NULL)
		WT_TRET(cursor->close(cursor));

	return ret;
}

/*
 * __checkpoint_apply_all --
 *	Apply an operation to all files involved in a checkpoint.
 */
static int __checkpoint_apply_all(WT_SESSION_IMPL* session, const char* cfg[], int (*op)(WT_SESSION_IMPL *, const char *[]), int *fullp)
{
	WT_CONFIG targetconf;
	WT_CONFIG_ITEM cval, k, v;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	int ckpt_closed, named, target_list;

	target_list = 0;

	/*从配置项里面取出checkpoint的命名，并检查名字的是否合适*/
	WT_RET(__wt_config_gets(session, cfg, "name", &cval));
	named = (cval.len != 0);
	if(named)
		WT_RET(__wt_checkpoint_name_ok(session, cval.str, cval.len));

	/* Step through the targets and optionally operate on each one. */
	WT_ERR(__wt_config_gets(session, cfg, "target", &cval));
	WT_ERR(__wt_config_subinit(session, &targetconf, &cval));
	while((ret = __wt_config_next(&targetconf, &k, &v)) == 0){
		if(!target_list){
			WT_ERR(__wt_scr_alloc(session, 512, &tmp));
			target_list = 1;
		}

		if(v.len != 0)
			WT_ERR_MSG(session, EINVAL, "invalid checkpoint target %.*s: URIs may require quoting", (int)cval.len, (char *)cval.str);

		/* Some objects don't support named checkpoints. */
		if (named)
			WT_ERR(__checkpoint_name_check(session, k.str));

		if (op == NULL)
			continue;

		/*这是什么个意思？__wt_schema_worker！！！*/
		WT_ERR(__wt_buf_fmt(session, tmp, "%.*s", (int)k.len, k.str));
		if ((ret = __wt_schema_worker(session, tmp->data, op, NULL, cfg, 0)) != 0)
			WT_ERR_MSG(session, ret, "%s", (const char *)tmp->data);
	}

	WT_ERR_NOTFOUND_OK(ret);
	if(!target_list && named)
		WT_ERR(__checkpoint_name_check(session, NULL));

	if(!target_list && op == NULL){
		ckpt_closed = named;
		if (!ckpt_closed) {
			WT_ERR(__wt_config_gets(session, cfg, "drop", &cval));
			ckpt_closed = cval.len != 0;
		}
		WT_ERR(ckpt_closed ? __wt_meta_btree_apply(session, op, cfg) : __wt_conn_btree_apply(session, 0, NULL, op, cfg));
	}

	if(fullp != NULL)
		*fullp = !target_list;

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*
 * __checkpoint_apply --
 *	Apply an operation to all handles locked for a checkpoint.
 */
static int __checkpoint_apply(WT_SESSION_IMPL* session, const char* cfg[], int (*op)(WT_SESSION_IMPL *, const char *[]))
{
	WT_DECL_RET;
	u_int i;

	/* If we have already locked the handles, apply the operation. */
	for(i = 0; i < session->ckpt_handle_next; ++i){
		if (session->ckpt_handle[i].dhandle != NULL)
			WT_WITH_DHANDLE(session, session->ckpt_handle[i].dhandle, ret = (*op)(session, cfg));
		else
			WT_WITH_DHANDLE_LOCK(session, ret = __wt_conn_btree_apply_single(session, session->ckpt_handle[i].name, NULL, op, cfg));
		WT_RET(ret);
	}

	return 0;
}

/*chectpoint all data sources*/
static int __checkpoint_data_source(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_NAMED_DATA_SOURCE *ndsrc;
	WT_DATA_SOURCE *dsrc;

	/*为所有的datasource建立一个checkpoint*/
	TAILQ_FOREACH(ndsrc, &S2C(session)->dsrcqh, q) {
		dsrc = ndsrc->dsrc;
		if (dsrc->checkpoint != NULL)
			WT_RET(dsrc->checkpoint(dsrc, (WT_SESSION *)session, (WT_CONFIG_ARG *)cfg));
	}

	return 0;
}

/*
 * __wt_checkpoint_list --
 *	Get a list of handles to flush.
 */
int __wt_checkpoint_list(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_DECL_RET;
	const char* name;

	WT_UNUSED(cfg);

	WT_ASSERT(session, session->dhandle->checkpoint == NULL);
	WT_ASSERT(session, WT_PREFIX_MATCH(session->dhandle->name, "file:"));

	/* Make sure there is space for the next entry. */
	WT_RET(__wt_realloc_def(session, &session->ckpt_handle_allocated, session->ckpt_handle_next + 1, &session->ckpt_handle));
	/* Not strictly necessary, but cleaner to clear the current handle. */
	name = session->dhandle->name;
	session->dhandle = NULL;

	/* Record busy file names, we'll deal with them in the checkpoint. */
	ret = __wt_session_get_btree(session, name, NULL, NULL, 0);
	if(ret == 0)
		session->ckpt_handle[session->ckpt_handle_next++].dhandle = session->dhandle;
	else if(ret == EBUSY)
		ret = __wt_strdup(session, name, &session->ckpt_handle[session->ckpt_handle_next++].name);

	return ret;
}

/*在建立checkpoint时，将所有脏页刷入磁盘*/
static int __checkpoint_write_leaves(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_UNUSED(cfg);
	return __wt_cache_op(session, NULL, WT_SYNC_WRITE_LEAVES);
}

/*更新checkpoint的时间统计*/
static void __checkpoint_stats(WT_SESSION_IMPL* session, struct timespec* start, struct timespec* stop)
{
	uint64_t msec;

	msec = WT_TIMEDIFF(*stop, *start) / WT_MILLION;
	if (msec > WT_CONN_STAT(session, txn_checkpoint_time_max))
		WT_STAT_FAST_CONN_SET(session, txn_checkpoint_time_max, msec);
	if (WT_CONN_STAT(session, txn_checkpoint_time_min) == 0 || msec < WT_CONN_STAT(session, txn_checkpoint_time_min))
		WT_STAT_FAST_CONN_SET(session, txn_checkpoint_time_min, msec);

	WT_STAT_FAST_CONN_SET(session, txn_checkpoint_time_recent, msec);
	WT_STAT_FAST_CONN_INCRV(session, txn_checkpoint_time_total, msec);
}

/*输出verbose checkpoint时间信息*/
static int __checkpoint_verbose_track(WT_SESSION_IMPL *session, const char *msg, struct timespec *start)
{
	WT_UNUSED(session);
	WT_UNUSED(msg);
	WT_UNUSED(start);
}

/*完成建立checkpoint的事务*/
int  __wt_txn_checkpoint(WT_SESSION_IMPL* session, const char* cfg[])
{
	struct timespec start, stop, verb_timer;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_ISOLATION saved_isolation;
	const char *txn_cfg[] =
	{ WT_CONFIG_BASE(session, session_begin_transaction),
	"isolation=snapshot", NULL };
	void *saved_meta_next;
	int full, logging, tracking;
	u_int i;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	saved_isolation = session->isolation;
	txn = &session->txn;
	full = logging = tracking = 0;

	/* Ensure the metadata table is open before taking any locks. */
	WT_RET(__wt_metadata_open(session));

	/*
	 * Do a pass over the configuration arguments and figure out what kind
	 * kind of checkpoint this is.
	 */
	WT_RET(__checkpoint_apply_all(session, cfg, NULL, &full));

	/*获得所有需要flush操作的session handles*/
	WT_WITH_SCHEMA_LOCK(session,
		WT_WITH_TABLE_LOCK(session,
		WT_WITH_DHANDLE_LOCK(session,
		ret = __checkpoint_apply_all(session, cfg, __wt_checkpoint_list, NULL))));

	WT_ERR(ret);

	/*
	 * Update the global oldest ID so we do all possible cleanup.
	 *
	 * This is particularly important for compact, so that all dirty pages
	 * can be fully written.
	 */
	__wt_txn_update_oldest(session);

	WT_ERR(__checkpoint_data_source(session, cfg));
	WT_ERR(__wt_epoch(session, &verb_timer));
	WT_ERR(__checkpoint_verbose_track(session, "starting write leaves", &verb_timer));
	/*进行脏页落盘*/
	session->isolation = txn->isolation = TXN_ISO_READ_COMMITTED;
	WT_ERR(__checkpoint_apply(session, cfg, __checkpoint_write_leaves));

	/*
	 * The underlying flush routine scheduled an asynchronous flush
	 * after writing the leaf pages, but in order to minimize I/O
	 * while holding the schema lock, do a flush and wait for the
	 * completion. Do it after flushing the pages to give the
	 * asynchronous flush as much time as possible before we wait.
	 */
	if (F_ISSET(conn, WT_CONN_CKPT_SYNC))
		WT_ERR(__checkpoint_apply(session, cfg, __wt_checkpoint_sync));

	/* Acquire the schema lock. 一个长时间的spin lock*/
	F_SET(session, WT_SESSION_SCHEMA_LOCKED);
	__wt_spin_lock(session, &conn->schema_lock);

	WT_ERR(__wt_meta_track_on(session));
	tracking = 1;

	/* Tell logging that we are about to start a database checkpoint. */
	if (FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED) && full)
		WT_ERR(__wt_txn_checkpoint_log(session, full, WT_TXN_LOG_CKPT_PREPARE, NULL)); /*预checkpoint*/

	/*启动事务*/
	WT_ERR(__checkpoint_verbose_track(session, "starting transaction", &verb_timer));

	/*
	 * Start a snapshot transaction for the checkpoint.
	 *
	 * Note: we don't go through the public API calls because they have
	 * side effects on cursors, which applications can hold open across
	 * calls to checkpoint.
	 */
	if (full)
		WT_ERR(__wt_epoch(session, &start));
	WT_ERR(__wt_txn_begin(session, txn_cfg));

	/* Ensure a transaction ID is allocated prior to sharing it globally */
	WT_ERR(__wt_txn_id_check(session));

	txn_global->checkpoint_id = session->txn.id;
	txn_global->checkpoint_snap_min = session->txn.snap_min;

	txn_global->checkpoint_gen += 1;
	WT_STAT_FAST_CONN_SET(session, txn_checkpoint_generation, txn_global->checkpoint_gen);

	/* Tell logging that we have started a database checkpoint. */
	if (FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED) && full) {
		WT_ERR(__wt_txn_checkpoint_log(session, full, WT_TXN_LOG_CKPT_START, NULL)); /*开始checkpoint*/
		logging = 1;
	}

	WT_ERR(__checkpoint_apply(session, cfg, __wt_checkpoint));
	session->dhandle = NULL;

	/* Commit the transaction before syncing the file(s). */
	WT_ERR(__wt_txn_commit(session, NULL));

	WT_ERR(__checkpoint_verbose_track(session, "committing transaction", &verb_timer));

	/*
	 * Checkpoints have to hit disk (it would be reasonable to configure for
	 * lazy checkpoints, but we don't support them yet).
	 */
	if (F_ISSET(conn, WT_CONN_CKPT_SYNC))
		WT_ERR(__checkpoint_apply(session, cfg, __wt_checkpoint_sync));

	WT_ERR(__checkpoint_verbose_track(session, "sync completed", &verb_timer));

	/*sync meta file*/
	session->isolation = txn->isolation = TXN_ISO_READ_UNCOMMITTED;
	saved_meta_next = session->meta_track_next;
	session->meta_track_next = NULL;
	WT_WITH_DHANDLE(session, session->meta_dhandle, ret = __wt_checkpoint(session, cfg));
	session->meta_track_next = saved_meta_next;
	WT_ERR(ret);
	if (F_ISSET(conn, WT_CONN_CKPT_SYNC)) {
		WT_WITH_DHANDLE(session, session->meta_dhandle, ret = __wt_checkpoint_sync(session, NULL));
		WT_ERR(ret);
	}

	WT_ERR(__checkpoint_verbose_track(session, "metadata sync completed", &verb_timer));

	/*进行checkpoint的时间统计*/
	if (full) {
		WT_ERR(__wt_epoch(session, &stop));
		__checkpoint_stats(session, &start, &stop);
	}

err:	/*
	 * XXX
	 * Rolling back the changes here is problematic.
	 *
	 * If we unroll here, we need a way to roll back changes to the avail
	 * list for each tree that was successfully synced before the error
	 * occurred.  Otherwise, the next time we try this operation, we will
	 * try to free an old checkpoint again.
	 *
	 * OTOH, if we commit the changes after a failure, we have partially
	 * overwritten the checkpoint, so what ends up on disk is not
	 * consistent.
	 */
	session->isolation = txn->isolation = TXN_ISO_READ_UNCOMMITTED;
	if (tracking)
		WT_TRET(__wt_meta_track_off(session, 0, ret != 0));

	if (F_ISSET(txn, TXN_RUNNING)) {
		/*
		 * Clear the dhandle so the visibility check doesn't get
		 * confused about the snap min. Don't bother restoring the
		 * handle since it doesn't make sense to carry a handle across
		 * a checkpoint.
		 */
		session->dhandle = NULL;
		WT_TRET(__wt_txn_rollback(session, NULL));
	}

	/* Ensure the checkpoint IDs are cleared on the error path. */
	txn_global->checkpoint_id = WT_TXN_NONE;
	txn_global->checkpoint_snap_min = WT_TXN_NONE;

	/* Tell logging that we have finished a database checkpoint. */
	if (logging)
		WT_TRET(__wt_txn_checkpoint_log(session, full, (ret == 0) ? WT_TXN_LOG_CKPT_STOP : WT_TXN_LOG_CKPT_FAIL, NULL));

	/*释放掉所有的checkpoint handles*/
	for (i = 0; i < session->ckpt_handle_next; ++i) {
		if (session->ckpt_handle[i].dhandle == NULL) {
			__wt_free(session, session->ckpt_handle[i].name);
			continue;
		}
		WT_WITH_DHANDLE(session, session->ckpt_handle[i].dhandle, WT_TRET(__wt_session_release_btree(session)));
	}
	__wt_free(session, session->ckpt_handle);
	session->ckpt_handle_allocated = session->ckpt_handle_next = 0;

	/*释放schma spin lock*/
	if (F_ISSET(session, WT_SESSION_SCHEMA_LOCKED)) {
		F_CLR(session, WT_SESSION_SCHEMA_LOCKED);
		__wt_spin_unlock(session, &conn->schema_lock);
	}

	/*设回checkpoint前的隔离级别*/
	session->isolation = txn->isolation = saved_isolation;

	return (ret);
}

/*将所有与name匹配的checkpoint删除*/
static void __drop_from(WT_CKPT* ckptbase, const char* name, size_t len)
{
	WT_CKPT* ckpt;
	int matched;

	/* name 与"all"匹配，删除所有checkpoint，这里是标记删除*/
	if(WT_STRING_MATCH("all", name, len)){
		WT_CKPT_FOREACH(ckptbase, ckpt)
			F_SET(ckpt, WT_CKPT_DELETE);

		return;
	}

	/*名字匹配删除*/
	matched = 0;
	WT_CKPT_FOREACH(ckptbase, ckpt) {
		if (!matched && !WT_STRING_MATCH(ckpt->name, name, len))
			continue;

		matched = 1;
		F_SET(ckpt, WT_CKPT_DELETE);
	}
}

/*删除除name名字以外的所有checkpoints*/
static void __drop_to(WT_CKPT* ckptbase, const char* name, size_t len)
{
	WT_CKPT *ckpt, *mark;
	
	mark = NULL;
	WT_CKPT_FOREACH(ckptbase, ckpt){
		if (WT_STRING_MATCH(ckpt->name, name, len))
			mark = ckpt;
	}

	if(mark = NULL)
		return ;

	WT_CKPT_FOREACH(ckptbase, ckpt) {
		F_SET(ckpt, WT_CKPT_DELETE);

		if (ckpt == mark)
			break;
	}
}

