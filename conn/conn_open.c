
#include "wt_internal.h"

/*打开一个connection,主要是创建default session / 创建evict cache 和初始化事务管理器*/
int __wt_connection_open(WT_CONNECTION_IMPL *conn, const char *cfg[])
{
	WT_SESSION_IMPL* session;
	session = conn->default_session;
	WT_ASSERT(session, session->iface.connection == &conn->iface);

	F_SET(conn, WT_CONN_SERVER_RUN | WT_CONN_LOG_SERVER_RUN);

	/* WT_SESSION_IMPL array. */
	WT_RET(__wt_calloc(session, conn->session_size, sizeof(WT_SESSION_IMPL), &conn->sessions));

	/*
	 * Open the default session.  We open this before starting service
	 * threads because those may allocate and use session resources that
	 * need to get cleaned up on close.
	 */
	WT_RET(__wt_open_internal_session(conn, "connection", 1, 0, &session));

	conn->default_session = session;
	/*
	 * Publish: there must be a barrier to ensure the connection structure
	 * fields are set before other threads read from the pointer.
	 */
	WT_WRITE_BARRIER();

	/* Create the cache. */
	WT_RET(__wt_cache_create(session, cfg));

	/* Initialize transaction support. */
	WT_RET(__wt_txn_global_init(session, cfg));

	return 0;
}

/*关闭connection*/
int __wt_connection_close(WT_CONNECTION_IMPL *conn)
{
	WT_CONNECTION *wt_conn;
	WT_DECL_RET;
	WT_DLH *dlh;
	WT_FH *fh;
	WT_SESSION_IMPL *s, *session;
	WT_TXN_GLOBAL *txn_global;
	u_int i;

	wt_conn = &conn->iface;
	txn_global = &conn->txn_global;
	session = conn->default_session;

	/*
	 * We're shutting down.  Make sure everything gets freed.
	 *
	 * It's possible that the eviction server is in the middle of a long
	 * operation, with a transaction ID pinned.  In that case, we will loop
	 * here until the transaction ID is released, when the oldest
	 * transaction ID will catch up with the current ID.
	 * 等待eviction server的退出
	 */
	for (;;) {
		__wt_txn_update_oldest(session);
		if (txn_global->oldest_id == txn_global->current)
			break;
		__wt_yield();
	}

	/*清除异步操作队列*/
	WT_TRET(__wt_async_flush(session));

	/*
	 * Shut down server threads other than the eviction server, which is
	 * needed later to close btree handles.  Some of these threads access
	 * btree handles, so take care in ordering shutdown to make sure they
	 * exit before files are closed.
	 */
	F_CLR(conn, WT_CONN_SERVER_RUN);
	WT_TRET(__wt_async_destroy(session));
	WT_TRET(__wt_lsm_manager_destroy(session));

	F_SET(conn, WT_CONN_CLOSING);

	WT_TRET(__wt_checkpoint_server_destroy(session));
	WT_TRET(__wt_statlog_destroy(session, 1));
	WT_TRET(__wt_sweep_destroy(session));

	/*关闭connection打开的dhandle和btree*/
	WT_TRET(__wt_conn_dhandle_discard(session));

	/*所有的库关闭后，告诉log模块进行log checkpoint并关闭日志模块*/
	if (FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED))
		WT_TRET(__wt_txn_checkpoint_log(session, 1, WT_TXN_LOG_CKPT_STOP, NULL));
	F_CLR(conn, WT_CONN_LOG_SERVER_RUN);
	WT_TRET(__wt_logmgr_destroy(session));

	/* Free memory for collators, compressors, data sources. */
	WT_TRET(__wt_conn_remove_collator(session));
	WT_TRET(__wt_conn_remove_compressor(session));
	WT_TRET(__wt_conn_remove_data_source(session));
	WT_TRET(__wt_conn_remove_extractor(session));

	/*关闭所有打开的文件，提示不能关闭的文件并忽略他，在稍后片刻再关闭它*/
	SLIST_FOREACH(fh, &conn->fhlh, l) {
		if (fh == conn->lock_fh)
			continue;

		__wt_errx(session, "Connection has open file handles: %s", fh->name);
		WT_TRET(__wt_close(session, &fh));
		fh = SLIST_FIRST(&conn->fhlh);
	}

	/* Shut down the eviction server thread. */
	WT_TRET(__wt_evict_destroy(session));

	/* Disconnect from shared cache - must be before cache destroy. */
	WT_TRET(__wt_conn_cache_pool_destroy(session));

	/* Discard the cache. */
	WT_TRET(__wt_cache_destroy(session));

	/* Discard transaction state. */
	__wt_txn_global_destroy(session);

	/* Close extensions, first calling any unload entry point. */
	while ((dlh = TAILQ_FIRST(&conn->dlhqh)) != NULL) {
		TAILQ_REMOVE(&conn->dlhqh, dlh, q);

		if (dlh->terminate != NULL)
			WT_TRET(dlh->terminate(wt_conn));
		WT_TRET(__wt_dlclose(session, dlh));
	}

	/*
	 * Close the internal (default) session, and switch back to the dummy
	 * session in case of any error messages from the remaining operations
	 * while destroying the connection handle.
	 */
	if (session != &conn->dummy_session) {
		WT_TRET(session->iface.close(&session->iface, NULL));
		session = conn->default_session = &conn->dummy_session;
	}

	/*
	 * The session's split stash isn't discarded during normal session close
	 * because it may persist past the life of the session.  Discard it now.
	 */
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i)
			__wt_split_stash_discard_all(session, s);

	/*
	 * The session's hazard pointer memory isn't discarded during normal
	 * session close because access to it isn't serialized.  Discard it
	 * now.
	 */
	if((s = conn->sessions) != NULL){
		for (i = 0; i < conn->session_size; ++s, ++i){
			if (s != session) {
				if (s->dhhash != NULL)
					__wt_free(session, s->dhhash);
				if (s->tablehash != NULL)
					__wt_free(session, s->tablehash);
				__wt_free(session, s->hazard);
			}
		}
	}

	/* Destroy the handle. */
	WT_TRET(__wt_connection_destroy(conn));

	return ret;
}

/*启动conecton和对应的service thread*/
int __wt_connection_workers(WT_SESSION_IMPL *session, const char *cfg[])
{
/*
	 * Start the eviction thread.
	 */
	WT_RET(__wt_evict_create(session));

	/*
	 * Start the handle sweep thread.
	 */
	WT_RET(__wt_sweep_create(session));

	/*
	 * Start the optional statistics thread.  Start statistics first so that
	 * other optional threads can know if statistics are enabled or not.
	 */
	WT_RET(__wt_statlog_create(session, cfg));

	/* Start the optional async threads. */
	WT_RET(__wt_async_create(session, cfg));

	WT_RET(__wt_logmgr_create(session, cfg));

	/* Run recovery. 进行日志推演*/
	WT_RET(__wt_txn_recover(session));

	/*
	 * Start the optional logging/archive thread.
	 * NOTE: The log manager must be started before checkpoints so that the
	 * checkpoint server knows if logging is enabled.
	 */
	WT_RET(__wt_logmgr_open(session));

	/* Start the optional checkpoint thread. */
	WT_RET(__wt_checkpoint_server_create(session, cfg));

	return 0;
}

