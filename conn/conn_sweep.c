
#include "wt_internal.h"

/*将connect list中的处于关闭状态的dhandle删除*/
static int __sweep_remove_handles(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle, *dhandle_next;
	WT_DECL_RET;

	conn = S2C(session);
	dhandle = SLIST_FIRST(&conn->dhlh);

	for (; dhandle != NULL; dhandle = dhandle_next){
		dhandle_next = SLIST_NEXT(dhandle, l);
		/*元数据的dhandle不做删除*/
		if (WT_IS_METADATA(dhandle))
			continue;
		/*打开状态的dhandle不做删除*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
			continue;
		/* Make sure we get exclusive access. */
		if ((ret = __wt_try_writelock(session, dhandle->rwlock)) == EBUSY)
			continue;
		WT_RET(ret);

		/*正在被引用的dhandle不做删除，有可能其他的地方正在销毁它*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) || dhandle->session_inuse != 0 || dhandle->session_ref != 0){
			WT_RET(__wt_writeunlock(session, dhandle->rwlock));
			continue;
		}

		/*销毁dhandle,并从connection list中删除*/
		WT_WITH_DHANDLE(session, dhandle, ret = __wt_conn_dhandle_discard_single(session, 0));
		/* If the handle was not successfully discarded, unlock it. */
		if (ret != 0)
			WT_TRET(__wt_writeunlock(session, dhandle->rwlock));
		WT_RET_BUSY_OK(ret);
		WT_STAT_FAST_CONN_INCR(session, dh_conn_ref);
	}

	return (ret == EBUSY ? 0 : ret);
}

/*从session对应的connection中关闭不在使用的dhandle*/
static int __sweep(WT_SESSION_IMPL* session)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	time_t now;
	int closed_handles;

	conn = S2C(session);
	closed_handles = 0;

	WT_RET(__wt_seconds(session, &now));

	WT_STAT_FAST_CONN_INCR(session, dh_conn_sweeps);
	LIST_FOREACH(dhandle, &conn->dhlh, l){
		/*不关闭元数据的dhandle*/
		if (WT_IS_METADATA(dhandle))
			continue;
		
		/*dhandle已经关闭了*/
		if (!F_ISSET(dhandle, WT_DHANDLE_OPEN) && dhandle->session_inuse == 0 && dhandle->session_ref == 0) {
			++closed_handles;
			continue;
		}

		/*延迟关闭的时间还未到，dhandle不做关闭*/
		if (dhandle->session_inuse != 0 || now <= dhandle->timeofdeath + conn->sweep_idle_time)
			continue;

		if (dhandle->timeofdeath == 0) {
			dhandle->timeofdeath = now;
			WT_STAT_FAST_CONN_INCR(session, dh_conn_tod);
			continue;
		}
		/*
		* We have a candidate for closing; if it's open, acquire an
		* exclusive lock on the handle and close it.
		*
		* The close would require I/O if an update cannot be written
		* (updates in a no-longer-referenced file might not yet be
		* globally visible if sessions have disjoint sets of files
		* open).  In that case, skip it: we'll retry the close the
		* next time, after the transaction state has progressed.
		*
		* We don't set WT_DHANDLE_EXCLUSIVE deliberately, we want
		* opens to block on us rather than returning an EBUSY error to
		* the application.
		*/
		if ((ret = __wt_try_writelock(session, dhandle->rwlock)) == EBUSY)
			continue;
		WT_RET(ret);

		/* Only sweep clean trees where all updates are visible.*/
		btree = dhandle->handle;
		if (btree->modified || !__wt_txn_visible_all(session, btree->rec_max_txn))
			goto unlock;

		/*尝试关闭dhanle*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN)){
			WT_WITH_DHANDLE(session, dhandle, ret = __wt_conn_btree_sync_and_close(session, 0, 0));
			/* We closed the btree handle, bump the statistic. */
			if (ret == 0)
				WT_STAT_FAST_CONN_INCR(session, dh_conn_handles);
		}
		/*设置关闭计数器*/
		if (dhandle->session_inuse == 0 && dhandle->session_ref == 0)
			++closed_handles;
unlock:
		WT_TRET(__wt_writeunlock(session, dhandle->rwlock));
		WT_RET_BUSY_OK(ret);
	}
	/*从connection list当中删除关闭的dhandle*/
	if (closed_handles > 0) {
		WT_WITH_DHANDLE_LOCK(session, ret = __sweep_remove_handles(session));
		WT_RET(ret);
	}

	return 0;
}

/*handle sweep线程主体*/
static WT_THREAD_RET __sweep_server(void* arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = arg;
	conn = S2C(session);

	while (F_ISSET(conn, WT_CONN_SERVER_RUN) && F_ISSET(conn, WT_CONN_SERVER_SWEEP)){
		WT_ERR(__wt_cond_wait(session, conn->sweep_cond, (uint64_t)conn->sweep_interval * WT_MILLION));

		WT_ERR(__sweep(session));
	}

	if (0){
err:
		WT_PANIC_MSG(session, ret, "handle sweep server error");
	}

	return WT_THREAD_RET_VALUE;
}

/*读取cfg关于sweep的配置项*/
int __wt_sweep_config(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*读取closed handle的延迟释放时间*/
	WT_RET(__wt_config_gets(session, cfg, "file_manager.close_idle_time", &cval));
	conn->sweep_idle_time = (time_t)cval.val;
	/*读取线程主体检查间隔时间*/
	WT_RET(__wt_config_gets(session, cfg, "file_manager.close_scan_interval", &cval));
	conn->sweep_interval = (time_t)cval.val;

	return 0;
}

/*启动sweep thread*/
int __wt_sweep_create(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* Set first, the thread might run before we finish up. */
	F_SET(conn, WT_CONN_SERVER_SWEEP);

	WT_RET(__wt_open_internal_session(conn, "sweep-server", 1, 1, &conn->sweep_session));
	session = conn->sweep_session;

	/*
	* Handle sweep does enough I/O it may be called upon to perform slow
	* operations for the block manager.
	*/
	F_SET(session, WT_SESSION_CAN_WAIT);

	WT_RET(__wt_cond_alloc(session, "handle sweep server", 0, &conn->sweep_cond));

	WT_RET(__wt_thread_create(session, &conn->sweep_tid, __sweep_server, session));
	conn->sweep_tid_set = 1;

	return (0);
}

/*停止sweep thread*/
int __wt_sweep_destroy(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_SWEEP);
	if (conn->sweep_tid_set) {
		WT_TRET(__wt_cond_signal(session, conn->sweep_cond));
		WT_TRET(__wt_thread_join(session, conn->sweep_tid));
		conn->sweep_tid_set = 0;
	}
	WT_TRET(__wt_cond_destroy(session, &conn->sweep_cond));

	if (conn->sweep_session != NULL) {
		wt_session = &conn->sweep_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));

		conn->sweep_session = NULL;
	}

	return ret;
}



