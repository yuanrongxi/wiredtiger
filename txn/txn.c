
#include "wt_internal.h"

/*比较两个事务ID的大小，函数的作用是为了排序检索用的*/
int WT_CDECL __wt_txnid_cmp(const void* v1, const void* v2)
{
	uint64_t id1, id2;

	id1 = *(uint64_t *)v1;
	id2 = *(uint64_t *)v2;

	return ((id1 == id2) ? 0 : TXNID_LT(id1, id2) ? -1 : 1);
}

/*按事务ID按从小到大对snapshot数组进行排序*/
static void __txn_sort_snapshot(WT_SESSION_IMPL* session, uint32_t n, uint64_t snap_max)
{
	WT_TXN* txn;
	txn = &session->txn;

	if(n > 1)
		qsort(txn->snapshot, n, sizeof(uint64_t), __wt_txnid_cmp);
	txn->snapshot_count = n;
	txn->snap_max = snap_max;
	txn->snap_min = (n > 0 && TXNID_LE(txn->snapshot[0], snap_max)) ? txn->snapshot[0] : snap_max;
	F_SET(txn, TXN_HAS_SNAPSHOT);

	WT_ASSERT(session, n == 0 || txn->snap_min != WT_TXN_NONE);
}

/*release当前的事务的snapshot*/
void __wt_txn_release_snapshot(WT_SESSION_IMPL* session)
{
	WT_TXN *txn;
	WT_TXN_STATE *txn_state;

	txn = &session->txn;
	txn_state = &S2C(session)->txn_global.states[session->id];

	if(txn_state->snap_min != WT_TXN_NONE){
		WT_ASSERT(session, session->txn.isolation == TXN_ISO_READ_UNCOMMITTED || !__wt_txn_visible_all(session, txn_state->snap_min));
		txn_state->snap_min = WT_TXN_NONE;
	}

	F_CLR(txn, TXN_HAS_SNAPSHOT);
}

/*
 * __wt_txn_update_oldest --
 *	Sweep the running transactions to update the oldest ID required.
 */
void __wt_txn_update_oldest(WT_SESSION_IMPL* session)
{
	/*
	 * !!!
	 * If a data-source is calling the WT_EXTENSION_API.transaction_oldest
	 * method (for the oldest transaction ID not yet visible to a running
	 * transaction), and then comparing that oldest ID against committed
	 * transactions to see if updates for a committed transaction are still
	 * visible to running transactions, the oldest transaction ID may be
	 * the same as the last committed transaction ID, if the transaction
	 * state wasn't refreshed after the last transaction committed.  Push
	 * past the last committed transaction.
	 */
	/* 只是更新oldest id和old session! */
	__wt_txn_refresh(session, 0);
}

/*填充snapshot ID数组*/
void __wt_txn_refresh(WT_SESSION_IMPL* session, int get_snapshot)
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *s, *txn_state;
	uint64_t current_id, id, oldest_id;
	uint64_t prev_oldest_id, snap_min;
	uint32_t i, n, oldest_session, session_cnt;
	int32_t count;

	conn = S2C(session);
	txn = &session->txn;
	txn_global = &conn->txn_global;
	txn_state = &txn_global->states[session->id];

	current_id = snap_min = txn_global->current;
	prev_oldest_id = txn_global->oldest_id;

	/*当前事务已经是最早且存活的事务，不用扫描整个全局事务队列，将snapshot中的txn id数组排序即可*/
	if(prev_oldest_id == current_id){
		if (get_snapshot) {
			txn_state->snap_min = current_id;
			__txn_sort_snapshot(session, 0, current_id);
		}
		/*  在排序过程中，oldest id没有发生变化，直接退出 */
		if (prev_oldest_id == txn_global->oldest_id && txn_global->scan_count == 0)
			return;
	}

	/*等待其他线程完成对oldest update id的设置*/
	do {
		if ((count = txn_global->scan_count) < 0)
			WT_PAUSE();
	} while (count < 0 || !WT_ATOMIC_CAS4(txn_global->scan_count, count, count + 1));

	/**/
	prev_oldest_id = txn_global->oldest_id;
	current_id = oldest_id = snap_min = txn_global->current;
	oldest_session = 0;

	/*扫描整个全局事务队列，将小于当前事务ID的事务ID都加入到snapshot中*/
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for(i = n = 0, s = txn_global->states; i < session_cnt; i++, s++){
		/* Skip the checkpoint transaction; it is never read from. */
		if (txn_global->checkpoint_id != WT_TXN_NONE && s->id == txn_global->checkpoint_id)
			continue;

		/*这个执行事务的ID小于当前执行事务的ID，但是却不是oldest范围以外的txn,将其他加入snapshot数组*/
		if(s != txn_state && (id = s->id) != WT_TXN_NONE && TXNID_LE(prev_oldest_id, id)){
			if (get_snapshot)
				txn->snapshot[n++] = id;
			if (TXNID_LT(id, snap_min))
				snap_min = id;
		}

		/*
		 * Ignore the session's own snap_min: we are about to update it.
		 */
		if(get_snapshot && s == txn_state)
			continue;

		/*确定oldest id和oldest session，这个做法是兼容其他的read uncommited类型事务在操作开始之前会进行snap_min的刷新,所以这里在判断的时候，需要对这个情况进行判定*/
		if((id = s->snap_min) != WT_TXN_NONE && TXNID_LT(id, oldest_id)){
			oldest_id = id;
			oldest_session = i;
		}
	}

	/*假如我们需要获取snapshot信息，那么就需要确定这次snapshot的最小事务ID*/
	if (get_snapshot) {
		WT_ASSERT(session, TXNID_LE(prev_oldest_id, snap_min));
		WT_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
		txn_state->snap_min = snap_min;
	}

	/*更新全局的last running事务ID*/
	if (!get_snapshot || snap_min > txn_global->last_running + 100)
		txn_global->last_running = snap_min;

	/*
	 * Update the oldest ID if we have a newer ID and we can get exclusive
	 * access.  During normal snapshot refresh, only do this if we have a
	 * much newer value.  Once we get exclusive access, do another pass to
	 * make sure nobody else is using an earlier ID.
	 */
	if (TXNID_LT(prev_oldest_id, oldest_id) && (!get_snapshot || oldest_id - prev_oldest_id > 100) 
		&& WT_ATOMIC_CAS4(txn_global->scan_count, 1, -1)) { /*这个地方会让其他还是未开始scan的线程在scan时候进行等待,因为整个过程必须是独占方式确定oldest*/
		WT_ORDERED_READ(session_cnt, conn->session_cnt);
		for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
			if (txn_global->checkpoint_id != WT_TXN_NONE && s->id == txn_global->checkpoint_id)
				continue;

			if ((id = s->id) != WT_TXN_NONE && TXNID_LT(id, oldest_id))
				oldest_id = id;
			if ((id = s->snap_min) != WT_TXN_NONE && TXNID_LT(id, oldest_id))
				oldest_id = id;
		}
		if (TXNID_LT(txn_global->oldest_id, oldest_id)) /*更新全局oldest，所以需要独占*/
			txn_global->oldest_id = oldest_id;
		txn_global->scan_count = 0; /*让其他线程进行scan操作*/
	} 
	else {
		if (WT_VERBOSE_ISSET(session, WT_VERB_TRANSACTION) && current_id - oldest_id > 10000 &&
		    txn_global->oldest_session != oldest_session) {
			(void)__wt_verbose(session, WT_VERB_TRANSACTION,
			    "old snapshot %" PRIu64 " pinned in session %d [%s] with snap_min %" PRIu64 "\n",
			    oldest_id, oldest_session, conn->sessions[oldest_session].lastop,
			    conn->sessions[oldest_session].txn.snap_min);
			/*确定全局的oldest session*/
			txn_global->oldest_session = oldest_session;
		}

		WT_ASSERT(session, txn_global->scan_count > 0);
		(void)WT_ATOMIC_SUB4(txn_global->scan_count, 1);
	}

	if (get_snapshot)
		__txn_sort_snapshot(session, n, current_id);
}

/*session开始一个事务*/
int __wt_txn_begin(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_TXN* txn;

	txn = &session->txn;

	/*读取事务隔离级别配置项值*/
	WT_RET(__wt_config_gets_def(session, cfg, "isolation", 0, &cval));
	if(cval.len == 0) /*没有配置，直接用session的默认的隔离级别*/
		txn->isolation = session->isolation;
	else{
		txn->isolation = WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
			TXN_ISO_SNAPSHOT : (WT_STRING_MATCH("read-committed", cval.str, cval.len) ? TXN_ISO_READ_COMMITTED : TXN_ISO_READ_UNCOMMITTED);
	}

	/*
	 * The default sync setting is inherited from the connection, but can
	 * be overridden by an explicit "sync" setting for this transaction.
	 *
	 * !!! This is an unusual use of the config code: the "default" value
	 * we pass in is inherited from the connection.  If flush is not set in
	 * the connection-wide flag and not overridden here, we end up clearing
	 * all flags.
	 */
	txn->txn_logsync = S2C(session)->txn_logsync;
	WT_RET(__wt_config_gets_def(session, cfg, "sync", FLD_ISSET(txn->txn_logsync, WT_LOG_FLUSH) ? 1 : 0, &cval));
	if(!cval.val)
		txn->txn_logsync = 0;

	/*设置事务运行标示*/
	F_SET(txn, TXN_RUNNING);
	if(txn->isolation == TXN_ISO_SNAPSHOT){
		if (session->ncursors > 0)
			WT_RET(__wt_session_copy_values(session));
		__wt_txn_refresh(session, 1); /*在事务开始时，先建立一个snapshot*/
	}

	return 0;
}

/*释放session执行的事务关联的资源*/
void __wt_txn_release(WT_SESSION_IMPL* session)
{
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *txn_state;

	txn = &session->txn;
	WT_ASSERT(session, txn->mod_count == 0);
	txn->notify = NULL;

	txn_global = &S2C(session)->txn_global;
	txn_state = &txn_global->states[session->id];

	/* 清除全局事务队列中的事务ID,将事务设置为提交状态 */
	if(F_ISSET(txn, TXN_HAS_ID)){
		WT_ASSERT(session, txn_state->id != WT_TXN_NONE && txn->id != WT_TXN_NONE);
		WT_PUBLISH(txn_state->id, WT_TXN_NONE);
		txn->id = WT_TXN_NONE;
	}

	/*释放logrec对象空间*/
	__wt_logrec_free(session, &txn->logrec);

	/* Discard any memory from the session's split stash that we can. */
	WT_ASSERT(session, session->split_gen == 0);
	if (session->split_stash_cnt > 0)
		__wt_split_stash_discard(session);

	/*release snapshot*/
	__wt_txn_release_snapshot(session);
	/*重置隔离级别*/
	txn->isolation = session->isolation;
	/*重置事务状态*/
	F_CLR(txn, TXN_ERROR | TXN_HAS_ID | TXN_RUNNING);
}

/*事务提交过程*/
int __wt_txn_commit(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_DECL_RET;
	WT_TXN *txn;
	WT_TXN_OP *op;
	u_int i;

	txn = &session->txn;

	WT_ASSERT(session, !F_ISSET(txn, TXN_ERROR));

	/*如果不是运行状态下的事务是不能commit的*/
	if(!F_ISSET(txn, TXN_RUNNING))
		WT_RET_MSG(session, EINVAL, "No transaction is active");

	/*通知上层DBMS进入事务提交状态*/
	if(txn->notify != NULL)
		WT_TRET(txn->notify->notify(txn->notify, (WT_SESSION *)session, txn->id, 1));

	/*进行事务日志落盘提交，force log at commit*/
	if(ret == 0 && txn->mod_count > 0 && FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED)
		&& !F_ISSET(session, WT_SESSION_NO_LOGGING)){
		/*
		 * We are about to block on I/O writing the log.
		 * Release our snapshot in case it is keeping data pinned.
		 * This is particularly important for checkpoints.
		 */
		__wt_txn_release_snapshot(session);
		ret = __wt_txn_log_commit(session, cfg);
	}

	/*日志落盘失败，进行事务回滚*/
	if(ret != 0){
		WT_TRET(__wt_txn_rollback(session, cfg));
		return ret;
	}

	/*释放事务的操作对象列表单元，不能在日志提交前做，防止不能rollback*/
	for(i = 0, op = txn->mod; i < txn->mod_count; i ++, op++)
		__wt_txn_op_free(session, op);
	txn->mod_count = 0;

	if (session->ncursors > 0)
		WT_RET(__wt_session_copy_values(session));

	/*释放session执行的事务关联的资源*/
	__wt_txn_release(session);

	return 0;
}

/*事务回滚*/
int __wt_txn_rollback(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_DECL_RET;
	WT_TXN *txn;
	WT_TXN_OP *op;
	u_int i;

	WT_UNUSED(cfg);

	txn = &session->txn;
	if (!F_ISSET(txn, TXN_RUNNING))
		WT_RET_MSG(session, EINVAL, "No transaction is active");

	/* Rollback notification. */
	if (txn->notify != NULL)
		WT_TRET(txn->notify->notify(txn->notify, (WT_SESSION *)session, txn->id, 0));

	for(i = 0, op = txn->mod; i < txn->mod_count; i++, op++){
		/* Metadata updates are never rolled back. */
		if(op->fileid == WT_METAFILE_ID)
			continue;

		/* Metadata updates are never rolled back. */
		if (op->fileid == WT_METAFILE_ID)
			continue;

		/*取消更新操作*/
		switch (op->type) {
		case TXN_OP_BASIC:
		case TXN_OP_INMEM:
			op->u.upd->txnid = WT_TXN_ABORTED;
			break;
		case TXN_OP_REF:
			__wt_delete_page_rollback(session, op->u.ref);
			break;
		case TXN_OP_TRUNCATE_COL:
		case TXN_OP_TRUNCATE_ROW:
			/*
			 * Nothing to do: these operations are only logged for
			 * recovery.  The in-memory changes will be rolled back
			 * with a combination of TXN_OP_REF and TXN_OP_INMEM
			 * operations.
			 */
			break;
		}
		/*释放操作对象*/
		__wt_txn_op_free(session, op);
	}

	txn->mod_count = 0;
	/*释放事务关联的资源*/
	__wt_txn_release(session);
	return ret;
}

/*初始化session事务对象*/
int __wt_txn_init(WT_SESSION_IMPL* session)
{
	WT_TXN* txn;
	txn = &session->txn;
	txn->id = WT_TXN_NONE;

	WT_RET(__wt_calloc_def(session, S2C(session)->session_size, &txn->snapshot));

	/*
	 * Take care to clean these out in case we are reusing the transaction for eviction.
	 */
	txn->mod = NULL;
	txn->isolation = session->isolation;

	return 0;
}

/* 更新事务的统计信息 */
void __wt_txn_stats_update(WT_SESSION_IMPL *session)
{
	WT_TXN_GLOBAL *txn_global;
	WT_CONNECTION_IMPL *conn;
	WT_CONNECTION_STATS *stats;
	uint64_t checkpoint_snap_min;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	stats = &conn->stats;
	checkpoint_snap_min = txn_global->checkpoint_snap_min;

	WT_STAT_SET(stats, txn_pinned_range, txn_global->current - txn_global->oldest_id);
	WT_STAT_SET(stats, txn_pinned_checkpoint_range, checkpoint_snap_min == WT_TXN_NONE ? 0 : txn_global->current - checkpoint_snap_min);
}

/*销毁一个事务对象*/
void __wt_txn_destroy(WT_SESSION_IMPL* session)
{
	WT_TXN* txn;

	txn = &session->txn;
	__wt_free(session, txn->mod);
	__wt_free(session, txn->snapshot);
}

/*初始化全局的事务对象管理器*/
int __wt_txn_global_init(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *s;
	u_int i;

	WT_UNUSED(cfg);
	conn = S2C(session);

	txn_global = &conn->txn_global;
	txn_global->current = txn_global->last_running = txn_global->oldest_id = WT_TXN_FIRST;

	WT_RET(__wt_calloc_def(session, conn->session_size, &txn_global->states));
	for (i = 0, s = txn_global->states; i < conn->session_size; i++, s++)
		s->id = s->snap_min = WT_TXN_NONE;

	return 0;
}

/*销毁全局的事务对象管理器*/
void __wt_txn_global_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN_GLOBAL *txn_global;

	conn = S2C(session);
	txn_global = &conn->txn_global;

	if (txn_global != NULL)
		__wt_free(session, txn_global->states);
}
