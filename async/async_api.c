
#include "wt_internal.h"

/* 通过uri和config信息在async format queue中查找k/v format,并设置到op中 */
static int __async_get_format(WT_CONNECTION_IMPL* conn, const char* uri, const char* config, WT_ASYNC_OP_IMPL* op)
{
	WT_ASYNC *async;
	WT_ASYNC_FORMAT *af;
	WT_CURSOR *c;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	WT_SESSION_IMPL *session;
	uint64_t cfg_hash, uri_hash;

	async = conn->async;
	c = NULL;
	op->format = NULL;

	if (uri != NULL)
		uri_hash = __wt_hash_city64(uri, strlen(uri));
	else
		uri_hash = 0;

	if (config != NULL)
		cfg_hash = __wt_hash_city64(config, strlen(config));
	else
		cfg_hash = 0;

	/*
	* We don't need to hold a lock around this walk.  The list is
	* permanent and always valid.  We might race an insert and there
	* is a possibility a duplicate entry might be inserted, but
	* that is not harmful.
	*/
	STAILQ_FOREACH(af, &async->formatqh, q) {
		if (af->uri_hash == uri_hash && af->cfg_hash == cfg_hash)
			goto setup;
	}
   
   /*
	* We didn't find one in the cache.  Allocate and initialize one.
	* Insert it at the head expecting LRU usage.  We need a real session
	* for the cursor.
	*/
	WT_RET(__wt_open_internal_session(conn, "async-cursor", 1, 1, &session));
	__wt_spin_lock(session, &async->ops_lock);
	WT_ERR(__wt_calloc_one(session, &af));
	WT_ERR(__wt_strdup(session, uri, &af->uri));
	WT_ERR(__wt_strdup(session, config, &af->config));
	af->uri_hash = uri_hash;
	af->cfg_hash = cfg_hash;

	wt_session = &session->iface;
	WT_ERR(wt_session->open_cursor(wt_session, uri, NULL, NULL, &c));
	WT_ERR(__wt_strdup(session, c->key_format, &af->key_format));
	WT_ERR(__wt_strdup(session, c->value_format, &af->value_format));
	WT_ERR(c->close(c));
	c = NULL;
	/*添加到format queue head中*/
	STAILQ_INSERT_HEAD(&async->formatqh, af, q);
	__wt_spin_unlock(session, &async->ops_lock);
	WT_ERR(wt_session->close(wt_session, NULL));

setup:
	op->format = af;
	op->iface.c.key_format = op->iface.key_format = af->key_format;
	op->iface.c.value_format = op->iface.value_format = af->value_format;

	return 0;

err:
	if (c != NULL)
		(void)c->close(c);

	__wt_free(session, af->uri);
	__wt_free(session, af->config);
	__wt_free(session, af->key_format);
	__wt_free(session, af->value_format);
	__wt_free(session, af);

	return ret;
}

/*在async_ops池中获得一个op对象*/
static int __async_new_op_alloc(WT_SESSION_IMPL* session, const char* uri, const char* config, WT_ASYNC_OP_IMPL** opp)
{
	WT_ASYNC *async;
	WT_ASYNC_OP_IMPL *op;
	WT_CONNECTION_IMPL *conn;
	uint32_t i, save_i, view;

	conn = S2C(session);
	async = conn->async;
	WT_STAT_FAST_CONN_INCR(session, async_op_alloc);
	*opp = NULL;

retry:
	op = NULL;
	WT_ORDERED_READ(save_i, async->ops_index);
	/*
	* Look after the last one allocated for a free one.  We'd expect
	* ops to be freed mostly FIFO so we should quickly find one.
	*/
	/*在async_ops中查找一个free状态下的op对象*/
	for (view = 1, i = save_i; i < conn->async_size; i++, view++){
		op = &async->async_ops[i];
		if (op->state == WT_ASYNCOP_FREE)
			break;
	}

	/*
	* Loop around back to the beginning if we need to.
	*/
	if (op == NULL || op->state != WT_ASYNCOP_FREE){
		for (i = 0; i < save_i; i++, view++){
			op = &async->async_ops[i];
			if (op->state == WT_ASYNCOP_FREE)
				break;
		}
	}

	/*没找合适的空闲op对象, 直接返回忙*/
	if (op == NULL || op->state != WT_ASYNCOP_FREE){
		WT_STAT_FAST_CONN_INCR(session, async_full);
		WT_RET(EBUSY);
	}

	/*设置op的状态，表示已经占用状态*/
	if (!WT_ATOMIC_CAS4(op->state, WT_ASYNCOP_FREE, WT_ASYNCOP_READY)) {
		WT_STAT_FAST_CONN_INCR(session, async_alloc_race);
		goto retry;
	}

	/*对OP对象的k/v format、unique_id、optype的设置*/
	WT_STAT_FAST_CONN_INCRV(session, async_alloc_view, view);
	WT_RET(__async_get_format(conn, uri, config, op));
	op->unique_id = WT_ATOMIC_ADD8(async->op_id, 1);
	op->optype = WT_AOP_NONE;

	(void)WT_ATOMIC_STORE4(async->ops_index, (i + 1) % conn->async_size);
	*opp = op;

	return 0;
}

/*获取async中的配置信息*/
static int __async_config(WT_SESSION_IMPL* session, WT_CONNECTION_IMPL* conn, const char** cfg, int *runp)
{
	WT_CONFIG_ITEM cval;

	/*
	* The async configuration is off by default.
	*/
	WT_RET(__wt_config_gets(session, cfg, "async.enabled", &cval));
	*runp = cval.val != 0;

	WT_RET(__wt_config_gets(session, cfg, "async.op_max", &cval));
	conn->async_size = (uint32_t)WT_MAX(cval.val, 10);

	WT_RET(__wt_config_gets(session, cfg, "async.threads", &cval));
	conn->async_workers = (uint32_t)cval.val;

	WT_ASSERT(session, conn->async_workers <= WT_ASYNC_MAX_WORKERS);

	return 0;
}

/*更新async的统计信息*/
void __wt_async_stats_update(WT_SESSION_IMPL* session)
{
	WT_ASYNC *async;
	WT_CONNECTION_IMPL *conn;
	WT_CONNECTION_STATS *stats;

	conn = S2C(session);
	async = conn->async;
	if (async == NULL)
		return;

	stats = &conn->stats;
	WT_STAT_SET(stats, async_cur_queue, async->cur_queue);
	WT_STAT_SET(stats, async_max_queue, async->max_queue);
	F_SET(conn, WT_CONN_SERVER_ASYNC);
}

static int __async_start(WT_SESSION_IMPL* session)
{
	WT_ASYNC *async;
	WT_CONNECTION_IMPL *conn;
	uint32_t i;

	conn = S2C(session);
	conn->async_cfg = 1;

	/*创建一个async flush子模块对象*/
	WT_RET(__wt_calloc_one(session, &conn->async));
	async = conn->async;
	STAILQ_INIT(&async->formatqh);
	WT_RET(__wt_spin_init(session, &async->ops_lock, "ops"));
	WT_RET(__wt_cond_alloc(session, "async flush", 0, &async->flush_cond));
	WT_RET(__wt_async_op_init(session));

	/*根据配置信息启动sync threads,先创建各个线程所用的session对象，再启动线程*/
	F_SET(conn, WT_CONN_SERVER_ASYNC);
	for (i = 0; i < conn->async_workers; i++){
		WT_RET(__wt_open_internal_session(conn, "async-worker", 1, 1, &async->worker_sessions[i]));
		F_SET(async->worker_sessions[i], WT_SESSION_SERVER_ASYNC);
	}

	for (i = 0; i < conn->async_workers; i++){
		WT_RET(__wt_thread_create(session, &async->worker_tids[i], __wt_async_worker, async->worker_sessions[i]));
	}

	__wt_async_stats_update(session);
	return 0;
}

/*创建并启动async flush模块*/
int __wt_async_create(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_CONNECTION_IMPL *conn;
	int run;

	conn = S2C(session);
	run = 0;
	WT_RET(__async_config(session, conn, cfg, &run));

	if (!run)
		return 0;

	return __async_start(session);
}

/**/
int __wt_async_reconfig(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_ASYNC *async;
	WT_CONNECTION_IMPL *conn, tmp_conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	int run;
	uint32_t i;

	conn = S2C(session);
	async = conn->async;
	memset(&tmp_conn, 0, sizeof(tmp_conn));
	tmp_conn.async_cfg = conn->async_cfg;
	tmp_conn.async_workers = conn->async_workers;
	tmp_conn.async_size = conn->async_size;
	
	/* Handle configuration. */
	run = conn->async_cfg;
	WT_RET(__async_config(session, &tmp_conn, cfg, &run));

	/*
	* There are some restrictions on the live reconfiguration of async.
	* Unlike other subsystems where we simply destroy anything existing
	* and restart with the new configuration, async is not so easy.
	* If the user is just changing the number of workers, we want to
	* allow the existing op handles and other information to remain in
	* existence.  So we must handle various combinations of changes
	* individually.
	*
	* One restriction is that if async is currently on, the user cannot
	* change the number of async op handles available.  The user can try
	* but we do nothing with it.  However we must allow the ops_max config
	* string so that a user can completely start async via reconfigure.
	*/

	/*
	* Easy cases:
	* 1. If async is on and the user wants it off, shut it down.
	* 2. If async is off, and the user wants it on, start it.
	* 3. If not a toggle and async is off, we're done.
	*/
	if (conn->async_cfg > 0 && !run){ /*用户关闭async子模块*/
		/*Case 1*/
		WT_TRET(__wt_async_flush(session));
		ret = __wt_async_destroy(session);
		conn->async_cfg = 0;
		return ret;
	}
	else if (conn->async_cfg == 0 && run)
		__async_start(session);
	else if (conn->async_cfg == 0) /*不是快关控制信号*/
		return 0;

	/*
	* Running async worker modification cases:
	* 4. If number of workers didn't change, we're done.
	* 5. If more workers, start new ones.
	* 6. If fewer workers, kill some.
	*/
	if (conn->async_workers == tmp_conn.async_workers)
		return 0;
	if (conn->async_workers < tmp_conn.async_workers) /*需要启动更多workers*/{
		/*先新建启动线程需要的session对象*/
		for (i = conn->async_workers; i < tmp_conn.async_workers; i++) {
			WT_RET(__wt_open_internal_session(conn, "async-worker", 1, 1, &async->worker_sessions[i]));
			F_SET(async->worker_sessions[i], WT_SESSION_SERVER_ASYNC);
		}
		/*启动需要线程任务*/
		for (i = conn->async_workers; i < tmp_conn.async_workers; i++)
			WT_RET(__wt_thread_create(session, &async->worker_tids[i], __wt_async_worker, async->worker_sessions[i]));

		conn->async_workers = tmp_conn.async_workers;
	}

	/*停止掉多余的线程任务*/
	if (conn->async_workers > tmp_conn.async_workers){
		for (i = conn->async_workers - 1;
			i >= tmp_conn.async_workers; i--) {
			/*
			* Join any worker we're stopping.
			* After the thread is stopped, close its session.
			*/
			WT_ASSERT(session, async->worker_tids[i] != 0);
			WT_ASSERT(session, async->worker_sessions[i] != NULL);
			F_CLR(async->worker_sessions[i], WT_SESSION_SERVER_ASYNC);
			WT_TRET(__wt_thread_join(session, async->worker_tids[i]));
			async->worker_tids[i] = 0;
			wt_session = &async->worker_sessions[i]->iface;
			WT_TRET(wt_session->close(wt_session, NULL));
			async->worker_sessions[i] = NULL;
		}
		conn->async_workers = tmp_conn.async_workers;
	}

	return 0;
}

/*撤销async子模块*/
int __wt_async_destroy(WT_SESSION_IMPL* session)
{
	WT_ASYNC *async;
	WT_ASYNC_FORMAT *af, *afnext;
	WT_ASYNC_OP *op;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	uint32_t i;

	conn = S2C(session);
	async = conn->async;

	if (!conn->async_cfg)
		return 0;

	/*停止线程*/
	F_CLR(conn, WT_CONN_SERVER_ASYNC);
	for (i = 0; i < conn->async_workers; i++){
		if (async->worker_tids[i] != 0) {
			WT_TRET(__wt_thread_join(session, async->worker_tids[i]));
			async->worker_tids[i] = 0;
		}
	}
	/*销毁flush io信号量*/
	WT_TRET(__wt_cond_destroy(session, &async->flush_cond));

	/* Close the server threads' sessions. */
	for (i = 0; i < conn->async_workers; i++){
		if (async->worker_sessions[i] != NULL) {
			wt_session = &async->worker_sessions[i]->iface;
			WT_TRET(wt_session->close(wt_session, NULL));
			async->worker_sessions[i] = NULL;
		}
	}

	/* 释放async ops池中op的k/v缓冲区 */
	for (i = 0; i < conn->async_size; i++) {
		op = (WT_ASYNC_OP *)&async->async_ops[i];
		if (op->c.key.data != NULL)
			__wt_buf_free(session, &op->c.key);
		if (op->c.value.data != NULL)
			__wt_buf_free(session, &op->c.value);
	}

	/* 释放format queue中的k/v format */
	af = STAILQ_FIRST(&async->formatqh);
	while (af != NULL) {
		afnext = STAILQ_NEXT(af, q);
		__wt_free(session, af->uri);
		__wt_free(session, af->config);
		__wt_free(session, af->key_format);
		__wt_free(session, af->value_format);
		__wt_free(session, af);
		af = afnext;
	}

	__wt_free(session, async->async_queue);
	__wt_free(session, async->async_ops);
	__wt_spin_destroy(session, &async->ops_lock);
	__wt_free(session, conn->async);

	return ret;
}

