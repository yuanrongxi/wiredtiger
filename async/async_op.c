
#include "wt_internal.h"

/*通过可变参数的输入来读取asyncop中cursor的key*/
static int __async_get_key(WT_ASYNC_OP* asyncop, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, asyncop);
	ret = __wt_cursor_get_keyv(&asyncop->c, asyncop->c.flags, ap);
	va_end(ap);

	return ret;
}

/*通过可变参数的输出来设置asyncop中cursor的key*/
static void __async_set_key(WT_ASYNC_OP *asyncop, ...)
{
	WT_CURSOR *c;
	WT_DECL_RET;
	va_list ap;

	c = &asyncop->c;
	va_start(ap, asyncop);

	__wt_cursor_set_keyv(c, c->flags, ap);
	if (!WT_DATA_IN_ITEM(&(c->key)) && !WT_CURSOR_RECNO(c)) /*将key的data位置移动到membuf的起始位置*/
		WT_ERR(__wt_buf_set(O2S((WT_ASYNC_OP_IMPL *)asyncop), &c->key, c->key.data, c->key.size));

	va_end(ap);
err:
	c->saved_err = ret;
}

/*通过可变参数的输入来读取asyncop中cursor的value*/
static int __async_get_value(WT_ASYNC_OP* asyncop, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, asyncop);
	ret = __wt_cursor_get_valuev(&asyncop->c, ap);
	va_end(ap);
	return ret;
}

/*通过可变参数的输出来设置asyncop中cursor的value*/
static void __async_set_value(WT_ASYNC_OP* asyncop, ...)
{
	WT_CURSOR *c;
	WT_DECL_RET;
	va_list ap;

	c = &asyncop->c;
	va_start(ap, asyncop);
	__wt_cursor_set_valuev(c, ap);

	/* Copy the data, if it is pointing at data elsewhere. */
	if (!WT_DATA_IN_ITEM(&c->value))
		WT_ERR(__wt_buf_set(O2S((WT_ASYNC_OP_IMPL *)asyncop), &c->value, c->value.data, c->value.size));
	va_end(ap);

err:
	c->saved_err = ret;
}

/*asnyc操作封装*/
static int __async_op_wrap(WT_ASYNC_OP_IMPL* op, WT_ASYNC_OPTYPE type)
{
	op->optype = type;
	return __wt_async_op_enqueue(O2S(op), op);
}

/*将一个async search操作封装进op queue*/
static int __async_search(WT_ASYNC_OP* asyncop)
{
	WT_ASYNC_OP_IMPL *op;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	op = (WT_ASYNC_OP_IMPL*)asyncop;

	ASYNCOP_API_CALL(O2C(op), session, search);
	WT_STAT_FAST_CONN_INCR(O2S(op), async_op_search);
	WT_ERR(__async_op_wrap(op, WT_AOP_SEARCH));

err:
	API_END_RET(session, ret);
}

/* 将一个async insert操作封装进op queue */
static int __async_insert(WT_ASYNC_OP* asyncop)
{
	WT_ASYNC_OP_IMPL *op;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	op = (WT_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, insert);
	WT_STAT_FAST_CONN_INCR(O2S(op), async_op_insert);
	WT_ERR(__async_op_wrap(op, WT_AOP_INSERT));

err:	
	API_END_RET(session, ret);
}

/* 将一个async udate操作封装进op queue */
static int __async_update(WT_ASYNC_OP *asyncop)
{
	WT_ASYNC_OP_IMPL *op;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	op = (WT_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, update);
	WT_STAT_FAST_CONN_INCR(O2S(op), async_op_update);
	WT_ERR(__async_op_wrap(op, WT_AOP_UPDATE));

err:	
	API_END_RET(session, ret);
}

/* 将一个async remove操作封装进op queue */
static int __async_remove(WT_ASYNC_OP *asyncop)
{
	WT_ASYNC_OP_IMPL *op;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	op = (WT_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, remove);
	WT_STAT_FAST_CONN_INCR(O2S(op), async_op_remove);
	WT_ERR(__async_op_wrap(op, WT_AOP_REMOVE));

err:	
	API_END_RET(session, ret);
}

/* 将一个async compact操作封装进op queue */
static int __async_compact(WT_ASYNC_OP *asyncop)
{
	WT_ASYNC_OP_IMPL *op;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	op = (WT_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, compact);
	WT_STAT_FAST_CONN_INCR(O2S(op), async_op_compact);
	WT_ERR(__async_op_wrap(op, WT_AOP_COMPACT));

err:	
	API_END_RET(session, ret);
}

/* 获取asyncop id */
static uint64_t __async_get_id(WT_ASYNC_OP* asyncop)
{
	return (((WT_ASYNC_OP_IMPL *)asyncop)->unique_id);
}

/* 获取asyncop type */
static WT_ASYNC_OPTYPE __async_get_type(WT_ASYNC_OP* asyncop)
{
	return (((WT_ASYNC_OP_IMPL *)asyncop)->optype);
}

/*初始化一个op操作对象*/
static int __async_op_init(WT_CONNECTION_IMPL* conn, WT_ASYNC_OP_IMPL* op, uint32_t id)
{
	WT_ASYNC_OP *asyncop;

	/*安装API外部回调函数*/
	asyncop = (WT_ASYNC_OP *)op;
	asyncop->connection = (WT_CONNECTION *)conn;
	asyncop->key_format = asyncop->value_format = NULL;
	asyncop->c.key_format = asyncop->c.value_format = NULL;
	asyncop->get_key = __async_get_key;
	asyncop->get_value = __async_get_value;
	asyncop->set_key = __async_set_key;
	asyncop->set_value = __async_set_value;
	asyncop->search = __async_search;
	asyncop->insert = __async_insert;
	asyncop->update = __async_update;
	asyncop->remove = __async_remove;
	asyncop->compact = __async_compact;
	asyncop->get_id = __async_get_id;
	asyncop->get_type = __async_get_type;

	/*
	 * The cursor needs to have the get/set key/value functions initialized.
	 * It also needs the key/value related fields set up.
	 */
	asyncop->c.get_key = __wt_cursor_get_key;
	asyncop->c.set_key = __wt_cursor_set_key;
	asyncop->c.get_value = __wt_cursor_get_value;
	asyncop->c.set_value = __wt_cursor_set_value;

	memset(asyncop->c.raw_recno_buf, 0, sizeof(asyncop->c.raw_recno_buf));
	memset(&asyncop->c.key, 0, sizeof(asyncop->c.key));
	memset(&asyncop->c.value, 0, sizeof(asyncop->c.value));
	asyncop->c.session = (WT_SESSION *)conn->default_session;
	asyncop->c.saved_err = 0;
	asyncop->c.flags = 0;

	op->internal_id = id;
	op->state = WT_ASYNCOP_FREE;

	return 0;
}

/*在work queue中进行操作排队*/
int __wt_async_op_enqueue(WT_SESSION_IMPL* session, WT_ASYNC_OP_IMPL* op)
{
	WT_ASYNC *async;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint64_t cur_head, cur_tail, my_alloc, my_slot;

	conn = S2C(session);
	async = conn->async;

	/*
	 * If an application re-uses a WT_ASYNC_OP, we end up here with an invalid object.
	 */
	if (op->state != WT_ASYNCOP_READY)
		WT_RET_MSG(session, EINVAL, "application error: WT_ASYNC_OP already in use");

	/*确定排队操作在队列中的位置，async_queue是一个ring buffer,循环队列无锁？？*/
	my_alloc = WT_ATOMIC_ADD8(async->alloc_head, 1);
	my_slot = my_alloc % async->async_qsize;

	/*等待自己slot操作时机,相当于spin wait*/
	WT_ORDERED_READ(cur_tail, async->tail_slot);
	while (cur_tail == my_slot){
		__wt_yield();
		WT_ORDERED_READ(cur_tail, async->tail_slot);
	}

	/*将操作插入到队列当中,并修改当前排队的op数量*/
	WT_PUBLISH(async->async_queue[my_slot], op);
	op->state = WT_ASYNCOP_ENQUEUED;
	if (WT_ATOMIC_ADD4(async->cur_queue, 1) > async->max_queue)
		WT_PUBLISH(async->max_queue, async->cur_queue);

	/*等待自己的op的排队确认*/
	WT_ORDERED_READ(cur_head, async->head);
	while (cur_head != (my_alloc - 1)) {
		__wt_yield();
		WT_ORDERED_READ(cur_head, async->head);
	}
	WT_PUBLISH(async->head, my_alloc);

	return ret;
}

/*初始化sync op handler*/
int __wt_async_op_init(WT_SESSION_IMPL* session)
{
	WT_ASYNC *async;
	WT_ASYNC_OP_IMPL *op;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint32_t i;

	conn = S2C(session);
	async = conn->async;

	/*初始化 flush op操作对象*/
	WT_RET(__async_op_init(conn, &async->flush_op, OPS_INVALID_INDEX));

	/*
	* Allocate and initialize the work queue.  This is sized so that
	* the ring buffer is known to be big enough such that the head
	* can never overlap the tail.  Include extra for the flush op.
	*/
	async->async_qsize = conn->async_size + 2;
	WT_RET(__wt_calloc_def(session, async->async_qsize, &async->async_queue));

	WT_ERR(__wt_calloc_def(session, conn->async_size, &async->async_ops));
	for (i = 0; i < conn->async_size; i++) {
		op = &async->async_ops[i];
		WT_ERR(__async_op_init(conn, op, i));
	}

	return 0;

err:
	if (async->async_ops != NULL) {
		__wt_free(session, async->async_ops);
		async->async_ops = NULL;
	}
	if (async->async_queue != NULL) {
		__wt_free(session, async->async_queue);
		async->async_queue = NULL;
	}

	return ret;
}

