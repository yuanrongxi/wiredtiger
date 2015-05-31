

static inline int __wt_buf_grow(WT_SESSION_IMPL* session, WT_ITEM* buf, size_t size)
{
	return (size > buf->memsize || !WT_DATA_IN_ITEM(buf) ? __wt_buf_grow_worker(session, buf, size) : 0);
}


static inline int __wt_buf_extend(WT_SESSION_IMPL* session, WT_ITEM *buf, size_t size)
{
	return (size > buf->memsize ? __wt_buf_grow(session, buf, WT_MAX(size, 2 * buf->memsize)) : 0);
}

static inline int __wt_buf_init(WT_SESSION_IMPL* session, WT_ITEM* buf, size_t size)
{
	buf->data = buf->mem;
	buf->size = 0;
	WT_RET(__wt_buf_grow(session, buf, size));

	return (0);
}

/*初始化wt_item*/
static inline int __wt_buf_initsize(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
{
	buf->data = buf->mem;
	buf->size = 0;				/* Clear existing data length */
	WT_RET(__wt_buf_grow(session, buf, size));
	buf->size = size;			/* Set the data length. */

	return (0);
}

static inline int __wt_buf_set(WT_SESSION_IMPL *session, WT_ITEM *buf, const void *data, size_t size)
{
	/*用data初始化wt_item*/
	WT_RET(__wt_buf_initsize(session, buf, size));

	memmove(buf->mem, data, size);

	return (0);
}


static inline int __wt_buf_setstr(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *s)
{
	return (__wt_buf_set(session, buf, s, strlen(s) + 1));
}

static inline int __wt_buf_set_printable(WT_SESSION_IMPL *session, WT_ITEM *buf, const void *from_arg, size_t size)
{
	return (__wt_raw_to_esc_hex(session, from_arg, size, buf));
}

/*释放item->mem*/
static inline void __wt_buf_free(WT_SESSION_IMPL *session, WT_ITEM *buf)
{
	__wt_free(session, buf->mem);
	memset(buf, 0, sizeof(WT_ITEM));
}

/*scratch方式回收WT_ITEM缓冲区*/
static inline void __wt_scr_free(WT_SESSION_IMPL *session, WT_ITEM **bufp)
{
	WT_ITEM *buf;

	buf = *bufp;
	if (buf != NULL) {
		*bufp = NULL;

		/*如果scratch_cached已经超出session_scratch_max最大的缓冲区大小，直接FREE掉ITEM,否则srcatch对WT_ITEM进行回收*/
		if (session->scratch_cached + buf->memsize >= S2C(session)->session_scratch_max) {
			__wt_free(session, buf->mem);
			buf->memsize = 0;
		} 
		else
			session->scratch_cached += buf->memsize;

		buf->data = NULL;
		buf->size = 0;
		/*清除占用状态*/
		F_CLR(buf, WT_ITEM_INUSE);
	}
}

