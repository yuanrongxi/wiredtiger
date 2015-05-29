/***************************************************************************
*对WT_ITEM中mem的分配操作
***************************************************************************/
#include "wt_internal.h"

/*尝试充分配ITEM中的mem*/
int __wt_buf_grow_worker(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
{
	size_t offset;
	int copy_data;	/*需要拷贝数据的长度*/

	/*判断buf->data是否在buf->mem上，如果在，不用做数据拷贝*/
	if(WT_DATA_IN_ITEM(buf)){
		offset =WT_PTRDIFF(buf->data, buf->mem);
		copy_data = 0;
	}
	else{
		offset = 0;
		copy_data = buf->size ? 1 : 0;
	}

	/*进行内存重分配*/
	if(size > buf->memsize){
		if (F_ISSET(buf, WT_ITEM_ALIGNED))
			WT_RET(__wt_realloc_aligned(session, &buf->memsize, size, &buf->mem));
		else
			WT_RET(__wt_realloc(session, &buf->memsize, size, &buf->mem));
	}

	if(buf->data == NULL){
		buf->data = buf->mem;
		buf->size = 0;
	}
	else{
		if (copy_data) /*进行数据拷贝*/
			memcpy(buf->mem, buf->data, buf->size);

		buf->data = (uint8_t *)buf->mem + offset;
	}

	return 0;
}

/*将数据格式化到buf中*/
int __wt_buf_fmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;
	size_t len;

	for (;;) {
		va_start(ap, fmt);
		len = (size_t)vsnprintf(buf->mem, buf->memsize, fmt, ap);
		va_end(ap);

		/*检查buf是否有猪狗的空间存下格式化的串*/
		if (len < buf->memsize) {
			buf->data = buf->mem;
			buf->size = len;

			return (0);
		}

		/* If not, double the size of the buffer: we're dealing with
		 * strings, and we don't expect these numbers to get huge.
		 * buf->mem无法存下格式化后的字符串，进行扩大*/
		WT_RET(__wt_buf_extend(session, buf, len + 1));
	}
}

/*append方式格式化串到buf中，可能在调用此函数之前，buf中已经存有一定的数据，格式化后将追加在后面*/
int __wt_buf_catfmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;
	size_t len, space;
	char *p;

	WT_ASSERT(session, buf->data == NULL || WT_DATA_IN_ITEM(buf));

	for(;;){
		va_start(ap, fmt);

		p = (char *)((uint8_t *)buf->mem + buf->size);
		WT_ASSERT(session, buf->memsize >= buf->size);
		space = buf->memsize - buf->size;
		len = (size_t)vsnprintf(p, (size_t)space, fmt, ap);

		va_end(ap);

		if(len < space){
			buf->size += len;
			return 0;
		}

		WT_RET(__wt_buf_extend(session, buf, buf->size + len + 1));
	}
}

int __wt_scr_alloc_func(WT_SESSION_IMPL *session, size_t size, WT_ITEM **scratchp)
{
	WT_DECL_RET;
	WT_ITEM *buf, **p, **best, **slot;
	size_t allocated;
	u_int i;

	/* Don't risk the caller not catching the error. */
	*scratchp = NULL;

	/*
	 * Each WT_SESSION_IMPL has an array of scratch buffers available for
	 * use by any function.  We use WT_ITEM structures for scratch memory
	 * because we already have functions that do variable-length allocation
	 * on a WT_ITEM.  Scratch buffers are allocated only by a single thread
	 * of control, so no locking is necessary.
	 *
	 * Walk the array, looking for a buffer we can use.
	 * 在session的scratch buffer中获取能利用的WT_ITEM缓冲区*/
	for (i = 0, best = slot = NULL,
	    p = session->scratch; i < session->scratch_alloc; ++i, ++p) {
		/* If we find an empty slot, remember it. */
		if ((buf = *p) == NULL) {
			if (slot == NULL)
				slot = p;
			continue;
		}

		/*WT_ITEM已经被占用了*/
		if (F_ISSET(buf, WT_ITEM_INUSE))
			continue;

		/*
		 * If we find a buffer that's not in-use, check its size: we
		 * want the smallest buffer larger than the requested size,
		 * or the largest buffer if none are large enough.
		 */
		if (best == NULL ||
		    (buf->memsize <= size && buf->memsize > (*best)->memsize) ||
		    (buf->memsize >= size && buf->memsize < (*best)->memsize))
			best = p;

		/* If we find a perfect match, use it. */
		if ((*best)->memsize == size)
			break;
	}

	/*
	 * If we didn't find a free buffer, extend the array and use the first
	 * slot we allocated.
	 * 在session的scratch中没有找到合适的WT_ITEM,扩大scratch缓冲区*/
	if (best == NULL && slot == NULL) {
		allocated = session->scratch_alloc * sizeof(WT_ITEM *);
		WT_ERR(__wt_realloc(session, &allocated, (session->scratch_alloc + 10) * sizeof(WT_ITEM *), &session->scratch));
		/*指定新分配内存的WT_ITEM在scratch的位置*/
		slot = session->scratch + session->scratch_alloc;
		session->scratch_alloc += 10;
	}

	/*分配一个新的WT_ITEM并将它放入scratch中*/
	if (best == NULL) {
		WT_ASSERT(session, slot != NULL);
		best = slot;

		WT_ERR(__wt_calloc_one(session, best));

		/* Scratch buffers must be aligned. */
		F_SET(*best, WT_ITEM_ALIGNED);
	}

	/* Grow the buffer as necessary and return. */
	session->scratch_cached -= (*best)->memsize;
	WT_ERR(__wt_buf_init(session, *best, size));
	/*设置为占用状态*/
	F_SET(*best, WT_ITEM_INUSE); 

	*scratchp = *best;
	return (0);

err:	
	WT_RET_MSG(session, ret, "session unable to allocate a scratch buffer");
}

/*释放session->scratch和它管理的wt_item*/
void __wt_scr_discard(WT_SESSION_IMPL *session)
{
	WT_ITEM **bufp;
	u_int i;

	for (i = 0, bufp = session->scratch; i < session->scratch_alloc; ++i, ++bufp) {
		if (*bufp == NULL)
			continue;

		if (F_ISSET(*bufp, WT_ITEM_INUSE))
			__wt_errx(session, "scratch buffer allocated and never discarded");

		__wt_buf_free(session, *bufp);
		__wt_free(session, *bufp);
	}

	session->scratch_alloc = 0;
	session->scratch_cached = 0;
	__wt_free(session, session->scratch);
}

/*从session的scratch中获得一个WT_ITEM,并将其mem作为缓冲区返回*/
void *__wt_ext_scr_alloc(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, size_t size)
{
	WT_ITEM *buf;
	WT_SESSION_IMPL *session;

	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	return (__wt_scr_alloc(session, size, &buf) == 0 ? buf->mem : NULL);
}

/*将缓冲区p对应的WT_ITEM归还给scratch*/
void __wt_ext_scr_free(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, void *p)
{
	WT_ITEM **bufp;
	WT_SESSION_IMPL *session;
	u_int i;

	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	for (i = 0, bufp = session->scratch; i < session->scratch_alloc; ++i, ++bufp){
		if (*bufp != NULL && (*bufp)->mem == p) {
			F_CLR(*bufp, WT_ITEM_INUSE); /*清除占用状态*/
			return;
		}
	}

	__wt_errx(session, "extension free'd non-existent scratch buffer");
}


