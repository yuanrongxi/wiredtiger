#include "wt_internal.h"

/*flush stderr*/
static int __handle_error_default(WT_EVENT_HANDLER* handler, WT_SESSION* wt_session, int error, const char* errmsg)
{
	WT_UNUSED(handler);
	WT_UNUSED(wt_session);
	WT_UNUSED(error);

	WT_RET(__wt_fprintf(stderr, "%s\n", errmsg));
	WT_RET(__wt_fflush(stderr));

	return 0;
}

/*flush stdout*/
static int __handle_message_default(WT_EVENT_HANDLER* handler, WT_SESSION* wt_session, const char* message)
{
	WT_UNUSED(handler);
	WT_UNUSED(wt_session);

	WT_RET(__wt_fprintf(stdout, "%s\n", message));
	WT_RET(__wt_fflush(stdout));

	return 0;
}

static int __handle_progress_default(WT_EVENT_HANDLER* handler, WT_SESSION* wt_session, const char* operation, uint64_t progress)
{
	WT_UNUSED(handler);
	WT_UNUSED(wt_session);
	WT_UNUSED(operation);
	WT_UNUSED(progress);

	return 0;
}

static int __handle_close_default(WT_EVENT_HANDLER* handler, WT_SESSION* wt_session, WT_CURSOR* cursor)
{
	WT_UNUSED(handler);
	WT_UNUSED(wt_session);
	WT_UNUSED(cursor);

	return 0;
}

/*消息输出对象*/
static WT_EVENT_HANDLER __event_handler_default = {
	__handle_error_default,
	__handle_message_default,
	__handle_progress_default,
	__handle_close_default
};

/*错误信息输出*/
static void __handler_failure(WT_SESSION_IMPL* session, int error, const char* which, int error_handler_failed)
{
	WT_EVENT_HANDLER *handler;
	WT_SESSION *wt_session;

	char s[256];

	snprintf(s, sizeof(s), "application %s event handler failed: %s", which, __wt_strerror(session, error, NULL, 0));
	wt_session = (WT_SESSION *)session;
	handler = session->event_handler;
	if (!error_handler_failed && handler->handle_error != __handle_error_default && handler->handle_error(handler, wt_session, error, s) == 0)
		return;

	__handle_error_default(NULL, wt_session, error, s);
}

/*设置信息输出对象*/
void __wt_event_handler_set(WT_SESSION_IMPL *session, WT_EVENT_HANDLER *handler)
{
	if (handler == NULL)
		handler = &__event_handler_default;
	else {
		if (handler->handle_error == NULL)
			handler->handle_error = __handle_error_default;
		if (handler->handle_message == NULL)
			handler->handle_message = __handle_message_default;
		if (handler->handle_progress == NULL)
			handler->handle_progress = __handle_progress_default;
	}

	session->event_handler = handler;
}

int __wt_eventv(WT_SESSION_IMPL *session, int msg_event, int error, const char *file_name, int line_number, const char *fmt, va_list ap)
{
	WT_EVENT_HANDLER *handler;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	struct timespec ts;
	size_t len, remain, wlen;
	int prefix_cnt;
	const char *err, *prefix;
	char *end, *p, tid[128];


	char s[2048];

	if (session == NULL) {
		WT_RET(__wt_fprintf(stderr, "WiredTiger Error%s%s: ",
			error == 0 ? "" : ": ",
			error == 0 ? "" : __wt_strerror(session, error, NULL, 0)));

		WT_RET(__wt_vfprintf(stderr, fmt, ap));
		WT_RET(__wt_fprintf(stderr, "\n"));

		return __wt_fflush(stderr);
	}

	p = s;
	end = s + sizeof(s);

	prefix_cnt = 0;

	/*获取当前系统时间和线程ID，并将这两个信息加入到输出信息中*/
	if (__wt_epoch(session, &ts) == 0) {
		__wt_thread_id(tid, sizeof(tid));
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, "[%" PRIuMAX ":%" PRIuMAX "][%s]", (uintmax_t)ts.tv_sec, (uintmax_t)ts.tv_nsec / 1000, tid);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}

	/*错误提示前缀输出到信息中*/
	if ((prefix = S2C(session)->error_prefix) != NULL) {
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}

	prefix = (session->dhandle == NULL ? NULL : session->dhandle->name);
	if (prefix != NULL) {
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}

	if ((prefix = session->name) != NULL) {
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}

	if (prefix_cnt != 0) {
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, ": ");
		p = wlen >= remain ? end : p + wlen;
	}

	if (file_name != NULL) {
		remain = WT_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, "%s, %d: ", file_name, line_number);
		p = wlen >= remain ? end : p + wlen;
	}

	remain = WT_PTRDIFF(end, p);
	wlen = (size_t)vsnprintf(p, remain, fmt, ap);
	p = wlen >= remain ? end : p + wlen;

	/*获取错误码信息，并输出到信息中*/
	if(error != 0){
		err = __wt_strerror(session, error, NULL, 0);
		len = strlen(err);
		if (WT_PTRDIFF(p, s) < len || strcmp(p - len, err) != 0) {
			remain = WT_PTRDIFF(end, p);
			(void)snprintf(p, remain, ": %s", err);
		}
	}

	/*对信息进行打印输出*/
	wt_session = (WT_SESSION *)session;
	handler = session->event_handler;
	if (msg_event) {
		ret = handler->handle_message(handler, wt_session, s);
		if (ret != 0)
			__handler_failure(session, ret, "message", 0);
	} 
	else {
		ret = handler->handle_error(handler, wt_session, error, s);
		if (ret != 0 && handler->handle_error != __handle_error_default)
			__handler_failure(session, ret, "error", 1);
	}

	return ret;
}

/*错误信息打印*/
void __wt_err(WT_SESSION_IMPL *session, int error, const char *fmt, ...)WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;

	va_start(ap, fmt);
	(void)__wt_eventv(session, 0, error, NULL, 0, fmt, ap);
	va_end(ap);
}

void __wt_errx(WT_SESSION_IMPL *session, const char *fmt, ...)  WT_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	va_list ap;

	va_start(ap, fmt);
	(void)__wt_eventv(session, 0, 0, NULL, 0, fmt, ap);
	va_end(ap);
}

int __wt_ext_err_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt, ...) WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __wt_eventv(session, 0, 0, NULL, 0, fmt, ap);
	va_end(ap);
	return (ret);
}

static int info_msg(WT_SESSION_IMPL *session, const char *fmt, va_list ap)
{
	WT_EVENT_HANDLER *handler;
	WT_SESSION *wt_session;

	/*
	 * !!!
	 * SECURITY:
	 * Buffer placed at the end of the stack in case snprintf overflows.缓冲区溢出
	 */
	char s[2048];

	(void)vsnprintf(s, sizeof(s), fmt, ap);

	wt_session = (WT_SESSION *)session;
	handler = session->event_handler;
	return (handler->handle_message(handler, wt_session, s));
}

int __wt_msg(WT_SESSION_IMPL *session, const char *fmt, ...)WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = info_msg(session, fmt, ap);
	va_end(ap);

	return ret;
}

int __wt_ext_msg_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt, ...) WT_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	va_start(ap, fmt);
	ret = info_msg(session, fmt, ap);
	va_end(ap);
	return (ret);
}

const char* __wt_ext_strerror(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, int error)
{
	if (wt_session == NULL)
		wt_session = (WT_SESSION *)((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	return (wt_session->strerror(wt_session, error));
}

int __wt_progress(WT_SESSION_IMPL *session, const char *s, uint64_t v)
{
	WT_DECL_RET;
	WT_EVENT_HANDLER *handler;
	WT_SESSION *wt_session;

	wt_session = (WT_SESSION *)session;
	handler = session->event_handler;
	if (handler != NULL && handler->handle_progress != NULL)
		if ((ret = handler->handle_progress(handler,wt_session, s == NULL ? session->name : s, v)) != 0)
			__handler_failure(session, ret, "progress", 0);

	return 0;
}

/*输出ASSERT信息*/
void __wt_assert(WT_SESSION_IMPL* session, int error, const char* file_name,
					int line_number, const char* fmt, ...)WT_GCC_FUNC_ATTRIBUTE((format (printf, 5, 6)))
{
	va_list ap;

	va_start(ap, fmt);
	(void)__wt_eventv(session, 0, error, file_name, line_number, fmt, ap);
	va_end(ap);
}

/*输出异常信息*/
int __wt_panic(WT_SESSION_IMPL *session)
{
	F_SET(S2C(session), WT_CONN_PANIC);
	__wt_err(session, WT_PANIC, "the process must exit and restart");

	return WT_PANIC;
}

/*打印一个不合法的值的提示*/
int __wt_illegal_value(WT_SESSION_IMPL* session, const char* name)
{
	__wt_errx(session, "%s%s%s", name == NULL ? "" : name, name == NULL ? "" : ": ",
		"encountered an illegal file format or internal value");

	return __wt_panic(session);
}

int __wt_object_unsupported(WT_SESSION_IMPL *session, const char *uri)
{
	WT_RET_MSG(session, ENOTSUP, "unsupported object operation: %s", uri);
}

int __wt_bad_object_type(WT_SESSION_IMPL *session, const char *uri)
{
	if (WT_PREFIX_MATCH(uri, "backup:") ||
		WT_PREFIX_MATCH(uri, "colgroup:") ||
		WT_PREFIX_MATCH(uri, "config:") ||
		WT_PREFIX_MATCH(uri, "file:") ||
		WT_PREFIX_MATCH(uri, "index:") ||
		WT_PREFIX_MATCH(uri, "log:") ||
		WT_PREFIX_MATCH(uri, "lsm:") ||
		WT_PREFIX_MATCH(uri, "statistics:") ||
		WT_PREFIX_MATCH(uri, "table:"))
		return (__wt_object_unsupported(session, uri));

	WT_RET_MSG(session, ENOTSUP, "unknown object type: %s", uri);
}












