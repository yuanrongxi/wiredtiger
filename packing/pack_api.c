/****************************************************************************
*对pack/unpack的API封装,类似printf/scanf的可变参数封装
****************************************************************************/

#include "wt_internal.h"


/*对格式化结构进行pack,内存存入buffer,相当于printf*/
int wiredtiger_struct_pack(WT_SESSION* wt_session, void* buffer, size_t size, const char* fmt, ...)
{	
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	session = (WT_SESSION_IMPL *)wt_session;

	va_start(ap, fmt);
	ret = __wt_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return ret;
}

/*对格式化的数据的pack长度进行计算和pack*/
int wiredtiger_struct_size(WT_SESSION* wt_session, size_t* sizep, const char* fmt, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL* session;
	va_list ap;

	session = (WT_SESSION_IMPL* )wt_session;

	va_start(ap, fmt);
	ret = __wt_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return ret;
}

/*对buffer中的数据格式化unpack，相当于scanf*/
int wiredtiger_struct_unpack(WT_SESSION *wt_session, const void *buffer, size_t size, const char *fmt, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	session = (WT_SESSION_IMPL *)wt_session;

	va_start(ap, fmt);
	ret = __wt_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return ret;
}

/*带wt_api对象的pack*/
int __wt_ext_struct_pack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
	void *buffer, size_t size, const char *fmt, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;
	
	/*如果wt_session = NULL,从wt_api中获取一个默认的session来做调用参数*/
	session = (wt_session != NULL) ? (WT_SESSION_IMPL *)wt_session :
		((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __wt_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return ret;
}

int __wt_ext_struct_size(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
	size_t *sizep, const char *fmt, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	session = (wt_session != NULL) ? (WT_SESSION_IMPL *)wt_session :
		((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __wt_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return ret;
}

int __wt_ext_struct_unpack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
	const void *buffer, size_t size, const char *fmt, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	va_list ap;

	session = (wt_session != NULL) ? (WT_SESSION_IMPL *)wt_session :
		((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __wt_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return ret;
}









