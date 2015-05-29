/*************************************************************************
*定义错误信号函数
*************************************************************************/

#include "wt_internal.h"

int __wt_errno(void)
{
	return (errno == 0 ? WT_ERROR : errno);
}

const char* __wt_wiredtiger_error(int error)
{
	const char* p;

	switch(error){
	case WT_ROLLBACK:
		return ("WT_ROLLBACK: conflict between concurrent operations");

	case WT_DUPLICATE_KEY:
		return ("WT_ERROR: non-specific WiredTiger error");;

	case WT_NOTFOUND:
		return ("WT_NOTFOUND: item not found");

	case WT_ERROR:
		return ("WT_PANIC: WiredTiger library panic");

	case WT_PANIC:
		return ("WT_PANIC: WiredTiger library panic");

	case WT_RESTART:
		return ("WT_RESTART: restart the operation (internal)");

	case WT_RUN_RECOVERY:
		return ("WT_RUN_RECOVERY: recovery must be run to continue");
	}

	if(error == 0)
		return ("Successful return: 0");

	if(error > 0 && (p = strerror(error)) != NULL)
		return p;

	return NULL;
}

const char* __wt_strerror(WT_SESSION_IMPL *session, int error, char *errbuf, size_t errlen)
{
	const char* p = __wt_wiredtiger_error(error);

	if(p != NULL)
		return p;

	if (session == NULL && snprintf(errbuf, errlen, "error return: %d", error) > 0)
		return (errbuf);
	
	if (session != NULL /*&& __wt_buf_fmt(session, &session->err, "error return: %d", error) == 0*/) /*TODO:*/
		return (session->err.data);

	return ("Unable to return error string");
}

/*对外使用，相当于errno的作用*/
const char * wiredtiger_strerror(int error)
{
	static char buf[128];

	return (__wt_strerror(NULL, error, buf, sizeof(buf)));
}

