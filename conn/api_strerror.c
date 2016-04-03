#include "wt_internal.h"

const char* __wt_wiredtiger_error(int error)
{
	const char *p;

	/*
	 * Check for WiredTiger specific errors.
	 */
	switch (error) {
	case WT_ROLLBACK:
		return ("WT_ROLLBACK: conflict between concurrent operations");
	case WT_DUPLICATE_KEY:
		return ("WT_DUPLICATE_KEY: attempt to insert an existing key");
	case WT_ERROR:
		return ("WT_ERROR: non-specific WiredTiger error");
	case WT_NOTFOUND:
		return ("WT_NOTFOUND: item not found");
	case WT_PANIC:
		return ("WT_PANIC: WiredTiger library panic");
	case WT_RESTART:
		return ("WT_RESTART: restart the operation (internal)");
	case WT_RUN_RECOVERY:
		return ("WT_RUN_RECOVERY: recovery must be run to continue");
	}

	/*
	 * POSIX errors are non-negative integers; check for 0 explicitly
	 * in-case the underlying strerror doesn't handle 0, some don't.
	 */
	if (error == 0)
		return ("Successful return: 0");
	if (error > 0 && (p = strerror(error)) != NULL)
		return (p);

	return (NULL);
}

const char* wiredtiger_strerror(int error)
{
	static char buf[128];
	return (__wt_strerror(NULL, error, buf, sizeof(buf)));
}

