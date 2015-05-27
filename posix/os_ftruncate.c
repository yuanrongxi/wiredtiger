#include "wt_internal.h"

/*将fh指定的文件大小调整成len大小*/
int __wt_ftruncate(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t len)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(ftruncate(fh->fd, len), ret);
	if (ret == 0) {
		fh->size = fh->extend_size = len;
		return (0);
	}

	WT_RET_MSG(session, ret, "%s ftruncate error", fh->name);
}


