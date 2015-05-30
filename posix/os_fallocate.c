#include "wt_internal.h"

#include <linux/falloc.h>
#include <sys/syscall.h>

void __wt_fallocate_config(WT_SESSION_IMPL *session, WT_FH *fh)
{
	WT_UNUSED(session);

	fh->fallocate_available = WT_FALLOCATE_NOT_AVAILABLE;
	fh->fallocate_requires_locking = 0;

	fh->fallocate_available = WT_FALLOCATE_AVAILABLE;
	fh->fallocate_requires_locking = 1;
}

/*为文件预留分配空间，从offset处预留len长度的空间,TODO:需要确定会不会自动增大文件?*/
static int __wt_std_fallocate(WT_FH* fh, wt_off_t offset, wt_off_t len)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(fallocate(fh->fd, FALLOC_FL_KEEP_SIZE, offset, len), ret);
	return ret;
}

/*调用内核函数做文件预留空间处理*/
static int __wt_sys_fallocate(WT_FH* fh, wt_off_t offset, wt_off_t len)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(syscall(SYS_fallocate, fh->fd, FALLOC_FL_KEEP_SIZE, offset, len), ret);
}

/*调用posix函数做文件空间预留,如果文件大小小于len + offset，会自动扩大到这个长度*/
static int __wt_posix_fallocate(WT_FH* fh, wt_off_t offset, wt_off_t len)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(posix_fallocate(fh->fd, offset, len), ret);
	return ret;
}

/*对文件进行空间预留的调用函数*/
int __wt_fallocate(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, wt_off_t len)
{
	WT_DECL_RET;

	switch(fh->fallocate_available){
	case WT_FALLOCATE_POSIX:
		WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: posix_fallocate", fh->name));
		if((ret = __wt_posix_fallocate(fh, offset, len)) == 0)
			return 0;

		WT_RET_MSG(session, ret, "%s: posix_fallocate", fh->name);

	case WT_FALLOCATE_STD:
		WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: fallocate", fh->name));
		if ((ret = __wt_std_fallocate(fh, offset, len)) == 0)
			return 0;

		WT_RET_MSG(session, ret, "%s: fallocate", fh->name);

	case WT_FALLOCATE_SYS:
		WT_RET(__wt_verbose(
			session, WT_VERB_FILEOPS, "%s: sys_fallocate", fh->name));
		if ((ret = __wt_sys_fallocate(fh, offset, len)) == 0)
			return 0;

		WT_RET_MSG(session, ret, "%s: sys_fallocate", fh->name);

	case WT_FALLOCATE_AVAILABLE:
		/* 先调用__wt_std_fallocate，如果失败再调用__wt_sys_fallocate， 如果失败最后__wt_posix_fallocate
		 * 如果全部失败，就会走到WT_FALLOCATE_NOT_AVAILABLE这个状态*/
		if ((ret = __wt_std_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = WT_FALLOCATE_STD;
			fh->fallocate_requires_locking = 0;
			return (0);
		}
		if ((ret = __wt_sys_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = WT_FALLOCATE_SYS;
			fh->fallocate_requires_locking = 0;
			return (0);
		}
		if ((ret = __wt_posix_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = WT_FALLOCATE_POSIX;
			fh->fallocate_requires_locking = 0;
			return (0);
		}

	case WT_FALLOCATE_NOT_AVAILABLE:
	default:
		fh->fallocate_available = WT_FALLOCATE_NOT_AVAILABLE;
		return ENOTSUP;
	}
}






