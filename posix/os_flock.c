#include "wt_internal.h"

/*file lock,锁定byte偏移位置的字节数据*/
int __wt_bytelock(WT_FH *fhp, wt_off_t byte, int lock)
{
	struct flock fl;
	WT_DECL_RET;

	fl.l_start = byte;
	fl.l_len = 1;
	fl.l_type = lock ? F_WRLCK : F_UNLCK;
	fl.l_whence = SEEK_SET;

	WT_SYSCALL_RETRY(fcntl(fhp->fd, F_SETLK, &fl), ret);

	return ret;
}


