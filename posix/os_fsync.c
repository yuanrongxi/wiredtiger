#include "wt_internal.h"


static int __wt_handle_sync(int fd)
{
	WT_DECL_RET;

#if defined(HAVE_FDATASYNC)
	WT_SYSCALL_RETRY(fdatasync(fd), ret);
#else
	WT_SYSCALL_RETRY(fsync(fd), ret);
#endif

	return ret;
}

int __wt_directory_sync_fh(WT_SESSION_IMPL *session, WT_FH *fh)
{
	WT_DECL_RET;
	ret = __wt_handle_sync(fh->fd);
	if(ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "%s:fsync", fh->name);
}

/*对目录索引文件进行sync*/
int __wt_directory_sync(WT_SESSION_IMPL *session, char *path)
{
	WT_DECL_RET;
	int fd, tret;
	char *dir;

	/*
	 * POSIX 1003.1 does not require that fsync of a file handle ensures the
	 * entry in the directory containing the file has also reached disk (and
	 * there are historic Linux filesystems requiring this), do an explicit
	 * fsync on a file descriptor for the directory to be sure.
	 */
	if (path == NULL || (dir = strrchr(path, '/')) == NULL) {
		dir = NULL;
		path = (char *)S2C(session)->home;
	} 
	else
		*dir = '\0';

	WT_SYSCALL_RETRY(((fd = open(path, O_RDONLY, 0444)) == -1 ? 1 : 0), ret);
	if (dir != NULL)
		*dir = '/';

	if (ret != 0)
		WT_RET_MSG(session, ret, "%s: open", path);

	if ((ret = __wt_handle_sync(fd)) != 0)
		WT_ERR_MSG(session, ret, "%s: fsync", path);

err:	
	WT_SYSCALL_RETRY(close(fd), tret);
	/*
	if (tret != 0)
		__wt_err(session, tret, "%s: close", path);
		*/
	/*过滤数据库的特定错误*/
	WT_TRET(tret);

	return (ret);
}

int __wt_fsync(WT_SESSION_IMPL *session, WT_FH *fh)
{
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: fsync", fh->name));

	if ((ret = __wt_handle_sync(fh->fd)) == 0)
		return (0);
	WT_RET_MSG(session, ret, "%s fsync error", fh->name);
}

/*利用sync_file_range进行异步sync*/
int __wt_fsync_async(WT_SESSION_IMPL *session, WT_FH *fh)
{
	WT_DECL_RET;

	WT_RET(__wt_verbose(
		session, WT_VERB_FILEOPS, "%s: sync_file_range", fh->name));

	WT_SYSCALL_RETRY(sync_file_range(fh->fd, 0, 0, SYNC_FILE_RANGE_WRITE), ret);
	if (ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "%s: sync_file_range", fh->name);
}
