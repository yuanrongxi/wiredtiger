/***************************************************************************
*对文件打开和关闭系统调用的封装
***************************************************************************/
#include "wt_internal.h"

/*打开一个目录索引文件*/
static int __open_directory(WT_SESSION_IMPL* session, char* path, int* fd)
{
	WT_DECL_RET;
	char* dir;

	if((dir = strrchr(path, '/')) == NULL)
		path = (char *)".";
	else
		*dir = '\0'; /*去掉文件名，例如path = "/home/x.jpg",会被转化成/home*/

	/*打开目录索引文件*/
	WT_SYSCALL_RETRY(((*fd = open(path, O_RDONLY, 0444)) == -1 ? 1 : 0), ret);

	/*恢复文件名全路径*/
	if(dir != NULL)
		*dir = '/';
	
	if(ret != 0)
		WT_RET_MSG(session, ret, "%s: __open_directory", path);

	return ret;
}

int __wt_open(WT_SESSION_IMPL *session, const char *name, int ok_create, int exclusive, int dio_type, WT_FH **fhp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *fh, *tfh;
	mode_t mode;
	uint64_t bucket, hash;
	int direct_io, f, fd, matched;
	char *path;

	conn = S2C(session);
	direct_io = 0;
	fh = NULL;
	fd = -1;
	path = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: open", name));

	/* Increment the reference count if we already have the file open. */
	matched = 0;
	hash = __wt_hash_city64(name, strlen(name));
	bucket = hash % WT_HASH_ARRAY_SIZE;

	/*查找文件是否已经处于打开状态*/
	__wt_spin_lock(session, &conn->fh_lock);
	SLIST_FOREACH(tfh, &conn->fhhash[bucket], hashl) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = 1;
			break;
		}
	}
	__wt_spin_unlock(session, &conn->fh_lock);
	if (matched)
		return (0);

	/*确定打开的文件名，并打开对应目录索引文件*/
	WT_RET(__wt_filename(session, name, &path));

	if (dio_type == WT_FILE_TYPE_DIRECTORY) {
		WT_ERR(__open_directory(session, path, &fd));
		goto setupfh;
	}

	/*确定文件的打开模式*/
	f = O_RDWR;
#ifdef O_BINARY
	/* Windows clones: we always want to treat the file as a binary. */
	f |= O_BINARY;
#endif

#ifdef O_CLOEXEC
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.
	 */
	f |= O_CLOEXEC;
#endif
#ifdef O_NOATIME
	/* Avoid updating metadata for read-only workloads. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		f |= O_NOATIME;
#endif

	if (ok_create) {
		f |= O_CREAT;
		if (exclusive)
			f |= O_EXCL;
		mode = 0666;
	} else
		mode = 0;

#ifdef O_DIRECT
	if (dio_type && FLD_ISSET(conn->direct_io, dio_type)) {
		f |= O_DIRECT;
		direct_io = 1;
	}
#endif

	if (dio_type == WT_FILE_TYPE_LOG && FLD_ISSET(conn->txn_logsync, WT_LOG_DSYNC))
#ifdef O_DSYNC
		f |= O_DSYNC;
#elif defined(O_SYNC)
		f |= O_SYNC;
#else
		WT_ERR_MSG(session, ENOTSUP, "Unsupported log sync mode requested");
#endif

	/*进行文件打开*/
	WT_SYSCALL_RETRY(((fd = open(path, f, mode)) == -1 ? 1 : 0), ret);
	if (ret != 0)
		WT_ERR_MSG(session, ret, direct_io ?
		    "%s: open failed with direct I/O configured, some "
		    "filesystem types do not support direct I/O" : "%s", path);

setupfh:
#if defined(HAVE_FCNTL) && defined(FD_CLOEXEC) && !defined(O_CLOEXEC)
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.  There's an obvious
	 * race here, so we prefer the flag to open if available.
	 */
	if ((f = fcntl(fd, F_GETFD)) == -1 ||
	    fcntl(fd, F_SETFD, f | FD_CLOEXEC) == -1)
		WT_ERR_MSG(session, __wt_errno(), "%s: fcntl", name);
#endif

#if defined(HAVE_POSIX_FADVISE)
	/* Disable read-ahead on trees: it slows down random read workloads. */
	if (dio_type == WT_FILE_TYPE_DATA || dio_type == WT_FILE_TYPE_CHECKPOINT)
		WT_ERR(posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM));
#endif

	WT_ERR(__wt_calloc_one(session, &fh));
	WT_ERR(__wt_strdup(session, name, &fh->name));
	fh->name_hash = hash;
	fh->fd = fd;
	fh->ref = 1;
	fh->direct_io = direct_io;

	/* Set the file's size. 确定文件大小*/
	WT_ERR(__wt_filesize(session, fh, &fh->size));

	/* Configure file extension. */
	if (dio_type == WT_FILE_TYPE_DATA || dio_type == WT_FILE_TYPE_CHECKPOINT)
		fh->extend_len = conn->data_extend_len;

	/* Configure fallocate/posix_fallocate calls. */
	//TODO: __wt_fallocate_config(session, fh);

	/*
	 * Repeat the check for a match, but then link onto the database's list
	 * of files.再次确定文件是不是在已经打开的列表中,并修改引用计数
	 */
	matched = 0;
	__wt_spin_lock(session, &conn->fh_lock);
	SLIST_FOREACH(tfh, &conn->fhhash[bucket], hashl) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = 1;
			break;
		}
	}

	/*如果已打开文件列表中没有此文件，对统计信息进行更新*/
	if (!matched) {
		WT_CONN_FILE_INSERT(conn, fh, bucket);
		WT_STAT_FAST_CONN_INCR(session, file_open);

		*fhp = fh;
	}
	__wt_spin_unlock(session, &conn->fh_lock);

	/*如果文件已经处于打开状态，直接关闭本次打开，前面已经修改了引用计数，表示文件已有多个打开*/
	if (matched) {
err:		
		if (fh != NULL) {
			__wt_free(session, fh->name);
			__wt_free(session, fh);
		}
		if (fd != -1)
			(void)close(fd);
	}

	__wt_free(session, path);

	return ret;
}

int __wt_close(WT_SESSION_IMPL *session, WT_FH **fhp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *fh;
	uint64_t bucket;

	conn = S2C(session);

	if(*fhp == NULL)
		return 0;

	/*在关闭之前将fhp句柄赋值为NULL*/
	fh = *fhp;
	fhp = NULL;

	__wt_spin_lock(session, &conn->fh_lock);
	/*先进行计数器-1*/
	if(fh == NULL || fh->ref == 0 || --fh->ref > 0){
		__wt_spin_unlock(session, &conn->fh_lock);
		return 0;
	}

	/* 从已打开的文件列表中删除 */
	bucket = fh->name_hash % WT_HASH_ARRAY_SIZE;
	WT_CONN_FILE_REMOVE(conn, fh, bucket);
	WT_STAT_FAST_CONN_DECR(session, file_open);

	__wt_spin_unlock(session, &conn->fh_lock);

	/*关闭文件*/
	if(close(fh->fd) != 0){
		ret = __wt_errno();
		__wt_err(session, ret, "close:%s", fh->name);
	}

	__wt_free(session, fh->name);
	__wt_free(session, fh);

	return ret;
}

