#include "wt_internal.h"

int __wt_filename(WT_SESSION_IMPL *session, const char *name, char **path)
{
	return __wt_nfilename(session, name, strlen(name), path);
}

int __wt_nfilename(WT_SESSION_IMPL *session, const char *name, size_t namelen, char **path)
{
	WT_CONNECTION_IMPL *conn;
	size_t len;
	char *buf;

	conn = S2C(session);
	*path = NULL;

	if(__wt_absolute_path(name))
		WT_RET(__wt_strndup(session, name, namelen, path));
	else{
		len = strlen(conn->home) + 1 + namelen + 1;
		WT_RET(__wt_calloc(session, 1, len, &buf));
		snprintf(buf, len, "%s%s%.*s", conn->home, __wt_path_separator(), (int)namelen, name);
		*path = buf;
	}

	return 0;
}

/*删除一个存在的文件*/
int __wt_remove_if_exists(WT_SESSION_IMPL *session, const char *name)
{
	int exist;

	WT_RET(__wt_exist(session, name, &exist));
	if (exist)
		WT_RET(__wt_remove(session, name));

	return 0;
}

/*对fhp文件进行改名*/
int __wt_sync_and_rename_fh(WT_SESSION_IMPL *session, WT_FH **fhp, const char *from, const char *to)
{
	WT_DECL_RET;
	WT_FH *fh;

	fh = *fhp;
	*fhp = NULL;

	/*进行fsync将脏页落盘*/
	ret = __wt_fsync(session, fh);
	/*关闭文件*/
	WT_TRET(__wt_close(session, &fh));
	WT_RET(ret);

	/*进行文件改名*/
	WT_RET(__wt_rename(session, from, to));

	/*改名后fsync目录索引文件*/
	return __wt_directory_sync(session, NULL);
}

/*对fpp对应的文件改名*/
int __wt_sync_and_rename_fp(WT_SESSION_IMPL *session, FILE **fpp, const char *from, const char *to)
{
	FILE *fp;

	fp = *fpp;
	*fpp = NULL;

	WT_RET(__wt_fclose(&fp, WT_FHANDLE_WRITE));

	WT_RET(__wt_rename(session, from, to));

	return __wt_directory_sync(session, NULL);
}

