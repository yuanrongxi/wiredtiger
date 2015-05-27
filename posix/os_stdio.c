#include "wt_internal.h"

/*打开文件name*/
int __wt_fopen(WT_SESSION_IMPL *session, const char *name, WT_FHANDLE_MODE mode_flag, u_int flags, FILE **fpp)
{
	WT_DECL_RET;
	const char *mode, *path;
	char *pathbuf;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: fopen", name));

	pathbuf = NULL;
	if (LF_ISSET(WT_FOPEN_FIXED))
		path = name;
	else {
		WT_RET(__wt_filename(session, name, &pathbuf));
		path = pathbuf;
	}

	mode = NULL;
	switch (mode_flag) {
	case WT_FHANDLE_APPEND:
		mode = WT_FOPEN_APPEND;
		break;
	case WT_FHANDLE_READ:
		mode = WT_FOPEN_READ;
		break;
	case WT_FHANDLE_WRITE:
		mode = WT_FOPEN_WRITE;
		break;
	}
	*fpp = fopen(path, mode);
	if (*fpp == NULL)
		ret = __wt_errno();

	if (pathbuf != NULL)
		__wt_free(session, pathbuf);

	if (ret == 0)
		return (0);
	WT_RET_MSG(session, ret, "%s: fopen", name);
}

/*格式化输出信息到文件中*/
int __wt_vfprintf(FILE *fp, const char *fmt, va_list ap)
{
	return (vfprintf(fp, fmt, ap) < 0 ? __wt_errno() : 0);
}

/*格式化输出信息到文件中*/
int __wt_fprintf(FILE *fp, const char *fmt, ...) WT_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __wt_vfprintf(fp, fmt, ap);
	va_end(ap);

	return (ret);
}

/*flush*/
int __wt_fflush(FILE *fp)
{
	return (fflush(fp) == 0 ? 0 : __wt_errno());
}

/*对文件进行关闭*/
int __wt_fclose(FILE **fpp, WT_FHANDLE_MODE mode_flag)
{
	FILE *fp;
	WT_DECL_RET;

	if (*fpp == NULL)
		return (0);

	fp = *fpp;
	*fpp = NULL;

	/*flush和fsync操作*/
	if (mode_flag == WT_FHANDLE_APPEND || mode_flag == WT_FHANDLE_WRITE) {
		ret = __wt_fflush(fp);
		if (fsync(fileno(fp)) != 0)
			WT_TRET(__wt_errno());
	}

	if(fclose(fp) != 0)
		WT_TRET(__wt_errno()); /*过滤并返回错误*/

	return ret;
}

