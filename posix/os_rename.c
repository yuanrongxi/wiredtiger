
#include "wt_internal.h"

/*调用rename系统调用*/
int __wt_rename(WT_SESSION_IMPL *session, const char *from, const char *to)
{
	WT_DECL_RET;
	char *from_path, *to_path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "rename %s to %s", from, to));

	from_path = to_path = NULL;

	/*对路劲进行校验，如果不合法路径，直接返回*/
	WT_RET(__wt_filename(session, from, &from_path));
	WT_TRET(__wt_filename(session, to, &to_path));

	if (ret == 0)
		WT_SYSCALL_RETRY(rename(from_path, to_path), ret);

	__wt_free(session, from_path);
	__wt_free(session, to_path);

	if(ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "rename %s to %s", from, to);
}

