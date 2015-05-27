#include "wt_internal.h"


static void __remove_file_check(WT_SESSION_IMPL* session, const char* name)
{
	WT_UNUSED(session);
	WT_UNUSED(name);
}

/*调用系统remove进行文件删除*/
int __wt_remove(WT_SESSION_IMPL *session, const char *name)
{
	WT_DECL_RET;
	char* path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: remove", name));

	__remove_file_check(session, name, &path);

	WT_SYSCALL_RETRY(remove(path), ret);

	__wt_free(session, path);

	if(ret == 0 || ret == ENOENT)
		return 0;

	WT_RET_MSG(session, ret, "%s: remove", name);
}



