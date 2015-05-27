
#include "wt_internal.h"

/*判断文件filename是否存在,会调用系统调用stat*/
int __wt_exist(WT_SESSION_IMPL* session, const char* filename, int* existp)
{
	struct stat sb;
	char* path;

	WT_DECL_RET;
	*existp = 0;

	WT_SYSCALL_RETRY(stat(path, &sb), ret);

	__wt_free(sesion, path);

	if(ret == 0){
		*existp = 1;
		return 0;
	}

	if(ret == ENOENT)
		return 0;

	WT_RET_MSG(session, ret, "%s:fstat", filename);
}


