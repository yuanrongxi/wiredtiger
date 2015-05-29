#include "wt_internal.h"

/*判断一个文件名是否带绝对路径,如果带绝对路径，返回1,否则返回0*/
int __wt_absolute_path(const char *path)
{
	return (path[0] == '/' ? 1 : 0);
}

/*获得目录分隔符*/
const char* __wt_path_separator()
{
	return ("/");
}


