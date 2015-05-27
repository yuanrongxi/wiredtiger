#include "wt_internal.h"

/*判断进程执行权限*/
int __wt_has_priv(void)
{
	return (getuid() != geteuid() || getgid() != getegid());
}



