#include "wt_internal.h"

/*将字符串转化成对应的无符号uint64_t数，例如：“1234567” 可以转成1234567*/
uint64_t __wt_strtouq(const char *nptr, char **endptr, int base)
{
	return strtouq(nptr, endptr, base);
}

