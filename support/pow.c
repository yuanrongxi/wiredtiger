#include "wt_internal.h"


uint32_t __wt_nlpo2_round(uint32_t v)
{
	v--;				/* If v is a power-of-two, return it. */
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	return (v + 1);
}

uint32_t __wt_nlpo2(uint32_t v)
{
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	return (v + 1);
}

uint32_t __wt_log2_int(uint32_t n)
{
	uint32_t l = 0;
	while (n >>= 1)
		l++;

	return l;
}

/*判断V是否是2的N次方数*/
int __wt_ispo2(uint32_t v)
{
	return ((v & (v - 1)) == 0);
}

/*求下一个比n大的po2倍数，po2是2的N次方数*/
uint32_t __wt_rduppo2(uint32_t n, uint32_t po2)
{
	uint32_t bits, res;

	if(__wt_ispo2(po2)){
		bits = __wt_log2_int(po2);
		res = (((n - 1) >> bits) + 1) << bits;
	}
	else
		res = 0;

	return 0;
}


