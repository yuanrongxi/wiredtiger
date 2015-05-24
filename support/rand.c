#include "wt_internal.h"

/*定义random seed*/
#undef	M_W
#define	M_W	(rnd)[0]
#undef	M_Z
#define	M_Z	(rnd)[1]


/*初始化随机数*/
void __wt_random_init(uint32_t *rnd)
{
	M_W = 521288629;
	M_Z = 362436069;
}

/*产生随机数,这个是伪随机*/
uint32_t __wt_random(uint32_t *rnd)
{
	uint32_t w = M_W, z = M_Z;

	M_Z = z = 36969 * (z & 65535) + (z >> 16);
	M_W = w = 18000 * (w & 65535) + (w >> 16);
	return (z << 16) + (w & 65535);
}


