#include <stdlib.h>
#include "wt_internal.h"


#define	FNV1A_64_INIT ((uint64_t)0xcbf29ce484222325ULL)


static inline uint64_t fnv_64a_buf(const void* buf, size_t len, uint64_t hval)
{
	const unsigned char* bp = buf;
	const unsigned char* be = bp + len;

	while(bp < be){
		hval ^= (uint64_t)*bp++;

		hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
	}

	return hval;
}

/*hash函数，返回一个uint64_t*/
uint64_t __wt_hash_fnv64(const void *string, size_t len)
{
	return fnv_64a_buf(string, len, FNV1A_64_INIT);
}

