/****************************************************************************
*对intpack过程的测试
****************************************************************************/

#include <assert.h>
#include "wt_internal.h"

void test_pack()
{
	uint8_t buf[10], *p, *end;
	int64_t i;

	for(i = 1; i < (1LL << 60); i <<= 1){
		end = buf;

		/*无符号shu*/
		assert(__wt_vpack_uint(&end, sizeof(buf), (uint64_t)i) == 0);
		printf("%" PRId64 " ", i);

		for (p = buf; p < end; p++)
			printf("%02x", *p);

		printf("\n");

		end = buf;
		assert(__wt_vpack_int(&end, sizeof(buf), -i) == 0);
		printf("%" PRId64 " ", -i);
		for (p = buf; p < end; p++)
			printf("%02x", *p);

		printf("\n");
	}
}


int main()
{
	test_pack();
}



