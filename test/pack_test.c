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

void test_pack_unpack()
{
	const uint8_t *cp;
	uint8_t buf[10], *p;
	uint64_t ncalls, r, r2, s;
	int i;

	ncalls = 0;

	/*运行10亿次*/
	for(i = 0; i < 100000000; i ++){
		for (s = 0; s < 50; s += 5) {
			++ncalls;
			r = 1ULL << s;

			p = buf;
			/*将r pack到buf中*/
			assert(__wt_vpack_uint(&p, sizeof(buf), r) == 0);
			cp = buf;
			/*将buf中的数据unpack到r2*/
			assert(__wt_vunpack_uint(&cp, sizeof(buf), &r2) == 0);
			/*进行r和r2的校验*/
			if(r != r2){
				fprintf(stderr, "mismatch!\n");
				break;
			}
		}
	}

	printf("Number of calls: %llu\n", (unsigned long long)ncalls);
}

static size_t pack_struct(char* buf, size_t size, const char *fmt, ...)
{
	char *end, *p;
	va_list ap;
	size_t len;

	len = 0;

	/*编码一个格式化的struct占用的长度空间*/
	va_start(ap, fmt);
	assert(__wt_struct_sizev(NULL, &len, fmt, ap) == 0);
	va_end(ap);
	 
	assert(len < size);

	/*编码一个格式化的struct数据*/
	va_start(ap, fmt);
	assert(__wt_struct_packv(NULL, buf, size, fmt, ap) == 0);
	va_end(ap);

	printf("%s ", fmt);
	for (p = buf, end = p + len; p < end; p++)
		printf("%02x", *p & 0xff);

	printf("\n");

	return len;
}

static void unpack_struct(char* buf, size_t size, const char* fmt, ...)
{
	va_list ap;
	 
	va_start(ap, fmt);
	assert(__wt_struct_unpackv(NULL, buf, size, fmt, ap) == 0);
	va_end(ap);
}


void test_pack_struct()
{
	char buf[256] = {0};
	int i1, i2, i3;
	char* s;
	/*序列化编码测试*/
	size_t len = pack_struct(buf, 256, "iiS", 0, 101, "zerok");
	/*发序列化解码测试*/
	unpack_struct(buf, len, "iiS", &i1, &i2, &s);

	printf("i1 = %d, i2 = %d, i3 = %s\n", i1, i2, s);
}

int main()
{
	//test_pack();
	//test_pack_unpack();
	//test_pack_struct();
}



