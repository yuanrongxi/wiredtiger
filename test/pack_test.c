/****************************************************************************
*对intpack过程的测试
****************************************************************************/

#include <assert.h>
#include "wt_internal.h"

WT_SESSION_IMPL* session = NULL;
WT_CONNECTION_IMPL* conn = NULL;

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

const uint8_t data[16] ={
	0x01, 0x02, 0x03, 0x04,
	0x01, 0x02, 0x03, 0x04,
	0x01, 0x02, 0x03, 0x04,
	0x01, 0x02, 0x03, 0x04,
};

/*对一条logrec的pack/unpack测试*/
void test_pack_log()
{
	WT_ITEM* logrec;
	
	WT_ITEM item;
	memset(&item, 0, sizeof(WT_ITEM));
	FILE* fd = NULL;
	const uint8_t* p;

	__wt_fopen(session, "/home/1.txt", WT_FHANDLE_WRITE, 0, &fd);
	//fd = fopen("/home/1.txt", "w");

	__wt_buf_init(session, &item, sizeof(data) + 1);
	assert(__wt_logrec_alloc(session, 128, &logrec) == 0);
	/*写入一个二进制串对应的HEX字符串*/
	__wt_raw_to_hex(session, data, 16, &item);

	__wt_logop_col_put_pack(session, logrec, 1, 1200, &item);
	/*跳过WT_LOG_RECORD头*/
	p = LOG_SKIP_HEADER(logrec->data);
	__wt_txn_op_printlog(session, &p, logrec->data + logrec->size, fd);

	__wt_logrec_free(session, &logrec);
	__wt_buf_free(session, &item);

	__wt_fclose(&fd, WT_FHANDLE_WRITE);
}

void open_wt_session()
{
	WT_EVENT_HANDLER* handler = NULL;
	/*模拟创建一个session*/
	session = calloc(1, sizeof(WT_SESSION_IMPL));
	conn = calloc(2, sizeof(WT_CONNECTION_IMPL));
	session->iface.connection = (WT_CONNECTION *)conn;
	/*加载日志输出函数*/
	__wt_event_handler_set(session, handler);
}

void close_wt_session()
{
	free(session);
	free(conn);
}

int main()
{
	open_wt_session();

	//test_pack();
	//test_pack_unpack();
	//test_pack_struct();
	test_pack_log();

	close_wt_session();
}



