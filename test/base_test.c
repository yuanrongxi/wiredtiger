#include "wt_internal.h"

#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <pthread.h>

void test_atomic()
{
	int8_t v1 = 1;
	int16_t v2 = 1;

	int8_t o8 = WT_ATOMIC_STORE1(v1, 2);
	printf("old = %d, v1 = %d\n", o8, v1);

	__sync_lock_release(&v1);
	printf("v1 = %d\n", v1);

	if(WT_ATOMIC_CAS2(v2, 1, 9)){
		printf("WT_ATOMIC_CAS2 ok, v2 = %d \n", v2);
	}
	else{
		printf("WT_ATOMIC_CAS2 failed! v2 = %d\n", v2);
	}
}

void test_packed_struct()
{
	WT_PACKED_STRUCT_BEGIN(test_t)
		int x;
		int8_t y;
	WT_PACKED_STRUCT_END;

	struct test_t test;
	printf("sizeof(test_t) = %d\n", sizeof(test));
}

#define FIRST_BIT		0x01
#define SECOND_BIT		0x02

int first_count = 0;
int second_count = 0;

int lock_count = 0;

struct lock_flag_t
{
	uint8_t flags_atomic;
};

struct lock_flag_t* l = NULL;
#ifndef EBUSY
#define EBUSY 1
#endif

static WT_SPINLOCK lock;

static void* func(void* arg)
{
	int i = 0;
	int ret = 0;
	int count = 0;

	while(i < 1000000){
		/*具有lock的功能*/
		for(;;){
			F_CAS_ATOMIC(l, FIRST_BIT, ret);
			if(ret == 0)
				break;
		}
		first_count ++;
		F_CLR_ATOMIC(l, FIRST_BIT);

		/*不具有锁的功能，只是原子操作设置了SECOND_BIT位,*/
		F_SET_ATOMIC(l, SECOND_BIT);
		second_count = second_count + 1;
		F_CLR_ATOMIC(l, SECOND_BIT);

		/*自旋锁测试*/
		__wt_spin_lock(NULL, &lock);
		lock_count ++;
		__wt_spin_unlock(NULL, &lock);

		i ++;
	}
}

void test_rwlock_struct()
{
	WT_RWLOCK rwlock;
	rwlock.name = "test rwlock";
	wt_rwlock_t *lk;

	uint64_t old, new, pad, users, writers;

	rwlock.rwlock.u = 0;
	lk = &(rwlock.rwlock);

	pad = lk->s.pad;
	users = lk->s.users;
	writers = lk->s.writers;

	/*try read lock*/
	old = (pad << 48) + (users << 32) + (users << 16) + writers;
	new = (pad << 48) + ((users + 1) << 32) + ((users + 1) << 16) + writers;

	if(WT_ATOMIC_CAS_VAL8(lk->u, old, new) == old){
		printf("try rwlock ok!\n");
		printf("l->u =%" PRIu64 ",old = " PRIu64 ", new = %" PRIu64 "\n", lk->u, old, new);
		printf("l->pad = %d, l->user = %d, l->readers = %d, l->writers = %d\n", lk->s.pad, lk->s.users, lk->s.readers, lk->s.writers);
	}

	/*read unlock*/
	WT_ATOMIC_ADD2(lk->s.writers, 1);
	printf("l->u =%" PRIu64 ",old = " PRIu64 ", new = %" PRIu64 "\n", lk->u, old, new);
	printf("l->pad = %d, l->user = %d, l->readers = %d, l->writers = %d\n", lk->s.pad, lk->s.users, lk->s.readers, lk->s.writers);
}

/*测试原子操作和spin lock*/
void test_cas_lock()
{
	int i;
	pthread_t id[10];
	__wt_spin_init(NULL, &lock, "test spin lock");

	l = calloc(1, sizeof(struct lock_flag_t));

	for(i = 0; i < 2; i++){
		pthread_create(&id[i], NULL, func, NULL);
	}

	for(i = 0; i < 2; i++)
		pthread_join(id[i], NULL);

	printf("first = %d, second = %d, lock = %d\n", first_count, second_count, lock_count);

	__wt_spin_destroy(NULL, &lock);

	free(l);
}

void test_wt_align()
{
	int n, v;
	v = 8;
	n = 17;

	printf("%d\n", WT_ALIGN(n, v));
}

static uint32_t rw_count = 0;
WT_RWLOCK	rwlock;
WT_SESSION_IMPL* session = NULL;
WT_CONNECTION_IMPL* conn = NULL;

static void* r_fun(void* arg)
{
	int i = 0;
	uint32_t count = 0;
	while(i ++ < 10000){
		__wt_readlock(session, &rwlock);
		count = rw_count;
		__wt_readunlock(session, &rwlock);
	}

	return NULL;
}

static void* w_fun(void* arg)
{
	int i = 0;
	uint32_t count = 0;

	while(i ++ < 10000){
		__wt_writelock(session, &rwlock);
		rw_count ++;
		__wt_writeunlock(session, &rwlock);
	}

	return NULL;
}

void test_rw_lock()
{
	int i;
	pthread_t rid[10];
	pthread_t wid[10];


	rwlock.rwlock.u = 0;
	rwlock.name = "test rw lock";

	for(i = 0; i < 2; i++){
		__wt_thread_create(session, &rid[i],r_fun, NULL);
	}

	for(i = 0; i < 2; i++){
		__wt_thread_create(session, &wid[i], w_fun, NULL);
	}

	for(i = 0; i < 2; i++)
		__wt_thread_join(session, rid[i]);

	for(i = 0; i < 2; i++)
		__wt_thread_join(session, wid[i]);

	printf("rw_count = %d\n", rw_count);
}

void test_err_info()
{
	WT_DECL_RET;

	WT_PANIC_MSG(session, 0, "%s", "panic");
	__wt_object_unsupported(session, "http://yhd.com");

	WT_PANIC_ERR(session, 0, "error = %s", "error info");
err:
	return ;
}

void test_checksum()
{
	const char* ptr = "zerokkkkkdkfkddfjkdjkdfjkdfjfdkdfjdf";
	uint64_t crc = __wt_cksum(ptr, strlen(ptr));
	printf("crc = %d\n", crc);
}

struct test_body_s
{
	int	x;
	/*
	struct{
		struct test_body_s* sle_next;
	} q;
	*/
	SLIST_ENTRY(test_body_s) q;
};

typedef struct test_body_s test_body_t;

void test_list()
{
	test_body_t b;
	b.x = 12334;
	SLIST_HEAD(test_q_list, test_body_s) l;

	SLIST_INIT(&l);
	SLIST_INSERT_HEAD(&l, &b, q);
	test_body_t* v;
	SLIST_FOREACH(v, &l, q){
		printf("v = %d\n", v->x);
	}
}

void test_filename()
{
	const char* f1 = "/home/1.txt";
	const char* f2 = "/home/2.txt";
	int exist = 0;
	if(__wt_exist(session, f1, &exist) == 0 && exist == 1){
		printf("rename %s to %s\n", f1, f2);
		/*改文件名*/
		__wt_rename(session, f1, f2);
		/*删除文件*/
		__wt_remove(session, f2);
	}
}

void test_hex()
{
	const uint8_t data[16] ={
		0x01, 0x02, 0x03, 0x04,
		0x01, 0x02, 0x03, 0x04,
		0x01, 0x02, 0x03, 0x04,
		0x01, 0x02, 0x03, 0x04,
	};

	WT_ITEM item, it;
	memset(&item, 0, sizeof(WT_ITEM));
	memset(&it, 0, sizeof(WT_ITEM));

	__wt_buf_init(session, &item, sizeof(data) + 1);
	__wt_raw_to_hex(session, data, 16, &item);
	printf("%s\n", (char*)item.mem);

	__wt_buf_init(session, &item, item.memsize);
	__wt_raw_to_esc_hex(session, data, 16, &item);
	char* ptr = item.mem;
	printf("%s\n", ptr);

	__wt_buf_free(session, &item);
}

void test_random()
{
	uint64_t n = 0;
	int i;
	uint32_t seed[2];
	__wt_random_init(seed);
	for(i = 0; i < 100; i ++){
		n = __wt_random(seed);
		printf("n = %d\n", n);
	}
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
	__wt_cksum_init();

	open_wt_session();

	//test_atomic();
	//test_packed_struct();
	//test_cas_lock();
	//test_wt_align();
	//test_rwlock_struct();
	//test_rw_lock();

	//test_err_info();
	//test_checksum();
	//test_list();
	//test_filename();
	//test_random();
	test_hex();

	close_wt_session();
}

