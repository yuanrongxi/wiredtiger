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

static void* r_fun(void* arg)
{
	int i = 0;
	uint32_t count = 0;
	while(i ++ < 10000){
		__wt_readlock(NULL, &rwlock);
		count = rw_count;
		__wt_readunlock(NULL, &rwlock);
	}

	return NULL;
}

static void* w_fun(void* arg)
{
	int i = 0;
	uint32_t count = 0;

	while(i ++ < 1000){
		__wt_writelock(NULL, &rwlock);
		rw_count ++;
		__wt_writeunlock(NULL, &rwlock);
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
		pthread_create(&rid[i], NULL, r_fun, NULL);
	}

	for(i = 0; i < 2; i++){
		pthread_create(&wid[i], NULL, w_fun, NULL);
	}

	for(i = 0; i < 2; i++)
		pthread_join(rid[i], NULL);

	for(i = 0; i < 2; i++)
		pthread_join(wid[i], NULL);

	printf("rw_count = %d\n", rw_count);
}

int main()
{
	//test_atomic();
	//test_packed_struct();
	//test_cas_lock();
	//test_wt_align();

	//test_rwlock_struct();
}

