#include "wiredtiger.h"
#include "gcc.h"
#include "hardware.h"
#include "verify_build.h"

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

	if(WT_ATOMIC_CAS2(v2, 2, 9)){
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

struct lock_flag_t
{
	uint8_t flags_atomic;
};

struct lock_flag_t* l = NULL;
#define EBUSY 1

static int lock  = 0;
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

		/*不具有锁的功能，只是原子操作设置了SECOND_BIT位，不能保证内存指令执行顺序*/
		F_SET_ATOMIC(l, SECOND_BIT);
		second_count = second_count + 1;
		F_CLR_ATOMIC(l, SECOND_BIT);
		i ++;
	}
}

void test_cas_lock()
{
	int i;
	pthread_t id[10];

	l = calloc(1, sizeof(struct lock_flag_t));

	for(i = 0; i < 2; i++){
		pthread_create(&id[i], NULL, func, NULL);
	}

	for(i = 0; i < 2; i++)
		pthread_join(id[i], NULL);

	printf("first = %d, second = %d\n", first_count, second_count);

	free(l);
}

int main()
{
	test_atomic();
	test_packed_struct();
	test_cas_lock();
}

