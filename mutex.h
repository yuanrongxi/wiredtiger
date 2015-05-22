
struct __wt_condvar
{
	const char*		name;
	wt_mutex_t		mtx;
	wt_cond_t		cond;

	int				waiters;
};

/*关于对读写锁的理解：
*如果writers = users, 且readers = users，表示可以同时s-lock也可以获得x-lock,锁处于free状态
*如果writers = users, readers != users, 设计上不存在
*如果readers = users, writes != users,表示rwlock不排斥read lock，可以获得s-lock,不能获得x-lock, 有线程正在占用s-lock
*如果readers != users, writes != users,表示有线程正在waiting lock,另外一个线程正在占用x-lock
*所有这些值的改变采用CAS操作，设计上很取巧
*/
typedef union
{
	uint64_t u;
	uint32_t us;			/*为了原子性的对writers和readers同时赋值,做到x-lock和s-lock的公平性*/
	struct{
		uint16_t writers;
		uint16_t readers;
		uint16_t users;
		uint16_t pad;
	} s;
}wt_rwlock_t;

struct __wt_rwlock
{
	const char*		name;
	wt_rwlock_t		rwlock;
};

#define	SPINLOCK_GCC					0
#define	SPINLOCK_PTHREAD_MUTEX			1
#define	SPINLOCK_PTHREAD_MUTEX_ADAPTIVE	2
#define	SPINLOCK_PTHREAD_MUTEX_LOGGING	3
#define	SPINLOCK_MSVC					4

/*定义spin lock的类型*/
typedef int  WT_SPINLOCK;
