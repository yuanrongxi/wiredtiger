/*****************************************************************
*wt_rwlock_t的结构定义在mutex.h中
*对rw_lock的实现，原理如下：
*如果writers = users, 且readers = users，表示可以同时s-lock也可以获得x-lock,锁处于free状态
*如果writers = users, readers != users, 设计上不存在
*如果readers = users, writes != users,表示rwlock不排斥read lock，可以获得s-lock,不能获得x-lock, 有线程正在占用s-lock
*如果readers != users, writes != users,表示有线程正在waiting lock,另外一个线程正在占用x-lock
*所有这些值的改变采用CAS操作，设计上很取巧
*****************************************************************/

int __wt_rwlock_alloc(WT_SESSION_IMPL* session, WT_RWLOCK** rwlockp, const char* name)
{
	WT_RWLOCK* rwlock;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: alloc %s", name));
	WT_RET(__wt_calloc_one(session, &rwlock));

	rwlock->name = name;
	*rwlockp = rwlock;

	return 0;
}

int __wt_try_readlock(WT_SESSION_IMPL* session, WT_RWLOCK* rwlock)
{
	wt_rwlock_t* l;
	uint64_t old, new, pad, users, writers;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: try_readlock %s", rwlock->name));
	l = &rwlock->rwlock;
	pad = l->s.pad;
	users = l->s.users;
	writers = l->s.writers;

	/*只会判断readers是否和user是一致的，如果是一直的，表示可以获得s-lock*/
	old = (pad << 48) + (users << 32) + (users << 16) + writers;
	/*同时对reader和users + 1,因为是try lock,所以必须原子性的完成*/
	new = (pad << 48) + ((users + 1) << 32) + ((users + 1) << 16) + writers;

	return (WT_ATOMIC_CAS_VAL8(l->u, old, new) == old ? 0 : EBUSY);
}

int __wt_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *rwlock)
{
	wt_rwlock_t* l;
	uint64_t me;
	uint16_t val;
	int pause_cnt;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: readlock %s", rwlock->name));
	WT_STAT_FAST_CONN_INCR(session, rwlock_read);

	l = &rwlock->rwlock;
	/*对users + 1*/
	me = WT_ATOMIC_FETCH_ADD8(l->u, (uint64_t)1 << 32);
	/*获得user + 1之前的user的值*/
	val = (uint16_t)(me >> 32);

	/*判断+1之前的users值是否和readers一致，如果不一致，表示有x-lock排斥*/
	for(pause_cnt = 0; val != l->s.readers;){
		/*防止CPU过度消耗,尤其是在大量线程对rwlock竞争时。*/
		if(++pause_cnt < 1000)
			WT_PAUSE();
		else
			__wt_sleep(0, 10);
	}

	/*获得s-lock,将readers设置成和users一致，这样可以让其他的线程也可以获得s-lock*/
	++ l->s.readers ++;

	return 0;
}

int __wt_readunlock(WT_SESSION_IMPL* session, WT_RWLOCK* rwlock)
{
	wt_rwlock_t *l;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: read unlock %s", rwlock->name));

	/*让正在等待的x-lock获得锁权，这里*/
	l = &rwlock->rwlock;
	WT_ATOMIC_ADD2(l->s.writers, 1);
}

int __wt_try_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *rwlock)
{
	wt_rwlock_t *l;
	uint64_t old, new, pad, readers, users;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: try_writelock %s", rwlock->name));
	WT_STAT_FAST_CONN_INCR(session, rwlock_write);

	l = &rwlock->rwlock;
	pad = l->s.pad;
	readers = l->s.readers;
	users = l->s.users;

	/*只需要比较users和writers是否一致，如果一直，就可以获得x-lock*/
	old = (pad << 48) + (users << 32) + (readers << 16) + users;
	/*对users + 1,因为x-lock是排read和writer的，所以其他的不需要+1*/
	new = (pad << 48) + ((users + 1) << 32) + (readers << 16) + users;

	return (WT_ATOMIC_CAS_VAL8(l->u, old, new) == old ? 0 : EBUSY);
}

int __wt_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *rwlock)
{
	wt_rwlock_t *l;
	uint64_t me;
	uint16_t val;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: writelock %s", rwlock->name));
	WT_STAT_FAST_CONN_INCR(session, rwlock_write);

	l = &rwlock->rwlock;
	me = WT_ATOMIC_FETCH_ADD8(l->u, (uint64_t)1 << 32);
	val = (uint16_t)(me >> 32); /*读取+1之前的users*/
	/*如果+1之前的users和writers一致，可以获得x-lock*/
	while(val != l->s.writers)
		WT_PAUSE();

	return 0;
}

int __wt_writeunlock(WT_SESSION_IMPL *session, WT_RWLOCK *rwlock)
{
	wt_rwlock_t *l, copy;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: writeunlock %s", rwlock->name));

	/*原子性对readers和writers同时 + 1，使得等待锁的线程可以公平的得到锁*/
	l = &rwlock->rwlock;
	copy = *l;

	WT_BARRIER();

	++copy.s.writers;
	++copy.s.readers;

	/*原子性更新readers和writers*/
	l->us = copy.us;

	return 0;
}

int __wt_rwlock_destroy(WT_SESSION_IMPL *session, WT_RWLOCK **rwlockp)
{
	WT_RWLOCK *rwlock;

	rwlock = *rwlockp;		/* Clear our caller's reference. */
	if (rwlock == NULL)
		return (0);

	*rwlockp = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "rwlock: destroy %s", rwlock->name));

	__wt_free(session, rwlock);
	return (0);
}


/******************************************************************/
