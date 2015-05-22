

#define	WT_DECL_SPINLOCK_ID(i)

#define	__wt_spin_trylock(session, lock, idp)	__wt_spin_trylock_func(session, lock)

#ifndef WT_SPIN_COUNT
#define	WT_SPIN_COUNT 1000
#endif

static inline int __wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK* t, const char* name)
{
	WT_UNUSED(session);
	WT_UNUSED(name);

	*(t) = 0;
	return 0;
}

static inline int __wt_spin_trylock_func(WT_SESSION_IMPL* session, WT_SPINLOCK* t)
{
	WT_UNUSED(session);
	return (__sync_lock_test_and_set(t, 1) == 0 ? 0 : EBUSY);
}

static inline void __wt_spin_destroy(WT_SESSION_IMPL* session, WT_SPINLOCK* t)
{
	WT_UNUSED(session);
	*(t) = 0;
}

#ifndef __wt_yield
#define __wt_yield sched_yield
#endif

static inline void __wt_spin_lock(WT_SESSION_IMPL* session, WT_SPINLOCK* t)
{
	int i;

	WT_UNUSED(session);

	while(__sync_lock_test_and_set(t, 1)){
		for(i = 0; i < WT_SPIN_COUNT; i ++)
			WT_PAUSE();

		if(*t)
			__wt_yield();
	}
}

static inline void __wt_spin_unlock(WT_SESSION_IMPL* session, WT_SPINLOCK *t)
{
	WT_UNUSED(session);

	__sync_lock_release(t);
}

