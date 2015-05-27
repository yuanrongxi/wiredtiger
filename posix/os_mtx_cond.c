/**************************************************************************
*线程信号量封装
**************************************************************************/

#include "wt_internal.h"

/*创建一个信号量*/
int __wt_cond_alloc(WT_SESSION_IMPL *session, const char *name, int is_signalled, WT_CONDVAR **condp)
{
	WT_CONDVAR *cond;
	WT_DECL_RET;

	WT_RET(__wt_calloc_one(session, &cond));
	/*对cond的初始化*/
	WT_ERR(pthread_mutex_init(&cond->mtx, NULL));
	WT_ERR(pthread_cond_init(&cond->cond, NULL));

	cond->name = name;
	cond->waiters = is_signalled ? -1 : 0;

	*condp = cond;

	return 0;

err:
	__wt_free(session, cond);
	return ret;
}

/*等待信号触发*/
int __wt_cond_wait(WT_SESSION_IMPL *session, WT_CONDVAR *cond, uint64_t usecs)
{
	struct timespec ts;
	WT_DECL_RET;
	int locked;

	locked = 0;

	/*信号的等待者计数器原子性+１,如果+1之前WAITERS = -1，表示信号已经触发*/
	if (WT_ATOMIC_ADD4(cond->waiters, 1) == 0)
		return (0);

	if (session != NULL) {
		WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "wait %s cond (%p)", cond->name, cond));
		WT_STAT_FAST_CONN_INCR(session, cond_wait);
	}

	/*先获得cond->mtx*/
	WT_ERR(pthread_mutex_lock(&cond->mtx));
	locked = 1;

	if (usecs > 0) {
		WT_ERR(__wt_epoch(session, &ts));
		ts.tv_sec += (time_t) (((uint64_t)ts.tv_nsec + 1000 * usecs) / WT_BILLION);
		ts.tv_nsec = (long)(((uint64_t)ts.tv_nsec + 1000 * usecs) % WT_BILLION);
		ret = pthread_cond_timedwait(&cond->cond, &cond->mtx, &ts);
	} 
	else
		ret = pthread_cond_wait(&cond->cond, &cond->mtx);

	if (ret == EINTR || ret == ETIMEDOUT)
		ret = 0;
	
	/*原子性减少一个等待者*/
	WT_ATOMIC_SUB4(cond->waiters, 1);

err:
	if (locked)
		WT_TRET(pthread_mutex_unlock(&cond->mtx));

	if(ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "pthread_cond_wait");
}

int __wt_cond_signal(WT_SESSION_IMPL *session, WT_CONDVAR *cond)
{
	WT_DECL_RET;
	int locked = 0;

	if(session != NULL)
		WT_RET(__wt_verbose(session, WT_VERB_MUTEX, "signal %s cond (%p)", cond->name, cond));

	/*检查信号已经signal*/
	if(cond->waiters == -1)
		return 0;

	/*有等待者或者waiters不为0，有可能是-1,在此过程中，如果waiters=0，表示有个waiters正获得了信号*/
	if (cond->waiters > 0 || !WT_ATOMIC_CAS4(cond->waiters, 0, -1)) {
		WT_ERR(pthread_mutex_lock(&cond->mtx));
		locked = 1;
		WT_ERR(pthread_cond_broadcast(&cond->cond));
	}

err:	
	if (locked)
		WT_TRET(pthread_mutex_unlock(&cond->mtx));

	if (ret == 0)
		return (0);

	WT_RET_MSG(session, ret, "pthread_cond_broadcast");
}

int __wt_cond_destroy(WT_SESSION_IMPL *session, WT_CONDVAR **condp)
{
	WT_CONDVAR* cond;
	WT_DECL_RET;

	cond = *condp;
	if(cond == NULL)
		return 0;

	/*撤销信号*/
	ret = pthread_cond_destroy(&cond->cond);
	WT_TRET(pthread_mutex_destroy(&cond->mtx));

	__wt_free(session, *condp);

	return ret;
}

