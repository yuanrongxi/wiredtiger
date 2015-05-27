/**************************************************************************
*线程封装
**************************************************************************/
#include "wt_internal.h"

/*创建一个线程*/
int __wt_thread_create(WT_SESSION_IMPL *session, wt_thread_t *tidret, WT_THREAD_CALLBACK(*func)(void *), void *arg)
{
	WT_DECL_RET;

	ret = pthread_create(tidret, NULL, func, arg);
	if(ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "pthread_create");
}

/*thread jion*/
int __wt_thread_join(WT_SESSION_IMPL *session, wt_thread_t tid)
{
	WT_DECL_RET;

	ret = pthread_join(tid, NULL);
	if(ret == 0)
		return 0;

	WT_RET_MSG(session, ret, "pthread_join");
}

/*获取线程ID，并格式化到buf中*/
void __wt_thread_id(char* buf, size_t buflen)
{
	pthread_t self;

	self = pthread_self();
	(void)snprintf(buf, buflen, "%" PRIu64 ":%p", (uint64_t)getpid(), (void *)self);
}

/*一次性初始化调用*/
int __wt_once(void (*init_routine)(void))
{
	static pthread_once_t once_control = PTHREAD_ONCE_INIT;

	return pthread_once(&once_control, init_routine);
}





