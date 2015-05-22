
#ifndef ULLONG_MAX
#define	ULLONG_MAX	0xffffffffffffffffULL
#endif

#ifndef	LLONG_MAX
#define	LLONG_MAX	0x7fffffffffffffffLL
#endif

#ifndef	LLONG_MIN
#define	LLONG_MIN	(-0x7fffffffffffffffLL - 1)
#endif

#define	O_BINARY 	0

typedef pthread_cond_t		wt_cond_t;
typedef pthread_mutex_t		wt_mutex_t;
typedef pthread_t			wt_thread_t;

#define	WT_THREAD_CALLBACK(x)		void* (x)
#define	WT_THREAD_RET				void*
#define	WT_THREAD_RET_VALUE	NULL


#define WT_CDECL
