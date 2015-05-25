/*************************************************************************
*wiredtiger的状态信息统计
**************************************************************************/

struct __wt_stats
{
	const char* *desc;					/*统计值描述*/
	uint64_t	v;						/*统计值*/
};



#define WT_STAT(stats, fld)						((stats)->fld.v)

/*原子性减*/
#define WT_STAT_ATOMIC_DECRV(stats, fld, value) do{					\
	(void)(WT_ATOMIC_SUB8(WT_STAT(stats, fld), (value)));			\
}while(0)
#define WT_STAT_ATOMIC_DECR(stats, fld) WT_STAT_ATOMIC_DECRV(stats, fld, 1)

/*原子性加*/
#define WT_STAT_ATOMIC_INCRV(stats, fld, value) do{					\
	(void)WT_ATOMIC_ADD8(WT_STAT(stats, fld), (value));				\
} while(0)
#define	WT_STAT_ATOMIC_INCR(stats, fld) WT_ATOMIC_ADD(WT_STAT(stats, fld), 1)

/*减，没有竞争保护*/
#define	WT_STAT_DECRV(stats, fld, value) do {						\
	(stats)->fld.v -= (value);										\
} while (0)
#define	WT_STAT_DECR(stats, fld) WT_STAT_DECRV(stats, fld, 1)

/*加，没有竞争保护*/
#define	WT_STAT_INCRV(stats, fld, value) do {						\
	(stats)->fld.v += (value);										\
} while (0)
#define	WT_STAT_INCR(stats, fld) WT_STAT_INCRV(stats, fld, 1)

#define	WT_STAT_SET(stats, fld, value) do {							\
	(stats)->fld.v = (uint64_t)(value);								\
} while (0)



