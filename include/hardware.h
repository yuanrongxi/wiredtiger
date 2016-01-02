/********************************************************************
*原子操作的互斥等待定义等
********************************************************************/

/*在v设置成val之前，所有的CPU STORES操作必须完成*/
#define WT_PUBLISH(v, val) do{ \
	WT_WRITE_BARRIER(); \
	(v) = (val); \
}while(0)

/*在v=val之后，保证后面所有的读操作不会读到与val不一致的值*/
#define WT_ORDERED_READ(v, val) do{ \
	(v) = (val); \
	WT_READ_BARRIER(); \
}while(0)

/*对p的flags进行mask位过滤，并得到过滤后的值*/
#define	F_ISSET_ATOMIC(p, mask)	((p)->flags_atomic & (uint8_t)(mask))

/*对p的flags进行mask位设置，如果flags在load和store之间发生了改变，CAS1会再次load,直到CAS1成功为止,应用于多线程竞争状态*/
#define	F_SET_ATOMIC(p, mask) do {																\
	uint8_t __orig;																				\
	do {																						\
	__orig = (p)->flags_atomic;																	\
	}while(!WT_ATOMIC_CAS1((p)->flags_atomic, __orig, __orig | (uint8_t)(mask)));				\
} while (0)

/*对p的flags进行进行mask设置，如果flags的mask位有被其他操作设置了，直接将ret设置成EBUSY状态，一般应用于多线程竞争状态*/
#define F_CAS_ATOMIC(p, mask, ret) do{															\
	uint8_t __orig;																				\
	ret = 0;																					\
	do{																							\
		__orig = (p)->flags_atomic;																\
		if((__orig & (uint8_t)(mask)) != 0){													\
			ret = EBUSY;																		\
			break;																				\
		}																						\
	}while(!WT_ATOMIC_CAS1((p)->flags_atomic, __orig, __orig | (uint8_t)(mask)));				\
}while(0)

/*原子性取消p的flags的mask位,一般用于多线程竞争状态*/
#define	F_CLR_ATOMIC(p, mask)	do {															\
	uint8_t __orig;																				\
	do {																						\
		__orig = (p)->flags_atomic;																\
	} while (!WT_ATOMIC_CAS1((p)->flags_atomic,	__orig, __orig & ~(uint8_t)(mask)));			\
} while (0)

/* Cache line alignment, CPU L1 cache行单位*/
#define	WT_CACHE_LINE_ALIGNMENT	64	

/********************************************************************/
