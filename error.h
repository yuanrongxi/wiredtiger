/*****************************************************************
*定义一些错误处理宏
*
*****************************************************************/

#define	WT_DEBUG_POINT	((void *)0xdeadbeef)
#define	WT_DEBUG_BYTE	(0xab)

#define	WT_DIAGNOSTIC_YIELD

/*运行a函数，如果结果不为0，跳转到err处*/
#define WT_ERR(a) do{									\
	if((ret = (a)) != 0)								\
		goto err;										\
}while(0)												

/*错误信息输出，会先执行v函数，然后将执行结果输出到错误信息中*/
#define WT_ERR_MSG(session, v, ...)	do{					\
	ret = (v);											\
	_wt_err(session, ret, __VA_ARGS__);					\
	goto err;											\
}while(0)

/*执行a，如果a执行返回失败，会执行v,并结果赋值给ret,并跳转到err处*/
#define WT_ERR_TEST(a, v) do{							\
	if(a){												\
		ret = (v);										\
		goto err;										\
	}													\
	else												\
		ret = 0;										\
}while(0)

/*执行a，如果a执行返回失败，直接return a执行的结果*/
#define WT_RET(a) do{									\
	int __ret;											\
	if((__ret = (a)) != 0)								\
		return __ret;									\
}while(0)

/*执行v,并将执行结果输出到__wt_err中*/
#define	WT_RET_MSG(session, v, ...) do {				\
	int __ret = (v);									\
	__wt_err(session, __ret, __VA_ARGS__);				\
	return __ret;										\
} while (0)

/*执行a,如果a执行为true,返回v值*/
#define	WT_RET_TEST(a, v) do {							\
	if (a)												\
	return (v);											\					
}while(0)

/*执行a，返回值为EBUSY,继续向下执行*/
#define WT_RET_BUSY_OK(a) do {							\
	int __ret = (a);									\
	WT_RET_TEST(__ret != 0 && __ret != EBUSY, __ret);	\
}while(0)

/*执行a,返回值如果是WT_NOTFOUND,继续向下运行*/
#define WT_RET_NOTFOUND_OK(a) do{						\
	int __ret = (a);									\
	WT_RET_TEST(__ret != 0 && __ret != WT_NOTFOUND, __ret);		\
}while(0)

/*执行a,执行失败，如果ret为WT_PANIC/WT_DUPLICATE_KEY/WT_NOTFOUND,直接将执行结果赋值给ret*/
#define	WT_TRET(a) do {									\
	int __ret;											\
	if ((__ret = (a)) != 0 &&							\
	(__ret == WT_PANIC || ret == 0						\
	|| ret == WT_DUPLICATE_KEY || ret == WT_NOTFOUND))	\
	ret = __ret;										\
} while (0)

/*执行a,执行失败，返回值不为EBYSY,且ret为WT_PANIC/WT_DUPLICATE_KEY/WT_NOTFOUND/SUCC,直接将结果赋值给ret*/
#define	WT_TRET_BUSY_OK(a) do {							\
	int __ret;											\
	if ((__ret = (a)) != 0 && __ret != EBUSY &&			\
	(__ret == WT_PANIC ||								\
	ret == 0 || ret == WT_DUPLICATE_KEY || ret == WT_NOTFOUND))	\
	ret = __ret;										\
} while (0)

#define	WT_TRET_NOTFOUND_OK(a) do {						\
	int __ret;											\
	if ((__ret = (a)) != 0 && __ret != WT_NOTFOUND &&	\
	(__ret == WT_PANIC ||								\
	ret == 0 || ret == WT_DUPLICATE_KEY || ret == WT_NOTFOUND))	\
	ret = __ret;										\
} while (0)

/*default情况执行__wt_illegal_value，并返回执行结果*/
#define WT_ILLEGAL_VALUE(session)						\
	default:											\
		return __wt_illegal_value(session, NULL)	

/*default情况，执行__wt_illegal_value,如果执行错误，goto err处*/
#define WT_ILLEGAL_VALUE_ERR(session)					\
	default:											\
	WT_ERR(__wt_illegal_value(session, NULL))

/*default情况，执行__wt_illegal_value,将执行结果赋值给ret*/
#define	WT_ILLEGAL_VALUE_SET(session)					\
	default:											\
	ret = __wt_illegal_value(session, NULL);			\
	break

/*向session抛出一个错误消息，并用__wt_panic触发一个异常*/
#define	WT_PANIC_MSG(session, v, ...) do {				\
	__wt_err(session, v, __VA_ARGS__);					\
	(void)__wt_panic(session);							\
} while (0)

/*向session触发一个异常，并goto err处*/
#define	WT_PANIC_ERR(session, v, ...) do {				\
	WT_PANIC_MSG(session, v, __VA_ARGS__);				\
	WT_ERR(WT_PANIC);									\
} while (0)

/*向session触发一个异常，并return WT_PANIC值*/
#define	WT_PANIC_RET(session, v, ...) do {				\
	WT_PANIC_MSG(session, v, __VA_ARGS__);				\
	return (WT_PANIC);									\
} while (0)

#define	WT_ASSERT(session, exp)		WT_UNUSED(session)
