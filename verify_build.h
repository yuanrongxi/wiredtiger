/*******************************************************************
*编译安全定义,主要是在编译期间提示错误ASSERT，防止运行时错误
*******************************************************************/

#undef ALIGN_CHECK
#undef SIZE_CHECK

/*如果cond == false,编译器会提示错误*/
#define	WT_STATIC_ASSERT(cond)	(void)sizeof(char[1 - 2 * !(cond)])

/*sizeof(type) != e时，编译器会提示出错*/
#define SIZE_CHECK(type, e) do{ \
	char __check_##type[1 - 2 * !(sizeof(type) == (e))];		\
	(void)__check##type;	\
}while(0)

/*对类型type进行2幂数对齐检查*/
#define ALIGN_CHECK(type, a) WT_STATIC_ASSERT(WT_ALIGN(sizeof(type), (a)) == sizeof(type))

static inline void __wt_verify_build(void)
{
	/*检查指定的结构体对齐*/
	//SIZE_CHECK(WT_BLOCK_DESC, WT_BLOCK_DESC_SIZE);
	//SIZE_CHECK(WT_REF, WT_REF_SIZE);

	/*64为操作系统检查,不支持其他位的操作系统*/
	WT_STATIC_ASSERT(sizeof(size_t) >= 8);
	WT_STATIC_ASSERT(sizeof(wt_off_t) == 8);
}

#undef ALIGN_CHECK
#undef SIZE_CHECK

/******************************************************************/



