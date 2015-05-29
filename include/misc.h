/*************************************************************************
*定义一些常用的宏
*************************************************************************/

/*静默编译时一些没有使用的变量或者函数警告*/
#define WT_UNUSED(var)		(void)(var)

/*一些基本的数量级常量*/
#define	WT_MILLION	(1000000)
#define	WT_BILLION	(1000000000)

#define	WT_KILOBYTE	(1024)
#define	WT_MEGABYTE	(1048576)
#define	WT_GIGABYTE	(1073741824)
#define	WT_TERABYTE	((uint64_t)1099511627776)
#define	WT_PETABYTE	((uint64_t)1125899906842624)
#define	WT_EXABYTE	((uint64_t)1152921504606846976)


/*关于size的一些宏*/
#define WT_STORE_SIZE(s)				((uint32_t)(s))
/*计算两个指针的距离偏移量*/
#define WT_PTRDIFF(end, begin)			((size_t)((uint8_t *)(end) - (uint8_t *)(begin)))
#define WT_PTRDIFF32(end, begin)		WT_STORE_SIZE(WT_PTRDIFF(end, begin))

/*检查P缓冲区是否在begin缓冲区之中*/
#define WT_BLOCK_FITS(p, len, begin, maxlen)	\
	((uint8_t *)(p) >= (uint8_t *)(begin) && ((uint8_t *)(p) + (len) <= (uint8_t *)(begin) + (maxlen)))

#define WT_PTR_IN_RANGE(p, begin, maxlen) WT_BLOCK_FITS((p), 1, (begin), (maxlen))

/*n按照v对齐时的值，例如n = 17, v = 8，那么对齐值就是24*/
#define WT_ALIGN(n, v) ((((uintmax_t)(n)) + ((v) - 1)) & ~(((uintmax_t)(v)) - 1))

/*min, max宏*/
#define WT_MIN(a, b)		((a) < (b) ? (a) : (b))
#define WT_MAX(a, b)		((a) < (b) ? (b) : (a))

/* Elements in an array. */
#define	WT_ELEMENTS(a)		(sizeof(a) / sizeof(a[0]))

/*skip lists的参数*/
#define	WT_SKIP_MAXDEPTH	10
#define	WT_SKIP_PROBABILITY	(UINT32_MAX >> 2)

/*关于内存分配的宏封装*/
#define	__wt_calloc_def(session, number, addr)			\
	__wt_calloc(session, (size_t)(number), sizeof(**(addr)), addr)

#define	__wt_calloc_one(session, addr)					\
	__wt_calloc(session, (size_t)1, sizeof(**(addr)), addr)

#define	__wt_realloc_def(session, sizep, number, addr)	\
	(((number) * sizeof(**(addr)) <= *(sizep)) ? 0 :	\
	__wt_realloc(session, sizep, WT_MAX(*(sizep) * 2, WT_MAX(10, (number)) * sizeof(**(addr))), addr))

#define	__wt_free(session, p) do {						\
	if ((p) != NULL)									\
		__wt_free_int(session, (void *)&(p));			\
} while (0)

#define	__wt_overwrite_and_free(session, p)				__wt_free(session, p)
#define	__wt_overwrite_and_free_len(session, p, len)	__wt_free(session, p)


/*关于位操作封装宏*/
#define	F_CLR(p, mask)			((p)->flags &= ~((uint32_t)(mask)))
#define	F_ISSET(p, mask)		((p)->flags & ((uint32_t)(mask)))
#define	F_SET(p, mask)			((p)->flags |= ((uint32_t)(mask)))

#define	LF_CLR(mask)			((flags) &= ~((uint32_t)(mask)))
#define	LF_ISSET(mask)			((flags) & ((uint32_t)(mask)))
#define	LF_SET(mask)			((flags) |= ((uint32_t)(mask)))

#define	FLD_CLR(field, mask)	((field) &= ~((uint32_t)(mask)))
#define	FLD_ISSET(field, mask)	((field) & ((uint32_t)(mask)))
#define	FLD_SET(field, mask)	((field) |= ((uint32_t)(mask)))

#define	WT_VERBOSE_ISSET(session, f)	0

#define WT_CLEAR(s)				memset(&(s), 0, sizeof(s))

/*对str进行pfx前缀格式判定*/
#define	WT_PREFIX_MATCH(str, pfx)								\
	(((const char *)str)[0] == ((const char *)pfx)[0] && strncmp((str), (pfx), strlen(pfx)) == 0)

#define	WT_PREFIX_MATCH_LEN(str, len, pfx)	((len) >= strlen(pfx) && WT_PREFIX_MATCH(str, pfx))

/*跳过pfx返回字符串str剩下的内容*/
#define	WT_PREFIX_SKIP(str, pfx)			(WT_PREFIX_MATCH(str, pfx) ? ((str) += strlen(pfx), 1) : 0)

/*判断两个字符串是否相等*/
#define WT_STREQ(s, cs)						(sizeof(cs) == 2 ? (s)[0] == (cs)[0] && (s)[1] == '\0' : strcmp(s, cs) == 0)

/*检查str的长度为len并且内容和bytes一致*/
#define	WT_STRING_MATCH(str, bytes, len)						\
	(((const char *)str)[0] == ((const char *)bytes)[0] && strncmp(str, bytes, len) == 0 && (str)[(len)] == '\0')


#define	WT_UNCHECKED_STRING(str) #str

#define	WT_DECL_ITEM(i)			WT_ITEM *i = NULL
#define	WT_DECL_RET				int ret = 0

/*判断一个item的data是否是在他的memory chunk上分配的*/
#define	WT_DATA_IN_ITEM(i)						\
	((i)->mem != NULL && (i)->data >= (i)->mem && WT_PTRDIFF((i)->data, (i)->mem) < (i)->memsize)

/*将src的data赋值给dst的data,是指针赋值，不是内容拷贝,WT_TIEM操作*/
#define	WT_ITEM_SET(dst, src) do {				\
	(dst).data = (src).data;					\
	(dst).size = (src).size;					\
} while (0)


#define	__wt_scr_alloc(session, size, scratchp)			__wt_scr_alloc_func(session, size, scratchp)
#define	__wt_page_in(session, ref, flags)				__wt_page_in_func(session, ref, flags)
#define	__wt_page_swap(session, held, want, flags)		__wt_page_swap_func(session, held, want, flags)

/************************************************************************/

