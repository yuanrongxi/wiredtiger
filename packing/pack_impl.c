/**************************************************************************
*struct pack/unpack在wiredtiger内部使用的函数封装
**************************************************************************/

#include "wt_internal.h"

/*检查packing fmt格式串是否是正常的*/
int __wt_struct_check(WT_SESSION_IMPL *session, const char *fmt, size_t len, int *fixedp, uint32_t *fixed_lenp)
{
	WT_DECL_PACK_VALUE(pv);
	WT_DECL_RET;
	WT_PACK pack;
	int fields;

	/*检查fmt的合法性*/
	WT_RET(__pack_initn(session, &pack, fmt, len));
	for (fields = 0; (ret = __pack_next(&pack, &pv)) == 0; fields++)
		;

	if(ret != WT_NOTFOUND)
		return ret;

	if(fixedp != NULL && fixed_lenp != NULL){ 
		if (fields == 0) { /*只有一个项*/
			*fixedp = 1;
			*fixed_lenp = 0;
		} 
		else if (fields == 1 && pv.type == 't'){ /*多项，但是重复*/
			*fixedp = 1;
			*fixed_lenp = pv.size;
		} 
		else /*多项，不重复*/
			*fixedp = 0;
	}

	return 0;
}

int __wt_struct_confchk(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *v)
{
	return __wt_struct_check(session, v->str, v->len, NULL, NULL);
}

/*内部使用的struct size pack函数封装*/
int __wt_struct_size(WT_SESSION_IMPL *session, size_t *sizep, const char *fmt, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __wt_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return (ret);
}

/*内部使用的struct pack函数封装*/
int __wt_struct_pack(WT_SESSION_IMPL *session,
	void *buffer, size_t size, const char *fmt, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __wt_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*内部使用的struct unpack函数封装*/
int __wt_struct_unpack(WT_SESSION_IMPL *session,
	const void *buffer, size_t size, const char *fmt, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __wt_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}








