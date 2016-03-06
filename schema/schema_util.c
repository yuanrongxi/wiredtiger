
#include "wt_internal.h"

/*通过name名字查找对应的data source，如果没有查找到，返回NULL*/
WT_DATA_SOURCE* __wt_schema_get_source(WT_SESSION_IMPL *session, const char *name)
{
	WT_NAMED_DATA_SOURCE *ndsrc;

	TAILQ_FOREACH(ndsrc, &S2C(session)->dsrcqh, q)
		if (WT_PREFIX_MATCH(name, ndsrc->prefix))
			return (ndsrc->dsrc);

	return NULL;
}

/*检查str中是否有和"WiredTiger"一样的命名，如果有，抛出一个错误信息*/
int __wt_str_name_check(WT_SESSION_IMPL* session, const char* str)
{
	const char *name, *sep;
	int skipped;

	name = str;
	for(skipped = 0; skipped < 2; skipped++){
		if ((sep = strchr(name, ':')) == NULL)
			break;

		name = sep + 1;
		if (WT_PREFIX_MATCH(name, "WiredTiger"))
			WT_RET_MSG(session, EINVAL, "%s: the \"WiredTiger\" name space may not be used by applications", name);
	}

	/*
	 * Disallow JSON quoting characters -- the config string parsing code
	 * supports quoted strings, but there's no good reason to use them in
	 * names and we're not going to do the testing.
	 * 也不允许json格式化的命名
	 */
	if (strpbrk(name, "{},:[]\\\"'") != NULL)
		WT_RET_MSG(session, EINVAL, "%s: WiredTiger objects should not include grouping characters in their names", name);

	return 0;
}

/*检查str中是否有和"WiredTiger"一样的命名，如果有，抛出一个错误信息,这个函数与上个函数的区别是指定了字符串的长度范围
 *也就是说可以进行一个长字符串中一部分字符串的比较。
 */
int __wt_name_check(WT_SESSION_IMPL *session, const char *str, size_t len)
{
	WT_DECL_RET;
	WT_DECL_ITEM(tmp);

	WT_RET(__wt_scr_alloc(session, len, &tmp));
	WT_ERR(__wt_buf_fmt(session, tmp, "%.*s", (int)len, str));

	ret = __wt_str_name_check(session, (const char*)tmp->data);

err:	__wt_scr_free(session, &tmp);
	return ret;
}

