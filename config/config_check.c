
#include "wt_internal.h"

static int config_check(WT_SESSION_IMPL *, const WT_CONFIG_CHECK *, u_int, const char *, size_t);

/*并将p插入到conn->foc中*/
static int __conn_foc_add(WT_SESSION_IMPL* session, const void* p)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	WT_RET(__wt_realloc_def(session, &conn->foc_size, conn->foc_cnt + 1, &conn->foc));
	conn->foc[conn->foc_cnt++] = (void *)p;

	return 0;
}

/*销毁conn->foc,包括里面的存有的内存对象p*/
void __wt_conn_foc_discard(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	size_t i;

	conn = S2C(session);

	for (i = 0; i < conn->foc_cnt; ++i)
		__wt_free(session, conn->foc[i]);

	__wt_free(session, conn->foc);
}

/*
 * __wt_configure_method --
 *	WT_CONNECTION.configure_method.
 */
int __wt_configure_method(WT_SESSION_IMPL *session, const char *method, const char *uri, const char *config, const char *type, const char *check)
{
	const WT_CONFIG_CHECK *cp;
	WT_CONFIG_CHECK *checks, *newcheck;
	const WT_CONFIG_ENTRY **epp;
	WT_CONFIG_ENTRY *entry;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	size_t cnt;
	char *newcheck_name, *p;

	WT_UNUSED(uri);

	conn = S2C(session);
	checks = newcheck = NULL;
	entry = NULL;
	newcheck_name = NULL;

	if (config == NULL)
		WT_RET_MSG(session, EINVAL, "no configuration specified");

	if (type == NULL)
		WT_RET_MSG(session, EINVAL, "no configuration type specified");

	if (strcmp(type, "boolean") != 0 && strcmp(type, "int") != 0 && strcmp(type, "list") != 0 && strcmp(type, "string") != 0)
		WT_RET_MSG(session, EINVAL, "type must be one of \"boolean\", \"int\", \"list\" or \"string\"");

	/*在conn->config enties中找到和method方法匹配的entry*/
	for (epp = conn->config_entries; (*epp)->method != NULL; ++epp)
		if (strcmp((*epp)->method, method) == 0)
			break;

	if ((*epp)->method == NULL)
		WT_RET_MSG(session, WT_NOTFOUND, "no method matching %s found", method);

	/*
	 * Technically possible for threads to race, lock the connection while
	 * adding the new configuration information.  We're holding the lock
	 * for an extended period of time, but configuration changes should be
	 * rare and only happen during startup.
	 */

	__wt_spin_lock(session, &conn->api_lock);

	/*创建一个entry，并将epp中的一部分内容赋值给entry*/
	WT_ERR(__wt_calloc_one(session, &entry));
	entry->method = (*epp)->method;

	WT_ERR(__wt_calloc_def(session, strlen((*epp)->base) + strlen(",") + strlen(config) + 1, &p));
	(void)strcpy(p, (*epp)->base);
	(void)strcat(p, ",");
	(void)strcat(p, config);
	entry->base = p;

	/*
	 * There may be a default value in the config argument passed in (for
	 * example, (kvs_parallelism=64").  The default value isn't part of the
	 * name, build a new one.
	 */
	WT_ERR(__wt_strdup(session, config, &newcheck_name));
	if ((p = strchr(newcheck_name, '=')) != NULL)
		*p = '\0';

	cnt = 0;
	if ((*epp)->checks != NULL){
		for (cp = (*epp)->checks; cp->name != NULL; ++cp)
			++cnt;
	}

	WT_ERR(__wt_calloc_def(session, cnt + 2, &checks));
	cnt = 0;
	if ((*epp)->checks != NULL){
		for (cp = (*epp)->checks; cp->name != NULL; ++cp){
			if (strcmp(newcheck_name, cp->name) != 0)
				checks[cnt++] = *cp;
		}
	}

	newcheck = &checks[cnt];
	newcheck->name = newcheck_name;
	WT_ERR(__wt_strdup(session, type, &newcheck->type));
	if (check != NULL)
		WT_ERR(__wt_strdup(session, check, &newcheck->checks));

	entry->checks = checks;
	entry->checks_entries = 0;

	WT_ERR(config_check(session, entry->checks, entry->checks_entries, config, 0));

	/*将开辟的内存放入foc中统一回收,连接关闭的时候回收？不能立即释放的，怕其他地方还在引用*/
	(void)__conn_foc_add(session, entry->base);
	(void)__conn_foc_add(session, entry);
	(void)__conn_foc_add(session, checks);
	(void)__conn_foc_add(session, newcheck->type);
	(void)__conn_foc_add(session, newcheck->checks);
	(void)__conn_foc_add(session, newcheck_name);

	/*触发一个写屏障，让entry设置到epp位置上，并在每个cpu的L1cache中生效*/
	WT_PUBLISH(*epp, entry);

	__wt_spin_unlock(session, &conn->api_lock);
	return ret;

err:		
	if (entry != NULL) {
		__wt_free(session, entry->base);
		__wt_free(session, entry);
	}

	__wt_free(session, checks);
	if (newcheck != NULL) {
		__wt_free(session, newcheck->type);
		__wt_free(session, newcheck->checks);
	}
	__wt_free(session, newcheck_name);


	__wt_spin_unlock(session, &conn->api_lock);

	return ret;
}

/**/
int __wt_config_check(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *entry, const char *config, size_t config_len)
{
	return (config == NULL || entry->checks == NULL ? 0 : config_check(session, entry->checks, entry->checks_entries, config, config_len));
}

/*通过str与checks数组中的check.name进行匹配，找到与str相同的check对象并返回其下标*/
static inline int config_check_search(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int entries, const char *str, size_t len, int *ip)
{
	u_int base, indx, limit;
	int cmp;

	if (entries == 0) {
		/*通过check name匹配到对应的config check并返回其对应在checks数组中的下标*/
		for (indx = 0; checks[indx].name != NULL; indx++){
			if (WT_STRING_MATCH(checks[indx].name, str, len)) {
				*ip = (int)indx;
				return 0;
			}
		}
	} 
	else{
		/*用二分查找法在checks中进行查找,checks一定是按name继续进行排序的*/
		for (base = 0, limit = entries; limit != 0; limit >>= 1) {
			indx = base + (limit >> 1);
			cmp = strncmp(checks[indx].name, str, len);
			if (cmp == 0 && checks[indx].name[len] == '\0') {
				*ip = (int)indx;
				return 0;
			}

			if (cmp < 0) {
				base = indx + 1;
				--limit;
			}
		}
	}

	WT_RET_MSG(session, EINVAL, "unknown configuration key: '%.*s'", (int)len, str);
}

static int config_check(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int checks_entries, const char *config, size_t config_len)
{
	WT_CONFIG parser, cparser, sparser;
	WT_CONFIG_ITEM k, v, ck, cv, dummy;
	WT_DECL_RET;
	int badtype, found, i;

	/*
	* The config_len parameter is optional, and allows passing in strings
	* that are not nul-terminated.
	*/
	if (config_len == 0)
		WT_RET(__wt_config_init(session, &parser, config));
	else
		WT_RET(__wt_config_initn(session, &parser, config, config_len));

	while ((ret = __wt_config_next(&parser, &k, &v)) == 0) {
		if (k.type != WT_CONFIG_ITEM_STRING && k.type != WT_CONFIG_ITEM_ID)
			WT_RET_MSG(session, EINVAL, "Invalid configuration key found: '%.*s'", (int)k.len, k.str);

		/* Search for a matching entry. */
		WT_RET(config_check_search(session, checks, checks_entries, k.str, k.len, &i));

		if (strcmp(checks[i].type, "boolean") == 0) {
			badtype = (v.type != WT_CONFIG_ITEM_BOOL &&(v.type != WT_CONFIG_ITEM_NUM || (v.val != 0 && v.val != 1)));
		} 
		else if (strcmp(checks[i].type, "category") == 0) {
			/* Deal with categories of the form: XXX=(XXX=blah). */
			ret = config_check(session, checks[i].subconfigs, checks[i].subconfigs_entries, k.str + strlen(checks[i].name) + 1, v.len);
			if (ret != EINVAL)
				badtype = 0;
			else
				badtype = 1;
		} 
		else if (strcmp(checks[i].type, "format") == 0) {
			badtype = 0;
		} 
		else if (strcmp(checks[i].type, "int") == 0) {
			badtype = (v.type != WT_CONFIG_ITEM_NUM);
		} 
		else if (strcmp(checks[i].type, "list") == 0) {
			badtype = (v.len > 0 &&
				v.type != WT_CONFIG_ITEM_STRUCT);
		} 
		else if (strcmp(checks[i].type, "string") == 0) {
			badtype = 0;
		} 
		else
			WT_RET_MSG(session, EINVAL, "unknown configuration type: '%s'", checks[i].type);

		if (badtype)
			WT_RET_MSG(session, EINVAL, "Invalid value for key '%.*s': expected a %s", (int)k.len, k.str, checks[i].type);

		if (checks[i].checkf != NULL)
			WT_RET(checks[i].checkf(session, &v));

		if (checks[i].checks == NULL)
			continue;

		/* Setup an iterator for the check string. */
		WT_RET(__wt_config_init(session, &cparser, checks[i].checks));
		while ((ret = __wt_config_next(&cparser, &ck, &cv)) == 0) {
			if (WT_STRING_MATCH("min", ck.str, ck.len)) {
				if (v.val < cv.val){
					WT_RET_MSG(session, EINVAL, "Value too small for key '%.*s' the minimum is %.*s", 
						(int)k.len, k.str, (int)cv.len, cv.str);
				}
			} 
			else if (WT_STRING_MATCH("max", ck.str, ck.len)) {
				if (v.val > cv.val){
					WT_RET_MSG(session, EINVAL, "Value too large for key '%.*s' the maximum is %.*s",
						(int)k.len, k.str, (int)cv.len, cv.str);
				} 
				else if (WT_STRING_MATCH("choices", ck.str, ck.len)) {
					if (v.len == 0)
						WT_RET_MSG(session, EINVAL, "Key '%.*s' requires a value", (int)k.len, k.str);

					if (v.type == WT_CONFIG_ITEM_STRUCT) {
						found = 1;
						WT_RET(__wt_config_subinit(session, &sparser, &v));
						while (found && (ret = __wt_config_next(&sparser, &v, &dummy)) == 0) {
							ret = __wt_config_subgetraw(session, &cv, &v, &dummy);
							found = (ret == 0);
						}
					} 
					else{
						ret = __wt_config_subgetraw(session, &cv, &v, &dummy);
						found = (ret == 0);
					}

					if (ret != 0 && ret != WT_NOTFOUND)
						return (ret);

					if (!found)
						WT_RET_MSG(session, EINVAL, "Value '%.*s' not a permitted choice for key '%.*s'",
						(int)v.len, v.str, (int)k.len, k.str);
				} 
				else
					WT_RET_MSG(session, EINVAL, "unexpected configuration description keyword %.*s", (int)ck.len, ck.str);
			}
		}
	}

	if (ret == WT_NOTFOUND)
		ret = 0;

	return (ret);
}



