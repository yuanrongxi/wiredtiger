#include "wt_internal.h"

/*配置合并，将cfg配置数组中的配置信息合并到config_ret字符串中，并进行返回*/
int __wt_config_concat(WT_SESSION_IMPL *session, const char **cfg, char **config_ret)
{
	WT_CONFIG cparser;
	WT_CONFIG_ITEM k, v;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	const char **cp;

	WT_RET(__wt_scr_alloc(session, 0, &tmp));

	for (cp = cfg; *cp != NULL; ++cp) {
		WT_ERR(__wt_config_init(session, &cparser, *cp));

		while ((ret = __wt_config_next(&cparser, &k, &v)) == 0) {
			if (k.type != WT_CONFIG_ITEM_STRING && k.type != WT_CONFIG_ITEM_ID)
				WT_ERR_MSG(session, EINVAL, "Invalid configuration key found: '%s'\n", k.str);

			/* Include the quotes around string keys/values. */
			if (k.type == WT_CONFIG_ITEM_STRING) {
				--k.str;
				k.len += 2;
			}

			if (v.type == WT_CONFIG_ITEM_STRING) {
				--v.str;
				v.len += 2;
			}

			/*进行key = value拼凑到tmp中*/
			WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s%s%.*s,", (int)k.len, k.str, (v.len > 0) ? "=" : "", (int)v.len, v.str));
		}

		if(ret != WT_NOTFOUND)
			goto err;
	}

	if (tmp->size != 0)
		--tmp->size;

	ret = __wt_strndup(session, tmp->data, tmp->size, config_ret);

err:
	__wt_scr_free(session, &tmp);
	return ret;
}




