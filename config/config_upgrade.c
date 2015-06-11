
#include "wt_internal.h"

/*从新读取(lsm merge)一个配置,如果配置有追加项的话*/
int __wt_config_upgrade(WT_SESSION_IMPL *session, WT_ITEM *buf)
{
	WT_CONFIG_ITEM v;
	const char *config;

	config = (const char*)(buf->data);

	if(__wt_config_getones(session, config, "lsm merge", &v) != WT_NOTFOUND){
		WT_RET(__wt_buf_catfmt(session, buf, ",lsm_manager=(merge=%s)", v.val ? "true" : "false"));
	}

	return 0;
}


