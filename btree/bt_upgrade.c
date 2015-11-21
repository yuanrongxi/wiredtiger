#include "wt_internal.h"

int __wt_upgrade(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_UNUSED(cfg);

	/* There's nothing to upgrade, yet. */
	WT_RET(__wt_progress(session, NULL, 1));
	return (0);
}


