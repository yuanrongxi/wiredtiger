#include "wt_internal.h"

void __wt_abort(WT_SESSION_IMPL *session)
{
	__wt_errx(session, "aborting WiredTiger library");

	abort();
}


