#include "wt_internal.h"


void __wt_yield()
{
	sched_yield();
}

