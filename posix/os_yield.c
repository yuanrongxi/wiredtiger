#include "wt_internal.h"


void __wt_yield(void)
{
	sched_yield();
}


