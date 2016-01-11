
/*对session的cache read generation做自加*/
static inline void __wt_cache_read_gen_incr(WT_SESSION_IMPL* session)
{
	++S2C(session)->cache->read_gen;
}



