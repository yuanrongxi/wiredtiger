/**************************************************************************
*对session对应的WT_EXT/WT_SIZE缓冲池的操作管理
**************************************************************************/

#include "wt_internal.h"

/*一般每个session会缓冲的一些重复利用的WT_EXT,防止内存重复分配和释放*/
typedef struct  
{
	WT_EXT*		ext_cache;			/*EXT handler的列表,其实就是一个对象缓冲池*/
	u_int		ext_cache_cnt;		/*EXT handler的个数*/
	WT_SIZE*	sz_cache;			/*WT SIZE handler的列表上*/
	u_int		sz_cache_cnt;		/*WT SIZE handler的个数*/
}WT_BLOCK_MGR_SESSION;

/*分配一个skip list所需的ext单元*/
static int __block_ext_alloc(WT_SESSION_IMPL* session, WT_EXT** extp)
{
	WT_EXT* ext;
	/*skip list深度*/
	u_int skipdepth;

	/*随机选择一个skip的深度值*/
	skipdepth = __wt_skip_choose_depth(session);
	/*分配WT_EXT调表单元,这里用了2倍的深度作为next数组的长度，可能是为了调表的无锁并发和copy on write??还需要确认*/
	WT_RET(__wt_calloc(session, 1, sizeof(WT_EXT) + skipdepth * 2 * sizeof(WT_EXT *), &ext));
	ext->depth = (uint8_t)skipdepth;
	(*extp) = ext;

	return 0;
}

int __wt_block_ext_alloc(WT_SESSION_IMPL *session, WT_EXT **extp)
{
	WT_EXT *ext;
	WT_BLOCK_MGR_SESSION *bms;
	u_int i;

	bms = (WT_BLOCK_MGR_SESSION *)session->block_manager;

	/*尝试从cache list获取一个可以重复利用的WT_EXT*/
	if(bms != NULL && bms->ext_cache != NULL){
		ext = bms->ext_cache;
		bms->ext_cache = ext->next[0];

		/*清空掉原来WT_EXT的调表关系*/
		for(i = 0; i < ext->depth; ++i){
			ext->next[i] = ext->next[i + ext->depth] = NULL;
		}

		/*对重用池中的计数器-1*/
		if (bms->ext_cache_cnt > 0)
			--bms->ext_cache_cnt;

		*extp = ext;

		return 0;
	}

	/*如果是session中没有可重用的WT_EXT,直接从内存中分配一个*/
	return (__block_ext_alloc(session, extp));
}

/*为一个session预分配一些WT_EXT对象到cache中*/
static int __block_ext_prealloc(WT_SESSION_IMPL* session, u_int max)
{
	WT_BLOCK_MGR_SESSION *bms;
	WT_EXT *ext;

	bms = (WT_BLOCK_MGR_SESSION *)session->block_manager;
	
	for(; bms->ext_cache_cnt < max; ++bms->ext_cache_cnt){
		WT_RET(__block_ext_alloc(session, &ext));
		/*将新分配的WT_EXT放入cache中,以便使用*/
		ext->next[0] = bms->ext_cache;
		bms->ext_cache = ext;
	}

	return 0;
}

/*释放一个WT_EXT,如果session没有设置cache机制，直接释放，如果有设置cache机制，将ext对象放入cache中*/
void __wt_block_ext_free(WT_SESSION_IMPL *session, WT_EXT *ext)
{
	WT_BLOCK_MGR_SESSION *bms;

	bms = (WT_BLOCK_MGR_SESSION *)session->block_manager;

	if (bms == NULL)
		__wt_free(session, ext);
	else {
		ext->next[0] = bms->ext_cache;
		bms->ext_cache = ext;
		++bms->ext_cache_cnt;
	}
}

/*释放session缓冲池中过多的WT_EXT,数量大于max为准.防止内存占用过多*/
static int __block_ext_discard(WT_SESSION_IMPL *session, u_int max)
{
	WT_BLOCK_MGR_SESSION *bms;
	WT_EXT *ext, *next;

	bms = (WT_BLOCK_MGR_SESSION*)session->block_manager;
	if (max != 0 && bms->ext_cache_cnt <= max)
		return (0);

	for (ext = bms->ext_cache; ext != NULL;) {
		next = ext->next[0];
		__wt_free(session, ext);
		ext = next;

		--bms->ext_cache_cnt;
		if (max != 0 && bms->ext_cache_cnt <= max)
			break;
	}
	bms->ext_cache = ext;

	if (max == 0 && bms->ext_cache_cnt != 0)
		WT_RET_MSG(session, WT_ERROR, "incorrect count in session handle's block manager cache");

	return 0;
}

/*在系统内存上分配WT_SIZE对象*/
static int __block_size_alloc(WT_SESSION_IMPL *session, WT_SIZE **szp)
{
	return __wt_calloc_one(session, szp);
}

/*分配一个WT_SIZE,如果session的缓冲池中有可利用的WT_SIZE从缓冲池中取，如果没有，从系统内存中分配*/
int __wt_block_size_alloc(WT_SESSION_IMPL *session, WT_SIZE **szp)
{
	WT_BLOCK_MGR_SESSION *bms;

	bms = (WT_BLOCK_MGR_SESSION*)session->block_manager;

	/* Return a WT_SIZE structure for use from a cached list. */
	if (bms != NULL && bms->sz_cache != NULL) {
		(*szp) = bms->sz_cache;
		bms->sz_cache = bms->sz_cache->next[0];

		/*
		 * The count is advisory to minimize our exposure to bugs, but
		 * don't let it go negative.
		 */
		if (bms->sz_cache_cnt > 0)
			--bms->sz_cache_cnt;
		return 0;
	}

	return __block_size_alloc(session, szp);
}

/*对session的WT_SIZE预分配，并将预分配的WT_SIZE对象放入缓冲池中*/
static int __block_size_prealloc(WT_SESSION_IMPL *session, u_int max)
{
	WT_BLOCK_MGR_SESSION *bms;
	WT_SIZE *sz;

	bms = (WT_BLOCK_MGR_SESSION*)session->block_manager;

	for (; bms->sz_cache_cnt < max; ++bms->sz_cache_cnt) {
		WT_RET(__block_size_alloc(session, &sz));

		sz->next[0] = bms->sz_cache;
		bms->sz_cache = sz;
	}

	return 0;
}

/*对一个使用完毕的WT_SIZE释放，如果session的缓冲池能缓冲对应，直接放入缓冲池中，否则直接释放*/
void __wt_block_size_free(WT_SESSION_IMPL *session, WT_SIZE *sz)
{
	WT_BLOCK_MGR_SESSION *bms;

	bms = (WT_BLOCK_MGR_SESSION *)session->block_manager;
	if (bms == NULL)
		__wt_free(session, sz);
	else {
		sz->next[0] = bms->sz_cache;
		bms->sz_cache = sz;

		++bms->sz_cache_cnt;
	}
}

/*释放缓冲池中过多的WT_SIZE对象，防止内存占用过多*/
static int __block_size_discard(WT_SESSION_IMPL *session, u_int max)
{
	WT_BLOCK_MGR_SESSION *bms;
	WT_SIZE *sz, *nsz;

	bms = (WT_BLOCK_MGR_SESSION*)session->block_manager;
	if (max != 0 && bms->sz_cache_cnt <= max)
		return (0);

	for (sz = bms->sz_cache; sz != NULL;) {
		nsz = sz->next[0];
		__wt_free(session, sz);
		sz = nsz;

		--bms->sz_cache_cnt;
		if (max != 0 && bms->sz_cache_cnt <= max)
			break;
	}
	bms->sz_cache = sz;

	if (max == 0 && bms->sz_cache_cnt != 0)
		WT_RET_MSG(session, WT_ERROR, "incorrect count in session handle's block manager cache");

	return 0;
}

/*对session的对象缓冲池做清空释放*/
static int __block_manager_session_cleanup(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;

	if (session->block_manager == NULL)
		return (0);

	WT_TRET(__block_ext_discard(session, 0));
	WT_TRET(__block_size_discard(session, 0));

	__wt_free(session, session->block_manager);

	return ret;
}

/*创建一个session的对象缓冲池，并为session的WT_SIZE/WT_EXT对象缓冲池预分配max个对象*/
int __wt_block_ext_prealloc(WT_SESSION_IMPL *session, u_int max)
{
	if (session->block_manager == NULL) {
		WT_RET(__wt_calloc(session, 1, sizeof(WT_BLOCK_MGR_SESSION), &session->block_manager));
		session->block_manager_cleanup =
			__block_manager_session_cleanup;
	}
	WT_RET(__block_ext_prealloc(session, max));
	WT_RET(__block_size_prealloc(session, max));

	return 0;
}

/*如果缓冲池中的对象过多，释放掉一些对象*/
int __wt_block_ext_discard(WT_SESSION_IMPL *session, u_int max)
{
	WT_RET(__block_ext_discard(session, max));
	WT_RET(__block_size_discard(session, max));

	return (0);
}

