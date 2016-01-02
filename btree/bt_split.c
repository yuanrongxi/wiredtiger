/************************************************************************
 * btree split实现
 ***********************************************************************/

#include "wt_internal.h"

/*内存计数交换*/
#define	WT_MEM_TRANSFER(from_decr, to_incr, len) do {			\
	size_t __len = (len);										\
	from_decr += __len;											\
	to_incr += __len;											\
} while (0)

/* 获得connection对应最早active状态的split generation */
static uint64_t __split_oldest_gen(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *s;
	uint64_t gen, oldest;
	u_int i, session_cnt;

	conn = S2C(session);
	/* 在这用内存屏障是防止读乱序 */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = 0, s = conn->sessions, oldest = conn->split_gen + 1; i < session_cnt; i++, s++){
		if (((gen = s->split_gen) != 0) && gen < oldest)
			oldest = gen;
	}

	return oldest;
}

/* 向session的split stash list中增加一个新的entry对象 */
static int __split_stash_add(WT_SESSION_IMPL* session, uint64_t split_gen, void* p, size_t len)
{
	WT_SPLIT_STASH *stash;

	WT_ASSERT(session, p != NULL);

	/*扩大split_stash_alloc*/
	WT_RET(__wt_realloc_def(session, &session->split_stash_alloc, session->split_stash_cnt + 1, &session->split_stash));

	stash = session->split_stash + session->split_stash_cnt++;
	stash->split_gen = split_gen;
	stash->p = p;
	stash->len = len;

	WT_STAT_FAST_CONN_ATOMIC_INCRV(session, rec_split_stashed_bytes, len);
	WT_STAT_FAST_CONN_ATOMIC_INCR(session, rec_split_stashed_objects);

	/*尝试释放split stash list中可以释放*/
	if(session->split_stash_cnt > 1)
		__wt_split_stash_discard(session);

	return 0;
}

/* 废弃释放掉不是active状态的split stash单元 */
void __wt_split_stash_discard(WT_SESSION_IMPL* session)
{
	WT_SPLIT_STASH *stash;
	uint64_t oldest;
	size_t i;

	/* 获得最老的split generation */
	oldest = __split_oldest_gen(session);

	for(i = 0, stash = session->split_stash; i < session->split_stash_cnt; ++i, ++stash){
		if (stash->p == NULL)
			continue;
		else if(stash->split_gen >= oldest) /*找到最早active状态的split stash*/
			break;

		/* 释放掉已经断开的 split stash */
		WT_STAT_FAST_CONN_ATOMIC_DECRV(session, rec_split_stashed_bytes, stash->len);
		WT_STAT_FAST_CONN_ATOMIC_DECR(session, rec_split_stashed_objects);

		__wt_overwrite_and_free_len(session, stash->p, stash->len);
	}

	/*进行空间收缩*/
	if (i > 100 || i == session->split_stash_cnt){
		if ((session->split_stash_cnt -= i) > 0)
			memmove(session->split_stash, stash, session->split_stash_cnt * sizeof(*stash));
	}
}

/*释放所有session中的split stash*/
void __wt_split_stash_discard_all(WT_SESSION_IMPL *session_safe, WT_SESSION_IMPL *session)
{
	WT_SPLIT_STASH *stash;
	size_t i;

	/*
	 * This function is called during WT_CONNECTION.close to discard any
	 * memory that remains.  For that reason, we take two WT_SESSION_IMPL
	 * arguments: session_safe is still linked to the WT_CONNECTION and
	 * can be safely used for calls to other WiredTiger functions, while
	 * session is the WT_SESSION_IMPL we're cleaning up.
	 */
	for(i = 0, stash = session->split_stash; i < session->split_stash_cnt; ++i, ++stash){
		if(stash->p != NULL)
			__wt_free(session_safe, stash->p);
	}

	__wt_free(session_safe, session->split_stash);
	session->split_stash_cnt = session->split_stash_alloc = 0;
}
