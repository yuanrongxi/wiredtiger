/*****************************************************************************
*定义chunk的merge线程
*****************************************************************************/
#include "wt_internal.h"

static int __lsm_worker_general_op(WT_SESSION_IMPL*, WT_LSM_WORKER_ARGS* , int*);

static WT_THREAD_RET __lsm_worker(void*);


/*启动一个lsm worker工作线程*/
int __wt_lsm_worker_start(WT_SESSION_IMPL *session, WT_LSM_WORKER_ARGS *args)
{
	WT_RET(__wt_verbose(session, WT_VERB_LSM, "Start LSM worker %d type 0x%x", args->id, args->type));

	return __wt_thread_create(session, &args->tid, __lsm_worker, args);
}

/*执行一个bloom，或者执行一个work单元的删除和flush*/
static int __lsm_worker_general_op(WT_SESSION_IMPL* session, WT_LSM_WORKER_ARGS* cookie, int* completed)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_WORK_UNIT *entry;
	int force;

	*completed = 0;

	if (!FLD_ISSET(cookie->type, WT_LSM_WORK_BLOOM | WT_LSM_WORK_DROP | WT_LSM_WORK_FLUSH))
		return WT_NOTFOUND;

	ret = __wt_lsm_manager_pop_entry(session, cookie->type, &entry);
	if (ret != 0 || entry == NULL)
		return ret;

	if(entry->type == WT_LSM_WORK_FLUSH){
		force = F_ISSET(entry, WT_LSM_WORK_FORCE);
		F_CLR(entry, WT_LSM_WORK_FORCE);
		WT_ERR(__wt_lsm_get_chunk_to_flush(session, entry->lsm_tree, force, &chunk));

		if(chunk != NULL){
			WT_ERR(__wt_verbose(session, WT_VERB_LSM, "Flush%s chunk %d %s", force ? " w/ force" : "", chunk->id, chunk->uri));
			ret = __wt_lsm_checkpoint_chunk(session, entry->lsm_tree, chunk);

			WT_ASSERT(session, chunk->refcnt > 0);
			WT_ATOMIC_SUB4(chunk->refcnt, 1);

			WT_ERR(ret);
		}
	}
	else if (entry->type == WT_LSM_WORK_DROP){
		WT_ERR(__wt_lsm_free_chunks(session, entry->lsm_tree));
	}
	else if(entry->type == WT_LSM_WORK_BLOOM){
		WT_ERR(__wt_lsm_work_bloom(session, entry->lsm_tree));
	}

	*completed = 1;

err:
	__wt_lsm_manager_free_work_unit(session, entry);
	return ret;
}

/*LSM树线程的执行体，主要执行chunk的merge、flush disk等操作*/
static WT_THREAD_RET __lsm_worker(void* args)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LSM_WORK_UNIT *entry;
	WT_LSM_WORKER_ARGS *cookie;
	WT_SESSION_IMPL *session;
	int progress, ran;

	cookie = (WT_LSM_WORKER_ARGS *)args;
	session = cookie->session;
	conn = S2C(session);

	entry = NULL;
	while(F_ISSET(conn, WT_CONN_SERVER_RUN) && F_ISSET(cookie, WT_LSM_WORKER_RUN)){
		progress = 0;

		while(FLD_ISSET(cookie->type, WT_LSM_WORK_SWITCH) && (ret = __wt_lsm_manager_pop_entry(session, WT_LSM_WORK_SWITCH, &entry)) == 0
			&& entry != NULL){
				WT_ERR(__wt_lsm_work_switch(session, &entry, &progress));
		}

		WT_ERR(ret);

		ret = __lsm_worker_general_op(session, cookie, &ran);
		if(ret == EBUSY || ret == WT_NOTFOUND)
			ret = 0;

		WT_ERR(ret);
		progress = progress || ran;

		if (FLD_ISSET(cookie->type, WT_LSM_WORK_MERGE) &&(ret = __wt_lsm_manager_pop_entry(session, WT_LSM_WORK_MERGE, &entry)) == 0 &&
			entry != NULL) {
				WT_ASSERT(session, entry->type == WT_LSM_WORK_MERGE);
				/*lsm 树枝的merge操作，可能耗费的时间很长*/
				ret = __wt_lsm_merge(session,entry->lsm_tree, cookie->id);
				if (ret == WT_NOTFOUND) {
					F_CLR(entry->lsm_tree, WT_LSM_TREE_COMPACTING);
					ret = 0;
				}
				else if (ret == EBUSY)
					ret = 0;

				/* Paranoia: clear session state. */
				session->dhandle = NULL;

				__wt_lsm_manager_free_work_unit(session, entry);
				entry = NULL;
				progress = 1;
		}

		WT_ERR(ret);

		/*等待下一次任务信号的触发*/
		if (!progress) {
			WT_ERR(__wt_cond_wait(session, cookie->work_cond, 10000));
			continue;
		}
	}

	if(ret != 0){
err:
		__wt_lsm_manager_free_work_unit(session, entry);
		WT_PANIC_MSG(session, ret, "Error in LSM worker thread %d", cookie->id);
	}

	return WT_THREAD_RET_VALUE;
}


