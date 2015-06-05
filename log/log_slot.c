/********************************************************************************
*一个基于buffer slot的log buffer实现，通过不同的slot来提高log buffer的并发效率
*具体细节可以参考：
*http://infoscience.epfl.ch/record/170505/files/aether-smpfulltext.pdf
*论文
********************************************************************************/

#include "wt_internal.h"

/*初始化session对应的log对象的log slot*/
int __wt_log_slot_init(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	WT_LOGSLOT *slot;
	int32_t i;

	/*获得session对应的log对象*/
	conn = S2C(session);
	log = conn->log;

	/*进行log slot pool初始化*/
	for (i = 0; i < SLOT_POOL; i++) {
		log->slot_pool[i].slot_state = WT_LOG_SLOT_FREE;
		log->slot_pool[i].slot_index = SLOT_INVALID_INDEX;
	}

	/*为log设定一个已经准备好的slot，下次write log用这个slot*/
	for(i = 0; i < SLOT_ACTIVE; i++){
		slot = &log->slot_pool[i];
		slot->slot_index = (uint32_t)i;
		slot->slot_state = WT_LOG_SLOT_READY;
		log->slot_array[i] = slot;
	}

	/*为各个slot分配日志写入缓冲区*/
	for(i = 0; i < SLOT_POOL; i++){
		WT_ERR(__wt_buf_init(session, &log->slot_pool[i].slot_buf, WT_LOG_SLOT_BUF_INIT_SIZE));
		/*初始化slot标识位*/
		F_SET(&log->slot_pool[i], SLOT_INIT_FLAGS);
	}

	/*记录log buffer的统计信息*/
	WT_STAT_FAST_CONN_INCRV(session, log_buffer_size, WT_LOG_SLOT_BUF_INIT_SIZE * SLOT_POOL);
	
	return ret;

err:
	__wt_buf_free(session, &log->slot_pool[i].slot_buf);
	return ret;
}

/*撤销session对应的log slot buffer*/
int __wt_log_slot_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;
	int i;

	conn = S2C(session);
	log = conn->log;
	/*释放开辟的内存空间*/
	for (i = 0; i < SLOT_POOL; i++)
		__wt_buf_free(session, &log->slot_pool[i].slot_buf);

	return 0;
}

/*选取一个ready slot来进行log write，在这个函数中会确定写入的slot以及写入的位置*/
int __wt_log_slot_join(WT_SESSION_IMPL* session, uint64_t mysize, uint32_t flags, WT_MYSLOT* myslotp)
{
	WT_CONNECTION_IMPL*	conn;
	WT_LOG*				log;
	WT_LOGSLOT*			slot;
	int64_t				cur_state, new_state, old_state;
	uint32_t			allocated_slot, slot_grow_attempts;

	conn = S2C(session);
	log = conn->log;

	slot_grow_attempts = 0;

find_slot:
	/*随机在active slots中获取一个已经处于准备状态的slot*/
	allocated_slot = __wt_random(session->rnd) % SLOT_ACTIVE;
	slot = log->slot_array[allocated_slot];
	old_state = slot->slot_state;

join_slot:
	if(old_state < WT_LOG_SLOT_READY){ /*表示这个slot已经处于不可选取状态，必须重新选择。因为在find_slot的过程有可能有多个线程同时find_slot,整个重选过程相当于spin*/
		WT_STAT_FAST_CONN_INCR(session, log_slot_transitions);
		goto find_slot;
	}

	/*
	 * Add in our size to the state and then atomically swap that
	 * into place if it is still the same value.
	 * 这个相当于抢占写入的位置和长度,如果位置被别的线程抢占了
	 * 必须重新去选取slot
	 */
	new_state = old_state + (int64_t)mysize;
	if(new_state < old_state){
		WT_STAT_FAST_CONN_INCR(session, log_slot_toobig);
		goto find_slot;
	}

	/*如果slot buffer的剩余的空间无法承载新写入的LOG，进行重新选取*/
	if(new_state > (int64_t)slot->slot_buf.memsize){
		F_SET(slot, SLOT_BUF_GROW);
		if(++slot_grow_attempts > 5){ /*尝试5次*/
			WT_STAT_FAST_CONN_INCR(session, log_slot_toosmall);
			return ENOMEM;
		}

		goto find_slot;
	}

	/*进行原子性更改*/
	cur_state = WT_ATOMIC_CAS_VAL8(slot->slot_state, old_state, new_state);
	if(cur_state != old_state){ /*表示已经有其他的线程已经抢占了这个slot,从新join可写的位置*/
		old_state = cur_state;
		WT_STAT_FAST_CONN_INCR(session, log_slot_races);
		goto join_slot;
	}

	WT_ASSERT(session, myslotp != NULL);

	/*更新joins的统计信息*/
	WT_STAT_FAST_CONN_INCR(session, log_slot_joins);

	/*确定fsync的方式*/
	if (LF_ISSET(WT_LOG_DSYNC | WT_LOG_FSYNC))
		F_SET(slot, SLOT_SYNC_DIR);

	if (LF_ISSET(WT_LOG_FSYNC))
		F_SET(slot, SLOT_SYNC);

	/*确定log写的slot和位置*/
	myslotp->slot = slot;
	myslotp->offset = (wt_off_t)old_state - WT_LOG_SLOT_READY;

	return 0;
}

/*close一个slot，禁止其他线程再对其join, 这个函数会先将其从ready array中删除，并选取一个新的slot顶替它的工作*/
int __wt_log_slot_close(WT_SESSION_IMPL *session, WT_LOGSLOT *slot)
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;
	WT_LOGSLOT *newslot;
	int64_t old_state;
	int32_t yields;
	uint32_t pool_i, switch_fails;

	conn = S2C(session);
	log = conn->log;
	switch_fails = 0;

retry:
	/*获得当前log buffer使用的slot*/
	pool_i = log->pool_index;
	newslot = &(log->slot_pool[pool_i]);
	/*如果下一个index超出slot pool的范围，则回到pool的第一个单元上*/
	if(++log->pool_index >= SLOT_POOL){
		log->pool_index = 0;
	}

	/*当前的slot不是可以关闭的状态，进行等待重试*/
	if(newslot->slot_state != WT_LOG_SLOT_FREE){
		WT_STAT_FAST_CONN_INCR(session, log_slot_switch_fails);

		/* slot churn计数是线程为了等待slot可close状态而占用过多的CPU时间设计的，如果slot_churn > 0,则表示在
		 * slot在WT_LOG_SLOT_FREE，这个close线程需要释放CPU控制权，让操作系统再进行一次公平调度*/
		if(++switch_fails % SLOT_POOL == 0 && slot->slot_churn < 5)
			++slot->slot_churn;

		__wt_yield();
		goto retry;
	}
	else if(slot->slot_churn > 0){
		--slot->slot_churn;
		WT_ASSERT(session, slot->slot_churn >= 0);
	}

	/*释放CPU控制权，让其他线程有机会进行抢占CPU时间片*/
	for(yields = slot->slot_churn; yields >= 0; yields --)
		__wt_yield();

	/*进行状态更新*/
	WT_STAT_FAST_CONN_INCR(session, log_slot_closes);

	/*进行new slot ready标识*/
	newslot->slot_state = WT_LOG_SLOT_READY;

	/*新的slot顶替close的slot在ready array中的位置*/
	newslot->slot_index = slot->slot_index; 
	/*设置一个新的slot放入ready队列进行随机join*/
	log->slot_array[newslot->slot_index] = &log->slot_pool[pool_i];

	/*原子性将close的slot设置为WT_LOG_SLOT_PENDING状态*/
	old_state = WT_ATOMIC_STORE8(slot->slot_state, WT_LOG_SLOT_PENDING);
	/*计算log buffer中的数据长度*/
	slot->slot_group_size = (uint64_t)(old_state - WT_LOG_SLOT_READY);

	WT_STAT_FAST_CONN_INCRV(session, log_slot_consolidated, (uint64_t)slot->slot_group_size);

	return 0;
}

/*将slot的状态只为writting状态*/
int __wt_log_slot_notify(WT_SESSION_IMPL *session, WT_LOGSLOT *slot)
{
	WT_UNUSED(session);

	slot->slot_state = (int64_t)WT_LOG_SLOT_DONE - (int64_t)slot->slot_group_size;
	return (0);
}

/*等待slot leader的分配写位置*/
int __wt_log_slot_wait(WT_SESSION_IMPL *session, WT_LOGSLOT *slot)
{
	int yield_count = 0;

	WT_UNUSED(session);

	while (slot->slot_state > WT_LOG_SLOT_DONE){
		if (++yield_count < 1000)
			__wt_yield();
		else
			__wt_sleep(0, 200);
	}

	return 0;
}

int64_t __wt_log_slot_release(WT_LOGSLOT *slot, uint64_t size)
{
	int64_t newsize;

	/*
	 * Add my size into the state.  When it reaches WT_LOG_SLOT_DONE
	 * all participatory threads have completed copying their piece.
	 */
	newsize = WT_ATOMIC_ADD8(slot->slot_state, (int64_t)size);
	return newsize;
}

/*将slot置为设置为SLOT_FREE状态，以便重新SLOT READY,在这个过程会检查slot flag，
 *如果flag中有需要grow buffer的请求，此函数会对slot buf重分配，空间扩大到原来的2倍*/
int __wt_log_slot_free(WT_SESSION_IMPL *session, WT_LOGSLOT *slot)
{
	WT_DECL_RET;

	ret = 0;

	if(F_ISSET(slot, SLOT_BUF_GROW)){
		/*进行统计信息更新*/
		WT_STAT_FAST_CONN_INCR(session, log_buffer_grow);
		WT_STAT_FAST_CONN_INCRV(session, log_buffer_size, slot->slot_buf.memsize);

		WT_ERR(__wt_buf_grow(session, &slot->slot_buf, slot->slot_buf.memsize * 2));
	}

err:
	/*进行slot状态更新*/
	slot->flags = SLOT_INIT_FLAGS;
	slot->slot_state = WT_LOG_SLOT_FREE;

	return ret;
}

/*对slot pool中的slot进行grow buffer检查，并对对应的slot buffer做grow操作*/
int __wt_log_slot_grow_buffers(WT_SESSION_IMPL *session, size_t newsize)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	WT_LOGSLOT *slot;
	int64_t orig_state;
	uint64_t old_size, total_growth;
	int i;

	conn = S2C(session);
	log = conn->log;
	total_growth = 0;

	/*更新统计计数*/
	WT_STAT_FAST_CONN_INCR(session, log_buffer_grow);

	/*这个spin lock是防止其他线程同时grow buffer*/
	__wt_spin_lock(session, &log->log_slot_lock);
	for(i < 0; i < SLOT_POOL; i++){
		slot = &log->slot_pool[i];

		/*正在使用的slot不在grow buffer之列,也避开对应的slot原子操作spin*/
		if (slot->slot_state != WT_LOG_SLOT_FREE && slot->slot_state != WT_LOG_SLOT_READY)
			continue;

		/*不强制没有设置grow buffer的slot进行grow*/
		if(slot->slot_buf.memsize > 10 * newsize && !F_ISSET(slot, SLOT_BUF_GROW))
			continue;

		/*抢占slot*/
		orig_state = WT_ATOMIC_CAS_VAL8(slot->slot_state, WT_LOG_SLOT_FREE, WT_LOG_SLOT_PENDING);
		if (orig_state != WT_LOG_SLOT_FREE) {
			orig_state = WT_ATOMIC_CAS_VAL8(slot->slot_state, WT_LOG_SLOT_READY, WT_LOG_SLOT_PENDING);
			if (orig_state != WT_LOG_SLOT_READY)
				continue;
		}
		
		/*进行grow buffer,每一放大到原来的2倍空间*/
		old_size = slot->slot_buf.memsize;
		F_CLR(slot, SLOT_BUF_GROW);
		WT_ERR(__wt_buf_grow(session, &slot->slot_buf, WT_MAX(slot->slot_buf.memsize * 2, newsize)));

		/*buffer grow过程完成，需要将slot原来的状态设置回来*/
		slot->slot_state = orig_state;
		total_growth += slot->slot_buf.memsize - old_size;

		slot->slot_state = orig_state;
	}

err:
	__wt_spin_unlock(session, &log->log_slot_lock);
	WT_STAT_FAST_CONN_INCRV(session, log_buffer_size, total_growth);

	return ret;
}





