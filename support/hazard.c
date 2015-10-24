/****************************************************************
* 实现session对象的hazard pointer,关于hazard pointer原理，请参照：
* https://en.wikipedia.org/wiki/Hazard_pointer
****************************************************************/

#include "wt_internal.h"


/*将一个page作为hazard pointer设置到session hazard pointer list中*/
int __wt_hazard_set(WT_SESSION_IMPL* session, WT_REF* ref, int* busyp)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_HAZARD *hp;
	int restarts = 0;

	btree = S2BT(session);
	conn = S2C(session);
	*busyp = 0;

	/*btree不会从内中淘汰page,hazard pointer是无意义的*/
	if (F_ISSET(btree, WT_BTREE_NO_HAZARD))
		return 0;

	for (hp = session->hazard + session->nhazard;; ++hp){
		/*hp超出hazard的数组范围,需要做几个处理，如果是初始扫描溢出范围的话，定位到hazard的开始部分继续扫描,如果hazard满的话，放大session->hazard_size，这个过程不能超过session->hazard_max*/
		if (hp >= session->hazard + session->hazard_size){
			if ((hp >= session->hazard + session->hazard_size) && restarts++ == 0)
				hp = session->hazard;
			else if (session->hazard_size >= conn->hazard_max)	/*超出session存储hazard pointer最大个数,返回一个系统异常*/
				break;
			else /*放大hazard size,这里通过了内存屏障来实现，防止CPU乱序执行造成程序并发异常*/
				WT_PUBLISH(session->hazard_size, WT_MIN(session->hazard_size + WT_HAZARD_INCR, conn->hazard_max));
		}

		/*这个位置被其他hazard pointer占用了，寻找下一个空位*/
		if (hp->page != NULL)
			continue;

		hp->page = ref->page;
		/*发布新设置的hazard pointer，这里有内存屏障为了写生效，防止先执行内存屏障后面的代码*/
		WT_FULL_BARRIER();

		/*有可能多个线程同时执行hp->page = ref->page， 这个时候设置为无效设置，并返回忙状态，告诉上层线程可能需要重试*/
		if (ref->page == hp->page && ref->state == WT_REF_MEM) {
			++session->nhazard;
			return 0;
		}

		hp->page = NULL;
		*busyp = 1;
		return 0;
	}

	return ENOMEM;
}

/*从session hazard列表清除一个hazard pointer*/
int __wt_hazard_clear(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BTREE *btree;
	WT_HAZARD *hp;

	btree = S2BT(session);

	/*btree不做page淘汰，也就不存在hazard pointer*/
	if (F_ISSET(btree, WT_BTREE_NO_HAZARD))
		return 0;

	for (hp = session->hazard + session->hazard_size - 1; hp >= session->hazard; --hp){
		if (hp->page == page){
			/*这个地方不需要用内存屏障来保证，因为hp->page在设置NULL的过程，不需要保证完全正确*/
			hp->page = NULL;
			--session->nhazard; /*这个值在会不会出现负数呢？*/
			return 0;
		}
	}

	WT_PANIC_RET(session, EINVAL, "session %p: clear hazard pointer: %p: not found", session, page);
}

/*清除掉session hazard列表中所有的hazard pointer*/
void __wt_hazard_close(WT_SESSION_IMPL* session)
{
	WT_HAZARD *hp;
	int found;

	/*查找有没有hazard pointer*/
	for (found = 0, hp = session->hazard; hp < session->hazard + session->hazard_size; ++hp){
		if (hp->page != NULL) {
			found = 1;
			break;
		}
	}

	/*hazard 列表中没有hazard pointer,直接返回，只有这两个值判断一致方可视为没有hazard pointer,在多线程执行下，这两值在执行的各个阶段有可能不一致*/
	if (session->nhazard == 0 && !found)
		return;

	/*清除hazard pointer*/
	for (hp = session->hazard; hp < session->hazard + session->hazard_size; ++hp){
		if (hp->page != NULL) { 
			hp->page = NULL;
			--session->nhazard;
		}
	}

	if (session->nhazard != 0)
		__wt_errx(session, "session %p: close hazard pointer table: count didn't match entries",session);
}




