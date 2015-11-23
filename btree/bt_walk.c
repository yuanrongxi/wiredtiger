/************************************************************
* btree 的cursor向前或者向后移动page操作实现
************************************************************/
#include "wt_internal.h"

int __wt_tree_walk(WT_SESSION_IMPL* session, WT_REF** refp, uint64_t* walkcntp, uint32_t flags)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *couple, *couple_orig, *ref;
	int prev, skip;
	uint32_t slot;

	btree = S2BT(session);

	/*
	* Tree walks are special: they look inside page structures that splits
	* may want to free.  Publish that the tree is active during this window.
	*/
	WT_ENTER_PAGE_INDEX(session);

	/*fast truncate 仅仅在行存储btree中使用*/
	if (btree->type != BTREE_ROW)
		LF_CLR(WT_READ_TRUNCATE);

	prev = LF_ISSET(WT_READ_PREV) ? 1 : 0;
	couple = couple_orig = ref = *refp;
	*refp = NULL;

	/*ref是NULL，表示是从root page开始*/
	if (ref == NULL){
		ref = &btree->root;
		if (ref->page == NULL)
			goto done;
		goto descend;
	}

ascend:
	/*假如回到root page,表示已经walk完毕，释放我们所保持的harzard pointer*/
	if (__wt_ref_is_root(ref)){
		WT_ERR(__wt_page_release(session, couple, flags));
		goto done;
	}
	/*获得当前ref信息在internal page的slot位置*/
	__wt_page_refp(session, ref, &pindex, &slot);

	for (;;){
		/*已经到internal page的末尾，应该换下一个internal page来索引*/
		if (prev && slot == 0 || (!prev && slot == pindex->entries - 1)){
			ref = ref->home->pg_intl_parent_ref; /*回到父亲internal*/

			/*只检索所在的internal page,不做夸internal page的检索*/
			if (LF_ISSET(WT_READ_SKIP_INTL))
				goto ascend;

			/*回到root page了，表示已经walk完毕，释放我们所保持的harzard pointer*/
			if (__wt_ref_is_root(ref))
				WT_ERR(__wt_page_release(session, couple, flags));
			else{
				__wt_page_refp(session, ref, &pindex, &slot);
				if ((ret = __wt_page_swap(session, couple, ref, flags)) != 0) { /*读取父亲节点来顶替孩子节点，然后继续索引*/
					WT_TRET(__wt_page_release(session, couple, flags));
					WT_ERR(ret);
				}
			}
			*refp = ref;
			goto done;
		}

		/*前移或者后移*/
		if (prev)
			--slot;
		else
			++slot;

		/*步数计数*/
		if (walkcntp != NULL)
			++(*walkcntp);

		for (;;){
			ref = pindex->index[slot];
			if (LF_ISSET(WT_READ_CACHE)){
				if (LF_ISSET(WT_READ_NO_WAIT) && ref->state != WT_REF_MEM)
					break;
			}
			else if (LF_ISSET(WT_READ_TRUNCATE)){
				/*页如果已经被删除了，跳过它*/
				if (ref->state == WT_REF_DELETED && __wt_delete_page_skip(session, ref))
					break;
			}
			else if (LF_ISSET(WT_READ_COMPACT)){
				if (ref->state == WT_REF_DELETED)
					break;

				/*如果page是在磁盘上，那么需要检查它是否需要compact，如果需要，那么需要跳过它*/
				if (ref->state == WT_REF_DISK) {
					WT_ERR(__wt_compact_page_skip(session, ref, &skip));
					if (skip)
						break;
				}
			}
			else{
				/*page已经标示删除了，跳过它*/
				if (ref->state == WT_REF_DELETED && __wt_delete_page_skip(session, ref))
					break;
			}
			/*读取ref对应的page到内存中*/
			ret = __wt_page_swap(session, couple, ref, flags);
			if (ret == WT_NOTFOUND){
				ret = 0;
				break;
			}

			/*
			* If a new walk that never coupled from the
			* root to a new saved position in the tree,
			* restart the walk.
			*/
			if (ret == WT_RESTART){
				ret = 0;
				if (couple == &btree->root) {
					ref = &btree->root;
					if (ref->page == NULL)
						goto done;
					goto descend;
				}
				/*重新回到初始位置再来一遍*/
				WT_ASSERT(session, couple == couple_orig || WT_PAGE_IS_INTERNAL(couple->page));
				ref = couple;
				__wt_page_refp(session, ref, &pindex, &slot);
				if (couple == couple_orig)
					break;
			}

			WT_ERR(ret);
descend:
			couple = ref;
			page = ref->page;
			/*内部索引page，获取前一个索引*/
			if (page->type == WT_PAGE_ROW_INT || page->type == WT_PAGE_COL_INT){
				WT_INTL_INDEX_GET(session, page, pindex);
				slot = prev ? pindex->entries - 1 : 0;
			}
			else{
				*refp = ref;
				goto done;
			}
		}
	}
done:
err:
	WT_LEAVE_PAGE_INDEX(session);
   return ret;
}



