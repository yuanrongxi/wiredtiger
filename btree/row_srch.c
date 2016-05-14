/************************************************************
 * row store btree的key搜索定位操作实现
 ***********************************************************/

#include "wt_internal.h"

/* 在insert append列表中查找定位key对应的记录, 并构建一个skip list stack 对象返回*/
static inline int __wt_search_insert_append(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, WT_ITEM* srch_key, int* donep)
{
	WT_BTREE *btree;
	WT_COLLATOR *collator;
	WT_INSERT *ins;
	WT_INSERT_HEAD *inshead;
	WT_ITEM key;
	int cmp, i;

	btree = S2BT(session);
	collator = btree->collator;
	*donep = 0;

	inshead = cbt->ins_head;
	if ((ins = WT_SKIP_LAST(inshead)) == NULL)
		return NULL;

	/*通过ins获得key的值*/
	key.data = WT_INSERT_KEY(ins);
	key.size = WT_INSERT_KEY_SIZE(ins);

	/*进行值比较*/
	WT_RET(__wt_compare(session, collator, srch_key, &key, &cmp));
	if (cmp >= 0){
		/*
		* !!!
		* We may race with another appending thread.
		*
		* To catch that case, rely on the atomic pointer read above
		* and set the next stack to NULL here.  If we have raced with
		* another thread, one of the next pointers will not be NULL by
		* the time they are checked against the next stack inside the
		* serialized insert function.
		*/

		for (i = WT_SKIP_MAXDEPTH - 1; i >= 0; i--) {
			cbt->ins_stack[i] = (i == 0) ? &ins->next[0] :
				(inshead->tail[i] != NULL) ? &inshead->tail[i]->next[i] : &inshead->head[i];

			cbt->next_stack[i] = NULL;
		}
		cbt->compare = -cmp;
		cbt->ins = ins;
		*donep = 1;
	}

	return 0;
}

/* 为row store方式检索insert list,并构建一个skip list stack */
int __wt_search_insert(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt, WT_ITEM* srch_key)
{
	WT_BTREE *btree;
	WT_COLLATOR *collator;
	WT_INSERT *ins, **insp, *last_ins;
	WT_INSERT_HEAD *inshead;
	WT_ITEM key;
	size_t match, skiphigh, skiplow;
	int cmp, i;

	btree = S2BT(session);
	collator = btree->collator;
	inshead = cbt->ins_head;
	cmp = 0;				/* -Wuninitialized */

	match = skiphigh = skiplow = 0;
	ins = last_ins = NULL;
	for (i = WT_SKIP_MAXDEPTH - 1, insp = &inshead->head[i]; i >= 0;){
		if ((ins = *insp) == NULL){
			cbt->next_stack[i] = NULL;
			cbt->ins_stack[i--] = insp--;
			continue;
		}

		if (ins != last_ins) {
			last_ins = ins;
			key.data = WT_INSERT_KEY(ins);
			key.size = WT_INSERT_KEY_SIZE(ins);
			match = WT_MIN(skiplow, skiphigh);
			WT_RET(__wt_compare_skip(session, collator, srch_key, &key, &cmp, &match));
		}

		/*
		* For every insert element we review, we're getting closer to a better
		* choice; update the compare field to its new value.  If we went past
		* the last item in the list, return the last one: that is used to
		* decide whether we are positioned in a skiplist.
		*/
		if (cmp > 0){			/* Keep going at this level */
			insp = &ins->next[i];
			skiplow = match;
		}
		else if (cmp < 0){		/* Drop down a level */
			cbt->next_stack[i] = ins;
			cbt->ins_stack[i--] = insp--;
			skiphigh = match;
		}
		else{ /*找到了！！！*/
			for (; i >= 0; i--) {
				cbt->next_stack[i] = ins->next[i];
				cbt->ins_stack[i] = &ins->next[i];
			}
		}
	}

	/*
	* For every insert element we review, we're getting closer to a better
	* choice; update the compare field to its new value.  If we went past
	* the last item in the list, return the last one: that is used to
	* decide whether we are positioned in a skiplist.
	*/
	cbt->compare = -cmp;
	cbt->ins = (ins != NULL) ? ins : last_ins;

	return 0;
}

/* 用指定的key在ref对应的page进行查找定位，存储方式为row store方式 */
int __wt_row_search(WT_SESSION_IMPL* session, WT_ITEM* srch_key, WT_REF* leaf, WT_CURSOR_BTREE* cbt, int insert)
{
	WT_BTREE *btree;
	WT_COLLATOR *collator;
	WT_DECL_RET;
	WT_ITEM *item;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *current, *descent;
	WT_ROW *rip;
	size_t match, skiphigh, skiplow;
	uint32_t base, indx, limit;
	int append_check, cmp, depth, descend_right, done;

	btree = S2BT(session);
	collator = btree->collator;
	item = &cbt->search_key;

	/* btree cursor 复位 */
	__cursor_pos_clear(cbt);

	skiphigh = skiplow = 0;

	/*
	* If a cursor repeatedly appends to the tree, compare the search key
	* against the last key on each internal page during insert before
	* doing the full binary search.
	*
	* Track if the descent is to the right-side of the tree, used to set
	* the cursor's append history.
	*/
	append_check = insert && cbt->append_tree;
	descend_right = 1;

	/*如果BTREE SPLITS, 只能检索单个叶子节点, 而不能检索整个树*/
	if (leaf != NULL){
		current = leaf;
		goto leaf_only;
	}

	cmp = -1;
	current = &btree->root;
	/*对internal page做检索定位*/
	for (depth = 2;; ++depth){
restart:
		page = current->page;
		if (page->type != WT_PAGE_ROW_INT) /*已经到叶子节点了，退出对叶子节点做检索*/
			break;

		WT_INTL_INDEX_GET(session, page, pindex);
		/*只有一个孩子，直接深入下一级孩子节点做检索*/
		if (pindex->entries == 1) {
			descent = pindex->index[0];
			goto descend;
		}

		/* Fast-path appends. 追加式插入，只需要定位最后一个entry,判断最后一个entry是否包含了插入KEY的值范围，如果是，直接进入下一层 */
		if (append_check) {
			descent = pindex->index[pindex->entries - 1];
			__wt_ref_key(page, descent, &item->data, &item->size);
			WT_ERR(__wt_compare(session, collator, srch_key, item, &cmp));
			if (cmp >= 0)
				goto descend;

			/* A failed append check turns off append checks. */
			append_check = 0;
		}

		/*用二分法进行内部索引页内定位,定位到key对应的leaf page*/
		base = 1;
		limit = pindex->entries - 1;
		if (collator == NULL){ /*key范围增量比较,防止比较过程运算过多*/
			for (; limit != 0; limit >>= 1) {
				indx = base + (limit >> 1);
				descent = pindex->index[indx];
				__wt_ref_key(page, descent, &item->data, &item->size);

				match = WT_MIN(skiplow, skiphigh);
				cmp = __wt_lex_compare_skip(srch_key, item, &match);
				if (cmp > 0) {
					skiplow = match;
					base = indx + 1;
					--limit;
				}
				else if (cmp < 0)
					skiphigh = match;
				else
					goto descend;
			}
		}
		else{/*通过collator来比较*/
			for (; limit != 0; limit >>= 1) {
				indx = base + (limit >> 1);
				descent = pindex->index[indx];
				__wt_ref_key(page, descent, &item->data, &item->size);

				WT_ERR(__wt_compare(session, collator, srch_key, item, &cmp));
				if (cmp > 0) {
					base = indx + 1;
					--limit;
				}
				else if (cmp == 0)
					goto descend;
			}
		}
		/*定位到存储key的范围page ref*/
		descent = pindex->index[base - 1];

		if (pindex->entries != base - 1)
			descend_right = 0;

	descend:
		/*进行下一级页读取，如果有限制，先从内存中淘汰正在操作的page,如果正要读取的page在splits,那么我们从新检索当前(current)的page*/
		ret = __wt_page_swap(session, current, descent, 0);
		switch (ret){
		case 0:
			current = descent;
			break;

		case WT_RESTART: /*读取失败，从新再试*/
			skiphigh = skiplow = 0;
			goto restart;
			break;

		default:
			return ret;
		}
	}

	/*检索超过了btree的最大层级，那么扩大最大层级限制*/
	if (depth > btree->maximum_depth)
		btree->maximum_depth = depth;

leaf_only:
	page = current->page;
	cbt->ref = current;

	/*
	* In the case of a right-side tree descent during an insert, do a fast
	* check for an append to the page, try to catch cursors appending data
	* into the tree.
	*
	* It's tempting to make this test more rigorous: if a cursor inserts
	* randomly into a two-level tree (a root referencing a single child
	* that's empty except for an insert list), the right-side descent flag
	* will be set and this comparison wasted.  The problem resolves itself
	* as the tree grows larger: either we're no longer doing right-side
	* descent, or we'll avoid additional comparisons in internal pages,
	* making up for the wasted comparison here.  Similarly, the cursor's
	* history is set any time it's an insert and a right-side descent,
	* both to avoid a complicated/expensive test, and, in the case of
	* multiple threads appending to the tree, we want to mark them all as
	* appending, even if this test doesn't work.
	*/

	/*增加到后一个page对应的分支上*/
	if (insert && descend_right){
		cbt->append_tree = 1;
		/*叶子page上没有记录，那么直接在第一个entry slot上进行数据插入*/
		if (page->pg_row_entries == 0){
			cbt->slot = WT_ROW_SLOT(page, page->pg_row_d);
			F_SET(cbt, WT_CBT_SEARCH_SMALLEST);
			cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
		}
		else{
			/*定位到最后一个slot位置？*/
			cbt->slot = WT_ROW_SLOT(page, page->pg_row_d + (page->pg_row_entries - 1));
			cbt->ins_head = WT_ROW_INSERT_SLOT(page, cbt->slot);
		}

		WT_ERR(__wt_search_insert_append(session, cbt, srch_key, &done));
		if (done) /*已经定位到了，直接返回*/
			return 0;

		cbt->ins_head = NULL;
	}

	/*叶子节点上的二分查找*/
	base = 0;
	limit = page->pg_row_entries;
	if (collator == NULL){ /* 没有指定比较器，用默认的内存比较大小来做比较器查找 */
		for (; limit != 0; limit >>= 1) {
			indx = base + (limit >> 1);
			rip = page->pg_row_d + indx;
			WT_ERR(__wt_row_leaf_key(session, page, rip, item, 1));
			/*match是当前匹配key的内存位置*/
			match = WT_MIN(skiplow, skiphigh);
			cmp = __wt_lex_compare_skip(srch_key, item, &match);
			if (cmp > 0) {
				skiplow = match;
				base = indx + 1;
				--limit;
			}
			else if (cmp < 0)
				skiphigh = match;
			else
				goto leaf_match;
		}
	}
	else{
		for (; limit != 0; limit >>= 1) {
			indx = base + (limit >> 1);
			rip = page->pg_row_d + indx;
			WT_ERR(__wt_row_leaf_key(session, page, rip, item, 1));

			WT_ERR(__wt_compare(session, collator, srch_key, item, &cmp));
			if (cmp > 0) {
				base = indx + 1;
				--limit;
			}
			else if (cmp == 0)
				goto leaf_match;
		}
	}

	if (0) {
	leaf_match:	
		/* 完全定位到了对应的key的row位置，直接设置到btree cursor */
		cbt->compare = 0;
		cbt->slot = WT_ROW_SLOT(page, rip);
		return (0);
	}
	
	/*已经到本page的第一个entry，表示指定检索的key是小于本page最小存在row key*/
	if (base == 0) {
		cbt->compare = 1;
		cbt->slot = WT_ROW_SLOT(page, page->pg_row_d);

		F_SET(cbt, WT_CBT_SEARCH_SMALLEST);
		cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
	}
	else {
		/* 没找到匹配的key,但KEY是在这个page的存储范围之内，确定其存储的后一个slot base, 那么存储这个KEY的值的位置应该是base -1的位置 */
		cbt->compare = -1;
		cbt->slot = WT_ROW_SLOT(page, page->pg_row_d + (base - 1));

		cbt->ins_head = WT_ROW_INSERT_SLOT(page, cbt->slot);
	}

	if (WT_SKIP_FIRST(cbt->ins_head) == NULL)
		return 0;

	/*进行insert list中的定位*/
	if (insert) {
		WT_ERR(__wt_search_insert_append(session, cbt, srch_key, &done));
		if (done)
			return 0;
	}

	WT_ERR(__wt_search_insert(session, cbt, srch_key));

	return 0;

	/*驱逐内部索引page*/
err:
	if (leaf != NULL)
		WT_TRET(__wt_page_release(session, current, 0));

	return ret;
}

/* 将btree cursor随机定位到btree上一个记录位置 */
int __wt_row_random(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_INSERT *p, *t;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *current, *descent;

	btree = S2BT(session);

	/* 复位btree cursor位置 */
	__cursor_pos_clear(cbt);

restart:
	current = &btree->root;
	for (;;){
		page = current->page;
		if (page->type != WT_PAGE_ROW_INT) /* 定位到叶子节点了， 在叶子节点中进行查找 */
			break;

		/* 随机定位一个内部索引页的entry上 */
		WT_INTL_INDEX_GET(session, page, pindex);
		descent = pindex->index[__wt_random(session->rnd) % pindex->entries];
		/* 读取其孩子节点，并驱逐current page */
		if ((ret = __wt_page_swap(session, current, descent, 0)) == 0) {
			current = descent;
			continue;
		}

		/* __wt_page_swap返回restart, 可能读取的leaf page正在split, 重新在上一层的page上重新定位 */
		if (ret == WT_RESTART && (ret = __wt_page_release(session, current, 0)) == 0)
			goto restart;

		return ret;
	}

	/* 进行叶子节点随机定位 */
	if (page->pg_row_entries != 0) {
		/*
		* The use case for this call is finding a place to split the
		* tree.  Cheat (it's not like this is "random", anyway), and
		* make things easier by returning the first key on the page.
		* If the caller is attempting to split a newly created tree,
		* or a tree with just one big page, that's not going to work,
		* check for that.
		*/

		/* 叶子节点page里面存储有行记录，进行随机定位 */
		cbt->ref = current;
		cbt->compare = 0;
		WT_INTL_INDEX_GET(session, btree->root.page, pindex);
		cbt->slot = pindex->entries < 2 ? __wt_random(session->rnd) % page->pg_row_entries : 0;

		/*获得叶子节点的值，并赋值给btree cursor的search key*/
		return (__wt_row_leaf_key(session, page, page->pg_row_d + cbt->slot, &cbt->search_key, 0));
	}

	/* leaf page是刚刚新建的，那么取第一个entry的insert list 进行定位 */
	F_SET(cbt, WT_CBT_SEARCH_SMALLEST);
	if ((cbt->ins_head = WT_ROW_INSERT_SMALLEST(page)) == NULL)
		WT_ERR(WT_NOTFOUND);

	/*
	 * 假如这个新建的btree是有insert list记录，那么在insert list中定位一个中间的key作为返回
	 */
	for (p = t = WT_SKIP_FIRST(cbt->ins_head);;) {
		if ((p = WT_SKIP_NEXT(p)) == NULL)
			break;
		if ((p = WT_SKIP_NEXT(p)) == NULL)
			break;
		t = WT_SKIP_NEXT(t);
	}

	cbt->ref = current;
	cbt->compare = 0;
	cbt->ins = t;

err:
	WT_TRET(__wt_page_release(session, current, 0));
	return ret;
}



