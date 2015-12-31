/***************************************************
 * column存储方式的检索实现
 **************************************************/

/*在inshead skip list中定位到一个刚好大于recno的记录位置*/
static inline WT_INSERT* __col_insert_search_gt(WT_INSERT_HEAD* inshead, uint64_t recno)
{
	WT_INSERT *ins, **insp;
	int i;

	/* inshead不包含recno所在的范围，不需要继续检索 */
	if ((ins = WT_SKIP_LAST(inshead)) == NULL)
		return NULL;

	/*直接判断recno序号是否已经到inshead skip list的末尾，如果到了末尾，直接返回未定位到*/
	if (recno >= WT_INSERT_RECNO(ins))
		return NULL;

	/*skip list定位过程*/
	ins = NULL;
	for (i = WT_SKIP_MAXDEPTH - 1, insp = &inshead->head[i]; i > 0;){
		if (*insp != NULL && recno >= WT_INSERT_RECNO(*insp)) {
			ins = *insp;	/* GTE: keep going at this level */
			insp = &(*insp)->next[i];
		}
		else {
			--i;		/* LT: drop down a level */
			--insp;
		}
	}

	if (ins == NULL)
		ins = WT_SKIP_FIRST(inshead);
	while (recno >= WT_INSERT_RECNO(ins))
		ins = WT_SKIP_NEXT(ins);
	return ins;
}

/*在inshead skip list中定位到比recno小的前一条记录位置*/
static inline WT_INSERT* __col_insert_search_lt(WT_INSERT_HEAD* inshead, uint64_t recno)
{
	WT_INSERT *ins, **insp;
	int i;

	/*inshead 不包含recno所在的范围*/
	if ((ins = WT_SKIP_FIRST(inshead)) == NULL)
		return (NULL);

	if (recno <= WT_INSERT_RECNO(ins))
		return (NULL);

	for (i = WT_SKIP_MAXDEPTH - 1, insp = &inshead->head[i]; i >= 0;){
		if (*insp != NULL && recno > WT_INSERT_RECNO(*insp)) {
			ins = *insp;	/* GT: keep going at this level */
			insp = &(*insp)->next[i];
		}
		else  {
			--i;		/* LTE: drop down a level */
			--insp;
		}
	}

	return ins;
}

/*在inshead skip list中精确定位到recno所在的位置*/
static inline WT_INSERT* __col_insert_search_match(WT_INSERT_HEAD* inshead, uint64_t recno)
{
	WT_INSERT **insp, *ret_ins;
	uint64_t ins_recno;
	int cmp, i;

	if ((ret_ins = WT_SKIP_LAST(inshead)) == NULL)
		return NULL;

	if (recno > WT_INSERT_RECNO(ret_ins))
		return NULL;
	else if (recno == WT_INSERT_RECNO(ret_ins))
		return ret_ins;

	/*skip list定位过程*/
	for (i = WT_SKIP_MAXDEPTH - 1, insp = &inshead->head[i]; i >= 0;) {
		if (*insp == NULL) {
			--i;
			--insp;
			continue;
		}

		ins_recno = WT_INSERT_RECNO(*insp);
		cmp = (recno == ins_recno) ? 0 : (recno < ins_recno) ? -1 : 1;
		if (cmp == 0)			/* Exact match: return */
			return (*insp);
		else if (cmp > 0)		/* Keep going at this level */
			insp = &(*insp)->next[i];
		else {					/* Drop down a level */
			--i;
			--insp;
		}
	}

	return NULL;
}

/*为插入的新记录定位插入skip list的位置,并把skip 的路径返回*/
static inline WT_INSERT* __col_insert_search(WT_INSERT_HEAD* inshead, WT_INSERT*** ins_stack, WT_INSERT** next_stack, uint64_t recno)
{
	WT_INSERT **insp, *ret_ins;
	uint64_t ins_recno;
	int cmp, i;

	/* If there's no insert chain to search, we're done. */
	if ((ret_ins = WT_SKIP_LAST(inshead)) == NULL)
		return NULL;

	/*插入到skip list的最后面，构建插入前后节点的关系stack*/
	if (recno >= WT_INSERT_RECNO(ret_ins)) {
		for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
			ins_stack[i] = (i == 0) ? &ret_ins->next[0] : (inshead->tail[i] != NULL) ? &inshead->tail[i]->next[i] : &inshead->head[i];
			next_stack[i] = NULL;
		}
		return ret_ins;
	}

	/*在skip list中间进行定位，并保存前后关系*/
	for (i = WT_SKIP_MAXDEPTH - 1, insp = &inshead->head[i]; i >= 0;) {
		if ((ret_ins = *insp) == NULL) { /*本层已经到最后了，保存前后关系， 需要进行下一层的定位*/
			next_stack[i] = NULL;
			ins_stack[i--] = insp--;
			continue;
		}

		ins_recno = WT_INSERT_RECNO(ret_ins);
		cmp = (recno == ins_recno) ? 0 : (recno < ins_recno) ? -1 : 1;
		if (cmp > 0)			/* Keep going at this level */
			insp = &ret_ins->next[i];
		else if (cmp == 0)		/* Exact match: return */
			for (; i >= 0; i--) {
				next_stack[i] = ret_ins->next[i];
				ins_stack[i] = &ret_ins->next[i];
			}
		else {				/* Drop down a level */
			next_stack[i] = ret_ins;
			ins_stack[i--] = insp--;
		}
	}
	return ret_ins;
}

/*获得variable-length column store page中最大的recno值*/
static inline uint64_t __col_var_last_recno(WT_PAGE* page)
{
	WT_COL_RLE *repeat;

	if (page->pg_var_nrepeats == 0)
		return (page->pg_var_entries == 0 ? 0 : page->pg_var_recno + (page->pg_var_entries - 1));

	repeat = &page->pg_var_repeats[page->pg_var_nrepeats - 1];
	return ((repeat->recno + repeat->rle) - 1 + (page->pg_var_entries - (repeat->indx + 1)));
}

/*获得fix-length column store page中最大的recno值*/
static inline uint64_t __col_fix_last_recno(WT_PAGE* page)
{
	return (page->pg_fix_entries == 0 ? 0 : page->pg_fix_recno + (page->pg_fix_entries - 1));
}

/*  */
static inline WT_COL* __col_var_search(WT_PAGE* page, uint64_t recno, uint64_t* start_recnop)
{
	WT_COL_RLE *repeat;
	uint64_t start_recno;
	uint32_t base, indx, limit, start_indx;

	/*二分法进行定位*/
	for (base = 0, limit = page->pg_var_nrepeats; limit != 0; limit >>= 1){
		indx = base + (limit >> 1);

		repeat = page->pg_var_repeats + indx;
		/*定位到recno对应col位置直接返回*/
		if (recno >= repeat->recno && recno < repeat->recno + repeat->rle) {
			if (start_recnop != NULL)
				*start_recnop = repeat->recno;
			return (page->pg_var_d + repeat->indx);
		}

		if (recno < repeat->recno)
			continue;

		base = indx + 1;
		--limit;
	}

	/*是在第一个repeat中，返回起始位置*/
	if (base == 0) {
		start_indx = 0;
		start_recno = page->pg_var_recno;
	}
	else {
		/*在中间未存有和recno重叠的repeat中，从repeat的recno作为startrecno*/
		repeat = page->pg_var_repeats + (base - 1);
		start_indx = repeat->indx + 1;
		start_recno = repeat->recno + repeat->rle;
	}

	/*对选定的repeat做校验*/
	if (recno >= start_recno + (page->pg_var_entries - start_indx))
		return NULL;

	/*确定返回的col对象*/
	return page->pg_var_d + start_indx + (uint32_t)(recno - start_recno);
}









