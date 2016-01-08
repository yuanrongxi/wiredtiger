
/*Confirm the page's write generation number won't wrap*/
static inline int __page_write_gen_wrapped_check(WT_PAGE* page)
{
	return (page->modify->write_gen > UINT32_MAX - WT_MILLION ? WT_RESTART : 0);
}

/*向指定skip list中增加一个WT_INSERT entry*/
static inline int __insert_serial_func(WT_SESSION_IMPL *session, WT_INSERT_HEAD *ins_head,
	WT_INSERT ***ins_stack, WT_INSERT *new_ins, u_int skipdepth)
{
	u_int i;
	WT_UNUSED(session);

	/*先做错误检查，确定位置是否是skip list中的位置*/
	for (i = 0; i < skipdepth; i++){
		if(ins_stack[i] == NULL || (*ins_stack[i] != new_ins->next[i]))
			return (WT_RESTART);
		if (new_ins->next[i] == NULL && ins_head->tail[i] != NULL && ins_stack[i] != &ins_head->tail[i]->next[i])
			return (WT_RESTART);
	}

	/*将new ins插入到ins_head skip list*/
	for (i = 0; i < skipdepth; i++){
		if (ins_head->tail[i] == NULL || ins_stack[i] == &ins_head->tail[i]->next[i])
			ins_head->tail[i] = new_ins;
		*ins_stack[i] = new_ins;
	}

	return 0;
}

