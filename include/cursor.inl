
/*设置行号*/
static inline void __cursor_set_recno(WT_CURSOR_BTREE* cbt, uint64_t v)
{
	cbt->iface.recno = v;
	cbt->recno = v;
}

/*重置CURSOR_BTREE结构*/
static inline void __cursor_pos_clear(WT_CURSOR_BTREE* cbt)
{
	cbt->recno = 0;

	cbt->ins = NULL;
	cbt->ins_head = NULL;
	cbt->ins_stack[0] = NULL;

	cbt->cip_saved = NULL;
	cbt->rip_saved = NULL;

	/*清除flags的WT_CBT_ACTIVE值*/
	F_CLR(cbt, ~WT_CBT_ACTIVE);
}

/*将session对应的cursors置为无效,假如没有激活状态的cursor，我们将释放所有为了读隔离的snapshot*/
static inline void __cursor_leave(WT_SESSION_IMPL* session)
{
	/*
	 * Decrement the count of active cursors in the session.  When that
	 * goes to zero, there are no active cursors, and we can release any
	 * snapshot we're holding for read committed isolation.
	 */

	WT_ASSERT(session, session->ncursors > 0);
	-- session->ncursors;
	if(session->ncursors == 0){
		__wt_txn_read_last(session);
	}
}

/*激活一个CURSOR_BTREE*/
static inline int __curfile_enter(WT_CURSOR_BTREE *cbt)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_RET(__cursor_enter(session));
	F_SET(cbt, WT_CBT_ACTIVE);

	return 0;
}

/*对cbt进行复位*/
static inline int __curfile_leave(WT_CURSOR_BTREE* cbt)
{
	WT_SESSION_IMPL* session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	/*如果cursor_btree的状态是active，我们先清除其ACTIVE标识*/
	if(F_ISSET(cbt, WT_CBT_ACTIVE)){
		__cursor_leave(session);
		F_CLR(cbt, WT_CBT_ACTIVE);
	}

	/*如果在某个page上删除太多的记录，那么我们释放cursor时会尝试evict这个page*/
	if(cbt->ref != NULL && cbt->page_deleted_count > WT_BTREE_DELETE_THRESHOLD)
		__wt_page_evict_soon(cbt->ref->page);

	cbt->page_deleted_count = 0;

	WT_RET(__wt_page_release(session, cbt->ref, 0));
	cbt->ref = NULL;

	return 0;
}

/*对session的dhandle的in-use计数器 +１，如果刚开始为0时，调用这个函数过后，其timeofdeath将置为0*/
static inline void __wt_cursor_dhandle_incr_use(WT_SESSION_IMPL* session)
{
	WT_DATA_HANDLE* dhandle;

	dhandle = session->dhandle;

	if(WT_ATOMIC_ADD4(dhandle->session_inuse, 1) == 1 && dhandle->timeofdeath != 0)
		dhandle->timeofdeath = 0;
}

/*对session的dhandle的in-use计数器-1*/
static inline void __wt_cursor_dhandle_decr_use(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE *dhandle;

	dhandle = session->dhandle;

	/* If we close a handle with a time of death set, clear it. */
	WT_ASSERT(session, dhandle->session_inuse > 0);
	if (WT_ATOMIC_SUB4(dhandle->session_inuse, 1) == 0 && dhandle->timeofdeath != 0)
		dhandle->timeofdeath = 0;
}

/*对cbt进行初始化，并激活它*/
static inline int __cursor_func_init(WT_CURSOR_BTREE* cbt, int reenter)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL*)(cbt->iface.session);
	if(reenter){ /*对cbt进行撤销*/
		WT_RET(__curfile_leave(cbt));
	}

	/* If the transaction is idle, check that the cache isn't full. */
	WT_RET(__wt_txn_idle_cache_check(session));

	/*激活cbt对象*/
	if(!F_ISSET(cbt, WT_CBT_ACTIVE)){
		WT_RET(__curfile_enter(cbt));
	}

	__wt_txn_cursor_op(session);

	return 0;
}

/*对cbt进行重置*/
static inline int __cursor_reset(WT_CURSOR_BTREE* cbt)
{
	WT_DECL_RET;
	/*对cbt进行撤销，并进行状态复位*/
	ret = __curfile_leave(cbt);
	__cursor_pos_clear(cbt);

	return ret;
}

/*返回一个行(row)的叶子page的slot的KV键值对，值是存在cursor的value中*/
static inline __cursor_row_slot_return(WT_CURSOR_BTREE* cbt, WT_ROW* rip, WT_UPDATE* upd)
{
	WT_BTREE *btree;
	WT_ITEM *kb, *vb;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	void* copy;


	session = (WT_SESSION_IMPL *)(cbt->iface.session);
	btree = S2BT(session);
	page = cbt->ref->page;

	unpack = NULL;

	kb = &(cbt->iface.key);
	vb = &(cbt->iface.value);
	/*获得key的指针*/
	copy = WT_ROW_KEY_COPY(rip);

	/*
	 * Get a key: we could just call __wt_row_leaf_key, but as a cursor
	 * is running through the tree, we may have additional information
	 * here (we may have the fully-built key that's immediately before
	 * the prefix-compressed key we want, so it's a faster construction).
	 *
	 * First, check for an immediately available key.
	 */
	if(__wt_row_leaf_key_info(page, copy, &cell, &kb->data, &kb->size))
		goto value;

	if(btree->huffman_key != NULL)
		goto slow;

	unpack = &_unpack;
	__wt_cell_unpack(cell, unpack);
	if(unpack->type == WT_CELL_KEY && cbt->rip_saved != NULL && cbt->rip_saved == rip - 1) {
		WT_ASSERT(session, cbt->tmp.size >= unpack->prefix);

		cbt->tmp.size = unpack->prefix;
		WT_RET(__wt_buf_grow(session, &cbt->tmp, cbt->tmp.size + unpack->size));
		cbt->tmp.size += unpack->size;
	}
	else{
		/*
		 * Call __wt_row_leaf_key_work instead of __wt_row_leaf_key: we
		 * already did __wt_row_leaf_key's fast-path checks inline.
		 */
slow:
		WT_RET(__wt_row_leaf_key_work(session, page, rip, &cbt->tmp, 0));
	}

	kb->data = cbt->tmp.data;
	kb->size = cbt->tmp.size;
	cbt->rip_saved = rip;

value:
	if(upd != NULL){
		vb->data = WT_UPDATE_DATA(upd);
		vb->size = upd->size;
		return 0;
	}

	if(__wt_row_leaf_value(page, rip, vb))
		return 0;

	if ((cell = __wt_row_leaf_value_cell(page, rip, unpack)) == NULL) {
		vb->data = "";
		vb->size = 0;
		return (0);
	}

	unpack = &_unpack;
	__wt_cell_unpack(cell, unpack);

	return __wt_page_cell_data_ref(session, cbt->ref->page, unpack, vb);
}







