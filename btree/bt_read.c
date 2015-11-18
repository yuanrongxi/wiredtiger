/****************************************************
* 从文件中读取一个page的数据到内存中并构建其内存对象
****************************************************/
#include "wt_internal.h"

int __wt_cache_read(WT_SESSION_IMPL* session, WT_REF* ref)
{
	WT_DECL_RET;
	WT_ITEM tmp;
	WT_PAGE *page;
	WT_PAGE_STATE previous_state;
	size_t addr_size;
	const uint8_t *addr;

	page = NULL;

	WT_CLEAR(tmp);

	/*检查ref是否已经开始对page的磁盘数据进行读取*/
	if (WT_ATOMIC_CAS4(ref->state, WT_REF_DISK, WT_REF_READING))
		previous_state = WT_REF_DISK;
	else if (WT_ATOMIC_CAS4(ref->state, WT_REF_DELETED, WT_REF_LOCKED)) /*检查ref的状态是否是deleted，如果是，先lock他*/
		previous_state = WT_REF_DELETED;
	else
		return 0;

	/*获得page的block address 和 cookie*/
	WT_ERR(__wt_ref_info(session, ref, &addr, &addr_size, NULL));
	if (addr == NULL){ /*没找到block addr,直接新建一个page*/
		WT_ASSERT(session, previous_state == WT_REF_DELETED);
		/*新建一个leaf page内存对象*/
		WT_ERR(__wt_btree_new_leaf_page(session, &page));
		ref->page = page;
	}
	else{
		/*从磁盘文件上将page数据读入内存*/
		WT_ERR(__wt_bt_read(session, &tmp, addr, addr_size));
		/*构建page数据组织结构和对象*/
		WT_ERR(__wt_page_inmem(session, ref, tmp.data, tmp.memsize, WT_DATA_IN_ITEM(&tmp) ? WT_PAGE_DISK_ALLOC : WT_PAGE_DISK_MAPPED, &page));
		tmp.mem = NULL;

		/*如果page已经处于删除状态，从BTREE树上删除掉这个page的节点*/
		if (previous_state == WT_REF_DELETED)
			WT_ERR(__wt_delete_page_instantiate(session, ref));
	}

	WT_ERR(__wt_verbose(session, WT_VERB_READ, "page %p: %s", page, __wt_page_type_string(page->type)));
	/*将page的内存状态生效到索引页上*/
	WT_PUBLISH(ref->state, WT_REF_MEM);

	return 0;

err:
	/*在读取过程中错误了，将page驱逐出cache并回滚到上一个ref状态*/
	if (ref->page != NULL)
		__wt_page_out(session, page);

	WT_PUBLISH(ref->state, previous_state);
	/*释放掉__wt_bt_read读取的block时分配的内存块，若是page构建成功，是不能释放这个块的，因为page是映射在这个块上*/
	__wt_buf_free(session, &tmp);

	return ret;
}
