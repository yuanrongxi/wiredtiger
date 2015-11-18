/***********************************************************************
* 返回一个page ref的 k/v键值对的内容
***********************************************************************/

#include "wt_internal.h"

int __wt_kv_return(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_CURSOR *cursor;
	WT_PAGE *page;
	WT_ROW *rip;
	uint8_t v;

	switch (page->type){
	case WT_PAGE_COL_FIX:
		cursor->recno = cbt->recno;
		/*cursor对应的是一个upd,直接返回value*/
		if (upd != NULL){
			cursor->value.data = WT_UPDATE_DATA(upd);
			cursor->value.size = upd->size;
			return 0;
		}

		v = __bit_getv_recno(page, cbt->iface.recno, btree->bitcnt);
		return __wt_buf_set(session, &cursor->value, &v, 1);
		
	case WT_PAGE_COL_VAR:
		cursor->recno = cbt->recno;

		if (upd != NULL) {
			cursor->value.data = WT_UPDATE_DATA(upd);
			cursor->value.size = upd->size;
			return (0);
		}
		/*获得对应的cell,并通过cell得到K/V值*/
		cell = WT_COL_PTR(page, &page->pg_var_d[cbt->slot]);
		break;

	case WT_PAGE_ROW_LEAF:
		rip = &page->pg_row_d[cbt->slot];

		if (cbt->ins != NULL){ /*插入的k/v对*/
			cursor->key.data = WT_INSERT_KEY(cbt->ins);
			cursor->key.size = WT_INSERT_KEY_SIZE(cbt->ins);
		}
		else if (cbt->compare == 0){/*比较器定位到了对应的k/v对*/
			cursor->key.data = cbt->search_key.data;
			cursor->key.size = cbt->search_key.size;
		}
		else
			WT_RET(__wt_row_leaf_key(session, page, rip, &cursor->key, 0)); /*设置key的值*/

		if (upd != NULL) {
			cursor->value.data = WT_UPDATE_DATA(upd);
			cursor->value.size = upd->size;
			return (0);
		}
		/*可以直接通过rip指针获得value*/
		if (__wt_row_leaf_value(page, rip, &cursor->value))
			return 0;

		/*没有定位到对应value的内存位置，表示没查找到*/
		if (cell = __wt_row_leaf_value_cell(page, rip, NULL) == NULL){
			cursor->value.size = 0;
			return 0;
		}
		break;

		WT_ILLEGAL_VALUE(session);
	}
	/*通过cell解析到对应的value值*/
	__wt_cell_unpack(cell, &unpack);
	WT_RET(__wt_page_cell_data_ref(session, page, &unpack, &cursor->value));

	return 0;
}


