#include "wt_internal.h"

/*page类型字符串的提示*/
const char* __wt_page_type_string(u_int type)
{
	switch (type) {
	case WT_PAGE_INVALID:
		return ("invalid");
	case WT_PAGE_BLOCK_MANAGER:
		return ("block manager");
	case WT_PAGE_COL_FIX:
		return ("column-store fixed-length leaf");
	case WT_PAGE_COL_INT:
		return ("column-store internal");
	case WT_PAGE_COL_VAR:
		return ("column-store variable-length leaf");
	case WT_PAGE_OVFL:
		return ("overflow");
	case WT_PAGE_ROW_INT:
		return ("row-store internal");
	case WT_PAGE_ROW_LEAF:
		return ("row-store leaf");
	default:
		return ("unknown");
	}
}

/*cell类型的字符串提示*/
const char* __wt_cell_type_string(uint8_t type)
{
	switch (type) {
	case WT_CELL_ADDR_DEL:
		return ("addr/del");
	case WT_CELL_ADDR_INT:
		return ("addr/int");
	case WT_CELL_ADDR_LEAF:
		return ("addr/leaf");
	case WT_CELL_ADDR_LEAF_NO:
		return ("addr/leaf-no");
	case WT_CELL_DEL:
		return ("deleted");
	case WT_CELL_KEY:
		return ("key");
	case WT_CELL_KEY_PFX:
		return ("key/pfx");
	case WT_CELL_KEY_OVFL:
		return ("key/ovfl");
	case WT_CELL_KEY_SHORT:
		return ("key/short");
	case WT_CELL_KEY_SHORT_PFX:
		return ("key/short,pfx");
	case WT_CELL_KEY_OVFL_RM:
		return ("key/ovfl,rm");
	case WT_CELL_VALUE:
		return ("value");
	case WT_CELL_VALUE_COPY:
		return ("value/copy");
	case WT_CELL_VALUE_OVFL:
		return ("value/ovfl");
	case WT_CELL_VALUE_OVFL_RM:
		return ("value/ovfl,rm");
	case WT_CELL_VALUE_SHORT:
		return ("value/short");
	default:
		return ("unknown");
	}
}

/*获得ref对应page的block的格式化特征信息字符串*/
const char* __wt_page_addr_string(WT_SESSION_IMPL* session, WT_REF* ref, WT_ITEM* buf)
{
	size_t addr_size;
	const uint8_t* addr;

	if(__wt_ref_is_root(ref)){
		buf->data = "[Root]";
		buf->size = strlen("[Root]");
		return buf->data;
	}
	/*通过ref信息读取addr cookie信息*/
	__wt_ref_info(session, ref, &addr, &addr_size, NULL);
	return __wt_addr_string(session, addr, addr_size, buf);
}

/*获得addr cookie对block的信息字符串*/
const char* __wt_addr_string(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, WT_ITEM *buf)
{
	WT_BM* bm;
	bm = S2BT(session)->bm;

	if(addr == NULL){
		buf->data = "[NoAddr]";
		buf->size = strlen("[NoAddr]");
	}
	else if(bm->addr_string(bm, session, buf, addr, addr_size) != 0){ /*获得addr cookie的block格式化信息*/
		buf->data = "[error]";
		buf->size = strlen("[Error]");
	}

	return (const char*)buf->data;
}


