/**************************************************************************
*
**************************************************************************/

#include "wt_internal.h"

static int __verify_ckptfrag_add(WT_SESSION_IMPL *, WT_BLOCK *, wt_off_t, wt_off_t);
static int __verify_ckptfrag_chk(WT_SESSION_IMPL *, WT_BLOCK *);
static int __verify_filefrag_add(WT_SESSION_IMPL *, WT_BLOCK *, const char *, wt_off_t, wt_off_t, int);
static int __verify_filefrag_chk(WT_SESSION_IMPL *, WT_BLOCK *);
static int __verify_last_avail(WT_SESSION_IMPL *, WT_BLOCK *, WT_CKPT *);
static int __verify_last_truncate(WT_SESSION_IMPL *, WT_BLOCK *, WT_CKPT *);

/*计算off偏移位置是block对齐个数，除掉了文件开头的block desc第一个对齐长度*/
#define	WT_wt_off_TO_FRAG(block, off)		((off) / (block)->allocsize - 1)
/*通过对齐下标，计算offset偏移位置*/
#define	WT_FRAG_TO_OFF(block, frag)			(((wt_off_t)(frag + 1)) * (block)->allocsize)

/*开一个对block的核实校验*/
int __wt_block_verify_start(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_CKPT *ckptbase)
{
	WT_CKPT *ckpt;
	wt_off_t size;

	/*定位到最后一个的checkpoint元信息,如果它是个无效的空信息单元，不需要继续往下校验??*/
	WT_CKPT_FOREACH(ckptbase, ckpt)
		;

	for (;; --ckpt) {
		if (ckpt->name != NULL && !F_ISSET(ckpt, WT_CKPT_FAKE))
			break;

		/*ckpt是第一个，无需继续校验*/
		if (ckpt == ckptbase)
			return 0;
	}

	/* Truncate the file to the size of the last checkpoint. */
	WT_RET(__verify_last_truncate(session, block, ckpt));

	/*文件没有任何page页数据，无法校验*/
	size = block->fh->size;
	if (size <= block->allocsize)
		return 0;

	/*文件不是以block要求的长队对齐的，直接返回错误*/
	if (size % block->allocsize != 0)
		WT_RET_MSG(session, WT_ERROR, "the file size is not a multiple of the allocation size");

	/* 获得文件有多少个按block要求的对齐长度块,并分配一个fragfile bit位与它对应，
	 * 例如size = 1TB,那么fragfile = (((1 * 2^40) / 512) / 8) = 256 * 2^20 = 256MB*/
	block->frags = (uint64_t)WT_wt_off_TO_FRAG(block, size);
	WT_RET(__bit_alloc(session, block->frags, &block->fragfile));

	WT_RET(__wt_block_extlist_init(session, &block->verify_alloc, "verify", "alloc", 0));
	/*根据最后一个checkpoint信息来确定fragfile中那些对齐块中出现了extent对象*/
	WT_RET(__verify_last_avail(session, block, ckpt));

	block->verify = 1;
	return 0;
}

static int __verify_last_avail(WT_SESSION_IMPL* session, WT_BLOCK* block, WT_CKPT* ckpt)
{
	WT_BLOCK_CKPT *ci, _ci;
	WT_DECL_RET;
	WT_EXT *ext;
	WT_EXTLIST *el;

	ci = &_ci;
	/*初始化ci结构*/
	WT_RET(__wt_block_ckpt_init(session, ci, ckpt->name));
	/*将ckpt中的checkpoint元信息反序列到ci中*/
	WT_ERR(__wt_block_buffer_to_ckpt(session, block, ckpt->raw.data, ci));

	el = &ci->avail;

	if(el->offset != WT_BLOCK_INVALID_OFFSET){
		/*将文件中的ext对象读取到el列表中，但是会除去el本身占用的ext对象(entry.off/size占用的ext)*/
		WT_ERR(__wt_block_extlist_read_avail(session, block, el, ci->file_size));

		/*遍历el列表，确定el中的ext.off在哪些对齐块中？*/
		WT_EXT_FOREACH(ext, el->off){
			if ((ret = __verify_filefrag_add(session, block, "avail-list chunk", ext->off, ext->size, 1)) != 0)
				break;
		}
	}

err:
	__wt_block_ckpt_destroy(session, ci);

	return ret;
}

/*强制将文件设置成最后一个checkpoint指定的文件长度*/
static int __verify_last_truncate(WT_SESSION_IMPL* session, WT_BLOCK* block, WT_CKPT* ckpt)
{
	WT_BLOCK_CKPT *ci, _ci;
	WT_DECL_RET;

	ci = &_ci;
	WT_RET(__wt_block_ckpt_init(session, ci, ckpt->name));
	WT_ERR(__wt_block_buffer_to_ckpt(session, block, ckpt->raw.data, ci));
	WT_ERR(__wt_ftruncate(session, block->fh, ci->file_size));

err:	
	__wt_block_ckpt_destroy(session, ci);
	return ret;
}

/*结束对一个block的核实校验操作*/
int __wt_block_verify_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_DECL_RET;

	ret = __verify_filefrag_chk(session, block);
	/*将verify alloc中的ext释放掉*/
	__wt_block_extlist_free(session, &block->verify_alloc);

	__wt_free(session, block->fragfile);
	__wt_free(session, block->fragckpt);

	block->verify = 0;
	return ret;
}

int __wt_verify_ckpt_load(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
{
	WT_EXTLIST *el;
	WT_EXT *ext;
	uint64_t frag, frags;

	block->verify_size = ci->file_size;

	/*将checkpoint元数据位置也设置到filefrag中*/
	if (ci->root_offset != WT_BLOCK_INVALID_OFFSET)
		WT_RET(__verify_filefrag_add(session, block, "checkpoint", ci->root_offset, (wt_off_t)ci->root_size, 1));

	if (ci->alloc.offset != WT_BLOCK_INVALID_OFFSET)
		WT_RET(__verify_filefrag_add(session, block, "alloc list", ci->alloc.offset, (wt_off_t)ci->alloc.size, 1));

	if (ci->avail.offset != WT_BLOCK_INVALID_OFFSET)
		WT_RET(__verify_filefrag_add(session, block, "avail list", ci->avail.offset, (wt_off_t)ci->avail.size, 1));

	if (ci->discard.offset != WT_BLOCK_INVALID_OFFSET)
		WT_RET(__verify_filefrag_add(session, block, "discard list", ci->discard.offset, (wt_off_t)ci->discard.size, 1));

	el = &ci->alloc;
	if (el->offset != WT_BLOCK_INVALID_OFFSET) {
		/*将alloc中的ext对象读出*/
		WT_RET(__wt_block_extlist_read(session, block, el, ci->file_size));
		/*将el合并到verify_alloc中*/
		WT_RET(__wt_block_extlist_merge( session, el, &block->verify_alloc));
		__wt_block_extlist_free(session, el);
	}

	el = &ci->discard;
	if (el->offset != WT_BLOCK_INVALID_OFFSET) {
		/*将discard list中的ext读出，并将这些ext重verify中去掉*/
		WT_RET(__wt_block_extlist_read(session, block, el, ci->file_size));
		WT_EXT_FOREACH(ext, el->off){
			WT_RET(__wt_block_off_remove_overlap(session, &block->verify_alloc, ext->off, ext->size));
		}
		__wt_block_extlist_free(session, el);
	}

	/*读取avail中ext对象,主要是校验avail list的合法性*/
	el = &ci->avail;
	if (el->offset != WT_BLOCK_INVALID_OFFSET) {
		WT_RET(__wt_block_extlist_read(session, block, el, ci->file_size));
		__wt_block_extlist_free(session, el);
	}

	/*删除掉root ext(root.off, root.size)与block->verify_alloc重叠部分*/
	if (ci->root_offset != WT_BLOCK_INVALID_OFFSET)
		WT_RET(__wt_block_off_remove_overlap(session, &block->verify_alloc, ci->root_offset, ci->root_size));

	WT_RET(__bit_alloc(session, block->frags, &block->fragckpt));
	el = &block->verify_alloc;

	/*确定ext的末尾一个对齐块bit上设置1，为最后校验*/
	WT_EXT_FOREACH(ext, el->off) {
		frag = (uint64_t)WT_wt_off_TO_FRAG(block, ext->off);
		frags = (uint64_t)(ext->size / block->allocsize);
		__bit_nset(block->fragckpt, frag, frag + (frags - 1));
	}

	return 0;
}

/*进行block表空间合法性校验*/
int __wt_verify_ckpt_unload(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_DECL_RET;

	ret = __verify_ckptfrag_chk(session, block);
	__wt_free(session, block->fragckpt);

	return ret;
}





