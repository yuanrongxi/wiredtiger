/***************************************************************************
*文件修复操作函数实现
***************************************************************************/
#include "wt_internal.h"

/*对block对应的文件进行初始化设置，相当于文件修复重整*/
int __wt_block_salvage_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	wt_off_t len;
	uint32_t allocsize;

	allocsize = block->allocsize;

	/*将block描述信息写入到文件的第一个allocsize对齐的位置上*/
	WT_RET(__wt_desc_init(session, block->fh, allocsize));

	/*初始化live结构*/
	WT_RET(__wt_block_ckpt_init(session, &(block->live), "live"));

	/*计算按allocsize重新调整文件大小*/
	if (block->fh->size > allocsize) {
		len = (block->fh->size / allocsize) * allocsize;
		if (len != block->fh->size)
			WT_RET(__wt_ftruncate(session, block->fh, len));
	} else
		len = allocsize;
	block->live.file_size = len;

	/*设置salvage的位置，因为只写入了一个allocsize长度的block 描述头信息*/
	block->slvg_off = allocsize;

	/*设置alloc可用的文件范围，相当于新建了一个(allocsize, len - allocsize)ext 对象*/
	WT_RET(__wt_block_insert_ext(session, &block->live.alloc, allocsize, len - allocsize));

	return 0;
}

/*结束文件的重整*/
int __wt_block_salvage_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	/* Discard the checkpoint. */
	return (__wt_block_checkpoint_unload(session, block, 0));
}

/*判断(off,size)相对block的合法性*/
int __wt_block_offset_invalid(WT_BLOCK *block, wt_off_t offset, uint32_t size)
{
	if (size == 0)				/* < minimum page size */
		return 1;

	if (size % block->allocsize != 0)	/* not allocation-size units */
		return 1;

	if (size > WT_BTREE_PAGE_SIZE_MAX)	/* > maximum page size */
		return 1;

	if (offset + (wt_off_t)size > block->fh->size) 	/* past end-of-file */
		return 1;

	return 0;
}

/*从block文件中获取下一个可以正常恢复的page数据*/
int __wt_block_salvage_next(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t *addr, size_t *addr_sizep, int *eofp)
{
	WT_BLOCK_HEADER *blk;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_FH *fh;
	wt_off_t max, offset;
	uint32_t allocsize, cksum, size;
	uint8_t *endp;

	*eofp = 0;

	fh = block->fh;
	allocsize = block->allocsize;

	WT_ERR(__wt_scr_alloc(session, allocsize, &tmp));

	max = fh->size;
	for(;;){
		offset = block->slvg_off;
		if (offset >= max) {			/* 已经到文件末尾了 */
			*eofp = 1;
			goto done;
		}

		/*读取一个对齐长度到tmp缓冲中*/
		WT_ERR(__wt_read(session, fh, offset, (size_t)allocsize, tmp->mem));

		/*获得block header*/
		blk = (WT_BLOCK_HEADER*)WT_BLOCK_HEADER_REF(tmp->mem);
		size = blk->disk_size;
		cksum = blk->cksum;

		/*如果offset,size合法，从offset处开始读取size个字节到tmp中，相当于一个page*/
		if (!__wt_block_offset_invalid(block, offset, size) &&
			__wt_block_read_off(session, block, tmp, offset, size, cksum) == 0)
			break;

		/*读取offset处的page失败，可能数据破坏了*/
		WT_ERR(__wt_verbose(session, WT_VERB_SALVAGE, "skipping %" PRIu32 "Bat file offset %" PRIuMAX, allocsize, (uintmax_t)offset));

		WT_ERR(__wt_block_off_free(session, block, offset, (wt_off_t)allocsize));
		/*跳过一个对齐长度，寻找下一个未破坏的page*/
		block->slvg_off += allocsize;
	}

	endp = addr;
	/*将page的offset size和cksum序列化到addr缓冲区中*/
	WT_ERR(__wt_block_addr_to_buffer(block, &endp, offset, size, cksum));
	*addr_sizep = WT_PTRDIFF(endp, addr);

done:
err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*addr位置的page是否合法，进行block修复状态更新*/
int __wt_block_salvage_valid(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t *addr, size_t addr_size, int valid)
{
	wt_off_t offset;
	uint32_t size, cksum;

	WT_UNUSED(session);
	WT_UNUSED(addr_size);

	/*将addr缓冲中的数据反序列，获得off/size/cksum三个值*/
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, cksum));
	if (valid)
		block->slvg_off = offset + size;
	else{ /*只能向前更新一个对齐长度*/
		WT_RET(__wt_block_off_free(session, block, offset, (wt_off_t)block->allocsize));
		block->slvg_off = offset + block->allocsize;
	}

	return 0;
}
