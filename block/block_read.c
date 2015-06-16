/**************************************************************************
*block的读操作实现
**************************************************************************/
#include "wt_internal.h"

/*对addr指向的block进行预加载*/
int __wt_bm_preload(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	WT_BLOCK *block;
	WT_DECL_RET;
	wt_off_t offset;
	uint32_t cksum, size;
	int mapped;


	WT_UNUSED(addr_size);
	block = bm->block;
	ret = EINVAL;

	/* 校验addr的合法性,并读取对应的offset/size/checksum*/
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	/*检查是否可以为mmap方式提供预读*/
	mapped = bm->map != NULL && offset + size <= (wt_off_t)bm->maplen;
	if(mapped){
		/*进行mmap方式的文件预加载*/
		WT_RET(__wt_mmap_preload(session, (uint8_t *)bm->map + offset, size));
	}
	else{ /*直接对文件方式的预加载*/
		ret = posix_fadvise(block->fh->fd, (wt_off_t)offset, (wt_off_t)size, POSIX_FADV_WILLNEED);

		/*预加载失败，直接进行预读，这样文件这部分数据一定会在page cache中*/
		if (ret != 0) {
			WT_DECL_ITEM(tmp);
			WT_RET(__wt_scr_alloc(session, size, &tmp));
			ret = __wt_block_read_off(session, block, tmp, offset, size, cksum);
			__wt_scr_free(session, &tmp);
			WT_RET(ret);
		}
	}

	/*更新预加载信息*/
	WT_STAT_FAST_CONN_INCR(session, block_preload);

	return 0;
}

/*将addr对应的block数据读取到buf中*/
int __wt_bm_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
	WT_BLOCK *block;
	int mapped;
	wt_off_t offset;
	uint32_t cksum, size;

	WT_UNUSED(addr_size);
	block = bm->block;

	/*从addr中读取offset/checksum/size*/
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	/*在mmap隐射的范围内*/
	mapped = bm->map != NULL && offset + size <= (wt_off_t)bm->maplen;
	if (mapped) {
		buf->data = (uint8_t *)bm->map + offset;
		buf->size = size;
		WT_RET(__wt_mmap_preload(session, buf->data, buf->size));

		WT_STAT_FAST_CONN_INCR(session, block_map_read);
		WT_STAT_FAST_CONN_INCRV(session, block_byte_map_read, size);
		return 0;
	}
	/*将block数据读取到buf中*/
	WT_RET(__wt_block_read_off(session, block, buf, offset, size, cksum));

	/*假如page cache缓冲的block内容已经超过设置的上线，将block对应的文件page cache清空*/
	if (block->os_cache_max != 0 && (block->os_cache += size) > block->os_cache_max) {
		WT_DECL_RET;
		block->os_cache = 0;
		ret = posix_fadvise(block->fh->fd,(wt_off_t)0, (wt_off_t)0, POSIX_FADV_DONTNEED);
		if (ret != 0 && ret != EINVAL)
			WT_RET_MSG(session, ret, "%s: posix_fadvise", block->name);
	}

	return 0;
}

/*按照offset和size的信息，将block的数据读取到buf中,并校验checksum*/
int __wt_block_read_off(WT_SESSION_IMPL* session, WT_BLOCK* block, WT_ITEM* buf, wt_off_t offset, uint32_t size, uint32_t cksum)
{
	WT_BLOCK_HEADER *blk;
	size_t bufsize;
	uint32_t page_cksum;

	WT_RET(__wt_verbose(session, WT_VERB_READ, "off %" PRIuMAX ", size %" PRIu32 ", cksum %" PRIu32, (uintmax_t)offset, size, cksum));
	/*更新状态统计信息*/
	WT_STAT_FAST_CONN_INCR(session, block_read);
	WT_STAT_FAST_CONN_INCRV(session, block_byte_read, size);

	/*进行bufsize的对齐*/
	if (F_ISSET(buf, WT_ITEM_ALIGNED))
		bufsize = size;
	else {
		F_SET(buf, WT_ITEM_ALIGNED);
		bufsize = WT_MAX(size, buf->memsize + 10);
	}

	/*确保buf空闲大小为bufsize*/
	WT_RET(__wt_buf_init(session, buf, bufsize));
	/*从文件中读取数据到buf中*/
	WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));
	buf->size = size;

	/*进行checksum校验*/
	blk = WT_BLOCK_HEADER_REF(buf->mem);
	page_cksum = blk->cksum;
	if (page_cksum == cksum) {
		blk->cksum = 0;
		page_cksum = __wt_cksum(buf->mem, F_ISSET(blk, WT_BLOCK_DATA_CKSUM) ?size : WT_BLOCK_COMPRESS_SKIP);
		if (page_cksum == cksum)
			return 0;
	}

	/*block数据被破坏*/
	if (!F_ISSET(session, WT_SESSION_SALVAGE_CORRUPT_OK))
		__wt_errx(session, "read checksum error [%" PRIu32 "B @ %" PRIuMAX ", %"
		PRIu32 " != %" PRIu32 "]", size, (uintmax_t)offset, cksum, page_cksum);

	/* Panic if a checksum fails during an ordinary read. */
	return (block->verify || F_ISSET(session, WT_SESSION_SALVAGE_CORRUPT_OK) ? WT_ERROR : __wt_illegal_value(session, block->name));
}
