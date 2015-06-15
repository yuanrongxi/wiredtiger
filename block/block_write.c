/**************************************************************************
*实现对block的写操作
**************************************************************************/
#include "wt_internal.h"

u_int __wt_block_header(WT_BLOCK* block)
{
	WT_UNUSED(block);

	return WT_BLOCK_HEADER_SIZE;
}

/*计算写入block需要的buffer的长度,进行size对齐*/
int __wt_block_write_size(WT_SESSION_IMPL *session, WT_BLOCK *block, size_t *sizep)
{
	WT_UNUSED(session);

	*sizep = (size_t)WT_ALIGN(*sizep + WT_BLOCK_HEADER_BYTE_SIZE, block->allocsize);

	return (*sizep > UINT32_MAX - 1024 ? EINVAL : 0);
}

/*将block数据写入到buf缓冲区中*/
int __wt_block_write(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf, uint8_t *addr, size_t *addr_sizep, int data_cksum)
{
	wt_off_t offset;
	uint32_t size, cksum;
	uint8_t *endp;

	WT_RET(__wt_block_write_off(session, block, buf, &offset, &size, &cksum, data_cksum, 0));

	endp = addr;
	/*将block的checksum/长度对齐个数/偏移位置写入addr中*/
	WT_RET(__wt_block_addr_to_buffer(block, &endp, offset, size, cksum));
	*addr_sizep = WT_PTRDIFF(endp, addr);

	return 0;
}

/*将block的数据写入到buf中，并计算checksum和size*/
int __wt_block_write_off(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf, wt_off_t *offsetp, 
						uint32_t *sizep, uint32_t *cksump, int data_cksum, int caller_locked)
{
	WT_BLOCK_HEADER *blk;
	WT_DECL_RET;
	WT_FH *fh;
	size_t align_size;
	wt_off_t offset;
	int local_locked;

	blk = WT_BLOCK_HEADER_REF(buf->mem);
	fh = block->fh;
	local_locked = 0;

	/*buf不是对齐模式，不能进行写,因为这个是和磁盘相关的写入，必须是对齐的*/
	if(!F_ISSET(buf, WT_ITEM_ALIGNED)){
		WT_ASSERT(session, F_ISSET(buf, WT_ITEM_ALIGNED));
		WT_RET_MSG(session, EINVAL, "direct I/O check: write buffer incorrectly allocated");
	}

	/*计算buf->size按block对齐,对齐后有可能会比现有的buf->memsize大，如果大的话，不能进行写，有可能会缓冲区溢出*/
	align_size = WT_ALIGN(buf->size, block->allocsize);
	if (align_size > buf->memsize) {
		WT_ASSERT(session, align_size <= buf->memsize);
		WT_RET_MSG(session, EINVAL, "buffer size check: write buffer incorrectly allocated");
	}
	/*超过4G*/
	if (align_size > UINT32_MAX) {
		WT_ASSERT(session, align_size <= UINT32_MAX);
		WT_RET_MSG(session, EINVAL, "buffer size check: write buffer too large to write");
	}

	/*将对其后pading的buffer位置进行清0*/
	memset((uint8_t*)buf->mem + buf->size, 0, align_size - buf->size);

	/*设置block header,计算存储的数据长度*/
	blk->disk_size = WT_STORE_SIZE(align_size);
	blk->flags = 0;
	if(data_cksum)
		F_SET(blk, WT_BLOCK_DATA_CKSUM);

	/*计算buf的cksum*/
	blk->cksum = __wt_cksum(buf->mem, data_cksum ? align_size : WT_BLOCK_COMPRESS_SKIP);

	if (!caller_locked) {
		WT_RET(__wt_block_ext_prealloc(session, 5));
		__wt_spin_lock(session, &block->live_lock);
		local_locked = 1;
	}

	ret = __wt_block_alloc(session, block, &offset, (wt_off_t)align_size);
	/*判断文件是否需要进行扩大,如果不扩大就有可能存不下写入的block数据*/
	if(ret == 0 && fh->extend_len != 0 && (fh->extend_size <= fh->size ||
		(offset + fh->extend_len <= fh->extend_size && offset + fh->extend_len + (wt_off_t)align_size >= fh->extend_size))){
			/*调整extend_size为原来的offset + extend_len的两倍*/
			fh->extend_size = offset + fh->extend_len * 2;
			if (fh->fallocate_available != WT_FALLOCATE_NOT_AVAILABLE) {
				/*释放block->live_lock的自旋锁，因为重设文件大小会时间比较长，需要先释放自旋锁，防止CPU空转*/
				if (!fh->fallocate_requires_locking && local_locked) {
					__wt_spin_unlock(session, &block->live_lock);
					local_locked = 0;
				}

				/*扩大文件的占用空间*/
				if ((ret = __wt_fallocate(session,fh, offset, fh->extend_len * 2)) == ENOTSUP) {
					ret = 0;
					goto extend_truncate;
				}
			}
			else{
extend_truncate:
				if (!caller_locked && local_locked == 0) {
					__wt_spin_lock(session, &block->live_lock);
					local_locked = 1;
				}
				/*直接调整文件大小,这个比__wt_fallocate更慢*/
				if ((ret = __wt_ftruncate(session, fh, offset + fh->extend_len * 2)) == EBUSY)
					ret = 0;
			}
	}

	if(local_locked){
		__wt_spin_unlock(session, &block->live_lock);
		local_locked = 0;
	}

	WT_RET(ret);
	/*进行block的数据写入*/
	ret =__wt_write(session, fh, offset, align_size, buf->mem);
	if (ret != 0) {
		if (!caller_locked)
			__wt_spin_lock(session, &block->live_lock);

		WT_TRET(__wt_block_off_free(session, block, offset, (wt_off_t)align_size));
		if (!caller_locked)
			__wt_spin_unlock(session, &block->live_lock);

		WT_RET(ret);
	}

#ifdef HAVE_SYNC_FILE_RANGE
	/*需要进行fsync操作*/
	if (block->os_cache_dirty_max != 0 && (block->os_cache_dirty += align_size) > block->os_cache_dirty_max && __wt_session_can_wait(session)) {
			block->os_cache_dirty = 0;
			WT_RET(__wt_fsync_async(session, fh));
	}
#endif

#ifdef HAVE_POSIX_FADVISE
	/*清理fh->fd文件对应的system page cache中的数据,这个过程可能会有IO操作,相当于同步的sync调用*/
	if (block->os_cache_max != 0 && (block->os_cache += align_size) > block->os_cache_max) {
		block->os_cache = 0;
		if ((ret = posix_fadvise(fh->fd, (wt_off_t)0, (wt_off_t)0, POSIX_FADV_DONTNEED)) != 0)
			WT_RET_MSG( session, ret, "%s: posix_fadvise", block->name);
	}
#endif

	WT_STAT_FAST_CONN_INCR(session, block_write);
	WT_STAT_FAST_CONN_INCRV(session, block_byte_write, align_size);

	WT_RET(__wt_verbose(session, WT_VERB_WRITE, "off %" PRIuMAX ", size %" PRIuMAX ", cksum %" PRIu32, 
							(uintmax_t)offset, (uintmax_t)align_size, blk->cksum));

	*offsetp = offset;
	*sizep = WT_STORE_SIZE(align_size);
	*cksump = blk->cksum;

	return ret;
}



