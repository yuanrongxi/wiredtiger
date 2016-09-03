/************************************************************************
*对MMAP函数封装
************************************************************************/
#include "wt_internal.h"

/*mmap一个文件*/
int __wt_mmap(WT_SESSION_IMPL* session, WT_FH* fh, void* mapp, size_t* lenp, void** mappingcookie)
{
	void* map;
	size_t orig_size;

	UT_UNUSED(mappingcookie);

	orig_size = (size_t)fh->size;

	map = mmap(NULL, PROT_READ, orig_size, MAP_PRIVATE, fh->fd, (wt_off_t)0);
	if(map == MAP_FAILED){
		WT_RET_MSG(session, __wt_errno(), "%s map error: failed to map %" WT_SIZET_FMT " bytes", fh->name, orig_size);
	}

	__wt_verbose(session, WT_VERB_FILEOPS, "%s: map %p: %" WT_SIZET_FMT " bytes", fh->name, map, orig_size);

	*(void **)mapp = map;
	*lenp = orig_size;

	return 0;
}

/* Linux requires the address be aligned to a 4KB boundary. */
#define	WT_VM_PAGESIZE	4096

/*预加载block manager对应文件的page cache,从p地址开始，预加载size长度的数据,为了顺序读*/
int __wt_mmap_preload(WT_SESSION_IMPL *session, const void *p, size_t size)
{

	WT_BM *bm = S2BT(session)->bm;
	WT_DECL_RET;

	/**4KB对齐寻址,向前对齐，例如p = 4097,那么blk = 4096, size = size + 1*/
	void *blk = (void *)((uintptr_t)p & ~(uintptr_t)(WT_VM_PAGESIZE - 1));
	size += WT_PTRDIFF(p, blk);

	/* XXX proxy for "am I doing a scan?" -- manual read-ahead,,必须2M为单位的清除文件缓存,因为预读是2M为单位*/
	if (F_ISSET(session, WT_SESSION_NO_CACHE)) {
		/* Read in 2MB blocks every 1MB of data. */
		if (((uintptr_t)((uint8_t *)blk + size) & (uintptr_t)((1<<20) - 1)) < (uintptr_t)blk)
			return 0;

		/*从新确定SIZE,最小清除空间2M*/
		size = WT_MIN(WT_MAX(20 * size, 2 << 20), WT_PTRDIFF((uint8_t *)bm->map + bm->maplen, blk));
	}

	/*4KB对齐*/
	size &= ~(size_t)(WT_VM_PAGESIZE - 1);

	/*文件缓冲预加载*/
	if (size > WT_VM_PAGESIZE && (ret = posix_madvise(blk, size, POSIX_MADV_WILLNEED)) != 0)
		WT_RET_MSG(session, ret, "posix_madvise will need");

	return 0;
}

/*放弃mmap隐射的内存，从P地址开始，长度为size*/
int __wt_mmap_discard(WT_SESSION_IMPL *session, void *p, size_t size)
{
	WT_DECL_RET;
	/*4k对齐，linux文件高速缓冲区是4KB为一个page cache*/
	void *blk = (void *)((uintptr_t)p & ~(uintptr_t)(WT_VM_PAGESIZE - 1));
	size += WT_PTRDIFF(p, blk);

	if ((ret = posix_madvise(blk, size, POSIX_MADV_DONTNEED)) != 0)
		WT_RET_MSG(session, ret, "posix_madvise don't need");

	return 0;
}

/*注销一个文件的mmap内存隐射*/
int __wt_munmap(WT_SESSION_IMPL *session, WT_FH *fh, void *map, size_t len, void **mappingcookie)
{
	WT_UNUSED(mappingcookie);

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: unmap %p: %" WT_SIZET_FMT " bytes", fh->name, map, len));

	if (munmap(map, len) == 0)
		return (0);

	WT_RET_MSG(session, __wt_errno(), "%s unmap error: failed to unmap %" WT_SIZET_FMT " bytes",
		fh->name, len);
}


