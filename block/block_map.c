/*************************************************************************
*block文件挂载mmap
*************************************************************************/

#include "wt_internal.h"

/*conn指定了用mmap方式操作文件，那么block的操作方式需要设置成mmap方式操作*/
int __wt_block_map(WT_SESSION_IMPL *session, WT_BLOCK *block, void *mapp, size_t *maplenp, void **mappingcookie)
{
	*(void **)mapp = NULL;
	*maplenp = 0;

	/*如果block设置了verify操作函数，不能用mmap*/
	if (block->verify)
		return (0);

	/*文件采用的是direct io方式进行读写，无法用mmap*/
	if (block->fh->direct_io)
		return (0);

	/*block对应的文件设置了os page cache，无法使用mmap,因为文件需要根据os_cache_max去清空os page cache,这是mmap做不到的*/
	if (block->os_cache_max != 0)
		return (0);

	/*block 文件的mmap隐射挂载*/
	__wt_mmap(session, block->fh, mapp, maplenp, mappingcookie);

	return 0;
}

/*卸载block文件的mmap*/
int __wt_block_unmap(WT_SESSION_IMPL *session, WT_BLOCK *block, void *map, size_t maplen, void **mappingcookie)
{
	return __wt_munmap(session, block->fh, map, maplen, mappingcookie);
}

