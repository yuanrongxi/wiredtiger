/***************************************************************************
*block的创建与销毁操作,block header信息的读写操作
***************************************************************************/
#include "wt_internal.h"

static int __desc_read(WT_SESSION_IMPL* session, WT_BLOCK* block);

/*重置一个block manager对应的文件*/
int __wt_block_manager_truncate(WT_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	WT_DECL_RET;
	WT_FH *fh;

	/*打开文件filename并清空文件内容*/
	WT_RET(__wt_open(session, filename, 0, 0, WT_FILE_TYPE_DATA, &fh));
	WT_ERR(__wt_ftruncate(session, fh, (wt_off_t)0));

	/*写入文件的一些元数据描述,并将数据落盘*/
	WT_ERR(__wt_desc_init(session, fh, allocsize));
	WT_ERR(__wt_fsync(session, fh));

err:
	WT_TRET(__wt_close(session, &fh));

	return ret;
}

/*为block manager创建一个文件，并写入基本的元数据信息*/
int __wt_block_manager_create(WT_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	WT_DECL_RET;
	WT_FH *fh;
	char *path;

	/* Create the underlying file and open a handle. */
	WT_RET(__wt_open(session, filename, 1, 1, WT_FILE_TYPE_DATA, &fh));

	/*写入block file需要的元数据信息，并将写入的数据落盘*/
	ret = __wt_desc_init(session, fh, allocsize);
	WT_TRET(__wt_fsync(session, fh));
	WT_TRET(__wt_close(session, &fh));

	/*如果是需要checkpoint操作，需要将文件目录索引文件也刷盘*/
	if (ret == 0 && F_ISSET(S2C(session), WT_CONN_CKPT_SYNC) &&
	    (ret = __wt_filename(session, filename, &path)) == 0) {
		ret = __wt_directory_sync(session, path);
		__wt_free(session, path);
	}

	/*如果失败，删除前面创建的文件*/
	if (ret != 0)
		WT_TRET(__wt_remove(session, filename));

	return ret;
}

/*从conn中释放一个block内存对象*/
static int __block_destroy(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint64_t bucket;

	conn = S2C(session);
	bucket = block->name_hash % WT_HASH_ARRAY_SIZE;
	/*将block从connection的queue中删除*/
	WT_CONN_BLOCK_REMOVE(conn, block, bucket);

	if (block->name != NULL)
		__wt_free(session, block->name);

	if (block->fh != NULL)
		WT_TRET(__wt_close(session, &block->fh));

	__wt_spin_destroy(session, &block->live_lock);

	__wt_overwrite_and_free(session, block);

	return ret;
}

void __wt_block_configure_first_fit(WT_BLOCK* block, int on)
{
	/*
	 * Switch to first-fit allocation so we rewrite blocks at the start of
	 * the file; use atomic instructions because checkpoints also configure
	 * first-fit allocation, and this way we stay on first-fit allocation
	 * as long as any operation wants it.
	 */
	if (on)
		(void)WT_ATOMIC_ADD4(block->allocfirst, 1);
	else
		(void)WT_ATOMIC_SUB4(block->allocfirst, 1);
}

/*为session创建并打开一个block对象*/
int __wt_block_open(WT_SESSION_IMPL *session, const char *filename, const char *cfg[], int forced_salvage, 
					int readonly, uint32_t allocsize, WT_BLOCK **blockp)
{
	WT_BLOCK *block;
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint64_t bucket, hash;

	WT_TRET(__wt_verbose(session, WT_VERB_BLOCK, "open: %s", filename));

	/*确定filename对应的block在conn->queue中的bucket序号*/
	conn = S2C(session);
	*blockp = NULL;
	hash = __wt_hash_city64(filename, strlen(filename));
	bucket = hash % WT_HASH_ARRAY_SIZE;

	/*判断filename文件的block是否已经在conn->queue中,如果在，不需要创建，直接返回存在的block即可
	 *block_lock是为了保护conn管理的block所用的*/
	__wt_spin_lock(session, &conn->block_lock);
	SLIST_FOREACH(block, &conn->blockhash[bucket], hashl) {
		if (strcmp(filename, block->name) == 0) {
			++block->ref;
			*blockp = block;

			__wt_spin_unlock(session, &conn->block_lock);
			return 0;
		}
	}

	/*分配一个block对象*/
	WT_ERR(__wt_calloc_one(session, &block));
	block->ref = 1;
	WT_CONN_BLOCK_INSERT(conn, block, bucket);

	/*设置block的name和对齐长度*/
	WT_ERR(__wt_strdup(session, filename, &block->name));
	block->name_hash = hash;
	block->allocsize = allocsize;

	/*读取配置，并根据block_allocation来确定allocfirst的初始值*/
	WT_ERR(__wt_config_gets(session, cfg, "block_allocation", &cval));
	block->allocfirst = WT_STRING_MATCH("first", cval.str, cval.len) ? 1 : 0;

	/*设置错误，文件page cache方式和direct io方式是不能兼容的*/
	if (conn->direct_io && block->os_cache_max)
		WT_ERR_MSG(session, EINVAL, "os_cache_max not supported in combination with direct_io");
	
	/*读取配置中的最大脏数据长度*/
	WT_ERR(__wt_config_gets(session, cfg, "os_cache_dirty_max", &cval));
	block->os_cache_dirty_max = (size_t)cval.val;

	/*direct io是不允许os 层有文件脏页存在的*/
	if (conn->direct_io && block->os_cache_dirty_max)
		WT_ERR_MSG(session, EINVAL, "os_cache_dirty_max not supported in combination with direct_io");

	/*打开filename文件*/
	WT_ERR(__wt_open(session, filename, 0, 0, readonly ? WT_FILE_TYPE_CHECKPOINT : WT_FILE_TYPE_DATA, &block->fh));

	/*初始化live_lock*/
	WT_ERR(__wt_spin_init(session, &block->live_lock, "block manager"));

	/*除Salvage操作外，都需要读取文件开始的描述信息到block中*/
	if (!forced_salvage)
		WT_ERR(__desc_read(session, block));

	*blockp = block;
	__wt_spin_unlock(session, &conn->block_lock);

	return 0;

err:
	WT_TRET(__block_destroy(session, block));
	__wt_spin_unlock(session, &conn->block_lock);

	return ret;
}

/*关闭session中的一个block,有可能会造成block销毁*/
int __wt_block_close(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	if (block == NULL)				/* Safety check */
		return (0);

	conn = S2C(session);

	WT_TRET(__wt_verbose(session, WT_VERB_BLOCK, "close: %s", block->name == NULL ? "" : block->name ));

	__wt_spin_lock(session, &conn->block_lock);

	/*引用计数是为了延迟释放，可能在调这个函数时，其他地方正在引用这个block*/
	if (block->ref == 0 || --block->ref == 0)
		WT_TRET(__block_destroy(session, block));

	__wt_spin_unlock(session, &conn->block_lock);

	return ret;
}

/*写入一个block header信息*/
int __wt_desc_init(WT_SESSION_IMPL *session, WT_FH *fh, uint32_t allocsize)
{
	WT_BLOCK_DESC *desc;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;

	/*进行buf大小对齐，为了完整写入磁盘*/
	WT_RET(__wt_scr_alloc(session, allocsize, &buf));
	memset(buf->mem, 0, allocsize);

	desc = (WT_BLOCK_DESC*)(buf->mem);
	desc->magic = WT_BLOCK_MAGIC;
	desc->majorv = WT_BLOCK_MAJOR_VERSION;
	desc->minorv = WT_BLOCK_MINOR_VERSION;

	/* Update the checksum. */
	desc->cksum = __wt_cksum(desc, allocsize);

	ret = __wt_write(session, fh, (wt_off_t)0, (size_t)allocsize, desc);

	__wt_scr_free(session, &buf);

	return ret;
}

/*从block对应的文件中读取block的header信息*/
static int __desc_read(WT_SESSION_IMPL* session, WT_BLOCK* block)
{
	WT_BLOCK_DESC *desc;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	uint32_t cksum;

	/*分配一个block对齐大小的缓冲区*/
	WT_RET(__wt_scr_alloc(session, block->allocsize, &buf));
	/*从文件开始处读取一个对齐大小的内容*/
	WT_RET(__wt_read(session, block->fh, (wt_off_t)0, (size_t)block->allocsize, buf->mem));

	desc = (WT_BLOCK_DESC *)(buf->mem);

	WT_ERR(__wt_verbose(session, WT_VERB_BLOCK,
		"%s: magic %" PRIu32
		", major/minor: %" PRIu32 "/%" PRIu32
		", checksum %#" PRIx32,
		block->name, desc->magic,
		desc->majorv, desc->minorv,
		desc->cksum));

	cksum = desc->cksum;
	desc->cksum = 0;
	/*校验魔法字和checksum*/
	if (desc->magic != WT_BLOCK_MAGIC || cksum != __wt_cksum(desc, block->allocsize))
		WT_ERR_MSG(session, WT_ERROR, "%s does not appear to be a WiredTiger file", block->name);

	/*校验block版本信息,低版本wiredtiger引擎不能处理高版本磁盘上的block*/
	if (desc->majorv > WT_BLOCK_MAJOR_VERSION || (desc->majorv == WT_BLOCK_MAJOR_VERSION && desc->minorv > WT_BLOCK_MINOR_VERSION))
		WT_ERR_MSG(session, WT_ERROR,
		"unsupported WiredTiger file version: this build only "
		"supports major/minor versions up to %d/%d, and the file "
		"is version %d/%d",
		WT_BLOCK_MAJOR_VERSION, WT_BLOCK_MINOR_VERSION,
		desc->majorv, desc->minorv);

err:
	__wt_scr_free(session, &buf);
	return ret;
}

/*将block的头信息设置到stats当中,以便统计显示*/
void __wt_block_stat(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_DSRC_STATS *stats)
{
	/*
	 * We're looking inside the live system's structure, which normally
	 * requires locking: the chances of a corrupted read are probably
	 * non-existent, and it's statistics information regardless, but it
	 * isn't like this is a common function for an application to call.
	 */
	__wt_spin_lock(session, &block->live_lock);

	WT_STAT_SET(stats, allocation_size, block->allocsize);
	WT_STAT_SET(stats, block_checkpoint_size, block->live.ckpt_size);
	WT_STAT_SET(stats, block_magic, WT_BLOCK_MAGIC);
	WT_STAT_SET(stats, block_major, WT_BLOCK_MAJOR_VERSION);
	WT_STAT_SET(stats, block_minor, WT_BLOCK_MINOR_VERSION);
	WT_STAT_SET(stats, block_reuse_bytes, block->live.avail.bytes);
	WT_STAT_SET(stats, block_size, block->fh->size);

	__wt_spin_unlock(session, &block->live_lock);
}

