/*************************************************************************
*初始化block manager及其管理回调函数
**************************************************************************/

#include "wt_internal.h"

static void __bm_method_set(WT_BM *, int);


static int __bm_readonly(WT_BM* bm, WT_SESSION_IMPL* session)
{
	WT_RET_MSG(session, ENOTSUP, "%s: write operation on read-only checkpoint handle", bm->block->name);
}

/*将block addr格式化到buf中*/
static int __bm_addr_string(WT_BM* bm, WT_SESSION_IMPL* session, WT_ITEM* buf, const uint8_t* addr, size_t addr_size)
{
	return __wt_block_addr_string(session, bm->block, buf, addr, addr_size);
}

static int __bm_addr_valid(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__wt_block_addr_valid(session, bm->block, addr, addr_size, bm->is_live));
}

static u_int __bm_block_header(WT_BM *bm)
{
	return (__wt_block_header(bm->block));
}

/*将buffer数据写入到block，并建立一个checkpoint*/
static int __bm_checkpoint(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_CKPT *ckptbase, int data_cksum)
{
	return __wt_block_checkpoint(session, bm->block, buf, ckptbase, data_cksum);
}

/*将bm->block对应的文件脏数据sync到磁盘上*/
static int __bm_sync(WT_BM *bm, WT_SESSION_IMPL *session, int async)
{
	return (async ? __wt_fsync_async(session, bm->block->fh) :
	__wt_fsync(session, bm->block->fh));
}

/*载入一个block checkpoint*/
static int __bm_checkpoint_load(WT_BM* bm, WT_SESSION_IMPL* session, const uint8_t* addr, size_t addr_size,
								 uint8_t *root_addr, size_t *root_addr_sizep, int checkpoint)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*加入没有打开一个checkpoint，将直接将system有不活跃状态变成活跃状态*/
	bm->is_live = !checkpoint;
	WT_RET(__wt_block_checkpoint_load(session, bm->block, addr, addr_size, root_addr, root_addr_sizep, checkpoint));
	if(checkpoint){
		if (conn->mmap) /*使用mmap方式来做文件读写*/
			WT_RET(__wt_block_map(session, bm->block, &bm->map, &bm->maplen, &bm->mappingcookie));

		__bm_method_set(bm, 1);
	}

	return 0;
}

static int __bm_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return __wt_block_checkpoint_resolve(session, bm->block);
}

/*卸载一个checkpoint*/
static int __bm_checkpoint_unload(WT_BM *bm, WT_SESSION_IMPL *session)
{
	WT_DECL_RET;

	/* Unmap any mapped segment. */
	if (bm->map != NULL)
		WT_TRET(__wt_block_unmap(session,bm->block, bm->map, bm->maplen, &bm->mappingcookie));

	/* Unload the checkpoint. */
	WT_TRET(__wt_block_checkpoint_unload(session, bm->block, !bm->is_live));

	return ret;
}

/*关闭一个block manager*/
static int __bm_close(WT_BM* bm, WT_SESSION_IMPL* session)
{
	WT_DECL_RET;

	if (bm == NULL)				/* Safety check */
		return 0;

	ret = __wt_block_close(session, bm->block);

	__wt_overwrite_and_free(session, bm);
	return ret;
}

/*开始一个block manager的合并操作*/
static int __bm_compact_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return (__wt_block_compact_start(session, bm->block));
}

/*为合并操作获取一个可以用的page*/
static int __bm_compact_page_skip(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, int *skipp)
{
	return (__wt_block_compact_page_skip(session, bm->block, addr, addr_size, skipp));
}

/*返回一个可以可合并的文件*/
static int __bm_compact_skip(WT_BM *bm, WT_SESSION_IMPL *session, int *skipp)
{
	return __wt_block_compact_skip(session, bm->block, skipp);
}

/*结束一个block manager*/
static int __bm_compact_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return (__wt_block_compact_end(session, bm->block));
}

/*block manager释放一个block*/
static int __bm_free(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__wt_block_free(session, bm->block, addr, addr_size));
}

/*查询block manager的状态信息*/
static int __bm_stat(WT_BM *bm, WT_SESSION_IMPL *session, WT_DSRC_STATS *stats)
{
	__wt_block_stat(session, bm->block, stats);
	return (0);
}

/*将一个buf中的数据写入到block对应的文件中，并就算block addr信息(addr)*/
static int __bm_write(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t *addr, size_t *addr_sizep, int data_cksum)
{
	return __wt_block_write(session, bm->block, buf, addr, addr_sizep, data_cksum);
}

/*计算buffer数据写入的长度,会进行block->allocsize方式长度对齐*/
static int __bm_write_size(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
	return __wt_block_write_size(session, bm->block, sizep);
}

/*开始修复一个block?*/
static int __bm_salvage_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return (__wt_block_salvage_start(session, bm->block));
}

static int __bm_salvage_valid(WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, int valid)
{
	return __wt_block_salvage_valid(session, bm->block, addr, addr_size, valid);
}


static int __bm_salvage_next(WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, int *eofp)
{
	return (__wt_block_salvage_next(session, bm->block, addr, addr_sizep, eofp));
}

static int __bm_salvage_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return (__wt_block_salvage_end(session, bm->block));
}

/*
 * __bm_verify_start --
 *	Start a block manager verify.
 */
static int __bm_verify_start(WT_BM *bm, WT_SESSION_IMPL *session, WT_CKPT *ckptbase)
{
	return (__wt_block_verify_start(session, bm->block, ckptbase));
}

/*
 * __bm_verify_addr --
 *	Verify an address.
 */
static int __bm_verify_addr(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__wt_block_verify_addr(session, bm->block, addr, addr_size));
}

/*
 * __bm_verify_end --
 *	End a block manager verify.
 */
static int __bm_verify_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
	return (__wt_block_verify_end(session, bm->block));
}

/*安装和设置block manager的处理函数*/
static void __bm_method_set(WT_BM *bm, int readonly)
{
	if (readonly) { /*只读的block manager*/
		bm->addr_string = __bm_addr_string;
		bm->addr_valid = __bm_addr_valid;
		bm->block_header = __bm_block_header;
		bm->checkpoint = (int (*)(WT_BM *,WT_SESSION_IMPL *, WT_ITEM *, WT_CKPT *, int))__bm_readonly;
		bm->checkpoint_load = __bm_checkpoint_load;
		bm->checkpoint_resolve =(int (*)(WT_BM *, WT_SESSION_IMPL *))__bm_readonly;
		bm->checkpoint_unload = __bm_checkpoint_unload;
		bm->close = __bm_close;
		bm->compact_end =(int (*)(WT_BM *, WT_SESSION_IMPL *))__bm_readonly;
		bm->compact_page_skip = (int (*)(WT_BM *, WT_SESSION_IMPL *,const uint8_t *, size_t, int *))__bm_readonly;
		bm->compact_skip = (int (*)(WT_BM *, WT_SESSION_IMPL *, int *))__bm_readonly;
		bm->compact_start =(int (*)(WT_BM *, WT_SESSION_IMPL *))__bm_readonly;
		bm->free = (int (*)(WT_BM *,WT_SESSION_IMPL *, const uint8_t *, size_t))__bm_readonly;
		bm->preload = __wt_bm_preload;
		bm->read = __wt_bm_read;
		bm->salvage_end = (int (*)(WT_BM *, WT_SESSION_IMPL *))__bm_readonly;
		bm->salvage_next = (int (*)(WT_BM *, WT_SESSION_IMPL *,uint8_t *, size_t *, int *))__bm_readonly;
		bm->salvage_start = (int (*)(WT_BM *, WT_SESSION_IMPL *))__bm_readonly;
		bm->salvage_valid = (int (*)(WT_BM *,WT_SESSION_IMPL *, uint8_t *, size_t, int))__bm_readonly;
		bm->stat = __bm_stat;
		bm->sync =(int (*)(WT_BM *, WT_SESSION_IMPL *, int))__bm_readonly;
		bm->verify_addr = __bm_verify_addr;
		bm->verify_end = __bm_verify_end;
		bm->verify_start = __bm_verify_start;
		bm->write = (int (*)(WT_BM *, WT_SESSION_IMPL *,WT_ITEM *, uint8_t *, size_t *, int))__bm_readonly;
		bm->write_size = (int (*)(WT_BM *, WT_SESSION_IMPL *, size_t *))__bm_readonly;
	} 
	else { /*读写的block manager*/
		bm->addr_string = __bm_addr_string;
		bm->addr_valid = __bm_addr_valid;
		bm->block_header = __bm_block_header;
		bm->checkpoint = __bm_checkpoint;
		bm->checkpoint_load = __bm_checkpoint_load;
		bm->checkpoint_resolve = __bm_checkpoint_resolve;
		bm->checkpoint_unload = __bm_checkpoint_unload;
		bm->close = __bm_close;
		bm->compact_end = __bm_compact_end;
		bm->compact_page_skip = __bm_compact_page_skip;
		bm->compact_skip = __bm_compact_skip;
		bm->compact_start = __bm_compact_start;
		bm->free = __bm_free;
		bm->preload = __wt_bm_preload;
		bm->read = __wt_bm_read;
		bm->salvage_end = __bm_salvage_end;
		bm->salvage_next = __bm_salvage_next;
		bm->salvage_start = __bm_salvage_start;
		bm->salvage_valid = __bm_salvage_valid;
		bm->stat = __bm_stat;
		bm->sync = __bm_sync;
		bm->verify_addr = __bm_verify_addr;
		bm->verify_end = __bm_verify_end;
		bm->verify_start = __bm_verify_start;
		bm->write = __bm_write;
		bm->write_size = __bm_write_size;
	}
}

/*打开一个block manager*/
int __wt_block_manager_open(WT_SESSION_IMPL *session, const char *filename, const char *cfg[],
						int forced_salvage, int readonly, uint32_t allocsize, WT_BM **bmp)
{
	WT_BM *bm;
	WT_DECL_RET;

	*bmp = NULL;

	WT_RET(__wt_calloc_one(session, &bm));
	__bm_method_set(bm, 0);

	WT_ERR(__wt_block_open(session, filename, cfg,
		forced_salvage, readonly, allocsize, &bm->block));

	*bmp = bm;
	return (0);

err:	
	WT_TRET(bm->close(bm, session));
	return (ret);
}
