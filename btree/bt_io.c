/***********************************************************
* btree对file block的读写操作实现
***********************************************************/

#include "wt_internal.h"

/*从block addr对应的文件位置中读取对应page的数据，根据btree的属性来觉得是否需要解压*/
int __wt_bt_read(WT_SESSION_IMPL* session, WT_ITEM* buf, const uint8_t* addr, size_t addr_size)
{
	WT_BM* bm;
	WT_BTREE* btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;

	const WT_PAGE_HEADER* dsk;
	size_t result_len;

	btree = S2BT(session);
	bm = btree->bm;

	/*加入btree是一个压缩型的数据存储，必须先解压,那么读取的数据必须先存入一个临时缓冲区中，然后解压到buf中*/
	if (btree->compressor == NULL){
		WT_RET(bm->read(bm, session, buf, addr, addr_size));
		dsk = buf->data;
	}
	else{
		WT_RET(__wt_scr_alloc(session, 0, &tmp));
		WT_ERR(bm->read(bm, session, tmp, addr, addr_size));
		dsk = tmp->data;
	}

	/*判断block数据是否是压缩过的，如果是，进行解压*/
	if (F_ISSET(dsk, WT_PAGE_COMPRESSED)){
		if (btree->compressor == NULL || btree->compressor->decompress == NULL)
			WT_ERR_MSG(session, WT_ERROR, "read compressed block where no compression engine configured");

		WT_ERR(__wt_buf_initsize(session, buf, dsk->mem_size));

		memcpy(buf->mem, tmp->data, WT_BLOCK_COMPRESS_SKIP);
		ret = btree->compressor->decompress(btree->compressor, &session->iface,
			(uint8_t *)tmp->data + WT_BLOCK_COMPRESS_SKIP,
			tmp->size - WT_BLOCK_COMPRESS_SKIP,
			(uint8_t *)buf->mem + WT_BLOCK_COMPRESS_SKIP,
			dsk->mem_size - WT_BLOCK_COMPRESS_SKIP, &result_len);

		/*解压失败或者压缩的数据长度不对，返回一个系统错误，可能是因为磁盘数据损坏造成的*/
		if (ret != 0 || result_len != dsk->mem_size - WT_BLOCK_COMPRESS_SKIP)
			WT_ERR(F_ISSET(btree, WT_BTREE_VERIFY) || F_ISSET(session, WT_SESSION_SALVAGE_CORRUPT_OK) ?
				WT_ERROR :__wt_illegal_value(session, btree->dhandle->name));
	}
	else{
		if (btree->compressor == NULL)
			buf->size = dsk->mem_size;
		else{
			/*
			* We guessed wrong: there was a compressor, but this
			* block was not compressed, and now the page is in the
			* wrong buffer and the buffer may be of the wrong size.
			* This should be rare, but happens with small blocks
			* that aren't worth compressing.
			* 如果数据太短是不会压缩的
			*/
			WT_ERR(__wt_buf_set(session, buf, tmp->data, dsk->mem_size));
		}
	}

	/*进行数据校验*/
	if (F_ISSET(btree, WT_BTREE_VERIFY)) {
		if (tmp == NULL)
			WT_ERR(__wt_scr_alloc(session, 0, &tmp));

		WT_ERR(bm->addr_string(bm, session, tmp, addr, addr_size));
		WT_ERR(__wt_verify_dsk(session, (const char *)tmp->data, buf));
	}

	/*修改统计信息*/
	WT_STAT_FAST_CONN_INCR(session, cache_read);
	WT_STAT_FAST_DATA_INCR(session, cache_read);
	if (F_ISSET(dsk, WT_PAGE_COMPRESSED))
		WT_STAT_FAST_DATA_INCR(session, compress_read);
	WT_STAT_FAST_CONN_INCRV(session, cache_bytes_read, dsk->mem_size);
	WT_STAT_FAST_DATA_INCRV(session, cache_bytes_read, dsk->mem_size);

err:
	__wt_scr_free(session, &tmp);
}

/*将buf中的数据写入到addr对应page中的block中*/
int __wt_bt_write(WT_SESSION_IMPL* session, WT_ITEM* buf, uint8_t* addr, size_t* addr_sizep, int checkpoint, int compressed)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_ITEM *ip;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE_HEADER *dsk;
	size_t len, src_len, dst_len, result_len, size;
	int data_cksum, compression_failed;
	uint8_t *src, *dst;

	btree = S2BT(session);
	bm = btree->bm;

	WT_ASSERT(session, (checkpoint == 0 && addr != NULL && addr_sizep != NULL) || (checkpoint == 1 && addr == NULL && addr_sizep == NULL));

	/*对数据压缩的判断，如果要压缩，必须先将buf中的数据通过btree->compressor->compress函数压缩到一个临时缓冲区中*/
	if (btree->compressor == NULL || btree->compressor->compress == NULL || compressed)
		ip = buf;
	else if (buf->size <= btree->allocsize) /*数据太短了，不做压缩*/
		ip = buf;
	else{ /*进行数据压缩*/
		src = (uint8_t *)buf->mem + WT_BLOCK_COMPRESS_SKIP;
		src_len = buf->size - WT_BLOCK_COMPRESS_SKIP;

		/*预先计算压缩后的数据长度，长度用于分配临时缓冲区和标记page header*/
		if (btree->compressor->pre_size == NULL)
			len = src_len;
		else
			WT_ERR(btree->compressor->pre_size(btree->compressor, &session->iface, src, src_len, &len));

		/*计算block的数据长度size*/
		size = len + WT_BLOCK_COMPRESS_SKIP;
		WT_ERR(bm->write_size(bm, session, &size));
		WT_ERR(__wt_scr_alloc(session, size, &tmp));

		dst = (uint8_t *)tmp->mem + WT_BLOCK_COMPRESS_SKIP;
		dst_len = len;

		/*数据压缩*/
		compression_failed = 0;
		WT_ERR(btree->compressor->compress(btree->compressor,
			&session->iface,
			src, src_len,
			dst, dst_len,
			&result_len, &compression_failed));
		result_len += WT_BLOCK_COMPRESS_SKIP;

		/*假如数据压缩失败，直接写入原始数据*/
		if (compression_failed || buf->size / btree->allocsize <= result_len / btree->allocsize) {
			ip = buf;
			WT_STAT_FAST_DATA_INCR(session, compress_write_fail);
		}
		else{
			compressed = 1;
			WT_STAT_FAST_DATA_INCR(session, compress_write);

			memcpy(tmp->mem, buf->mem, WT_BLOCK_COMPRESS_SKIP);
			tmp->size = result_len;
			ip = tmp;
		}
	}

	/*将ip的mem作为page的空间首地址*/
	dsk = ip->mem;
	/*设置page的压缩标志*/
	if (compressed)
		F_SET(dsk, WT_PAGE_COMPRESSED);

	/*
	* We increment the block's write generation so it's easy to identify
	* newer versions of blocks during salvage.  (It's common in WiredTiger,
	* at least for the default block manager, for multiple blocks to be
	* internally consistent with identical first and last keys, so we need
	* a way to know the most recent state of the block.  We could check
	* which leaf is referenced by a valid internal page, but that implies
	* salvaging internal pages, which I don't want to do, and it's not
	* as good anyway, because the internal page may not have been written
	* after the leaf page was updated.  So, write generations it is.
	*
	* Nothing is locked at this point but two versions of a page with the
	* same generation is pretty unlikely, and if we did, they're going to
	* be roughly identical for the purposes of salvage, anyway.
	*/
	dsk->write_gen = ++btree->write_gen;

	switch (btree->checksum){
	case CKSUM_ON:
		data_cksum = 1;
		break;

	case CKSUM_OFF:
		data_cksum = 0;
		break;

	case CKSUM_UNCOMPRESSED:
	default:
		data_cksum = !compressed;
		break;
	}

	/*进行checkpoint数据落盘*/
	WT_ERR(checkpoint ? bm->checkpoint(bm, session, ip, btree->ckpt, data_cksum) : bm->write(bm, session, ip, addr, addr_sizep, data_cksum));

	WT_STAT_FAST_CONN_INCR(session, cache_write);
	WT_STAT_FAST_DATA_INCR(session, cache_write);
	WT_STAT_FAST_CONN_INCRV(session, cache_bytes_write, dsk->mem_size);
	WT_STAT_FAST_DATA_INCRV(session, cache_bytes_write, dsk->mem_size);

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

