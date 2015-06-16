/***************************************************************************
*block的compact操作实现
***************************************************************************/
#include "wt_internal.h"

static int __block_dump_avail(WT_SESSION_IMPL* session, WT_BLOCK* block);

/*启动block文件的compact操作*/
int __wt_block_compact_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_UNUSED(session);

	/* Switch to first-fit allocation. */
	__wt_block_configure_first_fit(block, 1);
	/*compact完成的百分比，开始为0*/
	block->compact_pct_tenths = 0;

	return 0;
}

/*结束block文件的compact操作*/
int __wt_block_compact_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_UNUSED(session);

	/* Restore the original allocation plan. */
	__wt_block_configure_first_fit(block, 0);

	block->compact_pct_tenths = 0;

	return 0;
}

/*判断block对应的文件是否需要compact操作，如果skipp =1，表示不需要,否则需要进行compact*/
int __wt_block_compact_skip(WT_SESSION_IMPL *session, WT_BLOCK *block, int *skipp)
{
	WT_DECL_RET;
	WT_EXT *ext;
	WT_EXTLIST *el;
	WT_FH *fh;
	wt_off_t avail_eighty, avail_ninety, eighty, ninety;

	*skipp = 1;

	fh = block->fh;

	/*
	 * We do compaction by copying blocks from the end of the file to the
	 * beginning of the file, and we need some metrics to decide if it's
	 * worth doing.  Ignore small files, and files where we are unlikely
	 * to recover 10% of the file.
	 */

	/*文件太小，不能进行compaction*/
	if(fh->size <= 10 * 1024)
		return 0;

	__wt_spin_lock(session, &block->live_lock);

	if(WT_VERBOSE_ISSET(session, WT_VERB_COMPACT))
		WT_ERR(__block_dump_avail(session, block));

	avail_eighty =0;
	avail_ninety = 0;
	ninety = fh->size - fh->size / 10;
	eighty = fh->size - ((fh->size / 10) * 2);

	el = &block->live.avail;
	/*从el->off的头上开始遍历*/
	WT_EXT_FOREACH(ext, el->off){
		if (ext->off < ninety) { /*位置没有占满90%*/
			avail_ninety += ext->size; /*进行累加,统计90% ext的大小总和*/
			if (ext->off < eighty)/*位置没有占满90%*/
				avail_eighty += ext->size;/*进行累加,统计80% ext的大小总和*/
		}
	}
	/*输出compact的文件大小和百分比*/
	WT_ERR(__wt_verbose(session, WT_VERB_COMPACT,"%s: %" PRIuMAX "MB (%" PRIuMAX ") available space in the first 80%% of the file", block->name, 
		(uintmax_t)avail_eighty / WT_MEGABYTE, (uintmax_t)avail_eighty));

	WT_ERR(__wt_verbose(session, WT_VERB_COMPACT, "%s: %" PRIuMAX "MB (%" PRIuMAX ") available space in the first 90%% of the file",
		block->name, (uintmax_t)avail_ninety / WT_MEGABYTE, (uintmax_t)avail_ninety));

	WT_ERR(__wt_verbose(session, WT_VERB_COMPACT, "%s: require 10%% or %" PRIuMAX "MB (%" PRIuMAX ") in the first 90%% of the file to perform compaction, compaction %s",
		block->name, (uintmax_t)(fh->size / 10) / WT_MEGABYTE, (uintmax_t)fh->size / 10, *skipp ? "skipped" : "proceeding"));

	/*文件前面0 ~ 90%位置上存储的ext的数据量大于文件大小的1/10时,说明10%的有效数据在前面90%的空间上,
	 *需要进行compact操作,那么只compact文件末尾的10%的数据*/
	if (avail_ninety >= fh->size / 10) {
		*skipp = 0;
		block->compact_pct_tenths = 1;
		if (avail_eighty >= ((fh->size / 10) * 2))
			block->compact_pct_tenths = 2;
	}

err:
	__wt_spin_unlock(session, &block->live_lock);
	return ret;
}

/*判断addr对应的block数据是否需要进行compact,skipp = 1表示不需要*/
int __wt_block_compact_page_skip(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr, size_t addr_size, int *skipp)
{
	WT_DECL_RET;
	WT_EXT *ext;
	WT_EXTLIST *el;
	WT_FH *fh;
	wt_off_t limit, offset;
	uint32_t size, cksum;

	WT_UNUSED(addr_size);
	*skipp = 1;				/* Return a default skip. */

	fh = block->fh;

	/*从addr中读取block的位置信息和checksum*/
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	__wt_spin_lock(session, &block->live_lock);

	/*计算compact文件的起始位置,从文件后面开始compact*/
	limit = fh->size - ((fh->size / 10) * block->compact_pct_tenths);
	/*block处于compact的范围中*/
	if (offset > limit) {
		el = &block->live.avail;
		WT_EXT_FOREACH(ext, el->off) {
			if (ext->off >= limit)
				break;

			if (ext->size >= size) {
				*skipp = 0;
				break;
			}
		}
	}

	__wt_spin_unlock(session, &block->live_lock);

	return ret;
}

/*dump block中可用ext的compact进度*/
static int __block_dump_avail(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_EXTLIST *el;
	WT_EXT *ext;
	wt_off_t decile[10], percentile[100], size, v;
	u_int i;

	el = &block->live.avail;
	size = block->fh->size;

	WT_RET(__wt_verbose(session, WT_VERB_COMPACT,  "file size %" PRIuMAX "MB (%" PRIuMAX ") with %" PRIuMAX "%% space available %" PRIuMAX "MB (%" PRIuMAX ")",
	    (uintmax_t)size / WT_MEGABYTE, (uintmax_t)size,
	    ((uintmax_t)el->bytes * 100) / (uintmax_t)size,
	    (uintmax_t)el->bytes / WT_MEGABYTE, (uintmax_t)el->bytes));

	if (el->entries == 0)
		return (0);

	/*
	 * Bucket the available memory into file deciles/percentiles.  Large
	 * pieces of memory will cross over multiple buckets, assign to the
	 * decile/percentile in 512B chunks.
	 */
	memset(decile, 0, sizeof(decile));
	memset(percentile, 0, sizeof(percentile));
	WT_EXT_FOREACH(ext, el->off)
		for (i = 0; i < ext->size / 512; ++i) {
			++decile[((ext->off + i * 512) * 10) / size];
			++percentile[((ext->off + i * 512) * 100) / size];
		}


	for (i = 0; i < WT_ELEMENTS(decile); ++i) {
		v = decile[i] * 512;
		WT_RET(__wt_verbose(session, WT_VERB_COMPACT,
		    "%2u%%: %12" PRIuMAX "MB, (%" PRIuMAX "B, %"
		    PRIuMAX "%%)",
		    i * 10, (uintmax_t)v / WT_MEGABYTE, (uintmax_t)v,
		    (uintmax_t)((v * 100) / (wt_off_t)el->bytes)));
	}

	return (0);
}

