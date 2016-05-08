/**************************************************************************
*对block进行文件空间校验
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

/*开始一个对block的核实校验*/
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

/*校验最后一个checkpoint中的avail list是否是空闲的*/
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

/*将checkpoint alloc未使用的ext进行bit = 1范围标识*/
int __wt_verify_ckpt_load(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
{
	WT_EXTLIST *el;
	WT_EXT *ext;
	uint64_t frag, frags;

	block->verify_size = ci->file_size;

	/*将checkpoint元数据位置也设置到filefrag中,因为这个是算使用了的ext对象*/
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
		WT_RET(__wt_block_extlist_merge(session, el, &block->verify_alloc));
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

	/*将整个ext对应的frag置为1*/
	WT_EXT_FOREACH(ext, el->off) {
		frag = (uint64_t)WT_wt_off_TO_FRAG(block, ext->off);
		frags = (uint64_t)(ext->size / block->allocsize);
		/*将ext对应的范围全部置为1*/
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

/*将addr对应的(off,size)ext在filefrags对应的位范围全部值为1，对应ckptfrags位范围全部值为0,相当于
 *从ckptfrags转移到filefrags中*/
int __wt_block_verify_addr(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr, size_t addr_size)
{
	wt_off_t offset;
	uint32_t cksum, size;

	WT_UNUSED(addr_size);

	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	WT_RET(__verify_filefrag_add(session, block, NULL, offset, size, 0));
	WT_RET(__verify_ckptfrag_add(session, block, offset, size));

	return 0;
}

/*更改某个ext(off,size)对应的filefrag，将其所有的bit位值置为1*/
static int __verify_filefrag_add(WT_SESSION_IMPL *session, WT_BLOCK *block, const char *type, wt_off_t offset, wt_off_t size, int nodup)
{
	uint64_t f, frag, frags, i;

	WT_RET(__wt_verbose(session, WT_VERB_VERIFY,
		"add file block%s%s%s at %" PRIuMAX "-%" PRIuMAX " (%" PRIuMAX ")",
		type == NULL ? "" : " (",type == NULL ? "" : type, type == NULL ? "" : ")", (uintmax_t)offset, (uintmax_t)(offset + size), (uintmax_t)size));

	/*判断(off, size)的合法性*/
	if(offset + size > block->fh->size){
		WT_RET_MSG(session, WT_ERROR,
			"fragment %" PRIuMAX "-%" PRIuMAX " references "
			"non-existent file blocks",
			(uintmax_t)offset, (uintmax_t)(offset + size));
	}

	/*计算ext在frags中的起始位置和范围*/
	frag = (uint64_t)WT_wt_off_TO_FRAG(block, offset);
	frags = (uint64_t)(size / block->allocsize);

	if(nodup){
		for (f = frag, i = 0; i < frags; ++f, ++i)
			if (__bit_test(block->fragfile, f)) /*判断f位上是否已经有1*/
				WT_RET_MSG(session, WT_ERROR, "file fragment at %" PRIuMAX " referenced multiple times", (uintmax_t)offset);
	}

	/*将ext（off,size）范围设置为1*/
	__bit_nset(block->fragfile, frag, frag + (frags - 1));

	return 0;
}

/*fragefile所有bit位必须全为1*/
static int __verify_filefrag_chk(WT_SESSION_IMPL* session, WT_BLOCK* block)
{
	uint64_t count, first, last;

	if(block->frags == 0)
		return 0;

	/*将整个fragefile未使用的末尾部分填充为1，直到一个有填充的块上*/
	for (last = block->frags - 1; last != 0; --last) {
		if (__bit_test(block->fragfile, last))
			break;

		__bit_set(block->fragfile, last);
	}

	for(count = 0;; ++ count){
		/*确定第一个不为0的bit位置序号,如果有不为1的位，那么count一定会>1,也就表示中间有一个ext没有被使用,这是违背设计初衷的*/
		if (__bit_ffc(block->fragfile, block->frags, &first) != 0)
			break;

		for (last = first + 1; last < block->frags; ++last) {
			if (__bit_test(block->fragfile, last))
				break;
			/*将0位设置为1*/
			__bit_set(block->fragfile, last);
		}

		if (!WT_VERBOSE_ISSET(session, WT_VERB_VERIFY))
			continue;

		__wt_errx(session,
			"file range %" PRIuMAX "-%" PRIuMAX " never verified",
			(uintmax_t)WT_FRAG_TO_OFF(block, first),
			(uintmax_t)WT_FRAG_TO_OFF(block, last));
	}

	if(count == 0)
		return 0;

	__wt_errx(session, "file ranges never verified: %" PRIu64, count);

	return WT_ERROR;
}

/*将checkpoint frags的(off,size)对应的bit清空,这个位置在checkpoint frags的bit范围中的值必须全是1*/
static int __verify_ckptfrag_add(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t offset, wt_off_t size)
{
	uint64_t f, frag, frags, i;

	WT_RET(__wt_verbose(session, WT_VERB_VERIFY, "add checkpoint block at %" PRIuMAX "-%" PRIuMAX " (%" PRIuMAX ")",
		(uintmax_t)offset, (uintmax_t)(offset + size), (uintmax_t)size));

	/*校验(off,size)的合法性*/
	if (offset + size > block->verify_size){
		WT_RET_MSG(session, WT_ERROR,
		"fragment %" PRIuMAX "-%" PRIuMAX " references "
		"file blocks outside the checkpoint",
		(uintmax_t)offset, (uintmax_t)(offset + size));
	}

	frag = (uint64_t)WT_wt_off_TO_FRAG(block, offset);
	frags = (uint64_t)(size / block->allocsize);

	/*校验(off,size)对应的fragckpt的位置是否全为1，如果不是，说明有问题*/
	for (f = frag, i = 0; i < frags; ++f, ++i){
		if (!__bit_test(block->fragckpt, f))
			WT_RET_MSG(session, WT_ERROR,
			"fragment at %" PRIuMAX " referenced multiple "
			"times in a single checkpoint or found in the "
			"checkpoint but not listed in the checkpoint's "
			"allocation list",	(uintmax_t)offset);
	}

	__bit_nclr(block->fragckpt, frag, frag + (frags - 1));

	return 0;
}

/*检查session->fragckpt是否全为0，如果不为0，表示fragckpt不合法*/
static int __verify_ckptfrag_chk(WT_SESSION_IMPL* session, WT_BLOCK* block)
{
	uint64_t count, first, last;

	if(block->fragckpt == NULL)
		return 0;

	/*检查checkpoint fragckpt应该是全部被0填充，如果有不为0的bit，说明fragckpt不合法*/
	for (count = 0;; ++count) {
		if (__bit_ffs(block->fragckpt, block->frags, &first) != 0)
			break;

		__bit_clear(block->fragckpt, first);

		for (last = first + 1; last < block->frags; ++last) {
			if (!__bit_test(block->fragckpt, last))
				break;

			__bit_clear(block->fragckpt, last);
		}

		if (!WT_VERBOSE_ISSET(session, WT_VERB_VERIFY))
			continue;

		__wt_errx(session,
			"checkpoint range %" PRIuMAX "-%" PRIuMAX " never verified",
			(uintmax_t)WT_FRAG_TO_OFF(block, first),
			(uintmax_t)WT_FRAG_TO_OFF(block, last));
	}

	if (count == 0)
		return 0;

	__wt_errx(session, "checkpoint ranges never verified: %" PRIu64, count);

	return WT_ERROR;
}







