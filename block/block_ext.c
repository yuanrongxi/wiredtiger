/***************************************************************************
*WT_EXT和WT_SIZE相关跳表的实现
*block磁盘空间的分配操作
*block内存查找实现
***************************************************************************/

#include "wt_internal.h"

static int __block_append(WT_SESSION_IMPL *, WT_EXTLIST *, wt_off_t, wt_off_t);
static int __block_ext_overlap(WT_SESSION_IMPL *, WT_BLOCK *, WT_EXTLIST *, WT_EXT **, WT_EXTLIST *, WT_EXT **);
static int __block_extlist_dump(WT_SESSION_IMPL *, const char *, WT_EXTLIST *, int);
static int __block_merge(WT_SESSION_IMPL *, WT_EXTLIST *, wt_off_t, wt_off_t);

/*获得跳表每一层的最后一个单元，并存入stack中.返回值是第0层底层的最后一个单元，也就是跳表的最后一个单元*/
static inline WT_EXT* __block_off_srch_last(WT_EXT** head, WT_EXT ***stack)
{
	WT_EXT** extp;
	WT_EXT* last;
	int i;

	last = NULL;

	for(i = WT_SKIP_MAXDEPTH - 1, extp = &head[i]; i>=0;){
		if (*extp != NULL) {
			last = *extp;
			extp = &(*extp)->next[i];
		} 
		else
			stack[i--] = extp--;
	}

	return last;
}

/*在跳表中进行off位置查找，并标识整体查找路径，把路径单元存入stack中*/
static inline void __block_off_srch(WT_EXT **head, wt_off_t off, WT_EXT ***stack, int skip_off)
{
	WT_EXT** extp;
	int i;

	for(i = WT_SKIP_MAXDEPTH - 1, extp = &head[i]; i >= 0;){
		if (*extp != NULL && (*extp)->off < off)
			extp = &(*extp)->next[i + (skip_off ? (*extp)->depth : 0)];
		else /*在下一层进行查找，下一层跳表的细度更新,直到0层为止*/
			stack[i--] = extp--;
	}
}

/*获得对应ext大小与size匹配的跳表查找路径*/
static inline int __block_first_srch(WT_EXT** head, wt_off_t size, WT_EXT*** stack)
{
	WT_EXT *ext;
	
	/*在跳表的0层进行遍历，查找ext->size与size匹配的单元,O(n)*/
	WT_EXT_FOREACH(ext, head){
		if (ext->size >= size)
			break;
	}

	if (ext == NULL)
		return 0;

	/*进行ext的跳表路径标记近似O(logN)*/
	__block_off_srch(head, ext->off, stack, 0);

	return 1;
}

/*在WT_SIZE的跳表中通过size定位对应的WT_SIZE,并获得查找路径,复杂度近似O(LogN)*/
static inline void __block_size_srch(WT_SIZE** head, wt_off_t size, WT_SIZE*** stack)
{
	WT_SIZE **szp;
	int i;

	for (i = WT_SKIP_MAXDEPTH - 1, szp = &head[i]; i >= 0;)
		if (*szp != NULL && (*szp)->size < size)
			szp = &(*szp)->next[i];
		else
			stack[i--] = szp--;
}

/*查找off对应WT_EXT跳表中的前一个位置和后一个位置*/
static inline void __block_off_srch_pair(WT_EXTLIST* el, wt_off_t off, WT_EXT** beforep, WT_EXT** afterp)
{
	WT_EXT **head, **extp;
	int i;

	*beforep =NULL;
	*afterp = NULL;

	head = el->off;

	for (i = WT_SKIP_MAXDEPTH - 1, extp = &head[i]; i >= 0;) {
		if (*extp == NULL) { /*本层已经到末尾,进行下一层查找*/
			--i;
			--extp;
			continue;
		}

		if ((*extp)->off < off) {
			*beforep = *extp;
			extp = &(*extp)->next[i];
		} 
		else { /*往下一层查找了*/		
			*afterp = *extp;
			--i;
			--extp;
		}
	}
}

/*向WT_EXT跳表中插入一个ext单元,同时也会插入一个WT_SIZE到对应的跳表中*/
static int __block_ext_insert(WT_SESSION_IMPL* session, WT_EXTLIST* el, WT_EXT* ext)
{
	WT_EXT **astack[WT_SKIP_MAXDEPTH];
	WT_SIZE *szp, **sstack[WT_SKIP_MAXDEPTH];
	u_int i;

	if (el->track_size) { /*需要进行size查找，则需要先将size信息插入到WT_SIZE跳表中*/
		/*先通过ext->size定位到在WT_SIZE跳表中的查找路径*/
		__block_size_srch(el->sz, ext->size, sstack);

		szp = *sstack[0];
		if (szp == NULL || szp->size != ext->size) {
			/*分配一个WT_SIZE对象，并将size和WT_EXT对应的深度设置到WT_SIZE中，并进行跳表插入*/
			WT_RET(__wt_block_size_alloc(session, &szp));
			szp->size = ext->size;
			szp->depth = ext->depth;
			for (i = 0; i < ext->depth; ++i) {
				szp->next[i] = *sstack[i];
				*sstack[i] = szp;
			}
		}

		/*进行WT_EXT对应的跳表插入,在WT_EXT的的分配了2 倍skipdepth的缓冲区，0 ~ skipdepth是一个跳表关联关系，
		skipdepth ~ 2 x skipdepth是一个关联关系，这里更新的是后者，在if结束后会更新前者,可能是为了和WT_SIZE保持一致？*/
		__block_off_srch(szp->off, ext->off, astack, 1);
		for (i = 0; i < ext->depth; ++i) {
			ext->next[i + ext->depth] = *astack[i];
			*astack[i] = ext;
		}
	}
	
	__block_off_srch(el->off, ext->off, astack, 0);
	for (i = 0; i < ext->depth; ++i){
		ext->next[i] = *astack[i];
		*astack[i] = ext;
	}

	/*计数器更新*/
	++el->entries;
	el->bytes += (uint64_t)ext->size;

	/*设置最后一个单元标识*/
	if (ext->next[0] == NULL)
		el->last = ext;

	return 0;
}

/*向off skip list中插入一个off对应关系*/
static int __block_off_insert(WT_SESSION_IMPL* session, WT_EXTLIST* el, wt_off_t off, wt_off_t size)
{
	WT_EXT *ext;

	WT_RET(__wt_block_ext_alloc(session, &ext));
	ext->off = off;
	ext->size = size;

	return __block_ext_insert(session, el, ext);
}

/*进行跳表中的off删除*/
static int __block_off_remove(WT_SESSION_IMPL* session, WT_EXTLIST* el, wt_off_t off, WT_EXT** extp)
{
	WT_EXT *ext, **astack[WT_SKIP_MAXDEPTH];
	WT_SIZE *szp, **sstack[WT_SKIP_MAXDEPTH];
	u_int i;

	/*在WT_EXT的0 ~ skipdepth跳表关系中进行删除*/
	__block_off_srch(el->off, off, astack, 0);
	ext = *astack[0];
	if (ext == NULL || ext->off != off)
		goto corrupt;
	/*进行关系解除*/
	for (i = 0; i < ext->depth; ++i)
		*astack[i] = ext->next[i];

	/*如果设置了WT_SIZE跳表关联，进行相对应的删除*/
	if (el->track_size) {
		__block_size_srch(el->sz, ext->size, sstack);
		szp = *sstack[0];
		if (szp == NULL || szp->size != ext->size)
			return (EINVAL);

		__block_off_srch(szp->off, off, astack, 1);
		ext = *astack[0];
		if (ext == NULL || ext->off != off)
			goto corrupt;

		for (i = 0; i < ext->depth; ++i)
			*astack[i] = ext->next[i + ext->depth];

		if (szp->off[0] == NULL) {
			for (i = 0; i < szp->depth; ++i)
				*sstack[i] = szp->next[i];

			__wt_block_size_free(session, szp);
		}
	}

	--el->entries;
	el->bytes -= (uint64_t)ext->size;

	if (extp == NULL) /*直接进行extp回收*/
		__wt_block_ext_free(session, ext);
	else
		*extp = ext;

	if (el->last == ext)
		el->last = NULL;

	return 0;

corrupt: 
	WT_PANIC_RET(session, EINVAL, "attempt to remove non-existent offset from an extent list");
}

/*将off对应的WT_EXT进行范围分裂，分裂成两个WT_EXT,并重新插入skip list中,中间会删除off后面对应的size个数据
*	ext     |---------|--------remove range------|------------------|
*		ext->off	off						off + size
* 删除	ext1|---------|				ext2		 |------------------|
*		   a_off								b_off
*/
int __wt_block_off_remove_overlap(WT_SESSION_IMPL *session, WT_EXTLIST *el, wt_off_t off, wt_off_t size)
{
	WT_EXT *before, *after, *ext;
	wt_off_t a_off, a_size, b_off, b_size;

	WT_ASSERT(session, off != WT_BLOCK_INVALID_OFFSET);

	/*匹配到off对应的位置*/
	__block_off_srch_pair(el, off, &before, &after);
	/*before就是off对应的EXT*/
	if (before != NULL && before->off + before->size > off) {
		WT_RET(__block_off_remove(session, el, before->off, &ext));

		/*计算分裂的位置*/
		a_off = ext->off;
		a_size = off - ext->off;
		b_off = off + size;
		b_size = ext->size - (a_size + size);
	}
	else if (after != NULL && off + size > after->off) { /*after就是off对应的WT_EXT*/
		WT_RET(__block_off_remove(session, el, after->off, &ext));

		a_off = WT_BLOCK_INVALID_OFFSET;
		a_size = 0;
		b_off = off + size;
		b_size = ext->size - (b_off - ext->off);
	} 
	else{
		return (WT_NOTFOUND);
	}

	if (a_size != 0) {
		ext->off = a_off;
		ext->size = a_size;
		WT_RET(__block_ext_insert(session, el, ext));
		ext = NULL;
	}

	if (b_size != 0) {
		if (ext == NULL)
			WT_RET(__block_off_insert(session, el, b_off, b_size));
		else 
		{
			ext->off = b_off;
			ext->size = b_size;
			WT_RET(__block_ext_insert(session, el, ext));
			ext = NULL;
		}
	}

	if (ext != NULL)
		__wt_block_ext_free(session, ext);

	return 0;
}

/*扩大一个block对应的文件大小*/
static inline int __block_extend( WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t *offp, wt_off_t size)
{
	WT_FH* fh;

	fh = block->fh;

	/*不能从一个空文件开始*/
	/*
	 * Callers of this function are expected to have already acquired any
	 * locks required to extend the file.
	 *
	 * We should never be allocating from an empty file.
	 */
	if (fh->size < block->allocsize)
		WT_RET_MSG(session, EINVAL, "file has no description information");

	/*文件超过最大值了，不能再扩展*/
	if(fh->size > (wt_off_t)INT64_MAX - size)
		WT_RET_MSG(session, WT_ERROR, "block allocation failed, file cannot grow further");

	*offp = fh->size;
	fh->size += size;

	WT_STAT_FAST_DATA_INCR(session, block_extension);
	WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "file extend %" PRIdMAX "B @ %" PRIdMAX, (intmax_t)size, (intmax_t)*offp));

	return 0;
}

/*在一个block对应的文件中分配一个数据空间(chunk)*/
int __wt_block_alloc(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t *offp, wt_off_t size)
{
	WT_EXT *ext, **estack[WT_SKIP_MAXDEPTH];
	WT_SIZE *szp, **sstack[WT_SKIP_MAXDEPTH];

	WT_ASSERT(session, block->live.avail.track_size != 0);

	WT_STAT_FAST_DATA_INCR(session, block_alloc);

	/*size没有和block要求的对齐方式对齐，不进行下一步处理*/
	if(size % block->allocsize != 0){
		WT_RET_MSG(session, EINVAL, "cannot allocate a block size %" PRIdMAX " that is not a multiple of the allocation size %" PRIu32, (intmax_t)size, block->allocsize);
	}

	/*block数据长度还未达到要求建立数据空间的长度，对block对应的文件进行扩充,暂时不分配数据空间*/
	if(block->live.avail.bytes < (uint64_t)size)
		goto append;

	if (block->allocfirst){
		/*查找off跳表中第一个能存入size大小的ext对象位置*/
		if (!__block_first_srch(block->live.avail.off, size, estack))
			goto append;
		ext = *estack[0];
	}
	else{
		/*在WT_SIZE的跳表中找能存下size长度的ext对象*/
		__block_size_srch(block->live.avail.sz, size, sstack);
		if ((szp = *sstack[0]) == NULL) {
append:
			WT_RET(__block_extend(session, block, offp, size));
			WT_RET(__block_append(session, &block->live.alloc, *offp, (wt_off_t)size));

			return 0;
		}

		ext = szp->off[0];
	}

	/*将ext从avail列表中删除，表示这个ext被占用了*/
	WT_RET(__block_off_remove(session, &block->live.avail, ext->off, &ext));
	*offp = ext->off;

	/*数据没有充满ext对象长度，去掉已经占用的，把未占用的重新放入到live.avail中继续使用*/
	if(ext->size > size){
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK,
			"allocate %" PRIdMAX " from range %" PRIdMAX "-%"
			PRIdMAX ", range shrinks to %" PRIdMAX "-%" PRIdMAX,
			(intmax_t)size,
			(intmax_t)ext->off, (intmax_t)(ext->off + ext->size),
			(intmax_t)(ext->off + size),
			(intmax_t)(ext->off + size + ext->size - size)));

		ext->off += size;
		ext->size -= size;
		WT_RET(__block_ext_insert(session, &block->live.avail, ext));
	}
	else{ /*刚好充满这个ext,释放掉这个ext对象*/
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "allocate range %" PRIdMAX "-%" PRIdMAX,
			(intmax_t)ext->off, (intmax_t)(ext->off + ext->size)));

		__wt_block_ext_free(session, ext);
	}

	/*将新获得的文件数据空间合并到live.alloc列表中*/
	WT_RET(__block_merge(session, &block->live.alloc, *offp, (wt_off_t)size));

	return 0;
}

/*释放一个block addr对应的数据空间(chunk)*/
int __wt_block_free(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr, size_t addr_size)
{
	WT_DECL_RET;
	wt_off_t offset;
	uint32_t cksum, size;

	WT_UNUSED(addr_size);
	WT_STAT_FAST_DATA_INCR(session, block_free);

	/*从addr获得off /size /checksum*/
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	WT_RET(__wt_verbose(session, WT_VERB_BLOCK,
		"free %" PRIdMAX "/%" PRIdMAX, (intmax_t)offset, (intmax_t)size));
	/*为session预分配5个WT_EXT,用于重复利用WT_EXT对象*/
	WT_RET(__wt_block_ext_prealloc(session, 5));

	__wt_spin_lock(session, &block->live_lock);
	ret = __wt_block_off_free(session, block, offset, (wt_off_t)size);
	__wt_spin_unlock(session, &block->live_lock);
}

/*在block对应的文件中释放掉(off, size)位置的数据空间(chunk)*/
int __wt_block_off_free(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t offset, wt_off_t size)
{
	WT_DECL_RET;

	/*进行范围释放，有可能是在某个WT_EXT范围之内*/
	ret = __wt_block_off_remove_overlap(session, &block->live.alloc, offset, size);
	if (ret == 0) /*在alloc中有存在，表示是从avail中分配的，先合并到avail中，重复使用*/
		ret = __block_merge(session, &block->live.avail, offset, (wt_off_t)size);
	else if(ret == WT_NOTFOUND) /*在alloc中没有找到，直接合并到废弃的空间跳表中*/
		ret = __block_merge(session, &block->live.discard, offset, (wt_off_t)size);

	return ret;
}

/*检查alloc与dicard的重叠部分，将已经使用的部分移到checkpoint的avail跳表中*/
int __wt_block_extlist_overlap(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
{
	WT_EXT *alloc, *discard;

	alloc = ci->alloc.off[0];
	discard = ci->discard.off[0];

	while(alloc != NULL && discard != NULL){
		/*将alloc和discard的off范围一致*/
		if (alloc->off + alloc->size <= discard->off) {
			alloc = alloc->next[0];
			continue;
		}

		if (discard->off + discard->size <= alloc->off) {
			discard = discard->next[0];
			continue;
		}

		WT_RET(__block_ext_overlap(session, block, &ci->alloc, &alloc, &ci->discard, &discard));
	}

	return 0;
}

static int __block_ext_overlap(WT_SESSION_IMPL * session, WT_BLOCK *block, WT_EXTLIST *ael, WT_EXT **ap, WT_EXTLIST *bel, WT_EXT **bp)
{
	WT_EXT *a, *b, **ext;
	WT_EXTLIST *avail, *el;
	wt_off_t off, size;

	avail = &block->live.ckpt_avail;

		/*
	 * The ranges overlap, choose the range we're going to take from each.
	 *
	 * We can think of the overlap possibilities as 11 different cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		ranges are the same
	 * #2	BBBBBBBBBBBBB				overlaps the beginning
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #8			AAAAAAAAAAAAAAAA	same as #2
	 * #9		AAAAA				A is a prefix of B
	 * #10			AAAAAA			A is middle of B
	 * #11			AAAAAAAAAA		A is a suffix of B
	 *
	 *
	 * By swapping the arguments so "A" is always the lower range, we can
	 * eliminate cases #2, #8, #10 and #11, and only handle 7 cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		ranges are the same
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #9		AAAAA				A is a prefix of B
	 */

	a = *ap;
	b = *bp;
	if (a->off > b->off) {				/* Swap,为了减少重复代码，将a和b做个身份交换*/
		b = *ap;
		a = *bp;
		ext = ap; ap = bp; bp = ext;
		el = ael; ael = bel; bel = el;
	}

	if(a->off == b->off){ /*case 1, 4, 9*/
		if(a->size == b->size){ /*case 1, AB相同*/
			*ap = (*ap)->next[0];
			*bp = (*bp)->next[0];
			/*直接将A和B的重叠部分移到checkpint avail list中*/
			WT_RET(__block_merge(session, avail, b->off, b->size));

			WT_RET(__block_off_remove(session, ael, a->off, NULL));
			WT_RET(__block_off_remove(session, bel, b->off, NULL));
		}
		else if(a->size > b->size){ /*case 4, B是A的一部分，且是A的前缀部分*/
			/*删除掉A的前缀部分，并把剩余的作为a的(off,size)作为新的ext重新插入到ael跳表中*/
			WT_RET(__block_off_remove(session, ael, a->off, &a));
			a->off += b->size;
			a->size -= b->size;
			WT_RET(__block_ext_insert(session, ael, a));

			/*将B整个从bel中删除，并加入到checkpoint avail list中*/
			*bp = (*bp)->next[0];
			WT_RET(__block_merge(session, avail, b->off, b->size));
			WT_RET(__block_off_remove(session, bel, b->off, NULL));
		}
		else{ /*case 9*/
			/*A是B的一部分,并且A是B的前缀部分,将重叠部分从bel中删除*/
			WT_RET(__block_off_remove(session, bel, b->off, &b));
			b->off += a->size;
			b->size -= a->size;
			WT_RET(__block_ext_insert(session, bel, b));

			/*将A从ael中删除，并合并到checkpoint avail list中*/
			*ap = (*ap)->next[0];
			WT_RET(__block_merge(session, avail, a->off, a->size));
			WT_RET(__block_off_remove(session, ael, a->off, NULL));
		}
	}
	else if (a->off + a->size == b->off + b->size) { /*case 6，B是A的后缀部分*/
		/*从新设置a的长度即可，然后重新插入到ael中*/
		WT_RET(__block_off_remove(session, ael, a->off, &a));
		a->size -= b->size;
		WT_RET(__block_ext_insert(session, ael, a));

		/*将B整个从bel中删除，并加入到checkpoint avail list中*/
		*bp = (*bp)->next[0];
		WT_RET(__block_merge(session, avail, b->off, b->size));
		WT_RET(__block_off_remove(session, bel, b->off, NULL));
	}
	else if(a->off + a->size < b->off + b->size){ /*case 3, 7, B的前半部分和A的后半部分重贴*/
		/*将重叠部分作为新的EXT插入到checkpoint avail list中*/
		off = b->off;
		size = (a->off + a->size) - b->off;
		WT_RET(__block_merge(session, avail, off, size));

		/*在A中删除掉重叠部分*/
		WT_RET(__block_off_remove(session, ael, a->off, &a));
		a->size -= size;
		WT_RET(__block_ext_insert(session, ael, a));

		/*在B中删除掉重叠部分*/
		WT_RET(__block_off_remove(session, bel, b->off, &b));
		b->off += size;
		b->size -= size;
		WT_RET(__block_ext_insert(session, bel, b));
	}
	else{ /*case 5,B是A的中间部分*/
		/*计算后面部分的WT_EXT对象位置*/
		off = b->off + b->size;
		size = (a->off + a->size) - off;

		/*将A的不和B重复前半部分作为一个ext对象插入到ael中*/
		WT_RET(__block_off_remove(session, ael, a->off, &a));
		a->size = b->off - a->off;
		WT_RET(__block_ext_insert(session, ael, a));
		/*将A与B不重叠后半部分合并到ael中，这里不使用单独的EXT,有可能ael中和*/
		WT_RET(__block_merge(session, ael, off, size));
	}

	return 0;
}

/*合并a和b，并将结果保存到b中*/
int __wt_block_extlist_merge(WT_SESSION_IMPL *session, WT_EXTLIST *a, WT_EXTLIST *b)
{
	WT_EXT *ext;
	WT_EXTLIST tmp;
	u_int i;

	WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "merging %s into %s", a->name, b->name));
	/*如果a跳表元素比b多，交换ab的位置*/
	if (a->track_size == b->track_size && a->entries > b->entries){
		/*进行a b交换*/
		tmp = *a;
		a->bytes = b->bytes;
		b->bytes = tmp.bytes;
		a->entries = b->entries;
		b->entries = tmp.entries;
		/*进行a b跳表指针交换*/
		for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
			a->off[i] = b->off[i];
			b->off[i] = tmp.off[i];
			a->sz[i] = b->sz[i];
			b->sz[i] = tmp.sz[i];
		}
	}

	/*将a合并到b中*/
	WT_EXT_FOREACH(ext, a->off){
		WT_RET(__block_merge(session, b, ext->off, ext->size));
	}

	return 0;
}

/*在block的alloclist后面append一个数据空间长度(off,size)*/
static int __block_append(WT_SESSION_IMPL *session, WT_EXTLIST *el, wt_off_t off, wt_off_t size)
{
	WT_EXT *ext, **astack[WT_SKIP_MAXDEPTH];
	u_int i;

	WT_ASSERT(session, el->track_size == 0);

	ext = el->last;
	/*最后一个ext正好是off的位置*/
	if (ext != NULL && ext->off + ext->size == off)
		ext->size += size; /*直接扩大ext的size即可*/
	else{
		/*定位到el->off最后一个ext*/
		ext = __block_off_srch_last(el->off, astack);
		/*正好最后一个ext的末尾是off偏移,也可以直接扩大*/
		if (ext != NULL && ext->off + ext->size == off)
			ext->size += size;
		else{ /*off一定大于ext->off + size*/
			/*重新申请一个新的ext，并插入到ef的off跳表中*/
			WT_RET(__wt_block_ext_alloc(session, &ext));
			ext->off = off;
			ext->size = size;
			/*这里是顺序插入，所以只需要插叙到最后一个单元的后面即可*/
			for (i = 0; i < ext->depth; ++i)
				*astack[i] = ext;

			++el->entries;
		}

		el->last = ext;
	}

	el->bytes += (uint64_t)size;

	return 0;
}

/*将(off,size)数据空间对应关系合并到el中*/
int __wt_block_insert_ext(WT_SESSION_IMPL *session, WT_EXTLIST *el, wt_off_t off, wt_off_t size)
{
	return __block_merge(session, el, off, size);
}

/*将(off,size)数据空间对应关系合并到el中*/
static int __block_merge(WT_SESSION_IMPL* session, WT_EXTLIST* el, wt_off_t off, wt_off_t size)
{
	WT_EXT *ext, *after, *before;

	/*获得off在el跳表中的对应关系*/
	__block_off_srch_pair(el, off, &before, &after);

	if(before != NULL){
		/*(off,size)是在before空间之内的,这是不可能的，可能是程序出现了BUG,这里抛出一个异常*/
		if (before->off + before->size > off){
			WT_PANIC_RET(session, EINVAL, "%s: existing range %" PRIdMAX "-%" PRIdMAX  "overlaps with merge range %" PRIdMAX "-%" PRIdMAX,
				el->name, (intmax_t)before->off, (intmax_t)(before->off + before->size),
				(intmax_t)off, (intmax_t)(off + size));
		}

		/*off不是紧接着before的末尾，也就意味着不需要合并到before中*/
		if(before->off + before->size != off)
			before = NULL;
	}

	if(after != NULL){
		/*(off,size)是在after之内，可能是程序的错误，抛出一个异常*/
		if (off + size > after->off)
			WT_PANIC_RET(session, EINVAL,
			"%s: merge range %" PRIdMAX "-%" PRIdMAX
			" overlaps with existing range %" PRIdMAX
			"-%" PRIdMAX,
			el->name,
			(intmax_t)off, (intmax_t)(off + size),
			(intmax_t)after->off,
			(intmax_t)(after->off + after->size));

		/*(off,size)不是在紧接着after的头，也就无需合并到after中*/
		if (off + size != after->off)
			after = NULL;
	}
	/*既不和before相连也不和after相连，说明是独立插入一个ext,进行信息提示，这个应该是为了磁盘的顺序读写做的设计*/
	if (before == NULL && after == NULL) {
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "%s: insert range %" PRIdMAX "-%" PRIdMAX,
			el->name, (intmax_t)off, (intmax_t)(off + size)));

		return __block_off_insert(session, el, off, size);
	}

	/*和after进行合并*/
	if (before == NULL){
		WT_RET(__block_off_remove(session, el, after->off, &ext));
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK,
			"%s: range grows from %" PRIdMAX "-%" PRIdMAX ", to %" PRIdMAX "-%" PRIdMAX,
			el->name, (intmax_t)ext->off, (intmax_t)(ext->off + ext->size),
			(intmax_t)off, (intmax_t)(off + ext->size + size)));

		ext->off = off;
		ext->size += size;
	}
	else{ /*和before、after进行合并*/
		if (after != NULL) { /*before、current和after是相连的，将三个EXT进行合并,这个if是合并after*/
			size += after->size;
			WT_RET(__block_off_remove(session, el, after->off, NULL));
		}

		/*合并before*/
		WT_RET(__block_off_remove(session, el, before->off, &ext));

		WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "%s: range grows from %" PRIdMAX "-%" PRIdMAX ", to %"
			PRIdMAX "-%" PRIdMAX, el->name,
			(intmax_t)ext->off, (intmax_t)(ext->off + ext->size),
			(intmax_t)ext->off,
			(intmax_t)(ext->off + ext->size + size)));

		ext->size += size;
	}
	/*将合并后的ext插入到ext skiplist中*/
	return __block_ext_insert(session, el, ext);
}

/*读取block的avail ext对象到el中,根据el.off/el.size进行读取，但数据的长度不能超过ckpt_size*/
int __wt_block_extlist_read_avail(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el, wt_off_t ckpt_size)
{
	WT_DECL_RET;

	if (el->offset == WT_BLOCK_INVALID_OFFSET)
		return 0;

	/*从e*/
	WT_ERR(__wt_block_extlist_read(session, block, el, ckpt_size));

	WT_ERR_NOTFOUND_OK(__wt_block_off_remove_overlap(session, el, el->offset, el->size));

err:
	return ret;
}

#define	WT_EXTLIST_READ(p, v) do {					\
	uint64_t _v;									\
	WT_ERR(__wt_vunpack_uint(&(p), 0, &_v));		\
	(v) = (wt_off_t)_v;								\
} while (0)

/*从磁盘上读取一个ext list所有ext对象信息，并加入到ext list当中,这个相当于entry的索引信息表*/
int __wt_block_extlist_read(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el, wt_off_t ckpt_size)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	wt_off_t off, size;
	int (*func)(WT_SESSION_IMPL *, WT_EXTLIST *, wt_off_t, wt_off_t);
	const uint8_t *p;

	if(el->offset == WT_BLOCK_INVALID_OFFSET)
		return 0;

	WT_RET(__wt_scr_alloc(session, el->size, &tmp));
	/*从block对应的文件中读取一个数据空间块到tmp的缓冲区中*/
	WT_ERR(__wt_block_read_off(session, block, tmp, el->offset, el->size, el->cksum));

	/*获得block header指针位置*/
	p = WT_BLOCK_HEADER_BYTE(tmp->mem);
	/*前2个uint64_t的值是extlist魔法校验值和一个0值的size*/
	/*读取off值*/
	WT_EXTLIST_READ(p, off);
	/*读取size值*/
	WT_EXTLIST_READ(p, size);

	/*进行合法性校验*/
	if (off != WT_BLOCK_EXTLIST_MAGIC || size != 0)
		goto corrupted;

	func = el->track_size == 0 ? __block_append : __block_merge;
	for(;;){
		WT_EXTLIST_READ(p, off);
		WT_EXTLIST_READ(p, size);
		if(off == WT_BLOCK_INVALID_OFFSET)
			break;

		/*(off,size)的合法性校验,off和size一定是block->allocsize整数倍，off + size的偏移必须在检查点位置范围内*/
		if (off < block->allocsize || off % block->allocsize != 0 ||
			size % block->allocsize != 0 || off + size > ckpt_size){
corrupted:		
			WT_PANIC_RET(session, WT_ERROR, "file contains a corrupted %s extent list, range %" PRIdMAX "-%" PRIdMAX " past end-of-file",
				el->name, (intmax_t)off, (intmax_t)(off + size));
		}
		/*进行(off,size)的ext对象加入*/
		WT_ERR(func(session, el, off, size));
	}

	if (WT_VERBOSE_ISSET(session, WT_VERB_BLOCK))
		WT_ERR(__block_extlist_dump(session, "read extlist", el, 0));

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

#define	WT_EXTLIST_WRITE(p, v)		WT_ERR(__wt_vpack_uint(&(p), 0, (uint64_t)(v)))

/*将一个ext list索引表信息写入到block对应文件中*/
int __wt_block_extlist_write(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el, WT_EXTLIST *additional)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_EXT *ext;
	WT_PAGE_HEADER *dsk;
	size_t size;
	uint32_t entries;
	uint8_t *p;

	if(WT_VERBOSE_ISSET(session, WT_VERB_BLOCK))
		WT_RET(__block_extlist_dump(session, "write extlist", el, 0));

	/*计算entry个数*/
	entries = el->entries + (additional == NULL ? 0 : additional->entries);
	if (entries == 0) { /*ext list没有任何有效的单元，直接返回*/
		el->offset = WT_BLOCK_INVALID_OFFSET;
		el->cksum = el->size = 0;
		return 0;
	}

	/*计算缓冲区的大小并分配一个相对应的缓冲区*/
	size = (entries + 2) * 2 * WT_INTPACK64_MAXSIZE; /*前面有一个（魔法校验值 + size的值），所以entry是 + 2的*/
	WT_RET(__wt_block_write_size(session, block, &size));
	WT_RET(__wt_scr_alloc(session, size, &tmp));

	/*设置页头位置*/
	dsk = (WT_PAGE_HEADER*)(tmp->mem);
	memset(dsk, 0, WT_BLOCK_HEADER_BYTE_SIZE);
	dsk->type = WT_PAGE_BLOCK_MANAGER;

	/*跳过block header，指向block数据区地址*/
	p = WT_BLOCK_HEADER_BYTE(dsk);
	/*先写一个extlist的魔法校验字*/
	WT_EXTLIST_WRITE(p, WT_BLOCK_EXTLIST_MAGIC);	/* Initial value */
	/*填充一个校验值0*/
	WT_EXTLIST_WRITE(p, 0);
	/*将ext的位置信息写入到缓冲区中*/
	WT_EXT_FOREACH(ext, el->off) {		
		WT_EXTLIST_WRITE(p, ext->off);
		WT_EXTLIST_WRITE(p, ext->size);
	}

	/*将附加的ext list也写入到缓冲区中*/
	if (additional != NULL){
		WT_EXT_FOREACH(ext, additional->off) {	
			WT_EXTLIST_WRITE(p, ext->off);
			WT_EXTLIST_WRITE(p, ext->size);
		}
	}

	/*写入一个结束符ext对象,为了读结束用*/
	WT_EXTLIST_WRITE(p, WT_BLOCK_INVALID_OFFSET);
	WT_EXTLIST_WRITE(p, 0);

	/*计算数据长度*/
	dsk->u.datalen = WT_PTRDIFF32(p, WT_BLOCK_HEADER_BYTE(dsk));
	/*计算整个page的长度*/
	tmp->size = WT_PTRDIFF32(p, dsk);
	dsk->mem_size = WT_PTRDIFF32(p, dsk);

	/*将数据写入磁盘中,并计算off/size/checksum*/
	WT_ERR(__wt_block_write_off(session, block, tmp, &el->offset, &el->size, &el->cksum, 1, 1));
	/*将el从live.alloc中移除*/
	WT_TRET(__wt_block_off_remove_overlap(session, &block->live.alloc, el->offset, el->size));

	WT_ERR(__wt_verbose(session, WT_VERB_BLOCK, "%s written %" PRIdMAX "/%" PRIu32, el->name, (intmax_t)el->offset, el->size));

err:
	__wt_scr_free(session, &tmp);
	return ret;
}

/*将block对应的文件缩小，缩小的大小为el中最后一个ext的大小，相当于将最后一个ext指向的数据被移除*/
int __wt_block_extlist_truncate(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el)
{
	WT_EXT *ext, **astack[WT_SKIP_MAXDEPTH];
	WT_FH *fh;
	wt_off_t orig, size;

	fh = block->fh;
	/*找到el中最后一个ext对象*/
	ext = __block_off_srch_last(el->off, astack);
	if (ext == NULL)
		return 0;

	WT_ASSERT(session, ext->off + ext->size <= fh->size);
	if (ext->off + ext->size < fh->size) /*空闲的ext不处于文件末尾，表示文件无需做truncate操作*/
		return 0;

	orig = fh->size;
	size = ext->off;
	/*将ext从el中移除*/
	WT_RET(__block_off_remove(session, el, size, NULL));
	/*将文件大小去除掉ext的大小，因为文件将truncate掉ext的数据*/
	fh->size = size;

	WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "truncate file from %" PRIdMAX " to %" PRIdMAX, (intmax_t)orig, (intmax_t)size));

	/*文件缩小*/
	WT_RET_BUSY_OK(__wt_ftruncate(session, block->fh, size));

	return 0;
}

/*初始化一个WT_EXT跳表对象*/
int __wt_block_extlist_init(WT_SESSION_IMPL *session, WT_EXTLIST *el, const char *name, const char *extname, int track_size)
{
	size_t size;

	WT_CLEAR(*el);

	size = (name == NULL ? 0 : strlen(name)) + strlen(".") + (extname == NULL ? 0 : strlen(extname) + 1);
	WT_RET(__wt_calloc_def(session, size, &el->name));

	snprintf(el->name, size, "%s.%s", name == NULL ? "" : name, extname == NULL ? "" : extname);

	el->offset = WT_BLOCK_INVALID_OFFSET;
	el->track_size = track_size;

	return 0;
}

/*释放一个ext list对象*/
void __wt_block_extlist_free(WT_SESSION_IMPL *session, WT_EXTLIST *el)
{
	WT_EXT *ext, *next;
	WT_SIZE *szp, *nszp;

	__wt_free(session, el->name);
	/*释放ext skip list*/
	for (ext = el->off[0]; ext != NULL; ext = next) {
		next = ext->next[0];
		__wt_free(session, ext);
	}
	/*释放size skip list*/
	for (szp = el->sz[0]; szp != NULL; szp = nszp) {
		nszp = szp->next[0];
		__wt_free(session, szp);
	}
	/*清空el的内存数据*/
	WT_CLEAR(*el);
}

/*对ext list dump操作*/
static int __block_extlist_dump(WT_SESSION_IMPL *session, const char *tag, WT_EXTLIST *el, int show_size)
{
	WT_EXT *ext;
	WT_SIZE *szp;

	WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "%s: %s: %" PRIu64 " bytes, by offset:%s",
		tag, el->name, el->bytes, el->entries == 0 ? " [Empty]" : ""));

	if (el->entries == 0)
		return (0);

	WT_EXT_FOREACH(ext, el->off){
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "\t{%" PRIuMAX "/%" PRIuMAX "}",
		(uintmax_t)ext->off, (uintmax_t)ext->size));
	}

	if (!show_size)
		return 0;

	WT_RET(__wt_verbose(session, WT_VERB_BLOCK,
		"%s: %s: by size:%s",
		tag, el->name, el->entries == 0 ? " [Empty]" : ""));
	if (el->entries == 0)
		return (0);

	/*dump size skip list*/
	WT_EXT_FOREACH(szp, el->sz) {
		WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "\t{%" PRIuMAX "}", (uintmax_t)szp->size));
		WT_EXT_FOREACH_OFF(ext, szp->off)
			WT_RET(__wt_verbose(session, WT_VERB_BLOCK, "\t\t{%" PRIuMAX "/%" PRIuMAX "}", (uintmax_t)ext->off, (uintmax_t)ext->size));
	}

	return 0;
}
