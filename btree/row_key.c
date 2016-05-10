/*********************************************************
*row store btree的key相关操作
*********************************************************/
#include "wt_internal.h"

static void __inmem_row_leaf_slots(uint8_t* , uint32_t, uint32_t, uint32_t);

/*在内存中实例化page中row中的key对象*/
int __wt_row_leaf_keys(WT_SESSION_IMPL* session, WT_PAGE* page)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(key);
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_ROW *rip;
	uint32_t gap, i;

	btree = S2BT(session);

	/*页没有行数据，设置已经完成内存中实现了row key实例化并直接返回*/
	if (page->pg_row_entries == 0){
		F_SET_ATOMIC(page, WT_PAGE_BUILD_KEYS);
		return 0;
	}

	WT_RET(__wt_scr_alloc(session, 0, &key));
	/*每一行占一个bit位置*/
	WT_RET(__wt_scr_alloc(session, (uint32_t)__bitstr_size(page->pg_row_entries), &tmp));

	/*确定行间距离*/
	if ((gap = btree->key_gap) == 0)
		gap = 1;

	/*将row slots槽位在内存中创建*/
	__inmem_row_leaf_slots(tmp->mem, 0, page->pg_row_entries, gap);

	for (rip = page->pg_row_d, i = 0; i < page->pg_row_entries; ++rip, ++i){
		if (__bit_test(tmp->mem, i)) /*确定这个行是否需要实例化key,是不是实例化是由__inmem_row_leaf_slots确定的*/
			WT_ERR(__wt_row_leaf_key_work(session, page, rip, key, 1));
	}

	/*设置完成实例化KEY标示*/
	F_SET_ATOMIC(page, WT_PAGE_BUILD_KEYS);

err:
	__wt_scr_free(session, &key);
	__wt_scr_free(session, &tmp);
	return ret;
}

/*按照gap间隔距离来标记需要实例化key的row slots*/
static void __inmem_row_leaf_slots(uint8_t* list, uint32_t base, uint32_t entries, uint32_t gap)
{
	uint32_t indx, limit;

	if (entries < gap)
		return;

	/* 用二分法进行key实例化的row做标记, 标记的间隔是gap大小 */
	limit = entries;
	indx = base + (limit >> 1);
	__bit_set(list, indx);

	__inmem_row_leaf_slots(list, base, limit >> 1, gap);

	base = indx + 1;
	--limit;
	__inmem_row_leaf_slots(list, base, limit >> 1, gap);
}

/*获得rip对应row key的拷贝*/
int __wt_row_leaf_key_copy(WT_SESSION_IMPL* session, WT_PAGE* page, WT_ROW* rip, WT_ITEM* key)
{
	WT_RET(__wt_row_leaf_key(session, page, rip, key, 0));

	if (!WT_DATA_IN_ITEM(key))
		WT_RET(__wt_buf_set(session, key, key->data, key->size));

	return 0;
}

/*获得一个row key,这个row是在叶子数据节点上，如果key没有被实例化到内存中，进行内存实例化*/
int __wt_row_leaf_key_work(WT_SESSION_IMPL* session, WT_PAGE* page, WT_ROW* rip_arg, WT_ITEM* keyb, int instantiate)
{
	enum { FORWARD, BACKWARD } direction;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_ROW *rip, *jump_rip;
	size_t size;
	u_int last_prefix;
	int jump_slot_offset, slot_offset;
	void *copy;
	const void *p;

	/*
	* !!!
	* It is unusual to call this function: most code should be calling the
	* front-end, __wt_row_leaf_key, be careful if you're calling this code
	* directly.
	*/
	btree = S2BT(session);
	unpack = &_unpack;
	rip = rip_arg;

	jump_rip = NULL;
	jump_slot_offset = 0;
	last_prefix = 0;

	p = NULL;
	size = 0;
	direction = BACKWARD;
	for (slot_offset = 0;;){
		if (0){
switch_and_jump:	
			/* Switching to a forward roll. */
			WT_ASSERT(session, direction == BACKWARD);
			direction = FORWARD;

			/* Skip list of keys with compatible prefixes. */
			rip = jump_rip;
			slot_offset = jump_slot_offset;
		}

		copy = WT_ROW_KEY_COPY(rip);
		/*对copy的值内容进行解析，得到key/value对的值，如果值在cell中，对cell进行unpack*/
		(void)__wt_row_leaf_key_info(page, copy, &ikey, &cell, &p, &size);
		/*直接可以到的key和value的值*/
		if (cell == NULL){
			keyb->data = p;
			keyb->size = size;
			
			/*
			* If this is the key we originally wanted, we don't
			* care if we're rolling forward or backward, or if
			* it's an overflow key or not, it's what we wanted.
			* This shouldn't normally happen, the fast-path code
			* that front-ends this function will have figured it
			* out before we were called.
			*
			* The key doesn't need to be instantiated, skip past
			* that test.
			*/
			if (slot_offset == 0)
				goto done;

			goto switch_and_jump;
		}

		/*判断key是否已经实例化*/
		if (ikey != NULL){
			if (slot_offset == 0) {
				keyb->data = p;
				keyb->size = size;
				goto done;
			}

			/*key值属于overflow类型，值是存储在其他地方的cell对象上*/
			if (__wt_cell_type(cell) == WT_CELL_KEY_OVFL)
				goto next;

			keyb->data = p;
			keyb->size = size;
			direction = FORWARD;
			goto next;
		}

		/*对cell进行unpack*/
		__wt_cell_unpack(cell, unpack);

		/* 3: 处理overflow key */
		if (unpack->type == WT_CELL_KEY_OVFL){
			if (slot_offset == 0) {
				WT_ERR(__wt_readlock(session, btree->ovfl_lock));

				/*读取key值存储的cell位置*/
				copy = WT_ROW_KEY_COPY(rip);
				if (!__wt_row_leaf_key_info(page, copy, NULL, &cell, &keyb->data, &keyb->size)) {
					__wt_cell_unpack(cell, unpack);
					ret = __wt_dsk_cell_data_ref(session, WT_PAGE_ROW_LEAF, unpack, keyb);
				}

				WT_TRET(__wt_readunlock(session, btree->ovfl_lock));
				WT_ERR(ret);
				break;
			}

			goto next;
		}

		/*key的前缀没有压缩*/
		if (unpack->prefix == 0){
			WT_ASSERT(session, btree->huffman_key != NULL);
			/*直接获取key值*/
			WT_ERR(__wt_dsk_cell_data_ref(session, WT_PAGE_ROW_LEAF, unpack, keyb));
			if (slot_offset == 0)
				goto done;
			goto switch_and_jump;
		}

		/*
		* 5: an on-page reference to a key that's prefix compressed.
		*	If rolling backward, keep looking for something we can
		* use.
		*	If rolling forward, build the full key and keep rolling
		* forward.
		* unpack前缀是压缩的,先保存这个rip的位置，防止后面的row key的也
		* 是一个overflow类型，这样的话我们就要回退到现在的位置进行获取
		*/

		if (direction == BACKWARD) {
			/*
			* If there's a set of keys with identical prefixes, we
			* don't want to instantiate each one, the prefixes are
			* all the same.
			*
			* As we roll backward through the page, track the last
			* time the prefix decreased in size, so we can start
			* with that key during our roll-forward.  For a page
			* populated with a single key prefix, we'll be able to
			* instantiate the key we want as soon as we find a key
			* without a prefix.
			*/
			if (slot_offset == 0)
				last_prefix = unpack->prefix;
			if (slot_offset == 0 || last_prefix > unpack->prefix) {
				jump_rip = rip;
				jump_slot_offset = slot_offset;
				last_prefix = unpack->prefix;
			}
		}
		if (direction == FORWARD) {
			/*
			* Get a reference to the current key's bytes.  Usually
			* we want bytes from the page, fast-path that case.
			*/
			if (btree->huffman_key == NULL) {
				p = unpack->data;
				size = unpack->size;
			}
			else {
				if (tmp == NULL)
					WT_ERR(__wt_scr_alloc(session, 0, &tmp));
				WT_ERR(__wt_dsk_cell_data_ref(session, WT_PAGE_ROW_LEAF, unpack, tmp));
				p = tmp->data;
				size = tmp->size;
			}

			/*
			* Grow the buffer as necessary as well as ensure data
			* has been copied into local buffer space, then append
			* the suffix to the prefix already in the buffer.
			*
			* Don't grow the buffer unnecessarily or copy data we
			* don't need, truncate the item's data length to the
			* prefix bytes.
			*/
			keyb->size = unpack->prefix;
			WT_ERR(__wt_buf_grow(session, keyb, keyb->size + size));
			memcpy((uint8_t *)keyb->data + keyb->size, p, size);
			keyb->size += size;

			if (slot_offset == 0)
				break;
		}
next:
		switch (direction) {
		case  BACKWARD:
			--rip;
			++slot_offset;
			break;
		case FORWARD:
			++rip;
			--slot_offset;
			break;
		}
	}

	if (instantiate) {
		copy = WT_ROW_KEY_COPY(rip_arg);
		(void)__wt_row_leaf_key_info(page, copy, &ikey, &cell, NULL, NULL);
		if (ikey == NULL) {
			/*在内存中实例化ikey对象*/
			WT_ERR(__wt_row_ikey_alloc(session, WT_PAGE_DISK_OFFSET(page, cell), keyb->data, keyb->size, &ikey));
			/*
			* Serialize the swap of the key into place: on success,
			* update the page's memory footprint, on failure, free
			* the allocated memory.
			*/
			if (WT_ATOMIC_CAS8(WT_ROW_KEY_COPY(rip), copy, ikey))
				__wt_cache_page_inmem_incr(session, page, sizeof(WT_IKEY) + ikey->size);
			else
				__wt_free(session, ikey);
		}
	}

done:
err:
	__wt_scr_free(session, &tmp);
   return (ret);
}

/*在内存中创建一个WT_KEY结构，并拷贝赋值*/
int __wt_row_ikey_alloc(WT_SESSION_IMPL* session, uint32_t cell_offset, const void* key, size_t size, WT_IKEY **ikeyp)
{
	WT_IKEY *ikey;

	WT_RET(__wt_calloc(session, 1, sizeof(WT_IKEY) + size, &ikey));
	ikey->size = WT_STORE_SIZE(size);
	ikey->cell_offset = cell_offset;
	memcpy(WT_IKEY_DATA(ikey), key, size);
	*ikeyp = ikey;

	return 0;
}

int __wt_row_ikey_incr(WT_SESSION_IMPL *session, WT_PAGE *page, uint32_t cell_offset, const void *key, size_t size, WT_REF *ref)
{
	WT_RET(__wt_row_ikey(session, cell_offset, key, size, ref));

	__wt_cache_page_inmem_incr(session, page, sizeof(WT_IKEY) + size);

	return 0;
}

/*实例化一个指定的key结构对象*/
int __wt_row_ikey(WT_SESSION_IMPL *session, uint32_t cell_offset, const void *key, size_t size, WT_REF *ref)
{
	WT_IKEY *ikey;

	WT_RET(__wt_row_ikey_alloc(session, cell_offset, key, size, &ikey));
	ref->key.ikey = ikey;

	return 0;
}





