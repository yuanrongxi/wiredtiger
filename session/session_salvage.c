
#include "wt_internal.h"

/*修复一个btree file*/
int __wt_salvage(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_CKPT *ckptbase;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	dhandle = session->dhandle;


	/*
	 * XXX
	 * The salvage process reads and discards previous checkpoints, so the
	 * underlying block manager has to ignore any previous checkpoint
	 * entries when creating a new checkpoint, in other words, we can't use
	 * the metadata checkpoint list, it has all of those checkpoint listed
	 * and we don't care about them.  Build a clean checkpoint list and use
	 * it instead.
	 *
	 * Don't first clear the metadata checkpoint list and call the function
	 * to get a list of checkpoints: a crash between clearing the metadata
	 * checkpoint list and creating a new checkpoint list would look like a
	 * create or open of a file without a checkpoint to roll-forward from,
	 * and the contents of the file would be discarded.
	 */

	WT_RET(__wt_calloc_def(session, 2, &ckptbase));
	WT_ERR(__wt_strdup(session, WT_CHECKPOINT,  &ckptbase[0].name));
	F_SET(&ckptbase[0], WT_CKPT_ADD);

	WT_ERR(__wt_bt_salvage(session, ckptbase, cfg));

	/*
	 * If no checkpoint was created, well, it's probably bad news, but there
	 * is nothing to do but clear any recorded checkpoints for the file.  If
	 * a checkpoint was created, life is good, replace any existing list of
	 * checkpoints with the single new one.
	 * 如果在salvage过程中没有建立checkpoint,说明数据彻底坏了，只能进行清除掉
	 * btree file对应的checkpoint信息,假如建立好了checkpoint，那么说明数据修复
	 * 成功了，用新的checkpoint来代替原来的checkpoint信息
	 */
	if (ckptbase[0].raw.data == NULL)
		WT_ERR(__wt_meta_checkpoint_clear(session, dhandle->name));
	else
		WT_ERR(__wt_meta_ckptlist_set(session, dhandle->name, ckptbase, NULL));

err:
	__wt_meta_ckptlist_free(session, ckptbase);
	return ret;
}

