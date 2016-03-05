
#include "wt_internal.h"

/*根据配置的的kv信息读入一个WT_CKPT的checkpoint信息*/
static int __ckpt_load(WT_SESSION_IMPL* session, WT_CONFIG_ITEM* k, WT_CONFIG_ITEM* v, WT_CKPT* ckpt)
{
	WT_CONFIG_ITEM a;
	char timebuf[64];


	WT_RET(__wt_strndup(session, k->str, k->len, &ckpt->name));
	/*读取checkpoint对应的block addr*/
	WT_RET(__wt_config_subgets(session, v, "addr", &a));
	WT_RET(__wt_buf_set(session, &ckpt->addr, a.str, a.len));
	if (a.len == 0)
		F_SET(ckpt, WT_CKPT_FAKE);
	else
		WT_RET(__wt_nhex_to_raw(session, a.str, a.len, &ckpt->raw));

	/*读取checkpoint order*/
	WT_RET(__wt_config_subgets(session, v, "order", &a));
	if (a.len == 0)
		goto format;
	ckpt->order = a.val;
	
	/*读取checkpoint的时间戳*/
	WT_RET(__wt_config_subgets(session, v, "time", &a));
	if (a.len == 0 || a.len > sizeof(timebuf) - 1)
		goto format;
	memcpy(timebuf, a.str, a.len);
	timebuf[a.len] = '\0';
	if (sscanf(timebuf, "%" SCNuMAX, &ckpt->sec) != 1)
		goto format;

	/*读取checkpoint size*/
	WT_RET(__wt_config_subgets(session, v, "size", &a));
	ckpt->ckpt_size = (uint64_t)a.val;
	
	/*读取write_gen*/
	WT_RET(__wt_config_subgets(session, v, "write_gen", &a));
	if (a.len == 0)
		goto format;
	ckpt->write_gen = (uint64_t)a.val;

	return 0;

format:
	WT_RET_MSG(session, WT_ERROR, "corrupted checkpoint list");
}

/*返回config对应文件的最后一个checkpoint信息*/
static int __ckpt_last(WT_SESSION_IMPL* session, const char* config, WT_CKPT* ckpt)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;
	int64_t found;

	WT_RET(__wt_config_getones(session, config, "checkpoint", &v));
	WT_RET(__wt_config_subinit(session, &ckptconf, &v));
	for (found = 0; __wt_config_next(&ckptconf, &k, &v) == 0;) {
		/* Ignore checkpoints before the ones we've already seen. 其实就是读取最大order的checkpoint*/
		WT_RET(__wt_config_subgets(session, &v, "order", &a));
		if (found) {
			if (a.val < found)
				continue;
			__wt_meta_checkpoint_free(session, ckpt);
		}
		found = a.val;
		WT_RET(__ckpt_load(session, &k, &v, ckpt));
	}

	return (found ? 0 : WT_NOTFOUND);
}

/*返回config对应文件中最后一个未命名(以WT_CHECKPOINT字串为前缀命名的checkpoint)的checkpoint*/
static int __ckpt_last_name(WT_SESSION_IMPL* session, const char* config, const char** namep)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;
	WT_DECL_RET;
	int64_t found;

	*namep = NULL;

	WT_ERR(__wt_config_getones(session, config, "checkpoint", &v));
	WT_ERR(__wt_config_subinit(session, &ckptconf, &v));

	for(found = 0; __wt_config_next(&ckptconf, &k, &v) == 0;){
		/*
		 * We only care about unnamed checkpoints; applications may not
		 * use any matching prefix as a checkpoint name, the comparison
		 * is pretty simple.
		 * 也就是k = WT_CHECKPOINT前缀的key
		 */
		if(k.len < strlen(WT_CHECKPOINT) || strncmp(k.str, WT_CHECKPOINT, strlen(WT_CHECKPOINT)) != 0)
			continue;

		/* Ignore checkpoints before the ones we've already seen. */
		WT_ERR(__wt_config_subgets(session, &v, "order", &a));
		if (found && a.val < found)
			continue;

		if(*namep != NULL)
			__wt_free(session, *namep);

		WT_ERR(__wt_strndup(session, k.str, k.len, namep));
		found = a.val;
	}

	if(!found)
		ret = WT_NOTFOUND;
	if(0){
err: 
		__wt_free(session, namep);
	}

	return ret;
}

/*设置文件的checkpoint*/
static int __ckpt_set(WT_SESSION_IMPL* session, const char* fname, const char* v)
{
	WT_DECL_RET;
	const char *cfg[3];
	char *config, *newcfg;

	config = newcfg = NULL;

	/*通过文件名查找到对应文件的meta信息*/
	WT_ERR(__wt_metadata_search(session, fname, &config));

	/* Replace the checkpoint entry. 设置checkpoint到meta中，并更新meta信息*/
	cfg[0] = config;
	cfg[1] = v == NULL ? "checkpoint=()" : v;
	cfg[2] = NULL;
	WT_ERR(__wt_config_collapse(session, cfg, &newcfg));
	WT_ERR(__wt_metadata_update(session, fname, newcfg));

err:
	__wt_free(session, config);
	__wt_free(session, newcfg);
	return ret;
}

/*返回config配置对应文件被命名的checkpoint信息*/
static int __ckpt_named(WT_SESSION_IMPL* session, const char* checkpoint, const char* config, WT_CKPT* ckpt)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM k, v;

	WT_RET(__wt_config_getones(session, config, "checkpoint", &v));
	WT_RET(__wt_config_subinit(session, &ckptconf, &v));

	/*
	 * Take the first match: there should never be more than a single
	 * checkpoint of any name.
	 */
	while (__wt_config_next(&ckptconf, &k, &v) == 0)
		if (WT_STRING_MATCH(checkpoint, k.str, k.len))
			return (__ckpt_load(session, &k, &v, ckpt));

	return (WT_NOTFOUND);
}

/*比较两个checkpoint的order大小，主要是为qsort快速算法提供比较函数*/
static int WT_CDECL __ckpt_compare_order(const void* a, const* b)
{
	WT_CKPT *ackpt, *bckpt;

	ackpt = (WT_CKPT *)a;
	bckpt = (WT_CKPT *)b;

	return (ackpt->order > bckpt->order ? 1 : -1);
}

/*检查btree的版本是否匹配*/
static int __ckpt_version_chk(WT_SESSION_IMPL *session, const char *fname, const char *config)
{
	WT_CONFIG_ITEM a, v;
	int majorv, minorv;

	WT_RET(__wt_config_getones(session, config, "version", &v));
	WT_RET(__wt_config_subgets(session, &v, "major", &a));
	majorv = (int)a.val;
	WT_RET(__wt_config_subgets(session, &v, "minor", &a));
	minorv = (int)a.val;

	if (majorv < WT_BTREE_MAJOR_VERSION_MIN ||
		majorv > WT_BTREE_MAJOR_VERSION_MAX ||
		(majorv == WT_BTREE_MAJOR_VERSION_MIN && minorv < WT_BTREE_MINOR_VERSION_MIN) ||
		(majorv == WT_BTREE_MAJOR_VERSION_MAX && minorv > WT_BTREE_MINOR_VERSION_MAX))
		WT_RET_MSG(session, EACCES,
		"%s is an unsupported WiredTiger source file version %d.%d"
		"; this WiredTiger build only supports versions from %d.%d "
		"to %d.%d",
		fname,
		majorv, minorv,
		WT_BTREE_MAJOR_VERSION_MIN,
		WT_BTREE_MINOR_VERSION_MIN,
		WT_BTREE_MAJOR_VERSION_MAX,
		WT_BTREE_MINOR_VERSION_MAX);
	return 0;
}

/*获得fname对应文件的checkpoint信息*/
int __wt_meta_checkpoint(WT_SESSION_IMPL *session, const char *fname, const char *checkpoint, WT_CKPT *ckpt)
{
	WT_DECL_RET;
	char* config;

	config = NULL;
	/*查找fname对应的meta信息*/
	WT_ERR(__wt_metadata_search(session, fname, &config));
	/*检查版本*/
	WT_ERR(__ckpt_version_chk(session, fname, config));
	/*
	 * Retrieve the named checkpoint or the last checkpoint.
	 *
	 * If we don't find a named checkpoint, we're done, they're read-only.
	 * If we don't find a default checkpoint, it's creation, return "no
	 * data" and let our caller handle it.
	 */
	if(checkpoint == NULL){ /*没有指定checkpoint，直接取最后一个checkpoint*/
		if ((ret = __ckpt_last(session, config, ckpt)) == WT_NOTFOUND) {
			ret = 0;
			ckpt->addr.data = ckpt->raw.data = NULL;
			ckpt->addr.size = ckpt->raw.size = 0;
		}
	}
	else
		WT_ERR(__ckpt_named(session, checkpoint, config, ckpt));

err:
	__wt_free(session, config);
	return ret;
}

/*返回文件最后一个命名的的checkpoint信息*/
int __wt_meta_checkpoint_last_name(WT_SESSION_IMPL *session, const char *fname, const char **namep)
{
	WT_DECL_RET;
	char* config;

	config = NULL;

	WT_ERROR(__wt_metadata_search(session, fname, &config));
	/* Check the major/minor version numbers. */
	WT_ERR(__ckpt_version_chk(session, fname, config));
	/* Retrieve the name of the last unnamed checkpoint. */
	WT_ERR(__ckpt_last_name(session, config, namep));

err:	
	__wt_free(session, config);
	return (ret);
}

/*将fname对应的checkpoint元信息清空*/
int __wt_meta_checkpoint_clear(WT_SESSION_IMPL *session, const char *fname)
{
	WT_RET_NOTFOUND_OK(__ckpt_set(session, fname, NULL));

	return 0;
}

/*将fname对应的文件的checkpoint全部载入到ckptbase队列中*/
int __wt_meta_ckptlist_get(WT_SESSION_IMPL *session, const char *fname, WT_CKPT **ckptbasep)
{
	WT_CKPT *ckpt, *ckptbase;
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM k, v;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	size_t allocated, slot;
	char *config;

	*ckptbasep = NULL;

	ckptbase = NULL;
	allocated = slot = 0;
	config = NULL;

	WT_RET(__wt_metadata_search(session, fname, &config));

	/* Load any existing checkpoints into the array. */
	WT_ERR(__wt_scr_alloc(session, 0, &buf));
	/*将所有的checkpoint WT_CKPT读入ckptbase数组中*/
	if (__wt_config_getones(session, config, "checkpoint", &v) == 0 && __wt_config_subinit(session, &ckptconf, &v) == 0){
		for (; __wt_config_next(&ckptconf, &k, &v) == 0; ++slot) {
			WT_ERR(__wt_realloc_def(session, &allocated, slot + 1, &ckptbase));
			ckpt = &ckptbase[slot];

			WT_ERR(__ckpt_load(session, &k, &v, ckpt));
		}
	}

	WT_ERR(__wt_realloc_def(session, &allocated, slot + 2, &ckptbase));

	/* Sort in creation-order. 按order从小到大排序*/
	qsort(ckptbase, slot, sizeof(WT_CKPT), __ckpt_compare_order);

	*ckptbasep = ckptbase;

	if (0) {
err:		
		__wt_meta_ckptlist_free(session, ckptbase);
	}
	__wt_free(session, config);
	__wt_scr_free(session, &buf);

	return ret;
}

/*将一个WT_CKPT list中的meta checkpoint信息设置到对应的文件配置中*/
int __wt_meta_ckptlist_set(WT_SESSION_IMPL* session, const char* fname, WT_CKPT* ckptbase, WT_LSN* ckptlsn)
{
	WT_CKPT *ckpt;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	time_t secs;
	int64_t maxorder;
	const char *sep;

	WT_ERR(__wt_scr_alloc(session, 0, &buf));
	maxorder = 0;
	sep = "";
	WT_ERR(__wt_buf_fmt(session, buf, "checkpoint=("));

	WT_CKPT_FOREACH(ckptbase, ckpt){
		if (ckpt->order > maxorder)
			maxorder = ckpt->order;

		/* Skip deleted checkpoints. */
		if (F_ISSET(ckpt, WT_CKPT_DELETE))
			continue;

		if (F_ISSET(ckpt, WT_CKPT_ADD | WT_CKPT_UPDATE)) {
			/*
			 * We fake checkpoints for handles in the middle of a
			 * bulk load.  If there is a checkpoint, convert the
			 * raw cookie to a hex string.
			 */
			if (ckpt->raw.size == 0)
				ckpt->addr.size = 0;
			else
				WT_ERR(__wt_raw_to_hex(session, ckpt->raw.data, ckpt->raw.size, &ckpt->addr));

			/* Set the order and timestamp. */
			if (F_ISSET(ckpt, WT_CKPT_ADD))
				ckpt->order = ++maxorder;

			/*
			 * XXX
			 * Assumes a time_t fits into a uintmax_t, which isn't
			 * guaranteed, a time_t has to be an arithmetic type,
			 * but not an integral type.
			 */
			WT_ERR(__wt_seconds(session, &secs));
			ckpt->sec = (uintmax_t)secs;
		}
		/*构建checkpoint字串配置信息*/
		if (strcmp(ckpt->name, WT_CHECKPOINT) == 0)
			WT_ERR(__wt_buf_catfmt(session, buf,
			    "%s%s.%" PRId64 "=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64
			    ",write_gen=%" PRIu64 ")",
			    sep, ckpt->name, ckpt->order,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size,
			    ckpt->write_gen));
		else
			WT_ERR(__wt_buf_catfmt(session, buf,
			    "%s%s=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64
			    ",write_gen=%" PRIu64 ")",
			    sep, ckpt->name,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size,
			    ckpt->write_gen));
		sep = ",";
	}

	WT_ERR(__wt_buf_catfmt(session, buf, ")"));
	if (ckptlsn != NULL)
		WT_ERR(__wt_buf_catfmt(session, buf, ",checkpoint_lsn=(%" PRIu32 ",%" PRIuMAX ")", ckptlsn->file, (uintmax_t)ckptlsn->offset));
	WT_ERR(__ckpt_set(session, fname, buf->mem));

err:
	__wt_scr_free(session, &buf);
	return ret;
}

/*撤销checkpoint list*/
void __wt_meta_ckptlist_free(WT_SESSION_IMPL *session, WT_CKPT *ckptbase)
{
	WT_CKPT *ckpt;

	if (ckptbase == NULL)
		return;

	WT_CKPT_FOREACH(ckptbase, ckpt)
		__wt_meta_checkpoint_free(session, ckpt);
	__wt_free(session, ckptbase);
}

/*释放一个WT_CKPT结构对象*/
void __wt_meta_checkpoint_free(WT_SESSION_IMPL* session, WT_CKPT* ckpt)
{
	if (ckpt == NULL)
		return;

	__wt_free(session, ckpt->name);
	__wt_buf_free(session, &ckpt->addr);
	__wt_buf_free(session, &ckpt->raw);
	__wt_free(session, ckpt->bpriv);

	WT_CLEAR(*ckpt);		/* Clear to prepare for re-use. */
}

