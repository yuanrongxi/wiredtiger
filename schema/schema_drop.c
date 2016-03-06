
#include "wt_internal.h"

/*删除一个文件*/
static int __drop_file(WT_SESSION_IMPL* session, const char* uri, int force, const char* cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	int remove_files;
	const char *filename;

	WT_RET(__wt_config_gets(session, cfg, "remove_files", &cval));
	remove_files = (cval.val != 0); /*确定文件是要删除*/

	filename = uri;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		return (EINVAL);

	/*在获取到handle list lock后再执行__wt_conn_dhandle_close_all函数,关于这个文件所有关联的btree对象*/
	WT_WITH_DHANDLE_LOCK(session, ret = __wt_conn_dhandle_close_all(session, uri, force));
	WT_RET(ret);
	/*删除对应的文件meta数据*/
	WT_TRET(__wt_metadata_remove(session, uri));

	if (!remove_files)
		return ret;
	/*删除物理文件*/
	WT_TRET(__wt_remove_if_exists(session, filename));

	return ret;
}

/*删除uri对应的colgroup*/
static int __drop_colgroup(WT_SESSION_IMPL* session, const char* uri, int force, const char* cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_TABLE *table;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_TABLE_LOCKED));

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __wt_schema_get_colgroup( session, uri, force, &table, &colgroup)) == 0) {
			table->cg_complete = 0;
			WT_TRET(__wt_schema_drop(session, colgroup->source, cfg));
	}
	/*删除掉对应的meta数据*/
	WT_TRET(__wt_metadata_remove(session, uri));
	return ret;
}

/*删除一个索引对象*/
static int __drop_index(WT_SESSION_IMPL *session, const char *uri, int force, const char *cfg[])
{
	WT_INDEX *idx;
	WT_DECL_RET;
	WT_TABLE *table;

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __wt_schema_get_index(session, uri, force, &table, &idx)) == 0) {
			table->idx_complete = 0;
			WT_TRET(__wt_schema_drop(session, idx->source, cfg));
	}

	WT_TRET(__wt_metadata_remove(session, uri));
	return ret;
}

/*删除一个表对象*/
static int __drop_table(WT_SESSION_IMPL* session, const char* uri, const char* cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_INDEX *idx;
	WT_TABLE *table;
	const char *name;
	u_int i;

	name = uri;
	(void)WT_PREFIX_SKIP(name, "table:");

	table = NULL;
	WT_ERR(__wt_schema_get_table(session, name, strlen(name), 1, &table));

	/* Drop the column groups. */
	for (i = 0; i < WT_COLGROUPS(table); i++) {
		if ((colgroup = table->cgroups[i]) == NULL)
			continue;
		WT_ERR(__wt_metadata_remove(session, colgroup->name));
		WT_ERR(__wt_schema_drop(session, colgroup->source, cfg));
	}

	/* Drop the indices. */
	WT_ERR(__wt_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++) {
		if ((idx = table->indices[i]) == NULL)
			continue;
		WT_ERR(__wt_metadata_remove(session, idx->name));
		WT_ERR(__wt_schema_drop(session, idx->source, cfg));
	}

	WT_ERR(__wt_schema_remove_table(session, table));
	table = NULL;

	/* Remove the metadata entry (ignore missing items). */
	WT_ERR(__wt_metadata_remove(session, uri));

err:
	if(table != NULL)
		__wt_schema_release_table(session, table);

	return ret;
}

int __wt_schema_drop(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DATA_SOURCE *dsrc;
	WT_DECL_RET;
	int force;

	WT_RET(__wt_config_gets_def(session, cfg, "force", 0, &cval));
	force = (cval.val != 0);

	WT_RET(__wt_meta_track_on(session));

	/* Paranoia: clear any handle from our caller. */
	session->dhandle = NULL;

	if (WT_PREFIX_MATCH(uri, "colgroup:"))
		ret = __drop_colgroup(session, uri, force, cfg);
	else if (WT_PREFIX_MATCH(uri, "file:"))
		ret = __drop_file(session, uri, force, cfg);
	else if (WT_PREFIX_MATCH(uri, "index:"))
		ret = __drop_index(session, uri, force, cfg);
	else if (WT_PREFIX_MATCH(uri, "lsm:"))
		ret = __wt_lsm_tree_drop(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "table:"))
		ret = __drop_table(session, uri, cfg);
	else if ((dsrc = __wt_schema_get_source(session, uri)) != NULL)
		ret = dsrc->drop == NULL ?
		__wt_object_unsupported(session, uri) :
	dsrc->drop(
		dsrc, &session->iface, uri, (WT_CONFIG_ARG *)cfg);
	else
		ret = __wt_bad_object_type(session, uri);

	if(ret == WT_NOTFOUND || ret == ENOENT)
		ret = force ? 0 : ENOENT;

	/* Bump the schema generation so that stale data is ignored.标记schema的版本 */
	++ S2C(session)->schema_gen;

	WT_TRET(__wt_meta_track_off(session, 1, ret != 0));

	return ret;
}

