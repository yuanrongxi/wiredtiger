
#include "wt_internal.h"


/*检查key对应值是否是从turtle file中读取*/
static int __metadata_turtle(const char* key)
{
	switch (key[0]) {
	case 'f':
		if (strcmp(key, WT_METAFILE_URI) == 0)
			return 1;
		break;
	case 'W':
		if (strcmp(key, "WiredTiger version") == 0)
			return 1;
		if (strcmp(key, "WiredTiger version string") == 0)
			return 1;
		break;
	}
	return 0;
}

/*打开一个meta file文件*/
int __wt_metadata_open(WT_SESSION_IMPL* session)
{
	if(session->meta_dhandle != NULL)
		return 0;

	WT_RET(__wt_session_get_btree(session, WT_METAFILE_URI, NULL, NULL, 0));

	session->meta_dhandle = session->dhandle;
	WT_ASSERT(session, session->meta_dhandle != NULL);

	return __wt_session_release_btree(session);
}

/*打开一个meta cursor*/
int __wt_metadata_cursor(WT_SESSION_IMPL* session, const char* config, WT_CURSOR** cursorp)
{
	WT_DATA_HANDLE* saved_dhandle;
	WT_DECL_RET;
	const char *cfg[] = { WT_CONFIG_BASE(session, session_open_cursor), config, NULL };

	saved_dhandle = session->dhandle;
	WT_ERR(__wt_metadata_open(session));

	session->dhandle = session->meta_dhandle;

	WT_ERR(__wt_session_lock_dhandle(session, 0));
	WT_ERR(__wt_curfile_create(session, NULL, cfg, 0, 0, cursorp));
	__wt_cursor_dhandle_incr_use(session);

err:
	session->dhandle = saved_dhandle;
	return ret;
}

/*插入一个meta key/value对到meta中*/
int __wt_metadata_insert(WT_SESSION_IMPL* session, const char* key, const char* value)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,"Insert: key: %s, value: %s, tracking: %s, %s" "turtle",
		key, value, WT_META_TRACKING(session) ? "true" : "false", __metadata_turtle(key) ? "" : "not "));

	if(__metadata_turtle(key))
		WT_RET_MSG(session, EINVAL, "%s: insert not supported on the turtle file", key);

	/*构建一个meta cursor，进行insert操作*/
	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	WT_ERR(cursor->insert(cursor));
	if (WT_META_TRACKING(session))
		WT_ERR(__wt_meta_track_insert(session, key));

err:
	WT_TRET(cursor->close(cursor));
	return ret;
}
/*更新一个meta k/v对到meta中*/
int __wt_metadata_update(WT_SESSION_IMPL* session, const char* key, const char* value)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,
		"Update: key: %s, value: %s, tracking: %s, %s" "turtle",key, value, WT_META_TRACKING(session) ? "true" : "false",
		__metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		return (__wt_turtle_update(session, key, value));

	if (WT_META_TRACKING(session))
		WT_RET(__wt_meta_track_update(session, key));

	WT_RET(__wt_metadata_cursor(session, "overwrite", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	WT_ERR(cursor->insert(cursor));

err:	
	WT_TRET(cursor->close(cursor));
	return ret;
}

/*从meta中删除key对应的元数据kv对*/
int __wt_metadata_remove(WT_SESSION_IMPL* session, const char* key)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA, "Remove: key: %s, tracking: %s, %s" "turtle",key, 
		WT_META_TRACKING(session) ? "true" : "false", __metadata_turtle(key) ? "" : "not "));

	if(__metadata_turtle(key))
		WT_RET_MSG(session, EINVAL, "%s: remove not supported on the turtle file", key);

	/*通过meta cursor删除掉对应的kv对*/
	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	WT_ERR(cursor->search(cursor));
	if (WT_META_TRACKING(session))
		WT_ERR(__wt_meta_track_update(session, key));

	WT_ERR(cursor->remove(cursor));

err:
	WT_TRET(cursor->close(cursor));
	return ret;
}

/*在meta中查找key对应的value值，并拷贝对应的value进行返回*/
int __wt_metadata_search(WT_SESSION_IMPL* session, const char* key, char** valuep)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	const char *value;

	*valuep = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA, "Search: key: %s, tracking: %s, %s" "turtle", 
		key, WT_META_TRACKING(session) ? "true" : "false", __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		return (__wt_turtle_read(session, key, valuep));

	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	WT_ERR(cursor->search(cursor));
	WT_ERR(cursor->get_value(cursor, &value));
	WT_ERR(__wt_strdup(session, value, valuep));

err:
	WT_TRET(cursor->close(cursor));
	return ret;
}
