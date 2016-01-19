/***********************************************************
 * redo log推演实现
 **********************************************************/
#include "wt_internal.h"

typedef struct
{
	WT_SESSION_IMPL* session;

	struct WT_RECOVERY_FILE
	{
		const char* uri;		/* File URI. */
		WT_CURSOR* c;			/* Cursor used for recovery. */
		WT_LSN ckpt_lsn;		/* File's checkpoint LSN. */
	} *files;

	size_t file_alloc;			/* Allocated size of files array. */
	u_int max_fileid;			/* Maximum file ID seen. */
	u_int nfiles;				/* Number of files in the metadata. */

	WT_LSN ckpt_lsn;			/* Start LSN for main recovery loop. */

	int missing;				/* Were there missing files? */
	int modified;				/* Did recovery make any changes? */
	int metadata_only;			/* Set during the first recovery pass, when only the metadata is recovered. */
}WT_RECOVERY;

/*为日志推演构建一个cursor*/
static int __recovery_cursor(WT_SESSION_IMPL* session, WT_RECOVERY* r, WT_LSN* lsnp, u_int id, int duplicate, WT_CURSOR** cp)
{
	WT_CURSOR *c;
	const char *cfg[] = { WT_CONFIG_BASE(session, session_open_cursor), "overwrite", NULL };
	int metadata_op;

	c = NULL;

	/*
	* Metadata operations have an id of 0.  Match operations based
	* on the id and the current pass of recovery for metadata.
	*
	* Only apply operations in the correct metadata phase, and if the LSN
	* is more recent than the last checkpoint.  If there is no entry for a
	* file, assume it was dropped or missing after a hot backup.
	*/
	metadata_op = (id == WT_METAFILE_ID);
	if (r->metadata_only != metadata_op)
		;
	else if(id >= r->nfiles || r->files[id].uri == NULL){
		/* If a file is missing, output a verbose message once. */
		if (!r->missing)
			WT_RET(__wt_verbose(session, WT_VERB_RECOVERY,
			"No file found with ID %u (max %u)", id, r->nfiles));
		r->missing = 1;
	}
	else if(LOG_CMP(lsnp, &r->files[id].ckpt_lsn) >= 0){
		if ((c = r->files[id].c) == NULL) {
			WT_RET(__wt_open_cursor(session, r->files[id].uri, NULL, cfg, &c));
			r->files[id].c = c;
		}
	}

	/*open cursor,确保cursor是opened状态*/
	if (duplicate && c != NULL)
		WT_RET(__wt_open_cursor(session, r->files[id].uri, NULL, cfg, &c));

	*cp = c;

	return 0;
}

/*创建一个recovery cursor对象*/
#define	GET_RECOVERY_CURSOR(session, r, lsnp, fileid, cp)		\
	WT_ERR(__recovery_cursor(									\
	    (session), (r), (lsnp), (fileid), 0, (cp)));			\
	WT_ERR(__wt_verbose((session), WT_VERB_RECOVERY,			\
	    "%s op %d to file %d at LSN %u/%" PRIuMAX,				\
	    (cursor == NULL) ? "Skipping" : "Applying",				\
	    optype, fileid, lsnp->file, (uintmax_t)lsnp->offset));	\
	if (cursor == NULL)											\
		break

/*日志推演一个操作*/
static int txn_op_apply(WT_RECOVERY* r, WT_LSN* lsnp, const uint8_t** pp, const uint8_t* end)
{
	WT_CURSOR *cursor, *start, *stop;
	WT_DECL_RET;
	WT_ITEM key, start_key, stop_key, value;
	WT_SESSION_IMPL *session;
	uint64_t recno, start_recno, stop_recno;
	uint32_t fileid, mode, optype, opsize;

	session = r->session;
	cursor = NULL;

	/*读取一个日志的操作类型*/
	WT_ERR(__wt_logop_read(session, pp, end, &optype, &opsize));
	end = *pp + opsize;

	switch (optype){
	case WT_LOGOP_COL_PUT:
		WT_ERR(__wt_logop_col_put_unpack(session, pp, end, &fileid, &recno, &value));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		cursor->set_key(cursor, recno);
		__wt_cursor_set_raw_value(cursor, &value);
		WT_ERR(cursor->insert(cursor));
		break;

	case WT_LOGOP_COL_REMOVE:
		WT_ERR(__wt_logop_col_remove_unpack(session, pp, end, &fileid, &recno));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		cursor->set_key(cursor, recno);
		WT_ERR(cursor->remove(cursor));
		break;

	case WT_LOGOP_COL_TRUNCATE:
		WT_ERR(__wt_logop_col_truncate_unpack(session, pp, end, &fileid, &start_recno, &stop_recno));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);

		/*设置cursor的start/stop位置*/
		if (start_recno == 0){
			start = NULL;
			stop = cursor;
		}
		else if (stop_recno == 0){
			start = cursor;
			stop = NULL;
		}
		else{
			start = cursor;
			WT_ERR(__recovery_cursor(session, r, lsnp, filedid, 1, &stop));
		}

		/*设置KEY*/
		if (start != NULL)
			start->set_key(start, start_recno);
		if (stop != NULL)
			stop->set_key(stop, stop_recno);
		/*进行truncate操作*/
		WT_TRET(session->iface.truncate(&session->iface, NULL, start, stop, NULL));
		if (stop != NULL && stop != cursor)
			WT_TRET(stop->close(stop));
		WT_ERR(ret);
		break;

	case WT_LOGOP_ROW_PUT:
		WT_ERR(__wt_logop_row_put_unpack(session, pp, end, &fileid, &key, &value));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		__wt_cursor_set_raw_key(cursor, &key);
		__wt_cursor_set_raw_value(cursor, &value);
		WT_ERR(cursor->insert(cursor));
		break;

	case WT_LOGOP_ROW_REMOVE:
		WT_ERR(__wt_logop_row_remove_unpack(session, pp, end, &fileid, &key));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		__wt_cursor_set_raw_key(cursor, &key);
		WT_ERR(cursor->remove(cursor));
		break;

		/*row truncate*/
	case WT_LOGOP_ROW_TRUNCATE:
		WT_ERR(__wt_logop_row_truncate_unpack(session, pp, end, &fileid, &start_key, &stop_key, &mode));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);

		/*设置truncate操作start/stop位置*/
		start = stop = NULL;
		switch (mode){
		case TXN_TRUNC_ALL:
			/* Both cursors stay NULL. */
			break;
		case TXN_TRUNC_BOTH:
			start = cursor;
			WT_ERR(__recovery_cursor(session, r, lsnp, fileid, 1, &stop));
			break;
		case TXN_TRUNC_START:
			start = cursor;
			break;
		case TXN_TRUNC_STOP:
			stop = cursor;
			break;

			WT_ILLEGAL_VALUE_ERR(session);
		}
		/*设置start key /stop key*/
		if (start != NULL)
			__wt_cursor_set_raw_key(start, &start_key);
		if (stop != NULL)
			__wt_cursor_set_raw_key(stop, &stop_key);

		WT_TRET(session->iface.truncate(&session->iface, NULL, start, stop, NULL));
		/* If we opened a duplicate cursor, close it now. */
		if (stop != NULL && stop != cursor)
			WT_TRET(stop->close(stop));
		WT_ERR(ret);

		break;

	WT_ILLEGAL_VALUE_ERR(session);
	}

	/* Reset the cursor so it doesn't block eviction. */
	if (cursor != NULL)
		WT_ERR(cursor->reset(cursor));

	r->modified = 1;

err:
	if (ret != 0)
		__wt_err(session, ret, "Operation failed during recovery");

	return ret;
}

/*进行一条日志的推演*/
static int __txn_commit_apply(WT_RECOVERY* r, WT_LSN* lsnp, const uint8_t** pp, const uint8_t* end)
{
	WT_UNUSED(lsnp);

	while (*pp < end && **pp)
		WT_RET(__txn_op_apply(r, lsnp, pp, end));

	return 0;
}

/*读取一条logrec,并进行推演*/
static int __txn_log_recover(WT_SESSION_IMPL* session, WT_ITEM* logrec, WT_LSN* lsnp, WT_LSN* next_lsnp, void* cookie, int firstrecord)
{
	WT_RECOVERY *r;
	const uint8_t *end, *p;
	uint64_t txnid;
	uint32_t rectype;

	WT_UNUSED(next_lsnp);
	r = cookie;
	p = LOG_SKIP_HEADER(logrec->data);
	end = (const uint8_t *)logrec->data + logrec->size;
	WT_UNUSED(firstrecord);

	/*读取日志类型*/
	WT_RET(__wt_logrec_read(session, &p, end, &rectype));

	switch (rectype){
	case WT_LOGREC_CHECKPOINT: /*checkpoint*/
		if (r->metadata_only)
			WT_RET(__wt_txn_checkpoint_logread(session, &p, end, &r->ckpt_lsn));
		break;

	case WT_LOGREC_COMMIT:/*commit*/
		WT_RET(__wt_vunpack_uint(&p, WT_PTRDIFF(end, p), &txnid));
		WT_UNUSED(txnid);
		WT_RET(__txn_commit_apply(r, lsnp, &p, end));
		break;
	}

	return 0;
}

/*通过config信息确定一个recover file对象*/
static int __recovery_setup_file(WT_RECOVERY* r, const char* uri, const char* config)
{
	WT_CONFIG_ITEM cval;
	WT_LSN lsn;
	intmax_t offset;
	uint32_t fileid;

	WT_RET(__wt_config_getones(r->session, config, "id", &cval));
	fileid = (uint32_t)cval.val;

	/*确定max file*/
	if (fileid > r->max_fileid)
		r->max_fileid = fileid;

	/*扩大file对象数组*/
	if (r->nfiles <= fileid){
		WT_RET(__wt_realloc_def(r->session, &r->file_alloc, fileid + 1, &r->files));
		r->nfiles = fileid + 1;
	}

	/*赋值uri*/
	WT_RET(__wt_strdup(r->session, uri, &r->files[fileid].uri));
	WT_RET(__wt_config_getones(r->session, config, "checkpoint_lsn", &cval)); /*获得config中的checkpoint lsn*/

	if (cval.type != WT_CONFIG_ITEM_STRUCT)
		WT_INIT_LSN(&lsn);
	else if (sscanf(cval.str, "(%" SCNu32 ",%" SCNdMAX ")", &lsn.file, &offset) == 2) /*从cval中获取lsn信息*/
		lsn.offset = offset;
	else
		WT_RET_MSG(r->session, EINVAL, "Failed to parse checkpoint LSN '%.*s'", (int)cval.len, cval.str);

	r->files[fileid].ckpt_lsn = lsn;

	WT_RET(__wt_verbose(r->session, WT_VERB_RECOVERY, "Recovering %s with id %u @ (%" PRIu32 ", %" PRIu64 ")", uri, fileid, lsn.file, lsn.offset));

	return 0;
}

/*释放recover file对象和对应的cursor*/
static int __recovery_free(WT_RECOVERY* r)
{
	WT_CURSOR *c;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	u_int i;

	session = r->session;
	for (i = 0; i < r->nfiles; i++){
		__wt_free(session, r->files[i].uri);
		c = r->files[i].c;
		if (c != NULL)
			WT_RET(c->close(c));
	}

	__wt_free(session, r->files);

	return ret;
}

static int __recovery_file_scan(WT_RECOVERY* r)
{
	WT_DECL_RET;
	WT_CURSOR* c;
	const char* uri, *config;
	int cmp;

	c = r->files[0].c;
	c->set_key(c, "file:");
	
	ret = c->search_near(c, &cmp);
	if (ret != 0){
		if (ret == WT_NOTFOUND)
			ret = 0;
		goto err;
	}

	if (cmp < 0)
		WT_ERR_NOTFOUND_OK(c->next(c));

	/* 通过uri中的config设置recover file对象 */
	for (; ret == 0; ret = c->next(c)){
		WT_ERR(c->get_key(c, &uri));
		if (!WT_PREFIX_MATCH(uri, "file:"))
			break;
		WT_ERR(c->get_value(c, &config));
		WT_ERR(__recovery_setup_file(r, uri, config));
	}

	WT_ERR_NOTFOUND_OK(ret);

err:
	return ret;
}

/*redo log推演*/
int __wt_txn_recover(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_CURSOR *metac;
	WT_DECL_RET;
	WT_RECOVERY r;
	struct WT_RECOVERY_FILE *metafile;
	char *config;
	int needs_rec, was_backup;

	conn = S2C(session);
	WT_CLEAR(r);
	WT_INIT_LSN(&r.ckpt_lsn);
	was_backup = F_ISSET(conn, WT_CONN_WAS_BACKUP) ? 1 : 0;

	/*创建一个recover session*/
	WT_RET(__wt_open_session(conn, NULL, NULL, &session));
	F_SET(session, WT_SESSION_NO_LOGGING);
	r.session = session;

	/*确定meta log file的cursor*/
	WT_ERR(__wt_metadata_search(session, WT_METAFILE_URI, &config));
	WT_ERR(__recovery_setup_file(&r, WT_METAFILE_URI, config));
	WT_ERR(__wt_metadata_cursor(session, NULL, &metac));
	metafile = &r.files[WT_METAFILE_ID];
	metafile->c = metac;
   
   /*
	* If no log was found (including if logging is disabled), or if the
	* last checkpoint was done with logging disabled, recovery should not
	* run.  Scan the metadata to figure out the largest file ID.
	*/
	if (!FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_EXISTED) || WT_IS_MAX_LSN(&metafile->ckpt_lsn)){
		WT_ERR(__recovery_file_scan(&r));
		conn->next_file_id = r.max_fileid;
	}

	/*
	* First, do a pass through the log to recover the metadata, and
	* establish the last checkpoint LSN.  Skip this when opening a hot
	* backup: we already have the correct metadata in that case.
	* 推演meta redo log
	*/
	if (!was_backup){
		r.metadata_only = 1;
		if (WT_IS_INIT_LSN(&metafile->ckpt_lsn))
			WT_ERR(__wt_log_scan(session, NULL, WT_LOGSCAN_FIRST, __txn_log_recover, &r));
		else {
			/*
			* Start at the last checkpoint LSN referenced in the
			* metadata.  If we see the end of a checkpoint while
			* scanning, we will change the full scan to start from
			* there.
			*/
			r.ckpt_lsn = metafile->ckpt_lsn;
			WT_ERR(__wt_log_scan(session, &metafile->ckpt_lsn, 0, __txn_log_recover, &r));
		}
	}

	/* Scan the metadata to find the live files and their IDs. */
	WT_ERR(__recovery_file_scan(&r));

	/*
	* We no longer need the metadata cursor: close it to avoid pinning any
	* resources that could block eviction during recovery.
	*/
	r.files[0].c = NULL;
	WT_ERR(metac->close(metac));

	r.metadata_only = 0;
	WT_ERR(__wt_verbose(session, WT_VERB_RECOVERY, "Main recovery loop: starting at %u/%" PRIuMAX, r.ckpt_lsn.file, (uintmax_t)r.ckpt_lsn.offset));
	/*检查redo log重演标示*/
	WT_ERR(__wt_log_needs_recovery(session, &r.ckpt_lsn, &needs_rec));

   /*
	* Check if the database was shut down cleanly.  If not
	* return an error if the user does not want automatic
	* recovery.
	*/
	if (needs_rec && FLD_ISSET(conn->log_flags, WT_CONN_LOG_RECOVER_ERR))
		WT_ERR(WT_RUN_RECOVERY);

	/*
	* Always run recovery even if it was a clean shutdown.
	* We can consider skipping it in the future.
	*/
	if (WT_IS_INIT_LSN(&r.ckpt_lsn))
		WT_ERR(__wt_log_scan(session, NULL, WT_LOGSCAN_FIRST | WT_LOGSCAN_RECOVER, __txn_log_recover, &r)); /*从开始推演？？*/
	else
		WT_ERR(__wt_log_scan(session, &r.ckpt_lsn, WT_LOGSCAN_RECOVER, __txn_log_recover, &r));

	conn->next_file_id = r.max_fileid;

	/*推演完成，建立一个checkpoint*/
	WT_ERR(session->iface.checkpoint(&session->iface, "force=1"));

done:
err:
	WT_TRET(__recovery_free(&r));
   __wt_free(session, config);
   WT_TRET(session->iface.close(&session->iface, NULL));

   return (ret);
}




