/********************************************************
 * 事务的redo log实现
 *******************************************************/

#include "wt_internal.h"

/*记录当前事务执行操作的redo log*/
static int __txn_op_log(WT_SESSION_IMPL* session, WT_ITEM* logrec, WT_TXN_OP* op, WT_CURSOR_BTREE* cbt)
{
	WT_DECL_RET;
	WT_ITEM key, value;
	WT_UPDATE *upd;
	uint64_t recno;

	WT_CLEAR(key);
	upd = op->u.upd;
	value.data = WT_UPDATE_DATA(upd);
	value.size = upd->size;

	/*column store的操作日志*/
	if (cbt->btree->type != BTREE_ROW){
		WT_ASSERT(session, cbt->ins != NULL);
		recno = WT_INSERT_RECNO(cbt->ins);
		WT_ASSERT(session, recno != 0);

		if (WT_UPDATE_DELETED_ISSET(upd))
			WT_ERR(__wt_logop_col_remove_pack(session, logrec, op->fileid, recno)); /*column删除的log*/
		else
			WT_ERR(__wt_logop_col_put_pack(session, logrec, op->fileid, recno, &value));/*column修改的log*/
	}
	else{ /*row store的操作日志*/
		WT_ERR(__wt_cursor_row_leaf_key(cbt, &key));

		if (WT_UPDATE_DELETED_ISSET(upd))
			WT_ERR(__wt_logop_row_remove_pack(session, logrec, op->fileid, &key));
		else
			WT_ERR(__wt_logop_row_put_pack(session, logrec, op->fileid, &key, &value));
	}

err:
	__wt_buf_free(session, &key);
	return ret;
}

/*将log内容以json格式输出到out文件中*/
static int __txn_commit_printlog(WT_SESSION_IMPL* session, const uint8_t** pp, uint8_t* end, FILE* out)
{
	int firstrecord;

	firstrecord = 1;
	fprintf(out, "    \"ops\": [\n");

	while (*pp < end && **pp){
		if (!firstrecord)
			fprintf(out, ",\n");
		fprintf(out, "      {");
		
		firstrecord = 1;
		WT_RET(__wt_txn_op_printlog(session, pp, end, out)); /*日志以JSON格式输出*/
		fprintf(out, "\n      }");
	}

	fprintf(out, "\n    ]\n");
	return 0;
}

/*释放一个事务的opertion结构对象*/
void __wt_txn_op_free(WT_SESSION_IMPL* session, WT_TXN_OP* op)
{
	switch (op->type){
	case TXN_OP_BASIC:
	case TXN_OP_INMEM:
	case TXN_OP_REF:
	case TXN_OP_TRUNCATE_COL:
		break;

	case TXN_OP_TRUNCATE_ROW:
		__wt_buf_free(session, &op->u.truncate_row.start);
		__wt_buf_free(session, &op->u.truncate_row.stop);
		break;
	}
}

/*为事务分配一个log buffer,并初始化这个buffer,将事务的一些固定信息设置到buffer中*/
static int __txn_logrec_init(WT_SESSION_IMPL* session)
{
	WT_DECL_ITEM(logrec);
	WT_DECL_RET;
	WT_TXN *txn;
	const char *fmt = WT_UNCHECKED_STRING(Iq);
	uint32_t rectype = WT_LOGREC_COMMIT;
	size_t header_size;

	txn = &session->txn;
	if (txn->logrec != NULL)
		return 0;

	/*为txn分配一个logrec空间*/
	WT_ASSERT(session, txn->id != WT_TXN_NONE);
	WT_RET(__wt_struct_size(session, &header_size, fmt, rectype, txn->id));
	WT_RET(__wt_logrec_alloc(session, header_size, &logrec));

	/*格式一个txn log header到logrec中*/
	WT_ERR(__wt_struct_pack(session, (uint8_t *)logrec->data + logrec->size, header_size, fmt, rectype, txn->id));
	logrec->size += (uint32_t)header_size;
	txn->logrec = logrec;

	if (0){
err:
		__wt_logrec_free(session, &logrec);
	}

	return 0;
}

/*将事务操作列表中最后一个操作的日志记录到txn->logrec中*/
int __wt_txn_log_op(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* cbt)
{
	WT_ITEM *logrec;
	WT_TXN *txn;
	WT_TXN_OP *op;

	if (!FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED) || F_ISSET(session, WT_SESSION_NO_LOGGING))
		return 0;

	txn = &session->txn;
	/* We'd better have a transaction. */
	WT_ASSERT(session, F_ISSET(txn, TXN_RUNNING) && F_ISSET(txn, TXN_HAS_ID));

	WT_ASSERT(session, txn->mod_count > 0);
	op = txn->mod + txn->mod_count - 1; /*op是事务最后一个操作*/

	WT_ERR(__txn_logrec_init(session));
	logrec = txn->logrec;

	switch (op->type){
	case TXN_OP_BASIC:
		return (__txn_op_log(session, logrec, op, cbt));

	case TXN_OP_INMEM:
	case TXN_OP_REF:
		return 0;

	case TXN_OP_TRUNCATE_COL:
		return (__wt_logop_col_truncate_pack(session, logrec,
			op->fileid, op->u.truncate_col.start, op->u.truncate_col.stop));

	case TXN_OP_TRUNCATE_ROW:
		return (__wt_logop_row_truncate_pack(session, txn->logrec,
			op->fileid, &op->u.truncate_row.start, &op->u.truncate_row.stop, (uint32_t)op->u.truncate_row.mode));
	WT_ILLEGAL_VALUE(session);
	}
}

/*将session执行事务的log内容进行落盘,事务commit的时候调用--force log at commit*/
int __wt_txn_log_commit(WT_SESSION_IMPL* session, const char* cfg[])
{
	WT_TXN *txn;

	WT_UNUSED(cfg);
	txn = &session->txn;

	/* Write updates to the log. 日志落盘*/
	return (__wt_log_write(session, txn->logrec, NULL, txn->txn_logsync));
}

/*写入一条btree数据sync file的日志*/
static int __txn_log_file_sync(WT_SESSION_IMPL* session, uint32_t flags, WT_LSN* lsnp)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(logrec);
	WT_DECL_RET;
	size_t header_size;
	uint32_t rectype = WT_LOGREC_FILE_SYNC;
	int start, need_sync;
	const char *fmt = WT_UNCHECKED_STRING(III);

	btree = S2BT(session);
	start = LF_ISSET(WT_TXN_LOG_CKPT_START);
	need_sync = LF_ISSET(WT_TXN_LOG_CKPT_SYNC);

	/*分配一个logrec buffer*/
	WT_RET(__wt_struct_size(session, &header_size, fmt, rectype, btree->id, start));
	WT_RET(__wt_logrec_alloc(session, header_size, &logrec));
	/*写入一条WT_LOGREC_FILE_SYNC类型的日志，日志的意思是btree数据进行了sync file操作*/
	WT_ERR(__wt_struct_pack(session, (uint8_t *)logrec->data + logrec->size, header_size, fmt, rectype, btree->id, start));
	logrec->size += (uint32_t)header_size;

	/*将这条日志写入log file*/
	WT_ERR(__wt_log_write(session, logrec, lsnp, need_sync ? WT_LOG_FSYNC : 0));

err:
	__wt_logrec_free(session, &logrec);
	return ret;
}

/*读取一条checkpoint信息的log，推演日志用于事务恢复*/
int __wt_txn_checkpoint_logread(WT_SESSION_IMPL* session, const uint8_t** pp, const uint8_t* end, WT_LSN* ckpt_lsn)
{
	WT_ITEM ckpt_snapshot;
	u_int ckpt_nsnapshot;
	const char *fmt = WT_UNCHECKED_STRING(IQIU);

	WT_RET(__wt_struct_unpack(session, *pp, WT_PTRDIFF(end, *pp), fmt,
		&ckpt_lsn->file, &ckpt_lsn->offset, &ckpt_nsnapshot, &ckpt_snapshot));

	WT_UNUSED(ckpt_nsnapshot);
	WT_UNUSED(ckpt_snapshot);
	*pp = end;

	return 0;
}

/*写入一条checkpoint操作的log*/
int __wt_txn_checkpoint_log(WT_SESSION_IMPL *session, int full, uint32_t flags, WT_LSN *lsnp)
{
	WT_DECL_ITEM(logrec);
	WT_DECL_RET;
	WT_ITEM *ckpt_snapshot, empty;
	WT_LSN *ckpt_lsn;
	WT_TXN *txn;
	uint8_t *end, *p;
	size_t recsize;
	uint32_t i, rectype = WT_LOGREC_CHECKPOINT;
	const char *fmt = WT_UNCHECKED_STRING(IIQIU);

	txn = &session->txn;
	ckpt_lsn = &txn->ckpt_lsn;

	/*
	 * If this is a file sync, log it unless there is a full checkpoint in
	 * progress.
	 */
	if (!full){
		if (txn->full_ckpt){
			if (lsnp != NULL)
				*lsnp = *ckpt_lsn;
			return 0;
		}
		else
			return __txn_log_file_sync(session, flags, lsnp);
	}

	switch (flags){
	case WT_TXN_LOG_CKPT_PREPARE:
		txn->full_ckpt = 1;
		*ckpt_lsn = S2C(session)->log->alloc_lsn;
		break;

	case WT_TXN_LOG_CKPT_START:
		/*将txn中的snapshot值全部拷贝到checkpoint_snapshot中*/
		txn->ckpt_nsnapshot = txn->snapshot_count;
		recsize = txn->ckpt_nsnapshot * WT_INTPACK64_MAXSIZE;
		WT_ERR(__wt_scr_alloc(session, recsize, &txn->ckpt_snapshot));
		p = txn->ckpt_snapshot->mem;
		end = p + recsize;
		for (i = 0; i < txn->snapshot_count; i++)
			WT_ERR(__wt_vpack_uint(&p, WT_PTRDIFF(end, p), txn->snapshot[i]));
		break;

	case WT_TXN_LOG_CKPT_STOP:
		/*
		* During a clean connection close, we get here without the
		* prepare or start steps.  In that case, log the current LSN
		* as the checkpoint LSN.
		*/
		if (!txn->full_ckpt) {
			txn->ckpt_nsnapshot = 0;
			WT_CLEAR(empty);
			ckpt_snapshot = &empty;
			*ckpt_lsn = S2C(session)->log->alloc_lsn;
		}
		else
			ckpt_snapshot = txn->ckpt_snapshot;

		WT_ERR(__wt_struct_size(session, &recsize, fmt, rectype, ckpt_lsn->file, ckpt_lsn->offset, txn->ckpt_nsnapshot, ckpt_snapshot));
		WT_ERR(__wt_logrec_alloc(session, recsize, &logrec));
		/*将snapshot数据打包到logrec缓冲区中*/
		WT_ERR(__wt_struct_pack(session, (uint8_t *)logrec->data + logrec->size, recsize, fmt,
			rectype, ckpt_lsn->file, ckpt_lsn->offset, txn->ckpt_nsnapshot, ckpt_snapshot));
		logrec->size += (uint32_t)recsize;
		/*将logrec写入到log文件*/
		WT_ERR(__wt_log_write(session, logrec, lsnp, F_ISSET(S2C(session), WT_CONN_CKPT_SYNC) ? WT_LOG_FSYNC : 0));

		/*通知checkpoint归档线程checkpoint已经完成，可以将小于checkpoint LSN的日志归档*/
		if (!S2C(session)->hot_backup)
			WT_ERR(__wt_log_ckpt(session, ckpt_lsn));

	case WT_TXN_LOG_CKPT_FAIL: /*checkpoint失败，清空所有WT_TXN_LOG_CKPT_START保存的snapshot数据*/
		WT_INIT_LSN(ckpt_lsn);
		txn->ckpt_nsnapshot = 0;
		__wt_scr_free(session, &txn->ckpt_snapshot);
		txn->full_ckpt = 0;
		break;

	WT_ILLEGAL_VALUE_ERR(session);
	}

err:
	__wt_logrec_free(session, &logrec);
	return ret;
}

/*为truncate btree range记录一条操作日志*/
int __wt_txn_truncate_log(WT_SESSION_IMPL* session, WT_CURSOR_BTREE* start, WT_CURSOR_BTREE* stop)
{
	WT_BTREE *btree;
	WT_ITEM *item;
	WT_TXN_OP *op;

	btree = S2BT(session);

	/*获得执行事务队列中的一个操作对象，用于记录truncate file操作*/
	WT_RET(__txn_next_op(session, &op));

	if (btree->type == BTREE_ROW){ /* row store */
		op->type = TXN_OP_TRUNCATE_ROW;
		op->u.truncate_row.mode = TXN_TRUNC_ALL;
		WT_CLEAR(op->u.truncate_row.start);
		WT_CLEAR(op->u.truncate_row.stop);

		/*设置truncate start位置*/
		if (start != NULL){
			op->u.truncate_row.mode = TXN_TRUNC_START;
			item = &op->u.truncate_row.start;
			WT_RET(__wt_cursor_get_raw_key(&start->iface, item));
			WT_RET(__wt_buf_set(session, item, item->data, item->size));
		}
		/*设置truncate stop位置*/
		if (stop != NULL){
			op->u.truncate_row.mode = (op->u.truncate_row.mode == TXN_TRUNC_ALL) ? TXN_TRUNC_STOP : TXN_TRUNC_BOTH;
			item = &op->u.truncate_row.stop;
			WT_RET(__wt_cursor_get_raw_key(&stop->iface, item));
			WT_RET(__wt_buf_set(session, item, item->data, item->size));
		}
	}
	else{ /*column store,设置truncate范围位置*/
		op->type = TXN_OP_TRUNCATE_COL;
		op->u.truncate_col.start = (start == NULL) ? 0 : start->recno;
		op->u.truncate_col.stop = (stop == NULL) ? 0 : stop->recno;
	}
	/* Write that operation into the in-memory log（txn->logrec）. */
	WT_RET(__wt_txn_log_op(session, NULL));

	WT_ASSERT(session, !F_ISSET(session, WT_SESSION_LOGGING_INMEM));
	/*设置日志记录的标示状态（INMEM状态），*/
	F_SET(session, WT_SESSION_LOGGING_INMEM);

	return 0;
}

/*
 * __wt_txn_truncate_end --
 *	Finish truncating a range of a file.
 */
int __wt_txn_truncate_end(WT_SESSION_IMPL* session)
{
	F_CLR(session, WT_SESSION_LOGGING_INMEM);
	return 0;
}

/*
* __txn_printlog --
*	Print a log record in a human-readable(JSON) format.
*/
static int __txn_printlog(WT_SESSION_IMPL *session, WT_ITEM *rawrec, WT_LSN *lsnp, WT_LSN *next_lsnp, void *cookie, int firstrecord)
{
	FILE *out;
	WT_LOG_RECORD *logrec;
	WT_LSN ckpt_lsn;
	int compressed;
	uint64_t txnid;
	uint32_t fileid, rectype;
	int32_t start;
	const uint8_t *end, *p;
	const char *msg;

	WT_UNUSED(next_lsnp);
	out = cookie;

	p = LOG_SKIP_HEADER(rawrec->data);
	end = (const uint8_t *)rawrec->data + rawrec->size;
	logrec = (WT_LOG_RECORD *)rawrec->data;
	compressed = F_ISSET(logrec, WT_LOG_RECORD_COMPRESSED);

	/* First, peek at the log record type. */
	WT_RET(__wt_logrec_read(session, &p, end, &rectype));

	if (!firstrecord)
		fprintf(out, ",\n");

	if (fprintf(out, "  { \"lsn\" : [%" PRIu32 ",%" PRId64 "],\n",
		lsnp->file, lsnp->offset) < 0 ||
		fprintf(out, "    \"hdr_flags\" : \"%s\",\n",
		compressed ? "compressed" : "") < 0 ||
		fprintf(out, "    \"rec_len\" : %" PRIu32 ",\n", logrec->len) < 0 ||
		fprintf(out, "    \"mem_len\" : %" PRIu32 ",\n",
		compressed ? logrec->mem_len : logrec->len) < 0)
		return (errno);

	switch (rectype) {
	case WT_LOGREC_CHECKPOINT:
		WT_RET(__wt_struct_unpack(session, p, WT_PTRDIFF(end, p),
			WT_UNCHECKED_STRING(IQ), &ckpt_lsn.file, &ckpt_lsn.offset));
		if (fprintf(out, "    \"type\" : \"checkpoint\",\n") < 0 ||
			fprintf(
			out, "    \"ckpt_lsn\" : [%" PRIu32 ",%" PRId64 "]\n",
			ckpt_lsn.file, ckpt_lsn.offset) < 0)
			return (errno);
		break;

	case WT_LOGREC_COMMIT:
		WT_RET(__wt_vunpack_uint(&p, WT_PTRDIFF(end, p), &txnid));
		if (fprintf(out, "    \"type\" : \"commit\",\n") < 0 ||
			fprintf(out, "    \"txnid\" : %" PRIu64 ",\n", txnid) < 0)
			return (errno);
		WT_RET(__txn_commit_printlog(session, &p, end, out));
		break;

	case WT_LOGREC_FILE_SYNC:
		WT_RET(__wt_struct_unpack(session, p, WT_PTRDIFF(end, p),
			WT_UNCHECKED_STRING(Ii), &fileid, &start));
		if (fprintf(out, "    \"type\" : \"file_sync\",\n") < 0 ||
			fprintf(out, "    \"fileid\" : %" PRIu32 ",\n",
			fileid) < 0 ||
			fprintf(out, "    \"start\" : %" PRId32 "\n", start) < 0)
			return (errno);
		break;

	case WT_LOGREC_MESSAGE:
		WT_RET(__wt_struct_unpack(session, p, WT_PTRDIFF(end, p),
			WT_UNCHECKED_STRING(S), &msg));
		if (fprintf(out, "    \"type\" : \"message\",\n") < 0 ||
			fprintf(out, "    \"message\" : \"%s\"\n", msg) < 0)
			return (errno);
		break;
	}

	if (fprintf(out, "  }") < 0)
		return (errno);

	return (0);
}

/*
* __wt_txn_printlog --
*	Print the log in a human-readable(JSON) format.
*/
int __wt_txn_printlog(WT_SESSION *wt_session, FILE *out)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	if (fprintf(out, "[\n") < 0)
		return (errno);

	WT_RET(__wt_log_scan(session, NULL, WT_LOGSCAN_FIRST, __txn_printlog, out));
	if (fprintf(out, "\n]\n") < 0)
		return (errno);

	return (0);
}



