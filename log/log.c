/***************************************************************************
*redo日志管理实现
***************************************************************************/

#include "wt_internal.h"

static int __log_decompress(WT_SESSION_IMPL *, WT_ITEM *, WT_ITEM **);
static int __log_read_internal(WT_SESSION_IMPL *, WT_ITEM *, WT_LSN *, uint32_t);
static int __log_write_internal(WT_SESSION_IMPL *, WT_ITEM *, WT_LSN *, uint32_t);

/*数据压缩的起始位置，会跳过logrec的头*/
#define WT_LOG__COMPRESS_SKIP	(offsetof(WT_LOG_RECORD, record))

/*触发log建立一个checkpoint，向archive thread触发一个checkpoint信号*/
int __wt_log_ckpt(WT_SESSION_IMPL *session, WT_LSN *ckp_lsn)
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;

	conn = S2C(session);
	log = conn->log;
	log->ckpt_lsn = *ckp_lsn;

	if(conn->log_cond != NULL)
		WT_RET(__wt_cond_signal(session, conn->log_cond));

	return 0;
}

/*检查redo log是否需要进行重演，如果返回0表示重启后无需重演，1表示需要进行log重演*/
int __wt_log_needs_recovery(WT_SESSION_IMPL *session, WT_LSN *ckp_lsn, int *rec)
{
	WT_CONNECTION_IMPL *conn;
	WT_CURSOR *c;
	WT_DECL_RET;
	WT_LOG *log;

	conn = S2C(session);
	log = conn->log;

	*rec = 1;
	if(log == NULL)
		return 0;

	/*初始化cursor log,主要是安装*/
	WT_RET(__wt_curlog_open(session, "log:", NULL, &c));

	/*用file + offset作为KEY在进行查找*/
	c->set_key(c, ckp_lsn->file, ckp_lsn->offset, 0);
	ret = c->search(c);
	if(ret == 0){
		ret = c->next(c);
		if(ret == WT_NOTFOUND){
			*rec = 0;
			ret = 0;
		}
	}
	else if(ret == WT_NOTFOUND){
		ret = 0;
	}
	else{
		WT_ERR(ret);
	}

err: 
	WT_TRET(c->close(c));

	return ret;
}

/*在checkpoint后重新设置log为可以写状态,并将log_written的值置为0，
 *因为log中的数据都同步到page的磁盘上了*/
void __wt_log_written_reset(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL* conn;
	WT_LOG* log;

	conn = S2C(session);
	if(!FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED))
		return ;

	log = con->log;
	log->log_written = 0;

	return 0;
}

/*通过一个前缀匹配符，找出所有相关的日志文件名*/
static int __log_get_files(WT_SESSION_IMPL* session, const char* file_prefix, char*** filesp, u_int* countp)
{
	WT_CONNECTION_IMPL *conn;
	const char *log_path;

	*countp = 0;
	*filesp = NULL;

	conn = S2C(session);
	log_path = conn->log_path;
	if (log_path == NULL)
		log_path = "";

	return __wt_dirlist(session, log_path, file_prefix, WT_DIRLIST_INCLUDE, filesp, countp);
}

/*获得所有的日志文件名或者查找仅仅active的日志文件名(大于check lsn的日志文件)*/
int __wt_log_get_all_files(WT_SESSION_IMPL *session, char ***filesp, u_int *countp, uint32_t *maxid, int active_only)
{
	WT_DECL_RET;
	WT_LOG *log;
	char **files;
	uint32_t id, max;
	u_int count, i;

	id = 0;
	log = S2C(session)->log;

	*maxid = 0;
	/*此处会分配内存，如果异常，需要释放*/
	WT_RET(__log_get_files(session, WT_LOG_FILENAME, &filesp, &count));

	/*过滤掉所有小于checkpoint LSN的文件*/
	for(max = 0, i = 0; i < count;){
		WT_ERR(__wt_log_extract_lognum(session, files[i], &id));

		if(active_only && id < log->ckpt_lsn.file){
			__wt_free(session, files[i]);
			files[--count] = NULL;
		}
		else{
			if(id > max)
				max = id;
			i++;
		}
	}

	*maxid = max;
	*filesp = files;
	*countp = count;

	return ret;

err:
	__wt_log_files_free(session, files, count);
	return ret;
}

/*释放log文件名列表*/
void __wt_log_files_free(WT_SESSION_IMPL *session, char **files, u_int count)
{
	u_int i;
	for(i = 0; i < count; i++)
		__wt_free(session, files[i]);

	__wt_free(session, files);
}

/*从log文件名中解析一个log number(LSN中的file id)*/
int __wt_log_extract_lognum(WT_SESSION_IMPL *session, const char *name, uint32_t *id)
{
	const char *p;

	WT_UNUSED(session);
	
	if(id == NULL || name == NULL)
		return WT_ERROR;

	if ((p = strrchr(name, '.')) == NULL || sscanf(++p, "%" SCNu32, id) != 1)
		WT_RET_MSG(session, WT_ERROR, "Bad log file name '%s'", name);

	return 0;
}

static int __log_filename(WT_SESSION_IMPL *session, uint32_t id, const char *file_prefix, WT_ITEM *buf)
{
	const char *log_path;

	log_path = S2C(session)->log_path;

	if (log_path != NULL && log_path[0] != '\0')
		WT_RET(__wt_buf_fmt(session, buf, "%s/%s.%010" PRIu32, log_path, file_prefix, id));
	else
		WT_RET(__wt_buf_fmt(session, buf, "%s.%010" PRIu32, file_prefix, id));

	return (0);
}

/*对fh对应的log文件进行空间设定（size = log_file_max），一般是新建log文件时做的*/
static int __log_prealloc(WT_SESSION_IMPL* session, WT_FH* fh)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;

	conn = S2C(session);
	log = conn->log;
	ret = 0;

	if(fh->fallocate_available == WT_FALLOCATE_AVAILABLE ||(ret = __wt_fallocate(session, fh, LOG_FIRST_RECORD, conn->log_file_max)) == ENOTSUP){
		ret = __wt_ftruncate(session, fh, LOG_FIRST_RECORD + conn->log_file_max);
	}

	return ret;
}

/*判断lsn指向的log文件的剩余空间是否能存储下一个新的logrec数据*/
static int __log_size_fit(WT_SESSION_IMPL* session, WT_LSN* lsn, uint64_t recsize)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);
	return (lsn->offset + (wt_off_t)recsize < conn->log_file_max);
}

static int __log_acquire(WT_SESSION_IMPL* session, uint64_t recsize, WT_LOGSLOT* slot)
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;
	int created_log;

	conn = S2C(session);
	log = conn->log;
	created_log = 1;

	slot->slot_release_lsn = log->alloc_lsn;

	/*如果当前alloc_lsn对应的文件剩余空间无法存下recsize大小的数据，
	 *需要新建一个新的log file来保存，这有可能会涉及到递归调用
	 *__wt_log_newfile->__wt_log_allocfile->__log_file_header->__log_acquire*/
	if(!__log_size_fit(session, &log->alloc_lsn, recsize)){
		WT_RET(__wt_log_newfile(session, 0, &created_log));
		if(log->log_close_fh != NULL)
			F_SET(slot, SLOT_CLOSEFH); /*将对应的slot置为将关闭状态*/
	}

	/*checkpoint的时机是由log->written决定的，所以这里必须更新对应的统计,并触发checkpoint操作*/
	if(WT_CKPT_LOGSIZE(conn)){
		log->log_written += (wt_off_t)recsize;
		WT_RET(__wt_checkpoint_signal(session, log->log_written));
	}

	slot->slot_start_lsn = log->alloc_lsn;
	slot->slot_start_offset = log->alloc_lsn.offset;

	/*如果alloc_lsn对应的是LOG的第一条记录，并且创建了新的文件,表示当前alloc_lsn无法存下recsize的数据，需要进行文件空间扩充*/
	if(log->alloc_lsn.offset == LOG_FIRST_RECORD && created_log)
		WT_RET(__log_prealloc(session, log->log_fh));

	/*修改log的状态和slot的状态*/
	log->alloc_lsn.offset += (wt_off_t)recsize;
	slot->slot_end_lsn = log->alloc_lsn;
	slot->slot_error = 0;
	slot->slot_fh = log->log_fh;

	return 0;
}

/*对logrec的数据解压缩,这个函数会分配内存*/
static int __log_decompress(WT_SESSION_IMPL session*, WT_ITEM* in, WT_ITEM** out)
{
	WT_COMPRESSOR *compressor;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG_RECORD *logrec;
	size_t result_len, skip;
	uint32_t uncompressed_size;

	conn = S2C(session);
	logrec = (WT_LOG_RECORD *)in->mem;

	skip = WT_LOG__COMPRESS_SKIP;
	compressor = conn->log_compressor;
	if(compressor == NULL || compressor->decompress == NULL){
		WT_ERR_MSG(session, WT_ERROR, "log_read: Compressed record with no configured compressor");
	}
	
	/*进行out内存分配*/
	uncompressed_size = logrec->mem_len;
	WT_ERR(__wt_scr_alloc(session, 0, out));
	WT_ERR(__wt_buf_initsize(session, *out, uncompressed_size));
	memcpy((*out)->mem, in->mem, skip);

	/*进行解压缩*/
	WT_ERR(compressor->decompress(compressor, &session->iface,
		(uint8_t *)in->mem + skip, in->size - skip,
		(uint8_t *)(*out)->mem + skip,
		uncompressed_size - skip, &result_len));

	if(ret != 0 || result_len != uncompressed_size - WT_LOG__COMPRESS_SKIP)
		WT_ERR(WT_ERROR);

err:
	return ret;
}

/*log(record部分)数据写入*/
static int __log_fill(WT_SESSION_IMPL* session, WT_MYSLOT* myslot, int direct, WT_ITEM* record, WT_LSN* lsnp)
{
	WT_DECL_RET;
	WT_LOG_RECORD *logrec;

	logrec = (WT_LOG_RECORD *)record->mem;
	if(direct){
		/*对日志落直接落盘操作，需要等待IO完成*/
		WT_ERR(__wt_write(session, myslot->slot->slot_fh,
			myslot->offset + myslot->slot->slot_start_offset,
			(size_t)logrec->len, (void *)logrec));
	}
	else /*写到slot缓冲区中,效率更好，并发更好*/
		memcpy((char *)myslot->slot->slot_buf.mem + myslot->offset, logrec, logrec->len);

	WT_STAT_FAST_CONN_INCRV(session, log_bytes_written, logrec->len);
	if(lsnp != NULL){
		*lsnp = myslot->slot->slot_start_lsn;
		lsnp->offset += (wt_off_t)myslot->offset;
	}

err:
	if(ret != 0 && myslot->slot->slot_error == 0) /*slot错误保存*/
		myslot->slot->slot_error = ret;

	return ret;
}

/*构造日志头（logrec header）并写入日志文件(WT_FH)中*/
static int __log_file_header(WT_SESSION_IMPL* session, WT_FH* fh, WT_LSN* end_lsn, int prealloc)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	WT_LOG *log;
	WT_LOG_DESC *desc;
	WT_LOG_RECORD *logrec;
	WT_LOGSLOT tmp;
	WT_MYSLOT myslot;

	conn = S2C(session);
	log = conn->log;

	/*分配一个buf，这个BUF用于构造logrec*/
	WT_ASSERT(session, sizeof(WT_LOG_DESC) < log->allocsize);
	WT_RET(__wt_scr_alloc(session, log->allocsize, &buf));
	memset(buf->mem, 0, log->allocsize);

	/*设置头信息，主要是魔法校验字、WT版本信息和log->size,*/
	logrec = (WT_LOG_RECORD *)buf->mem;
	desc = (WT_LOG_DESC *)logrec->record;
	desc->log_magic = WT_LOG_MAGIC;
	desc->majorv = WT_LOG_MAJOR_VERSION;
	desc->minorv = WT_LOG_MINOR_VERSION;
	desc->log_size = (uint64_t)conn->log_file_max;

	/*计算logrec的checksum*/
	logrec->len = log->allocsize;
	logrec->checksum = 0;
	logrec->checksum = __wt_cksum(logrec, log->allocsize);

	WT_CLEAR(tmp);
	myslot.slot = &tmp;
	myslot.offset = 0;

	/*slot的fh已经做了空间扩充*/
	if(prealloc){
		WT_ASSERT(session, fh != NULL);
		tmp.slot_fh = fh;
	}
	else{ /*如果不是realloc过程的，那么就使用log当前对应的slot*/
		WT_ASSERT(session, fh == NULL);
		log->prep_missed++;
		WT_ERR(__log_acquire(session, logrec->len, &tmp));
	}

	/*直接写入log文件当中*/
	WT_ERR(__log_fill(session, &myslot, 1, buf, NULL));
	/*日志落盘*/
	WT_ERR(__wt_fsync(session, tmp.slot_fh));
	if(end_lsn != NULL)
		*end_lsn = tmp.slot_end_lsn;
err:
	__wt_scr_free(session, &buf);
	return ret;
}

/*打开一个日志文件*/
static int __log_openfile(WT_SESSION_IMPL* session, int ok_create, WT_FH** fh, const char* file_prefix, uint32_t id)
{
	WT_DECL_ITEM(path);
	WT_DECL_RET;

	/*构建一个path缓冲区*/
	WT_RET(__wt_scr_alloc(session, 0, &path));
	/*构建一个log文件名，并存入path中*/
	WT_ERR(__log_filename(session, id, file_prefix, path));
	WT_ERR(__wt_verbose(session, WT_VERB_LOG,"opening log %s", (const char *)path->data));
	/*创建并打开对应的log文件*/
	WT_ERR(__wt_open(session, path->data, ok_create, 0, WT_FILE_TYPE_LOG, fh));

err:
	__wt_scr_free(session, &path);
	return ret;
}

/*提取一个预分配log文件，并用to_num产生一个正式的log文件名顶替这个预分配文件*/
static int __log_alloc_prealloc(WT_SESSION_IMPL* session, uint32_t to_num)
{
	WT_DECL_ITEM(from_path);
	WT_DECL_ITEM(to_path);
	WT_DECL_RET;
	uint32_t	from_num;
	u_int		logcount;
	char**		logfiles;


	logfiles = NULL;

	/*获取一个与分配的log文件列表*/
	WT_ERR(__log_get_files(session, WT_LOG_PREPNAME, &logfiles, &logcount));
	if(logcount == 0)
		return WT_NOTFOUND;

	/*获得第一个预分配文件的lsn->file序号*/
	WT_ERR(__wt_log_extract_lognum(session, logfiles[0], &from_num));

	WT_ERR(__wt_scr_alloc(session, 0, &from_path));
	WT_ERR(__wt_scr_alloc(session, 0, &to_path));

	/*构建一个预分配文件路劲*/
	WT_ERR(__log_filename(session, from_num, WT_LOG_PREPNAME, from_path));
	/*构建一个顶替预分配LOG文件的正式log文件路径*/
	WT_ERR(__log_filename(session, to_num, WT_LOG_FILENAME, to_path));
	WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_alloc_prealloc: rename log %s to %s", (char *)from_path->data, (char *)to_path->data));

	WT_STAT_FAST_CONN_INCR(session, log_prealloc_used);
	/*文件正式更名生效*/
	WT_ERR(__wt_rename(session, (const char*)(from_path->data), to_path->data));

err:
	__wt_scr_free(session, &from_path);
	__wt_scr_free(session, &to_path);
	if (logfiles != NULL) /*释放__log_get_files的文件名列表*/
		__wt_log_files_free(session, logfiles, logcount);

	return ret;
}

/*对log文件进行truncate操作，使它和实际有效的log数据匹配*/
static int __log_truncate(WT_SESSION_IMPL* session, WT_LSN* lsn, const char* file_prefix, uint32_t this_log)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *log_fh, *tmp_fh;
	WT_LOG *log;
	uint32_t lognum;
	u_int i, logcount;
	char **logfiles;

	conn = S2C(session);
	log = conn->log;
	log_fh = NULL;
	logcount = 0;
	logfiles = NULL;

	/*长度调整LSN对应日志文件的长度到他的有效数据长度位置上*/
	WT_ERR(__log_openfile(session, 0, &log_fh, file_prefix, lsn->file));
	WT_ERR(__wt_ftruncate(session, log_fh, lsn->offset));
	tmp_fh = log_fh;
	log_fh = NULL;
	WT_ERR(__wt_fsync(session, tmp_fh));
	WT_ERR(__wt_close(session, &tmp_fh));

	if(this_log)
		goto err;

	/*获得正式log文件名列表*/
	WT_ERR(__log_get_files(session, WT_LOG_FILENAME, &logfiles, &logcount));

	/*把所有小于log trunc lsn的文件全部清空*/
	for(i = 0; i < logcount; i++){
		WT_ERR(__wt_log_extract_lognum(session, logfiles[i], &lognum));
		if(lognum > lsn->file && lognum < log->trunc_lsn.file){
			WT_ERR(__log_openfile(session, 0, &log_fh, file_prefix, lognum));
			/*只保留log file的头信息*/
			WT_ERR(__wt_ftruncate(session, log_fh, LOG_FIRST_RECORD));

			tmp_fh = log_fh;
			log_fh = NULL;
			WT_ERR(__wt_fsync(session, tmp_fh));
			WT_ERR(__wt_close(session, &tmp_fh));
		}
	}

err:
	WT_TRET(__wt_close(session, &log_fh));
	if(logfiles != NULL)
		__wt_log_files_free(session, logfiles, logcount);

	return ret;
}

/*分配一个指定类型的日志文件，在分配的过程中，先会通过临时日志文件将日志头写入到文件中，并进行改名,
 *采用临时文件应该是防止多线程操作的冲突*/
int __wt_log_allocfile(WT_SESSION_IMPL *session, uint32_t lognum, const char *dest, int prealloc)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_ITEM(from_path);
	WT_DECL_ITEM(to_path);
	WT_DECL_RET;
	WT_FH *log_fh, *tmp_fh;
	WT_LOG *log;

	conn = S2C(session);
	log = conn->log;
	log_fh = NULL;

	WT_RET(__wt_scr_alloc(session, 0, &from_path));
	WT_ERR(__wt_scr_alloc(session, 0, &to_path));
	WT_ERR(__log_filename(session, lognum, WT_LOG_TMPNAME, from_path));
	WT_ERR(__log_filename(session, lognum, dest, to_path));

	/*清空临时文件，并将一个初始化log header写入到临时文件中*/
	WT_ERR(__log_openfile(session, 1, &log_fh, WT_LOG_TMPNAME, lognum));
	WT_ERR(__log_file_header(session, log_fh, NULL, 1));
	WT_ERR(__wt_ftruncate(session, log_fh, LOG_FIRST_RECORD));
	if (prealloc)
		WT_ERR(__log_prealloc(session, log_fh));

	tmp_fh = log_fh;
	log_fh = NULL;

	WT_ERR(__wt_fsync(session, tmp_fh));
	WT_ERR(__wt_close(session, &tmp_fh));
	WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_prealloc: rename %s to %s", (char *)from_path->data, (char *)to_path->data));

	WT_ERR(__wt_rename(session, from_path->data, to_path->data));

err:
	__wt_scr_free(session, &from_path);
	__wt_scr_free(session, &to_path);
	WT_TRET(__wt_close(session, &log_fh));

	return ret;
}

/*根据log number移除一个对应的文件*/
int __wt_log_remove(WT_SESSION_IMPL *session, const char *file_prefix, uint32_t lognum)
{
	WT_DECL_ITEM(path);
	WT_DECL_RET;

	WT_RET(__wt_scr_alloc(session, 0, &path));
	WT_ERR(__log_filename(session, lognum, file_prefix, path));
	WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_remove: remove log %s", (char *)path->data));
	WT_ERR(__wt_remove(session, path->data));

err:
	__wt_scr_free(session, &path);
	return ret;
}

/*为session打开一个日志文件， 目的是找出已经存在日志文件的最大LSN,
 *并将其作为新日志文件的文件名，如果不存在旧日志文件，
 * 用__wt_log_newfile为其创建一个新的日志*/
int __wt_log_open(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	uint32_t firstlog, lastlog, lognum;
	u_int i, logcount;
	char **logfiles;

	conn = S2C(session);
	log = conn->log;
	logfiles = NULL;
	logcount = 0;
	lastlog = 0;
	firstlog = UINT32_MAX;

	/*打开session connection对应的日志文件目录索引文件*/
	if (log->log_dir_fh == NULL) {
		WT_RET(__wt_verbose(session, WT_VERB_LOG, "log_open: open fh to directory %s", conn->log_path));
		WT_RET(__wt_open(session, conn->log_path, 0, 0, WT_FILE_TYPE_DIRECTORY, &log->log_dir_fh));
	}

	/*在session对应的日志文件目录下将所有已经存在临时日志文件名存入logfiles中*/
	WT_ERR(__log_get_files(session, WT_LOG_TMPNAME, &logfiles, &logcount));
	/*删除所有的临时日志文件,因为要打开一个更大LSN对应的文件，就必须将现在关闭现在正在使用的LSN日志文件，那么它对应的
	 *临时文件就会被关闭*/
	for(i = 0; i < logcount; i++){
		WT_ERR(__wt_log_extract_lognum(session, logfiles[i], &lognum));
		WT_ERR(__wt_log_remove(session, WT_LOG_TMPNAME, lognum));
	}

	__wt_log_files_free(session, logfiles, logcount);

	logfiles = NULL;
	logcount = 0;

	/*获得所有预分配日志文件的文件名,并通过删除所有预分配文件*/
	WT_ERR(__log_get_files(session, WT_LOG_PREPNAME, &logfiles, &logcount));

	for (i = 0; i < logcount; i++) {
		WT_ERR(__wt_log_extract_lognum(session, logfiles[i], &lognum));
		WT_ERR(__wt_log_remove(session, WT_LOG_PREPNAME, lognum));
	}
	__wt_log_files_free(session, logfiles, logcount);
	logfiles = NULL;

	/*获取正式的日志文件名，并通过文件名确定lastlog和firstlog*/
	WT_ERR(__log_get_files(session, WT_LOG_FILENAME, &logfiles, &logcount));
	for (i = 0; i < logcount; i++) {
		WT_ERR(__wt_log_extract_lognum(session, logfiles[i], &lognum));
		lastlog = WT_MAX(lastlog, lognum);
		firstlog = WT_MIN(firstlog, lognum);
	}

	/*用lastlog作为fileid*/
	log->fileid = lastlog;
	WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_open: first log %d last log %d", firstlog, lastlog));
	/*没有最小的lsn，将其设置为（1，0)*/
	if (firstlog == UINT32_MAX) {
		WT_ASSERT(session, logcount == 0);
		WT_INIT_LSN(&log->first_lsn);
	} 
	else {
		log->first_lsn.file = firstlog;
		log->first_lsn.offset = 0;
	}
	/*用__wt_log_newfile创建一个新的日志文件，并将日志头信息写入文件中*/
	WT_ERR(__wt_log_newfile(session, 1, NULL));
	/*发现有先前的日志文件，保存log的状态*/
	if (logcount > 0) {
		log->trunc_lsn = log->alloc_lsn;
		FLD_SET(conn->log_flags, WT_CONN_LOG_EXISTED);
	}

err:
	if(logfiles != NULL)
		__wt_log_files_free(session, logfiles, logcount);

	return ret;
}

/*关闭session对应的日志文件*/
int __wt_log_close(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;

	conn = S2C(session);
	log = conn->log;

	/*关闭日志文件*/
	if (log->log_close_fh != NULL && log->log_close_fh != log->log_fh) {
		WT_RET(__wt_verbose(session, WT_VERB_LOG, "closing old log %s", log->log_close_fh->name));
		WT_RET(__wt_fsync(session, log->log_close_fh));
		WT_RET(__wt_close(session, &log->log_close_fh));
	}

	if (log->log_fh != NULL) {
		WT_RET(__wt_verbose(session, WT_VERB_LOG, "closing log %s", log->log_fh->name));
		WT_RET(__wt_fsync(session, log->log_fh));
		WT_RET(__wt_close(session, &log->log_fh));
		log->log_fh = NULL;
	}

	/*fsync日志目录索引文件*/
	if (log->log_dir_fh != NULL) {
		WT_RET(__wt_verbose(session, WT_VERB_LOG, "closing log directory %s", log->log_dir_fh->name));
		WT_RET(__wt_directory_sync_fh(session, log->log_dir_fh));
		WT_RET(__wt_close(session, &log->log_dir_fh));
		log->log_dir_fh = NULL;
	}

	return 0;
}

/*确定一个日志文件有效数据的空间大小*/
static int __log_filesize(WT_SESSION_IMPL* session, WT_FH* fh, wt_off_t* eof)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	wt_off_t log_size, off, off1;
	uint32_t allocsize, bufsz;
	char *buf, *zerobuf;

	conn = S2C(session);
	log = conn->log;
	if(eof == NULL)
		return 0;

	*eof = 0;
	WT_RET(__wt_filesize(session, fh, &log_size));
	if(log == NULL)
		allocsize = LOG_ALIGN;
	else
		allocsize = log->allocsize;

	buf = zerobuf = NULL;
	if (allocsize < WT_MEGABYTE && log_size > WT_MEGABYTE) /*最大以1M方式对齐*/
		bufsz = WT_MEGABYTE;
	else
		bufsz = allocsize;

	/*开辟两个匹配的缓冲区*/
	WT_RET(__wt_calloc_def(session, bufsz, &buf));
	WT_ERR(__wt_calloc_def(session, bufsz, &zerobuf));

	/*从文件末尾开始向前，每次读取1个对齐长度的数据，和zerobuf比较，直到有不为0的数据为止，
	 *不为0的数据表示到了log的末尾,日志文件在__log_prealloc是物理大小为log->log_file_max,数据为0的*/
	for (off = log_size - (wt_off_t)bufsz; off >= 0; off -= (wt_off_t)bufsz) {
		WT_ERR(__wt_read(session, fh, off, bufsz, buf));
		if (memcmp(buf, zerobuf, bufsz) != 0)
			break;
	}
	/*已经到了文件开始位置*/
	if(off < 0)
		off = 0;

	/*进行块内位置的确认，例如，在128 ~ 256这个buf段中有数据不匹配，找到最后一个不为0的偏移*/
	for (off1 = bufsz - allocsize; off1 > 0; off1 -= (wt_off_t)allocsize){
		if (memcmp(buf + off1, zerobuf, sizeof(uint32_t)) != 0)
			break;
	}

	off = off + off1;
	/*将eof设置到最后一个不为0的数据偏移上*/
	*eof = off + (wt_off_t)allocsize;

err:
	if (buf != NULL)
		__wt_free(session, buf);

	if(zerobuf != NULL)
		__wt_free(session, zerobuf);

	return ret;
}

/*release一个log对应的slot， 在这个过程先会将slot buffer中的数据写入到对应文件的page cache中
 *然后对文件进行sync操作，进行日志落盘*/
static int __log_release(WT_SESSION_IMPL* session, WT_LOGSLOT* slot, int* freep)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	WT_LSN sync_lsn;
	size_t write_size;
	int locked, yield_count;
	WT_DECL_SPINLOCK_ID(id);	

	conn = S2C(session);
	log = conn->log;
	locked = 0;
	yield_count = 0;
	*freep = 1;

	/*将slot的缓冲区中的log record数据写入到对应文件中*/
	if(F_ISSET(slot, SLOT_BUFFERED)){
		write_size = (size_t)(slot->slot_end_lsn.offset - slot->slot_start_offset);
		WT_ERR(__wt_write(session, slot->slot_fh, slot->slot_start_offset, write_size, slot->slot_buf.mem));
	}

	/*slot是一个dummy slot，不是buffer缓冲数据的slot,这个过程无需回收slot到slot pool中*/
	if(F_ISSET(slot, SLOT_BUFFERED) && !F_ISSET(slot, SLOT_SYNC | SLOT_SYNC_DIR)){
		*freep = 0;
		slot->slot_state = WT_LOG_SLOT_WRITTEN;

		WT_ERR(__wt_cond_signal(session, conn->log_wrlsn_cond));

		goto done;
	}

	/*修改统计信息*/
	WT_STAT_FAST_CONN_INCR(session, log_release_write_lsn);
	/*判断write lsn是否达到release lsn的位置，如果达到，进行write_lsn的更新*/
	while (LOG_CMP(&log->write_lsn, &slot->slot_release_lsn) != 0) {
		if (++yield_count < 1000)
			__wt_yield();
		else
			WT_ERR(__wt_cond_wait(session, log->log_write_cond, 200));
	}

	log->write_lsn = slot->slot_end_lsn;
	/*log write lsn做了更新，让等log_write_cond的线程重新进行write lsn判断*/
	WT_ERR(__wt_cond_signal(session, log->log_write_cond));

	/*如果slot处于关闭文件的表示，通知对应等待线程进行文件关闭*/
	if (F_ISSET(slot, SLOT_CLOSEFH))
		WT_ERR(__wt_cond_signal(session, conn->log_close_cond));

	while (F_ISSET(slot, SLOT_SYNC | SLOT_SYNC_DIR)){
		/*如果正在sync的file小于slot->slot_end_lsn.file，表示slot对应的日志文件还没有完成sync操作(不能刷end_lsn对应的文件)，必须进行等待*/
		if (log->sync_lsn.file < slot->slot_end_lsn.file || __wt_spin_trylock(session, &log->log_sync_lock, &id) != 0) {
				WT_ERR(__wt_cond_wait(session, log->log_sync_cond, 10000));
				continue;
		}
		/*到这个位置，本线程获得了log_sync_lock,可以进行sync操作*/
		locked = 1;

		sync_lsn = slot->slot_end_lsn;

		/*先刷新log dir path索引文件*/
		if (F_ISSET(slot, SLOT_SYNC_DIR) &&(log->sync_dir_lsn.file < sync_lsn.file)) {
			WT_ASSERT(session, log->log_dir_fh != NULL);
			WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_release: sync directory %s", log->log_dir_fh->name));
			WT_ERR(__wt_directory_sync_fh(session, log->log_dir_fh));
			log->sync_dir_lsn = sync_lsn;
			WT_STAT_FAST_CONN_INCR(session, log_sync_dir);
		}

		/*在刷新日志文件*/
		if (F_ISSET(slot, SLOT_SYNC) && LOG_CMP(&log->sync_lsn, &slot->slot_end_lsn) < 0) {
			WT_ERR(__wt_verbose(session, WT_VERB_LOG, "log_release: sync log %s", log->log_fh->name));
			WT_STAT_FAST_CONN_INCR(session, log_sync);
			WT_ERR(__wt_fsync(session, log->log_fh));
			/*更改sync_lsn，通知其他线程log->sync_lsn产生了改变，重新进行比对判断*/
			log->sync_lsn = sync_lsn;
			WT_ERR(__wt_cond_signal(session, log->log_sync_cond));
		}

		/*清除掉slot的SYNC标识*/
		F_CLR(slot, SLOT_SYNC | SLOT_SYNC_DIR);
		/*释放sync spin lock*/
		locked = 0;
		__wt_spin_unlock(session, &log->log_sync_lock);
		/*防止其他线程重新把SLOT_SYNC设置回来*/
		break;
	}
err:
	if(locked)
		__wt_spin_unlock(session, &log->log_sync_lock);

	/*如果err,设置slot的error值*/
	if (ret != 0 && slot->slot_error == 0)
		slot->slot_error = ret;

done:
	return ret;
}

/*为日志session建立一个新的日志文件，并将日志文件头信息写入到日志文件中*/
int __wt_log_newfile(WT_SESSION_IMPL *session, int conn_create, int *created)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	WT_LSN end_lsn;
	int create_log;

	conn = S2C(session);
	log = conn->log;
	create_log = 1;
	
	/*等待__log_close_server线程主体函数对log_close_fh的关闭完成，因为在新建新的日志文件，正在使用的
	 *日志文件可能正在被写，所以一定要等待其写完成*/
	while(log->log_close_fh != NULL){
		WT_STAT_FAST_CONN_INCR(session, log_close_yields);
		__wt_yield();
	}
	log->log_close_fh = log->log_fh;
	log->fileid++;

	ret = 0;
	/*预先分配一个日志文件，这样做的目的可能是加快文件的建立*/
	if(conn->log_prealloc){
		ret = __log_alloc_prealloc(session, log->fileid);
		/*如果返回的是ret = 0, 表示log->fileid对应的文件已经被其他的线程创建了*/
		if (ret == 0)
			create_log = 0;

		/*创建错误*/
		if (ret != 0 && ret != WT_NOTFOUND)
			return ret;
	}

	/*没有预分配文件，进行重新创建，并将日志文件头信息写入到新建立的日志文件中*/
	if (create_log && (ret = __wt_log_allocfile(session, log->fileid, WT_LOG_FILENAME, 0)) != 0)
		return ret;

	/*打开新创建的日志文件*/
	WT_RET(__log_openfile(session, 0, &log->log_fh, WT_LOG_FILENAME, log->fileid));

	/*当前日志alloc_lsn的位置做更新*/
	log->alloc_lsn.file = log->fileid;
	log->alloc_lsn.offset = LOG_FIRST_RECORD;
	end_lsn = log->alloc_lsn;

	if (conn_create) {
		/*对新建日志数据落盘*/
		WT_RET(__wt_fsync(session, log->log_fh));
		log->sync_lsn = end_lsn;
		log->write_lsn = end_lsn;
	}

	if (created != NULL)
		*created = create_log;

	return 0;
}

/*进行日志读取，一般在redo log重演时使用*/
int __wt_log_read(WT_SESSION_IMPL *session, WT_ITEM* record, WT_LSN* lsnp, uint32_t flags)
{
	WT_DECL_ITEM(uncitem);
	WT_DECL_RET;
	WT_LOG_RECORD *logrec;
	WT_ITEM swap;

	WT_ERR(__log_read_internal(session, record, lsnp, flags));
	logrec = (WT_LOG_RECORD *)record->mem;

	/*如果日志记录是经过了压缩的，进行解压缩,压缩日志个人感觉会影响日志写的效率！在wiredtiger占用的CPU过高时，可以考虑将日志压缩取消*/
	if (F_ISSET(logrec, WT_LOG_RECORD_COMPRESSED)) {
		WT_ERR(__log_decompress(session, record, &uncitem));

		swap = *record;
		*record = *uncitem;
		*uncitem = swap;
	}
err:
	__wt_scr_free(session, &uncitem);
	return ret;
}

/*从日志文件中读取lsnp指定偏移出的一条日志log record*/
static int __log_read_internal(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp, uint32_t flags)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *log_fh;
	WT_LOG *log;
	WT_LOG_RECORD *logrec;
	uint32_t cksum, rdup_len, reclen;

	WT_UNUSED(flags);

	if (lsnp == NULL || record == NULL)
		return 0;

	conn = S2C(session);
	log = conn->log;

	/*lsnp->offset一定是log->allocsize方式对齐的*/
	if (lsnp->offset % log->allocsize != 0 || lsnp->file > log->fileid)
		return WT_NOTFOUND;

	/*打开日志文件*/
	WT_RET(__log_openfile(session, 0, &log_fh, WT_LOG_FILENAME, lsnp->file));

	/*读取log rec,logrec最小单位是1个log->allocsize*/
	WT_ERR(__wt_buf_init(session, record, log->allocsize));
	WT_ERR(__wt_read(session, log_fh, lsnp->offset, (size_t)log->allocsize, record->mem));

	/*获得log rec的长度*/
	reclen = *(uint32_t *)record->mem;
	if (reclen == 0) {
		ret = WT_NOTFOUND;
		goto err;
	}

	/*读取logrec剩余部分*/
	if (reclen > log->allocsize) {
		rdup_len = __wt_rduppo2(reclen, log->allocsize);
		WT_ERR(__wt_buf_grow(session, record, rdup_len));
		WT_ERR(__wt_read(session, log_fh, lsnp->offset, (size_t)rdup_len, record->mem));
	}

	logrec = (WT_LOG_RECORD *)record->mem;
	cksum = logrec->checksum;
	logrec->checksum = 0;
	/*进行check sum检查*/
	logrec->checksum = __wt_cksum(logrec, logrec->len);
	if (logrec->checksum != cksum)
		WT_ERR_MSG(session, WT_ERROR, "log_read: Bad checksum");

	record->size = logrec->len;
	WT_STAT_FAST_CONN_INCR(session, log_reads);

err:
	WT_TRET(__wt_close(session, &log_fh));
	return ret;
}





