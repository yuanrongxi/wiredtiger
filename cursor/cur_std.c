/**************************************************************************
*对cursor游标操作的一些函数封装
**************************************************************************/
#include "wt_internal.h"

/*一个预留空函数，用户填充不必要的函数指针*/
int __wt_cursor_notsup(WT_CURSOR* cursor)
{
	WT_UNUSED(cursor);

	return ENOTSUP;
}

int __wt_cursor_noop(WT_CURSOR *cursor)
{
	WT_UNUSED(cursor);

	return 0;
}

/*注销cursor的回调函数*/
void __wt_cursor_set_notsup(WT_CURSOR* cursor)
{
	cursor->compare = (int (*)(WT_CURSOR *, WT_CURSOR *, int *))__wt_cursor_notsup;
	cursor->next = __wt_cursor_notsup;
	cursor->prev = __wt_cursor_notsup;
	cursor->reset = __wt_cursor_noop;
	cursor->search = __wt_cursor_notsup;
	cursor->search_near = (int (*)(WT_CURSOR *, int *))__wt_cursor_notsup;
	cursor->insert = __wt_cursor_notsup;
	cursor->update = __wt_cursor_notsup;
	cursor->remove = __wt_cursor_notsup;
}

/*kv的错误消息输出*/
int __wt_cursor_kv_not_set(WT_CURSOR *cursor, int key)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL*)cursor->session;

	WT_RET_MSG(session, cursor->saved_err == 0 ? EINVAL : cursor->saved_err, "requires %s be set", key ? "key" : "value");
}

/*cursor get_key的实现,,相当于把cursor的key格式化到字符串中*/
int __wt_cursor_get_key(WT_CURSOR *cursor, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, cursor);
	ret = __wt_cursor_get_keyv(cursor, cursor->flags, ap);
	va_end(ap);

	return ret;
}

/*cursor set_key的实现,设置cursor key的值*/
void __wt_cursor_set_key(WT_CURSOR *cursor, ...)
{
	va_list ap;

	va_start(ap, cursor);
	__wt_cursor_set_keyv(cursor, cursor->flags, ap);
	va_end(ap);
}

/*读取cursor raw key*/
int __wt_cursor_get_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
{
	WT_DECL_RET;
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);
	ret = cursor->get_key(cursor, key);
	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);

	return ret;
}

/*设置cursor row key*/
void __wt_cursor_set_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
{
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);

	cursor->set_key(cursor, key);

	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);
}

/*获取cursor的value值*/
int __wt_cursor_get_raw_value(WT_CURSOR *cursor, WT_ITEM *value)
{
	WT_DECL_RET;
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);
	ret = cursor->get_value(cursor, value);
	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);

	return ret;
}

/*设置cursor的值*/
void __wt_cursor_set_raw_value(WT_CURSOR *cursor, WT_ITEM *value)
{
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);

	cursor->set_value(cursor, value);

	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);
}

/*格式化读取cursor的key值*/
int __wt_cursor_get_keyv(WT_CURSOR *cursor, uint32_t flags, va_list ap)
{
	WT_DECL_RET;
	WT_ITEM *key;
	WT_SESSION_IMPL *session;
	size_t size;
	const char *fmt;

	CURSOR_API_CALL(cursor, session, get_key, NULL);

	if (!F_ISSET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_KEY_INT)) /*不是数字和字符串，无法格式化读取,进行错误*/
		WT_ERR(__wt_cursor_kv_not_set(cursor, 1));

	if (WT_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(WT_CURSTD_RAW)) {
			key = va_arg(ap, WT_ITEM *);
			key->data = cursor->raw_recno_buf;
			WT_ERR(__wt_struct_size(session, &size, "q", cursor->recno));

			key->size = size;
			ret = __wt_struct_pack(session, cursor->raw_recno_buf, sizeof(cursor->raw_recno_buf), "q", cursor->recno);
		} 
		else{
			*va_arg(ap, uint64_t *) = cursor->recno;
		}
	}
	else{
		fmt = cursor->key_format;
		if (LF_ISSET(WT_CURSOR_RAW_OK) || WT_STREQ(fmt, "u")) {
			key = va_arg(ap, WT_ITEM *);
			key->data = cursor->key.data;
			key->size = cursor->key.size;
		} 
		else if (WT_STREQ(fmt, "S"))
			*va_arg(ap, const char **) = cursor->key.data;
		else
			ret = __wt_struct_unpackv(session, cursor->key.data, cursor->key.size, fmt, ap);
	}

err:
	API_END_RET(session, ret);
}

void __wt_cursor_set_keyv(WT_CURSOR *cursor, uint32_t flags, va_list ap)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_ITEM *buf, *item, tmp;
	size_t sz;
	va_list ap_copy;
	const char *fmt, *str;

	buf = &cursor->key;
	tmp.mem = NULL;

	CURSOR_API_CALL(cursor, session, set_key, NULL);

	if (F_ISSET(cursor, WT_CURSTD_KEY_SET) && WT_DATA_IN_ITEM(buf)) {
		tmp = *buf;
		buf->mem = NULL;
		buf->memsize = 0;
	}

	/*清除设置标识，表明正在设置这个KEY的值*/
	F_CLR(cursor, WT_CURSTD_KEY_SET);

	if (WT_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(WT_CURSTD_RAW)) {
			item = va_arg(ap, WT_ITEM *);
			WT_ERR(__wt_struct_unpack(session, item->data, item->size, "q", &cursor->recno));
		} 
		else
			cursor->recno = va_arg(ap, uint64_t);
		if (cursor->recno == 0)
			WT_ERR_MSG(session, EINVAL,
			"Record numbers must be greater than zero");
		buf->data = &cursor->recno;
		sz = sizeof(cursor->recno);
	}
	else{
		/* Fast path some common cases and special case WT_ITEMs. */
		fmt = cursor->key_format;
		if (LF_ISSET(WT_CURSOR_RAW_OK | WT_CURSTD_DUMP_JSON) || WT_STREQ(fmt, "u")) {
			item = va_arg(ap, WT_ITEM *);
			sz = item->size;
			buf->data = item->data;
		} 
		else if (WT_STREQ(fmt, "S")) {
			str = va_arg(ap, const char *);
			sz = strlen(str) + 1;
			buf->data = (void *)str;
		} 
		else {
			va_copy(ap_copy, ap);
			ret = __wt_struct_sizev(session, &sz, cursor->key_format, ap_copy);
			va_end(ap_copy);
			WT_ERR(ret);

			WT_ERR(__wt_buf_initsize(session, buf, sz));
			WT_ERR(__wt_struct_packv(session, buf->mem, sz, cursor->key_format, ap));
		}
	}

	if(sz == 0){
		WT_ERR_MSG(session, EINVAL, "Empty keys not permitted");
	}
	else if((uint32_t)sz != sz){ /*sz是个64位的整形数*/
		WT_ERR_MSG(session, EINVAL, "Key size (%" PRIu64 ") out of range", (uint64_t)sz);
	}

	cursor->saved_err = 0;
	buf->size = sz;
	F_SET(cursor, WT_CURSTD_KEY_EXT);

	if(0){
err:
		cursor->saved_err = ret;
	}

	/*释放wt_item的内存空间*/
	if (tmp.mem != NULL) { 
		if (buf->mem == NULL) {
			buf->mem = tmp.mem;
			buf->memsize = tmp.memsize;
		} 
		else{ /*buf另外分配了新的缓冲区，需要对原来的缓冲区释放*/
			__wt_free(session, tmp.mem);
		}
	}

	API_END(session, ret);
}

/*从cursor中取出value的值，按格式化串取出*/
int __wt_cursor_get_value(WT_CURSOR *cursor, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, cursor);
	ret = __wt_cursor_get_valuev(cursor, ap);
	va_end(ap);
	
	return ret;
}

int __wt_cursor_get_valuev(WT_CURSOR *cursor, va_list ap)
{
	WT_DECL_RET;
	WT_ITEM *value;
	WT_SESSION_IMPL *session;
	const char *fmt;

	CURSOR_API_CALL(cursor, session, get_value, NULL);

	if (!F_ISSET(cursor, WT_CURSTD_VALUE_EXT | WT_CURSTD_VALUE_INT))
		WT_ERR(__wt_cursor_kv_not_set(cursor, 0));

	/* Fast path some common cases. */
	fmt = cursor->value_format;
	if (F_ISSET(cursor, WT_CURSOR_RAW_OK) || WT_STREQ(fmt, "u")) {
		value = va_arg(ap, WT_ITEM *);
		value->data = cursor->value.data;
		value->size = cursor->value.size;
	} 
	else if (WT_STREQ(fmt, "S"))
		*va_arg(ap, const char **) = cursor->value.data;
	else if (WT_STREQ(fmt, "t") ||(isdigit(fmt[0]) && WT_STREQ(fmt + 1, "t")))
		*va_arg(ap, uint8_t *) = *(uint8_t *)cursor->value.data;
	else
		ret = __wt_struct_unpackv(session, cursor->value.data, cursor->value.size, fmt, ap);

err:	
	API_END_RET(session, ret);
}

/*设置cursor的value值，按格式化输入设置*/
void __wt_cursor_set_value(WT_CURSOR *cursor, ...)
{
	va_list ap;

	va_start(ap, cursor);
	__wt_cursor_set_valuev(cursor, ap);
	va_end(ap);
}

void __wt_cursor_set_valuev(WT_CURSOR *cursor, va_list ap)
{
	WT_DECL_RET;
	WT_ITEM *buf, *item, tmp;
	WT_SESSION_IMPL *session;
	const char *fmt, *str;
	va_list ap_copy;
	size_t sz;

	buf = &cursor->value;
	tmp.mem = NULL;

	CURSOR_API_CALL(cursor, session, set_value, NULL);
	if (F_ISSET(cursor, WT_CURSTD_VALUE_SET) && WT_DATA_IN_ITEM(buf)) {
		tmp = *buf;
		buf->mem = NULL;
		buf->memsize = 0;
	}

	F_CLR(cursor, WT_CURSTD_VALUE_SET);

	/* Fast path some common cases. */
	fmt = cursor->value_format;
	if (F_ISSET(cursor, WT_CURSOR_RAW_OK | WT_CURSTD_DUMP_JSON) || WT_STREQ(fmt, "u")) {
		item = va_arg(ap, WT_ITEM *);
		sz = item->size;
		buf->data = item->data;
	} 
	else if (WT_STREQ(fmt, "S")) {
		str = va_arg(ap, const char *);
		sz = strlen(str) + 1;
		buf->data = str;
	} 
	else if (WT_STREQ(fmt, "t") || (isdigit(fmt[0]) && WT_STREQ(fmt + 1, "t"))) {
		sz = 1;
		WT_ERR(__wt_buf_initsize(session, buf, sz));
		*(uint8_t *)buf->mem = (uint8_t)va_arg(ap, int);
	} 
	else {
		va_copy(ap_copy, ap);
		ret = __wt_struct_sizev(session, &sz, cursor->value_format, ap_copy);
		va_end(ap_copy);

		WT_ERR(ret);
		WT_ERR(__wt_buf_initsize(session, buf, sz));
		WT_ERR(__wt_struct_packv(session, buf->mem, sz, cursor->value_format, ap));
	}
	F_SET(cursor, WT_CURSTD_VALUE_EXT);
	buf->size = sz;

	if (0) {
err:		
		cursor->saved_err = ret;
	}

	/*无效内存的释放，有可能在设置过程，新分配了缓冲区替换了旧的value缓冲区，那么需要释放原来旧的缓冲区*/
	if (tmp.mem != NULL) {
		if (buf->mem == NULL) {
			buf->mem = tmp.mem;
			buf->memsize = tmp.memsize;
		} 
		else
			__wt_free(session, tmp.mem);
	}

	API_END(session, ret);
}

/*关闭cursor函数*/
int __wt_cursor_close(WT_CURSOR *cursor)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cursor->session;
	/*释放存储key/value值的缓冲区*/
	__wt_buf_free(session, &cursor->key);
	__wt_buf_free(session, &cursor->value);

	if (F_ISSET(cursor, WT_CURSTD_OPEN)) {
		TAILQ_REMOVE(&session->cursors, cursor, q);

		WT_STAT_FAST_DATA_DECR(session, session_cursor_open);
		WT_STAT_FAST_CONN_ATOMIC_DECR(session, session_cursor_open);
	}

	__wt_free(session, cursor->internal_uri);
	__wt_free(session, cursor->uri);
	__wt_overwrite_and_free(session, cursor);

	return ret;
}

/*判断两个cursor是否相等，返回值为equalp，equalp = 1表示相等,0表示不相等*/
int __wt_cursor_equals(WT_CURSOR* cursor, WT_CURSOR* other, int* equalp)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	int cmp;

	session = (WT_SESSION_IMPL *)cursor->session;
	CURSOR_API_CALL(cursor, session, equals, NULL);

	WT_ERR(cursor->compare(cursor, other, &cmp));
	*equalp = (cmp == 0) ? 1 : 0;

err:
	API_END(session, ret);

	return ret;
}

/*重加载cursor对应的配置项(append/overwrite)*/
int __wt_cursor_reconfigure(WT_CURSOR *cursor, const char *config)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL*)(cursor->session);
	/*对cursor重置*/
	WT_RET(cursor->reset(cursor));

	/*append操作仅仅应用于列式存储*/
	if (WT_CURSOR_RECNO(cursor)) {
		/*从配置信息里读取append的配置*/
		if ((ret = __wt_config_getones(session, config, "append", &cval)) == 0) {
			if (cval.val)
				F_SET(cursor, WT_CURSTD_APPEND);
			else
				F_CLR(cursor, WT_CURSTD_APPEND);
		} 
		else
			WT_RET_NOTFOUND_OK(ret);
	}

	/*获得overwrite配置项*/
	if ((ret = __wt_config_getones(session, config, "overwrite", &cval)) == 0) {
		if (cval.val)
			F_SET(cursor, WT_CURSTD_OVERWRITE);
		else
			F_CLR(cursor, WT_CURSTD_OVERWRITE);
	} 
	else
		WT_RET_NOTFOUND_OK(ret);

	return 0;
}

/*将cursor指向to_dup指向的游标位置*/
int __wt_cursor_dup_position(WT_CURSOR *to_dup, WT_CURSOR *cursor)
{
	WT_ITEM key;

	/*将to_dup当前指向的key的值设置到cursor中*/
	WT_RET(__wt_cursor_get_raw_key(to_dup, &key));
	__wt_cursor_set_raw_key(cursor, &key);

	/*进行key定位*/
	WT_RET(cursor->search(cursor));

	return 0;
}

/*对cursor进行初始化*/
int __wt_cursor_init(WT_CURSOR *cursor, const char *uri, WT_CURSOR *owner, const char *cfg[], WT_CURSOR **cursorp)
{
	WT_CONFIG_ITEM cval;
	WT_CURSOR *cdump;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cursor->session;

	if (cursor->internal_uri == NULL)
		WT_RET(__wt_strdup(session, uri, &cursor->internal_uri));

	/*对append配置项的读取*/
	if (WT_CURSOR_RECNO(cursor)) {
		WT_RET(__wt_config_gets_def(session, cfg, "append", 0, &cval));
		if (cval.val != 0)
			F_SET(cursor, WT_CURSTD_APPEND);
	}

	/*读取checkpoint项，如果配置了checkpoint，表示数据是永久不变的，不能做增删改*/
	WT_RET(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
	if (cval.len != 0) {
		cursor->insert = __wt_cursor_notsup;
		cursor->update = __wt_cursor_notsup;
		cursor->remove = __wt_cursor_notsup;
	} 
	else {
		WT_RET(__wt_config_gets_def(session, cfg, "readonly", 0, &cval));
		if (cval.val != 0) {
			cursor->insert = __wt_cursor_notsup;
			cursor->update = __wt_cursor_notsup;
			cursor->remove = __wt_cursor_notsup;
		}
	}

	/*读取配置dump操作项*/
	WT_RET(__wt_config_gets_def(session, cfg, "dump", 0, &cval));
	if (cval.len != 0 && owner == NULL) {
		F_SET(cursor,
		    WT_STRING_MATCH("json", cval.str, cval.len) ? WT_CURSTD_DUMP_JSON :
		    (WT_STRING_MATCH("print", cval.str, cval.len) ? WT_CURSTD_DUMP_PRINT : WT_CURSTD_DUMP_HEX));

		WT_RET(__wt_curdump_create(cursor, owner, &cdump));
		owner = cdump;
	} 
	else
		cdump = NULL;

	/*读取overwrite项目*/
	WT_RET(__wt_config_gets_def(session, cfg, "overwrite", 1, &cval));
	if (cval.val)
		F_SET(cursor, WT_CURSTD_OVERWRITE);
	else
		F_CLR(cursor, WT_CURSTD_OVERWRITE);

	/*读取raw配置*/
	WT_RET(__wt_config_gets_def(session, cfg, "raw", 0, &cval));
	if (cval.val != 0)
		F_SET(cursor, WT_CURSTD_RAW);

	/*将cursor加入到session的游标队列中*/
	if (owner != NULL) {
		WT_ASSERT(session, F_ISSET(owner, WT_CURSTD_OPEN));
		TAILQ_INSERT_AFTER(&session->cursors, owner, cursor, q);
	} 
	else
		TAILQ_INSERT_HEAD(&session->cursors, cursor, q);

	/*设置cursor为open状态*/
	F_SET(cursor, WT_CURSTD_OPEN);

	WT_STAT_FAST_DATA_INCR(session, session_cursor_open);
	WT_STAT_FAST_CONN_ATOMIC_INCR(session, session_cursor_open);

	*cursorp = (cdump != NULL) ? cdump : cursor;

	return 0;
}

