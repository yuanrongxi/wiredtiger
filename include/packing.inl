
typedef struct  
{
	union
	{
		int64_t		i;
		uint64_t	u;
		const char*	s;
		WT_ITEM		item;
	} u;

	uint32_t		size;
	int8_t			havesize;
	char			type;
} WT_PACK_VALUE;

/*初始化WT_PACK_VAULE这个结构*/
#define WT_PACK_VALUE_INIT					{{0}, 0, 0, 0};
/*定义一个pv变量，并将其成员初始化为0*/
#define WT_DECL_PACK_VALUE(pv)				WT_PACK_VALUE pv =  WT_PACK_VALUE_INIT

typedef struct 
{
	WT_SESSION_IMPL*	session;
	const char*			cur;			/*当前处理位置*/
	const char*			end;			/*字符串末尾*/
	const char*			orig;			/*字符串开始*/
	unsigned long		repeats;		/*重复次数*/
	WT_PACK_VALUE		lastv;
}WT_PACK;

#define	WT_PACK_INIT						{ NULL, NULL, NULL, NULL, 0, WT_PACK_VALUE_INIT }
/*定义一个WT_PACK变量，并初始化*/
#define	WT_DECL_PACK(pack)					WT_PACK pack = WT_PACK_INIT


typedef struct 
{
	WT_CONFIG			config;
	char				buf[20];
	int					count;
	int					iskey;
	int					genname;
} WT_PACK_NAME;

/*通过fmt字符串构建一个WT_PACK对象*/
static inline int __pack_initn(WT_SESSION_IMPL* session, WT_PACK* pack, const char* fmt, size_t len)
{
	if(*fmt == '@' || *fmt == '<' || *fmt == '>')
		return EINVAL;

	if(*fmt == '.')
		++ fmt;

	pack->session = session;
	pack->cur = fmt;
	pack->orig = fmt;
	pack->end = fmt + len;
	pack->repeats = 0;

	return 0;
}

/*用fmt初始化pack对象*/
static inline int __pack_init(WT_SESSION_IMPL* session, WT_PACK* pack, const char* fmt)
{
	return __pack_initn(session, pack, fmt, strlen(fmt));
}

static inline int __pack_name_init(WT_SESSION_IMPL* session, WT_CONFIG_ITEM* names, int iskey, WT_PACK_NAME* pn)
{
	WT_CLEAR(*pn);
	pn->iskey = iskey;
	
	if(names->str != NULL){
		return 0;//WT_RET(__wt_config_subinit(session, &pn->config, names));TODO:
	}
	else
		pn->genname = 1;

	return 0;
}

/*构建pn下一个key或者value的名字，一般是key1 key2 ......*/
static inline int __pack_name_next(WT_PACK_NAME* pn, WT_CONFIG_ITEM* name)
{
	WT_CONFIG_ITEM ignore;

	if (pn->genname) {
		snprintf(pn->buf, sizeof(pn->buf), (pn->iskey ? "key%d" : "value%d"), pn->count);

		WT_CLEAR(*name);
		name->str = pn->buf;
		name->len = strlen(pn->buf);
		name->type = WT_CONFIG_ITEM_STRING;

		pn->count++;
	}
	else
		return 0;//WT_RET(__wt_config_next(&pn->config, name, &ignore)); TODO:

	return 0;
}

/*对pack的解析,并将解析后的结果放入pv中*/
static inline int __pack_next(WT_PACK* pack, WT_PACK_VALUE* pv)
{
	char* endsize;

	/*如果有重复，直接返回lastv*/
	if(pack->repeats > 0){
		*pv = pack->lastv;
		--pack->repeats;

		return 0;
	}

next:
	/*当前处理的位置已经到最后了，说明没有找到，直接返回*/
	if (pack->cur == pack->end)
		return WT_NOTFOUND;

	if(isdigit(*(pack->cur))){ /*检查当前字符是否为0 ~ 9之间的数字字符*/
		pv->havesize = 1;
		pv->size = WT_STORE_SIZE(strtoul(pack->cur, &endsize, 10)); /*获得pv的长度*/
		pack->cur = endsize;
	}
	else{
		pv->havesize = 0;
		pv->size = 1;
	}

	/*获得PV的类型*/
	pv->type = *pack->cur++;
	pack->repeats = 0;

	switch(pv->type){
	case 'S':
	case 's':
	case 'x':
		return 0;

	case 't':
		if (pv->size < 1 || pv->size > 8) /*有错误*/
			WT_RET_MSG(pack->session, EINVAL, "Bitfield sizes must be between 1 and 8 bits in format '%.*s'", 
						(int)(pack->end - pack->orig), pack->orig);
		return 0;

	case 'u':
	case 'U':
		/*从新确定类型*/
		pv->type = (!pv->havesize && *pack->cur != '\0') ? 'U' : 'u';
		return 0;

	case 'b':
	case 'h':
	case 'i':
	case 'B':
	case 'H':
	case 'I':
	case 'l':
	case 'L':
	case 'q':
	case 'Q':
	case 'r':
	case 'R':
		/* Integral types repeat <size> times. */
		if (pv->size == 0)
			goto next;
		pack->repeats = pv->size - 1;
		pack->lastv = *pv;

		return 0;

	default:
		WT_RET_MSG(pack->session, EINVAL, "Invalid type '%c' found in format '%.*s'", pv->type, (int)(pack->end - pack->orig), pack->orig);
	}
}

/*对pv中值的获取*/
#define	WT_PACK_GET(session, pv, ap) do {		\
	WT_ITEM *__item;							\
	switch (pv.type) {							\
	case 'x':									\
	break;										\
	case 's':									\
	case 'S':									\
	pv.u.s = va_arg(ap, const char *);			\
	break;										\
	case 'U':									\
	case 'u':									\
	__item = va_arg(ap, WT_ITEM *);				\
	pv.u.item.data = __item->data;				\
	pv.u.item.size = __item->size;				\
	break;										\
	case 'b':									\
	case 'h':									\
	case 'i':									\
	pv.u.i = va_arg(ap, int);					\
	break;										\
	case 'B':									\
	case 'H':									\
	case 'I':									\
	case 't':									\
	pv.u.u = va_arg(ap, unsigned int);			\
	break;										\
	case 'l':									\
	pv.u.i = va_arg(ap, long);					\
	break;										\
	case 'L':									\
	pv.u.u = va_arg(ap, unsigned long);			\
	break;										\
	case 'q':									\
	pv.u.i = va_arg(ap, int64_t);				\
	break;										\
	case 'Q':									\
	case 'r':									\
	case 'R':									\
	pv.u.u = va_arg(ap, uint64_t);				\
	break;										\
	/* User format strings have already been validated. */		\
	WT_ILLEGAL_VALUE(session);					\
}												\
} while (0)

/*根据pv->type返回PV值的size*/
static inline size_t __pack_size(WT_SESSION_IMPL* session, WT_PACK_VALUE* pv)
{
	size_t s, pad;

	switch(pv->type){
	case 'x':
		return pv->size;

	/*json*/
	case 'j':
	case 'J':
		if(pv->type == 'j' || pv->havesize != 0)
			s = pv->size;
		else{
			ssize_t len = __wt_json_strlen(pv->u.item.data, pv->u.item.size);
			WT_ASSERT(session, len >= 0);
			s = (size_t)len + 1;
		}
		return 0;

	case 's':
	case 'S':
		if(pv->type == 's' || pv->havesize != 0)
			s = pv->size;
		else
			s = strlen(pv->u.s) + 1;
		return s;

	case 'U':
	case 'u':
		s = pv->u.item.size;
		pad = 0;
		if(pv->havesize && pv->size < s)
			s = pv->size;
		else if(pv->havesize)
			pad = pv->size - s;

		if(pv->type == 'U')
			s += __wt_vsize_uint(s + pad);
		return (s + pad);

	case 'b':
	case 'B':
	case 't':
		return 1;

	case 'h':
	case 'i':
	case 'l':
	case 'q':
		return __wt_vsize_int(pv->u.i);

	case 'H':
	case 'I':
	case 'L':
	case 'Q':
	case 'r':
		return __wt_vsize_uint(pv->u.u);

	case 'R':
		return sizeof(uint64_t);
	}

	__wt_err(session, EINVAL, "unknown pack-value type: %c", (int)pv->type);
	return ((size_t)-1);
}

/*将pv PACK到一个二进制缓冲区中*/
static inline int __pack_write(WT_SESSION_IMPL* session, WT_PACK_VALUE* pv, uint8_t** pp, size_t maxlen)
{
	uint8_t* oldp;
	size_t s, pad;

	switch(pv->type){
	case 'x':
		WT_SIZE_CHECK(pv->size, maxlen);
		memset(*pp, 0, pv->size);
		*pp += pv->size;
		break;

		/*string pack*/
	case 's':
	case 'S':
		s = strlen(pv->u.s);
		if ((pv->type == 's' || pv->havesize) && pv->size < s) {
			s = pv->size;
			pad = 0;
		} 
		else if (pv->havesize)
			pad = pv->size - s;
		else
			pad = 1;

		WT_SIZE_CHECK(s + pad, maxlen);
		if (s > 0)
			memcpy(*pp, pv->u.s, s);

		*pp += s;
		if (pad > 0) {
			memset(*pp, 0, pad);
			*pp += pad;
		}
		break;

		/*json pack*/
	case 'j':
	case 'J':
		s = pv->u.item.size;
		if ((pv->type == 'j' || pv->havesize) && pv->size < s) {
			s = pv->size;
			pad = 0;
		} 
		else if (pv->havesize)
			pad = pv->size - s;
		else
			pad = 1;

		if (s > 0){
			oldp = *pp;
			WT_RET(__wt_json_strncpy((char **)pp, maxlen,
				pv->u.item.data, s));
			maxlen -= (size_t)(*pp - oldp);
		}

		if (pad > 0) {
			WT_SIZE_CHECK(pad, maxlen);
			memset(*pp, 0, pad);
			*pp += pad;
		}
		break;

		/*wt_item 二进制串pack*/
	case 'U':
	case 'u':
		s = pv->u.item.size;
		pad = 0;
		if (pv->havesize && pv->size < s)
			s = pv->size;
		else if (pv->havesize)
			pad = pv->size - s;
		if (pv->type == 'U') {
			oldp = *pp;
			/*
			 * Check that there is at least one byte available: the
			 * low-level routines treat zero length as unchecked.
			 */
			WT_SIZE_CHECK(1, maxlen);
			WT_RET(__wt_vpack_uint(pp, maxlen, s + pad));
			maxlen -= (size_t)(*pp - oldp);
		}

		WT_SIZE_CHECK(s + pad, maxlen);

		if (s > 0)
			memcpy(*pp, pv->u.item.data, s);
		*pp += s;
		if (pad > 0){
			memset(*pp, 0, pad);
			*pp += pad;
		}
		break;

		/*整型数pack*/
	case 'b':
		WT_SIZE_CHECK(1, maxlen);
		**pp = (uint8_t)(pv->u.i + 0x80);
		*pp += 1;
		break;

	case 'B':
	case 't':
		WT_SIZE_CHECK(1, maxlen);
		**pp = (uint8_t)pv->u.u;
		*pp += 1;
		break;

	case 'h':
	case 'i':
	case 'l':
	case 'q':
		WT_SIZE_CHECK(1, maxlen);
		WT_RET(__wt_vpack_int(pp, maxlen, pv->u.i));
		break;

	case 'H':
	case 'I':
	case 'L':
	case 'Q':
	case 'r':
		WT_SIZE_CHECK(1, maxlen);
		WT_RET(__wt_vpack_uint(pp, maxlen, pv->u.u));
		break;

	case 'R':
		WT_SIZE_CHECK(sizeof(uint64_t), maxlen);
		*(uint64_t *)*pp = pv->u.u;
		*pp += sizeof(uint64_t);
		break;

	default:
		WT_RET_MSG(session, EINVAL, "unknown pack-value type: %c", (int)pv->type);
	}

	return 0;
}

/*从pp二进制缓冲区中读取一个pv对象*/
static inline int __unpack_read(WT_SESSION_IMPL* session, WT_PACK_VALUE* pv, const uint8_t** pp, size_t maxlen)
{
	size_t s;

	switch (pv->type) {
	case 'x':
		WT_SIZE_CHECK(pv->size, maxlen);
		*pp += pv->size;
		break;
	case 's':
	case 'S':
		if (pv->type == 's' || pv->havesize)
			s = pv->size;
		else
			s = strlen((const char *)*pp) + 1;
		if (s > 0)
			pv->u.s = (const char *)*pp;
		WT_SIZE_CHECK(s, maxlen);
		*pp += s;
		break;

	case 'U':
		WT_SIZE_CHECK(1, maxlen);
		WT_RET(__wt_vunpack_uint(pp, maxlen, &pv->u.u));
	case 'u':
		if (pv->havesize)
			s = pv->size;
		else if (pv->type == 'U')
			s = (size_t)pv->u.u;
		else
			s = maxlen;
		WT_SIZE_CHECK(s, maxlen);
		pv->u.item.data = *pp;
		pv->u.item.size = s;
		*pp += s;
		break;

	case 'b':
		WT_SIZE_CHECK(1, maxlen);
		pv->u.i = (int8_t)(*(*pp)++ - 0x80);
		break;

	case 'B':
	case 't':
		WT_SIZE_CHECK(1, maxlen);
		pv->u.u = *(*pp)++;
		break;

	case 'h':
	case 'i':
	case 'l':
	case 'q':
		WT_SIZE_CHECK(1, maxlen);
		WT_RET(__wt_vunpack_int(pp, maxlen, &pv->u.i));
		break;

	case 'H':
	case 'I':
	case 'L':
	case 'Q':
	case 'r':
		WT_SIZE_CHECK(1, maxlen);
		WT_RET(__wt_vunpack_uint(pp, maxlen, &pv->u.u));
		break;

	case 'R':
		WT_SIZE_CHECK(sizeof(uint64_t), maxlen);
		pv->u.u = *(uint64_t *)*pp;
		*pp += sizeof(uint64_t);
		break;

	default:
		WT_RET_MSG(session, EINVAL, "unknown pack-value type: %c", (int)pv->type);
	}

	return 0;
}

#define	WT_UNPACK_PUT(session, pv, ap) do {		\
	WT_ITEM *__item;							\
	switch (pv.type) {							\
	case 'x':									\
	break;										\
	case 's':									\
	case 'S':									\
	*va_arg(ap, const char **) = pv.u.s;		\
	break;										\
	case 'U':									\
	case 'u':									\
	__item = va_arg(ap, WT_ITEM *);				\
	__item->data = pv.u.item.data;				\
	__item->size = pv.u.item.size;				\
	break;										\
	case 'b':									\
	*va_arg(ap, int8_t *) = (int8_t)pv.u.i;		\
	break;										\
	case 'h':									\
	*va_arg(ap, int16_t *) = (short)pv.u.i;		\
	break;										\
	case 'i':									\
	case 'l':									\
	*va_arg(ap, int32_t *) = (int32_t)pv.u.i;	\
	break;										\
	case 'q':									\
	*va_arg(ap, int64_t *) = pv.u.i;			\
	break;										\
	case 'B':									\
	case 't':									\
	*va_arg(ap, uint8_t *) = (uint8_t)pv.u.u;	\
	break;										\
	case 'H':									\
	*va_arg(ap, uint16_t *) = (uint16_t)pv.u.u;	\
	break;										\
	case 'I':									\
	case 'L':									\
	*va_arg(ap, uint32_t *) = (uint32_t)pv.u.u;	\
	break;										\
	case 'Q':									\
	case 'r':									\
	case 'R':									\
	*va_arg(ap, uint64_t *) = pv.u.u;			\
	break;										\
	WT_ILLEGAL_VALUE(session);					\
}												\
} while (0)

/*将一个fmt格式化了的pv串pack到buffer中*/
static inline int __wt_struct_packv(WT_SESSION_IMPL* session, void* buffer, size_t size, const char* fmt, va_list ap)
{
	WT_DECL_PACK_VALUE(pv);
	WT_DECL_RET;
	WT_PACK pack;
	uint8_t *p, *end;

	p = buffer;
	end = p + size;

	if (fmt[0] != '\0' && fmt[1] == '\0'){
		pv.type = fmt[0];
		/*填充PV的值*/
		WT_PACK_GET(session, pv, ap);
		/*将PV的值pack到p中*/
		return (__pack_write(session, &pv, &p, size));
	}

	/*将多个pv字串全部pack到p缓冲区*/
	WT_RET(__pack_init(session, &pack, fmt));
	while((ret = __pack_next(&pack, &pv)) == 0){
		WT_PACK_GET(session, pv, ap);
		WT_RET(__pack_write(session, &pv, &p, (size_t)(end - p)));
	}

	WT_ASSERT(session, p <= end);

	if(ret != WT_NOTFOUND)
		return ret;

	return 0;
}

/*计算格式化各个pv的size,并统计返回总共需要的长度*/
static inline int __wt_struct_sizev(WT_SESSION_IMPL *session, size_t *sizep, const char *fmt, va_list ap)
{
	WT_DECL_PACK_VALUE(pv);
	WT_PACK pack;
	size_t total;

	if (fmt[0] != '\0' && fmt[1] == '\0') {
		pv.type = fmt[0];
		WT_PACK_GET(session, pv, ap);
		*sizep = __pack_size(session, &pv);
		return (0);
	}

	WT_RET(__pack_init(session, &pack, fmt));
	for (total = 0; __pack_next(&pack, &pv) == 0;) {
		WT_PACK_GET(session, pv, ap);
		total += __pack_size(session, &pv);
	}

	*sizep = total;

	return 0;
}

/*从二进制缓冲区读取一个pv值，并用格式化方式输出*/
static inline int __wt_struct_unpackv(WT_SESSION_IMPL* session, const void* buffer, size_t size, const char* fmt, va_list ap)
{
	WT_DECL_PACK_VALUE(pv);
	WT_DECL_RET;
	WT_PACK pack;
	const uint8_t *p, *end;

	p = buffer;
	end = p + size;

	if (fmt[0] != '\0' && fmt[1] == '\0') {
		pv.type = fmt[0];
		if ((ret = __unpack_read(session, &pv, &p, size)) == 0)
			WT_UNPACK_PUT(session, pv, ap);
		return 0;
	}

	WT_RET(__pack_init(session, &pack, fmt));
	while((ret = __pack_next(&pack, &pv)) == 0){
		WT_RET(__unpack_read(session, &pv, &p, (size_t)(end - p)));
		WT_UNPACK_PUT(session, pv, ap);
	}

	WT_ASSERT(session, p <= end);
	if (ret != WT_NOTFOUND)
		return ret;

	return 0;
}

/*0压缩后，判断计算的size空间是否合法*/
static inline void __wt_struct_size_adjust(WT_SESSION_IMPL *session, size_t *sizep)
{
	size_t curr_size = *sizep;
	size_t field_size, prev_field_size = 1;

	while ((field_size = __wt_vsize_uint(curr_size)) != prev_field_size) {
		curr_size += field_size - prev_field_size;
		prev_field_size = field_size;
	}

	WT_ASSERT(session, field_size == __wt_vsize_uint(curr_size));

	*sizep = curr_size;
}



