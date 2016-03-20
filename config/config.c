
#include "wt_internal.h"

/*config错误的消息处理并提示*/
static int __config_err(WT_CONFIG* conf, const char* msg, int err)
{
	WT_RET_MSG(conf->session, err, "Error parsing '%.*s' at byte %u: %s",
		(int)(conf->end - conf->orig), conf->orig, (u_int)(conf->cur - conf->orig), msg);
}

/*初始化一个wt config对象，并将str设置为它的*/
int __wt_config_initn(WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str, size_t len)
{
	conf->session = session;
	conf->orig = conf->cur = str;
	conf->end = str + len;
	conf->depth = 0;
	conf->top = -1;
	conf->go = NULL;

	return 0;
}

/*初始化一个config, 和__wt_config_initn*/
int __wt_config_init(WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str)
{
	size_t len;

	len = (str == NULL) ? 0 : strlen(str);

	return __wt_config_initn(session, conf, str, len);
}

/*用wt item初始化config*/
int __wt_config_subinit(WT_SESSION_IMPL *session, WT_CONFIG *conf, WT_CONFIG_ITEM *item)
{
	return (__wt_config_initn(session, conf, item->str, item->len));
}

#define	PUSH(i, t) do {							\
	if (conf->top == -1)						\
	conf->top = conf->depth;					\
	if (conf->depth == conf->top) {				\
	if (out->len > 0)							\
	return (__config_err(conf,					\
	"New value starts without a separator",		\
	EINVAL));									\
	out->type = (t);							\
	out->str = (conf->cur + (i));				\
	}											\
} while (0)

#define	CAP(i) do {											\
	if (conf->depth == conf->top)							\
	out->len = (size_t)((conf->cur + (i) + 1) - out->str);	\
} while (0)

typedef enum {
	A_LOOP, 
	A_BAD, 
	A_DOWN, 
	A_UP, 
	A_VALUE, 
	A_NEXT, 
	A_QDOWN, 
	A_QUP,
	A_ESC, 
	A_UNESC, 
	A_BARE, 
	A_NUMBARE, 
	A_UNBARE, 
	A_UTF8_2,
	A_UTF8_3, 
	A_UTF8_4, 
	A_UTF_CONTINUE
} CONFIG_ACTION;

static const int8_t gostruct[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_LOOP, A_LOOP, A_BAD, A_BAD, A_LOOP, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_LOOP, A_BAD, A_QUP,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UP, A_DOWN, A_BAD, A_BAD,
	A_NEXT, A_NUMBARE, A_BARE, A_BARE, A_NUMBARE, A_NUMBARE,
	A_NUMBARE, A_NUMBARE, A_NUMBARE, A_NUMBARE, A_NUMBARE,
	A_NUMBARE, A_NUMBARE, A_NUMBARE, A_VALUE, A_BAD, A_BAD,
	A_VALUE, A_BAD, A_BAD, A_BAD, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_UP, A_BAD,
	A_DOWN, A_BAD, A_BARE, A_BAD, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_UP, A_BAD,
	A_DOWN, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

static const int8_t gobare[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_UNBARE, A_UNBARE, A_BAD, A_BAD, A_UNBARE, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNBARE,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_UNBARE, A_LOOP, A_LOOP, A_UNBARE, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_UNBARE, A_LOOP, A_LOOP, A_UNBARE, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_UNBARE,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_UNBARE, A_LOOP, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

static const int8_t gostring[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_LOOP, A_LOOP, A_QDOWN,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_ESC, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3,
	A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3,
	A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_4,
	A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4,
	A_UTF8_4, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

static const int8_t goutf8_continue[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD
};

static const int8_t goesc[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD,
	A_BAD, A_BAD, A_UNESC, A_BAD, A_UNESC, A_UNESC, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

static int __config_next(WT_CONFIG* conf, WT_CONFIG_ITEM* key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG_ITEM *out = key;
	int utf8_remain = 0;
	static const WT_CONFIG_ITEM true_value = {"", 0, 1, WT_CONFIG_ITEM_BOOL};

	key->len = 0;
	*value = true_value;

	if (conf->go == NULL)
		conf->go = gostruct;

	while (conf->cur < conf->end) {
		switch (conf->go[*(const uint8_t *)conf->cur]) {
		case A_LOOP:
			break;

		case A_BAD:
			return (__config_err(conf, "Unexpected character", EINVAL));

		case A_DOWN:
			--conf->depth;
			CAP(0);
			break;

		case A_UP:
			if (conf->top == -1)
				conf->top = 1;
			PUSH(0, WT_CONFIG_ITEM_STRUCT);
			++conf->depth;
			break;

		case A_VALUE:
			if (conf->depth == conf->top) {
				/*
				 * Special case: ':' is permitted in unquoted
				 * values.
				 */
				if (out == value && *conf->cur != ':')
					return (__config_err(conf, "Value already complete", EINVAL));
				out = value;
			}
			break;

		case A_NEXT:
			/*
			 * If we're at the top level and we have a complete
			 * key (and optional value), we're done.
			 */
			if (conf->depth == conf->top && key->len > 0) {
				++conf->cur;
				return (0);
			} else
				break;

		case A_QDOWN:
			CAP(-1);
			conf->go = gostruct;
			break;

		case A_QUP:
			PUSH(1, WT_CONFIG_ITEM_STRING);
			conf->go = gostring;
			break;

		case A_ESC:
			conf->go = goesc;
			break;

		case A_UNESC:
			conf->go = gostring;
			break;

		case A_BARE:
			PUSH(0, WT_CONFIG_ITEM_ID);
			conf->go = gobare;
			break;

		case A_NUMBARE:
			PUSH(0, WT_CONFIG_ITEM_NUM);
			conf->go = gobare;
			break;

		case A_UNBARE:
			CAP(-1);
			conf->go = gostruct;
			continue;

		case A_UTF8_2:
			conf->go = goutf8_continue;
			utf8_remain = 1;
			break;

		case A_UTF8_3:
			conf->go = goutf8_continue;
			utf8_remain = 2;
			break;

		case A_UTF8_4:
			conf->go = goutf8_continue;
			utf8_remain = 3;
			break;

		case A_UTF_CONTINUE:
			if (!--utf8_remain)
				conf->go = gostring;
			break;
		}

		conf->cur++;
	}

	if (conf->go == gobare) {
		CAP(-1);
		conf->go = gostruct;
	}

	if (conf->depth <= conf->top && key->len > 0)
		return (0);

	if (conf->depth == 0)
		return WT_NOTFOUND;

	return (__config_err(conf, "Closing brackets missing from config string", EINVAL));
}

#define	WT_SHIFT_INT64(v, s) do {					\
	if ((v) < 0)									\
	goto range;										\
	(v) = (int64_t)(((uint64_t)(v)) << (s));		\
} while (0)

/*进行指定配置项值的处理，按照数据类型来进行转换(一般是bool或者整型数)*/
static int __config_process_value(WT_CONFIG* conf, WT_CONFIG_ITEM* value)
{
	char *endptr;

	if (value->len == 0)
		return 0;

	/*确定数据类型*/
	if (value->type == WT_CONFIG_ITEM_ID) {/*bool类型*/
		if (WT_STRING_MATCH("false", value->str, value->len)) {
			value->type = WT_CONFIG_ITEM_BOOL;
			value->val = 0;
		} 
		else if (WT_STRING_MATCH("true", value->str, value->len)) {
			value->type = WT_CONFIG_ITEM_BOOL;
			value->val = 1;
		}
	}
	else if(value->type == WT_CONFIG_ITEM_NUM){ /*整形数*/
		errno = 0;
		value->val = strtoll(value->str, &endptr, 10);

		/* Check any leftover characters. */
		while (endptr < value->str + value->len){
			switch (*endptr++) {
			case 'b':
			case 'B':
				/* Byte: no change. */
				break;
			case 'k':
			case 'K':
				WT_SHIFT_INT64(value->val, 10);
				break;
			case 'm':
			case 'M':
				WT_SHIFT_INT64(value->val, 20);
				break;
			case 'g':
			case 'G':
				WT_SHIFT_INT64(value->val, 30);
				break;
			case 't':
			case 'T':
				WT_SHIFT_INT64(value->val, 40);
				break;
			case 'p':
			case 'P':
				WT_SHIFT_INT64(value->val, 50);
				break;
			default:
				value->type = WT_CONFIG_ITEM_ID;
				break;
			}

			if (value->type == WT_CONFIG_ITEM_NUM && errno == ERANGE)
				goto range;
		}
	}

	return 0;

range:
	/*写入一个错误信息到stderr中*/
	return __config_err(conf, "Number out of range", ERANGE);
}

/*读取下一个配置项的值*/
int __wt_config_next(WT_CONFIG *conf, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
{
	WT_RET(__config_next(conf, key, value));
	return __config_process_value(conf, value);
}

/*通过KEY找到对应的value配置*/
static int __config_getraw(WT_CONFIG *cparser, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value, int top)
{
	WT_CONFIG sparser;
	WT_CONFIG_ITEM k, v, subk;
	WT_DECL_RET;
	int found;

	found = 0;
	while ((ret = __config_next(cparser, &k, &v)) == 0) {
		if (k.type != WT_CONFIG_ITEM_STRING && k.type != WT_CONFIG_ITEM_ID)
			continue;

		if (k.len == key->len && strncmp(key->str, k.str, k.len) == 0) {
			*value = v;
			found = 1;
		} 
		else if (k.len < key->len && key->str[k.len] == '.' && strncmp(key->str, k.str, k.len) == 0) {
			subk.str = key->str + k.len + 1;
			subk.len = (key->len - k.len) - 1;
			WT_RET(__wt_config_initn(cparser->session, &sparser, v.str, v.len));

			if ((ret =__config_getraw(&sparser, &subk, value, 0)) == 0)
				found = 1;

			WT_RET_NOTFOUND_OK(ret);
		}
	}
	/*如果不是not find错误，直接返回*/
	WT_RET_NOTFOUND_OK(ret);

	if (!found)
		return WT_NOTFOUND;

	return (top ? __config_process_value(cparser, value) : 0);
}

/*通过key获得value*/
int __wt_config_get(WT_SESSION_IMPL *session, const char **cfg, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG cparser;
	WT_DECL_RET;
	int found;

	for (found = 0; *cfg != NULL; cfg++) {
		WT_RET(__wt_config_init(session, &cparser, *cfg));
		if ((ret = __config_getraw(&cparser, key, value, 1)) == 0)
			found = 1;
		else if(ret != WT_NOTFOUND)
			return ret;
	}

	return (found ? 0 : WT_NOTFOUND);
}

/*通过key字符串值获得value*/
int __wt_config_gets(WT_SESSION_IMPL *session, const char **cfg, const char *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG_ITEM key_item = { key, strlen(key), 0, WT_CONFIG_ITEM_STRING };

	return __wt_config_get(session, cfg, &key_item, value);
}

/*通过key获得value,如果value是none,将value设置成空串*/
int __wt_config_gets_none(WT_SESSION_IMPL *session, const char **cfg, const char *key, WT_CONFIG_ITEM *value)
{
	WT_RET(__wt_config_gets(session, cfg, key, value));
	if (WT_STRING_MATCH("none", value->str, value->len))
		value->len = 0;

	return 0;
}

int __wt_config_getone(WT_SESSION_IMPL *session, const char *config, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG cparser;

	WT_RET(__wt_config_init(session, &cparser, config));
	return __config_getraw(&cparser, key, value, 1);
}

int __wt_config_getones(WT_SESSION_IMPL *session, const char *config, const char *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG cparser;
	WT_CONFIG_ITEM key_item =
	{ key, strlen(key), 0, WT_CONFIG_ITEM_STRING };

	WT_RET(__wt_config_init(session, &cparser, config));
	return __config_getraw(&cparser, &key_item, value, 1);
}

int __wt_config_getones_none(WT_SESSION_IMPL *session, const char *config, const char *key, WT_CONFIG_ITEM *value)
{
	WT_RET(__wt_config_getones(session, config, key, value));
	if (WT_STRING_MATCH("none", value->str, value->len))
		value->len = 0;

	return 0;
}

int __wt_config_gets_def(WT_SESSION_IMPL *session, const char **cfg, const char *key, int def, WT_CONFIG_ITEM *value)
{
	static const WT_CONFIG_ITEM false_value = {"", 0, 0, WT_CONFIG_ITEM_NUM};

	*value = false_value;
	value->val = def;
	if (cfg == NULL || cfg[0] == NULL || cfg[1] == NULL)
		return 0;

	else if (cfg[2] == NULL)
		WT_RET_NOTFOUND_OK(__wt_config_getones(session, cfg[1], key, value));

	return (__wt_config_gets(session, cfg, key, value));
}

int __wt_config_subgetraw(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG cparser;

	WT_RET(__wt_config_initn(session, &cparser, cfg->str, cfg->len));
	return __config_getraw(&cparser, key, value, 1);
}

int __wt_config_subgets(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, const char *key, WT_CONFIG_ITEM *value)
{
	WT_CONFIG_ITEM key_item =
	{ key, strlen(key), 0, WT_CONFIG_ITEM_STRING };

	return __wt_config_subgetraw(session, cfg, &key_item, value);
}



