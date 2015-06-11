
#include "wt_internal.h"

/*关闭和释放一个配置解析器*/
static int __config_parser_close(WT_CONFIG_PARSER* wt_config_parser)
{
	WT_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (WT_CONFIG_PARSER_IMPL *)wt_config_parser;

	if (config_parser == NULL)
		return EINVAL;

	__wt_free(config_parser->session, config_parser);

	return 0;
}

/*用wt_config_parser配置解析器获得一个key对应的val值*/
static int __config_parser_get(WT_CONFIG_PARSER* wt_config_parser, const char* key, WT_CONFIG_ITEM* cval)
{
	WT_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (WT_CONFIG_PARSER_IMPL *)wt_config_parser;
	if(config_parser == NULL)
		return EINVAL;

	return __wt_config_subgets(config_parser->session, &config_parser->config_item, key, cval);
}

/*用wt_config_parser解析key对应的下一个val值*/
static int __config_parser_next(WT_CONFIG_PARSER* wt_config_parser, WT_CONFIG_ITEM* key, WT_CONFIG_ITEM* cval)
{
	WT_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (WT_CONFIG_PARSER_IMPL *)wt_config_parser;
	if (config_parser == NULL)
		return EINVAL;

	return __wt_config_next(&config_parser->config, key, cval);
}

/*配置解析器对象*/
static const WT_CONFIG_PARSER stds = {
	__config_parser_close,
	__config_parser_next,
	__config_parser_get
};

/*构建一个WT_CONFIG_PARSER对象，并初始化它，config是配置信息的字符串*/
int wiredtiger_config_parser_open(WT_SESSION *wt_session, const char *config, size_t len, WT_CONFIG_PARSER **config_parserp)
{
	WT_CONFIG_ITEM config_item = { config, len, 0, WT_CONFIG_ITEM_STRING };

	WT_CONFIG_PARSER_IMPL *config_parser;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	*config_parserp = NULL;
	session = (WT_SESSION_IMPL *)wt_session;

	memcpy(&config_parser->config_item, &config_item, sizeof(config_item));
	WT_ERR(__wt_config_initn(session, &config_parser->config, config, len));

	if(ret == 0)
		*config_parserp = (WT_CONFIG_PARSER *)config_parser;
	else
		__wt_free(session, config_parser);

err:
	return ret;
}


