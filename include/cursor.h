/***************************************************************************
*定义cursor游标的数据结构
***************************************************************************/


struct __wt_cursor_json 
{
	char	*key_buf;				/* JSON formatted string */
	char	*value_buf;				/* JSON formatted string */
	WT_CONFIG_ITEM key_names;		/* Names of key columns */
	WT_CONFIG_ITEM value_names;		/* Names of value columns */
};



