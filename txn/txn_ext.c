/***************************************************************
* 事务的外部API接口
***************************************************************/

#include "wt_internal.h"

/*返回session执行事务的ID，在这个过程会检查cache占用率，如果占用太高就会evict page,
 *如果txn id没有生成，会生成一个全局的txn id返回
 */
uint64_t __wt_ext_transaction_id(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session)
{
	WT_SESSION_IMPL* session;

	(void)wt_api;
	session = (WT_SESSION_IMPL*)wt_session;

	__wt_txn_id_check(session);
	return (session->txn.id);
}

/*获得session的隔离级别*/
int __wt_ext_transaction_isolation_level(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session)
{
	WT_SESSION_IMPL* session;
	WT_TXN* txn;

	(void)wt_api;					/* Unused parameters */
	session = (WT_SESSION_IMPL *)wt_session;
	txn = &session->txn;

	if (txn->isolation == TXN_ISO_READ_COMMITTED)
		return WT_TXN_ISO_READ_COMMITTED;
	if (txn->isolation == TXN_ISO_READ_UNCOMMITTED)
		return WT_TXN_ISO_READ_UNCOMMITTED;
	return WT_TXN_ISO_SNAPSHOT;
}

/*设置外部的一个事务回调notify对象*/
int __wt_ext_transaction_notify(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, WT_TXN_NOTIFY *notify)
{
	WT_SESSION_IMPL *session;
	WT_TXN *txn;

	(void)wt_api;					/* Unused parameters */

	session = (WT_SESSION_IMPL* )wt_session;
	txn = &session->txn;

	if(txn->notify == notify)
		return 0;
	if(txn->notify != NULL)
		return ENOMEM;

	txn->notify = notify;

	return 0;
}

/*获得最早且未结束的事务ID*/
uint64_t __wt_ext_transaction_oldest(WT_EXTENSION_API* wt_api)
{
	return (((WT_CONNECTION_IMPL *)wt_api->conn)->txn_global.oldest_id);
}

/*判断transaction_id是否对session执行的事务可见*/
int __wt_ext_transaction_visible(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, uint64_t transaction_id)
{
	(void)wt_api;					/* Unused parameters */
	return (__wt_txn_visible((WT_SESSION_IMPL *)wt_session, transaction_id));
}




