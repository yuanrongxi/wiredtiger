
#include "wt_internal.h"

/*
* __conn_dhandle_open_lock --
*	Spin on the current data handle until either (a) it is open, read
*	locked; or (b) it is closed, write locked.  If exclusive access is
*	requested and cannot be granted immediately because the handle is
*	in use, fail with EBUSY.
*
*	Here is a brief summary of how different operations synchronize using
*	either the schema lock, handle locks or handle flags:
*
*	open -- holds the schema lock, one thread gets the handle exclusive,
*		reverts to a shared handle lock and drops the schema lock
*		once the handle is open;
*	bulk load -- sets bulk and exclusive;
*	salvage, truncate, update, verify -- hold the schema lock, set a
*		"special" flag;
*	sweep -- gets a write lock on the handle, doesn't set exclusive
*
*	The schema lock prevents a lot of potential conflicts: we should never
*	see handles being salvaged or verified because those operation hold the
*	schema lock.  However, it is possible to see a handle that is being
*	bulk loaded, or that the sweep server is closing.
*
*	The principle here is that application operations can cause other
*	application operations to fail (so attempting to open a cursor on a
*	file while it is being bulk-loaded will fail), but internal or
*	database-wide operations should not prevent application-initiated
*	operations.  For example, attempting to verify a file should not fail
*	because the sweep server happens to be in the process of closing that
*	file.
*/

/*获得dhandle的read lock或者write lock，其中获得锁的过程是一个自旋过程,这个锁只是为了打开或者关闭dhandle作用*/
static int __conn_dhandle_open_lock(WT_SESSION_IMPL* session, WT_DATA_HANDLE* dhandle, uint32_t flags)
{
	WT_BTREE* btree;
	WT_DECL_RET;
	int is_open, lock_busy, want_exclusive;

	btree = dhandle->handle;
	lock_busy = 0;
	want_exclusive = LF_ISSET(WT_DHANDLE_EXCLUSIVE) ? 1 : 0;

	for (;;){
		/*
		* If the handle is already open for a special operation, give up.
		*/
		if (F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS))
			return EBUSY;

		/*尝试获得read lock*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) && (!want_exclusive || lock_busy)){
			WT_RET(__wt_readlock(session, dhandle->rwlock));
			is_open = F_ISSET(dhandle, WT_DHANDLE_OPEN) ? 1 : 0;
			if (is_open && !want_exclusive) /*如果只是获得read lock,那么直接返回即可*/
				return 0;
			WT_RET(__wt_readunlock(session, dhandle->rwlock));
		}
		else
			is_open = 0;

		/*尝试获得write lock*/
		if ((ret = __wt_try_writelock(session, dhandle->rwlock)) == 0){
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN) && !want_exclusive){ /*这次操作只是为了获得read lock,释放掉write lock,重新获取read lock*/
				lock_busy = 0;
				WT_RET(__wt_writeunlock(session, dhandle->rwlock));
				continue;
			}

			/* We have an exclusive lock, we're done. */
			F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
			return 0;
		}
		else if (ret != EBUSY || (is_open && want_exclusive)) /*write lock被其他的线程占用，而且datasource是open状态，直接返回*/
			return ret;
		else
			lock_busy = 1;

		__wt_yield();
	}
}

/*通过dhandle name和checkpoint name查找对应的dhandle,并将它设置到session中*/
int __wt_conn_dhandle_find(WT_SESSION_IMPL* session, const char* name, const char* ckpt, uint32_t flags)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	uint64_t bucket;

	WT_UNUSED(flags);	/* Only used in diagnostic builds */
	conn = S2C(session);

	/* We must be holding the handle list lock at a higher level. */
	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED) && !LF_ISSET(WT_DHANDLE_HAVE_REF));

	/* Increment the reference count if we already have the btree open. */
	bucket = __wt_hash_city64(name, strlen(name)) % WT_HASH_ARRAY_SIZE;
	SLIST_FOREACH(dhandle, &conn->dhhash[bucket], hashl)
		if (strcmp(name, dhandle->name) == 0 && ((ckpt == NULL && dhandle->checkpoint == NULL) ||
			(ckpt != NULL && dhandle->checkpoint != NULL && strcmp(ckpt, dhandle->checkpoint) == 0))) {
				session->dhandle = dhandle;
				return 0;
		}

	return WT_NOTFOUND;
}

/*根据name和checkpoint name获得dhandle,并获得对应的lock*/
static __conn_dhandle_get(WT_SESSION_IMPL *session, const char *name, const char *ckpt, uint32_t flags)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	uint32_t bucket;

	conn = S2C(session);
	/*在现有的dhandle 列表中查找，如果有，获得对应的lock并直接返回*/
	ret = __wt_conn_dhandle_find(session, name, ckpt, flags);
	if (ret == 0) {
		dhandle = session->dhandle;
		WT_RET(__conn_dhandle_open_lock(session, dhandle, flags)); /*根据flag获得dhandle的读写锁*/
		return 0;
	}
	WT_RET_NOTFOUND_OK(ret);

	/*如果没找到，根据名字和checkpoint name新建一个dhandle，并加入到connection hash list当中*/
	WT_RET(__wt_calloc_one(session, &dhandle));

	WT_ERR(__wt_rwlock_alloc(session, &dhandle->rwlock, "data handle"));

	dhandle->name_hash = __wt_hash_city64(name, strlen(name));
	WT_ERR(__wt_strdup(session, name, &dhandle->name));
	if (ckpt != NULL)
		WT_ERR(__wt_strdup(session, ckpt, &dhandle->checkpoint));

	/*新建一个btree对象*/
	WT_ERR(__wt_calloc_one(session, &btree));
	dhandle->handle = btree;
	btree->dhandle = dhandle;

	/*获得write lock,并将dhandle设置为独占模式*/
	WT_ERR(__wt_spin_init(session, &dhandle->close_lock, "data handle close"));
	F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
	WT_ERR(__wt_writelock(session, dhandle->rwlock));

	/*将dhandle加入到hash list当中*/
	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));
	bucket = dhandle->name_hash % WT_HASH_ARRAY_SIZE;
	WT_CONN_DHANDLE_INSERT(conn, dhandle, bucket);

	session->dhandle = dhandle;
	return 0;

err:
	WT_TRET(__wt_rwlock_destroy(session, &dhandle->rwlock));
	__wt_free(session, dhandle->name);
	__wt_free(session, dhandle->checkpoint);
	__wt_free(session, dhandle->handle);		/* btree free */
	__wt_spin_destroy(session, &dhandle->close_lock);
	__wt_overwrite_and_free(session, dhandle);

	return ret;
}

/*用同步阻塞的方式关闭一个dhandle的btree*/
int __wt_conn_btree_sync_and_close(WT_SESSION_IMPL* session, int final, int force)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	int no_schema_lock;

	dhandle = session->dhandle;
	btree = S2BT(session);

	if (!F_ISSET(dhandle, WT_DHANDLE_OPEN))
		return 0;
	/*
	* If we don't already have the schema lock, make it an error to try
	* to acquire it.  The problem is that we are holding an exclusive
	* lock on the handle, and if we attempt to acquire the schema lock
	* we might deadlock with a thread that has the schema lock and wants
	* a handle lock (specifically, checkpoint).
	*/
	no_schema_lock = 0;	
	if (!F_ISSET(session, WT_SESSION_SCHEMA_LOCKED)){
		no_schema_lock = 1;
		F_SET(session, WT_SESSION_NO_SCHEMA_LOCK);
	}
	__wt_spin_lock(session, &dhandle->close_lock);

	/*为dhandle对应的btree做一次checkpoint*/
	if(!F_ISSET(btree, WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY))
		WT_ERR(__wt_checkpoint_close(session, final, force));

	if (dhandle->checkpoint == NULL)
		--S2C(session)->open_btree_count;

	/*关闭dhandle的btree*/
	WT_TRET(__wt_btree_close(session));
	F_CLR(dhandle, WT_DHANDLE_OPEN);
	F_CLR(dhandle, WT_BTREE_SPECIAL_FLAGS);

err:
	__wt_spin_unlock(session, &dhandle->close_lock);

	if (no_schema_lock)
		F_CLR(session, WT_SESSION_NO_SCHEMA_LOCK);

	return ret;
}

/*清除掉session dhandle所有的配置信息*/
static void __conn_btree_config_clear(WT_SESSION_IMPL* session)
{
	WT_DATA_HANDLE *dhandle;
	const char **a;

	dhandle = session->dhandle;

	if (dhandle->cfg == NULL)
		return;
	for (a = dhandle->cfg; *a != NULL; ++a)
		__wt_free(session, *a);
	__wt_free(session, dhandle->cfg);
}

/*设置session dhandle的配置信息,这个信息是通过uri查找meta table得到的*/
static int __conn_btree_config_set(WT_SESSION_IMPL* session)
{
	WT_DATA_HANDLE* dhandle;
	WT_DECL_RET;
	char *metaconf;

	dhandle = session->dhandle;

	/*根据dhandle name查找dhandle的meta信息*/
	if ((ret = __wt_metadata_search(session, dhandle->name, &metaconf)) != 0){
		if (ret == WT_NOTFOUND)
			ret = ENOENT;
		WT_RET(ret);
	}
	/*
	* The defaults are included because underlying objects have persistent
	* configuration information stored in the metadata file.  If defaults
	* are included in the configuration, we can add new configuration
	* strings without upgrading the metadata file or writing special code
	* in case a configuration string isn't initialized, as long as the new
	* configuration string has an appropriate default value.
	*
	* The error handling is a little odd, but be careful: we're holding a
	* chunk of allocated memory in metaconf.  If we fail before we copy a
	* reference to it into the object's configuration array, we must free
	* it, after the copy, we don't want to free it.
	*/
	/*将dhandle的meta信息和dhandle关联*/
	WT_ERR(__wt_calloc_def(session, 3, &dhandle->cfg));
	WT_ERR(__wt_strdup(session, WT_CONFIG_BASE(session, file_meta), &dhandle->cfg[0]));
	dhandle->cfg[1] = metaconf;
	return 0;

err:
	__wt_free(session, metaconf);
	return ret;
}

/*打开session dhandle对应的btree*/
static int __conn_btree_open(WT_SESSION_IMPL* session, const char* cfg[], uint32_t flags)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	dhandle = session->dhandle;
	btree = S2BT(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED) &&
		F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE) && !LF_ISSET(WT_DHANDLE_LOCK_ONLY));

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_CLOSING));

	/*
	* If the handle is already open, it has to be closed so it can be
	* reopened with a new configuration.  We don't need to check again:
	* this function isn't called if the handle is already open in the
	* required mode.
	*
	* This call can return EBUSY if there's an update in the object that's
	* not yet globally visible.  That's not a problem because it can only
	* happen when we're switching from a normal handle to a "special" one,
	* so we're returning EBUSY to an attempt to verify or do other special
	* operations.  The reverse won't happen because when the handle from a
	* verify or other special operation is closed, there won't be updates
	* in the tree that can block the close.
	* 先关闭，然后用新的config信息打开btree
	*/
	if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
		WT_RET(__wt_conn_btree_sync_and_close(session, 0, 0));

	/*重新载入配置元信息*/
	__conn_btree_config_clear(session);
	WT_RET(__conn_btree_config_set(session));

	/* Set any special flags on the handle. */
	F_SET(btree, LF_ISSET(WT_BTREE_SPECIAL_FLAGS));

	do{
		WT_ERR(__wt_btree_open(session, cfg));
		F_SET(dhandle, WT_DHANDLE_OPEN);

		if (dhandle->checkpoint == NULL)
			++S2C(session)->open_btree_count;

		/* Drop back to a readlock if that is all that was needed. */
		if (!LF_ISSET(WT_DHANDLE_EXCLUSIVE)) {
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			WT_ERR(__wt_writeunlock(session, dhandle->rwlock));
			WT_ERR(__conn_dhandle_open_lock(session, dhandle, flags));
		}
	} while (!F_ISSET(dhandle, WT_DHANDLE_OPEN));

	if (0) {
err:		
		F_CLR(btree, WT_BTREE_SPECIAL_FLAGS);
		/* If the open failed, close the handle. */
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
			WT_TRET(__wt_conn_btree_sync_and_close(session, 0, 0));
	}

	return ret;
}

/*通过dhandle name和checkpoint name找到并打开dhandle的btree,在这个过程中会根据flags获取dhandle的lock*/
int __wt_conn_btree_get(WT_SESSION_IMPL* session, const char *name, const char *ckpt, const char *cfg[], uint32_t flags)
{
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	if (LF_ISSET(WT_DHANDLE_HAVE_REF))
		WT_RET(__conn_dhandle_open_lock(session, session->dhandle, flags));
	else {
		WT_WITH_DHANDLE_LOCK(session, ret = __conn_dhandle_get(session, name, ckpt, flags));
		WT_RET(ret);
	}
	dhandle = session->dhandle;

	/*打开btree并获得dhandle的lock*/
	if (!LF_ISSET(WT_DHANDLE_LOCK_ONLY) && (!F_ISSET(dhandle, WT_DHANDLE_OPEN) || LF_ISSET(WT_BTREE_SPECIAL_FLAGS))){
		if ((ret = __conn_btree_open(session, cfg, flags)) != 0) {
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			WT_TRET(__wt_writeunlock(session, dhandle->rwlock));
		}
	}

	WT_ASSERT(session, ret != 0 || LF_ISSET(WT_DHANDLE_EXCLUSIVE) == F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE));

	return ret;
}

/*找到对应的dhandle的btree， 并执行一个func函数*/
static int __conn_btree_apply_internal(WT_SESSION_IMPL* session, WT_DATA_HANDLE *dhandle,
	int(*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_DECL_RET;

	/*打开对应handle的btree*/
	ret = __wt_session_get_btree(session, dhandle->name, dhandle->checkpoint, NULL, 0);
	if (ret == 0) {
		WT_SAVE_DHANDLE(session, ret = func(session, cfg));
		if (WT_META_TRACKING(session))
			WT_TRET(__wt_meta_track_handle_lock(session, 0));
		else
			WT_TRET(__wt_session_release_btree(session));
	}
	else if (ret == EBUSY)
		ret = __wt_conn_btree_apply_single(session, dhandle->name, dhandle->checkpoint, func, cfg); /**/

	return ret;
}

/*在uri对应的btree上执行一个func函数，如果uri == NULL,那么所有的没有建立checkpoint的btree上都执行一次checkpoint*/
int __wt_conn_btree_apply(WT_SESSION_IMPL* session, int apply_checkpoints, const char *uri,
	int(*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	uint64_t bucket;

	conn = S2C(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));
	/*通过uri找到对应的dhandle list,在通过dhanle list找到对应的dhandle*/
	if (uri != NULL){
		bucket = __wt_hash_city64(uri, strlen(uri)) % WT_HASH_ARRAY_SIZE;
		SLIST_FOREACH(dhandle, &conn->dhhash[bucket], hashl)
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN) && strcmp(uri, dhandle->name) == 0 && (apply_checkpoints || dhandle->checkpoint == NULL))
				WT_RET(__conn_btree_apply_internal(session, dhandle, func, cfg));
	}
	else{ /*没有指定uri,那么将所有的dhandle的btree都执行一次func,meta file btree除外*/
		SLIST_FOREACH(dhandle, &conn->dhlh, l)
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
				(apply_checkpoints || dhandle->checkpoint == NULL) &&
				WT_PREFIX_MATCH(dhandle->name, "file:") && !WT_IS_METADATA(dhandle))
				WT_RET(__conn_btree_apply_internal(session, dhandle, func, cfg));
	}

	return 0;
}

/*通过uri(dhandle name)获得对应btree最后一个checkpoint,在通过uri和checkpoint name查找对应的dhandle并在其btree执行一个func函数*/
int __wt_conn_btree_apply_single_ckpt(WT_SESSION_IMPL* session, const char* uri, int(*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	const char *checkpoint;

	checkpoint = NULL;

	/*获得uri对应btree的最后一个checkpoint的信息*/
	WT_RET_NOTFOUND_OK(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
	if (cval.len != 0){
		/*
		* The internal checkpoint name is special, find the last
		* unnamed checkpoint of the object.
		*/
		if (WT_STRING_MATCH(WT_CHECKPOINT, cval.str, cval.len)) {
			WT_RET(__wt_meta_checkpoint_last_name(session, uri, &checkpoint));
		}
		else
			WT_RET(__wt_strndup(session, cval.str, cval.len, &checkpoint));
	}

	/*在btree上执行一个func*/
	ret = __wt_conn_btree_apply_single(session, uri, checkpoint, func, cfg);
	__wt_free(session, checkpoint);

	return ret;
}

/*通过uri和checkpoint在整个connection的dhandle队列中找到对应的dhandle,并在其btree上执行func函数*/
int __wt_conn_btree_apply_single(WT_SESSION_IMPL* session, const char *uri, const char *checkpoint,
	int(*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	uint64_t bucket, hash;

	conn = S2C(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));

	hash = __wt_hash_city64(uri, strlen(uri));
	bucket = hash % WT_HASH_ARRAY_SIZE;
	SLIST_FOREACH(dhandle, &conn->dhhash[bucket], hashl)
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
			(hash == dhandle->name_hash && strcmp(uri, dhandle->name) == 0) &&
			((dhandle->checkpoint == NULL && checkpoint == NULL) ||
			(dhandle->checkpoint != NULL && checkpoint != NULL && strcmp(dhandle->checkpoint, checkpoint) == 0))) {
			/*
			* We're holding the handle list lock which locks out
			* handle open (which might change the state of the
			* underlying object).  However, closing a handle
			* doesn't require the handle list lock, lock out
			* closing the handle and then confirm the handle is
			* still open.
			*/
			__wt_spin_lock(session, &dhandle->close_lock);
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
				WT_WITH_DHANDLE(session, dhandle, ret = func(session, cfg));
			}
			__wt_spin_unlock(session, &dhandle->close_lock);
			WT_RET(ret);
		}

	return 0;
}

/*同步关闭以name命名的data handle,TODO:中间关于meta track的过程没有完全弄懂？*/
int __wt_conn_dhandle_close_all(WT_SESSION_IMPL* session, const char* name, int force)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	uint64_t bucket;

	conn = S2C(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));
	WT_ASSERT(session, session->dhandle == NULL);

	bucket = __wt_hash_city64(name, strlen(name)) % WT_HASH_ARRAY_SIZE;
	SLIST_FOREACH(dhandle, &conn->dhhash[bucket], hashl){
		if (strcmp(dhandle->name, name) != 0)
			continue;

		session->dhandle = dhandle;

		/* Lock the handle exclusively. */
		WT_ERR(__wt_session_get_btree(session, dhandle->name, dhandle->checkpoint, NULL, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_LOCK_ONLY));
		if (WT_META_TRACKING(session))
			WT_ERR(__wt_meta_track_handle_lock(session, 0));

		/*
		* We have an exclusive lock, which means there are no cursors
		* open at this point.  Close the handle, if necessary.
		*/
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
			if ((ret = __wt_meta_track_sub_on(session)) == 0)
				ret = __wt_conn_btree_sync_and_close(session, 0, force);

			/*
			* If the close succeeded, drop any locks it acquired.
			* If there was a failure, this function will fail and
			* the whole transaction will be rolled back.
			*/
			if (ret == 0)
				ret = __wt_meta_track_sub_off(session);
		}

		if (!WT_META_TRACKING(session))
			WT_TRET(__wt_session_release_btree(session));

		WT_ERR(ret);
	}

err:
	session->dhandle = NULL;
	return ret;
}

/*将session对应的dhandle从connection dhandle中删除*/
static int __conn_dhandle_remove(WT_SESSION_IMPL* session, int final)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	uint64_t bucket;

	conn = S2C(session);
	dhandle = session->dhandle;
	bucket = dhandle->name_hash % WT_HASH_ARRAY_SIZE;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED));

	/* Check if the handle was reacquired by a session while we waited. */
	if (!final && (dhandle->session_inuse != 0 || dhandle->session_ref != 0))
		return (EBUSY);

	WT_CONN_DHANDLE_REMOVE(conn, dhandle, bucket);
	return 0;
}

/*同步关闭session对应的dhandle，并将其从connection dhandle list中删除,最后释放掉其内存中的对象*/
int __wt_conn_dhandle_discard_single(WT_SESSION_IMPL *session, int final)
{
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	int tret;

	dhandle = session->dhandle;

	if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
		tret = __wt_conn_btree_sync_and_close(session, final, 0);
		if (final && tret != 0) {
			__wt_err(session, tret, "Final close of %s failed", dhandle->name);
			WT_TRET(tret);
		}
		else if (!final)
			WT_RET(tret);
	}

	/*
	* Kludge: interrupt the eviction server in case it is holding the handle list lock.
	*/
	if (!F_ISSET(session, WT_SESSION_HANDLE_LIST_LOCKED))
		F_SET(S2C(session)->cache, WT_CACHE_CLEAR_WALKS);

	/* Try to remove the handle, protected by the data handle lock. */
	WT_WITH_DHANDLE_LOCK(session, WT_TRET(__conn_dhandle_remove(session, final)));

	/*
	* After successfully removing the handle, clean it up.
	*/

	if (ret == 0 || final) {
		WT_TRET(__wt_rwlock_destroy(session, &dhandle->rwlock));
		__wt_free(session, dhandle->name);
		__wt_free(session, dhandle->checkpoint);
		__conn_btree_config_clear(session);
		__wt_free(session, dhandle->handle);
		__wt_spin_destroy(session, &dhandle->close_lock);
		__wt_overwrite_and_free(session, dhandle);

		session->dhandle = NULL;
	}

	return ret;
}

/*关闭connection 中所有的dhandle，并销毁他们在内存中的dhandle对象*/
int __wt_conn_dhandle_discard(WT_SESSION_IMPL* session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	conn = S2C(session);

restart:
	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		if (WT_IS_METADATA(dhandle))
			continue;

		WT_WITH_DHANDLE(session, dhandle, WT_TRET(__wt_conn_dhandle_discard_single(session, 1)));
		goto restart;
	}

	__wt_session_close_cache(session);
	F_SET(session, WT_SESSION_NO_DATA_HANDLES);

	while (dhandle = SLIST_FIRST(&conn->dhlh) != NULL){
		WT_WITH_DHANDLE(session, dhandle, WT_TRET(__wt_conn_dhandle_discard_single(session, 1)));
	}

	return ret;
}


