#include "wt_internal.h"

/*向session的table list中增加WT_TABLE对象,并打开对应的table*/
static int __schema_add_table(WT_SESSION_IMPL* session, const char* name, size_t namelen, int ok_incomplete, WT_TABLE* tablep)
{
	WT_DECL_RET;
	WT_TABLE *table;
	uint64_t bucket;

	WT_RET(__wt_metadata_open(session));

	WT_WITH_TABLE_LOCK(session, ret = __wt_schema_open_table(session, name, namelen, ok_incomplete, &table));

	WT_RET(ret);
	/*hash table存储*/
	bucket = table->name_hash % WT_HASH_ARRAY_SIZE;
	SLIST_INSERT_HEAD(&session->tables, table, l);
	SLIST_INSERT_HEAD(&session->tablehash[bucket], table, hashl);

	*tablep = table;

	return 0;
}

/*通过表名在session table list中查找对应的表对象*/
static int __schema_find_table(WT_SESSION_IMPL* session, const char* name, size_t namelen, WT_TABLE** tablep)
{
	WT_TABLE *table;
	const char *tablename;
	uint64_t bucket;

	bucket = __wt_hash_city64(name, namelen) % WT_HASH_ARRAY_SIZE;

restart:
	SLIST_FOREACH(table, &session->tablehash[bucket], hashl) {
		tablename = table->name;
		(void)WT_PREFIX_SKIP(tablename, "table:");
		if(WT_STRING_MATCH(tablename, name, namelen)){
			/*
			 * Ignore stale tables.
			 *
			 * XXX: should be managed the same as btree handles,
			 * with a local cache in each session and a shared list
			 * in the connection.  There is still a race here
			 * between checking the generation and opening the
			 * first column group.
			 */
			if (table->schema_gen != S2C(session)->schema_gen) {
				if (table->refcnt == 0) {
					WT_RET(__wt_schema_remove_table(
						session, table));
					goto restart;
				}
				continue;
			}
			*tablep = table;
			return 0;
		}
	}

	return WT_NOTFOUND;
}

/*通过表名获得表对象，如果表对象没有载入内存，在内存中创建对应表对象,并标记引用计数*/
int __wt_schema_get_table(WT_SESSION_IMPL* session, const char* name, size_t namelen, int ok_incomplete, WT_TABLE** tablep)
{
	WT_DECL_RET;
	WT_TABLE *table;

	*tablep = table = NULL;
	ret = __schema_find_table(session, name, namelen, &table);

	if (ret == WT_NOTFOUND)
		ret = __schema_add_table(session, name, namelen, ok_incomplete, &table);

	if (ret == 0) {
		++table->refcnt;
		*tablep = table;
	}

	return ret;
}

/*释放一个表对象的引用*/
void __wt_schema_release_table(WT_SESSION_IMPL* session, WT_TABLE* table)
{
	WT_ASSERT(session, table->refcnt > 0);
	--table->refcnt;
}

/*释放colgourp对象*/
void __wt_schema_destroy_colgroup(WT_SESSION_IMPL* session, WT_COLGROUP* colgroup)
{
	__wt_free(session, colgroup->name);
	__wt_free(session, colgroup->source);
	__wt_free(session, colgroup->config);
	__wt_free(session, colgroup);
}

/*释放内存中的索引*/
int __wt_schema_destroy_index(WT_SESSION_IMPL* session, WT_INDEX* idx)
{
	WT_DECL_RET;

	/* If there is a custom collator configured, terminate it. */
	if (idx->collator != NULL &&
		idx->collator_owned && idx->collator->terminate != NULL) {
			WT_TRET(idx->collator->terminate(idx->collator, &session->iface));
			idx->collator = NULL;
			idx->collator_owned = 0;
	}

	/* If there is a custom extractor configured, terminate it. */
	if (idx->extractor != NULL &&idx->extractor_owned && idx->extractor->terminate != NULL) {
			WT_TRET(idx->extractor->terminate(idx->extractor, &session->iface));
			idx->extractor = NULL;
			idx->extractor_owned = 0;
	}

	__wt_free(session, idx->name);
	__wt_free(session, idx->source);
	__wt_free(session, idx->config);
	__wt_free(session, idx->key_format);
	__wt_free(session, idx->key_plan);
	__wt_free(session, idx->value_plan);
	__wt_free(session, idx->idxkey_format);
	__wt_free(session, idx->exkey_format);
	__wt_free(session, idx);

	return ret;
}

/*释放内存中表对象*/
int __wt_schema_destroy_table(WT_SESSION_IMPL* session, WT_TABLE* table)
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_INDEX *idx;
	u_int i;

	__wt_free(session, table->name);
	__wt_free(session, table->config);
	__wt_free(session, table->plan);
	__wt_free(session, table->key_format);
	__wt_free(session, table->value_format);
	/*释放colgroups*/
	if(table->cgroups != NULL){
		for (i = 0; i < WT_COLGROUPS(table); i++) {
			if ((colgroup = table->cgroups[i]) == NULL)
				continue;

			__wt_schema_destroy_colgroup(session, colgroup);
		}
		__wt_free(session, table->cgroups);
	}
	/*释放索引对象*/
	if(table->indices != NULL){
		for (i = 0; i < table->nindices; i++) {
			if ((idx = table->indices[i]) == NULL)
				continue;
			WT_TRET(__wt_schema_destroy_index(session, idx));
		}
		__wt_free(session, table->indices);
	}

	__wt_free(session, table);

	return ret;
}

/*从session 的table hash table中删除掉对应的table对象，并且释放对应的内存对象*/
int __wt_schema_remove_table(WT_SESSION_IMPL* session, WT_TABLE* table)
{
	uint64_t bucket;
	WT_ASSERT(session, table->refcnt <= 1);

	/*从session hash table中删除*/
	bucket = table->name_hash % WT_HASH_ARRAY_SIZE;
	SLIST_REMOVE(&session->tables, table, __wt_table, l);
	SLIST_REMOVE(&session->tablehash[bucket], table, __wt_table, hashl);

	return __wt_schema_destroy_table(session, table);
}

/*关闭session中所有的table对象*/
int __wt_schema_close_tables(WT_SESSION_IMPL* session)
{
	WT_DECL_RET;
	WT_TABLE *table;

	while((table = SLIST_FIRST(&session->tables)) != NULL)
		WT_TRET(__wt_schema_remove_table(session, table));
	return ret;
}

