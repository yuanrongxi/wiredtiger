
#include "wiredtiger.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

typedef struct
{
        WT_CURSOR *cursor;
        WT_SESSION *session;	
}db_info_t;

WT_CONNECTION *conn;

static int setup(db_info_t* info, int create_table)
{
	int ret;
	 ret = conn->open_session(conn, NULL, "isolation=read-committed", &info->session);
        if(ret != 0){
                printf("open_session failed!\n");
                return ret;
        }
	
	if(create_table){
		info->session->create(info->session, "table:mytable", "key_format=S,value_format=S");
	}

        ret = info->session->open_cursor(info->session, "table:mytable", NULL, NULL, &info->cursor);
        if(ret != 0){
                printf("open_cursor failed!\n");
                return ret;
        }
	
	return ret;
}

static void clean(db_info_t* info)
{
	if(info->cursor != NULL){
		info->cursor->close(info->cursor);
		info->cursor = NULL;
	}
	
	if(info->session != NULL){
		info->session->close(info->session, NULL);
		info->session = NULL;
	}
}

static int run = 1;

static void* read_thr(void* arg)
{
	db_info_t db = {NULL, NULL};
	char* key, *value;
	int ret;

	if(setup(&db, 0) != 0){
		printf("read thread setup db failed!\n");
		return NULL;
	}

	db.session->begin_transaction(db.session, NULL);

	while(run){
		db.cursor->reset(db.cursor);
		db.cursor->set_key(db.cursor, "k1");
		ret = db.cursor->search(db.cursor);
		if(ret == 0){
			ret = db.cursor->get_key(db.cursor, &key);
			ret = db.cursor->get_value(db.cursor, &value);
			printf("search, k = %s, v = %s\n", key, value);
		}
		else{
			printf("search failed! code = %d\n", ret);
		}
		sleep(1);
	}

	db.session->commit_transaction(db.session, NULL);
	
	clean(&db);
	
	return NULL;
}

#define WRITE_BARRIER() do{ __asm__ volatile("sfence" ::: "memory");}while(0)

int main(int argc, const char* argv[])
{
	db_info_t db = {NULL, NULL};
	pthread_t thr;
	const char *key, *value;
    int ret;
	
	const char* home = "WT_HOME";	

	ret = system("rm -rf WT_HOME && mkdir WT_HOME");

	if((ret = wiredtiger_open(home, NULL, "create", &conn) != 0)){
		printf("wiredtiger_open failed!\n");
		exit(0);
	}
	
	if(setup(&db, 1) != 0){
		goto close_conn;
	}
	
	db.cursor->set_key(db.cursor, "k1");
	db.cursor->set_value(db.cursor, "1");
	if((ret = db.cursor->insert(db.cursor)) != 0){
		printf("insert k/v failed, code = %u\n", ret);
	}
	else{
		printf("insert k/v success, k = k1, v = 1\n");
	}

	db.session->begin_transaction(db.session, NULL);

	pthread_create(&thr, NULL, read_thr, NULL);

	db.cursor->reset(db.cursor);
	db.cursor->set_key(db.cursor, "k1");
	db.cursor->set_value(db.cursor, "2");
	switch (db.cursor->update(db.cursor)){
	case 0:
		printf("update k = k1, v = 2\n");
		getchar();
		db.session->commit_transaction(db.session, NULL);
		printf("commit update transaction\n");
		break;

	case WT_ROLLBACK:
	default:
		db.session->rollback_transaction(db.session, NULL);
		printf("update failed! rollback\n");
	}
	
	getchar();
	getchar();

	run = 0;
	WRITE_BARRIER();

	pthread_join(thr, NULL);
	
	clean(&db);

close_conn:
	if((ret = conn->close(conn, NULL)) != 0){
		printf("wiredtiger_close failed!\n");
	}	
	return 0;
}



