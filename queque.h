#ifndef __DB_QUEUE_H_
#define __DB_QUEUE_H_

#ifndef defined(__cplusplus)
extern "C"{
#endif

#undef LIST_EMPTY
#undef LIST_ENTRY
#undef LIST_FIRST
#undef LIST_FOREACH
#undef LIST_HEAD
#undef LIST_HEAD_INITIALIZER
#undef LIST_INIT
#undef LIST_INSERT_AFTER
#undef LIST_INSERT_BEFORE
#undef LIST_INSERT_HEAD
#undef LIST_NEXT
#undef LIST_REMOVE
#undef QMD_TRACE_ELEM
#undef QMD_TRACE_HEAD
#undef QUEUE_MACRO_DEBUG
#undef SLIST_EMPTY
#undef SLIST_ENTRY
#undef SLIST_FIRST
#undef SLIST_FOREACH
#undef SLIST_FOREACH_PREVPTR
#undef SLIST_HEAD
#undef SLIST_HEAD_INITIALIZER
#undef SLIST_INIT
#undef SLIST_INSERT_AFTER
#undef SLIST_INSERT_HEAD
#undef SLIST_NEXT
#undef SLIST_REMOVE
#undef SLIST_REMOVE_HEAD
#undef STAILQ_CONCAT
#undef STAILQ_EMPTY
#undef STAILQ_ENTRY
#undef STAILQ_FIRST
#undef STAILQ_FOREACH
#undef STAILQ_HEAD
#undef STAILQ_HEAD_INITIALIZER
#undef STAILQ_INIT
#undef STAILQ_INSERT_AFTER
#undef STAILQ_INSERT_HEAD
#undef STAILQ_INSERT_TAIL
#undef STAILQ_LAST
#undef STAILQ_NEXT
#undef STAILQ_REMOVE
#undef STAILQ_REMOVE_HEAD
#undef STAILQ_REMOVE_HEAD_UNTIL
#undef TAILQ_CONCAT
#undef TAILQ_EMPTY
#undef TAILQ_ENTRY
#undef TAILQ_FIRST
#undef TAILQ_FOREACH
#undef TAILQ_FOREACH_REVERSE
#undef TAILQ_HEAD
#undef TAILQ_HEAD_INITIALIZER
#undef TAILQ_INIT
#undef TAILQ_INSERT_AFTER
#undef TAILQ_INSERT_BEFORE
#undef TAILQ_INSERT_HEAD
#undef TAILQ_INSERT_TAIL
#undef TAILQ_LAST
#undef TAILQ_NEXT
#undef TAILQ_PREV
#undef TAILQ_REMOVE
#undef TRACEBUF
#undef TRASHIT

#define QMD_TRACE_ELEM(elem)
#define QMD_TRACE_HEAD(head)
#define TRACEBUF
#define TRASHIT(x)


/*单向链表的申明*/
#define SLIST_HEAD(name, type)						\
struct name{										\
	struct type* slh_first;							\
}

#define SLIST_HEAD_INITIALIZER(head)				\
{NULL}

#define SLIST_ENTRY(type)							\
struct{												\
	struct type* sle_next;							\
}

/*单向链表的操作函数*/
#define	SLIST_EMPTY(head)	((head)->slh_first == NULL)

#define	SLIST_FIRST(head)	((head)->slh_first)

#define	SLIST_NEXT(elm, field)	((elm)->field.sle_next)

#define	SLIST_FOREACH(var, head, field)					\
	for ((var) = SLIST_FIRST((head));					\
	(var);												\
	(var) = SLIST_NEXT((var), field))

#define	SLIST_FOREACH_PREVPTR(var, varp, head, field)	\
	for ((varp) = &SLIST_FIRST((head));					\
	((var) = *(varp)) != NULL;							\
	(varp) = &SLIST_NEXT((var), field))

#define SLIST_INIT(head) do {							\
	SLIST_FIRST((head)) = NULL;							\
} while (0)

/*在slistelm后插入elm*/
#define	SLIST_INSERT_AFTER(slistelm, elm, field) do {			\
	SLIST_NEXT((elm), field) = SLIST_NEXT((slistelm), field);	\
	SLIST_NEXT((slistelm), field) = (elm);						\
} while (0)

/*将elm插入到头位置*/
#define	SLIST_INSERT_HEAD(head, elm, field) do {				\
	SLIST_NEXT((elm), field) = SLIST_FIRST((head));				\
	SLIST_FIRST((head)) = (elm);								\
} while (0)

/*从链表中删除掉对应的elm*/
#define	SLIST_REMOVE(head, elm, type, field) do {				\
	if (SLIST_FIRST((head)) == (elm)) {							\
	SLIST_REMOVE_HEAD((head), field);							\
	}															\
	else {														\
struct type *curelm = SLIST_FIRST((head));						\
	while (SLIST_NEXT(curelm, field) != (elm))					\
	curelm = SLIST_NEXT(curelm, field);							\
	SLIST_NEXT(curelm, field) =									\
	SLIST_NEXT(SLIST_NEXT(curelm, field), field);				\
	}															\
} while (0)

/*删除链表的头单元*/
#define	SLIST_REMOVE_HEAD(head, field) do {							\
	SLIST_FIRST((head)) = SLIST_NEXT(SLIST_FIRST((head)), field);	\
} while (0)

/*单向tail queue申明*/
#define STAILQ_HEAD(name, type)									\
struct name {													\
	struct type** stqh_first;									\
	struct type** stqh_last;									\
}

#define STAILQ_HEAD_INITIALIZER(head)							\
{ NULL, &(head).stqh_first }

#define STAILQ_ENTRY(type)										\
struct{															\
	struct type* stqe_next;										\
}

/*将head2的单元复制给head1,并清空head2*/
#define	STAILQ_CONCAT(head1, head2) do {						\
	if (!STAILQ_EMPTY((head2))) {								\
	*(head1)->stqh_last = (head2)->stqh_first;					\
	(head1)->stqh_last = (head2)->stqh_last;					\
	STAILQ_INIT((head2));										\
	}															\
} while (0)

#define	STAILQ_EMPTY(head)	((head)->stqh_first == NULL)

#define	STAILQ_FIRST(head)	((head)->stqh_first)

/*循环遍历TAIL QUEUE*/
#define	STAILQ_FOREACH(var, head, field)						\
	for ((var) = STAILQ_FIRST((head));							\
	(var);														\
	(var) = STAILQ_NEXT((var), field))

#define	STAILQ_INIT(head) do {									\
	STAILQ_FIRST((head)) = NULL;								\
	(head)->stqh_last = &STAILQ_FIRST((head));					\
} while (0)

/*在tqelm后插入一个elm*/
#define	STAILQ_INSERT_AFTER(head, tqelm, elm, field) do {		\
	if ((STAILQ_NEXT((elm), field) = STAILQ_NEXT((tqelm), field)) == NULL)\
	(head)->stqh_last = &STAILQ_NEXT((elm), field);				\
	STAILQ_NEXT((tqelm), field) = (elm);						\
} while (0)

/*在tail queue头上插入一个elm*/
#define	STAILQ_INSERT_HEAD(head, elm, field) do {					\
	if ((STAILQ_NEXT((elm), field) = STAILQ_FIRST((head))) == NULL)	\
	(head)->stqh_last = &STAILQ_NEXT((elm), field);					\
	STAILQ_FIRST((head)) = (elm);									\
} while (0)

/*在tail queue末尾插入一个elm*/
#define	STAILQ_INSERT_TAIL(head, elm, field) do {					\
	STAILQ_NEXT((elm), field) = NULL;								\
	*(head)->stqh_last = (elm);										\
	(head)->stqh_last = &STAILQ_NEXT((elm), field);					\
} while (0)

/*获得尾部单元*/
#define	STAILQ_LAST(head, type, field)			\
	(STAILQ_EMPTY((head)) ?						\
NULL :											\
	((struct type *)							\
	((char *)((head)->stqh_last) - __offsetof(struct type, field))))

#define	STAILQ_NEXT(elm, field)	((elm)->field.stqe_next)

/*从tail queue中删除一个单元*/
#define	STAILQ_REMOVE(head, elm, type, field) do {			\
	if (STAILQ_FIRST((head)) == (elm)) {					\
	STAILQ_REMOVE_HEAD((head), field);						\
	}														\
	else {													\
struct type *curelm = STAILQ_FIRST((head));					\
	while (STAILQ_NEXT(curelm, field) != (elm))				\
	curelm = STAILQ_NEXT(curelm, field);					\
	if ((STAILQ_NEXT(curelm, field) =						\
	STAILQ_NEXT(STAILQ_NEXT(curelm, field), field)) == NULL)\
	(head)->stqh_last = &STAILQ_NEXT((curelm), field);		\
	}														\
} while (0)

/*删除头单元*/
#define	STAILQ_REMOVE_HEAD(head, field) do {				\
	if ((STAILQ_FIRST((head)) =								\
	STAILQ_NEXT(STAILQ_FIRST((head)), field)) == NULL)		\
	(head)->stqh_last = &STAILQ_FIRST((head));				\
} while (0)

/*如果tail queue只有2个单元，删除非头单元*/
#define	STAILQ_REMOVE_HEAD_UNTIL(head, elm, field) do {			\
	if ((STAILQ_FIRST((head)) = STAILQ_NEXT((elm), field)) == NULL)	\
	(head)->stqh_last = &STAILQ_FIRST((head));		\
} while (0)

/*list申明*/
#define	LIST_HEAD(name, type)						\
struct name {										\
struct type *lh_first;	/* first element */			\
}

#define	LIST_HEAD_INITIALIZER(head)					\
	{ NULL }

#define	LIST_ENTRY(type)							\
struct {											\
struct type *le_next;	/* next element */			\
struct type **le_prev;	/* address of previous next element */	\
}

#define	LIST_EMPTY(head)	((head)->lh_first == NULL)

#define	LIST_FIRST(head)	((head)->lh_first)

#define	LIST_FOREACH(var, head, field)					\
	for ((var) = LIST_FIRST((head));					\
	(var);												\
	(var) = LIST_NEXT((var), field))

#define	LIST_INIT(head) do {							\
	LIST_FIRST((head)) = NULL;							\
} while (0)

#define	LIST_INSERT_AFTER(listelm, elm, field) do {		\
	if ((LIST_NEXT((elm), field) = LIST_NEXT((listelm), field)) != NULL)\
	LIST_NEXT((listelm), field)->field.le_prev =		\
	&LIST_NEXT((elm), field);							\
	LIST_NEXT((listelm), field) = (elm);				\
	(elm)->field.le_prev = &LIST_NEXT((listelm), field);\
} while (0)

#define	LIST_INSERT_BEFORE(listelm, elm, field) do {	\
	(elm)->field.le_prev = (listelm)->field.le_prev;	\
	LIST_NEXT((elm), field) = (listelm);				\
	*(listelm)->field.le_prev = (elm);					\
	(listelm)->field.le_prev = &LIST_NEXT((elm), field);\
} while (0)

#define	LIST_INSERT_HEAD(head, elm, field) do {						\
	if ((LIST_NEXT((elm), field) = LIST_FIRST((head))) != NULL)		\
	LIST_FIRST((head))->field.le_prev = &LIST_NEXT((elm), field);	\
	LIST_FIRST((head)) = (elm);										\
	(elm)->field.le_prev = &LIST_FIRST((head));						\
} while (0)

#define	LIST_NEXT(elm, field)	((elm)->field.le_next)

#define	LIST_REMOVE(elm, field) do {					\
	if (LIST_NEXT((elm), field) != NULL)				\
	LIST_NEXT((elm), field)->field.le_prev =			\
	(elm)->field.le_prev;								\
	*(elm)->field.le_prev = LIST_NEXT((elm), field);	\
} while (0)

/*tail queue*/
#define	TAILQ_HEAD(name, type)							\
struct name {											\
	struct type *tqh_first;	/* first element */			\
	struct type **tqh_last;	/* addr of last next element */		\
	TRACEBUF											\
}

#define	TAILQ_HEAD_INITIALIZER(head)					\
	{ NULL, &(head).tqh_first }

#define	TAILQ_ENTRY(type)								\
struct {												\
	struct type *tqe_next;	/* next element */			\
	struct type **tqe_prev;	/* address of previous next element */	\
	TRACEBUF											\
}

/*
 * Tail queue functions.
 */
#define	TAILQ_CONCAT(head1, head2, field) do {			\
	if (!TAILQ_EMPTY(head2)) {							\
		*(head1)->tqh_last = (head2)->tqh_first;		\
		(head2)->tqh_first->field.tqe_prev = (head1)->tqh_last;	\
		(head1)->tqh_last = (head2)->tqh_last;			\
		TAILQ_INIT((head2));							\
		QMD_TRACE_HEAD(head);							\
		QMD_TRACE_HEAD(head2);							\
	}													\
} while (0)

#define	TAILQ_EMPTY(head)	((head)->tqh_first == NULL)

#define	TAILQ_FIRST(head)	((head)->tqh_first)

#define	TAILQ_FOREACH(var, head, field)					\
	for ((var) = TAILQ_FIRST((head));					\
	    (var);											\
	    (var) = TAILQ_NEXT((var), field))

#define	TAILQ_FOREACH_REVERSE(var, head, headname, field)		\
	for ((var) = TAILQ_LAST((head), headname);			\
	    (var);											\
	    (var) = TAILQ_PREV((var), headname, field))

#define	TAILQ_INIT(head) do {							\
	TAILQ_FIRST((head)) = NULL;							\
	(head)->tqh_last = &TAILQ_FIRST((head));			\
	QMD_TRACE_HEAD(head);								\
} while (0)

#define	TAILQ_INSERT_AFTER(head, listelm, elm, field) do {		\
	if ((TAILQ_NEXT((elm), field) = TAILQ_NEXT((listelm), field)) != NULL)\
		TAILQ_NEXT((elm), field)->field.tqe_prev =		\
		    &TAILQ_NEXT((elm), field);					\
	else {												\
		(head)->tqh_last = &TAILQ_NEXT((elm), field);	\
		QMD_TRACE_HEAD(head);							\
	}													\
	TAILQ_NEXT((listelm), field) = (elm);				\
	(elm)->field.tqe_prev = &TAILQ_NEXT((listelm), field);		\
	QMD_TRACE_ELEM(&(elm)->field);						\
	QMD_TRACE_ELEM(&listelm->field);					\
} while (0)

#define	TAILQ_INSERT_BEFORE(listelm, elm, field) do {		\
	(elm)->field.tqe_prev = (listelm)->field.tqe_prev;		\
	TAILQ_NEXT((elm), field) = (listelm);					\
	*(listelm)->field.tqe_prev = (elm);						\
	(listelm)->field.tqe_prev = &TAILQ_NEXT((elm), field);	\
	QMD_TRACE_ELEM(&(elm)->field);							\
	QMD_TRACE_ELEM(&listelm->field);						\
} while (0)

#define	TAILQ_INSERT_HEAD(head, elm, field) do {			\
	if ((TAILQ_NEXT((elm), field) = TAILQ_FIRST((head))) != NULL)	\
		TAILQ_FIRST((head))->field.tqe_prev =				\
		    &TAILQ_NEXT((elm), field);						\
	else													\
		(head)->tqh_last = &TAILQ_NEXT((elm), field);		\
	TAILQ_FIRST((head)) = (elm);							\
	(elm)->field.tqe_prev = &TAILQ_FIRST((head));			\
	QMD_TRACE_HEAD(head);									\
	QMD_TRACE_ELEM(&(elm)->field);							\
} while (0)

#define	TAILQ_INSERT_TAIL(head, elm, field) do {			\
	TAILQ_NEXT((elm), field) = NULL;						\
	(elm)->field.tqe_prev = (head)->tqh_last;				\
	*(head)->tqh_last = (elm);								\
	(head)->tqh_last = &TAILQ_NEXT((elm), field);			\
	QMD_TRACE_HEAD(head);									\
	QMD_TRACE_ELEM(&(elm)->field);							\
} while (0)

#define	TAILQ_LAST(head, headname)							\
	(*(((struct headname *)((head)->tqh_last))->tqh_last))

#define	TAILQ_NEXT(elm, field) ((elm)->field.tqe_next)

#define	TAILQ_PREV(elm, headname, field)					\
	(*(((struct headname *)((elm)->field.tqe_prev))->tqh_last))

#define	TAILQ_REMOVE(head, elm, field) do {					\
	if ((TAILQ_NEXT((elm), field)) != NULL)					\
		TAILQ_NEXT((elm), field)->field.tqe_prev =			\
		    (elm)->field.tqe_prev;							\
	else {													\
		(head)->tqh_last = (elm)->field.tqe_prev;			\
		QMD_TRACE_HEAD(head);								\
	}														\
	*(elm)->field.tqe_prev = TAILQ_NEXT((elm), field);		\
	TRASHIT((elm)->field.tqe_next);							\
	TRASHIT((elm)->field.tqe_prev);							\
	QMD_TRACE_ELEM(&(elm)->field);							\
} while (0)

#ifndef defined(__cplusplus)
};
#endif

#endif
