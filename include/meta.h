/**************************************************************************
* 定义元数据的一些meta信息宏和checkpoint信息结构
**************************************************************************/

#define	WT_WIREDTIGER		"WiredTiger"				/* Version file */
#define	WT_SINGLETHREAD		"WiredTiger.lock"			/* Locking file */

#define	WT_BASECONFIG		"WiredTiger.basecfg"		/* Base configuration */
#define	WT_BASECONFIG_SET	"WiredTiger.basecfg.set"	/* Base config temp */

#define	WT_USERCONFIG		"WiredTiger.config"			/* User configuration */

#define	WT_METADATA_BACKUP	"WiredTiger.backup"			/* Hot backup file */
#define	WT_INCREMENTAL_BACKUP	"WiredTiger.ibackup"	/* Incremental backup */

#define	WT_METADATA_TURTLE	"WiredTiger.turtle"			/* Metadata metadata */
#define	WT_METADATA_TURTLE_SET	"WiredTiger.turtle.set"	/* Turtle temp file */

#define	WT_METADATA_URI		"metadata:"					/* Metadata alias */
#define	WT_METAFILE_URI		"file:WiredTiger.wt"		/* Metadata file URI */

#define	WT_METAFILE_NAME_HASH	1045034099109282882LLU	/* Metadata file hash */

#define	WT_IS_METADATA(dh)									\
	((dh)->name_hash == WT_METAFILE_NAME_HASH && strcmp((dh)->name, WT_METAFILE_URI) == 0)

#define	WT_METAFILE_ID		0			/* Metadata file ID */

#define	WT_METADATA_VERSION	"WiredTiger version"		/* Version keys */
#define	WT_METADATA_VERSION_STR	"WiredTiger version string"

#define	WT_CHECKPOINT		"WiredTigerCheckpoint"
#define	WT_CKPT_FOREACH(ckptbase, ckpt)						\
	for ((ckpt) = (ckptbase); (ckpt)->name != NULL; ++(ckpt))


#define	WT_CKPT_ADD		0x01			/* Checkpoint to be added */
#define	WT_CKPT_DELETE	0x02			/* Checkpoint to be deleted */
#define	WT_CKPT_FAKE	0x04			/* Checkpoint is a fake */
#define	WT_CKPT_UPDATE	0x08			/* Checkpoint requires update */

struct __wt_ckpt
{
	char*			name;				/*名称字符*/
	WT_ITEM			addr;				/*检查点的addr二进制数据，分别打包了root_off/root_size/root_checksum*/
	WT_ITEM			raw;				/*一个完整的checkpoint addr信息*/
	int64_t			order;				/**/
	uintmax_t		sec;				/*时间戳*/

	uint64_t		ckpt_size;			/**/
	uint64_t		write_gen;			/**/

	void*			bpriv;				/*一个WT_BLOCK_CKPT结构指针，连有详细的checkpiont信息*/
	uint32_t		flags;				/*checkpoint的状态标识*/
};

