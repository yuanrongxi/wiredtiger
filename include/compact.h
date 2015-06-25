/***************************************************************************
*LSM TREE compact信息定义
***************************************************************************/

struct __wt_compact 
{
	uint32_t	lsm_count;	/* Number of LSM trees seen */
	uint32_t	file_count;	/* Number of files seen */
	uint64_t	max_time;	/* Configured timeout */
};

