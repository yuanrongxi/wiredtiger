/************************************************************************
*bloom过滤器的实现，为了加快查sstable找而设计的
************************************************************************/
#include <stdint.h>

struct __wt_bloom
{
	const char*			uri;
	char*				config;			/*bloom配置项字符串*/
	uint8_t*			bitstring;		/*bloom bit map*/
	WT_SESSION_IMPL*	session;
	WT_CURSOR*			c;				/**/
	
	uint32_t			k;				/*hash定位的次数*/
	uint32_t			factor;			/*每个item(可以认为字节)占用的bit数*/
	uint64_t			m;				/*bloom slots总的bit数*/
	uint64_t			n;				/*bloom slots总的item数*/
};

struct __wt_bloom_hash
{
	uint64_t h1;
	uint64_t h2;
};

