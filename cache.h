#ifndef __CACHE_H__
#define __CACHE_H__


#include "postgres.h"
#include "storage/block.h"
#include "utils/relcache.h"

/* Position in cache */
typedef int CacheEntry;

void cryo_init_cache(void);
CacheEntry cryo_read_block(Relation rel, BlockNumber block);
uint32 cryo_cache_get_pg_nblocks(CacheEntry entry);
char *cryo_cache_get_data(CacheEntry entry);


#endif /* __CACHE_H__ */
