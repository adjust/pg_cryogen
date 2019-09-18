#ifndef __CACHE_H__
#define __CACHE_H__

#include "postgres.h"
#include "storage/block.h"
#include "utils/relcache.h"

typedef enum
{
    CRYO_ERR_SUCCESS = 0,
    CRYO_ERR_DECOMPRESSION_FAILED,
    CRYO_ERR_WRONG_STARTING_BLOCK
} CryoError;

/* Position in cache */
typedef int CacheEntry;

void cryo_init_cache(void);
CryoError cryo_read_block(Relation rel, BlockNumber block, CacheEntry *result);
uint32 cryo_cache_get_pg_nblocks(CacheEntry entry);
char *cryo_cache_get_data(CacheEntry entry);
char *cryo_cache_err(CryoError err);


#endif /* __CACHE_H__ */
