#ifndef __CACHE_H__
#define __CACHE_H__

#include "postgres.h"
#include "storage/block.h"
#include "utils/relcache.h"

#include "scan_iterator.h"


#define InvalidCacheEntry -1

typedef enum
{
    CRYO_ERR_SUCCESS = 0,
    CRYO_ERR_DECOMPRESSION_FAILED,
    CRYO_ERR_WRONG_STARTING_BLOCK,
    CRYO_ERR_CACHE_IS_FULL
} CryoError;

/* Position in cache */
typedef int CacheEntry;

void cryo_init_cache(void);
CryoError cryo_read_data(Relation rel, SeqScanIterator *iter, BlockNumber block,
                         CacheEntry *result);
CacheEntry cryo_cache_allocate(Relation rel, BlockNumber blockno);
void cryo_cache_release(CacheEntry entry);

uint32 cryo_cache_get_pg_nblocks(CacheEntry entry);
char *cryo_cache_get_data(CacheEntry entry);
TransactionId cryo_cache_get_xid(CacheEntry entry);
char *cryo_cache_err(CryoError err);


#endif /* __CACHE_H__ */
