#include <sys/time.h>

#include "cache.h"
#include "compression.h"
#include "storage.h"

#include "access/visibilitymap.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/rel.h"


/* TODO: make it configurable via GUC variable */
#define CACHE_SIZE 16

/* TODO: concatenate headers and put this definition in it */
#define MIN(a, b) ((a) < (b) ? (a) : (b))

typedef struct
{
    Oid         relid;
    BlockNumber blockno;
} PageId;

typedef struct
{
    PageId      key;
    CacheEntry  entry;
} PageIdCacheEntry;

/* Timestamp */
typedef unsigned long long TS;

typedef struct
{
    PageId      key;
    TS          ts;         /* timestamp for LRU */
    uint32      nblocks;    /* number of postgres blocks */
    TransactionId xid;      /* transaction created block */
    char        data[CRYO_BLCKSZ];
} CacheEntryHeader;

/* Cache itself */
CacheEntryHeader cache[CACHE_SIZE];

/* Hashtable for quick cache lookups by (relid, blockno) key */
HTAB *pagemap;

void
cryo_init_cache(void)
{
    HASHCTL     hash_ctl;

    /* Initialize hash tables used to track TIDs */
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(PageId);
    hash_ctl.entrysize = sizeof(PageIdCacheEntry);
    hash_ctl.hcxt = CacheMemoryContext;

    pagemap =
        hash_create("pg_cryogen cache",
                    CACHE_SIZE,
                    &hash_ctl,
                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Return current UNIX time in microseconds
 */
static TS
get_current_timestamp_ms(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);

    return (TS)(tv.tv_sec) * 1000 +
           (TS)(tv.tv_usec) / 1000;
}

/*
 * Read and reassemble compressed data from the buffer manager starting
 * with `block` and decompress it.
 *
 * Possible return values:
 *      CRYO_ERR_SUCCESS: success;
 *      CRYO_ERR_WRONG_STARTING_BLOCK: specified blockno is not a starting
 *          block of cryo page;
 *      CRYO_ERR_DECOMPRESSION_FAILED: decompression failure.
 *
 * XXX Move visibility check here to avoid unnecessary i/o.
 */
static CryoError
cryo_read_decompress(Relation rel, BlockNumber block, CacheEntryHeader *entry)
{
    Buffer      buf;
    Buffer      vmbuf = InvalidBuffer;
    CryoPageHeader *page;
    char       *compressed, *p;
    Size        compressed_size, size;
    uint8       vmflags;

    buf = ReadBuffer(rel, block);
    page = (CryoPageHeader *) BufferGetPage(buf);

    /*
     * In case of brin index bitmap scan can try to read data not from the
     * first page.
     */
    if (page->curpage != 0)
    {
        ReleaseBuffer(buf);
        return CRYO_ERR_WRONG_STARTING_BLOCK;
    }

    size = compressed_size = ((CryoFirstPageHeader *) page)->compressed_size;
    p = compressed = palloc(compressed_size);
    entry->nblocks = 1;

    /*
     * Check whether page is frozen.
     *
     * XXX We don't actually write FrozenTransactionId into page header as this
     * would require entire page copy to WAL (due to Generic WAL), and VACUUM
     * would be very write heavy. Instead we just write frozen bit into
     * visibility map.
     */
    vmflags = visibilitymap_get_status(rel, block, &vmbuf);
    entry->xid = (vmflags & VISIBILITYMAP_ALL_FROZEN) ?
        FrozenTransactionId : ((CryoFirstPageHeader *) page)->created_xid;
    if (BufferIsValid(vmbuf))
        ReleaseBuffer(vmbuf);

    while (true)
    {
        Size    content_size = BLCKSZ - CryoPageHeaderSize(page);
        Size    l = MIN(content_size, size);
        char   *page_content = ((char *) page) + CryoPageHeaderSize(page);

        memcpy(p, page_content, l);
        p += l;
        size -= l;
        ReleaseBuffer(buf);

        if (size == 0)
            break;

        /* read the next block */
        buf = ReadBuffer(rel, ++block);
        page = (CryoPageHeader *) BufferGetPage(buf);
        entry->nblocks++;
    }

    if (!cryo_decompress(COMP_LZ4, compressed, compressed_size, entry->data))
        return CRYO_ERR_DECOMPRESSION_FAILED;

    return CRYO_ERR_SUCCESS;
}

CryoError
cryo_read_data(Relation rel, BlockNumber blockno, CacheEntry *result)
{
    bool    found;
    PageId  pageId = {
        .relid = RelationGetRelid(rel),
        .blockno = blockno
    };
    PageIdCacheEntry *item;

     /* TODO: do not rely on RelationGetNumberOfBlocks; refer to metapage */
    if (RelationGetNumberOfBlocks(rel) <= blockno)
    {
        *result = InvalidCacheEntry;
        return CRYO_ERR_WRONG_STARTING_BLOCK;
    }

    /* check with hashtable */
    item = hash_search(pagemap, &pageId, HASH_FIND, &found);

    if (!found || item->entry == InvalidCacheEntry)
    {
        int i;
        CacheEntry  new_entry = InvalidCacheEntry;
        CryoError   err;
        TS          min_ts = -1; /* max unsigned long long */
        int         min_ts_pos;

        /* find available spot in cache or evict old cache entry */
        for (i = 0; i < CACHE_SIZE; ++i)
        {
            if (cache[i].ts == 0)
            {
                new_entry = i;
                break;
            }

            if (cache[i].ts < min_ts)
                min_ts_pos = i;
        }

        if (new_entry == InvalidCacheEntry)
        {
            /*
             * We didn't manage to find uninitialized cache entries. But we
             * found least recently used one. We also need to remove the old
             * record from pagemap.
             */
            elog(DEBUG1,
                 "pg_cryogen: evicted cache entry for (%i, %i)",
                 cache[min_ts_pos].key.relid,
                 cache[min_ts_pos].key.blockno);
            hash_search(pagemap, &cache[min_ts_pos].key, HASH_REMOVE, NULL);
            new_entry = min_ts_pos;
        }

        /* cache entry is not found, load data from disk */
        item = hash_search(pagemap, &pageId, HASH_ENTER, NULL);
        item->key.relid = RelationGetRelid(rel);
        item->key.blockno = blockno;
        item->entry = new_entry;

        /* load cryo block */
        cache[item->entry].ts = get_current_timestamp_ms();
        cache[item->entry].key = item->key;
        err = cryo_read_decompress(rel, blockno, &cache[new_entry]);
        if (err != CRYO_ERR_SUCCESS)
        {
            *result = item->entry = InvalidCacheEntry;
            return err;
        }
    }

    *result = item->entry;
    return CRYO_ERR_SUCCESS;
}

uint32
cryo_cache_get_pg_nblocks(CacheEntry entry)
{
    return cache[entry].nblocks;
}

char *
cryo_cache_get_data(CacheEntry entry)
{
    CacheEntryHeader *header = &cache[entry];

    /* TODO: check that entry is valid */

    /* update usage timestamp for LRU */
    header->ts = get_current_timestamp_ms();

    return header->data;
}

TransactionId
cryo_cache_get_xid(CacheEntry entry)
{
    return cache[entry].xid;
}

char *
cryo_cache_err(CryoError err)
{
    switch (err)
    {
        case CRYO_ERR_SUCCESS:
            return "success";
        case CRYO_ERR_WRONG_STARTING_BLOCK:
            return "wrong starting block number";
        case CRYO_ERR_DECOMPRESSION_FAILED:
            return "decompression failed";
        default:
            return "unknown error";
    }
}

