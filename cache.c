#include <sys/time.h>

#include "cache.h"
#include "storage.h"

#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/rel.h"

#include "lz4.h"


/* TODO: make it configurable via GUC variable */
#define CACHE_SIZE 16

#define InvalidCacheEntry -1

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
 * Decompress and store result in `out`
 */
static void
cryo_decompress(const char *compressed, Size compressed_size, char *out)
{
    int bytes;

    bytes = LZ4_decompress_safe(compressed, out, compressed_size, CRYO_BLCKSZ);
    if (bytes < 0)
        elog(ERROR, "pg_cryogen: decompression failed");

    Assert(CRYO_BLCKSZ == bytes);
}

/*
 * Read and reassemble compressed data from the buffer manager starting
 * with `block` and decompress it.
 */
static bool
cryo_read_decompress(Relation rel, BlockNumber block, uint32 *nblocks, char *out)
{
    Buffer      buf;
    CryoPageHeader *page;
    char       *compressed, *p;
    Size        page_content_size = BLCKSZ - sizeof(CryoPageHeader);
    Size        compressed_size, size;

    /* TODO: do not rely on RelationGetNumberOfBlocks; refer to metapage */
    if (RelationGetNumberOfBlocks(rel) <= block)
    {
        cryo_init_page((CryoDataHeader *) out);
        *nblocks = 0;
        return true; /* TODO */
    }

    buf = ReadBuffer(rel, block);
    page = (CryoPageHeader *) BufferGetPage(buf);

    /*
     * In case of brin index bitmap scan can try to read data not from the
     * first page.
     */
    if (page->curpage != 0)
    {
        ReleaseBuffer(buf);
        return false;
    }

    size = compressed_size = page->compressed_size;
    p = compressed = palloc(compressed_size);
    if (nblocks)
        *nblocks = 1;

    while (true)
    {
        Size    l = MIN(page_content_size, size);
        char   *page_content = ((char *) page) + sizeof(CryoPageHeader);

        memcpy(p, page_content, l);
        p += l;
        size -= l;
        ReleaseBuffer(buf);

        if (size == 0)
            break;

        /* read the next block */
        buf = ReadBuffer(rel, ++block);
        page = (CryoPageHeader *) BufferGetPage(buf);

        if (nblocks)
            (*nblocks)++;
    }

    cryo_decompress(compressed, compressed_size, out);

    return true;
}

CacheEntry
cryo_read_block(Relation rel, BlockNumber blockno)
{
    bool    found;
    PageId  pageId = {
        .relid = RelationGetRelid(rel),
        .blockno = blockno
    };
    PageIdCacheEntry *item;

    /* check with hashtable */
    item = hash_search(pagemap, &pageId, HASH_FIND, &found);

    if (!found || item->entry == InvalidCacheEntry)
    {
        int i;
        CacheEntry new_entry;

        /* find available spot in cache or evict old cache entry */
        /* TODO: add LRU strategy */
        for (i = 0; i < CACHE_SIZE; ++i)
        {
            if (cache[i].ts == 0)
            {
                new_entry = i;
                break;
            }
        }

        /* cache entry is not found, load data from disk */
        item = hash_search(pagemap, &pageId, HASH_ENTER, NULL);
        item->key.relid = RelationGetRelid(rel);
        item->key.blockno = blockno;
        item->entry = new_entry;

        /* load cryo block */
        cache[item->entry].ts = get_current_timestamp_ms();
        /* TODO: do it with more elegance */
        PG_TRY();
        {
            cryo_read_decompress(rel, blockno, &cache[new_entry].nblocks,
                                 cache[new_entry].data);
        }
        PG_CATCH();
        {
            item->entry = InvalidCacheEntry;
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    return item->entry;
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

#if 0
void
cryo_release_block(CacheEntry entry)
{
    cache[entry].pinned--;
}
#endif

