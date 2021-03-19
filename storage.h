#ifndef __STORAGE_H__
#define __STORAGE_H__

#include "postgres.h"
#include "access/htup.h"
#include "storage/bufpage.h"

#include "compression.h"


#define STORAGE_VERSION     1
#define CRYO_META_PAGE      0

/*
 * XXX probably it makes sense to make page size configurable for each table
 * but that would require more complex solution for cache
 */
#define CRYO_BLCKSZ (1 << 20)   /* 1Mb */

/*
 * PageHeaderData clone with only difference that it doesn't have flexible
 * array member `pd_linp` which allows to add it as a member to other structs.
 * It has the same length and the same structure as PageHeaderData so they are
 * mutually convertable.
 */
typedef struct
{
    PageXLogRecPtr  pd_lsn;
    uint16          pd_checksum;
    uint16          pd_flags;
    LocationIndex   pd_lower;
    LocationIndex   pd_upper;
    LocationIndex   pd_special;
    uint16          pd_pagesize_version;
    TransactionId   pd_prune_xid;
} PageHeaderClone;

typedef struct
{
    PageHeaderClone base;           /* to keep PageIsVerified quiet */
    uint16          version;        /* storage version */
    uint64          ntuples;        /* total number of tuples in relation */
    int8_t          page_mask[0];   /* non-empty pages bitmap */
} CryoMetaPage;

#define PAGE_MASK_SIZE (BLCKSZ - MAXALIGN(sizeof(PageHeaderClone)))

/*
 * Compressed cryo page maps to several postgres blocks. Each block has
 * a simple header containing block number relative to the starting one.
 */
typedef struct
{
    PageHeaderClone base;           /* we don't use it, but it is required by
                                     * GenericXLogFinish() */
    BlockNumber     first;
    BlockNumber     next;
} CryoPageHeader;

/*
 * First cryo page also contains additional metadata on compressed cryo block.
 */
typedef struct
{
    CryoPageHeader  cryo_base;
    TransactionId   created_xid;    /* transaction performed insertion */
    CompressionMethod compression_method;
    uint32          compressed_size;
    uint16          npages;         /* number of pages for this cryo block */
} CryoFirstPageHeader;

#define CryoPageHeaderSize(page, block) \
    ((page)->first == block ? sizeof(CryoFirstPageHeader) : sizeof(CryoPageHeader))

/* */
typedef struct
{
    uint32  off;
    uint32  len;
} CryoItemId;

typedef struct
{
    uint32  lower;
    uint32  upper;
    char    data[];
} CryoDataHeader;

#define CryoDataHeaderSize offsetof(CryoDataHeader, data)


void cryo_init_page(CryoDataHeader *hdr);
int cryo_storage_insert(CryoDataHeader *d, HeapTuple tuple);
HeapTuple cryo_storage_fetch(CryoDataHeader *d, int pos, HeapTuple tuple);


#endif /* __STORAGE_H__ */
