#ifndef __STORAGE_H__
#define __STORAGE_H__

#include "postgres.h"
#include "access/htup.h"
#include "storage/bufpage.h"

#include "compression.h"


#define STORAGE_VERSION     1

/*
 * XXX probably it makes sense to make page size configurable for each table
 * but that would require more complex solution for cache
 */
#define CRYO_BLCKSZ (1 << 20)   /* 1Mb */

typedef struct
{
    PageHeaderData  base;           /* to keep PageIsVerified quiet */
    uint16          version;        /* storage version */
    uint32          target_block;   /* the last block we inserted to;
                                     * zero if we haven't yet */
    uint64          ntuples;        /* total number of tuples in relation */
} CryoMetaPage;

/*
 * Compressed cryo page maps to several postgres blocks. Each block has
 * a simple header containing block number relative to the starting one.
 */
typedef struct
{
    PageHeaderData  base;           /* we don't use it, but it is required by
                                     * GenericXLogFinish() */
    //uint16          curpage;
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
