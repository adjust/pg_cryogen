#ifndef __STORAGE_H__
#define __STORAGE_H__

#include "postgres.h"
#include "access/htup.h"
#include "storage/bufpage.h"


#define CRYO_BLCKSZ (1 << 20)   /* 1Mb */

typedef struct
{
    uint32  target_block;
} CryoMetaPage;

/*
 * Compressed cryo page maps to several postgres blocks. Each block has
 * a simple header containing starting block number and relative block number
 * relative to the starting one.
 */
typedef struct
{
    PageHeaderData base;    /* we don't use it, but it is required by
                             * GenericXLogFinish() */
    uint16  npages;         /* number of pages for this cryo block */
    uint16  curpage;
    uint32  compressed_size;
} CryoPageHeader;

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
bool cryo_storage_insert(CryoDataHeader *d, HeapTuple tuple);


#endif /* __STORAGE_H__ */
