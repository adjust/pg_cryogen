#include "storage.h"
#include "access/htup_details.h"


/*
 * From tidbitmap.c; currently we can't store more than this number of tuples
 * if we want to use bitmapscan. Change it to INT_MAX if you don't need
 * bitmapscan.
 */
#define MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage

#define CryoPageLastItemPos(hdr) \
    (((d)->lower - CryoDataHeaderSize) / sizeof(CryoItemId))

void
cryo_init_page(CryoDataHeader *hdr)
{
    memset(hdr, 0, CRYO_BLCKSZ);
    hdr->lower = CryoDataHeaderSize;
    hdr->upper = CRYO_BLCKSZ;
}

/*
 * Insert tuple into storage. Returns item position.
 */
int
cryo_storage_insert(CryoDataHeader *d, HeapTuple tuple)
{
    CryoItemId  itemId;

    /* check there is enough space */
    if ((tuple->t_len + sizeof(ItemId)) > (d->upper - d->lower)
        || CryoPageLastItemPos(d) + 1 >= MAX_TUPLES_PER_PAGE)
    {
        /* not enough space */
        return -1;
    }

    /* insert tuple */
    d->upper -= MAXALIGN(tuple->t_len);
    memcpy((char *) d + d->upper, tuple->t_data, tuple->t_len);

    /* insert item id pointing to the tuple */
    itemId.off = d->upper;
    itemId.len = tuple->t_len;
    memcpy((char *) d + d->lower, &itemId, sizeof(ItemId));
    d->lower += sizeof(ItemId);

    return CryoPageLastItemPos(d);
}

/*
 * Allocate and return a tuple in the specified position.
 */
HeapTuple
cryo_storage_fetch(CryoDataHeader *d, int pos, HeapTuple tuple)
{
    CryoItemId *itemId;
    //HeapTuple tuple = palloc0(sizeof(HeapTuple));

    /* input pos is 1 based */
    itemId = (CryoItemId *) d->data + pos - 1;
    Assert((char *) itemId < (char *) d + d->lower);  /* check boundaries */

    tuple->t_data = (HeapTupleHeader) ((char *) d + itemId->off);
    tuple->t_len = itemId->len;

    return tuple;
}
