#include "storage.h"


void
cryo_init_page(CryoDataHeader *hdr)
{
    memset(hdr, 0, CRYO_BLCKSZ);
    hdr->lower = CryoDataHeaderSize;
    hdr->upper = CRYO_BLCKSZ;
}

bool
cryo_storage_insert(CryoDataHeader *d, HeapTuple tuple)
{
    CryoItemId  itemId;

    /* check there is enough space */
    if ((tuple->t_len + sizeof(ItemId)) > (d->upper - d->lower))
    {
        /* not enough space */
        return false;
    }

    /* insert tuple */
    d->upper -= MAXALIGN(tuple->t_len);
    memcpy((char *) d + d->upper, tuple->t_data, tuple->t_len);

    /* insert item id pointing to the tuple */
    itemId.off = d->upper;
    itemId.len = tuple->t_len;
    memcpy((char *) d + d->lower, &itemId, sizeof(ItemId));
    d->lower += sizeof(ItemId);

    return true;
}
