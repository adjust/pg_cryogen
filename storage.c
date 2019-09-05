#include "storage.h"


void
cryo_init_page(CryoDataHeader *hdr)
{
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
    memcpy(d->data + d->upper, tuple->t_data, tuple->t_len);

    /* insert item id pointing to the tuple */
    itemId.off = d->upper;
    itemId.len = tuple->t_len;
    memcpy(d->data + d->lower, &itemId, sizeof(ItemId));
    d->lower += MAXALIGN(sizeof(ItemId));

    return true;
}
