#ifndef SCAN_ITERATOR_H
#define SCAN_ITERATOR_H

/*
 * We store block number into ItemPointers during insert_tuple() but actual
 * write to disk occures later (by the end of transaction in some cases). We do
 * not want another process writing to the same block before us. What we do
 * is allocate a single block and its number would serve as an identification
 * for cryo block and be used in ItemPointers. The rest of the blocks may be
 * stored separately (e.g. another concurrent writing transaction allocates its
 * block it between). This cause blocks being stored separately one from
 * another. Therefore we need more sofisticated way to read them from disk and
 * prevent reading blocks that we have already read. To deal with that we use
 * SeqScanIterator which keeps track of blocks that we have read and serves
 * starting BlockNumber for next block to read.
 */

#include "postgres.h"
#include "nodes/pg_list.h"

#include "scan_iterator.h"


typedef struct
{
    BlockNumber start;
    BlockNumber end;
} BlockRange;

struct SeqScanIterator
{
    List       *ranges;
};

static void
add_range(SeqScanIterator *iter, BlockNumber start, BlockNumber end)
{
    BlockRange *r = palloc(sizeof(BlockRange));

    r->start = start;
    r->end = end;
    iter->ranges = lappend(iter->ranges, r);
}

SeqScanIterator *
cryo_seqscan_iter_create(void)
{
    SeqScanIterator *iter = palloc(sizeof(SeqScanIterator));

    iter->ranges = NIL;
    add_range(iter, 1, InvalidBlockNumber);
    return iter;
}

BlockNumber
cryo_seqscan_iter_next(SeqScanIterator *iter)
{
    BlockRange *r;
    BlockNumber res;

    Assert(iter->ranges);
    Assert(list_length(iter->ranges) > 0);

    /* take the first element of the first range */
    r = linitial(iter->ranges);
    res = r->start++;

    /*
     * Remove first block from iterator
     *
     * XXX probably let the client code to call cryo_seqscan_iter_exclude()
     * explicitly
     */
    if (r->start > r->end)
        list_delete_ptr(iter->ranges, r);

    return res;
}

void
cryo_seqscan_iter_exclude(SeqScanIterator *iter, BlockNumber block, bool miss_ok)
{
    ListCell   *lc;

    if (!iter)
        return;

    foreach (lc, iter->ranges)
    {
        BlockRange *r = lfirst(lc);

        if (block >= r->start && block <= r->end)
        {
            if (block == r->start)
                r->start++;
            else if (block == r->end)
                r->end--;
            else
            {
                /* split range */
                BlockRange *new_r = palloc(sizeof(BlockRange));

                new_r->start = block + 1;
                new_r->end = r->end;
                r->end = block - 1;
                lappend_cell(iter->ranges, lc, new_r);
                return;
            }

            if (r->start > r->end)
                list_delete_ptr(iter->ranges, r);
            return;
        }
    }

    if (!miss_ok)
        elog(ERROR,
             "pg_cryogen: iternal error; block %u is not the part of seqscan iterator",
             block);
}

#endif  /* SCAN_ITERATOR_H */
