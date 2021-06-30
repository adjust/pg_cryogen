#ifndef __PURGE_H__
#define __PURGE_H__

#include "storage/itemptr.h"


typedef struct TIDList
{
	int			max_tuples;		/* # slots allocated in array */
	int			num_tuples;		/* current # of entries */
	ItemPointerData itemptrs[FLEXIBLE_ARRAY_MEMBER];
} TIDList;

TIDList *tidlist_alloc(BlockNumber relblocks);
void tidlist_record_item(TIDList *tuples, BlockNumber blockno,
                         OffsetNumber offset);
void vacuum_indexes(Relation *irels, IndexBulkDeleteResult **stats,
                    int nindexes, TIDList *tuples);
void drop_blocks(Relation rel, List *blocks);

#endif
