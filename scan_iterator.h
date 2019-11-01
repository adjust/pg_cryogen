#include "postgres.h"
#include "storage/block.h"


typedef struct SeqScanIterator SeqScanIterator;

SeqScanIterator *cryo_seqscan_iter_create(void);
BlockNumber cryo_seqscan_iter_next(SeqScanIterator *iter);
void cryo_seqscan_iter_exclude(SeqScanIterator *iter, BlockNumber block, bool miss_ok);

