#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "cache.h"
#include "compression.h"
#include "scan_iterator.h"
#include "storage.h"


PG_MODULE_MAGIC;

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define NOT_IMPLEMENTED \
    do { \
        elog(ERROR, "function \"%s\" is not implemented", __func__); \
    } while (0)
#define PageIsEmpty(page) \
    (((CryoPageHeader *) page)->first == 0 && ((CryoPageHeader *) page)->next == 0)


#if 0
typedef struct
{
    dsm_handle dsm_hndl;
} CryoSharedState;
#endif

typedef struct CryoScanDescData
{
    TableScanDescData   rs_base;
    uint32              nblocks;
    BlockNumber         cur_block;
    uint32              cur_item;
    SeqScanIterator    *iterator;
    CacheEntry          cacheEntry; /* cached decompressed page reference */
} CryoScanDescData;
typedef CryoScanDescData *CryoScanDesc;

typedef struct
{
    IndexFetchTableData base;
    CacheEntry          cacheEntry; /* cached decompressed page reference */
} IndexFetchCryoData;

typedef struct CryoModifyState
{
    Relation    relation;
    Buffer      target_buffer;
    BlockNumber target_block;
    uint32      tuples_inserted;
    CacheEntry  cacheEntry;         /* cached decompressed page reference */
    char       *data;               /* decompressed data */
} CryoModifyState;

CryoModifyState modifyState = {
    .tuples_inserted = 0,
    .relation = NULL
};

// CryoSharedState shared_state;

// static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
ExecutorFinish_hook_type ExecutorFinish_hook_prev = NULL;


PG_FUNCTION_INFO_V1(cryo_tableam_handler);
static Buffer cryo_load_meta(Relation rel, int lockmode);
static void cryo_preserve(CryoModifyState *state, bool advance);
// static BlockNumber cryo_reserve_blockno(Relation rel);
static Buffer cryo_reserve_blockno(Relation rel);
static Buffer cryo_allocate_block(Relation rel);
static void cryo_executor_finish_hook(QueryDesc *queryDesc);
void _PG_init(void);


#if 0
void
_PG_init(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = cryo_shmem_startup;
}

static void
cryo_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    pgss = ShmemInitStruct("pg_stat_statements",
                           sizeof(pgssSharedState),
                           &found);

    if (!found)
    {
        shared_state.dsm_hndl = init_free_space_cache();
    }
	LWLockRelease(AddinShmemInitLock);
}
#endif

static void
flush_modify_state(void)
{
    if (!RelationIsValid(modifyState.relation))
        return;

    if (modifyState.tuples_inserted)
        cryo_preserve(&modifyState, true);
    modifyState.tuples_inserted = 0;

    cryo_cache_release(modifyState.cacheEntry);

    modifyState.relation = NULL;
}

static void
init_modify_state(Relation rel)
{
    CryoDataHeader *hdr;

    /* init meta page */
    if (RelationGetNumberOfBlocks(rel) == 0)
    {
        Buffer          metabuf;

        metabuf = cryo_load_meta(rel, BUFFER_LOCK_SHARE);
        UnlockReleaseBuffer(metabuf);
    }

    /*
     * We need to allocate a single block which number will serve as an
     * identification for ItemPointers. Read comment in scan_iterator.c for
     * details.
     */
    modifyState.target_buffer = cryo_allocate_block(rel);
    modifyState.target_block = BufferGetBlockNumber(modifyState.target_buffer);
    modifyState.cacheEntry = cryo_cache_allocate(rel, modifyState.target_block);
    modifyState.data = cryo_cache_get_data(modifyState.cacheEntry);
    hdr = (CryoDataHeader *) modifyState.data;

    /* initialize modify state */
    if (modifyState.target_block == 0)
    {
        /* This is a first data block in the relation */

        cryo_init_page(hdr);
        modifyState.target_block = 1;
    }
    else
    {
        /*
         * In order to ensure that storage is  consistent we create entirely
         * new block for every multi_insert instead of adding tuples to an
         * existing one. This also makes visibility check much simpler and
         * faster: we just check transaction id in a block header.
         */
        cryo_init_page(hdr);
    }
    modifyState.relation = rel;
    modifyState.tuples_inserted = 0;
}

static void
cryo_executor_finish_hook(QueryDesc *queryDesc)
{
    elog(NOTICE, "cryo_executor_finish_hook");
    ExecutorFinish_hook_prev(queryDesc);

    (void) flush_modify_state();
}

static void
cryo_xact_callback(XactEvent event, void *arg)
{
#if 0
    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
            (void) flush_modify_state();
            break;

        case XACT_EVENT_ABORT:
            modifyState.relation = NULL;
            modifyState.tuples_inserted = 0;
            break;

        default:
            /* do nothing */
            ;
    }
#endif

    if (event == XACT_EVENT_ABORT)
    {
        modifyState.relation = NULL;
        modifyState.tuples_inserted = 0;
    }
}

static void
cryo_relcache_callback(Datum arg, Oid relid)
{
    cryo_cache_invalidate_relation(relid);
}

void
_PG_init(void)
{
    ExecutorFinish_hook_prev = ExecutorFinish_hook;
    ExecutorFinish_hook = cryo_executor_finish_hook;

    cryo_init_cache();
    cryo_define_compression_gucs();
    RegisterXactCallback(cryo_xact_callback, NULL);
    CacheRegisterRelcacheCallback(cryo_relcache_callback, (Datum) 0);
}

static const TupleTableSlotOps *
cryo_slot_callbacks(Relation relation)
{
//    return &TTSOpsBufferHeapTuple;
    return &TTSOpsHeapTuple;
}

static void
mark_page_nonempty(CryoMetaPage *page, BlockNumber blkno)
{
    /*
     * We subtract 1 because the first block is always dedicated for
     * meta page.
     */
    int byteno = (blkno - 1) / 8;
    int bit = (blkno - 1) % 8;

    Assert(!(page->page_mask[byteno] & (1 << bit)));

    page->page_mask[byteno] |= (1 << bit);
}

static TableScanDesc
cryo_beginscan(Relation relation, Snapshot snapshot,
               int nkeys, ScanKey key,
               ParallelTableScanDesc parallel_scan,
               uint32 flags)
{
    CryoScanDesc    scan;

	RelationIncrementReferenceCount(relation);

    /*
     * allocate and initialize scan descriptor
     */
    scan = (CryoScanDesc) palloc(sizeof(CryoScanDescData));

    scan->rs_base.rs_rd = relation;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_flags = flags;
    scan->cur_item = 0;
    scan->cur_block = 0;
    scan->nblocks = 0;
    scan->cacheEntry = InvalidCacheEntry; 
    scan->iterator = cryo_seqscan_iter_create();

    return (TableScanDesc) scan;
}

static bool
xid_is_visible(Snapshot snapshot, TransactionId xid)
{
    switch (snapshot->snapshot_type)
    {
        case SNAPSHOT_MVCC:
            if (TransactionIdIsCurrentTransactionId(xid))
                return true;
            else if (XidInMVCCSnapshot(xid, snapshot))
                return false;
            else if (TransactionIdDidCommit(xid))
                return true;
            else
            {
                /* it must have been aborted or crashed */
                return false;
            }
        case SNAPSHOT_ANY:
            return true;
        default:
            elog(ERROR,
                 "pg_cryogen: visibility check for snapshot type %d is not implemented",
                 snapshot->snapshot_type);
    }
}

static bool
cryo_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
    CryoScanDesc    scan = (CryoScanDesc) sscan;
    CryoDataHeader *hdr;
    Relation        rel = scan->rs_base.rs_rd;
    CryoError       err;

    ExecClearTuple(slot);

    /* TODO: handle direction */
    if (direction == BackwardScanDirection)
        elog(ERROR, "pg_cryogen: backward scan is not implemented");

read_block:
    if (scan->cur_item == 0)
    {
        scan->cur_block = cryo_seqscan_iter_next(scan->iterator);

        /*
         * TODO: probably read the number of block just once and store in the
         * scan state
         */
        if (RelationGetNumberOfBlocks(rel) <= scan->cur_block)
            return false;

        err = cryo_read_data(rel, scan->iterator, scan->cur_block,
                             &scan->cacheEntry);
        if (err != CRYO_ERR_SUCCESS)
        {
            if (err == CRYO_ERR_EMPTY_BLOCK || err == CRYO_ERR_WRONG_STARTING_BLOCK )
            {
                /* that's ok, just read the next block*/
                goto read_block;
            }
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("pg_cryogen: %s", cryo_cache_err(err)),
                     errhint("block number: %u", scan->cur_block)));
        }
        scan->nblocks = cryo_cache_get_pg_nblocks(scan->cacheEntry);
        scan->cur_item = 1;

        if (!xid_is_visible(sscan->rs_snapshot, cryo_cache_get_xid(scan->cacheEntry)))
        {
            /* 
             * Transaction created this block is not visible, proceed to the
             * next block
             */
            scan->cur_item = 0;
            goto read_block;
        }
    }

    hdr = (CryoDataHeader *) cryo_cache_get_data(scan->cacheEntry);
    if ((scan->cur_item) * sizeof(CryoItemId) < hdr->lower)
    {
        /* read tuple */
        HeapTuple tuple;

        tuple = palloc0(sizeof(HeapTupleData));
        cryo_storage_fetch(hdr, scan->cur_item, tuple);
        tuple->t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
        ItemPointerSet(&tuple->t_self, scan->cur_block, scan->cur_item);

        ExecStoreHeapTuple(tuple, slot, true);
        scan->cur_item++;

        return true;
    }
    else
    {
        scan->cur_item = 0;
        goto read_block;
    }

    return false;
}

static void
cryo_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    CryoScanDesc scan = (CryoScanDesc) sscan;

    scan->cur_item = 0;
    scan->cur_block = 0;
    scan->nblocks = 0;
    scan->cacheEntry = InvalidCacheEntry;
}

static void
cryo_endscan(TableScanDesc sscan)
{
    CryoScanDesc scan = (CryoScanDesc) sscan;

    /*
     * decrement relation reference count and free scan descriptor storage
     */
    RelationDecrementReferenceCount(scan->rs_base.rs_rd);

    /*
    if (scan->rs_base.rs_key)
        pfree(scan->rs_base.rs_key);
    */

    if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
        UnregisterSnapshot(scan->rs_base.rs_snapshot);

    pfree(scan);
}

static IndexFetchTableData *
cryo_index_fetch_begin(Relation rel)
{
	IndexFetchCryoData *cscan = palloc0(sizeof(IndexFetchCryoData));

	cscan->base.rel = rel;
	return &cscan->base;
}

static void
cryo_index_fetch_reset(IndexFetchTableData *scan)
{
    /* nothing to reset yet */
}

static void
cryo_index_fetch_end(IndexFetchTableData *scan)
{
    pfree(scan);
}

/*
 * Currently it's a super inefficient implementation that reads and
 * decompresses entire block for every iteration. Need to add a buffer cache
 * for cryo pages.
 */
static bool
cryo_index_fetch_tuple(struct IndexFetchTableData *scan,
                       ItemPointer tid,
                       Snapshot snapshot,
                       TupleTableSlot *slot,
                       bool *call_again, bool *all_dead)
{
    IndexFetchCryoData *cscan = (IndexFetchCryoData *) scan;
    CryoDataHeader     *hdr;
    HeapTuple           tuple;
    CryoError           err;

    err = cryo_read_data(cscan->base.rel,
                         NULL, /* TODO */
                         ItemPointerGetBlockNumber(tid),
                         &cscan->cacheEntry);

    if (err != CRYO_ERR_SUCCESS)
        elog(ERROR, "pg_cryogen: %s", cryo_cache_err(err));

    if (!xid_is_visible(snapshot, cryo_cache_get_xid(cscan->cacheEntry)))
        return false;

    hdr = (CryoDataHeader *) cryo_cache_get_data(cscan->cacheEntry);

    tuple = palloc0(sizeof(HeapTupleData));
    tuple->t_tableOid = RelationGetRelid(cscan->base.rel);
    ItemPointerCopy(tid, &tuple->t_self);
    cryo_storage_fetch(hdr, ItemPointerGetOffsetNumber(tid), tuple);

    ExecStoreHeapTuple(tuple, slot, true);

    return true;
}

static bool
cryo_scan_bitmap_next_block(TableScanDesc scan,
                            struct TBMIterateResult *tbmres)
{
    CryoScanDesc    cscan = (CryoScanDesc) scan;
    BlockNumber     blockno;
    CryoError       err;

    cscan->cur_block = InvalidBlockNumber;
    blockno = tbmres->blockno;

    err = cryo_read_data(cscan->rs_base.rs_rd, NULL, blockno,
                         &cscan->cacheEntry);
    switch (err)
    {
        case CRYO_ERR_SUCCESS:
            /* everything is fine, carry on */
            break;
        case CRYO_ERR_WRONG_STARTING_BLOCK:
            /*
             * This is a usual case for BRIN index. It just scans every blockno
             * in a range. Notify bitmapscan node that there are no tuples in
             * this block and proceed to the next one.
             */
            return false;
        default:
            /* some actual error */
            elog(ERROR, "pg_cryogen: %s", cryo_cache_err(err));
    }

    cscan->nblocks = cryo_cache_get_pg_nblocks(cscan->cacheEntry);
    if (cscan->nblocks == 0)
        return false;
    cscan->cur_block = blockno;

    /* lossy scan? */
    if (tbmres->ntuples < 0)
    {
        /* for lossy scan we interpret cur_item as a position in the block */
        cscan->cur_item = 1;
    }
    else
    {
        /*
         * ...and for non-lossy cur_item points to the position in
         * tbmres->offsets.
         */
        cscan->cur_item = 0;
    }

    return true;
}

static bool
cryo_scan_bitmap_next_tuple(TableScanDesc scan,
                            struct TBMIterateResult *tbmres,
                            TupleTableSlot *slot)
{
    CryoScanDesc    cscan = (CryoScanDesc) scan;
    CryoDataHeader *hdr;
    HeapTuple tuple;
    int     pos; 

    /* prevent reading in the middle of cryo blocks */
    if (tbmres->blockno != cscan->cur_block)
        return false;

    if (!xid_is_visible(scan->rs_snapshot, cryo_cache_get_xid(cscan->cacheEntry)))
        return false;

    hdr = (CryoDataHeader *) cryo_cache_get_data(cscan->cacheEntry);

    if (tbmres->ntuples >= 0)
    {
        if (cscan->cur_item >= tbmres->ntuples)
            return false;

        pos = tbmres->offsets[cscan->cur_item];
    }
    else
    {
        if (cscan->cur_item * sizeof(CryoItemId) >= hdr->lower)
            return false;

        pos = cscan->cur_item;
    }

    tuple = palloc0(sizeof(HeapTupleData));
    cryo_storage_fetch(hdr, pos, tuple);
    tuple->t_tableOid = RelationGetRelid(cscan->rs_base.rs_rd);
    ItemPointerSet(&tuple->t_self, cscan->cur_block, pos);

    ExecStoreHeapTuple(tuple, slot, true);

    cscan->cur_item++;

    return true;
}

static bool
cryo_fetch_row_version(Relation relation,
                         ItemPointer tid,
                         Snapshot snapshot,
                         TupleTableSlot *slot)
{
    NOT_IMPLEMENTED;
}

static bool
cryo_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    NOT_IMPLEMENTED;
}

static bool
cryo_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                Snapshot snapshot)
{
    NOT_IMPLEMENTED;
}

static Buffer
cryo_load_meta(Relation rel, int lockmode)
{
    Buffer          metabuf;
    CryoMetaPage   *metapage;

    if (RelationGetNumberOfBlocks(rel) == 0)
    {
        GenericXLogState *xlogState;

        /* This is a brand new relation. Initialize a metapage */
        LockRelationForExtension(rel, ExclusiveLock);

        /*
         * Check that while we where locking relation nobody else has created
         * metapage. Because that would be just terrible.
         */
        if (RelationGetNumberOfBlocks(rel) == 0)
        {
            xlogState = GenericXLogStart(rel);
            metabuf = ReadBuffer(rel, P_NEW);
            LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
            UnlockRelationForExtension(rel, ExclusiveLock);
            metapage = (CryoMetaPage *)
                GenericXLogRegisterBuffer(xlogState, metabuf,
                                          GENERIC_XLOG_FULL_IMAGE);

            /*
             * Can't leave pd_upper = 0 because then page will be considered new
             * (see PageIsNew) and won't pass PageIsVerified check
             */
            metapage->base.pd_upper = BLCKSZ;
            metapage->base.pd_lower = sizeof(CryoMetaPage);
            // metapage->base.pd_lower = BLCKSZ;
            metapage->base.pd_special = BLCKSZ;
            metapage->version = STORAGE_VERSION;

            /* No target block yet */
            PageSetChecksumInplace((Page) metapage, CRYO_META_PAGE);

            GenericXLogFinish(xlogState);
            UnlockReleaseBuffer(metabuf);
        }
        else
        {
            UnlockRelationForExtension(rel, ExclusiveLock);
        }
    }

    metabuf = ReadBuffer(rel, CRYO_META_PAGE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);
    LockBuffer(metabuf, lockmode);

    return metabuf;
}

/*
 * cryo_allocate_block
 *      Try to find an empty block or extend relation.
 */
static Buffer
cryo_allocate_block(Relation rel)
{
    size_t  sz = BLCKSZ - sizeof(CryoPageHeader);
    Buffer  buffer;
    BlockNumber block;

    /*
     * First try to find an empty existing block (e.g. from rolled back
     * transaction)
     */
    block = GetPageWithFreeSpace(rel, sz);
    for (;;)
    {
        Page        page;

        if (!BlockNumberIsValid(block))
            break;

        /*
         * After we locked the buffer we have to make sure that it actually has
         * enough space.
         */
        buffer = ReadBuffer(rel, block);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buffer);
        if (PageIsEmpty(page))
        {
            RecordPageWithFreeSpace(rel, block, 0);
            return buffer;
        }

        block = RecordAndGetPageWithFreeSpace(rel, block, 0, sz);
        UnlockReleaseBuffer(buffer);
    }

    /*
     * If there is no empty block, extend the relation.
     */
    LockRelationForExtension(rel, ExclusiveLock);
    buffer = ReadBuffer(rel, P_NEW);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    UnlockRelationForExtension(rel, ExclusiveLock);

    return buffer;
}

static void
cryo_allocate_blocks(Relation rel, CryoMetaPage *metapage, Buffer *buffers, int npages,
                     BlockNumber skip_block)
{
    int byte, bit;
    int n = BLCKSZ - MAXALIGN(sizeof(CryoMetaPage));
    int i;
    int nbuf = 0;
    unsigned int nblocks = RelationGetNumberOfBlocks(rel);
    bool more_blocks = true;

    for (byte = 0; byte < n && more_blocks; byte++)
    {
        uint8_t mask = metapage->page_mask[byte];

        if (mask == 0xff)
            /* All bits are set */
            continue;

        for (bit = 0; bit < 8; bit++)
        {
            if (!(mask & (1 << bit)))
            {
                BlockNumber blkno = byte * 8 + bit + 1;

                if (blkno >= nblocks)
                {
                    more_blocks = false;
                    break;
                }
                /* Ignore already locked buffer */
                /* TODO: far from perfect */
                if (blkno == skip_block)
                    continue;

                /* Bit is unset meaning page is empty */
                buffers[nbuf] = ReadBuffer(rel, blkno);
                LockBuffer(buffers[nbuf], BUFFER_LOCK_EXCLUSIVE);

                /* Double check that page is actually empty */
                if (!PageIsNew(BufferGetPage(buffers[nbuf])))
                {
                    LockBuffer(buffers[nbuf], BUFFER_LOCK_UNLOCK);
                    continue;
                }

                nbuf++;
            }
        }
    }

    /* No more existing empty pages. Create new ones */
    while (nbuf < npages)
    {
        buffers[nbuf] = ReadBuffer(rel, P_NEW);
        LockBuffer(buffers[nbuf], BUFFER_LOCK_EXCLUSIVE);
        nbuf++;
    }
}

static inline void
cryo_multi_insert_internal(Relation rel,
                           TupleTableSlot **slots,
                           int ntuples)
{
    CryoDataHeader *hdr;
    int             i;


    if (!RelationIsValid(modifyState.relation))
    {
        init_modify_state(rel);
    }
    else if (RelationGetRelid(modifyState.relation) != RelationGetRelid(rel))
    {
        /*
         * Flush whatever changes we currently have in the modifyState and
         * reinitialize it for a new relation.
         *
         * XXX: that isn't the optimal solution in case when user inserts
         * into several tables out of order in a single transaction. Other
         * option would be to have separate cache slot for every table we are
         * inserting to, but then there is a risk to exhaust all cache slots.
         */
        flush_modify_state();
        init_modify_state(rel);
    }
    hdr = (CryoDataHeader *) modifyState.data;

    for (i = 0; i < ntuples; ++i)
    {
        HeapTuple   tuple;
        int         pos;

        CHECK_FOR_INTERRUPTS();

        tuple = ExecCopySlotHeapTuple(slots[i]);
        if ((pos = cryo_storage_insert(hdr, tuple)) < 0)
        {
            /*
             * Compress and flush current block to the disk (if needed),
             * create a new one and retry insertion
             */
            flush_modify_state();
            init_modify_state(rel);
            hdr = (CryoDataHeader *) modifyState.data;

            if ((pos = cryo_storage_insert(hdr, tuple)) < 0)
            {
                elog(ERROR, "tuple is too large to fit the cryo block");
            }
        }
        pfree(tuple);
        modifyState.tuples_inserted++;

        slots[i]->tts_tableOid = RelationGetRelid(rel);
        /* position in item pointer starts with 1 */
        ItemPointerSet(&slots[i]->tts_tid, modifyState.target_block, pos);
    }
}

bool flag = false;

static void
cryo_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                  int options, BulkInsertState bistate)
{
    (void) cryo_multi_insert_internal(relation, &slot, 1);
}

static void
cryo_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                CommandId cid, int options,
                                BulkInsertState bistate, uint32 specToken)
{
    NOT_IMPLEMENTED;
}

static void
cryo_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                  uint32 specToken, bool succeeded)
{
    NOT_IMPLEMENTED;
}

/*
 * Number of postgres pages needed to fit data block of the specified size.
 */
static inline uint8
cryo_pages_needed(Size size)
{
    int     pages = 1;
    Size    page_sz = BLCKSZ  - sizeof(CryoPageHeader);

    Assert(size > 0);

    size -= BLCKSZ - sizeof(CryoFirstPageHeader);
    pages += size > 0 ? (size + page_sz - 1) / page_sz : 0;

    return pages;
}

/*
 * Compress and store data in postgres buffers, write WAL and all that stuff.
 * If advance flag is set true, then also shift target_block of the state to
 * the next block after the last written page.
 */
static void
cryo_preserve(CryoModifyState *state, bool advance)
{
    GenericXLogState   *xlog_state;
    CryoMetaPage       *metapage;
    CompressionMethod   method = compression_method_guc;
    Relation    rel = state->relation;
    Size        size;
    char       *compressed, *p;
    int         npages;
    Buffer      metabuf;
    Buffer     *buffers;
    // Buffer      buf;
    int         i;
    BlockNumber block, first_block;

    p = compressed = cryo_compress(method, state->data, &size);

    metabuf = cryo_load_meta(rel, BUFFER_LOCK_EXCLUSIVE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);
    first_block = block = state->target_block;

    /* split data into pages */
    npages = cryo_pages_needed(size);
    buffers = palloc(npages * sizeof(Buffer));

#if 0
    block = MAX(1, block);

    /* allocate and lock bufferes */
    buffers[0] = ReadBuffer(rel, block);
    LockBuffer(buffers[0], BUFFER_LOCK_EXCLUSIVE);
    if (npages > 1)
    {
        /*
         * If we need more than one page then lock relation for extension
         *
         * XXX don't need to do that for local relations but thats unlikely
         * the case 
         */
        LockRelationForExtension(state->relation, ExclusiveLock);
        cryo_allocate_blocks(state->relation, metapage, buffers + 1, npages - 1, block);
        UnlockRelationForExtension(state->relation, ExclusiveLock);
    }
#endif
    buffers[0] = state->target_buffer;
    for (i = 1; i < npages; ++i)
    {
        Size space_needed = BLCKSZ - sizeof(CryoPageHeader);

        block = RecordAndGetPageWithFreeSpace(rel, block, 0, space_needed);
        if (BlockNumberIsValid(block))
        {
            buffers[i] = ReadBuffer(rel, block);
        }
        else
        {
            LockRelationForExtension(rel, ExclusiveLock);
            buffers[i] = ReadBuffer(rel, P_NEW);
            block = BufferGetBlockNumber(buffers[i]);
            UnlockRelationForExtension(rel, ExclusiveLock);
        }
        LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
    }
    RecordPageWithFreeSpace(rel, block, 0);

    /* copy data into buffers and release them*/
    for (i = 0; i < npages; ++i)
    {
        CryoPageHeader *hdr;
        Size        hdr_size;
        Size        content_size;
        // BlockNumber blkno;

        xlog_state = GenericXLogStart(rel);
        hdr = (CryoPageHeader *)
            GenericXLogRegisterBuffer(xlog_state, buffers[i], GENERIC_XLOG_FULL_IMAGE);

        // if (!PageIsNew((Page) hdr))
        if (hdr->first != 0 || hdr->next != 0)
            elog(ERROR, "pg_cryogen: page is not new (block number %i)", block);

        hdr->first = first_block;
        hdr->next = (i + 1 < npages) ? BufferGetBlockNumber(buffers[i + 1]) : InvalidBlockNumber;

        /* write additional info into the first page */
        if (i == 0)
        {
            CryoFirstPageHeader *first_hdr = (CryoFirstPageHeader *) hdr;

            first_hdr->npages = npages;
            first_hdr->compression_method = method;
            first_hdr->compressed_size = size;
            first_hdr->created_xid = GetCurrentTransactionId();
        }
        block = BufferGetBlockNumber(buffers[i]);
        hdr_size = CryoPageHeaderSize(hdr, block);
        content_size = BLCKSZ - hdr_size;

        /*
         * Can't leave pd_upper = 0 because then page will be considered new
         * (see PageIsNew) and won't pass PageIsVerified check
         */
        hdr->base.pd_upper = BLCKSZ;
        hdr->base.pd_lower = hdr_size + MIN(content_size, size);
        hdr->base.pd_special = BLCKSZ;
        memcpy((char *) hdr + hdr_size,
               p,
               MIN(content_size, size));
        PageSetChecksumInplace((Page) hdr, block);

        size -= content_size;
        p += content_size;

        // blkno = BufferGetBlockNumber(buffers[i]);
        // RecordPageWithFreeSpace(rel, blkno, PageGetFreeSpace((Page) hdr));

        MarkBufferDirty(buffers[i]);
        GenericXLogFinish(xlog_state);
    }

    /* update metadata */
    xlog_state = GenericXLogStart(rel);
    metapage = (CryoMetaPage *)
        GenericXLogRegisterBuffer(xlog_state, metabuf,
                                  GENERIC_XLOG_FULL_IMAGE);
    if (advance)
        block += i;
    state->target_block = block;
    metapage->ntuples += state->tuples_inserted;
    PageSetChecksumInplace((Page) metapage, CRYO_META_PAGE);

#if 0
    for (i = 0; i < npages; ++i)
        mark_page_nonempty(metapage, BufferGetBlockNumber(buffers[i]));
#endif

    MarkBufferDirty(metabuf);
    GenericXLogFinish(xlog_state);

    /* Release resources */
    for (i = 0; i < npages; ++i)
        UnlockReleaseBuffer(buffers[i]);
    UnlockReleaseBuffer(metabuf);
    pfree(buffers);
    pfree(compressed);
}

static void
cryo_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                  CommandId cid, int options, BulkInsertState bistate)
{
    (void) cryo_multi_insert_internal(relation, slots, ntuples);
}

static void
cryo_finish_bulk_insert(Relation rel, int options)
{
    if (!RelationIsValid(rel))
        return;

    flush_modify_state();
}

static TM_Result
cryo_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
                    Snapshot snapshot, Snapshot crosscheck, bool wait,
                    TM_FailureData *tmfd, bool changingPart)
{
    elog(ERROR, "pg_cryogen is an append only storage");
}

static TM_Result
cryo_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
                    CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                    bool wait, TM_FailureData *tmfd,
                    LockTupleMode *lockmode, bool *update_indexes)
{
    elog(ERROR, "pg_cryogen is an append only storage");
}

static TM_Result
cryo_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
                  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
                  LockWaitPolicy wait_policy, uint8 flags,
                  TM_FailureData *tmfd)
{
    CryoDataHeader *hdr;
    CryoError       err;
    CacheEntry      cacheEntry;
    HeapTuple       tuple;

    err = cryo_read_data(relation, NULL, ItemPointerGetBlockNumber(tid),
                         &cacheEntry);
    if (err != CRYO_ERR_SUCCESS)
        elog(ERROR, "pg_cryogen: %s", cryo_cache_err(err));

    if (!xid_is_visible(snapshot, cryo_cache_get_xid(cacheEntry)))
        return TM_Invisible;

    hdr = (CryoDataHeader *) cryo_cache_get_data(cacheEntry);

    tuple = palloc0(sizeof(HeapTupleData));
    cryo_storage_fetch(hdr, ItemPointerGetOffsetNumber(tid), tuple);
    tuple->t_tableOid = RelationGetRelid(relation);
    ItemPointerCopy(tid, &tuple->t_self);

    ExecStoreHeapTuple(tuple, slot, true);

    /*
     * We don't do any actual locking since data is not going anywhere as it's
     * an append-only storage
     */
    return TM_Ok;
}

static void
cryo_get_latest_tid(TableScanDesc scan,
                    ItemPointer tid)
{
    NOT_IMPLEMENTED;
}

static TransactionId
cryo_compute_xid_horizon_for_tuples(Relation rel,
                                    ItemPointerData *items,
                                    int nitems)
{
    NOT_IMPLEMENTED;
}

static void
cryo_relation_set_new_filenode(Relation rel,
                                 const RelFileNode *newrnode,
                                 char persistence,
                                 TransactionId *freezeXid,
                                 MultiXactId *minmulti)
{
	SMgrRelation srel;

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	smgrclose(srel);
}

static void
cryo_relation_nontransactional_truncate(Relation rel)
{
    NOT_IMPLEMENTED;
}

static void
cryo_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
    NOT_IMPLEMENTED;
}

static void
cryo_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
                               Relation OldIndex, bool use_sort,
                               TransactionId OldestXmin,
                               TransactionId *xid_cutoff,
                               MultiXactId *multi_cutoff,
                               double *num_tuples,
                               double *tups_vacuumed,
                               double *tups_recently_dead)
{
    NOT_IMPLEMENTED;
}

static bool
cryo_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                             BufferAccessStrategy bstrategy)
{
    CryoPageHeader *page;
    CryoScanDesc    cscan = (CryoScanDesc) scan;
    Buffer          buf;
    CryoError       err;

    /* Skip metapage */
    if (blockno == 0)
        return false;

    /*
     * pg_cryogen's page layout doesn't really fits into postgres sampler's
     * views on page layout. So we need to improvise a bit. Whenever sampler
     * asks for block which is not the first page of cryo block we find the
     * first page and decompress cryo block starting with this page.
     */
    buf = ReadBuffer(cscan->rs_base.rs_rd, blockno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = (CryoPageHeader *) BufferGetPage(buf);
    //blockno = blockno - page->curpage;
    blockno = page->first;
    UnlockReleaseBuffer(buf);

    /* Do not scan the same block twice */
    if (blockno == cscan->cur_block)
        return false;

    err = cryo_read_data(cscan->rs_base.rs_rd, cscan->iterator, blockno,
                         &cscan->cacheEntry);
    if (err != CRYO_ERR_SUCCESS)
    {
        if (err == CRYO_ERR_EMPTY_BLOCK)
            return false;
        elog(ERROR, "pg_cryogen: %s", cryo_cache_err(err));
    }

    /*
     * XXX scan->rs_snapshot is NULL here so visibility is checked in
     * cryo_scan_analyze_next_tuple() by comparing block xid with OldestXmin
     */

    cscan->cur_block = blockno;
    cscan->cur_item = 1;
    return true;
}

static bool
cryo_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    CryoScanDesc    cscan = (CryoScanDesc) scan;
    CryoDataHeader *hdr;
    TransactionId   xid;

    /*
     * TODO: it's a sketchy solution, should think more thoroughly. But the
     * idea is that we only consider blocks that have been created before any
     * running transaction started and corresponding transaction did actually
     * commit. Otherwise skip the entire block.
     */
    xid = cryo_cache_get_xid(cscan->cacheEntry);
    if (!TransactionIdPrecedes(xid, OldestXmin) || !TransactionIdDidCommit(xid))
        return false;

    hdr = (CryoDataHeader *) cryo_cache_get_data(cscan->cacheEntry);

    if ((cscan->cur_item) * sizeof(CryoItemId) < hdr->lower)
    {
        HeapTuple tuple;

        tuple = palloc0(sizeof(HeapTupleData));
        cryo_storage_fetch(hdr, cscan->cur_item, tuple);
        tuple->t_tableOid = RelationGetRelid(cscan->rs_base.rs_rd);
        ItemPointerSet(&tuple->t_self, cscan->cur_block, cscan->cur_item);

        ExecStoreHeapTuple(tuple, slot, true);
        cscan->cur_item++;
        (*liverows)++;

        return true;
    }

    return false;
}

static double
cryo_index_build_range_scan(Relation rel,
                            Relation indexRelation,
                            IndexInfo *indexInfo,
                            bool allow_sync,
                            bool anyvisible,
                            bool progress,
                            BlockNumber start_blockno,
                            BlockNumber numblocks,
                            IndexBuildCallback callback,
                            void *callback_state,
                            TableScanDesc scan)
{
    Datum		values[INDEX_MAX_KEYS];
    bool        isnull[INDEX_MAX_KEYS];
    double      reltuples;
	ExprState  *predicate;
    TupleTableSlot *slot;
    EState     *estate;
    ExprContext *econtext;
    Snapshot    snapshot;
	TransactionId OldestXmin;
	bool		need_unregister_snapshot = false;

    if (start_blockno != 0 || numblocks != InvalidBlockNumber)
        elog(ERROR, "partial range scan is not supported");

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.  Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = table_slot_create(rel, NULL);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

    /*
     * Prepare for scan of the base relation.  In a normal index build, we use
     * SnapshotAny because we must retrieve all tuples and do our own time
     * qual checks (because we have to index RECENTLY_DEAD tuples). In a
     * concurrent build, or during bootstrap, we take a regular MVCC snapshot
     * and index whatever's live according to that.
     */
    OldestXmin = InvalidTransactionId;

    /* okay to ignore lazy VACUUMs here */
    if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
        OldestXmin = GetOldestXmin(rel, PROCARRAY_FLAGS_VACUUM);

    if (!scan)
    {
        /*
         * Serial index build.
         *
         * Must begin our own heap scan in this case.  We may also need to
         * register a snapshot whose lifetime is under our direct control.
         */
        if (!TransactionIdIsValid(OldestXmin))
        {
            snapshot = RegisterSnapshot(GetTransactionSnapshot());
            need_unregister_snapshot = true;
        }
        else
            snapshot = SnapshotAny;

        scan = table_beginscan_strat(rel,  /* relation */
                                     snapshot,  /* snapshot */
                                     0, /* number of keys */
                                     NULL,  /* scan key */
                                     true,  /* buffer access strategy OK */
                                     allow_sync);   /* syncscan OK? */
    }
    else
    {
        /*
         * Parallel index build.
         *
         * Parallel case never registers/unregisters own snapshot.  Snapshot
         * is taken from parallel heap scan, and is SnapshotAny or an MVCC
         * snapshot, based on same criteria as serial case.
         */
        Assert(!IsBootstrapProcessingMode());
        Assert(allow_sync);
        snapshot = scan->rs_snapshot;
    }

    /* Publish the number of blocks to scan */
    if (progress)
    {
        /* TODO */
    }
    
    reltuples = 0;

    while (cryo_getnextslot(scan, ForwardScanDirection, slot))
    {
#if PG_VERSION_NUM < 130000
        HeapTuple   tuple;
#endif

        CHECK_FOR_INTERRUPTS();

        /* Report scan progress, if asked to. */
        if (progress)
        {
            /* TODO */
        }

        reltuples += 1;

        MemoryContextReset(econtext->ecxt_per_tuple_memory);

        /*
         * In a partial index, discard tuples that don't satisfy the
         * predicate.
         */
        if (predicate != NULL)
        {
            if (!ExecQual(predicate, econtext))
                continue;
        }

        FormIndexDatum(indexInfo,
                       slot,
                       estate,
                       values,
                       isnull);

        /* Call the AM's callback routine to process the tuple */
#if PG_VERSION_NUM < 130000
        tuple = ExecCopySlotHeapTuple(slot);
        tuple->t_self = slot->tts_tid;
        callback(indexRelation, tuple, values, isnull, true, callback_state);
        pfree(tuple);
#else
        callback(indexRelation, &slot->tts_tid, values, isnull, true, callback_state);
#endif
    }

    if (progress)
    {
        /* TODO */
    }
	table_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}


static void
cryo_index_validate_scan(Relation relation,
                         Relation indexRelation,
                         IndexInfo *indexInfo,
                         Snapshot snapshot,
                         ValidateIndexState *state)
{
    NOT_IMPLEMENTED;
}

static uint64
cryo_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64		nblocks = 0;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(rel);

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
			nblocks += smgrnblocks(rel->rd_smgr, i);
	}
	else
		nblocks = smgrnblocks(rel->rd_smgr, forkNumber);

	return nblocks * BLCKSZ;
}

static bool
cryo_relation_needs_toast_table(Relation rel)
{
    return false;
}

static void
cryo_estimate_rel_size(Relation rel, int32 *attr_widths,
                         BlockNumber *pages, double *tuples,
                         double *allvisfrac)
{
    Buffer          metabuf;
    CryoMetaPage   *metapage;

    metabuf = cryo_load_meta(rel, BUFFER_LOCK_SHARE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);

    /* TODO */
    *pages = 0;
    *tuples = metapage->ntuples;
    *allvisfrac = 0;

    UnlockReleaseBuffer(metabuf);
}

static bool
cryo_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
    NOT_IMPLEMENTED;
}

static bool
cryo_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
                              TupleTableSlot *slot)
{
    NOT_IMPLEMENTED;
}

static void
cryo_vacuum_rel(Relation onerel, VacuumParams *params,
                BufferAccessStrategy bstrategy)
{
    int         npages = RelationGetNumberOfBlocks(onerel);
    BlockNumber     blkno;
    TransactionId   oldest_xmin, freeze_limit, xid_full_scan_limit;
    MultiXactId     multi_xact_cutoff, multi_xact_full_scan_limit;
    SeqScanIterator *iter;

    if (params->options & VACOPT_FULL)
        elog(ERROR, "pg_cryogen: VACUUM FULL is not implemented");

    vacuum_set_xid_limits(onerel,
                          params->freeze_min_age,
                          params->freeze_table_age,
                          params->multixact_freeze_min_age,
                          params->multixact_freeze_table_age,
                          &oldest_xmin, &freeze_limit, &xid_full_scan_limit,
                          &multi_xact_cutoff, &multi_xact_full_scan_limit);

    iter = cryo_seqscan_iter_create();
    blkno = cryo_seqscan_iter_next(iter);

    while (blkno < npages)
    {
        CryoFirstPageHeader *first_page;
        CryoPageHeader      *page;
        Buffer      buf;
        Buffer      vmbuf = InvalidBuffer;
        uint8       vmflags;
        bool        commited;
        int         nblocks;
        int         i;

        buf = ReadBuffer(onerel, blkno);
        LockBufferForCleanup(buf);
        first_page = (CryoFirstPageHeader *) BufferGetPage(buf);
        page = (CryoPageHeader *) first_page;

        /* skip empty pages */
        if (PageIsNew((PageHeader *) first_page))
        {
            RecordPageWithFreeSpace(onerel, blkno, BLCKSZ - sizeof(CryoPageHeader));
            UnlockReleaseBuffer(buf);
            blkno = cryo_seqscan_iter_next(iter);
            continue;
        }

        if (page->first != blkno)
        {
            /*
             * This is not the first page of the cryo block. Skip it for now,
             * we'll get back to it later.
             * This is also the case when a page created by non commited
             * transaction was previously vacuumed (i.e. first == next == 0).
             */
            UnlockReleaseBuffer(buf);
            blkno = cryo_seqscan_iter_next(iter);
            continue;
        }

        /*
         * If transaction did not commit we consider the corresponding pages
         * empty.
         */
        commited = TransactionIdDidCommit(first_page->created_xid);

        vmflags = visibilitymap_get_status(onerel, blkno, &vmbuf);
        if (!(vmflags & VISIBILITYMAP_ALL_FROZEN))
        {
            if (TransactionIdPrecedes(first_page->created_xid, freeze_limit))
            {
                /* Freeze page by setting bit in visibility map */ 
                visibilitymap_pin(onerel, blkno, &vmbuf);

                if (!BufferIsValid(vmbuf))
                    elog(ERROR,
                         "pg_cryogen: failed to pin visibility map buffer for block %d of relation '%s'",
                         blkno, RelationGetRelationName(onerel));

                vmflags |= VISIBILITYMAP_ALL_FROZEN;
                visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
                                  vmbuf, freeze_limit, vmflags);
            }
        }

        if (!commited)
            RecordPageWithFreeSpace(onerel, blkno, BLCKSZ - sizeof(CryoPageHeader));

        nblocks = first_page->npages;
        Buffer *buffers = palloc0(sizeof(Buffer) * nblocks);

        /*
         * Mark the rest pages as read. Not optimal way to do it, but the only
         * solution I came up with so far.
         */
        buffers[0] = buf;
        for (i = 1; i < nblocks; ++i)
        {
            if (!BlockNumberIsValid(page->next))
                elog(ERROR, "unexpected page chain end");

            buffers[i] = ReadBuffer(onerel, page->next);
            LockBufferForCleanup(buffers[i]);
            page = (CryoPageHeader *) BufferGetPage(buffers[i]);
        }

        for (i = 0; i < nblocks; ++i)
        {
            if (!commited)
            {
                GenericXLogState *xlogState = GenericXLogStart(onerel);

                blkno = BufferGetBlockNumber(buffers[i]);

                page = (CryoPageHeader *)
                    GenericXLogRegisterBuffer(xlogState, buffers[i], GENERIC_XLOG_FULL_IMAGE);
                page->first = page->next = 0;
                page->base.pd_upper = BLCKSZ;
                page->base.pd_lower = sizeof(CryoMetaPage);
                page->base.pd_special = BLCKSZ;
                PageSetChecksumInplace((Page) page, blkno);
                GenericXLogFinish(xlogState);                

#if 0
                GenericXLogState *xlogState = GenericXLogStart(onerel);

                page = (CryoPageHeader *)
                    GenericXLogRegisterBuffer(xlogState, buffers[i], GENERIC_XLOG_FULL_IMAGE);
                memset(page, 0, BLCKSZ);
                PageSetChecksumInplace((Page) page, block);
                GenericXLogFinish(xlogState);
#endif

                RecordPageWithFreeSpace(onerel, BufferGetBlockNumber(buffers[i]),
                                        BLCKSZ - sizeof(CryoPageHeader));
            }

            if (i != 0)
                cryo_seqscan_iter_exclude(iter, BufferGetBlockNumber(buffers[i]), false);

            UnlockReleaseBuffer(buffers[i]);
        }
        pfree(buffers);

        if (BufferIsValid(vmbuf))
            ReleaseBuffer(vmbuf);

        blkno = cryo_seqscan_iter_next(iter);
    }

    FreeSpaceMapVacuum(onerel);
}

/*
 * Defenition of cryo table access method.
 */
static const TableAmRoutine cryo_methods = 
{
    .type = T_TableAmRoutine,

    .slot_callbacks = cryo_slot_callbacks,

    .scan_begin = cryo_beginscan,
    .scan_getnextslot = cryo_getnextslot,
    .scan_rescan = cryo_rescan,
    .scan_end = cryo_endscan,

    .parallelscan_estimate = table_block_parallelscan_estimate,
    .parallelscan_initialize = table_block_parallelscan_initialize,
    .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

    .index_fetch_begin = cryo_index_fetch_begin,
    .index_fetch_reset = cryo_index_fetch_reset,
    .index_fetch_end = cryo_index_fetch_end,
    .index_fetch_tuple = cryo_index_fetch_tuple,

    .tuple_insert = cryo_tuple_insert,
    .finish_bulk_insert = cryo_finish_bulk_insert,
    .tuple_insert_speculative = cryo_tuple_insert_speculative,
    .tuple_complete_speculative = cryo_tuple_complete_speculative,
    .multi_insert = cryo_multi_insert,
    .tuple_delete = cryo_tuple_delete,
    .tuple_update = cryo_tuple_update,
    .tuple_lock = cryo_tuple_lock,

    .tuple_fetch_row_version = cryo_fetch_row_version,
    .tuple_tid_valid = cryo_tuple_tid_valid,
    .tuple_get_latest_tid = cryo_get_latest_tid,
    .tuple_satisfies_snapshot = cryo_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = cryo_compute_xid_horizon_for_tuples,

    .relation_set_new_filenode = cryo_relation_set_new_filenode,
    .relation_nontransactional_truncate = cryo_relation_nontransactional_truncate,
    .relation_copy_data = cryo_relation_copy_data,
    .relation_copy_for_cluster = cryo_relation_copy_for_cluster,
    .relation_vacuum = cryo_vacuum_rel,
    .scan_analyze_next_block = cryo_scan_analyze_next_block,
    .scan_analyze_next_tuple = cryo_scan_analyze_next_tuple,
    .index_build_range_scan = cryo_index_build_range_scan,
    .index_validate_scan = cryo_index_validate_scan,

    .relation_size = cryo_relation_size,
    .relation_needs_toast_table = cryo_relation_needs_toast_table,

    .relation_estimate_size = cryo_estimate_rel_size,

    .scan_sample_next_block = cryo_scan_sample_next_block,
    .scan_sample_next_tuple = cryo_scan_sample_next_tuple,

    .scan_bitmap_next_block = cryo_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = cryo_scan_bitmap_next_tuple
};

Datum
cryo_tableam_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(&cryo_methods);
}

#include "access/nbtree.h"
#include "postmaster/autovacuum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define MAXDEADTUPLES(max_size) \
		(((max_size) - offsetof(LVDeadTuples, itemptrs)) / sizeof(ItemPointerData))
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage
#define SizeOfDeadTuples(cnt) \
	add_size(offsetof(LVDeadTuples, itemptrs), \
			 mul_size(sizeof(ItemPointerData), cnt))

static TIDBitmap *
index_get_bitmap(Relation irel, Oid atttype, Oid valtype, Datum val, int strategy)
{
    ScanKeyData scankey;
    AttrNumber  attno = 1;
    IndexScanDesc scandesc;
    TIDBitmap  *tbm;
    int     i = 0;
    bool    isnull;
    Datum   indclassDatum;
    oidvector *indclass;
    Oid     opclass;
    Oid     opfamily;
    Oid     op;
    Oid     funcid;

    /* Find an appropriate function for the specified strategy */
    indclassDatum = SysCacheGetAttr(INDEXRELID, irel->rd_indextuple,
                                    Anum_pg_index_indclass, &isnull);
    Assert(!isnull);
    indclass = ((oidvector *) DatumGetPointer(indclassDatum));
    opclass = indclass->values[attno - 1];
    opfamily = get_opclass_family(opclass);
    op = get_opfamily_member(opfamily, atttype, valtype, strategy);
    funcid = get_opcode(op);

    /*
     * initialize the scan key's fields appropriately
     *
     * See ExecIndexBuildScanKeys() as an example
     */
    ScanKeyEntryInitialize(&scankey,
                           0,
                           attno,       /* attribute number to scan */
                           strategy,    /* op's strategy */
                           atttype,     /* strategy subtype */
                           InvalidOid,  /* TODO: collation */
                           funcid,      /* reg proc to use */
                           val);	    /* constant */

    /* ExecInitBitmapIndexScan */
    scandesc = index_beginscan_bitmap(irel, GetActiveSnapshot(), 1);
    index_rescan(scandesc, &scankey, 1, NULL, 0);

    /* MultiExecBitmapIndexScan */
    tbm = tbm_create(work_mem * 1024L, NULL);
    index_getbitmap(scandesc, tbm);
    index_endscan(scandesc);

#if 0
    TBMIterator *it;
    TBMIterateResult *res;
    it = tbm_begin_iterate(tbm);
    while ((res = tbm_iterate(it)) != NULL)
    {
        elog(NOTICE, "block #%i", res->blockno);
    }
    tbm_end_iterate(it);
#endif

    return tbm;
}

int BlockNumberComparator(const void *a, const void *b)
{
    return (*(BlockNumber *) a < *(BlockNumber *) b) ? -1 : 1;
}

#include "utils/fmgroids.h"

static TIDBitmap *
index_extract_tids(Relation irel, Datum val, Oid valtype,
                   BlockNumber **blocks, int *nblocks)
{
    int         strategy = BTLessStrategyNumber;
    AttrNumber  attno = 1;
    TupleDesc   tupdesc;
    Oid         atttype;
    TypeCacheEntry *tce1, *tce2;
    Oid         opfuncid;
    TIDBitmap  *tbm_le, *tbm_ge, *tbm_eq;
    List       *border_blocks = NIL;

    tupdesc = RelationGetDescr(irel);
    atttype = TupleDescAttr(tupdesc, attno - 1)->atttypid;

    tbm_le = index_get_bitmap(irel, atttype, valtype, val, BTLessStrategyNumber);
    tbm_ge = index_get_bitmap(irel, atttype, valtype, val, BTGreaterEqualStrategyNumber);

#if 0
    /*
     * Find border case blocks, i.e. such blocks that contain values less than
     * val AND val itself.
     */
    tbm_intersect(tbm_ge, tbm_le);

    if (tbm_ge)
    {
        TBMIterator      *it;
        TBMIterateResult *res;
        // int i = 0;

        // *nblocks = tbm_ge->npages;
        // *blocks = (BlockNumber *) palloc(sizeof(BlockNumber) * (*nblocks));

        it = tbm_begin_iterate(tbm_ge);
        while ((res = tbm_iterate(it)) != NULL)
            // blocks[i++] = res->blockno;
            border_blocks = lappend_int(border_blocks, res->blockno);
        tbm_end_iterate(it);
    }
#endif

    /*
     * Find blocks on intersection of both bitmaps. Given the comment on
     * tbm_iterate() we assume that block numbers are ordered.
     */
#if 1
    if (tbm_le && tbm_ge)
    {
        TBMIterator *it1, *it2;
        TBMIterateResult *res1, *res2;

        it1 = tbm_begin_iterate(tbm_le);
        it2 = tbm_begin_iterate(tbm_ge);

        res1 = tbm_iterate(it1);
        res2 = tbm_iterate(it2);

        while (res1 != NULL && res2 != NULL)
        {
            BlockNumber bn1, bn2;

            while (res2 != NULL && res2->blockno < res1->blockno)
                res2 = tbm_iterate(it2);

            if (res2 == NULL)
                break;

            if (res2->blockno == res1->blockno)
                border_blocks = lappend_int(border_blocks, res2->blockno);

            // while ((res2 = tbm_iterate(it2)) != NULL && res2->blockno < res1->blockno)
            //     ;
            res1 = tbm_iterate(it1);
        }

        tbm_end_iterate(it2);
        tbm_end_iterate(it1);
    }
#endif

    /* Convert list of border blocks into array */
    *blocks = NULL;
    if (border_blocks)
    {
        ListCell *lc;
        int i = 0;

        *nblocks = list_length(border_blocks);
        *blocks = (BlockNumber *) palloc(sizeof(BlockNumber) * (*nblocks));

        foreach (lc, border_blocks)
        {
            (*blocks)[i++] = lfirst_int(lc);
        }
        // pg_qsort(*blocks, *nblocks, sizeof(BlockNumber), BlockNumberComparator);
    }

    return tbm_le;
}

/* Copied from access/heap/vacuumlazy.c */
typedef struct LVDeadTuples
{
	int			max_tuples;		/* # slots allocated in array */
	int			num_tuples;		/* current # of entries */
	/* List of TIDs of tuples we intend to delete */
	/* NB: this list is ordered by TID address */
	ItemPointerData itemptrs[FLEXIBLE_ARRAY_MEMBER];	/* array of
														 * ItemPointerData */
} LVDeadTuples;

static long
compute_max_dead_tuples(BlockNumber relblocks, bool useindex)
{
	long		maxtuples;
	int			vac_work_mem = IsAutoVacuumWorkerProcess() &&
	autovacuum_work_mem != -1 ?
	autovacuum_work_mem : maintenance_work_mem;

	if (useindex)
	{
		maxtuples = MAXDEADTUPLES(vac_work_mem * 1024L);
		maxtuples = Min(maxtuples, INT_MAX);
		maxtuples = Min(maxtuples, MAXDEADTUPLES(MaxAllocSize));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (maxtuples / LAZY_ALLOC_TUPLES) > relblocks)
			maxtuples = relblocks * LAZY_ALLOC_TUPLES;

		/* stay sane if small maintenance_work_mem */
		maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
	}
	else
		maxtuples = MaxHeapTuplesPerPage;

	return maxtuples;
}

static LVDeadTuples *
lazy_space_alloc(BlockNumber relblocks)
{
    LVDeadTuples *dead_tuples = NULL;
    long		maxtuples;

    maxtuples = compute_max_dead_tuples(relblocks, true);

    dead_tuples = (LVDeadTuples *) palloc(SizeOfDeadTuples(maxtuples));
    dead_tuples->num_tuples = 0;
    dead_tuples->max_tuples = (int) maxtuples;

    return dead_tuples;
}

static void
vac_record_tuple(LVDeadTuples *tuples, BlockNumber blockno, OffsetNumber offset)
{
    /*
     * The array shouldn't overflow under normal behavior, but perhaps it
     * could if we are given a really small maintenance_work_mem. In that
     * case, just forget the last few tuples (we'll get 'em next time).
     */
    if (tuples->num_tuples < tuples->max_tuples)
    {
        BlockIdSet(&tuples->itemptrs[tuples->num_tuples].ip_blkid, blockno);
        tuples->itemptrs[tuples->num_tuples].ip_posid = offset;
        tuples->num_tuples++;
        // pgstat_progress_update_param(PROGRESS_VACUUM_NUM_DEAD_TUPLES,
        //                              tuples->num_tuples);
    }
}

static int
cryo_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

static int
cryo_cmp_blockno(const void *left, const void *right)
{
    return (*(BlockNumber *) left) < (*(BlockNumber *) right) ? -1 :
        (*(BlockNumber *) left) == (*(BlockNumber *) right) ? 0 : 1;
}

static bool
cryo_tid_reaped(ItemPointer itemptr, void *state)
{
	LVDeadTuples *tuples = (LVDeadTuples *) state;
	ItemPointer res;

	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) tuples->itemptrs,
								tuples->num_tuples,
								sizeof(ItemPointerData),
								cryo_cmp_itemptr);

	return (res != NULL);
}

static void
cryo_vacuum_indexes(Relation *irels, IndexBulkDeleteResult **stats,
                    int nindexes, LVDeadTuples *tuples)
{
    for (int i = 0; i < nindexes; ++i)
    {
        IndexVacuumInfo ivinfo;

        ivinfo.index = irels[i];
        ivinfo.analyze_only = false;
        ivinfo.report_progress = false;
        ivinfo.estimated_count = true;
        ivinfo.message_level = DEBUG2;
        ivinfo.num_heap_tuples = 0; /* TODO */
        ivinfo.strategy = NULL; /* TODO? */

        stats[i] = index_bulk_delete(&ivinfo, stats[i], cryo_tid_reaped, (void *) tuples);
    }
}

static void
cryo_drop_blocks(Relation rel, List *blocks)
{
    ListCell *lc;

    foreach (lc, blocks)
    {
        CryoFirstPageHeader *first_page;
        CryoPageHeader      *page;
        Buffer      buf;
        Buffer     *buffers;
        int         nblocks;
        BlockNumber blockno = lfirst_int(lc);

        /* Drop cryo block */
        buf = ReadBuffer(rel, blockno);
        LockBufferForCleanup(buf);
        first_page = (CryoFirstPageHeader *) BufferGetPage(buf);
        page = (CryoPageHeader *) first_page;

        if (page->first != blockno)
            elog(ERROR, "index points to a non-starting block");
        
        nblocks = first_page->npages;
        buffers = palloc0(sizeof(Buffer) * nblocks);

        buffers[0] = buf;
        for (int i = 1; i < nblocks; i++)
        {
            if (!BlockNumberIsValid(page->next))
                elog(ERROR, "unexpected page chain end");

            buffers[i] = ReadBuffer(rel, page->next);
            LockBufferForCleanup(buffers[i]);
            page = (CryoPageHeader *) BufferGetPage(buffers[i]);
        }
        for (int i = 0; i < nblocks; i++)
        {
            GenericXLogState *xlogState = GenericXLogStart(rel);
            BlockNumber blkno = BufferGetBlockNumber(buffers[i]);

            page = (CryoPageHeader *)
                GenericXLogRegisterBuffer(xlogState, buffers[i], GENERIC_XLOG_FULL_IMAGE);
            page->first = page->next = 0;
            page->base.pd_upper = BLCKSZ;
            page->base.pd_lower = sizeof(CryoMetaPage);
            page->base.pd_special = BLCKSZ;
            PageSetChecksumInplace((Page) page, blkno);
            GenericXLogFinish(xlogState);                

            RecordPageWithFreeSpace(rel, BufferGetBlockNumber(buffers[i]),
                                    BLCKSZ - sizeof(CryoPageHeader));
            UnlockReleaseBuffer(buffers[i]);
            cryo_cache_invalidate_relblock(RelationGetRelid(rel), blkno);
        }
        pfree(buffers);
    }
}

//#define TBM_DEBUG

/*
 * cryo_purge
 *      Delete blocks containing values less than specified.
 */
PG_FUNCTION_INFO_V1(cryo_purge);
Datum
cryo_purge(PG_FUNCTION_ARGS)
{
    Oid         relid = PG_GETARG_OID(0);
    char       *attname = PG_GETARG_CSTRING(1);
    Datum       val = PG_GETARG_DATUM(2);
    Oid         valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
    Relation    rel;
    List       *indexlist = NIL;
    ListCell   *lc;
    int         attnum;
    TIDBitmap  *tbm;
    Relation    key_index = NULL;
    Relation   *irels;
    IndexBulkDeleteResult **indstats;
    int         nindexes = 0;
    int         i = 0;

    BlockNumber *border_blocks;
    int nborder_blocks;

    rel = relation_open(relid, RowExclusiveLock);
    attnum = attnameAttNum(rel, attname, false);
    indexlist = RelationGetIndexList(rel);

    irels = (Relation *)
        palloc0(list_length(indexlist) * sizeof(Relation));
    indstats = (IndexBulkDeleteResult **)
        palloc0(list_length(indexlist) * sizeof(IndexBulkDeleteResult *));

    foreach (lc, indexlist)
    {
        Form_pg_index index;
        Oid         oid = lfirst_oid(lc);
        Relation    irel;
        List       *expr_list;

        irel = index_open(oid, RowExclusiveLock);
        index = irel->rd_index;

        /* Ignore invalid indexes */
        if (!index->indisvalid)
        {
            index_close(irel, RowExclusiveLock);
            continue;
        }
        // indexlist = lappend(indexlist, irel);
        irels[nindexes++] = irel;

        Assert(index->indnatts > 0);

        /* indkey.values[0] == 0 means it's an expression */
        if (index->indkey.values[0] == attnum && !key_index)
        {
            key_index = irel;

            elog(NOTICE, "index found: %s!", RelationGetRelationName(irel));
        }
    }

    if (!key_index)
        elog(ERROR, "key index not found");

    tbm = index_extract_tids(key_index, val, valtype,
                             &border_blocks, &nborder_blocks);
    if (tbm)
    {
        TBMIterator    *iter;
        TBMIterateResult *tbmres;
        LVDeadTuples   *tuples;
        List           *dead_blocks = NIL;
        
        tuples = lazy_space_alloc(RelationGetNumberOfBlocks(rel));
        
        iter = tbm_begin_iterate(tbm);

        while((tbmres = tbm_iterate(iter)) != NULL)
        {
            BlockNumber *blockno_ptr;

            blockno_ptr = bsearch((void *) &tbmres->blockno,
                                  (void *) border_blocks,
                                  nborder_blocks,
                                  sizeof(BlockNumber),
                                  cryo_cmp_blockno);

            /* If current block is one of border blocks - skip it */
            if (blockno_ptr)
                continue;

            /* Did tuples struct reach max capacity? */
            if ((tuples->max_tuples - tuples->num_tuples) < MaxHeapTuplesPerPage &&
                tuples->num_tuples > 0)
            {
                /* TODO: update index stats */
#ifndef DEBUG_TBM
                cryo_vacuum_indexes(irels, indstats, nindexes, tuples);
                cryo_drop_blocks(rel, dead_blocks);
#endif
                elog(NOTICE, "index_bulk_delete() and drop pages");
                list_free(dead_blocks);
                dead_blocks = NIL;
            }

            /*
             * Got lossy result meaning we didn't get item pointers within
             * tidbitmap and have to read them directly from cryo page.
             */
            if (tbmres->ntuples == -1)
            {
                elog(ERROR, "not implemented yet");
            }
            else
            {
                for (int i = 0; i < tbmres->ntuples; ++i)
                {
                    vac_record_tuple(tuples, tbmres->blockno, tbmres->offsets[i]);
                }
            }
            dead_blocks = lappend_int(dead_blocks, tbmres->blockno);
        }
        /* TODO: update index stats */
#ifndef DEBUG_TBM
        cryo_vacuum_indexes(irels, indstats, nindexes, tuples);
        cryo_drop_blocks(rel, dead_blocks);
#endif
        elog(NOTICE, "final index_bulk_delete() and drop pages");

        tbm_end_iterate(iter);
    }

    for (int i = 0; i < nindexes; i++)
    {
        index_close(irels[i], RowExclusiveLock);
    }

    relation_close(rel, RowExclusiveLock);
}

