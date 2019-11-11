#include "postgres.h"

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
#include "storage/bufmgr.h"
#include "storage/checksum.h"
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

#define CRYO_META_PAGE 0
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define NOT_IMPLEMENTED \
    do { \
        elog(ERROR, "function \"%s\" is not implemented", __func__); \
    } while (0)

/*
 * There were old implementation that continues to write new tuples in the
 * last block. It wasn't very relyable as if there is an error somewhere 
 * between writes of pages of one block we'd end up with a broken storage with
 * half new pages and half old. To fix that we now write new tuples into a new
 * block on every multi_insert. It's a bit less space efficient but more
 * durable. To keep previous implementation just in case this define was
 * introduced.
 */
#define ONE_WRITE_ONE_BLOCK

typedef struct CryoScanDescData
{
    TableScanDescData   rs_base;
    uint32              nblocks;
    BlockNumber         cur_block;
    uint32              cur_item;
    BlockNumber         target_block;
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
    BlockNumber target_block;
    uint32      tuples_inserted;
    CacheEntry  cacheEntry; /* cached decompressed page reference */
    char       *data;               /* decompressed data */
} CryoModifyState;

CryoModifyState modifyState = {
    .tuples_inserted = 0,
    .relation = NULL
};


PG_FUNCTION_INFO_V1(cryo_tableam_handler);
static Buffer cryo_load_meta(Relation rel, int lockmode);
static void cryo_preserve(CryoModifyState *state, bool advance);
static BlockNumber cryo_reserve_blockno(Relation rel);
void _PG_init(void);

static void
flush_modify_state(void)
{
    if (!RelationIsValid(modifyState.relation))
        return;

    if (modifyState.tuples_inserted)
    {
#ifdef ONE_WRITE_ONE_BLOCK
        /*
         * Advance target_block so that next tuple will be written into a new
         * block and not into the same. Read comment on ONE_WRITE_ONE_BLOCK for
         * details.
         */
        cryo_preserve(&modifyState, true);
#else
        cryo_preserve(&modifyState, false);
#endif
    }
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
    modifyState.target_block = cryo_reserve_blockno(rel);
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
#ifdef ONE_WRITE_ONE_BLOCK
        /*
         * In order to ensure that storage is  consistent we create entirely
         * new block for every multi_insert instead of adding tuples to an
         * existing one. This also makes visibility check much simpler and
         * faster: we just check transaction id in a block header.
         */
        cryo_init_page(hdr);
#else
        /* Read the target block contents into modifyState.data */
        cryo_read_data(rel, NULL, modifyState.target_block,  modifyState.data);
#endif
    }
    modifyState.relation = rel;
    modifyState.tuples_inserted = 0;
}

static void
cryo_xact_callback(XactEvent event, void *arg)
{
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
}

static void
cryo_relcache_callback(Datum arg, Oid relid)
{
    cryo_cache_invalidate_relation(relid);
}

void
_PG_init(void)
{
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

static TableScanDesc
cryo_beginscan(Relation relation, Snapshot snapshot,
               int nkeys, ScanKey key,
               ParallelTableScanDesc parallel_scan,
               uint32 flags)
{
    CryoScanDesc    scan;
    Buffer          metabuf;
    CryoMetaPage   *metapage;

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

    metabuf = cryo_load_meta(relation, BUFFER_LOCK_SHARE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);
    scan->target_block = metapage->target_block;
    UnlockReleaseBuffer(metabuf);

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
            if (err == CRYO_ERR_EMPTY_BLOCK)
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

    cscan->cur_block = MAX(1, cscan->cur_block);  /* initial case */
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
            metapage->base.pd_special = BLCKSZ;
            metapage->version = STORAGE_VERSION;

            /* No target block yet */
            metapage->target_block = 0;
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

static BlockNumber
cryo_reserve_blockno(Relation rel)
{
    Buffer      buf;
    BlockNumber res;

    LockRelationForExtension(rel, ExclusiveLock);
    buf = ReadBuffer(rel, P_NEW);
    UnlockRelationForExtension(rel, ExclusiveLock);
    res = BufferGetBlockNumber(buf);
    ReleaseBuffer(buf);

    return res;
}

static inline void
cryo_multi_insert_internal(Relation rel,
                           TupleTableSlot **slots,
                           int ntuples,
                           XactCallback callback)
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
    (void) cryo_multi_insert_internal(relation, &slot, 1, cryo_xact_callback);
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

    Assert(size > 0);

    size -= MIN(BLCKSZ - sizeof(CryoFirstPageHeader), size);

    while (size > 0)
    {
        size -= MIN(BLCKSZ - sizeof(CryoPageHeader), size);
        pages++;
    }

    return pages;
};

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
    int         i;
    BlockNumber block;

    p = compressed = cryo_compress(method, state->data, &size);

    metabuf = cryo_load_meta(state->relation, BUFFER_LOCK_EXCLUSIVE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);
    block = state->target_block;

    /* split data into pages */
    npages = cryo_pages_needed(size);
    buffers = palloc(npages * sizeof(Buffer));

    block = MAX(1, block);

    /*
     * If we need more than one page then lock relation for extension
     *
     * XXX don't need to do that for local relations but thats unlikely
     * the case 
     */
    if (npages > 1)
        LockRelationForExtension(state->relation, ExclusiveLock);

    /* allocate and lock bufferes */
    for (i = 0; i < npages; ++i)
    {
        /* the first block was preallocated in init_modify_state */
        buffers[i] = ReadBuffer(rel, block);
        LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);

        /* but the rest of the blocks must be allocated here */
        block = P_NEW;
    }
    if (npages > 1)
        UnlockRelationForExtension(state->relation, ExclusiveLock);

    /* copy data into buffers and release them*/
    for (i = 0; i < npages; ++i)
    {
        CryoPageHeader *hdr;
        Size        hdr_size;
        Size        content_size;

        xlog_state = GenericXLogStart(state->relation);
        hdr = (CryoPageHeader *)
            GenericXLogRegisterBuffer(xlog_state, buffers[i],
                                      GENERIC_XLOG_FULL_IMAGE);
        hdr->first = BufferGetBlockNumber(buffers[0]);
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

        MarkBufferDirty(buffers[i]);
        GenericXLogFinish(xlog_state);
    }

    /* update metadata */
    xlog_state = GenericXLogStart(state->relation);
    metapage = (CryoMetaPage *)
        GenericXLogRegisterBuffer(xlog_state, metabuf,
                                  GENERIC_XLOG_FULL_IMAGE);
#ifdef ONE_WRITE_ONE_BLOCK
    if (advance)
        block += i;
#endif
    metapage->target_block = state->target_block = block;
    metapage->ntuples += state->tuples_inserted;
    PageSetChecksumInplace((Page) metapage, CRYO_META_PAGE);

    MarkBufferDirty(metabuf);
    GenericXLogFinish(xlog_state);

    /* Release all affected buffers */
    for (i = 0; i < npages; ++i)
        UnlockReleaseBuffer(buffers[i]);
    UnlockReleaseBuffer(metabuf);

#ifndef ONE_WRITE_ONE_BLOCK
    if (advance)
        state->target_block = state->target_block + i;
#endif
}

static void
cryo_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                  CommandId cid, int options, BulkInsertState bistate)
{
    (void) cryo_multi_insert_internal(relation, slots, ntuples, NULL);
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

    /* Publish number of blocks to scan */
    if (progress)
    {
        /* TODO */
    }
    
    reltuples = 0;

    while (cryo_getnextslot(scan, ForwardScanDirection, slot))
    {
        HeapTuple   tuple;

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
        tuple = ExecCopySlotHeapTuple(slot);
        tuple->t_self = slot->tts_tid;
        callback(indexRelation, tuple, values, isnull, true, callback_state);

        pfree(tuple);
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

        buf = ReadBuffer(onerel, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        first_page = (CryoFirstPageHeader *) BufferGetPage(buf);

        /* skip empty pages */
        if (PageIsNew((PageHeader *) first_page))
        {
            UnlockReleaseBuffer(buf);
            blkno = cryo_seqscan_iter_next(iter);
        }

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

        /*
         * Mark the rest pages as read. Not optimal way to do it, but the only
         * solution I came up with so far.
         */
        page = (CryoPageHeader *) first_page;
        while (BlockNumberIsValid(page->next))
        {
            Buffer  b;

            blkno = page->next;
            b = ReadBuffer(onerel, blkno);
            LockBuffer(b, BUFFER_LOCK_SHARE);
            page = (CryoPageHeader *) BufferGetPage(b);
            cryo_seqscan_iter_exclude(iter, blkno, false);
            UnlockReleaseBuffer(b);
        }

        if (BufferIsValid(vmbuf))
            ReleaseBuffer(vmbuf);
        UnlockReleaseBuffer(buf);

        blkno = cryo_seqscan_iter_next(iter);
    }
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
