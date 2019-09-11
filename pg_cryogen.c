#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "lz4.h"

#include "storage.h"


PG_MODULE_MAGIC;

#define CRYO_META_PAGE 0
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define NOT_IMPLEMENTED \
    do { \
        elog(ERROR, "function \"%s\" is not implemented", __func__); \
    } while (0)


typedef struct CryoScanDescData
{
    TableScanDescData   rs_base;
    uint32              nblocks;
    BlockNumber         cur_block;
    uint32              cur_item;
    char                data[CRYO_BLCKSZ];  /* decompressed data */
} CryoScanDescData;
typedef CryoScanDescData *CryoScanDesc;


typedef struct CryoModifyState
{
    Relation    relation;
    BlockNumber target_block;
    bool        modified;
    Buffer      metabuf;
    char        data[CRYO_BLCKSZ];  /* decompressed data */
} CryoModifyState;

CryoModifyState modifyState;


PG_FUNCTION_INFO_V1(cryo_tableam_handler);

void _PG_init(void);

void
_PG_init(void)
{
    /*
    modifyState.data = palloc0(CRYO_BLCKSZ);
    */
}

static const TupleTableSlotOps *
cryo_slot_callbacks(Relation relation)
{
//    return &TTSOpsBufferHeapTuple;
    return &TTSOpsHeapTuple;
}

static char *
cryo_compress(const char *data, Size *compressed_size)
{
    Size    estimate;
    char   *compressed;

    estimate = LZ4_compressBound(CRYO_BLCKSZ);
    compressed = palloc(estimate);

    *compressed_size = LZ4_compress_fast(data, compressed,
                                         CRYO_BLCKSZ, estimate, 0);
    if (*compressed_size == 0)
        elog(ERROR, "pg_cryogen: compression failed");

    return compressed;
}

/*
 * Decompress and store result in `out`
 */
static void
cryo_decompress(const char *compressed, Size compressed_size, char *out)
{
    int bytes;

    bytes = LZ4_decompress_safe(compressed, out, compressed_size, CRYO_BLCKSZ);
    if (bytes < 0)
        elog(ERROR, "pg_cryogen: decompression failed");

    Assert(CRYO_BLCKSZ == bytes);
}

/*
 * Read and reassemble compressed data from the buffer manager starting
 * with `block` and decompress it.
 */
static void
cryo_read_data(Relation rel, BlockNumber block, uint32 *nblocks, char *out)
{
    Buffer      buf;
    CryoPageHeader *page;
    char       *compressed, *p;
    Size        page_content_size = BLCKSZ - sizeof(CryoPageHeader);
    Size        compressed_size, size;

    /* TODO: do not rely on RelationGetNumberOfBlocks; refer to metapage */
    if (RelationGetNumberOfBlocks(rel) <= block)
    {
        cryo_init_page((CryoDataHeader *) out);
        *nblocks = 0;
        return;
    }

    buf = ReadBuffer(rel, block);
    page = (CryoPageHeader *) BufferGetPage(buf);
    size = compressed_size = page->compressed_size;
    p = compressed = palloc(compressed_size);
    if (nblocks)
        *nblocks = 1;

    while (true)
    {
        Size    l = MIN(page_content_size, size);
        char   *page_content = ((char *) page) + sizeof(CryoPageHeader);

        memcpy(p, page_content, l);
        p += l;
        size -= l;
        ReleaseBuffer(buf);

        if (size == 0)
            break;

        /* read the next block */
        buf = ReadBuffer(rel, ++block);
        page = (CryoPageHeader *) BufferGetPage(buf);

        if (nblocks)
            (*nblocks)++;
    }

    cryo_decompress(compressed, compressed_size, out);
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
    scan->cur_block = 1;

    cryo_read_data(relation, scan->cur_block, &scan->nblocks, scan->data);

    return (TableScanDesc) scan;
}

static bool
cryo_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
    CryoScanDesc scan = (CryoScanDesc) sscan;
    CryoDataHeader *hdr = (CryoDataHeader *) scan->data;

    ExecClearTuple(slot);

read_tuple:
    if ((scan->cur_item + 1) * sizeof(CryoItemId) < hdr->lower)
    {
        /* read tuple */
        /* TODO: move this logic to storage.c */
        CryoItemId *item = (CryoItemId *) hdr->data + scan->cur_item;
        HeapTuple tuple;

        tuple = palloc0(sizeof(HeapTupleData));
        tuple->t_len = item->len;
        tuple->t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
        ItemPointerSet(&tuple->t_self, scan->cur_block, scan->cur_item + 1);
        tuple->t_data = (HeapTupleHeader) (scan->data + item->off);

        ExecStoreHeapTuple(tuple, slot, false);

        scan->cur_item++;

        return true;
    }
    else
    {
        Relation rel = scan->rs_base.rs_rd;

        scan->cur_block += scan->nblocks;
        /* TODO: use metapage->target_block instead of number of blocks */
        if (RelationGetNumberOfBlocks(rel) <= scan->cur_block)
            return false;

        cryo_read_data(rel, scan->cur_block, &scan->nblocks, scan->data);
        scan->cur_item = 0;
        goto read_tuple;
    }

    return false;
}

static void
cryo_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    NOT_IMPLEMENTED;
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

typedef struct
{
    IndexFetchTableData base;
    char    data[CRYO_BLCKSZ];
} IndexFetchCryoData;

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

static bool
cryo_index_fetch_tuple(struct IndexFetchTableData *scan,
                       ItemPointer tid,
                       Snapshot snapshot,
                       TupleTableSlot *slot,
                       bool *call_again, bool *all_dead)
{
    IndexFetchCryoData *cscan = (IndexFetchCryoData *) scan;
    CryoDataHeader     *hdr = (CryoDataHeader *) cscan->data;
    HeapTuple   tuple;
    bool isnull = false;

    /* TODO: add some kind of buffer cache */
    cryo_read_data(cscan->base.rel,
                   ItemPointerGetBlockNumber(tid),
                   NULL,
                   cscan->data);

    /* position in item pointer starts with 1 */
    tuple = cryo_storage_fetch(hdr, ItemPointerGetOffsetNumber(tid) - 1);
    ExecStoreHeapTuple(tuple, slot, false);
    pfree(tuple);

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
cryo_load_meta(Relation rel)
{
    Buffer          metabuf;
    CryoMetaPage   *metapage;

    if (RelationGetNumberOfBlocks(rel) == 0)
    {
        GenericXLogState *xlogState = GenericXLogStart(rel);

        /* This is a brand new relation. Initialize a metapage */
        metabuf = ReadBuffer(rel, P_NEW);
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
        metapage = (CryoMetaPage *)
            GenericXLogRegisterBuffer(xlogState, metabuf,
                                      GENERIC_XLOG_FULL_IMAGE);

        /*
         * Can't leave pd_upper = 0 because then page will be considered new
         * (see PageIsNew) and won't pass PageIsVerified check
         */
        metapage->base.pd_upper = sizeof(CryoPageHeader);
        metapage->base.pd_lower = metapage->base.pd_upper;
        metapage->base.pd_special = BLCKSZ;

        /* No target block yet */
        metapage->target_block = 0;

        GenericXLogFinish(xlogState);
        UnlockReleaseBuffer(metabuf);
        //MarkBufferDirty(metabuf); -- done automatically by GenericXLogFinish()
    }

    metabuf = ReadBuffer(rel, CRYO_META_PAGE);
    metapage = (CryoMetaPage *) BufferGetPage(metabuf);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    modifyState.target_block = metapage->target_block;

    return metabuf;
}

static void
cryo_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                  int options, BulkInsertState bistate)
{
    NOT_IMPLEMENTED;
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
 * Compress and store data in postgres buffers, write WAL and all that stuff.
 * If advance flag is set true, then also shift target_block of the state to
 * the next block after the last written page.
 */
static void
cryo_preserve(CryoModifyState *state, bool advance)
{
    GenericXLogState   *xlog_state;
    CryoMetaPage       *metapage;
    Relation    rel = state->relation;
    Size        size;
    char       *compressed, *p;
    Size        page_content_size = BLCKSZ - sizeof(CryoPageHeader);
    int         npages;
    Buffer     *buffers;
    int         i;
    BlockNumber block = state->target_block;

    p = compressed = cryo_compress(state->data, &size);

    /* split data into pages */
    npages = (size + page_content_size - 1) / page_content_size; /* round up */
    buffers = palloc(npages * sizeof(Buffer));

    block = MAX(1, block);

    for (i = 0; i < npages; ++i)
    {
        /* read buffer or create new blocks if they are not there */
        if (RelationGetNumberOfBlocks(rel) > block + i)
            buffers[i] = ReadBuffer(rel, block + i);
        else
            buffers[i] = ReadBuffer(rel, P_NEW);
        LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
    }

    for (i = 0; i < npages; ++i)
    {
        CryoPageHeader *hdr;

        xlog_state = GenericXLogStart(state->relation);
        hdr = (CryoPageHeader *)
            GenericXLogRegisterBuffer(xlog_state, buffers[i],
                                      GENERIC_XLOG_FULL_IMAGE);
        hdr->npages = npages;
        hdr->curpage = i;
        hdr->compressed_size = size;
        hdr->base.pd_checksum = 0; /* TODO */
        /*
         * Can't leave pd_upper = 0 because then page will be considered new
         * (see PageIsNew) and won't pass PageIsVerified check
         */
        hdr->base.pd_upper = hdr->base.pd_lower = sizeof(CryoPageHeader);
        hdr->base.pd_special = BLCKSZ;
        memcpy((char *) hdr + sizeof(CryoPageHeader),
               p,
               MIN(page_content_size, size));

        size -= page_content_size;
        p += page_content_size;

        MarkBufferDirty(buffers[i]);
        GenericXLogFinish(xlog_state);
    }

    /* update metadata */
    xlog_state = GenericXLogStart(state->relation);
    metapage = (CryoMetaPage *)
        GenericXLogRegisterBuffer(xlog_state, state->metabuf,
                                  GENERIC_XLOG_FULL_IMAGE);
    metapage->target_block = state->target_block = block;
    MarkBufferDirty(state->metabuf);
    GenericXLogFinish(xlog_state);

    /* Release all affected buffers */
    for (i = 0; i < npages; ++i)
        UnlockReleaseBuffer(buffers[i]);

    if (advance)
        state->target_block = state->target_block + i;
}

static void
cryo_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                  CommandId cid, int options, BulkInsertState bistate)
{
    CryoDataHeader *hdr;
    int             i;

    hdr = (CryoDataHeader *) modifyState.data;

    /* initialize modify state */
    modifyState.metabuf = cryo_load_meta(relation);
    if (modifyState.target_block == 0)
    {
        /* This is a new block */
        cryo_init_page(hdr);
    }
    else
    {
        /* 
         * TODO: in order to ensure that storage is  consistent its better
         * to create entirely new block instead of adding tuples to an
         * existing one
         */

        /* Read the target block contents into modifyState.data */
        cryo_read_data(relation, modifyState.target_block, NULL, modifyState.data);
    }
    modifyState.relation = relation;
    modifyState.modified = false;

    for (i = 0; i < ntuples; ++i)
    {
        HeapTuple   tuple;
        int         pos;

        tuple = ExecCopySlotHeapTuple(slots[i]);
        if ((pos = cryo_storage_insert(hdr, tuple)) < 0)
        {
            /* 
             * Compress and flush current block to the disk (if needed),
             * create a new one and retry insertion
             */
            if (modifyState.modified)
            {
                cryo_preserve(&modifyState, true);
                modifyState.modified = false;
            }
            cryo_init_page(hdr);
            if ((pos = cryo_storage_insert(hdr, tuple)) < 0)
            {
                elog(ERROR, "tuple is too large to fit the cryo block");
            }
        }
        modifyState.modified = true;

        slots[i]->tts_tableOid = RelationGetRelid(relation);
        /* position in item pointer starts with 1 */
        ItemPointerSet(&slots[i]->tts_tid, modifyState.target_block, pos + 1);
    }
    cryo_preserve(&modifyState, false);
}

static void
cryo_finish_bulk_insert(Relation rel, int options)
{
    modifyState.modified = false;
    UnlockReleaseBuffer(modifyState.metabuf);
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
    NOT_IMPLEMENTED;
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
    NOT_IMPLEMENTED;
}

static bool
cryo_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    NOT_IMPLEMENTED;
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
    *pages = 0;
    *tuples = 0;
    *allvisfrac = 0;
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
    NOT_IMPLEMENTED;
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
    .scan_sample_next_tuple = cryo_scan_sample_next_tuple

};

Datum
cryo_tableam_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(&cryo_methods);
}
