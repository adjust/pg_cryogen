#include "postgres.h"

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/smgr.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"


PG_MODULE_MAGIC;


typedef struct CryoScanDescData
{
    TableScanDescData   rs_base;
} CryoScanDescData;
typedef CryoScanDescData *CryoScanDesc;


PG_FUNCTION_INFO_V1(cryo_tableam_handler);

static const TupleTableSlotOps *
cryo_slot_callbacks(Relation relation)
{
    return &TTSOpsBufferHeapTuple;
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

    return (TableScanDesc) scan;
}

static bool
cryo_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
    ExecClearTuple(slot);

    return false;
}

static void
cryo_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    elog(ERROR, "not implemented");
}

static void
cryo_endscan(TableScanDesc sscan)
{
    CryoScanDesc scan = (CryoScanDesc) sscan;

    /*
     * decrement relation reference count and free scan descriptor storage
     */
    RelationDecrementReferenceCount(scan->rs_base.rs_rd);

    if (scan->rs_base.rs_key)
        pfree(scan->rs_base.rs_key);

    if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
        UnregisterSnapshot(scan->rs_base.rs_snapshot);

    pfree(scan);
}

static IndexFetchTableData *
cryo_index_fetch_begin(Relation rel)
{
    elog(ERROR, "not implemented");
}

static void
cryo_index_fetch_reset(IndexFetchTableData *scan)
{
    elog(ERROR, "not implemented");
}

static void
cryo_index_fetch_end(IndexFetchTableData *scan)
{
    elog(ERROR, "not implemented");
}

static bool
cryo_index_fetch_tuple(struct IndexFetchTableData *scan,
                         ItemPointer tid,
                         Snapshot snapshot,
                         TupleTableSlot *slot,
                         bool *call_again, bool *all_dead)
{
    elog(ERROR, "not implemented");
}

static bool
cryo_fetch_row_version(Relation relation,
                         ItemPointer tid,
                         Snapshot snapshot,
                         TupleTableSlot *slot)
{
    elog(ERROR, "not implemented");
}

static bool
cryo_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    elog(ERROR, "not implemented");
}

static bool
cryo_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                Snapshot snapshot)
{
    elog(ERROR, "not implemented");
}

static void
cryo_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                    int options, BulkInsertState bistate)
{
    elog(ERROR, "not implemented");
}

static void
cryo_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                CommandId cid, int options,
                                BulkInsertState bistate, uint32 specToken)
{
    elog(ERROR, "not implemented");
}

static void
cryo_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                  uint32 specToken, bool succeeded)
{
    elog(ERROR, "not implemented");
}

static void
cryo_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                  CommandId cid, int options, BulkInsertState bistate)
{
    elog(ERROR, "not implemented");
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
    elog(ERROR, "not implemented");
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
    elog(ERROR, "not implemented");
}

static void
cryo_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
    elog(ERROR, "not implemented");
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
    elog(ERROR, "not implemented");
}

static bool
cryo_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                             BufferAccessStrategy bstrategy)
{
    elog(ERROR, "not implemented");
}

static bool
cryo_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    elog(ERROR, "not implemented");
}

static double
cryo_index_build_range_scan(Relation relation,
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
    elog(ERROR, "not implemented");
}

static void
cryo_index_validate_scan(Relation relation,
                         Relation indexRelation,
                         IndexInfo *indexInfo,
                         Snapshot snapshot,
                         ValidateIndexState *state)
{
    elog(ERROR, "not implemented");
}

static uint64
cryo_relation_size(Relation rel, ForkNumber forkNumber)
{
    elog(ERROR, "not implemented");
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
    elog(ERROR, "not implemented");
}

static bool
cryo_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
                              TupleTableSlot *slot)
{
    elog(ERROR, "not implemented");
}

static void
cryo_vacuum_rel(Relation onerel, VacuumParams *params,
                BufferAccessStrategy bstrategy)
{
    elog(ERROR, "not implemented");
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
    .tuple_insert_speculative = cryo_tuple_insert_speculative,
    .tuple_complete_speculative = cryo_tuple_complete_speculative,
    .multi_insert = cryo_multi_insert,
    .tuple_delete = cryo_tuple_delete,
    .tuple_update = cryo_tuple_update,
    .tuple_lock = cryo_tuple_lock,

    .tuple_fetch_row_version = cryo_fetch_row_version,
    .tuple_tid_valid = cryo_tuple_tid_valid,
    .tuple_satisfies_snapshot = cryo_tuple_satisfies_snapshot,

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
