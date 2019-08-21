#include "postgres.h"


typedef CryoScanDescData
{
    TableScanDescData   rs_base;
} CryoScanDescData;
typedef CryoScanDescData *CryoScanDesc;

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

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (HeapScanDesc) palloc(sizeof(HeapScanDescData));

    scan->rs_base.rs_rd = relation;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_flags = flasg;

    return NULL;
}

bool
heap_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

    ExecClearTuple(slot);

    return false;
}

static void
heap_endscan(TableScanDesc sscan)
{
	CryoScanDesc scan = (HeapScanDesc) sscan;

	/*
	 * decrement relation reference count and free scan descriptor storage
	 */
	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	pfree(scan);
}

static const TableAmRoutine cryo_methods = 
{
    .type = T_TableAmRoutine,

    .slot_callbacks = cryo_slot_callbacks,

    .scan_begin = cryo_begin_scan;
    .scan_getnextslot = cryo_getnextslot;
    .scan_end = cryo_end;
}
