#include "postgres.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "access/generic_xlog.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "parser/parse_relation.h"
#include "postmaster/autovacuum.h"
#include "storage/freespace.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_cryogen.h"
#include "cache.h"
#include "storage.h"


#define MAXDEADTUPLES(max_size) \
		(((max_size) - offsetof(TIDList, itemptrs)) / sizeof(ItemPointerData))
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage
#define SizeOfDeadTuples(cnt) \
	add_size(offsetof(TIDList, itemptrs), \
			 mul_size(sizeof(ItemPointerData), cnt))


typedef struct TIDList
{
	int			max_tuples;		/* # slots allocated in array */
	int			num_tuples;		/* current # of entries */
	ItemPointerData itemptrs[FLEXIBLE_ARRAY_MEMBER];
} TIDList;


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

    return tbm;
}

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
    TIDBitmap  *tbm_lt, *tbm_ge;
    List       *border_blocks = NIL;

    tupdesc = RelationGetDescr(irel);
    atttype = TupleDescAttr(tupdesc, attno - 1)->atttypid;

    tbm_lt = index_get_bitmap(irel, atttype, valtype, val, BTLessStrategyNumber);
    tbm_ge = index_get_bitmap(irel, atttype, valtype, val, BTGreaterEqualStrategyNumber);

    /*
     * Find blocks on intersection of both bitmaps. Given the comment on
     * tbm_iterate() we assume that block numbers are ordered.
     */
    if (tbm_lt && tbm_ge)
    {
        TBMIterator *it1, *it2;
        TBMIterateResult *res1, *res2;

        it1 = tbm_begin_iterate(tbm_lt);
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

            res1 = tbm_iterate(it1);
        }

        tbm_end_iterate(it2);
        tbm_end_iterate(it1);
    }

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
    }

    return tbm_lt;
}

/*
 * Copied from lazyvacuum.c
 */
static long
compute_max_tuples(BlockNumber relblocks, bool useindex)
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

static TIDList *
tidlist_alloc(BlockNumber relblocks)
{
    TIDList    *tuples = NULL;
    long		maxtuples;

    maxtuples = compute_max_tuples(relblocks, true);

    tuples = (TIDList *) palloc(SizeOfDeadTuples(maxtuples));
    tuples->num_tuples = 0;
    tuples->max_tuples = (int) maxtuples;

    return tuples;
}

static void
tidlist_record_item(TIDList *tuples, BlockNumber blockno, OffsetNumber offset)
{
    /*
     * The array shouldn't overflow under normal behavior, but perhaps it
     * could if we are given a really small maintenance_work_mem. In that
     * case, just forget the last few tuples (we'll get 'em next time).
     */
    if (tuples->num_tuples < tuples->max_tuples)
    {
        ItemPointerSet(&tuples->itemptrs[tuples->num_tuples], blockno, offset);
        tuples->num_tuples++;
        // pgstat_progress_update_param(PROGRESS_VACUUM_NUM_DEAD_TUPLES,
        //                              tuples->num_tuples);
    }
}

static int
cmp_itemptr(const void *left, const void *right)
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
cmp_blockno(const void *left, const void *right)
{
    return (*(BlockNumber *) left) < (*(BlockNumber *) right) ? -1 :
        (*(BlockNumber *) left) == (*(BlockNumber *) right) ? 0 : 1;
}

static bool
tid_reaped(ItemPointer itemptr, void *state)
{
	TIDList    *tuples = (TIDList *) state;
	ItemPointer res;

	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) tuples->itemptrs,
								tuples->num_tuples,
								sizeof(ItemPointerData),
								cmp_itemptr);

	return (res != NULL);
}

static void
vacuum_indexes(Relation *irels, IndexBulkDeleteResult **stats,
                    int nindexes, TIDList *tuples)
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

        stats[i] = index_bulk_delete(&ivinfo, stats[i], tid_reaped, (void *) tuples);
    }
}

static void
drop_blocks(Relation rel, List *blocks)
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
    FreeSpaceMapVacuum(rel);
}

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

    /* Does this relation use pg_cryogen as storage? */
    if (rel->rd_tableam != &CryoAmRoutine)
    {
        elog(ERROR,
             "relation \"%s\" does not use pg_cryogen as storage engine",
             RelationGetRelationName(rel));
    }

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
        irels[nindexes++] = irel;

        Assert(index->indnatts > 0);

        /* indkey.values[0] == 0 means it's an expression */
        if (index->indkey.values[0] == attnum && !key_index)
        {
            key_index = irel;

            elog(DEBUG1, "use index \"%s\" for the key \"%s\"",
                 RelationGetRelationName(irel), attname);
        }
    }

    if (!key_index)
        elog(ERROR, "index for key \"%s\" is not found", attname);

    tbm = index_extract_tids(key_index, val, valtype,
                             &border_blocks, &nborder_blocks);
    if (tbm)
    {
        TBMIterator    *iter;
        TBMIterateResult *tbmres;
        TIDList        *tuples;
        List           *dead_blocks = NIL;
        
        tuples = tidlist_alloc(RelationGetNumberOfBlocks(rel));
        
        iter = tbm_begin_iterate(tbm);

        while((tbmres = tbm_iterate(iter)) != NULL)
        {
            BlockNumber *blockno_ptr;

            if (border_blocks)
            {
                blockno_ptr = bsearch((void *) &tbmres->blockno,
                                      (void *) border_blocks,
                                      nborder_blocks,
                                      sizeof(BlockNumber),
                                      cmp_blockno);

                /* If current block is one of border blocks - skip it */
                if (blockno_ptr)
                    continue;
            }

            /* Did tuples struct reach max capacity? */
            if ((tuples->max_tuples - tuples->num_tuples) < MaxHeapTuplesPerPage &&
                tuples->num_tuples > 0)
            {
                /* TODO: update index stats */
                elog(DEBUG1,
                     "tuples struct at max capacity; cleanup indexes "
                     "and delete pages");
                vacuum_indexes(irels, indstats, nindexes, tuples);
                drop_blocks(rel, dead_blocks);
                list_free(dead_blocks);
                dead_blocks = NIL;
            }

            /*
             * Got lossy result meaning we didn't get item pointers within
             * tidbitmap and have to read them directly from the cryo page.
             */
            if (tbmres->ntuples == -1)
            {
                CacheEntry  cacheEntry;
                CryoError   err;
                CryoDataHeader *hdr;
                int         item = 1;

                err = cryo_read_data(rel, NULL, tbmres->blockno, &cacheEntry);
                hdr = (CryoDataHeader *) cryo_cache_get_data(cacheEntry);

                while (item * sizeof(CryoItemId) < hdr->lower)
                {
                    tidlist_record_item(tuples, tbmres->blockno, item++);
                }
            }
            else
            {
                for (int i = 0; i < tbmres->ntuples; ++i)
                {
                    tidlist_record_item(tuples, tbmres->blockno, tbmres->offsets[i]);
                }
            }
            dead_blocks = lappend_int(dead_blocks, tbmres->blockno);
        }

        /* TODO: update index stats */
        vacuum_indexes(irels, indstats, nindexes, tuples);
        drop_blocks(rel, dead_blocks);
        elog(DEBUG1, "final indexes cleanup and pages deletion");

        tbm_end_iterate(iter);
    }

    for (int i = 0; i < nindexes; i++)
    {
        index_close(irels[i], RowExclusiveLock);
    }

    relation_close(rel, RowExclusiveLock);
}
