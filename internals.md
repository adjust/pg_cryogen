# Internals of `pg_cryogen` storage

Top level overview of the iternals.

## Storage

`pg_cryogen` uses blocks of size `CRYO_BLCKSZ` (1 Mb), which before being flushed to disk are compressed, split into 8 Kb pages (chunks) and stored into postgres' buffer manager. Each page contains block number of the first and the next page. Thereby pages do not neccesseraly go one after another.

In case of concurrent writes the first page may be separated from the rest. The reason for this is that pluggable storage API does not have `end-modify-table` routine (as FDW API, for instance, has) and we have to use so called transaction callback to finilize our writes. Therefore writes are postponed till the end of transactions. There is a problem with that approach: we have to somehow allocate the block number in order to use it in ItemPointers otherwise two different transaction may start using the same block number for their index tuples. To prevent that the share exclusive lock can be used, but in this case the entire relation will be locked preventing concurrent transactions to progress. Another option is to allocate block before actual writing to it. The cons are that this requires more complex page layout (having `next` pointer for each page) and hence more complex seqscan algorithm which requires keeping track of pages that have already been read.

```
                                                  next
                                ┌──────────────────────────────────────┐ 
                                │                                      │
┌───────────┐┌───────────┐┌─────┴─────┐┌───────────┐┌───────────┐┌─────┴─────┐
│ metapage  ││ chunk A.1 ││ chunk B.1 ││ chunk A.2 ││ chunk A.3 ││ chunk B.2 │
└───────────┘└─────┬─────┘└───────────┘└───┬───┬───┘└─────┬─────┘└───────────┘
                   │                       │   │          │
                   └───────────────────────┘   └──────────┘
                              next                 next
```

## Cache

In order to minimize the number decompression operations, especially during index/bitmap scans, we use local per-backend cache consisting of `CACHE_SIZE` (currently 16) slots. Whenever block is requested we first look it up in the `pagemap` hashtapble using (relid, blockno) as a key and get a cache slot number from it. If block is not in the cache we read it from the buffer manager, decompress it and store into available cache slot. Or if there are no available slots we evict some block using the LRU strategy.

We also use cache when inserting new data. In this case we mark cache slot as pinned which prevents it from being evicted. This also allows us to read inserted data in subsequent `SELECT` operation even that those records are not persisted on the storage device yet.
