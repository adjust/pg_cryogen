[WIP]

# pg_cryogen

`pg_cryogen` is an append-only compressed pluggable storage.

## Dependencies

`pg_cryogen` requires `liblz4` installed.

## Build

Like with any other postgres extensions after clone just run in command line:

```
make install
```

Or if postgres binaries are not in the `PATH`:

```
make install PG_CONFIG=/path/to/pg_config
```

## Using

`pg_cryogen` implements pluggable storage API and hence requires PostgreSQL 12 or later. To use it as a storage it needs to be specified as an access method while creating a table. E.g.:

```sql
create extension pg_cryogen;
create table my_table (...) using pg_cryogen;
```

Due to the specifics of current implementation of pluggable storage API and storage layout of `pg_cryogen` it was only possible to implement bulk inserts via COPY command. Therefore regular inserts are not implemented.

Both index scan and bitmap scans are implemented as well.
