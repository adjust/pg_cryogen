[![Build Status](https://travis-ci.org/adjust/pg_cryogen.svg?branch=master)](https://travis-ci.org/adjust/pg_cryogen) ![experimental](https://img.shields.io/badge/status-experimental-orange)

# pg_cryogen

`pg_cryogen` is an append-only compressed pluggable storage.

## Dependencies

`pg_cryogen` requires `liblz4` and `libzstd` development packages installed.

## Build

After clone just run in command line:

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

`pg_cryogen` is an append only storage meaning that only INSERT and COPY commands are supported for data modification. Due to specifics of current implementation only inserts to a single table per transaction are possible. Also storage is optimized for bulk inserts and will create *at least* one separate block for each INSERT/COPY command.

Both index scan and bitmap scans are implemented.

### Compression parameters

`pg_cryogen` offers two compression methods: `lz4`, which provides on average better compression/decompression speed, and `zstd`, which provides better compression ratio. Since pluggable storage API currently does not provide ways to set custom parameters for storages, the only way to set them is by using GUCs:

* `pg_cryogen.compression_method`: possible options are `lz4` and `zstd` (default).
* `pg_cryogen.lz4_acceleration`: `lz4` specific parameter which specifies how fast compression would be; valid values are [1..50], where 1 is better compression ratio and higher value is better speed (default is 1);
* `pg_cryogen.zstd_compression_level`: `zstd` specific parameter for compression level; valid values are [-5..22] (default is 1).

Those GUCs can be set in either in postgresql.conf or for any particular session.

## Internals

Please refer to a separate [document](internals.md) for details.
