MODULE_big = pg_cryogen
OBJS = pg_cryogen.o storage.o cache.o compression.o scan_iterator.o
PGFILEDESC = "pg_cryogen - append-only compressed storage"

SHLIB_LINK = -llz4 -lzstd

EXTENSION = pg_cryogen
DATA = pg_cryogen--0.1.sql pg_cryogen--0.1--0.2.sql

REGRESS = pg_cryogen

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
