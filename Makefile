MODULE_big = pg_cryogen
OBJS = pg_cryogen.o storage.o
PGFILEDESC = "pg_cryogen - append-only compressed storage"

SHLIB_LINK = -llz4

EXTENSION = pg_cryogen
DATA = pg_cryogen--0.1.sql

EXTRA_CLEAN = sql/pg_cryogen.sql expected/pg_cryogen.out $(REGRESSION_DATA)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
