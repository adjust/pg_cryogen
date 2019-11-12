#!/usr/bin/env bash

set -ux

# global exports
export PGPORT=55435

# build extension
make install

# initialize database
initdb -D $PGDATA

# start cluster
pg_ctl start -l /tmp/postgres.log -w -o "-p $PGPORT"

# something's wrong, exit now!
[[ $? -ne 0 ]] && cat /tmp/postgres.log && exit 1;

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
make installcheck

# show diff if needed and exit if something's wrong
[[ $? -ne 0 ]] && { [[ -f regression.diffs ]] && cat regression.diffs ; exit 1 ; }

set +ux

