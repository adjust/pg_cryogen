#!/usr/bin/env bash

set -ux

# global exports
export PGPORT=55435
export CODECOV_TOKEN="26c616a7-e2aa-4b0e-a0c1-416b63e3457c"

# build extension
make install CFLAGS='-coverage'

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

# report to codecov
gcov *.c *.h
bash <(curl -s https://codecov.io/bash)

set +ux

