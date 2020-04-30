ARG PG_VERSION
FROM postgres:${PG_VERSION}-alpine

# Environment
ENV LANG=C.UTF-8 PGDATA=/pg/data

# Install dependencies
RUN apk add --no-cache make musl-dev gcc clang lz4-dev zstd-dev curl git

# Make directories
RUN	mkdir -p ${PGDATA} && \
	mkdir -p /pg/testdir

# Add data to test dir
ADD . /pg/testdir

# Grant privileges
RUN	chown -R postgres:postgres ${PGDATA} && \
	chown -R postgres:postgres /pg/testdir && \
	chmod a+rwx /usr/local/lib/postgresql && \
	chmod a+rwx /usr/local/share/postgresql/extension

COPY tests/run_tests.sh /run_tests.sh
RUN chmod 755 /run_tests.sh

USER postgres
WORKDIR /pg/testdir
ENTRYPOINT /run_tests.sh
