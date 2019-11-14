CREATE EXTENSION pg_cryogen;

CREATE TEMPORARY TABLE tmp_data
AS SELECT gs as id, md5(gs::TEXT) as msg FROM generate_series(1, 500) as gs;

COPY tmp_data TO '/tmp/pg_cryogen_data.csv' WITH csv;

CREATE TABLE data (id INT4, msg TEXT) USING pg_cryogen;
COPY data FROM '/tmp/pg_cryogen_data.csv' WITH csv;

SELECT * FROM data LIMIT 10;
SELECT avg(id) FROM data;
SELECT count(*) FROM data;

CREATE INDEX data_id_idx ON data USING btree (id);
SET enable_seqscan = f;
ANALYZE data;
EXPLAIN (COSTS OFF) SELECT * FROM data WHERE id = 500;
SELECT * FROM data WHERE id = 500;
DROP INDEX data_id_idx;
CREATE INDEX data_id_idx ON data USING brin (id);
EXPLAIN (COSTS OFF) SELECT * FROM data WHERE id = 500;
SELECT * FROM data WHERE id = 500;

COPY data FROM '/tmp/pg_cryogen_data.csv' WITH csv;
SELECT count(*) FROM data;

VACUUM data;
