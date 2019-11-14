CREATE EXTENSION pg_cryogen;

CREATE TEMPORARY TABLE tmp_data
AS SELECT gs as id, md5(gs::TEXT) as msg FROM generate_series(1, 500) as gs;

COPY tmp_data TO '/tmp/pg_cryogen_data.csv' WITH csv;

CREATE TABLE data (id INT4 NOT NULL, msg TEXT) USING pg_cryogen;
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
SET enable_seqscan = t;

SET pg_cryogen.compression_method = 'lz4';
COPY data FROM '/tmp/pg_cryogen_data.csv' WITH csv;
SELECT count(*) FROM data;

VACUUM data;

-- inserts
TRUNCATE data;
INSERT INTO data
SELECT gs as id, md5(gs::TEXT) as msg FROM generate_series(1, 500) as gs;

-- check FKs
DROP INDEX data_id_idx;
CREATE UNIQUE INDEX data_id_idx ON data USING btree (id);
CREATE TABLE ref_table (id SERIAL, data_id INT4 REFERENCES data (id));
INSERT INTO ref_table VALUES (1, 50), (2, 100), (3, 250); -- cryo_tuple_lock is called here
EXPLAIN (COSTS OFF) SELECT * FROM data JOIN ref_table ON data_id = data.id;
SELECT * FROM data JOIN ref_table ON data_id = data.id LIMIT 3;
DROP TABLE ref_table;
DROP INDEX data_id_idx;

-- check rescan
CREATE TABLE other_table AS
SELECT gs as id, '2000-01-01'::DATE + gs as dt FROM generate_series(1, 500) as gs;
SET enable_hashjoin = f;
SET enable_mergejoin = f;
SET enable_material = f;
EXPLAIN (COSTS OFF) SELECT * FROM data JOIN other_table USING (id);
SELECT * FROM data JOIN other_table USING (id) LIMIT 3;
