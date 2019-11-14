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

-- test tables with wide records which would require more than one postgres page
-- for cryo block
CREATE TABLE json_data(data jsonb) USING pg_cryogen;
BEGIN;
INSERT INTO json_data VALUES ('{"test": 1}'::jsonb);
ROLLBACK;
INSERT INTO json_data
SELECT ('
{
    "id": ' || gs || ',
    "a": "' || md5(gs::text || 'a') || '",
    "b": "' || md5(gs::text || 'b') || '",
    "c": "' || md5(gs::text || 'c') || '",
    "d": "' || md5(gs::text || 'd') || '",
    "e": "' || md5(gs::text || 'e') || '",
    "f": "' || md5(gs::text || 'f') || '",
    "g": "' || md5(gs::text || 'g') || '",
    "h": "' || md5(gs::text || 'h') || '",
    "i": "' || md5(gs::text || 'i') || '",
    "j": "' || md5(gs::text || 'j') || '",
    "k": "' || md5(gs::text || 'k') || '",
    "l": "' || md5(gs::text || 'l') || '",
    "m": "' || md5(gs::text || 'm') || '",
    "n": "' || md5(gs::text || 'n') || '",
    "o": "' || md5(gs::text || 'o') || '",
    "p": "' || md5(gs::text || 'p') || '",
    "q": "' || md5(gs::text || 'q') || '",
    "r": "' || md5(gs::text || 'r') || '",
    "s": "' || md5(gs::text || 's') || '",
    "t": "' || md5(gs::text || 't') || '",
    "u": "' || md5(gs::text || 'u') || '",
    "v": "' || md5(gs::text || 'v') || '",
    "w": "' || md5(gs::text || 'w') || '",
    "x": "' || md5(gs::text || 'x') || '",
    "y": "' || md5(gs::text || 'y') || '",
    "z": "' || md5(gs::text || 'z') || '"
}')::jsonb
FROM generate_series(1, 300) as gs;
SELECT count(*) FROM json_data;
