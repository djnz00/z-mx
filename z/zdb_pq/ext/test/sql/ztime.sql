SELECT - '42.01'::ztime;
SELECT - '0'::ztime;
SELECT - 'nan'::ztime;
SELECT '42.01'::ztime;
SELECT '-42.01'::ztime;
SELECT ''::ztime;
SELECT 'x'::ztime;
SELECT '42 x'::ztime;
SELECT - '999999999999999999.999999999999999999'::ztime;
SELECT  '-999999999999999999.999999999999999999'::ztime;
SELECT   '999999999999999999.999999999999999999'::ztime;
SELECT - '100000000000000000.000000000000000001'::ztime;
SELECT  '-100000000000000000.000000000000000001'::ztime;
SELECT   '100000000000000000.000000000000000001'::ztime;
SELECT - '1000000000000000000.000000000000000001'::ztime;
SELECT  '-1000000000000000000.000000000000000001'::ztime;
SELECT   '1000000000000000000.000000000000000001'::ztime;

SELECT ztime_sum(NULL::ztime, NULL::ztime);
SELECT ztime_sum(NULL::ztime, 1::ztime);
SELECT ztime_sum(2::ztime, NULL::ztime);
SELECT ztime_sum(2::ztime, 1::ztime);

SELECT sum(val::ztime) FROM (SELECT NULL::ztime WHERE false) _ (val);
SELECT sum(val::ztime) FROM (VALUES (1), (null), (2), (5)) _ (val);

SELECT avg(val::ztime) FROM (SELECT NULL::ztime WHERE false) _ (val);
SELECT avg(val::ztime) FROM (VALUES (1), (null), (2), (5), (6)) _ (val);
SELECT avg(val::ztime) FROM (VALUES (999999999999999999.999999999999999999), (-999999999999999999.999999999999999999)) _ (val);

CREATE TABLE test_ztime (x ztime);
CREATE UNIQUE INDEX test_ztime1_x_key ON test_ztime USING btree (x);
INSERT INTO test_ztime VALUES (-1), (0), (1), (2), (3), (4), (5), (null);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM test_ztime WHERE x = 3::ztime;
SELECT * FROM test_ztime WHERE x = 3::ztime;

EXPLAIN (COSTS OFF) SELECT * FROM test_ztime WHERE x = 3::ztime;
SELECT * FROM test_ztime WHERE x = 3::ztime;

DROP TABLE test_ztime;

RESET enable_seqscan;
RESET enable_bitmapscan;

SELECT '1'::ztime < '1'::ztime;
SELECT '5'::ztime < '2'::ztime;
SELECT '3'::ztime < '4'::ztime;
SELECT '1'::ztime <= '1'::ztime;
SELECT '5'::ztime <= '2'::ztime;
SELECT '3'::ztime <= '4'::ztime;
SELECT '1'::ztime = '1'::ztime;
SELECT '5'::ztime = '2'::ztime;
SELECT '3'::ztime = '4'::ztime;
SELECT '1'::ztime <> '1'::ztime;
SELECT '5'::ztime <> '2'::ztime;
SELECT '3'::ztime <> '4'::ztime;
SELECT '1'::ztime >= '1'::ztime;
SELECT '5'::ztime >= '2'::ztime;
SELECT '3'::ztime >= '4'::ztime;
SELECT '1'::ztime > '1'::ztime;
SELECT '5'::ztime > '2'::ztime;
SELECT '3'::ztime > '4'::ztime;

SELECT ztime_cmp('1'::ztime, '1'::ztime);
SELECT ztime_cmp('5'::ztime, '2'::ztime);
SELECT ztime_cmp('3'::ztime, '4'::ztime);
SELECT pg_typeof('1'::ztime + '1'::ztime);
SELECT '1'::ztime + '1'::ztime;
SELECT '3'::ztime + '4'::ztime;
SELECT '5'::ztime + '2'::ztime;
SELECT '-3'::ztime + '4'::ztime;
SELECT '-5'::ztime + '2'::ztime;
SELECT '3'::ztime + '-4'::ztime;
SELECT '5'::ztime + '-2'::ztime;
SELECT '127'::ztime + '127'::ztime;
SELECT pg_typeof('1'::ztime - '1'::ztime);
SELECT '1'::ztime - '1'::ztime;
SELECT '3'::ztime - '4'::ztime;
SELECT '5'::ztime - '2'::ztime;
SELECT '-3'::ztime - '4'::ztime;
SELECT '-5'::ztime - '2'::ztime;
SELECT '3'::ztime - '-4'::ztime;
SELECT '5'::ztime - '-2'::ztime;
SELECT pg_typeof('1'::ztime * '1'::ztime);
SELECT '1'::ztime * '1'::ztime;
SELECT '3'::ztime * '4'::ztime;
SELECT '5'::ztime * '2'::ztime;
SELECT '-3'::ztime * '4'::ztime;
SELECT '-5'::ztime * '2'::ztime;
SELECT '3'::ztime * '-4'::ztime;
SELECT '5'::ztime * '-2'::ztime;
SELECT '127'::ztime * '127'::ztime;
SELECT pg_typeof('1'::ztime / '1'::ztime);
SELECT '1'::ztime / '1'::ztime;
SELECT '3'::ztime / '4'::ztime;
SELECT '5'::ztime / '2'::ztime;
SELECT '-3'::ztime / '4'::ztime;
SELECT '-5'::ztime / '2'::ztime;
SELECT '3'::ztime / '-4'::ztime;
SELECT '5'::ztime / '-2'::ztime;
SELECT '5'::ztime / '0'::ztime;

SELECT CAST('42.01'::ztime AS int8);
SELECT CAST('-42.01'::ztime AS int8);

SELECT CAST('42.01'::ztime AS double precision);
SELECT CAST('-42.01'::ztime AS double precision);

SELECT CAST('42.01'::ztime AS numeric);
SELECT CAST('-42.01'::ztime AS numeric);

SELECT ztime_hash(42.01);
SELECT ztime_hash(42.01::ztime);
