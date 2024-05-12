CREATE TYPE ztime;

CREATE FUNCTION ztime_in(cstring) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_in';

CREATE FUNCTION ztime_out(ztime) RETURNS cstring
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_out';

CREATE FUNCTION ztime_recv(internal) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_recv';

CREATE FUNCTION ztime_send(ztime) RETURNS bytea
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_send';

CREATE TYPE ztime (
  INPUT = ztime_in,
  OUTPUT = ztime_out,
  RECEIVE = ztime_recv,
  SEND = ztime_send,
  INTERNALLENGTH = 16,
  ALIGNMENT = double
);

CREATE FUNCTION ztime_to_int8(ztime) RETURNS int8
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_to_int8';

CREATE FUNCTION ztime_from_int8(int8) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_from_int8';

CREATE FUNCTION ztime_to_float8(ztime) RETURNS double precision
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_to_float8';

CREATE FUNCTION ztime_from_float8(double precision) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_from_float8';

CREATE CAST (int8 AS ztime)
  WITH FUNCTION ztime_from_int8 AS ASSIGNMENT;
CREATE CAST (double precision AS ztime)
  WITH FUNCTION ztime_from_float8 AS ASSIGNMENT;
CREATE CAST (numeric AS ztime) WITH INOUT AS ASSIGNMENT;

CREATE CAST (ztime AS int8)
  WITH FUNCTION ztime_to_int8 AS IMPLICIT;
CREATE CAST (ztime AS double precision)
  WITH FUNCTION ztime_to_float8 AS IMPLICIT;
CREATE CAST (ztime AS numeric) WITH INOUT AS IMPLICIT;

CREATE FUNCTION ztime_neg(ztime) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_neg';

CREATE OPERATOR - (
  PROCEDURE = ztime_neg,
  RIGHTARG = ztime
);

CREATE FUNCTION ztime_add(ztime, ztime) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_add';

CREATE OPERATOR + (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = +,
  PROCEDURE = ztime_add
);

CREATE FUNCTION ztime_sub(ztime, ztime) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_sub';

CREATE OPERATOR - (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  PROCEDURE = ztime_sub
);

CREATE FUNCTION ztime_mul(ztime, double precision) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_mul';

CREATE OPERATOR * (
  LEFTARG = ztime,
  RIGHTARG = double precision,
  COMMUTATOR = *,
  PROCEDURE = ztime_mul
);

CREATE FUNCTION ztime_div(ztime, double precision) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_div';

CREATE OPERATOR / (
  LEFTARG = ztime,
  RIGHTARG = double precision,
  PROCEDURE = ztime_div
);

CREATE FUNCTION ztime_lt(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_lt';

CREATE OPERATOR < (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = >,
  NEGATOR = >=,
  RESTRICT = scalarltsel,
  JOIN = scalarltjoinsel,
  PROCEDURE = ztime_lt
);

CREATE FUNCTION ztime_le(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_le';

CREATE OPERATOR <= (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = >=,
  NEGATOR = >,
  RESTRICT = scalarltsel,
  JOIN = scalarltjoinsel,
  PROCEDURE = ztime_le
);

CREATE FUNCTION ztime_eq(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_eq';

CREATE OPERATOR = (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = =,
  NEGATOR = <>,
  RESTRICT = eqsel,
  JOIN = eqjoinsel,
  HASHES,
  MERGES,
  PROCEDURE = ztime_eq
);

CREATE FUNCTION ztime_ne(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_ne';

CREATE OPERATOR <> (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = <>,
  NEGATOR = =,
  RESTRICT = neqsel,
  JOIN = neqjoinsel,
  PROCEDURE = ztime_ne
);

CREATE FUNCTION ztime_ge(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_ge';

CREATE OPERATOR >= (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = <=,
  NEGATOR = <,
  RESTRICT = scalargtsel,
  JOIN = scalargtjoinsel,
  PROCEDURE = ztime_ge
);

CREATE FUNCTION ztime_gt(ztime, ztime) RETURNS boolean
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_gt';

CREATE OPERATOR > (
  LEFTARG = ztime,
  RIGHTARG = ztime,
  COMMUTATOR = <,
  NEGATOR = <=,
  RESTRICT = scalargtsel,
  JOIN = scalargtjoinsel,
  PROCEDURE = ztime_gt
);

CREATE FUNCTION ztime_cmp(ztime, ztime) RETURNS integer
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_cmp';

CREATE FUNCTION ztime_sort(internal) RETURNS void
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_sort';

CREATE OPERATOR CLASS ztime_ops
  DEFAULT FOR TYPE ztime USING btree AS
    OPERATOR	1	< ,
    OPERATOR	2	<= ,
    OPERATOR	3	= ,
    OPERATOR	4	>= ,
    OPERATOR	5	> ,
    FUNCTION	1	ztime_cmp(ztime, ztime),
    FUNCTION	2	ztime_sort(internal);

CREATE FUNCTION ztime_hash(ztime) RETURNS int4
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_hash';

CREATE OPERATOR CLASS ztime_ops
  DEFAULT FOR TYPE ztime USING hash AS
    OPERATOR	1	=,
    FUNCTION	1	ztime_hash(ztime);

CREATE FUNCTION ztime_smaller(ztime, ztime) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_smaller';

CREATE AGGREGATE min(ztime) (
  SFUNC = ztime_smaller,
  STYPE = ztime,
  SORTOP = <
);

CREATE FUNCTION ztime_larger(ztime, ztime) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_larger';

CREATE AGGREGATE max(ztime) (
  SFUNC = ztime_larger,
  STYPE = ztime,
  SORTOP = >
);

CREATE FUNCTION ztime_sum(ztime, ztime) RETURNS ztime
  IMMUTABLE LANGUAGE C
  AS '$libdir/ztime', 'ztime_sum';

CREATE AGGREGATE sum(ztime) (SFUNC = ztime_sum, STYPE = ztime);

CREATE FUNCTION ztime_acc(ztime[], ztime) RETURNS ztime[]
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_acc';

CREATE FUNCTION ztime_avg(ztime[]) RETURNS ztime
  IMMUTABLE STRICT LANGUAGE C
  AS '$libdir/ztime', 'ztime_avg';

CREATE AGGREGATE avg(ztime) (
  SFUNC = ztime_acc,
  STYPE = ztime[],
  FINALFUNC = ztime_avg,
  INITCOND = '{0,0}'
);
