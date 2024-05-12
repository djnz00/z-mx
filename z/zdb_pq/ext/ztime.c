#include <postgres.h>
#include <fmgr.h>
#include <libpq/pqformat.h>
#include <utils/array.h>
#include <utils/sortsupport.h>
#include <port/pg_bswap.h>

#include <zlib/zu_time.h>

inline static bool isspace__(char c) {
  return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

PG_FUNCTION_INFO_V1(ztime_in);
Datum ztime_in(PG_FUNCTION_ARGS) {
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  const char *s = PG_GETARG_CSTRING(0);
  unsigned int n;

  /* postgres uses NaN; meanwhile ZuDecimal intentionally omits
   * positive/negative infinity */
  if (unlikely(s[0] == 'N' && s[1] == 'a' && s[2] == 'N' && s[3] == '\0')) {
    v->value = zu_time_null();
    PG_RETURN_POINTER(v);
  }

  /* postgres numeric supports inputs like 0x, 0o, 0b for hex/octal/binary,
   * this would be mis-use of the type, intentionally omitted here */
  n = zu_time_in(v, s);

  /* SQL requires trailing spaces to be ignored while erroring out on other
   * "trailing junk"; together with postgres reliance on C string
   * null-termination, this prevents incrementally parsing values within
   * a containing string without copying or overwriting the data with
   * null terminators, but we'll play along, sigh */
  if (likely(n)) while (unlikely(isspace__(s[n]))) ++n;
  if (!n || s[n])
    ereport(
      ERROR,
      (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
       errmsg("invalid input syntax for ztime: \"%s\"", s))
    );

  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_out);
Datum ztime_out(PG_FUNCTION_ARGS) {
  const zu_time *v = (const zu_time *)PG_GETARG_POINTER(0);
  unsigned int n = zu_time_out_len(v);
  char *s = palloc(n);
  zu_time_out(s, v);
  /* postgres uses NaN */
  if (s[0] == 'n' && s[1] == 'a' && s[2] == 'n' && s[3] == '\0')
    s[0] = 'N', s[2] = 'N';
  PG_RETURN_CSTRING(s);
}

PG_FUNCTION_INFO_V1(ztime_recv);
Datum ztime_recv(PG_FUNCTION_ARGS) {
  StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  pq_copymsgbytes(buf, (char *)v, 16);
  v->value = pg_bswap128(v->value);
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_send);
Datum ztime_send(PG_FUNCTION_ARGS) {
  const zu_time *v = (const zu_time *)PG_GETARG_POINTER(0);
  uint128_t value = v->value;
  StringInfoData buf;
  pq_begintypsend(&buf);
  enlargeStringInfo(&buf, 16);
  Assert(buf.len + 16 <= buf.maxlen);
  value = pg_bswap128(value);
  memcpy((char *)(buf.data + buf.len), &value, 16);
  buf.len += 16;
  PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(ztime_to_int4);
Datum ztime_to_int4(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  if (p->value == zu_time_null()) PG_RETURN_NULL();
  PG_RETURN_INT32((int32)zu_time_to_int(p));
}

PG_FUNCTION_INFO_V1(ztime_from_int4);
Datum ztime_from_int4(PG_FUNCTION_ARGS) {
  int32 i = PG_GETARG_INT32(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_from_int(v, i));
}

PG_FUNCTION_INFO_V1(ztime_to_int8);
Datum ztime_to_int8(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  if (p->value == zu_time_null()) PG_RETURN_NULL();
  PG_RETURN_INT64(zu_time_to_int(p));
}

PG_FUNCTION_INFO_V1(ztime_from_int8);
Datum ztime_from_int8(PG_FUNCTION_ARGS) {
  int64 i = PG_GETARG_INT64(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_from_int(v, i));
}

PG_FUNCTION_INFO_V1(ztime_to_float4);
Datum ztime_to_float4(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  PG_RETURN_FLOAT4((float4)zu_time_to_double(p));
}

PG_FUNCTION_INFO_V1(ztime_from_float4);
Datum ztime_from_float4(PG_FUNCTION_ARGS) {
  float4 f = PG_GETARG_FLOAT4(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_from_double(v, (double)f));
}

PG_FUNCTION_INFO_V1(ztime_to_float8);
Datum ztime_to_float8(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  PG_RETURN_FLOAT8(zu_time_to_double(p));
}

PG_FUNCTION_INFO_V1(ztime_from_float8);
Datum ztime_from_float8(PG_FUNCTION_ARGS) {
  float8 d = PG_GETARG_FLOAT8(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_from_double(v, d));
}

PG_FUNCTION_INFO_V1(ztime_round);
Datum ztime_round(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  PG_RETURN_INT64(zu_time_round(p));
}

PG_FUNCTION_INFO_V1(ztime_neg);
Datum ztime_neg(PG_FUNCTION_ARGS) {
  const zu_time *p = (const zu_time *)PG_GETARG_POINTER(0);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_neg(v, p));
}

PG_FUNCTION_INFO_V1(ztime_add);
Datum ztime_add(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  zu_time_add(v, l, r);
  if (l->value != zu_time_null() &&
      r->value != zu_time_null() &&
      v->value == zu_time_null())
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	  errmsg("value out of range: overflow")));
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_sub);
Datum ztime_sub(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  zu_time_sub(v, l, r);
  if (l->value != zu_time_null() &&
      r->value != zu_time_null() &&
      v->value == zu_time_null())
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	  errmsg("value out of range: overflow")));
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_mul);
Datum ztime_mul(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  zu_time_mul(v, l, r);
  if (l->value != zu_time_null() &&
      r->value != zu_time_null() &&
      v->value == zu_time_null())
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	  errmsg("value out of range: overflow")));
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_div);
Datum ztime_div(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
  zu_time_div(v, l, r);
  if (l->value != zu_time_null() &&
      r->value != zu_time_null() &&
      v->value == zu_time_null())
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	  errmsg("value out of range: overflow")));
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(ztime_lt);
Datum ztime_lt(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_time_cmp(l, r) < 0);
}

PG_FUNCTION_INFO_V1(ztime_le);
Datum ztime_le(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_time_cmp(l, r) <= 0);
}

PG_FUNCTION_INFO_V1(ztime_eq);
Datum ztime_eq(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(!zu_time_cmp(l, r));
}

PG_FUNCTION_INFO_V1(ztime_ne);
Datum ztime_ne(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(!!zu_time_cmp(l, r));
}

PG_FUNCTION_INFO_V1(ztime_ge);
Datum ztime_ge(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_time_cmp(l, r) >= 0);
}

PG_FUNCTION_INFO_V1(ztime_gt);
Datum ztime_gt(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_time_cmp(l, r) > 0);
}

PG_FUNCTION_INFO_V1(ztime_cmp);
Datum ztime_cmp(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  PG_RETURN_INT32(zu_time_cmp(l, r));
}

static int ztime_sort_cmp(Datum l_, Datum r_, SortSupport _) {
  const zu_time *l = (const zu_time *)DatumGetPointer(l_);
  const zu_time *r = (const zu_time *)DatumGetPointer(r_);
  return zu_time_cmp(l, r);
}
PG_FUNCTION_INFO_V1(ztime_sort);
Datum ztime_sort(PG_FUNCTION_ARGS) {
  SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
  ssup->comparator = ztime_sort_cmp; // not exposed in SQL
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ztime_hash);
Datum ztime_hash(PG_FUNCTION_ARGS) {
  const zu_time *v = (const zu_time *)PG_GETARG_POINTER(0);
  PG_RETURN_INT32(zu_time_hash(v));
}

PG_FUNCTION_INFO_V1(ztime_smaller);
Datum ztime_smaller(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  int i = zu_time_cmp(l, r);
  PG_RETURN_POINTER(i < 0 ? l : r);
}

PG_FUNCTION_INFO_V1(ztime_larger);
Datum ztime_larger(PG_FUNCTION_ARGS) {
  const zu_time *l = (const zu_time *)PG_GETARG_POINTER(0);
  const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
  int i = zu_time_cmp(l, r);
  PG_RETURN_POINTER(i > 0 ? l : r);
}

PG_FUNCTION_INFO_V1(ztime_sum);
Datum ztime_sum(PG_FUNCTION_ARGS) {
  if (unlikely(PG_ARGISNULL(0))) {
    if (unlikely(PG_ARGISNULL(1))) PG_RETURN_NULL();
    PG_RETURN_POINTER(PG_GETARG_POINTER(1));
  }
  if (unlikely(PG_ARGISNULL(1))) PG_RETURN_POINTER(PG_GETARG_POINTER(0));
  {
    zu_time *l = (zu_time *)PG_GETARG_POINTER(0);
    const zu_time *r = (const zu_time *)PG_GETARG_POINTER(1);
	if (AggCheckCallContext(fcinfo, NULL)) {
	  PG_RETURN_POINTER(zu_time_add(l, l, r));
	} else {
	  zu_time *v = (zu_time *)palloc(sizeof(zu_time));
	  PG_RETURN_POINTER(zu_time_add(v, l, r));
	}
  }
}

PG_FUNCTION_INFO_V1(ztime_acc);
Datum ztime_acc(PG_FUNCTION_ARGS) {
  ArrayType *array = AggCheckCallContext(fcinfo, NULL) ?
    PG_GETARG_ARRAYTYPE_P(0) :
    PG_GETARG_ARRAYTYPE_P_COPY(0);
  zu_time *state;
  const zu_time *v;

  if (ARR_NDIM(array) != 1 ||
      ARR_DIMS(array)[0] != 2 ||
      ARR_HASNULL(array) ||
      ARR_SIZE(array) != ARR_OVERHEAD_NONULLS(1) + sizeof(zu_time) * 2) {
    elog(ERROR, "ztime_acc expected 2-element ztime array");
    PG_RETURN_ARRAYTYPE_P(array);
  }

  if (PG_ARGISNULL(1)) PG_RETURN_ARRAYTYPE_P(array);

  state = (zu_time *)ARR_DATA_PTR(array);
  v = (const zu_time *)PG_GETARG_POINTER(1);

  zu_time_add(&state[0], &state[0], v);
  state[1].value += zu_time_scale();

  PG_RETURN_ARRAYTYPE_P(array);
}

PG_FUNCTION_INFO_V1(ztime_avg);
Datum ztime_avg(PG_FUNCTION_ARGS) {
  ArrayType *array = AggCheckCallContext(fcinfo, NULL) ?
    PG_GETARG_ARRAYTYPE_P(0) :
    PG_GETARG_ARRAYTYPE_P_COPY(0);
  const zu_time *state;
  zu_time *v;

  if (ARR_NDIM(array) != 1 ||
      ARR_DIMS(array)[0] != 2 ||
      ARR_HASNULL(array) ||
      ARR_SIZE(array) != ARR_OVERHEAD_NONULLS(1) + sizeof(zu_time) * 2) {
    elog(ERROR, "ztime_avg expected 2-element ztime array");
    PG_RETURN_NULL();
  }

  state = (const zu_time *)ARR_DATA_PTR(array);

  if (unlikely(!state[1].value)) PG_RETURN_NULL();

  v = (zu_time *)palloc(sizeof(zu_time));
  PG_RETURN_POINTER(zu_time_div(v, &state[0], &state[1]));
}
