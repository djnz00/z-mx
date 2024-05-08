#include <postgres.h>
#include <fmgr.h>
#include <libpq/pqformat.h>
#include <utils/sortsupport.h>
#include <port/pg_bswap.h>

#include <zlib/zu_decimal.h>

#ifndef pg_bswap128
#ifdef WORDS_BIGENDIAN
#define pg_bswap128(x) (x)
#else
#if defined(__GNUC__) && !defined(__llvm__)
#define pg_bswap128(x) __builtin_bswap128(x)
#else
inline static uint128_t pg_bswap128(uint128_t i) {
  return
    ((uint128_t)(pg_bswap64(i))<<64) |
    (uint128_t)pg_bswap64(i>>64);
}
#endif /* __GNUC__ && !__llvm__ */
#endif /* WORDS_BIGENDIAN */
#endif /* pg_bswap128 */

PG_FUNCTION_INFO_V1(zdecimal_in);
Datum zdecimal_in(PG_FUNCTION_ARGS) {
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  const char *s = PG_GETARG_CSTRING(0);
  unsigned int n = zu_decimal_in(v, s);
  if (!n || s[n])
    ereport(
      ERROR,
      (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
       errmsg("invalid input syntax for zdecimal: \"%s\"", s))
    );
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(zdecimal_out);
Datum zdecimal_out(PG_FUNCTION_ARGS) {
  const zu_decimal *v = (const zu_decimal *)PG_GETARG_POINTER(0);
  unsigned int n = zu_decimal_out_len(v);
  char *s = palloc(n);
  zu_decimal_out(s, v);
  PG_RETURN_CSTRING(s);
}

PG_FUNCTION_INFO_V1(zdecimal_recv);
Datum zdecimal_recv(PG_FUNCTION_ARGS) {
  StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  pq_copymsgbytes(buf, (char *)v, 16);
  v->value = pg_bswap128(v->value);
  PG_RETURN_POINTER(v);
}

PG_FUNCTION_INFO_V1(zdecimal_send);
Datum zdecimal_send(PG_FUNCTION_ARGS) {
  const zu_decimal *v = (const zu_decimal *)PG_GETARG_POINTER(0);
  uint128_t value = v->value;
  StringInfoData buf;
  pq_begintypsend(&buf);
  enlargeStringInfo(&buf, 16);
  Assert(buf->len + 16 <= buf->maxlen);
  value = pg_bswap128(value);
  memcpy((char *)(buf.data + buf.len), &value, 16);
  buf.len += 16;
  PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(zdecimal_neg);
Datum zdecimal_neg(PG_FUNCTION_ARGS) {
  const zu_decimal *p = (const zu_decimal *)PG_GETARG_POINTER(0);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  PG_RETURN_POINTER(zu_decimal_neg(v, p));
}

PG_FUNCTION_INFO_V1(zdecimal_add);
Datum zdecimal_add(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  PG_RETURN_POINTER(zu_decimal_add(v, l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_sub);
Datum zdecimal_sub(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  PG_RETURN_POINTER(zu_decimal_sub(v, l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_mul);
Datum zdecimal_mul(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  PG_RETURN_POINTER(zu_decimal_mul(v, l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_div);
Datum zdecimal_div(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
  PG_RETURN_POINTER(zu_decimal_div(v, l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_lt);
Datum zdecimal_lt(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_decimal_cmp(l, r) < 0);
}

PG_FUNCTION_INFO_V1(zdecimal_le);
Datum zdecimal_le(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_decimal_cmp(l, r) <= 0);
}

PG_FUNCTION_INFO_V1(zdecimal_eq);
Datum zdecimal_eq(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(!zu_decimal_cmp(l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_ne);
Datum zdecimal_ne(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(!!zu_decimal_cmp(l, r));
}

PG_FUNCTION_INFO_V1(zdecimal_ge);
Datum zdecimal_ge(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_decimal_cmp(l, r) >= 0);
}

PG_FUNCTION_INFO_V1(zdecimal_gt);
Datum zdecimal_gt(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_BOOL(zu_decimal_cmp(l, r) > 0);
}

PG_FUNCTION_INFO_V1(zdecimal_cmp);
Datum zdecimal_cmp(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  PG_RETURN_INT32(zu_decimal_cmp(l, r));
}

static int zdecimal_sort_cmp(Datum l_, Datum r_, SortSupport _) {
  const zu_decimal *l = (const zu_decimal *)DatumGetPointer(l_);
  const zu_decimal *r = (const zu_decimal *)DatumGetPointer(r_);
  return zu_decimal_cmp(l, r);
}
PG_FUNCTION_INFO_V1(zdecimal_sort);
Datum zdecimal_sort(PG_FUNCTION_ARGS) {
  SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
  ssup->comparator = zdecimal_sort_cmp; // not exposed in SQL
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(zdecimal_hash);
Datum zdecimal_hash(PG_FUNCTION_ARGS) {
  const zu_decimal *v = (const zu_decimal *)PG_GETARG_POINTER(0);
  PG_RETURN_INT32(zu_decimal_hash(v));
}

PG_FUNCTION_INFO_V1(zdecimal_smaller);
Datum zdecimal_smaller(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  int i = zu_decimal_cmp(l, r);
  PG_RETURN_POINTER(i < 0 ? l : r);
}

PG_FUNCTION_INFO_V1(zdecimal_larger);
Datum zdecimal_larger(PG_FUNCTION_ARGS) {
  const zu_decimal *l = (const zu_decimal *)PG_GETARG_POINTER(0);
  const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
  int i = zu_decimal_cmp(l, r);
  PG_RETURN_POINTER(i > 0 ? l : r);
}

PG_FUNCTION_INFO_V1(zdecimal_sum);
Datum zdecimal_sum(PG_FUNCTION_ARGS) {
  if (unlikely(PG_ARGISNULL(0))) {
    if (unlikely(PG_ARGISNULL(1))) PG_RETURN_NULL();
    PG_RETURN_POINTER(PG_GETARG_POINTER(1));
  }
  if (unlikely(PG_ARGISNULL(1))) PG_RETURN_POINTER(PG_GETARG_POINTER(0));
  {
    zu_decimal *l = (zu_decimal *)PG_GETARG_POINTER(0);
    const zu_decimal *r = (const zu_decimal *)PG_GETARG_POINTER(1);
    PG_RETURN_POINTER(zu_decimal_add(l, l, r));
  }
}

typedef struct {
  zu_decimal	sum;
  uint64_t	count;
} zdecimal_agg_state;

static zdecimal_agg_state *
zdecimal_agg_state_new(FunctionCallInfo fcinfo, const zu_decimal *v) {
  zdecimal_agg_state *state;
  MemoryContext agg_context;
  MemoryContext old_context;

  if (!AggCheckCallContext(fcinfo, &agg_context))
    elog(ERROR, "aggregate function called in non-aggregate context");

  old_context = MemoryContextSwitchTo(agg_context);

  state = (zdecimal_agg_state *)palloc(sizeof(zdecimal_agg_state));
  state->sum.value = v->value;
  state->count = 1;

  MemoryContextSwitchTo(old_context);
  return state;
}

PG_FUNCTION_INFO_V1(zdecimal_acc);
Datum zdecimal_acc(PG_FUNCTION_ARGS) {
  zdecimal_agg_state *state =
    PG_ARGISNULL(0) ? NULL : (zdecimal_agg_state *)PG_GETARG_POINTER(0);

  if (likely(!PG_ARGISNULL(1))) {
    const zu_decimal *v = (const zu_decimal *)PG_GETARG_POINTER(1);

    if (unlikely(state == NULL)) {
      state = zdecimal_agg_state_new(fcinfo, v);
    } else {
      zu_decimal_add(&state->sum, &state->sum, v);
      ++state->count;
    }
  }

  PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(zdecimal_avg);
Datum zdecimal_avg(PG_FUNCTION_ARGS) {
  zdecimal_agg_state *state =
    PG_ARGISNULL(0) ? NULL : (zdecimal_agg_state *)PG_GETARG_POINTER(0);

  if (state == NULL || state->count == 0) PG_RETURN_NULL();

  {
    zu_decimal *v = (zu_decimal *)palloc(sizeof(zu_decimal));
    zu_decimal count;
    zu_decimal_from_int(&count, state->count);
    PG_RETURN_POINTER(zu_decimal_div(v, &state->sum, &count));
  }
}
