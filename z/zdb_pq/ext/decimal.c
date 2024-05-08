#include <postgres.h>
#include <fmgr.h>
#include <utils/sortsupport.h>

PG_FUNCTION_INFO_V1(complex_add);

Datum complex_add(PG_FUNCTION_ARGS) {
  Complex *a = (Complex *)PG_GETARG_POINTER(0);
  Complex *b = (Complex *)PG_GETARG_POINTER(1);
  Complex *result;

  result = (Complex *)palloc(sizeof(Complex));
  result->x = a->x + b->x;
  result->y = a->y + b->y;
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(complex_recv);

Datum complex_recv(PG_FUNCTION_ARGS) {
  StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
  Complex *result;

  // use pq_copymsgbytes(buf, (char *)decimal, 16);
  // ... then a ntoh byteswap if needed

  result = (Complex *)palloc(sizeof(Complex));
  result->x = pq_getmsgfloat8(buf); // use getmsgint64
  result->y = pq_getmsgfloat8(buf);
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(complex_send);

Datum complex_send(PG_FUNCTION_ARGS) {
  Complex *complex = (Complex *)PG_GETARG_POINTER(0);
  StringInfoData buf;

  pq_begintypsend(&buf);
  pq_sendfloat8(&buf, complex->x);
  pq_sendfloat8(&buf, complex->y);
  PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(complex_in);

Datum complex_in(PG_FUNCTION_ARGS) {
  char *str = PG_GETARG_CSTRING(0);
  double x, y;
  Complex *result;

  if (sscanf(str, " ( %lf , %lf )", &x, &y) != 2)
    ereport(
      ERROR,
      (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
       errmsg("invalid input syntax for type %s: \"%s\"", "complex", str))
    );

  result = (Complex *)palloc(sizeof(Complex));
  result->x = x;
  result->y = y;
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(int1smaller);
Datum int1smaller(PG_FUNCTION_ARGS) {
  int8 arg1 = PG_GETARG_INT8(0);
  int8 arg2 = PG_GETARG_INT8(1);
  int8 result;

  result = (arg1 < arg2) ? arg1 : arg2;

  PG_RETURN_INT8(result);
}

static int btint1fastcmp(Datum x, Datum y, SortSupport ssup) {
  int8 a = DatumGetInt8(x); /* will be GetPointer for decimal */
  int8 b = DatumGetInt8(y);

  if (a > b)
    return 1;
  else if (a == b)
    return 0;
  else
    return -1;
}

PG_FUNCTION_INFO_V1(btint1sortsupport);
Datum btint1sortsupport(PG_FUNCTION_ARGS) {
  SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

  ssup->comparator = btint1fastcmp; // not exposed in SQL
  PG_RETURN_VOID();
}

typedef struct NumericAggState {
  bool calcSumX2;            /* if true, calculate sumX2 */
  MemoryContext agg_context; /* context we're calculating in */
  int64 N;                   /* count of processed numbers */
  NumericSumAccum sumX;      /* sum of processed numbers */
  NumericSumAccum sumX2;     /* sum of squares of processed numbers */
  int maxScale;              /* maximum scale seen so far */
  int64 maxScaleCount;       /* number of values seen with maximum scale */
  /* These counts are *not* included in N!  Use NA_TOTAL_COUNT() as needed */
  int64 NaNcount;  /* count of NaN values */
  int64 pInfcount; /* count of +Inf values */
  int64 nInfcount; /* count of -Inf values */
} NumericAggState;

static NumericAggState *
makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2) {
  NumericAggState *state;
  MemoryContext agg_context;
  MemoryContext old_context;

  if (!AggCheckCallContext(fcinfo, &agg_context))
    elog(ERROR, "aggregate function called in non-aggregate context");

  old_context = MemoryContextSwitchTo(agg_context);

  /* will be GC'd by the postgres memory context */
  state = (NumericAggState *)palloc0(sizeof(NumericAggState));
  state->calcSumX2 = calcSumX2;
  state->agg_context = agg_context;

  MemoryContextSwitchTo(old_context);

  return state;
}

Datum numeric_accum(PG_FUNCTION_ARGS) {
  NumericAggState *state;

  state = PG_ARGISNULL(0) ? NULL : (NumericAggState *)PG_GETARG_POINTER(0);

  /* Create the state data on the first call */
  if (state == NULL) state = makeNumericAggState(fcinfo, true);

  if (!PG_ARGISNULL(1)) do_numeric_accum(state, PG_GETARG_NUMERIC(1));

  PG_RETURN_POINTER(state);
}

Datum numeric_avg(PG_FUNCTION_ARGS) {
  NumericAggState *state;
  Datum N_datum;
  Datum sumX_datum;
  NumericVar sumX_var;

  state = PG_ARGISNULL(0) ? NULL : (NumericAggState *)PG_GETARG_POINTER(0);

  /* If there were no non-null inputs, return NULL */
  if (state == NULL || NA_TOTAL_COUNT(state) == 0) PG_RETURN_NULL();

  if (state->NaNcount > 0) /* there was at least one NaN input */
    PG_RETURN_NUMERIC(make_result(&const_nan));

  /* adding plus and minus infinities gives NaN */
  if (state->pInfcount > 0 && state->nInfcount > 0)
    PG_RETURN_NUMERIC(make_result(&const_nan));
  if (state->pInfcount > 0) PG_RETURN_NUMERIC(make_result(&const_pinf));
  if (state->nInfcount > 0) PG_RETURN_NUMERIC(make_result(&const_ninf));

  N_datum = NumericGetDatum(int64_to_numeric(state->N));

  init_var(&sumX_var);
  accum_sum_final(&state->sumX, &sumX_var);
  sumX_datum = NumericGetDatum(make_result(&sumX_var));
  free_var(&sumX_var);

  PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumX_datum, N_datum));
}
