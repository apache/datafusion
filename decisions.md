# regexp_extract Design Decisions

## Why `datafusion/spark/src/function/string/`

This is a Spark-compatible function, so it belongs in the `datafusion-spark` crate alongside other Spark string functions (`substring`, `like`, `concat`, etc.). Placing it here keeps it out of core DataFusion and lets users opt in via `with_spark_features()`.

## Arguments

Spark's `regexp_extract` defaults `idx` to `1` when omitted. We support both 2-arg and 3-arg calls via `Signature::one_of` — the 2-arg form `regexp_extract(str, pattern)` defaults `idx` to `1`. This matches Spark behavior and avoids forcing users to pass a redundant literal `1` in the common case of extracting the first capture group.

## Why `Coercion::new_implicit` for idx

Users may pass `INT`, `SMALLINT`, or other integer types for the group index. Rather than forcing an explicit `CAST`, we use implicit coercion to `Int64`. This matches how Spark handles it and avoids unnecessary friction.

## Why error on idx > group count (not empty string)

We chose to return an error because (I think spark does the same):
- An out-of-range index is almost always a bug in the query
- Silent empty strings hide mistakes and make debugging harder
- Users get an actionable error message naming the pattern and group count

## Why error on negative idx (not empty string)

Same reasoning — a negative group index is never intentional. Failing loudly helps users catch mistakes early.

## Why `make_scalar_function` wrapper

DataFusion passes `ColumnarValue` (which can be `Scalar` or `Array`) to UDFs. The `make_scalar_function` wrapper normalizes everything to arrays so the inner function only deals with `&[ArrayRef]`. This is a standard pattern in the codebase — every sibling function uses it.

## Why regex is compiled per-row

The pattern can vary per row (it's a column, not just a literal). Caching would require a hash map keyed on pattern strings, adding complexity. Per-row compilation is simple and correct. If profiling shows this is a bottleneck (e.g., same literal pattern across millions of rows), a cache can be added later without changing the API.

## Why `Volatility::Immutable`

`regexp_extract` is deterministic — same inputs always produce the same output. Marking it `Immutable` lets the optimizer constant-fold calls with all-literal args and avoid redundant evaluations.

## Future considerations

- **Regex caching**: if the pattern column is a literal (common case), compile once instead of per-row
