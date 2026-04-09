# PySpark SLT Validation Report

Validation run: 239 files, 1919 queries
- **1227 passed**, **241 failed**, **451 skipped**

Issues marked with *(resolved)* were false positives in the validation script that have been fixed.

## Summary by Category

| Category | Failures | Status |
|---|---|---|
| `format_string` with `%f/%e/%g/%a` on Decimal types | 52 | .slt tests need explicit DOUBLE casts |
| `format_string` timestamp `%t` specifiers wrong results | 33 | Expected values are wrong for Spark |
| Interval formatting differences | 17 | DataFusion format vs PySpark format |
| `json_tuple` multi-column output | 12 | Generator function; test harness limitation |
| Timezone offset handling in `from/to_utc_timestamp` | 12 | DataFusion/Spark parse offsets differently |
| `shuffle` with seed argument | 8 | Spark `shuffle` only takes 1 arg |
| `make_interval` / INTERVAL parsing errors | 10 | PySpark version limitation for interval types |
| `TIME` data type not supported | 7 | Spark 4.0 only |
| Float precision differences | 8 | Float32 precision loss and extra significant digits |
| `format_string` `%c` with BIGINT | 4 | Spark requires INT not BIGINT |
| Binary/bytearray output formatting | 5 | unhex returns binary, not string |
| Escape sequence handling in soundex | 4 | Literal `\t`/`\n` vs escape interpretation |
| `array_contains` with NULL arguments | 2 | PySpark rejects NULL typed arguments |
| `date_add`/`date_sub` overflow behavior | 2 | Different overflow semantics |
| `substring` negative index behavior | 2 | Different negative index handling |
| Other (empty string, char null, map ordering, size semantics) | ~10 | Individual Spark behavior differences |
| *(resolved)* `try_parse_url` / `try_url_decode` | 52 | Now skipped (Spark 4.0 only) |
| *(resolved)* Type aliases (`FLOAT8`, `FLOAT4`, `INT8`, `BYTEA`) | 48 | Now translated to standard Spark types |
| *(resolved)* Array literal `[...]` syntax | 34 | Now translated to `array(...)` |
| *(resolved)* Column naming (`column1` vs `col1`) | 19 | Now translated automatically |
| *(resolved)* `DECIMAL` literal syntax | 4 | Now translated to `CAST()` |
| *(resolved)* Cast translation bug | 3 | Fixed `func(...)::TYPE` handling |

---

## Detailed Findings

### 1. `format_string` with `%f/%e/%g/%a` on Decimal Types (52 failures)

**File:** `string/format_string.slt`

PySpark errors with messages like `f != org.apache.spark.sql.types.Decimal`. Spark's `format_string` does not support `%f`, `%e`, `%g`, `%a` format specifiers with Decimal literals — requires explicit DOUBLE casts.

**Examples:**
```
Line 125: SELECT format_string('%f', 3.14);
  Error: f != org.apache.spark.sql.types.Decimal

Line 142: SELECT format_string('%e', 3.14);
  Error: e != org.apache.spark.sql.types.Decimal
```

**Fix:** Add explicit `CAST(3.14 AS DOUBLE)` in the .slt test queries.

---

### 2. `try_parse_url` / `try_url_decode` Not Available (52 failures)

**Files:** `url/try_parse_url.slt` (45), `url/try_url_decode.slt` (7)

These are Spark 4.0 functions not available in Spark 3.x.

```
Error: [UNRESOLVED_ROUTINE] Cannot resolve function 'try_parse_url'
Error: [UNRESOLVED_ROUTINE] Cannot resolve function 'try_url_decode'
```

**Fix:** Gate these tests on Spark version, or skip validation for Spark 4.0 functions.

---

### 3. Unsupported Type Aliases (48 failures)

**Files:** `math/mod.slt` (15), `math/pmod.slt` (19), `string/base64.slt` (5), `datetime/time_trunc.slt` (7), `string/format_string.slt` (2)

PySpark SQL does not recognize `FLOAT8`, `FLOAT4`, `INT8`, `BYTEA`, or `TIME` type aliases.

**Examples:**
```
Line 63 (mod.slt): SELECT mod(CAST(10 AS FLOAT8), CAST(3 AS FLOAT8));
  Error: [UNSUPPORTED_DATATYPE] Unsupported data type "FLOAT8"

Line 33 (base64.slt): SELECT base64(CAST('hello' AS BYTEA));
  Error: [UNSUPPORTED_DATATYPE] Unsupported data type "BYTEA"

Line 19 (time_trunc.slt): SELECT time_trunc('HOUR', TIME '12:34:56');
  Error: [UNSUPPORTED_DATATYPE] Unsupported data type "TIME"
```

**Fix:** Replace type aliases with Spark-standard equivalents: `FLOAT8` -> `DOUBLE`, `FLOAT4` -> `FLOAT`, `INT8` -> `BIGINT`, `BYTEA` -> `BINARY`. `TIME` is Spark 4.0 only.

---

### 4. Array Literal `[...]` Syntax (34 failures)

**Files:** `array/shuffle.slt` (10), `array/slice.slt` (12), `array/array_repeat.slt` (3), `array/array.slt` (1), `collection/size.slt` (2), `map/map_from_arrays.slt` (2), `map/map_from_entries.slt` (8)

PySpark SQL does not support `[1, 2, 3]` array literal syntax. Requires `array(1, 2, 3)`.

**Examples:**
```
Line 19 (shuffle.slt): SELECT shuffle([1, 2, 3, 4, 5], 42);
  Error: Syntax error at or near '['

Line 18 (slice.slt): SELECT slice([1, 2, 3, 4, 5], 2, 3);
  Error: Syntax error at or near '['
```

**Fix:** Replace `[...]` with `array(...)` in the .slt files.

---

### 5. `format_string` Timestamp `%t` Specifiers (33 failures)

**File:** `string/format_string.slt`

PySpark produces completely wrong results for `%t` timestamp format specifiers. Spark appears to interpret the timestamp argument as epoch milliseconds rather than as a timestamp.

**Examples:**
```
Line 356: SELECT format_string('Hour: %tH', TIMESTAMP '2023-12-25 14:30:45');
  Expected: Hour: 14
  PySpark:  Hour: 01

Line 416: SELECT format_string('Year: %tY', TIMESTAMP '2023-12-25 14:30:45');
  Expected: Year: 2023
  PySpark:  Year: 55953
```

**Root cause:** Spark's Java `format_string` treats `TIMESTAMP` arguments differently than the DataFusion `.slt` tests assume. The expected values may have been generated from DataFusion rather than from actual Spark.

---

### 6. Column Naming Mismatch (19 failures)

**Files:** `datetime/date_diff.slt` (2), `datetime/date_part.slt` (3), `datetime/date_trunc.slt` (1), `datetime/from_utc_timestamp.slt` (2), `datetime/to_utc_timestamp.slt` (2), `datetime/trunc.slt` (1), `string/substring.slt` (4), `array/array_repeat.slt` (2), `string/base64.slt` (1)

PySpark uses `col1`, `col2` for unnamed columns in `VALUES` clauses, while DataFusion uses `column1`, `column2`.

**Example:**
```
Line 103 (date_trunc.slt): SELECT date_trunc('YEAR', column1) FROM VALUES (...)
  PySpark error: Cannot resolve column `column1`. Did you mean: col1?
```

**Fix:** Use explicit aliases in VALUES clauses: `FROM VALUES (...) AS t(col1)`.

---

### 7. Interval Formatting Differences (17 failures)

**Files:** `datetime/make_dt_interval.slt` (16), `datetime/make_interval.slt` (1)

DataFusion formats intervals as `1 days 12 hours 30 mins 1.001001 secs`, PySpark formats as `1 day, 12:30:01.001001` or Python timedelta representations.

**Example:**
```
Line 26 (make_dt_interval.slt): SELECT make_dt_interval(1, 12, 30, 1.001001);
  Expected: 0 years 0 mons 1 days 12 hours 30 mins 1.001001 secs
  PySpark:  1 12:30:01.001001
```

**Root cause:** Different string representations for interval types between engines.

---

### 8. `json_tuple` Multi-Column Output (12 failures)

**File:** `json/json_tuple.slt`

`json_tuple` is a generator function that returns multiple columns. The expected output in .slt is struct format like `{c0: value1, c1: value2}`, but PySpark returns separate columns.

**Example:**
```
Line 25: SELECT json_tuple('{"a":1,"b":2}', 'a', 'b');
  Expected: {c0: 1, c1: 2}
  PySpark:  1
```

This is partly a test harness limitation (only captures first column) and partly a formatting difference.

---

### 9. Timezone Offset Handling (12 failures)

**Files:** `datetime/from_utc_timestamp.slt` (6), `datetime/to_utc_timestamp.slt` (6)

DataFusion parses `+02:00` timezone offset in input string, while Spark ignores it.

**Example:**
```
Line 35 (from_utc_timestamp.slt):
  SELECT from_utc_timestamp('2018-03-13T06:18:23+02:00', 'UTC');
  Expected: 2018-03-13T04:18:23  (DataFusion parsed +02:00 offset)
  PySpark:  2018-03-12T22:18:23  (Spark ignored offset, used local tz)
```

**Root cause:** Different timezone parsing semantics between DataFusion and Spark.

---

### 10. `make_interval` / INTERVAL Parsing Errors (10 failures)

**File:** `datetime/make_interval.slt`

PySpark version cannot parse INTERVAL data types returned by `make_interval`.

```
Error: CANNOT_PARSE_DATATYPE: 'interval'
Error: DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE for concat with interval
```

---

### 11. Float Precision Differences (8 failures)

**Files:** `math/csc.slt` (2), `math/sec.slt` (1), `math/round.slt` (1), `math/negative.slt` (5)

Extra significant digits or Float32 precision loss.

**Examples:**
```
Line 27 (csc.slt): SELECT csc(1);
  Expected: 1.188395105778121
  PySpark:  1.1883951057781212  (extra digit)

Line 85 (round.slt): SELECT round(CAST(2.345 AS FLOAT), 2);
  Expected: 2.35
  PySpark:  2.3499999046325684  (float32 precision loss)

Line 70 (negative.slt): SELECT negative(CAST(3.14 AS FLOAT));
  Expected: -3.14
  PySpark:  -3.140000104904175  (float32 precision loss)
```

---

### 12. Binary/Bytearray Output Formatting (5 failures)

**Files:** `math/unhex.slt` (4), `string/concat.slt` (1)

PySpark returns `bytearray(b'...')` for binary output, while .slt expects hex or decoded strings.

**Example:**
```
Line 19 (unhex.slt): SELECT unhex('537061726B2053514C');
  Expected: Spark SQL
  PySpark:  bytearray(b'Spark SQL')
```

---

### 13. DECIMAL Literal Syntax (4 failures)

**File:** `aggregate/try_sum.slt`

PySpark does not support `DECIMAL(10,2) '1.23'` typed literal syntax.

```
Line 55: SELECT try_sum(a) FROM (VALUES (DECIMAL(10,2) '1.23'), ...)
  Error: Syntax error at '('
```

**Fix:** Use `CAST('1.23' AS DECIMAL(10,2))` instead.

---

### 14. Escape Sequence Handling (4 failures)

**File:** `string/soundex.slt`

The .slt tests contain `\t` and `\n` which PySpark interprets as tab/newline, but the expected values assume literal backslash characters. Also, `soundex('#')` returns `#` in Spark but empty in DataFusion.

---

### 15. Cast Translation Bug (3 failures)

**Files:** `datetime/date_add.slt` (2), `datetime/date_sub.slt` (1)

The `::` cast regex translator merges function names with `CAST`:

```
Original: date_add(date, value::INT)
Translated: date_addCAST(date, value AS INT)  -- BUG
```

**Fix:** Fix the `::` regex in the validation script to handle function call boundaries.

---

### 16. `array_contains` with NULL Arguments (2 failures)

**File:** `array/array_contains.slt`

PySpark rejects NULL typed arguments to `array_contains`.

```
Line 56: SELECT array_contains(NULL, 1);
  Error: [DATATYPE_MISMATCH.NULL_TYPE] Null typed values cannot be used as arguments

Line 62: SELECT array_contains(array(1,2,3), NULL);
  Error: same
```

**Root cause:** DataFusion handles NULL arguments by returning NULL; Spark rejects them at analysis time.

---

### 17. Other Failures (8 total)

| File | Line | Issue |
|---|---|---|
| `string/char.slt` | 28 | `char(0)` -- expected `\0`, PySpark returns space |
| `string/soundex.slt` | 73, 188 | `soundex('#')` -- expected empty, PySpark returns `#` |
| `map/str_to_map.slt` | 43 | Empty string key: expected `{: NULL}`, got `{(empty): NULL}` |
| `map/map_from_arrays.slt` | 19, 24, 122 | Float key formatting, map ordering, duplicate key rejection |
| `map/map_from_entries.slt` | 155 | Duplicate key rejection in Spark |
| `collection/size.slt` | 62 | `size(map(array, array))` -- expected `3`, got `1` |
