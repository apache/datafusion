<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Data Types

DataFusion uses Arrow, and thus the Arrow type system, for query
execution. The SQL types from
[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/ast/data_type.rs#L49)
are mapped to [Arrow data types](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) according to the following table.
This mapping occurs when defining the schema in a `CREATE EXTERNAL TABLE` command or when performing a SQL `CAST` operation.

For background on extension types and custom metadata, see the
[Implementing User Defined Types and Custom Metadata in DataFusion] blog.

[implementing user defined types and custom metadata in datafusion]: https://datafusion.apache.org/blog/2025/09/21/custom-types-using-metadata

You can see the corresponding Arrow type for any SQL expression using
the `arrow_typeof` function. For example:

```sql
select arrow_typeof(interval '1 month');
+---------------------------------------------------------------------+
| arrow_typeof(IntervalMonthDayNano("79228162514264337593543950336")) |
+---------------------------------------------------------------------+
| Interval(MonthDayNano)                                              |
+---------------------------------------------------------------------+
```

You can cast a SQL expression to a specific Arrow type using the `arrow_cast` function
For example, to cast the output of `now()` to a `Timestamp` with second precision:

```sql
select arrow_cast(now(), 'Timestamp(s)') as "now()";
+---------------------+
| now()               |
+---------------------+
| 2025-10-24T20:02:45 |
+---------------------+
```

The older syntax still works as well:

```sql
select arrow_cast(now(), 'Timestamp(Second, None)') as "now()";
+---------------------+
| now()               |
+---------------------+
| 2023-03-03T17:19:21 |
+---------------------+
```

## Character Types

| SQL DataType        | Arrow DataType |
| ------------------- | -------------- |
| `CHAR`              | `Utf8View`     |
| `VARCHAR`           | `Utf8View`     |
| `TEXT`              | `Utf8View`     |
| `STRING`            | `Utf8View`     |
| `CHARACTER VARYING` | `Utf8View`     |
| `CHAR VARYING`      | `Utf8View`     |
| `NVARCHAR`          | `Utf8View`     |

By default, string types are mapped to `Utf8View`. This can be configured using the `datafusion.sql_parser.map_string_types_to_utf8view` setting. When set to `false`, string types are mapped to `Utf8` instead.

Note that `CHAR`, `VARCHAR`, `STRING`, `CHARACTER VARYING`, `CHAR VARYING`, and `NVARCHAR` can
optionally receive the maximum length (e.g., `VARCHAR(n)`). However, this limit is ignored. To raise
an error when the length is passed, set `datafusion.sql_parser.support_varchar_with_length` to
`false`.

## Numeric Types

| SQL DataType                                                                  | Arrow DataType                 |
| ----------------------------------------------------------------------------- | :----------------------------- |
| `TINYINT`                                                                     | `Int8`                         |
| `SMALLINT` or `INT2`                                                          | `Int16`                        |
| `INT` or `INTEGER` or `INT4`                                                  | `Int32`                        |
| `BIGINT` or `INT8`                                                            | `Int64`                        |
| `TINYINT UNSIGNED` or `UTINYINT`                                              | `UInt8`                        |
| `SMALLINT UNSIGNED` or `INT2 UNSIGNED` or `USMALLINT`                         | `UInt16`                       |
| `INT UNSIGNED` or `INTEGER UNSIGNED` or `INT4 UNSIGNED`                       | `UInt32`                       |
| `BIGINT UNSIGNED` or `INT8 UNSIGNED` or `UBIGINT`                             | `UInt64`                       |
| `FLOAT` or `FLOAT4` or `REAL`                                                 | `Float32`                      |
| `DOUBLE` or `DOUBLE PRECISION` or `FLOAT8`                                    | `Float64`                      |
| `DECIMAL(precision, scale) or NUMERIC(precision, scale)` where precision ≤ 38 | `Decimal128(precision, scale)` |
| `DECIMAL(precision, scale) or NUMERIC(precision, scale)` where precision > 38 | `Decimal256(precision, scale)` |

The maximum supported precision for `DECIMAL` types is 76.

## Date/Time Types

| SQL DataType                                                  | Arrow DataType                                             |
| ------------------------------------------------------------- | :--------------------------------------------------------- |
| `DATE`                                                        | `Date32`                                                   |
| `TIME`                                                        | `Time64(Nanosecond)`                                       |
| `TIMESTAMP`, `TIMESTAMP(p)`, `TIMESTAMP(p) WITHOUT TIME ZONE` | `Timestamp(unit, None)`                                    |
| `TIMESTAMPTZ(p)`, `TIMESTAMP(p) WITH TIME ZONE`               | `Timestamp(unit, None)` by default — see the warning below |
| `INTERVAL`                                                    | `Interval(MonthDayNano)`                                   |

:::{warning}
`TIMESTAMPTZ` and `TIMESTAMP WITH TIME ZONE` do **not** give a timezone-aware
type by default. The zone comes from the
[`datafusion.execution.time_zone`] setting, and that setting is unset unless you
set it. So `'2024-01-01T12:00:00Z'::timestamptz` gives `Timestamp(unit, None)`
by default: the result type does not keep timezone metadata.

An explicit offset in the input is still applied. DataFusion normalizes the
value to UTC. For example, with the default setting,
`'2024-01-01T12:00:00+07:00'::timestamptz` gives `2024-01-01T05:00:00`. It is
not read as the local time `12:00:00`.

This is a known bug. See [issue #25166]. PostgreSQL and DuckDB always give an
aware type here, because their session time zone always has a value.

Set `datafusion.execution.time_zone` to make these types aware. `'UTC'` is a
good value.
:::

`unit` comes from the optional precision `p` on the two `TIMESTAMP` rows. Use
`0`, `3`, `6` or `9` for `Second`, `Millisecond`, `Microsecond` or `Nanosecond`.
DataFusion rejects each other value of `p`. If you omit `p`, the unit is
`Nanosecond`. The precision goes after `TIMESTAMPTZ`, but before
`WITH TIME ZONE`: use `TIMESTAMPTZ(3)` or `TIMESTAMP(3) WITH TIME ZONE`.
`DATE`, `TIME` and `INTERVAL` do not accept a precision.

### Timestamp timezones and comparisons

DataFusion follows the [Arrow timestamp representation]. A timestamp has a time
unit and an optional timezone:

- `Timestamp(unit, Some(timezone))` identifies an instant. Its integer value is
  measured from the UTC Unix epoch; the timezone controls how that instant is
  displayed. Changing between two timezone-aware types preserves the instant.
- `Timestamp(unit, None)` is timezone-naive. It represents a wall-clock time
  without identifying the timezone in which that time occurs. It is not the same
  type as a timestamp with the timezone `"UTC"`.

For timestamp comparisons (`=`, `!=`, `<`, `<=`, `>` and `>=`), DataFusion coerces
the operands to a common timestamp type:

| Operand timezones               | Common timezone             |
| ------------------------------- | --------------------------- |
| Neither operand has a timezone  | None                        |
| Only one operand has a timezone | That operand's timezone     |
| Both operands have a timezone   | The left operand's timezone |

The common type uses the finer of the two time units. Two timezone-aware
timestamps are compared as instants, even when their timezones differ. For
example, noon at `+01:00` is the same instant as 11:00 UTC:

```sql
SELECT
  arrow_cast('2024-01-01T12:00:00+01:00', 'Timestamp(s, "+01:00")') =
  arrow_cast('2024-01-01T11:00:00Z', 'Timestamp(ms, "UTC")') AS same_instant;
+--------------+
| same_instant |
+--------------+
| true         |
+--------------+
```

When a timezone-naive timestamp is compared with a timezone-aware timestamp,
the naive value is interpreted as a local wall-clock time in the common
timezone. It is not implicitly interpreted as UTC. Here, the naive noon value
is interpreted in `+01:00`, so it also equals 11:00 UTC:

```sql
SELECT
  arrow_cast('2024-01-01T12:00:00', 'Timestamp(s)') =
  arrow_cast('2024-01-01T11:00:00Z', 'Timestamp(ms, "+01:00")') AS naive_is_local;
+----------------+
| naive_is_local |
+----------------+
| true           |
+----------------+
```

Use an explicit cast to a timezone-aware type before comparing if a naive
column's values belong to a different timezone from the other operand. For
named timezones, daylight saving transitions can make a local time ambiguous
or nonexistent; such a cast can fail.

These comparison rules do not apply to every timestamp operation. For example,
subtracting timestamps with different timezone metadata can fail during
planning. Cast both operands to the same timezone-aware type before performing
that operation. `"UTC"` and `"+00:00"` are treated as equivalent for timestamp
arithmetic.

[arrow timestamp representation]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Timestamp
[`datafusion.execution.time_zone`]: ../configs.md
[issue #25166]: https://github.com/apache/datafusion/issues/25166

## Boolean Types

| SQL DataType        | Arrow DataType |
| ------------------- | :------------- |
| `BOOLEAN` or `BOOL` | `Boolean`      |

## Binary Types

| SQL DataType | Arrow DataType |
| ------------ | :------------- |
| `BYTEA`      | `Binary`       |
| `BLOB`       | `Binary`       |
| `BINARY`     | `Binary`       |
| `VARBINARY`  | `Binary`       |
| `BYTES`      | `Binary`       |

You can create binary literals using a hex string literal such as
`X'1234'` to create a `Binary` value of two bytes, `0x12` and `0x34`.

## Nested Types

| SQL DataType                      | Arrow DataType                  |
| --------------------------------- | :------------------------------ |
| `ARRAY<T>` or `T[]`               | `List(T)`                       |
| `T[n]`                            | `FixedSizeList(n x T)`          |
| `MAP(k, v)`                       | `Map(k, v)`                     |
| `STRUCT<[c0] T0[, ..., [cn] Tn]>` | `Struct(c0: T0[, ..., cn: Tn])` |

While columns defined with `STRUCT` require all rows to have the same schema, `MAP` imposes no such
restriction.

Also note that `STRUCT` can be declared without defining the field names. In that case, they will be
set to `c0`, `c1`, ..., `cn`.

## Unsupported SQL Types

| SQL Data Type | Arrow DataType      |
| ------------- | :------------------ |
| `UUID`        | _Not yet supported_ |
| `CLOB`        | _Not yet supported_ |
| `REGCLASS`    | _Not yet supported_ |
| `CUSTOM`      | _Not yet supported_ |
| `ENUM`        | _Not yet supported_ |
| `SET`         | _Not yet supported_ |
| `DATETIME`    | _Not yet supported_ |
