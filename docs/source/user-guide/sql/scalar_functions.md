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

# Scalar Functions

Scalar functions operate on a single row at a time and return a single value.

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].
Please see the [Scalar Functions (new)](scalar_functions_new.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

## Conditional Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

## String Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

### `position`

Returns the position of `substr` in `origstr` (counting from 1). If `substr` does
not appear in `origstr`, return 0.

```
position(substr in origstr)
```

#### Arguments

- **substr**: The pattern string.
- **origstr**: The model string.

## Time and Date Functions

- [now](#now)
- [current_date](#current_date)
- [current_time](#current_time)
- [date_bin](#date_bin)
- [date_trunc](#date_trunc)
- [datetrunc](#datetrunc)
- [date_part](#date_part)
- [datepart](#datepart)
- [extract](#extract)
- [today](#today)
- [make_date](#make_date)
- [to_char](#to_char)
- [to_local_time](#to_local_time)
- [to_timestamp](#to_timestamp)
- [to_timestamp_millis](#to_timestamp_millis)
- [to_timestamp_micros](#to_timestamp_micros)
- [to_timestamp_seconds](#to_timestamp_seconds)
- [to_timestamp_nanos](#to_timestamp_nanos)
- [from_unixtime](#from_unixtime)
- [to_unixtime](#to_unixtime)

### `now`

Returns the current UTC timestamp.

The `now()` return value is determined at query time and will return the same timestamp,
no matter when in the query plan the function executes.

```
now()
```

### `current_date`

Returns the current UTC date.

The `current_date()` return value is determined at query time and will return the same date,
no matter when in the query plan the function executes.

```
current_date()
```

#### Aliases

- today

### `today`

_Alias of [current_date](#current_date)._

### `current_time`

Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time,
no matter when in the query plan the function executes.

```
current_time()
```

### `date_bin`

Calculates time intervals and returns the start of the interval nearest to the specified timestamp.
Use `date_bin` to downsample time series data by grouping rows into time-based "bins" or "windows"
and applying an aggregate or selector function to each window.

For example, if you "bin" or "window" data into 15 minute intervals, an input
timestamp of `2023-01-01T18:18:18Z` will be updated to the start time of the 15
minute bin it is in: `2023-01-01T18:15:00Z`.

```
date_bin(interval, expression, origin-timestamp)
```

#### Arguments

- **interval**: Bin interval.
- **expression**: Time expression to operate on.
  Can be a constant, column, or function.
- **origin-timestamp**: Optional. Starting point used to determine bin boundaries. If not specified
  defaults `1970-01-01T00:00:00Z` (the UNIX epoch in UTC).

The following intervals are supported:

- nanoseconds
- microseconds
- milliseconds
- seconds
- minutes
- hours
- days
- weeks
- months
- years
- century

### `date_trunc`

Truncates a timestamp value to a specified precision.

```
date_trunc(precision, expression)
```

#### Arguments

- **precision**: Time precision to truncate to.
  The following precisions are supported:

  - year / YEAR
  - quarter / QUARTER
  - month / MONTH
  - week / WEEK
  - day / DAY
  - hour / HOUR
  - minute / MINUTE
  - second / SECOND

- **expression**: Time expression to operate on.
  Can be a constant, column, or function.

#### Aliases

- datetrunc

### `datetrunc`

_Alias of [date_trunc](#date_trunc)._

### `date_part`

Returns the specified part of the date as an integer.

```
date_part(part, expression)
```

#### Arguments

- **part**: Part of the date to return.
  The following date parts are supported:

  - year
  - quarter _(emits value in inclusive range [1, 4] based on which quartile of the year the date is in)_
  - month
  - week _(week of the year)_
  - day _(day of the month)_
  - hour
  - minute
  - second
  - millisecond
  - microsecond
  - nanosecond
  - dow _(day of the week)_
  - doy _(day of the year)_
  - epoch _(seconds since Unix epoch)_

- **expression**: Time expression to operate on.
  Can be a constant, column, or function.

#### Aliases

- datepart

### `datepart`

_Alias of [date_part](#date_part)._

### `extract`

Returns a sub-field from a time value as an integer.

```
extract(field FROM source)
```

Equivalent to calling `date_part('field', source)`. For example, these are equivalent:

```sql
extract(day FROM '2024-04-13'::date)
date_part('day', '2024-04-13'::date)
```

See [date_part](#date_part).

### `make_date`

Make a date from year/month/day component parts.

```
make_date(year, month, day)
```

#### Arguments

- **year**: Year to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.
- **month**: Month to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.
- **day**: Day to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.

#### Example

```
> select make_date(2023, 1, 31);
+-------------------------------------------+
| make_date(Int64(2023),Int64(1),Int64(31)) |
+-------------------------------------------+
| 2023-01-31                                |
+-------------------------------------------+
> select make_date('2023', '01', '31');
+-----------------------------------------------+
| make_date(Utf8("2023"),Utf8("01"),Utf8("31")) |
+-----------------------------------------------+
| 2023-01-31                                    |
+-----------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/make_date.rs)

### `to_char`

Returns a string representation of a date, time, timestamp or duration based
on a [Chrono format]. Unlike the PostgreSQL equivalent of this function
numerical formatting is not supported.

```
to_char(expression, format)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function that results in a
  date, time, timestamp or duration.
- **format**: A [Chrono format] string to use to convert the expression.

#### Example

```
> select to_char('2023-03-01'::date, '%d-%m-%Y');
+----------------------------------------------+
| to_char(Utf8("2023-03-01"),Utf8("%d-%m-%Y")) |
+----------------------------------------------+
| 01-03-2023                                   |
+----------------------------------------------+
```

Additional examples can be found [here]

[here]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_char.rs

#### Aliases

- date_format

### `to_local_time`

Converts a timestamp with a timezone to a timestamp without a timezone (with no offset or
timezone information). This function handles daylight saving time changes.

```
to_local_time(expression)
```

#### Arguments

- **expression**: Time expression to operate on. Can be a constant, column, or function.

#### Example

```
> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp);
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels');
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT
  time,
  arrow_typeof(time) as type,
  to_local_time(time) as to_local_time,
  arrow_typeof(to_local_time(time)) as to_local_time_type
FROM (
  SELECT '2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels' AS time
);
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| time                      | type                                           | to_local_time       | to_local_time_type          |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| 2024-04-01T00:00:20+02:00 | Timestamp(Nanosecond, Some("Europe/Brussels")) | 2024-04-01T00:00:20 | Timestamp(Nanosecond, None) |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+

# combine `to_local_time()` with `date_bin()` to bin on boundaries in the timezone rather
# than UTC boundaries

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AS date_bin;
+---------------------+
| date_bin            |
+---------------------+
| 2024-04-01T00:00:00 |
+---------------------+

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AT TIME ZONE 'Europe/Brussels' AS date_bin_with_timezone;
+---------------------------+
| date_bin_with_timezone    |
+---------------------------+
| 2024-04-01T00:00:00+02:00 |
+---------------------------+
```

### `to_timestamp`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00Z`).
Supports strings, integer, unsigned integer, and double types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats] are provided.
Integers, unsigned integers, and doubles are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

Note: `to_timestamp` returns `Timestamp(Nanosecond)`. The supported range for integer input is between `-9223372037` and `9223372036`.
Supported range for string input is between `1677-09-21T00:12:44.0` and `2262-04-11T23:47:16.0`. Please use `to_timestamp_seconds`
for the input outside of supported bounds.

```
to_timestamp(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

[chrono format]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

#### Example

```
> select to_timestamp('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------+
| to_timestamp(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                             |
+-----------------------------------------------------------+
> select to_timestamp('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------+
| to_timestamp(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                          |
+--------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_millis`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as milliseconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_millis(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
> select to_timestamp_millis('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_millis(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123                                          |
+------------------------------------------------------------------+
> select to_timestamp_millis('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_millis(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123                                                                                       |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_micros`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as microseconds since the unix epoch (`1970-01-01T00:00:00Z`)
Returns the corresponding timestamp.

```
to_timestamp_micros(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
> select to_timestamp_micros('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_micros(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123456                                       |
+------------------------------------------------------------------+
> select to_timestamp_micros('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_micros(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456                                                                                    |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_nanos`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_nanos(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
> select to_timestamp_nanos('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------------+
| to_timestamp_nanos(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                                   |
+-----------------------------------------------------------------+
> select to_timestamp_nanos('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------------+
| to_timestamp_nanos(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                                |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_seconds`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_seconds(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
> select to_timestamp_seconds('2023-01-31T09:26:56.123456789-05:00');
+-------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-------------------------------------------------------------------+
| 2023-01-31T14:26:56                                               |
+-------------------------------------------------------------------+
> select to_timestamp_seconds('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+----------------------------------------------------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+----------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00                                                                                            |
+----------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `from_unixtime`

Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`)
return the corresponding timestamp.

```
from_unixtime(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `to_unixtime`

Converts a value to seconds since the unix epoch (`1970-01-01T00:00:00Z`).
Supports strings, dates, timestamps and double types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats] are provided.

```
to_unixtime(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
> select to_unixtime('2020-09-08T12:00:00+00:00');
+------------------------------------------------+
| to_unixtime(Utf8("2020-09-08T12:00:00+00:00")) |
+------------------------------------------------+
| 1599566400                                     |
+------------------------------------------------+
> select to_unixtime('01-14-2023 01:01:30+05:30', '%q', '%d-%m-%Y %H/%M/%S', '%+', '%m-%d-%Y %H:%M:%S%#z');
+-----------------------------------------------------------------------------------------------------------------------------+
| to_unixtime(Utf8("01-14-2023 01:01:30+05:30"),Utf8("%q"),Utf8("%d-%m-%Y %H/%M/%S"),Utf8("%+"),Utf8("%m-%d-%Y %H:%M:%S%#z")) |
+-----------------------------------------------------------------------------------------------------------------------------+
| 1673638290                                                                                                                  |
+-----------------------------------------------------------------------------------------------------------------------------+
```

## Array Functions

- [array_any_value](#array_any_value)
- [array_append](#array_append)
- [array_sort](#array_sort)
- [array_cat](#array_cat)
- [array_concat](#array_concat)
- [array_contains](#array_contains)
- [array_dims](#array_dims)
- [array_distance](#array_distance)
- [array_distinct](#array_distinct)
- [array_has](#array_has)
- [array_has_all](#array_has_all)
- [array_has_any](#array_has_any)
- [array_element](#array_element)
- [array_empty](#array_empty)
- [array_except](#array_except)
- [array_extract](#array_extract)
- [array_fill](#array_fill)
- [array_indexof](#array_indexof)
- [array_intersect](#array_intersect)
- [array_join](#array_join)
- [array_length](#array_length)
- [array_ndims](#array_ndims)
- [array_prepend](#array_prepend)
- [array_pop_front](#array_pop_front)
- [array_pop_back](#array_pop_back)
- [array_position](#array_position)
- [array_positions](#array_positions)
- [array_push_back](#array_push_back)
- [array_push_front](#array_push_front)
- [array_repeat](#array_repeat)
- [array_resize](#array_resize)
- [array_remove](#array_remove)
- [array_remove_n](#array_remove_n)
- [array_remove_all](#array_remove_all)
- [array_replace](#array_replace)
- [array_replace_n](#array_replace_n)
- [array_replace_all](#array_replace_all)
- [array_reverse](#array_reverse)
- [array_slice](#array_slice)
- [array_to_string](#array_to_string)
- [array_union](#array_union)
- [cardinality](#cardinality)
- [empty](#empty)
- [flatten](#flatten)
- [generate_series](#generate_series)
- [list_any_value](#list_any_value)
- [list_append](#list_append)
- [list_sort](#list_sort)
- [list_cat](#list_cat)
- [list_concat](#list_concat)
- [list_dims](#list_dims)
- [list_distance](#list_distance)
- [list_distinct](#list_distinct)
- [list_element](#list_element)
- [list_except](#list_except)
- [list_extract](#list_extract)
- [list_has](#list_has)
- [list_has_all](#list_has_all)
- [list_has_any](#list_has_any)
- [list_indexof](#list_indexof)
- [list_intersect](#list_intersect)
- [list_join](#list_join)
- [list_length](#list_length)
- [list_ndims](#list_ndims)
- [list_prepend](#list_prepend)
- [list_pop_back](#list_pop_back)
- [list_pop_front](#list_pop_front)
- [list_position](#list_position)
- [list_positions](#list_positions)
- [list_push_back](#list_push_back)
- [list_push_front](#list_push_front)
- [list_repeat](#list_repeat)
- [list_resize](#list_resize)
- [list_remove](#list_remove)
- [list_remove_n](#list_remove_n)
- [list_remove_all](#list_remove_all)
- [list_replace](#list_replace)
- [list_replace_n](#list_replace_n)
- [list_replace_all](#list_replace_all)
- [list_slice](#list_slice)
- [list_to_string](#list_to_string)
- [list_union](#list_union)
- [make_array](#make_array)
- [make_list](#make_list)
- [string_to_array](#string_to_array)
- [string_to_list](#string_to_list)
- [trim_array](#trim_array)
- [unnest](#unnest)
- [range](#range)

### `array_any_value`

Returns the first non-null element in the array.

```
array_any_value(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_any_value([NULL, 1, 2, 3]);
+--------------------------------------------------------------+
| array_any_value(List([NULL,1,2,3]))                          |
+--------------------------------------------------------------+
| 1                                                            |
+--------------------------------------------------------------+
```

### `array_append`

Appends an element to the end of an array.

```
array_append(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to append to the array.

#### Example

```
> select array_append([1, 2, 3], 4);
+--------------------------------------+
| array_append(List([1,2,3]),Int64(4)) |
+--------------------------------------+
| [1, 2, 3, 4]                         |
+--------------------------------------+
```

#### Aliases

- array_push_back
- list_append
- list_push_back

### `array_sort`

Sort array.

```
array_sort(array, desc, nulls_first)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **desc**: Whether to sort in descending order(`ASC` or `DESC`).
- **nulls_first**: Whether to sort nulls first(`NULLS FIRST` or `NULLS LAST`).

#### Example

```
> select array_sort([3, 1, 2]);
+-----------------------------+
| array_sort(List([3,1,2]))   |
+-----------------------------+
| [1, 2, 3]                   |
+-----------------------------+
```

#### Aliases

- list_sort

### `array_resize`

Resizes the list to contain size elements. Initializes new elements with value or empty if value is not set.

```
array_resize(array, size, value)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **size**: New size of given array.
- **value**: Defines new elements' value or empty if value is not set.

#### Example

```
> select array_resize([1, 2, 3], 5, 0);
+-------------------------------------+
| array_resize(List([1,2,3],5,0))     |
+-------------------------------------+
| [1, 2, 3, 0, 0]                     |
+-------------------------------------+
```

#### Aliases

- list_resize

### `array_cat`

_Alias of [array_concat](#array_concat)._

### `array_concat`

Concatenates arrays.

```
array_concat(array[, ..., array_n])
```

#### Arguments

- **array**: Array expression to concatenate.
  Can be a constant, column, or function, and any combination of array operators.
- **array_n**: Subsequent array column or literal array to concatenate.

#### Example

```
> select array_concat([1, 2], [3, 4], [5, 6]);
+---------------------------------------------------+
| array_concat(List([1,2]),List([3,4]),List([5,6])) |
+---------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                |
+---------------------------------------------------+
```

#### Aliases

- array_cat
- list_cat
- list_concat

### `array_contains`

_Alias of [array_has](#array_has)._

### `array_has`

Returns true if the array contains the element

```
array_has(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Scalar or Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has

### `array_has_all`

Returns true if all elements of sub-array exist in array

```
array_has_all(array, sub-array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **sub-array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has_all

### `array_has_any`

Returns true if any elements exist in both arrays

```
array_has_any(array, sub-array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **sub-array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has_any

### `array_dims`

Returns an array of the array's dimensions.

```
array_dims(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_dims([[1, 2, 3], [4, 5, 6]]);
+---------------------------------+
| array_dims(List([1,2,3,4,5,6])) |
+---------------------------------+
| [2, 3]                          |
+---------------------------------+
```

#### Aliases

- list_dims

### `array_distance`

Returns the Euclidean distance between two input arrays of equal length.

```
array_distance(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_distance([1, 2], [1, 4]);
+------------------------------------+
| array_distance(List([1,2], [1,4])) |
+------------------------------------+
| 2.0                                |
+------------------------------------+
```

#### Aliases

- list_distance

### `array_distinct`

Returns distinct values from the array after removing duplicates.

```
array_distinct(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_distinct([1, 3, 2, 3, 1, 2, 4]);
+---------------------------------+
| array_distinct(List([1,2,3,4])) |
+---------------------------------+
| [1, 2, 3, 4]                    |
+---------------------------------+
```

#### Aliases

- list_distinct

### `array_element`

Extracts the element with the index n from the array.

```
array_element(array, index)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **index**: Index to extract the element from the array.

#### Example

```
> select array_element([1, 2, 3, 4], 3);
+-----------------------------------------+
| array_element(List([1,2,3,4]),Int64(3)) |
+-----------------------------------------+
| 3                                       |
+-----------------------------------------+
```

#### Aliases

- array_extract
- list_element
- list_extract

### `array_extract`

_Alias of [array_element](#array_element)._

### `array_fill`

Returns an array filled with copies of the given value.

DEPRECATED: use `array_repeat` instead!

```
array_fill(element, array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to copy to the array.

### `flatten`

Converts an array of arrays to a flat array

- Applies to any depth of nested arrays
- Does not change arrays that are already flat

The flattened array contains all the elements from all source arrays.

#### Arguments

- **array**: Array expression
  Can be a constant, column, or function, and any combination of array operators.

```
flatten(array)
```

### `array_indexof`

_Alias of [array_position](#array_position)._

### `array_intersect`

Returns an array of elements in the intersection of array1 and array2.

```
array_intersect(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);       |
+----------------------------------------------------+
| [3, 4]                                             |
+----------------------------------------------------+
> select array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);       |
+----------------------------------------------------+
| []                                                 |
+----------------------------------------------------+
```

---

#### Aliases

- list_intersect

### `array_join`

_Alias of [array_to_string](#array_to_string)._

### `array_length`

Returns the length of the array dimension.

```
array_length(array, dimension)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **dimension**: Array dimension.

#### Example

```
> select array_length([1, 2, 3, 4, 5]);
+---------------------------------+
| array_length(List([1,2,3,4,5])) |
+---------------------------------+
| 5                               |
+---------------------------------+
```

#### Aliases

- list_length

### `array_ndims`

Returns the number of dimensions of the array.

```
array_ndims(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_ndims([[1, 2, 3], [4, 5, 6]]);
+----------------------------------+
| array_ndims(List([1,2,3,4,5,6])) |
+----------------------------------+
| 2                                |
+----------------------------------+
```

#### Aliases

- list_ndims

### `array_prepend`

Prepends an element to the beginning of an array.

```
array_prepend(element, array)
```

#### Arguments

- **element**: Element to prepend to the array.
- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_prepend(1, [2, 3, 4]);
+---------------------------------------+
| array_prepend(Int64(1),List([2,3,4])) |
+---------------------------------------+
| [1, 2, 3, 4]                          |
+---------------------------------------+
```

#### Aliases

- array_push_front
- list_prepend
- list_push_front

### `array_pop_front`

Returns the array without the first element.

```
array_pop_front(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_pop_front([1, 2, 3]);
+-------------------------------+
| array_pop_front(List([1,2,3])) |
+-------------------------------+
| [2, 3]                        |
+-------------------------------+
```

#### Aliases

- list_pop_front

### `array_pop_back`

Returns the array without the last element.

```
array_pop_back(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_pop_back([1, 2, 3]);
+-------------------------------+
| array_pop_back(List([1,2,3])) |
+-------------------------------+
| [1, 2]                        |
+-------------------------------+
```

#### Aliases

- list_pop_back

### `array_position`

Returns the position of the first occurrence of the specified element in the array.

```
array_position(array, element)
array_position(array, element, index)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for position in the array.
- **index**: Index at which to start searching.

#### Example

```
> select array_position([1, 2, 2, 3, 1, 4], 2);
+----------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2)) |
+----------------------------------------------+
| 2                                            |
+----------------------------------------------+
```

#### Aliases

- array_indexof
- list_indexof
- list_position

### `array_positions`

Searches for an element in the array, returns all occurrences.

```
array_positions(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for positions in the array.

#### Example

```
> select array_positions([1, 2, 2, 3, 1, 4], 2);
+-----------------------------------------------+
| array_positions(List([1,2,2,3,1,4]),Int64(2)) |
+-----------------------------------------------+
| [2, 3]                                        |
+-----------------------------------------------+
```

#### Aliases

- list_positions

### `array_push_back`

_Alias of [array_append](#array_append)._

### `array_push_front`

_Alias of [array_prepend](#array_prepend)._

### `array_repeat`

Returns an array containing element `count` times.

```
array_repeat(element, count)
```

#### Arguments

- **element**: Element expression.
  Can be a constant, column, or function, and any combination of array operators.
- **count**: Value of how many times to repeat the element.

#### Example

```
> select array_repeat(1, 3);
+---------------------------------+
| array_repeat(Int64(1),Int64(3)) |
+---------------------------------+
| [1, 1, 1]                       |
+---------------------------------+
```

```
> select array_repeat([1, 2], 2);
+------------------------------------+
| array_repeat(List([1,2]),Int64(2)) |
+------------------------------------+
| [[1, 2], [1, 2]]                   |
+------------------------------------+
```

#### Aliases

- list_repeat

### `array_remove`

Removes the first element from the array equal to the given value.

```
array_remove(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.

#### Example

```
> select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+
```

#### Aliases

- list_remove

### `array_remove_n`

Removes the first `max` elements from the array equal to the given value.

```
array_remove_n(array, element, max)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.
- **max**: Number of first occurrences to remove.

#### Example

```
> select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+
```

#### Aliases

- list_remove_n

### `array_remove_all`

Removes all elements from the array equal to the given value.

```
array_remove_all(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.

#### Example

```
> select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+
```

#### Aliases

- list_remove_all

### `array_replace`

Replaces the first occurrence of the specified element with another specified element.

```
array_replace(array, from, to)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.

#### Example

```
> select array_replace([1, 2, 2, 3, 2, 1, 4], 2, 5);
+--------------------------------------------------------+
| array_replace(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+--------------------------------------------------------+
| [1, 5, 2, 3, 2, 1, 4]                                  |
+--------------------------------------------------------+
```

#### Aliases

- list_replace

### `array_replace_n`

Replaces the first `max` occurrences of the specified element with another specified element.

```
array_replace_n(array, from, to, max)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.
- **max**: Number of first occurrences to replace.

#### Example

```
> select array_replace_n([1, 2, 2, 3, 2, 1, 4], 2, 5, 2);
+-------------------------------------------------------------------+
| array_replace_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(5),Int64(2)) |
+-------------------------------------------------------------------+
| [1, 5, 5, 3, 2, 1, 4]                                             |
+-------------------------------------------------------------------+
```

#### Aliases

- list_replace_n

### `array_replace_all`

Replaces all occurrences of the specified element with another specified element.

```
array_replace_all(array, from, to)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.

#### Example

```
> select array_replace_all([1, 2, 2, 3, 2, 1, 4], 2, 5);
+------------------------------------------------------------+
| array_replace_all(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+------------------------------------------------------------+
| [1, 5, 5, 3, 5, 1, 4]                                      |
+------------------------------------------------------------+
```

#### Aliases

- list_replace_all

### `array_reverse`

Returns the array with the order of the elements reversed.

```
array_reverse(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_reverse([1, 2, 3, 4]);
+------------------------------------------------------------+
| array_reverse(List([1, 2, 3, 4]))                          |
+------------------------------------------------------------+
| [4, 3, 2, 1]                                               |
+------------------------------------------------------------+
```

#### Aliases

- list_reverse

### `array_slice`

Returns a slice of the array based on 1-indexed start and end positions.

```
array_slice(array, begin, end)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **begin**: Index of the first element.
  If negative, it counts backward from the end of the array.
- **end**: Index of the last element.
  If negative, it counts backward from the end of the array.
- **stride**: Stride of the array slice. The default is 1.

#### Example

```
> select array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6);
+--------------------------------------------------------+
| array_slice(List([1,2,3,4,5,6,7,8]),Int64(3),Int64(6)) |
+--------------------------------------------------------+
| [3, 4, 5, 6]                                           |
+--------------------------------------------------------+
```

#### Aliases

- list_slice

### `array_to_string`

Converts each element to its text representation.

```
array_to_string(array, delimiter)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **delimiter**: Array element separator.

#### Example

```
> select array_to_string([[1, 2, 3, 4], [5, 6, 7, 8]], ',');
+----------------------------------------------------+
| array_to_string(List([1,2,3,4,5,6,7,8]),Utf8(",")) |
+----------------------------------------------------+
| 1,2,3,4,5,6,7,8                                    |
+----------------------------------------------------+
```

#### Aliases

- array_join
- list_join
- list_to_string

### `array_union`

Returns an array of elements that are present in both arrays (all elements from both arrays) with out duplicates.

```
array_union(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_union([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                 |
+----------------------------------------------------+
> select array_union([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 7, 8]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8]                           |
+----------------------------------------------------+
```

---

#### Aliases

- list_union

### `array_except`

Returns an array of the elements that appear in the first array but not in the second.

```
array_except(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select array_except([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2]                                 |
+----------------------------------------------------+
> select array_except([1, 2, 3, 4], [3, 4, 5, 6]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [3, 4, 5, 6]);           |
+----------------------------------------------------+
| [1, 2]                                 |
+----------------------------------------------------+
```

---

#### Aliases

- list_except

### `cardinality`

Returns the total number of elements in the array.

```
cardinality(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]);
+--------------------------------------+
| cardinality(List([1,2,3,4,5,6,7,8])) |
+--------------------------------------+
| 8                                    |
+--------------------------------------+
```

### `empty`

Returns 1 for an empty array or 0 for a non-empty array.

```
empty(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
> select empty([1]);
+------------------+
| empty(List([1])) |
+------------------+
| 0                |
+------------------+
```

#### Aliases

- array_empty,
- list_empty

### `generate_series`

Similar to the range function, but it includes the upper bound.

```
generate_series(start, stop, step)
```

#### Arguments

- **start**: start of the series. Ints, timestamps, dates or string types that can be coerced to Date32 are supported.
- **end**: end of the series (included). Type must be the same as start.
- **step**: increase by step (can not be 0). Steps less than a day are supported only for timestamp ranges.

#### Example

```
> select generate_series(1,3);
+------------------------------------+
| generate_series(Int64(1),Int64(3)) |
+------------------------------------+
| [1, 2, 3]                          |
+------------------------------------+
```

### `list_any_value`

_Alias of [array_any_value](#array_any_value)._

### `list_append`

_Alias of [array_append](#array_append)._

### `list_cat`

_Alias of [array_concat](#array_concat)._

### `list_concat`

_Alias of [array_concat](#array_concat)._

### `list_dims`

_Alias of [array_dims](#array_dims)._

### `list_distance`

_Alias of [array_distance](#array_distance)._

### `list_distinct`

_Alias of [array_distinct](#array_distinct)._

### `list_element`

_Alias of [array_element](#array_element)._

### `list_empty`

_Alias of [empty](#empty)._

### `list_except`

_Alias of [array_element](#array_except)._

### `list_extract`

_Alias of [array_element](#array_element)._

### `list_has`

_Alias of [array_has](#array_has)._

### `list_has_all`

_Alias of [array_has_all](#array_has_all)._

### `list_has_any`

_Alias of [array_has_any](#array_has_any)._

### `list_indexof`

_Alias of [array_position](#array_position)._

### `list_intersect`

_Alias of [array_position](#array_intersect)._

### `list_join`

_Alias of [array_to_string](#array_to_string)._

### `list_length`

_Alias of [array_length](#array_length)._

### `list_ndims`

_Alias of [array_ndims](#array_ndims)._

### `list_prepend`

_Alias of [array_prepend](#array_prepend)._

### `list_pop_back`

_Alias of [array_pop_back](#array_pop_back)._

### `list_pop_front`

_Alias of [array_pop_front](#array_pop_front)._

### `list_position`

_Alias of [array_position](#array_position)._

### `list_positions`

_Alias of [array_positions](#array_positions)._

### `list_push_back`

_Alias of [array_append](#array_append)._

### `list_push_front`

_Alias of [array_prepend](#array_prepend)._

### `list_repeat`

_Alias of [array_repeat](#array_repeat)._

### `list_resize`

_Alias of [array_resize](#array_resize)._

### `list_remove`

_Alias of [array_remove](#array_remove)._

### `list_remove_n`

_Alias of [array_remove_n](#array_remove_n)._

### `list_remove_all`

_Alias of [array_remove_all](#array_remove_all)._

### `list_replace`

_Alias of [array_replace](#array_replace)._

### `list_replace_n`

_Alias of [array_replace_n](#array_replace_n)._

### `list_replace_all`

_Alias of [array_replace_all](#array_replace_all)._

### `list_reverse`

_Alias of [array_reverse](#array_reverse)._

### `list_slice`

_Alias of [array_slice](#array_slice)._

### `list_sort`

_Alias of [array_sort](#array_sort)._

### `list_to_string`

_Alias of [array_to_string](#array_to_string)._

### `list_union`

_Alias of [array_union](#array_union)._

### `make_array`

Returns an Arrow array using the specified input expressions.

```
make_array(expression1[, ..., expression_n])
```

### `array_empty`

_Alias of [empty](#empty)._

#### Arguments

- **expression_n**: Expression to include in the output array.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

#### Example

```
> select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```

#### Aliases

- make_list

### `make_list`

_Alias of [make_array](#make_array)._

### `string_to_array`

Splits a string in to an array of substrings based on a delimiter. Any substrings matching the optional `null_str` argument are replaced with NULL.
`SELECT string_to_array('abc##def', '##')` or `SELECT string_to_array('abc def', ' ', 'def')`

```
starts_with(str, delimiter[, null_str])
```

#### Arguments

- **str**: String expression to split.
- **delimiter**: Delimiter string to split on.
- **null_str**: Substring values to be replaced with `NULL`

#### Aliases

- string_to_list

### `string_to_list`

_Alias of [string_to_array](#string_to_array)._

### `trim_array`

Removes the last n elements from the array.

DEPRECATED: use `array_slice` instead!

```
trim_array(array, n)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **n**: Element to trim the array.

### `unnest`

Transforms an array into rows.

#### Arguments

- **array**: Array expression to unnest.
  Can be a constant, column, or function, and any combination of array operators.

#### Examples

```
> select unnest(make_array(1, 2, 3, 4, 5));
+------------------------------------------------------------------+
| unnest(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5))) |
+------------------------------------------------------------------+
| 1                                                                |
| 2                                                                |
| 3                                                                |
| 4                                                                |
| 5                                                                |
+------------------------------------------------------------------+
```

```
> select unnest(range(0, 10));
+-----------------------------------+
| unnest(range(Int64(0),Int64(10))) |
+-----------------------------------+
| 0                                 |
| 1                                 |
| 2                                 |
| 3                                 |
| 4                                 |
| 5                                 |
| 6                                 |
| 7                                 |
| 8                                 |
| 9                                 |
+-----------------------------------+
```

### `range`

Returns an Arrow array between start and stop with step. `SELECT range(2, 10, 3) -> [2, 5, 8]` or `SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);`

The range start..end contains all values with start <= x < end. It is empty if start >= end.

Step can not be 0 (then the range will be nonsense.).

Note that when the required range is a number, it accepts (stop), (start, stop), and (start, stop, step) as parameters, but when the required range is a date or timestamp, it must be 3 non-NULL parameters.
For example,

```
SELECT range(3);
SELECT range(1,5);
SELECT range(1,5,1);
```

are allowed in number ranges

but in date and timestamp ranges, only

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
SELECT range(TIMESTAMP '1992-09-01', TIMESTAMP '1993-03-01', INTERVAL '1' MONTH);
```

is allowed, and

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', NULL);
SELECT range(NULL, DATE '1993-03-01', INTERVAL '1' MONTH);
SELECT range(DATE '1992-09-01', NULL, INTERVAL '1' MONTH);
```

are not allowed

#### Arguments

- **start**: start of the range. Ints, timestamps, dates or string types that can be coerced to Date32 are supported.
- **end**: end of the range (not included). Type must be the same as start.
- **step**: increase by step (can not be 0). Steps less than a day are supported only for timestamp ranges.

#### Aliases

- generate_series

## Struct Functions

- [unnest](#unnest-struct)

For more struct functions see the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

### `unnest (struct)`

Unwraps struct fields into columns.

#### Arguments

- **struct**: Object expression to unnest.
  Can be a constant, column, or function, and any combination of object operators.

#### Examples

```
> select * from foo;
+---------------------+
| column1             |
+---------------------+
| {a: 5, b: a string} |
+---------------------+

> select unnest(column1) from foo;
+-----------------------+-----------------------+
| unnest(foo.column1).a | unnest(foo.column1).b |
+-----------------------+-----------------------+
| 5                     | a string              |
+-----------------------+-----------------------+
```

## Map Functions

- [map](#map)
- [make_map](#make_map)
- [map_extract](#map_extract)
- [map_keys](#map_keys)
- [map_values](#map_values)

### `map`

Returns an Arrow map with the specified key-value pairs.

```
map(key, value)
map(key: value)
```

#### Arguments

- **key**: Expression to be used for key.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.
- **value**: Expression to be used for value.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT MAP(['POST', 'HEAD', 'PATCH'], [41, 33, null]);
----
{POST: 41, HEAD: 33, PATCH: }

SELECT MAP([[1,2], [3,4]], ['a', 'b']);
----
{[1, 2]: a, [3, 4]: b}

SELECT MAP { 'a': 1, 'b': 2 };
----
{a: 1, b: 2}
```

### `make_map`

Returns an Arrow map with the specified key-value pairs.

```
make_map(key_1, value_1, ..., key_n, value_n)
```

#### Arguments

- **key_n**: Expression to be used for key.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.
- **value_n**: Expression to be used for value.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT MAKE_MAP('POST', 41, 'HEAD', 33, 'PATCH', null);
----
{POST: 41, HEAD: 33, PATCH: }
```

### `map_extract`

Return a list containing the value for a given key or an empty list if the key is not contained in the map.

```
map_extract(map, key)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.
- `key`: Key to extract from the map.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT map_extract(MAP {'a': 1, 'b': NULL, 'c': 3}, 'a');
----
[1]
```

#### Aliases

- element_at

### `map_keys`

Return a list of all keys in the map.

```
map_keys(map)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.

#### Example

```
SELECT map_keys(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[a, b, c]

select map_keys(map([100, 5], [42,43]));
----
[100, 5]
```

### `map_values`

Return a list of all values in the map.

```
map_values(map)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.

#### Example

```
SELECT map_values(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[1, , 3]

select map_values(map([100, 5], [42,43]));
----
[42, 43]
```

## Other Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)
