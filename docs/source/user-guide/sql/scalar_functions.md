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

## Math Functions

### `abs(x)`

absolute value

### `acos(x)`

inverse cosine

### `asin(x)`

inverse sine

### `atan(x)`

inverse tangent

### `atan2(y, x)`

inverse tangent of y / x

### `ceil(x)`

nearest integer greater than or equal to argument

### `cos(x)`

cosine

### `exp(x)`

exponential

### `floor(x)`

nearest integer less than or equal to argument

### `ln(x)`

natural logarithm

### `log10(x)`

base 10 logarithm

### `log2(x)`

base 2 logarithm

### `power(base, exponent)`

base raised to the power of exponent

### `round(x)`

round to nearest integer

### `signum(x)`

sign of the argument (-1, 0, +1)

### `sin(x)`

sine

### `sqrt(x)`

square root

### `tan(x)`

tangent

### `trunc(x)`

truncate toward zero

## Conditional Functions

### `coalesce`

Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display.

### `nullif`

Returns a null value if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the `coalesce` expression. |

## String Functions

### `ascii`

### `bit_length`

### `btrim`

### `char_length`

### `character_length`

### `concat`

### `concat_ws`

### `chr`

### `initcap`

### `left`

### `length`

### `lower`

### `lpad`

### `ltrim`

### `md5`

### `octet_length`

### `repeat`

### `replace`

### `reverse`

### `right`

### `rpad`

### `rtrim`

### `digest`

### `split_part`

### `starts_with`

### `strpos`

### `substr`

### `translate`

### `trim`

### `upper`

## Regular Expression Functions

### regexp_match

### regexp_replace

## Temporal Functions

### `to_timestamp`

`to_timestamp()` is similar to the standard SQL function. It performs conversions to type `Timestamp(Nanoseconds, None)`, from:

- Timestamp strings
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are nanoseconds since Epoch UTC
- Other Timestamp() columns or values

Note that conversions from other Timestamp and Int64 types can also be performed using `CAST(.. AS Timestamp)`. However, the conversion functionality here is present for consistency with the other `to_timestamp_xx()` functions.

### `to_timestamp_millis`

`to_timestamp_millis()` does conversions to type `Timestamp(Milliseconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of Milliseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are milliseconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to millisecond resolution.

### `to_timestamp_micros`

`to_timestamp_micros()` does conversions to type `Timestamp(Microseconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of microseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are microseconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to microsecond resolution.

### `to_timestamp_seconds`

`to_timestamp_seconds()` does conversions to type `Timestamp(Seconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of secondseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are seconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to seconds resolution.

### `extract`

`extract(field FROM source)`

- The `extract` function retrieves subfields such as year or hour from date/time values.
  `source` must be a value expression of type timestamp, Date32, or Date64. `field` is an identifier that selects what field to extract from the source value.
  The `extract` function returns values of type u32.
  - `year` :`extract(year FROM to_timestamp('2020-09-08T12:00:00+00:00')) -> 2020`
  - `month`:`extract(month FROM to_timestamp('2020-09-08T12:00:00+00:00')) -> 9`
  - `week` :`extract(week FROM to_timestamp('2020-09-08T12:00:00+00:00')) -> 37`
  - `day`: `extract(day FROM to_timestamp('2020-09-08T12:00:00+00:00')) -> 8`
  - `hour`: `extract(hour FROM to_timestamp('2020-09-08T12:00:00+00:00')) -> 12`
  - `minute`: `extract(minute FROM to_timestamp('2020-09-08T12:01:00+00:00')) -> 1`
  - `second`: `extract(second FROM to_timestamp('2020-09-08T12:00:03+00:00')) -> 3`

### `date_part`

`date_part('field', source)`

- The `date_part` function is modeled on the postgres equivalent to the SQL-standard function `extract`.
  Note that here the field parameter needs to be a string value, not a name.
  The valid field names for `date_part` are the same as for `extract`.
  - `date_part('second', to_timestamp('2020-09-08T12:00:12+00:00')) -> 12`

### `date_trunc`

### `date_bin`

### `from_unixtime`

### `now`

Returns current time as `Timestamp(Nanoseconds, UTC)`. Returns same value for the function
wherever it appears in the statement, using a value chosen at planning time.

## Other Functions

### `array`

### `in_list`

### `random`

### `sha224`

### `sha256`

### `sha384`

### `sha512`

### `struct`

### `to_hex`
