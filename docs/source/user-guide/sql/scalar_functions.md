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

- [abs](#abs)
- [acos](#acos)
- [acosh](#acosh)
- [asin](#asin)
- [asinh](#asinh)
- [atan](#atan)
- [atanh](#atanh)
- [atan2](#atan2)
- [cbrt](#cbrt)
- [ceil](#ceil)
- [cos](#cos)
- [cosh](#cosh)
- [degrees](#degrees)
- [exp](#exp)
- [factorial](#factorial)
- [floor](#floor)
- [gcd](#gcd)
- [lcm](#lcm)
- [ln](#ln)
- [log](#log)
- [log10](#log10)
- [log2](#log2)
- [pi](#pi)
- [power](#power)
- [pow](#pow)
- [radians](#radians)
- [random](#random)
- [round](#round)
- [signum](#signum)
- [sin](#sin)
- [sinh](#sinh)
- [sqrt](#sqrt)
- [tan](#tan)
- [tanh](#tanh)
- [trunc](#trunc)

### `abs`

Returns the absolute value of a number.

```
abs(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `acos`

Returns the arc cosine or inverse cosine of a number.

```
acos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `acosh`

Returns the area hyperbolic cosine or inverse hyperbolic cosine of a number.

```
acosh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `asin`

Returns the arc sine or inverse sine of a number.

```
asin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `asinh`

Returns the area hyperbolic sine or inverse hyperbolic sine of a number.

```
asinh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atan`

Returns the arc tangent or inverse tangent of a number.

```
atan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atanh`

Returns the area hyperbolic tangent or inverse hyperbolic tangent of a number.

```
atanh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atan2`

Returns the arc tangent or inverse tangent of `expression_y / expression_x`.

```
atan2(expression_y, expression_x)
```

#### Arguments

- **expression_y**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cbrt`

Returns the cube root of a number.

```
cbrt(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `ceil`

Returns the nearest integer greater than or equal to a number.

```
ceil(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cos`

Returns the cosine of a number.

```
cos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cosh`

Returns the hyperbolic cosine of a number.

```
cosh(numeric_expression)
```

### `degrees`

Converts radians to degrees.

```
degrees(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `exp`

Returns the base-e exponential of a number.

```
exp(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to use as the exponent.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `factorial`

Factorial. Returns 1 if value is less than 2.

```
factorial(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `floor`

Returns the nearest integer less than or equal to a number.

```
floor(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `gcd`

Returns the greatest common divisor of `expression_x` and `expression_y`. Returns 0 if both inputs are zero.

```
gcd(expression_x, expression_y)
```

#### Arguments

- **expression_x**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_y**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `lcm`

Returns the least common multiple of `expression_x` and `expression_y`. Returns 0 if either input is zero.

```
lcm(expression_x, expression_y)
```

#### Arguments

- **expression_x**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_y**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `ln`

Returns the natural logarithm of a number.

```
ln(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log`

Returns the base-x logarithm of a number.
Can either provide a specified base, or if omitted then takes the base-10 of a number.

```
log(base, numeric_expression)
log(numeric_expression)
```

#### Arguments

- **base**: Base numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log10`

Returns the base-10 logarithm of a number.

```
log10(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log2`

Returns the base-2 logarithm of a number.

```
log2(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `pi`

Returns an approximate value of π.

```
pi()
```

### `power`

Returns a base expression raised to the power of an exponent.

```
power(base, exponent)
```

#### Arguments

- **base**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **exponent**: Exponent numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

#### Aliases

- pow

### `pow`

_Alias of [power](#power)._

### `radians`

Converts degrees to radians.

```
radians(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  =======

### `random`

Returns a random float value in the range [0, 1).
The random seed is unique to each row.

```
random()
```

### `round`

Rounds a number to the nearest integer.

```
round(numeric_expression[, decimal_places])
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **decimal_places**: Optional. The number of decimal places to round to.
  Defaults to 0.

### `signum`

Returns the sign of a number.
Negative numbers return `-1`.
Zero and positive numbers return `1`.

```
signum(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sin`

Returns the sine of a number.

```
sin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sinh`

Returns the hyperbolic sine of a number.

```
sinh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sqrt`

Returns the square root of a number.

```
sqrt(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `tan`

Returns the tangent of a number.

```
tan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `tanh`

Returns the hyperbolic tangent of a number.

```
tanh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `trunc`

Truncates a number toward zero (at the decimal point).

```
trunc(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## Conditional Functions

- [coalesce](#coalesce)
- [nullif](#nullif)

### `coalesce`

Returns the first of its arguments that is not _null_.
Returns _null_ if all arguments are _null_.
This function is often used to substitute a default value for _null_ values.

```
coalesce(expression1[, ..., expression_n])
```

#### Arguments

- **expression1, expression_n**:
  Expression to use if previous expressions are _null_.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  Pass as many expression arguments as necessary.

### `nullif`

Returns _null_ if _expression1_ equals _expression2_; otherwise it returns _expression1_.
This can be used to perform the inverse operation of [`coalesce`](#coalesce).

```
nullif(expression1, expression2)
```

#### Arguments

- **expression1**: Expression to compare and return if equal to expression2.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Expression to compare to expression1.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## String Functions

- [ascii](#ascii)
- [bit_length](#bit_length)
- [btrim](#btrim)
- [char_length](#char_length)
- [character_length](#character_length)
- [concat](#concat)
- [concat_ws](#concat_ws)
- [chr](#chr)
- [initcap](#initcap)
- [left](#left)
- [length](#length)
- [lower](#lower)
- [lpad](#lpad)
- [ltrim](#ltrim)
- [octet_length](#octet_length)
- [repeat](#repeat)
- [replace](#replace)
- [reverse](#reverse)
- [right](#right)
- [rpad](#rpad)
- [rtrim](#rtrim)
- [split_part](#split_part)
- [starts_with](#starts_with)
- [strpos](#strpos)
- [substr](#substr)
- [to_hex](#to_hex)
- [translate](#translate)
- [trim](#trim)
- [upper](#upper)
- [uuid](#uuid)

### `ascii`

Returns the ASCII value of the first character in a string.

```
ascii(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[chr](#chr)

### `bit_length`

Returns the bit length of a string.

```
bit_length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[length](#length),
[octet_length](#octet_length)

### `btrim`

Trims the specified trim string from the start and end of a string.
If no trim string is provided, all whitespace is removed from the start and end
of the input string.

```
btrim(str[, trim_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **trim_str**: String expression to trim from the beginning and end of the input string.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  _Default is whitespace characters_.

**Related functions**:
[ltrim](#ltrim),
[rtrim](#rtrim),
[trim](#trim)

### `char_length`

_Alias of [length](#length)._

### `character_length`

_Alias of [length](#length)._

### `concat`

Concatenates multiple strings together.

```
concat(str[, ..., str_n])
```

#### Arguments

- **str**: String expression to concatenate.
  Can be a constant, column, or function, and any combination of string operators.
- **str_n**: Subsequent string column or literal string to concatenate.

**Related functions**:
[contcat_ws](#contcat_ws)

### `concat_ws`

Concatenates multiple strings together with a specified separator.

```
concat(separator, str[, ..., str_n])
```

#### Arguments

- **separator**: Separator to insert between concatenated strings.
- **str**: String expression to concatenate.
  Can be a constant, column, or function, and any combination of string operators.
- **str_n**: Subsequent string column or literal string to concatenate.

**Related functions**:
[concat](#concat)

### `chr`

Returns the character with the specified ASCII or Unicode code value.

```
chr(expression)
```

#### Arguments

- **expression**: Expression containing the ASCII or Unicode code value to operate on.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

**Related functions**:
[ascii](#ascii)

### `initcap`

Capitalizes the first character in each word in the input string.
Words are delimited by non-alphanumeric characters.

```
initcap(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[lower](#lower),
[upper](#upper)

### `left`

Returns a specified number of characters from the left side of a string.

```
left(str, n)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of characters to return.

**Related functions**:
[right](#right)

### `length`

Returns the number of characters in a string.

```
length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

#### Aliases

- char_length
- character_length

**Related functions**:
[bit_length](#bit_length),
[octet_length](#octet_length)

### `lower`

Converts a string to lower-case.

```
lower(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[initcap](#initcap),
[upper](#upper)

### `lpad`

Pads the left side of a string with another string to a specified string length.

```
lpad(str, n[, padding_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: String length to pad to.
- **padding_str**: String expression to pad with.
  Can be a constant, column, or function, and any combination of string operators.
  _Default is a space._

**Related functions**:
[rpad](#rpad)

### `ltrim`

Removes leading spaces from a string.

```
ltrim(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[btrim](#btrim),
[rtrim](#rtrim),
[trim](#trim)

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `octet_length`

Returns the length of a string in bytes.

```
octet_length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[bit_length](#bit_length),
[length](#length)

### `repeat`

Returns a string with an input string repeated a specified number.

```
repeat(str, n)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of times to repeat the input string.

### `replace`

Replaces all occurrences of a specified substring in a string with a new substring.

```
replace(str, substr, replacement)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring expression to replace in the input string.
  Can be a constant, column, or function, and any combination of string operators.
- **replacement**: Replacement substring expression.
  Can be a constant, column, or function, and any combination of string operators.

### `reverse`

Reverses the character order of a string.

```
reverse(str)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.

### `right`

Returns a specified number of characters from the right side of a string.

```
right(str, n)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of characters to return.

**Related functions**:
[left](#left)

### `rpad`

Pads the right side of a string with another string to a specified string length.

```
rpad(str, n[, padding_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: String length to pad to.
- **padding_str**: String expression to pad with.
  Can be a constant, column, or function, and any combination of string operators.
  _Default is a space._

**Related functions**:
[lpad](#lpad)

### `rtrim`

Removes trailing spaces from a string.

```
rtrim(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[btrim](#btrim),
[ltrim](#ltrim),
[trim](#trim)

### `split_part`

Splits a string based on a specified delimiter and returns the substring in the
specified position.

```
split_part(str, delimiter, pos)
```

#### Arguments

- **str**: String expression to spit.
  Can be a constant, column, or function, and any combination of string operators.
- **delimiter**: String or character to split on.
- **pos**: Position of the part to return.

### `starts_with`

Tests if a string starts with a substring.

```
starts_with(str, substr)
```

#### Arguments

- **str**: String expression to test.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring to test for.

### `strpos`

Returns the starting position of a specified substring in a string.
Positions begin at 1.
If the substring does not exist in the string, the function returns 0.

```
strpos(str, substr)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring expression to search for.
  Can be a constant, column, or function, and any combination of string operators.

### `substr`

Extracts a substring of a specified number of characters from a specific
starting position in a string.

```
substr(str, start_pos[, length])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **start_pos**: Character position to start the substring at.
  The first character in the string has a position of 1.
- **length**: Number of characters to extract.
  If not specified, returns the rest of the string after the start position.

### `translate`

Translates characters in a string to specified translation characters.

```
translate(str, chars, translation)
```

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **chars**: Characters to translate.
- **translation**: Translation characters. Translation characters replace only
  characters at the same position in the **chars** string.

### `to_hex`

Converts an integer to a hexadecimal string.

```
to_hex(int)
```

#### Arguments

- **int**: Integer expression to convert.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `trim`

Removes leading and trailing spaces from a string.

```
trim(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[btrim](#btrim),
[ltrim](#ltrim),
[rtrim](#rtrim)

### `upper`

Converts a string to upper-case.

```
upper(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[initcap](#initcap),
[lower](#lower)

### `uuid`

Returns UUID v4 string value which is unique per row.

```
uuid()
```

## Regular Expression Functions

Apache DataFusion uses the POSIX regular expression syntax and
supports the following regular expression functions:

- [regexp_match](#regexp_match)
- [regexp_replace](#regexp_replace)

### `regexp_match`

Returns a list of regular expression matches in a string.

```
regexp_match(str, regexp)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **regexp**: Regular expression to match against.
  Can be a constant, column, or function.

### `regexp_replace`

Replaces substrings in a string that match a regular expression.

```
regexp_replace(str, regexp, replacement, flags)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **regexp**: Regular expression to match against.
  Can be a constant, column, or function.
- **replacement**: Replacement string expression.
  Can be a constant, column, or function, and any combination of string operators.
- **flags**: Regular expression flags that control the behavior of the
  regular expression. The following flags are supported.
  - **g**: (global) Search globally and don't return after the first match.
  - **i**: (insensitive) Ignore case when matching.

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
- [to_timestamp](#to_timestamp)
- [to_timestamp_millis](#to_timestamp_millis)
- [to_timestamp_micros](#to_timestamp_micros)
- [to_timestamp_seconds](#to_timestamp_seconds)
- [from_unixtime](#from_unixtime)

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
- **timestamp**: Starting point used to determine bin boundaries.

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

- **expression**: Time expression to operate on.
  Can be a constant, column, or function.

#### Aliases

- datepart

### `datepart`

_Alias of [date_part](#date_part)._

### `extract`

Returns a sub-field from a time value as an integer.
Similar to `date_part`, but with different arguments.

```
extract(field FROM source)
```

#### Arguments

- **field**: Part or field of the date to return.
  The following date fields are supported:

  - year
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

- **source**: Source time expression to operate on.
  Can be a constant, column, or function.

### `to_timestamp`

Converts a value to RFC3339 nanosecond timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 nanosecond timestamp.

```
to_timestamp(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `to_timestamp_millis`

Converts a value to RFC3339 millisecond timestamp format (`YYYY-MM-DDT00:00:00.000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_millis(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `to_timestamp_micros`

Converts a value to RFC3339 microsecond timestamp format (`YYYY-MM-DDT00:00:00.000000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_micros(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `to_timestamp_seconds`

Converts a value to RFC3339 second timestamp format (`YYYY-MM-DDT00:00:00Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_seconds(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `from_unixtime`

Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Input is parsed as a Unix nanosecond timestamp and returns the corresponding
RFC3339 timestamp.

```
from_unixtime(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## Array Functions

- [array_append](#array_append)
- [array_concat](#array_concat)
- [array_dims](#array_dims)
- [array_fill](#array_fill)
- [array_length](#array_length)
- [array_ndims](#array_ndims)
- [array_position](#array_position)
- [array_positions](#array_positions)
- [array_prepend](#array_prepend)
- [array_remove](#array_remove)
- [array_replace](#array_replace)
- [array_to_string](#array_to_string)
- [cardinality](#cardinality)
- [make_array](#make_array)
- [trim_array](#trim_array)

### `array_append`

Appends an element to the end of an array.

```
array_append(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to append to the array.

### `array_concat`

Concatenates arrays.

```
array_concat(array[, ..., array_n])
```

#### Arguments

- **array**: Array expression to concatenate.
  Can be a constant, column, or function, and any combination of array operators.
- **array_n**: Subsequent array column or literal array to concatenate.

### `array_dims`

Returns an array of the array's dimensions.

```
array_dims(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

### `array_fill`

Returns an array filled with copies of the given value.

```
array_fill(element, array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to copy to the array.

### `array_length`

Returns the length of the array dimension.

```
array_length(array, dimension)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **dimension**: Array dimension.

### `array_ndims`

Returns the number of dimensions of the array.

```
array_ndims(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

### `array_position`

Returns a string with an input string repeated a specified number.

```
array_position(array, element)
array_position(array, element, index)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for position in the array.
- **index**: Index at which to start searching.

### `array_positions`

Searches for an element in the array, returns all occurrences.

```
array_positions(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for positions in the array.

### `array_prepend`

Prepends an element to the beginning of an array.

```
array_prepend(element, array)
```

#### Arguments

- **element**: Element to prepend to the array.
- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

### `array_remove`

Removes all elements equal to the given value from the array.

```
array_remove(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.

### `array_replace`

Replaces a specified element with another specified element.

```
array_replace(array, from, to)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.

### `array_to_string`

Converts each element to its text representation.

```
array_to_string(array, delimeter)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **delimeter**: Array element separator.

### `cardinality`

Returns the total number of elements in the array.

```
cardinality(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

### `make_array`

Returns an Arrow array using the specified input expressions.

```
make_array(expression1[, ..., expression_n])
```

#### Arguments

- **expression_n**: Expression to include in the output array.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

### `trim_array`

Removes the last n elements from the array.

```
trim_array(array, n)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **n**: Element to trim the array.

## Struct Functions

- [struct](#struct)

### `struct`

Returns an Arrow struct using the specified input expressions.
Fields in the returned struct use the `cN` naming convention.
For example: `c0`, `c1`, `c2`, etc.

```
struct(expression1[, ..., expression_n])
```

For example, this query converts two columns `a` and `b` to a single column with
a struct type of fields `c0` and `c1`:

```sql
❯ select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+

❯ select struct(a, b) from t;
+-----------------+
| struct(t.a,t.b) |
+-----------------+
| {c0: 1, c1: 2}  |
| {c0: 3, c1: 4}  |
+-----------------+
```

#### Arguments

- **expression_n**: Expression to include in the output struct.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

## Hashing Functions

- [digest](#digest)
- [md5](#md5)
- [sha224](#sha224)
- [sha256](#sha256)
- [sha384](#sha384)
- [sha512](#sha512)

### `digest`

Computes the binary hash of an expression using the specified algorithm.

```
digest(expression, algorithm)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **algorithm**: String expression specifying algorithm to use.
  Must be one of:

  - md5
  - sha224
  - sha256
  - sha384
  - sha512
  - blake2s
  - blake2b
  - blake3

### `md5`

Computes an MD5 128-bit checksum for a string expression.

```
md5(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha224`

Computes the SHA-224 hash of a binary string.

```
sha224(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha256`

Computes the SHA-256 hash of a binary string.

```
sha256(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha384`

Computes the SHA-384 hash of a binary string.

```
sha384(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha512`

Computes the SHA-512 hash of a binary string.

```
sha512(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

## Other Functions

- [arrow_cast](#arrow_cast)
- [arrow_typeof](#arrow_typeof)

### `arrow_cast`

Casts a value to a specific Arrow data type:

```
arrow_cast(expression, datatype)
```

#### Arguments

- **expression**: Expression to cast.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.
- **datatype**: [Arrow data type](https://arrow.apache.org/datafusion/user-guide/sql/data_types.html)
  to cast to.

### `arrow_typeof`

Returns the underlying Arrow data type of the expression:

```
arrow_typeof(expression)
```

#### Arguments

- **expression**: Expression to evaluate.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.
