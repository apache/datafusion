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
- [asin](#asin)
- [atan](#atan)
- [atan2](#atan2)
- [ceil](#ceil)
- [cos](#cos)
- [exp](#exp)
- [floor](#floor)
- [ln](#ln)
- [log10](#log10)
- [log2](#log2)
- [power](#power)
- [random](#random)
- [round](#round)
- [signum](#signum)
- [sin](#sin)
- [sqrt](#sqrt)
- [tan](#tan)
- [trunc](#trunc)

### `abs`

Returns the absolute value of a number.

```
abs(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `acos`

Returns the arc cosine or inverse cosine of a number.

```
acos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `asin`

Returns the arc sine or inverse sine of a number.

```
asin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `atan`

Returns the arc tangent or inverse tangent of a number.

```
atan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `atan2`

Returns the arc tangent or inverse tangent of `expression_y / expression_x`.

```
atan2(expression_y / expression_x)
```

#### Arguments

- **expression_y**: First numeric column or literal value to operate on.
- **expression_x**: Second numeric column or literal value to operate on.

### `ceil`

Returns the nearest integer greater than or equal to a number.

```
ceil(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `cos`

Returns the cosine of a number.

```
cos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `exp`

Returns the base-e exponential of a number.

```
exp(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to use as the exponent.

### `floor`

Returns the nearest integer less than or equal to a number.

```
floor(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `ln`

Returns the natural logarithm of a number.

```
ln(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `log10`

Returns the base-10 logarithm of a number.

```
log10(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `log2`

Returns the base-2 logarithm or a number.

```
log2(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `power`

Returns a base number raised to the power of an exponent.

```
power(base, exponent)
```

#### Arguments

- **power**: Base numeric column or literal value to operate on.
- **exponent**: Exponent numeric column or literal value to operate on.

### `random`

Returns a random float value between 0 and 1.
The random seed is unique to each row.

```
random()
```

### `round`

Rounds a number to the nearest integer.

```
round(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `signum`

Returns the sign of a number.
Negative numbers return `-1`.
Zero and positive numbers return `1`.

```
signum(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `sin`

Returns the sine of a number.

```
sin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `sqrt`

Returns the square root of a number.

```
sqrt(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `tan`

Returns the tangent of a number.

```
tan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

### `trunc`

Truncates a number toward zero (at the decimal point).

```
trunc(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric column or literal value to operate on.

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
  Column or literal value to use if previous expressions are _null_.
  Pass as many expression arguments as necessary.

### `nullif`

Returns _null_ if _expression1_ equals _expression2_; otherwise it returns _expression1_.
This can be used to perform the inverse operation of [`coalesce`](#coalesce).

```
nullif(expression1, expression2)
```

#### Arguments

- **expression1**: Column or literal value to compare and return if equal to expression2.
- **expression2**: Column or literal value to compare to expression1.

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

### `ascii`

Returns the ASCII value of the first character in a string.

```
ascii(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

**Related functions**:
[chr](#chr)

### `bit_length`

Returns the bit length of a string.

```
bit_length(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

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

- **str**: String column or literal string to operate on.
- **trim_str**: String column or literal string to trim from the beginning and
  end of the input string. _Default is whitespace characters_.

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

- **str**: String column or literal string to concatenate.
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
- **str**: String column or literal string to concatenate.
- **str_n**: Subsequent string column or literal string to concatenate.

**Related functions**:
[concat](#concat)

### `chr`

Returns the character with the specified ASCII code value.

```
chr(acsii)
```

#### Arguments

- **ascii**: ASCII code value to operate on.

**Related functions**:
[ascii](#ascii)

### `initcap`

Capitalizes the first character in each word in the input string.
Words are delimited by non-alphanumeric characters.

```
initcap(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

**Related functions**:
[lower](#lower),
[upper](#upper)

### `left`

Returns a specified number of characters from the left side of a string.

```
left(str, n)
```

#### Arguments

- **str**: String column or literal string to operate on.
- **n**: Number of characters to return.

**Related functions**:
[right](#right)

### `length`

Returns the number of characters in a string.

```
length(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

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

- **str**: String column or literal string to operate on.

**Related functions**:
[initcap](#initcap),
[upper](#upper)

### `lpad`

Pads the left side a string with another string to a specified string length.

```
lpad(str, n[, padding_str])
```

#### Arguments

- **str**: String column or literal string to operate on.
- **n**: String length to pad to.
- **padding_str**: String column or literal string to pad with.
  _Default is a space._

**Related functions**:
[rpad](#rpad)

### `ltrim`

Removes leading spaces from a string.

```
ltrim(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

**Related functions**:
[btrim](#btrim),
[rtrim](#rtrim),
[trim](#trim)

#### Arguments

- **str**: String column or literal string to operate on.

### `octet_length`

Returns the length of a string in bytes.

```
octet_length(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

**Related functions**:
[bit_length](#bit_length),
[length](#length)

### `repeat`

Returns a string with an input string repeated a specified number.

```
repeat(str, n)
```

#### Arguments

- **str**: String column or literal string to repeat.
- **n**: Number of times to repeat the input string.

### `replace`

Replaces all occurrences of a specified substring in a string with a new substring.

```
replace(str, substr, replacement)
```

#### Arguments

- **str**: String column or literal string to repeat.
- **substr**: Substring to replace in the input string.
- **replacement**: Replacement substring.

### `reverse`

Reverses the character order of a string.

```
reverse(str)
```

#### Arguments

- **str**: String column or literal string to repeat.

### `right`

Returns a specified number of characters from the right side of a string.

```
right(str, n)
```

#### Arguments

- **str**: String column or literal string to operate on.
- **n**: Number of characters to return.

**Related functions**:
[left](#left)

### `rpad`

right side a string with another string to a specified string length.

```
rpad(str, n[, padding_str])
```

#### Arguments

- **str**: String column or literal string to operate on.
- **n**: String length to pad to.
- **padding_str**: String column or literal string to pad with.
  _Default is a space._

**Related functions**:
[lpad](#lpad)

### `rtrim`

Removes trailing spaces from a string.

```
rtrim(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

**Related functions**:
[btrim](#btrim),
[ltrim](#ltrim),
[trim](#trim)

### `split_part`

Splits a string based on a specified delimiter and returns the substring a the
specified position.

```
split_part(str, delimiter, pos)
```

#### Arguments

- **str**: String column or literal string to spit.
- **delimiter**: String or character to split on.
- **pos**: Position of the part to return.

### `starts_with`

Tests if a string starts with a substring.

```
starts_with(str, substr)
```

#### Arguments

- **str**: String column or literal string to test.
- **substr**: Substring to test for.

### `strpos`

Returns the starting position of a specified substring in a string.
Positions begin at 1.
If the substring does not exist in the string, the function returns 0.

```
strpos(str, substr)
```

#### Arguments

- **str**: String column or literal string to operate on.
- **substr**: Substring to search for.

### `substr`

Extracts a substring of a specified number of characters from a specific
starting position in a string.

```
substr(str, start_pos[, length])
```

#### Arguments

- **str**: String column or literal string to operate on.
- **start_pos**: Character position to start the substring at.
  The first character in the string has a position of 1.
- **length**: Number of characters to extract.
  If not specified, returns the rest of the string after the start position.

### `translate`

Translates characters in a string to specified translation characters.

```
translate(str, chars, translation)
```

- **str**: String column or literal string to operate on.
- **chars**: Characters to translate.
- **translation**: Translation characters. Translation characters replace only
  characters at the same position in the **chars** string.

### `to_hex`

Converts an integer to a hexadecimal string.

```
to_hex(int)
```

#### Arguments

- **int**: Integer column or literal integer to convert.

### `trim`

Removes leading and trailing spaces from a string.

```
trim(str)
```

#### Arguments

- **str**: String column or literal string to operate on.

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

- **str**: String column or literal string to operate on.

**Related functions**:
[initcap](#initcap),
[lower](#lower)

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

- **str**: String column or literal string to operate on.
- **regexp**: Regular expression to match against.

### `regexp_replace`

Replaces substrings in a string that match a regular expression.

```
regexp_replace(str, regexp, replacement, flags)
```

#### Arguments

- **str**: String column or literal string to operate on.
- **regexp**: Regular expression to match against.
- **replacement**: Replacement string.
- **flags**: Regular expression flags that control the behavior of the
  regular expression. The following flags are supported.
  - **g**: (global) Search globally and don't return after the first match.
  - **i**: (insensitive) Ignore case when matching.

## Time and Date Functions

- [now](#now)
- [date_bin](#date_bin)
- [date_trunc](#date_trunc)
- [date_part](#date_part)
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
- **expression**: Column or timestamp literal to operate on.
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

  - year
  - month
  - week
  - day
  - hour
  - minute
  - second

- **expression**: Column or timestamp literal to operate on.

### `date_part`

Returns the specified part of the date as an integer.

```
date_part(part, expression)
```

#### Arguments

- **part**: Part of the date to return.
  The follow date parts are supported:

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

- **expression**: Column or timestamp literal to operate on.

### `extract`

Returns a sub-field from a time value as an integer.
Similar to `date_part`, but with different arguments.

```
extract(field FROM source)
```

### `to_timestamp`

Converts a value to RFC3339 nanosecond timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 nanosecond timestamp.

```
to_timestamp(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `to_timestamp_millis`

Converts a value to RFC3339 millisecond timestamp format (`YYYY-MM-DDT00:00:00.000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_millis(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `to_timestamp_micros`

Converts a value to RFC3339 microsecond timestamp format (`YYYY-MM-DDT00:00:00.000000Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_micros(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `to_timestamp_seconds`

Converts a value to RFC3339 second timestamp format (`YYYY-MM-DDT00:00:00Z`).
Supports timestamp, integer, and unsigned integer types as input.
Integers and unsigned integers are parsed as Unix nanosecond timestamps and
return the corresponding RFC3339 timestamp.

```
to_timestamp_seconds(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `from_unixtime`

Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Input is parsed as a Unix nanosecond timestamp and returns the corresponding
RFC3339 timestamp.

```
from_unixtime(expression)
```

#### Arguments

- **expression**: Column or integer literal to operate on.

## Hashing Functions

- [md5](#md5)
- [sha224](#sha224)
- [sha256](#sha256)
- [sha384](#sha384)
- [sha512](#sha512)

### `md5`

Computes an MD5 128-bit checksum for a string expression.

```
md5(expression)
```

#### Arguments

- **expression**: Column or string literal to operate on.

### `sha224`

Computes the SHA-224 hash of a binary string.

```
sha224(expression)
```

#### Arguments

- **expression**: Column or string literal to operate on.

### `sha256`

Computes the SHA-256 hash of a binary string.

```
sha256(expression)
```

#### Arguments

- **expression**: Column or string literal to operate on.

### `sha384`

Computes the SHA-384 hash of a binary string.

```
sha384(expression)
```

#### Arguments

- **expression**: Column or string literal to operate on.

### `sha512`

Computes the SHA-512 hash of a binary string.

```
sha512(expression)
```

#### Arguments

- **expression**: Column or string literal to operate on.

## Other Functions

- [array](#array)
- [arrow_typeof](#arrow_typeof)
- [struct](#struct)

### `array`

Returns an Arrow array using the specified input expressions.

```
array(expression1[, ..., expression_n])
```

#### Arguments

- **expression_n**: Column or literal value to include in the output array.

### `arrow_typeof`

Returns the underlying Arrow type of the the expression:

```
arrow_typeof(expression)
```

#### Arguments

- **expression**: Column or literal value to evaluate.

### `struct`

Returns an Arrow struct using the specified input expressions.
Fields in the returned struct use the `cN` naming convention.
For example: `c0`, `c1`, `c2`, etc.

```
struct(expression1[, ..., expression_n])
```

#### Arguments

- **expression_n**: Column or literal value to include in the output struct.
