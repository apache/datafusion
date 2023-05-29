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

# Expression API

DataFrame methods such as `select` and `filter` accept one or more logical expressions and there are many functions
available for creating logical expressions. These are documented below.

Expressions can be chained together using a fluent-style API:

```rust
// create the expression `(a > 6) AND (b < 7)`
col("a").gt(lit(6)).and(col("b").lt(lit(7)))
```

## Identifiers

| Function | Notes                                        |
| -------- | -------------------------------------------- |
| col      | Reference a column in a dataframe `col("a")` |

## Literal Values

| Function | Notes                                              |
| -------- | -------------------------------------------------- |
| lit      | Literal value such as `lit(123)` or `lit("hello")` |

## Boolean Expressions

| Function | Notes                                     |
| -------- | ----------------------------------------- |
| and      | `and(expr1, expr2)` or `expr1.and(expr2)` |
| or       | `or(expr1, expr2)` or `expr1.or(expr2)`   |
| not      | `not(expr)` or `expr.not()`               |

## Bitwise expressions

| Function            | Notes                                                                     |
| ------------------- | ------------------------------------------------------------------------- |
| bitwise_and         | `bitwise_and(expr1, expr2)` or `expr1.bitwise_and(expr2)`                 |
| bitwise_or          | `bitwise_or(expr1, expr2)` or `expr1.bitwise_or(expr2)`                   |
| bitwise_xor         | `bitwise_xor(expr1, expr2)` or `expr1.bitwise_xor(expr2)`                 |
| bitwise_shift_right | `bitwise_shift_right(expr1, expr2)` or `expr1.bitwise_shift_right(expr2)` |
| bitwise_shift_left  | `bitwise_shift_left(expr1, expr2)` or `expr1.bitwise_shift_left(expr2)`   |

## Comparison Expressions

| Function | Notes                 |
| -------- | --------------------- |
| eq       | `expr1.eq(expr2)`     |
| gt       | `expr1.gt(expr2)`     |
| gt_eq    | `expr1.gt_eq(expr2)`  |
| lt       | `expr1.lt(expr2)`     |
| lt_eq    | `expr1.lt_eq(expr2)`  |
| not_eq   | `expr1.not_eq(expr2)` |

## Math Functions

In addition to the math functions listed here, some Rust operators are implemented for expressions, allowing
expressions such as `col("a") + col("b")` to be used.

| Function              | Notes                                             |
| --------------------- | ------------------------------------------------- |
| abs(x)                | absolute value                                    |
| acos(x)               | inverse cosine                                    |
| acosh(x)              | inverse hyperbolic cosine                         |
| asin(x)               | inverse sine                                      |
| asinh(x)              | inverse hyperbolic sine                           |
| atan(x)               | inverse tangent                                   |
| atanh(x)              | inverse hyperbolic tangent                        |
| atan2(y, x)           | inverse tangent of y / x                          |
| cbrt(x)               | cube root                                         |
| ceil(x)               | nearest integer greater than or equal to argument |
| cos(x)                | cosine                                            |
| cosh(x)               | hyperbolic cosine                                 |
| degrees(x)            | converts radians to degrees                       |
| exp(x)                | exponential                                       |
| factorial(x)          | factorial                                         |
| floor(x)              | nearest integer less than or equal to argument    |
| gcd(x, y)             | greatest common divisor                           |
| lcm(x, y)             | least common multiple                             |
| ln(x)                 | natural logarithm                                 |
| log(base, x)          | logarithm of x for a particular base              |
| log10(x)              | base 10 logarithm                                 |
| log2(x)               | base 2 logarithm                                  |
| pi()                  | approximate value of π                            |
| power(base, exponent) | base raised to the power of exponent              |
| radians(x)            | converts degrees to radians                       |
| round(x)              | round to nearest integer                          |
| signum(x)             | sign of the argument (-1, 0, +1)                  |
| sin(x)                | sine                                              |
| sinh(x)               | hyperbolic sine                                   |
| sqrt(x)               | square root                                       |
| tan(x)                | tangent                                           |
| tanh(x)               | hyperbolic tangent                                |
| trunc(x)              | truncate toward zero                              |

### Math functions usage notes:

Unlike to some databases the math functions in Datafusion works the same way as Rust math functions, avoiding failing on corner cases e.g

```
❯ select log(-1), log(0), sqrt(-1);
+----------------+---------------+-----------------+
| log(Int64(-1)) | log(Int64(0)) | sqrt(Int64(-1)) |
+----------------+---------------+-----------------+
| NaN            | -inf          | NaN             |
+----------------+---------------+-----------------+
```

## Bitwise Operators

| Operator | Notes                                           |
| -------- | ----------------------------------------------- |
| &        | Bitwise AND => `(expr1 & expr2)`                |
| &#124;   | Bitwise OR => <code>(expr1 &#124; expr2)</code> |
| #        | Bitwise XOR => `(expr1 # expr2)`                |
| <<       | Bitwise left shift => `(expr1 << expr2)`        |
| >>       | Bitwise right shift => `(expr1 << expr2)`       |

## Conditional Expressions

| Function | Notes                                                                                                                                                                                                    |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| coalesce | Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display. |
| case     | CASE expression. Example: `case(expr).when(expr, expr).when(expr, expr).otherwise(expr).end()`.                                                                                                          |
| nullif   | Returns a null value if `value1` equals `value2`; otherwise it returns `value1`. This can be used to perform the inverse operation of the `coalesce` expression.                                         |

## String Expressions

| Function                                       | Notes                                                                                                                                                                                                                                    |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ascii(character)                               | Returns a numeric representation of the character (`character`). Example: `ascii('a') -> 97`                                                                                                                                             |
| bit_length(text)                               | Returns the length of the string (`text`) in bits. Example: `bit_length('spider') -> 48`                                                                                                                                                 |
| btrim(text, characters)                        | Removes all specified characters (`characters`) from both the beginning and the end of the string (`text`). Example: `btrim('aabchelloccb', 'abc') -> hello`                                                                             |
| char_length(text)                              | Returns number of characters in the string (`text`). The same as `character_length` and `length`. Example: `character_length('lion') -> 4`                                                                                               |
| character_length(text)                         | Returns number of characters in the string (`text`). The same as `char_length` and `length`. Example: `char_length('lion') -> 4`                                                                                                         |
| concat(value1, [value2 [, ...]])               | Concatenates the text representations (`value1, [value2 [, ...]]`) of all the arguments. NULL arguments are ignored. Example: `concat('aaa', 'bbc', NULL, 321) -> aaabbc321`                                                             |
| concat_ws(separator, value1, [value2 [, ...]]) | Concatenates the text representations (`value1, [value2 [, ...]]`) of all the arguments with the separator (`separator`). NULL arguments are ignored. `concat_ws('/', 'path', 'to', NULL, 'my', 'folder', 123) -> path/to/my/folder/123` |
| chr(integer)                                   | Returns a character by its numeric representation (`integer`). Example: `chr(90) -> 8`                                                                                                                                                   |
| initcap                                        | Converts the first letter of each word to upper case and the rest to lower case. Example: `initcap('hi TOM') -> Hi Tom`                                                                                                                  |
| left(text, number)                             | Returns a certain number (`number`) of first characters (`text`). Example: `left('like', 2) -> li`                                                                                                                                       |
| length(text)                                   | Returns number of characters in the string (`text`). The same as `character_length` and `char_length`. Example: `length('lion') -> 4`                                                                                                    |
| lower(text)                                    | Converts all characters in the string (`text`) into lower case. Example: `lower('HELLO') -> hello`                                                                                                                                       |
| lpad(text, length, [, fill])                   | Extends the string to length (`length`) by prepending the characters (`fill`) (a space by default). Example: `lpad('bb', 5, 'a') → aaabb`                                                                                                |
| ltrim(text, text)                              | Removes all specified characters (`characters`) from the beginning of the string (`text`). Example: `ltrim('aabchelloccb', 'abc') -> helloccb`                                                                                           |
| md5(text)                                      | Computes the MD5 hash of the argument (`text`).                                                                                                                                                                                          |
| octet_length(text)                             | Returns number of bytes in the string (`text`).                                                                                                                                                                                          |
| repeat(text, number)                           | Repeats the string the specified number of times. Example: `repeat('1', 4) -> 1111`                                                                                                                                                      |
| replace(string, from, to)                      | Replaces a specified string (`from`) with another specified string (`to`) in the string (`string`). Example: `replace('Hello', 'replace', 'el') -> Hola`                                                                                 |
| reverse(text)                                  | Reverses the order of the characters in the string (`text`). Example: `reverse('hello') -> olleh`                                                                                                                                        |
| right(text, number)                            | Returns a certain number (`number`) of last characters (`text`). Example: `right('like', 2) -> ke`                                                                                                                                       |
| rpad(text, length, [, fill])                   | Extends the string to length (`length`) by prepending the characters (`fill`) (a space by default). Example: `rpad('bb', 5, 'a') → bbaaa`                                                                                                |
| rtrim                                          | Removes all specified characters (`characters`) from the end of the string (`text`). Example: `rtrim('aabchelloccb', 'abc') -> aabchello`                                                                                                |
| digest(input, algorithm)                       | Computes the binary hash of `input`, using the `algorithm`.                                                                                                                                                                              |
| split_part(string, delimiter, index)           | Splits the string (`string`) based on a delimiter (`delimiter`) and picks out the desired field based on the index (`index`).                                                                                                            |
| starts_with(string, prefix)                    | Returns `true` if the string (`string`) starts with the specified prefix (`prefix`). If not, it returns `false`. Example: `starts_with('Hi Tom', 'Hi') -> true`                                                                          |
| strpos                                         | Finds the position from where the `substring` matches the `string`                                                                                                                                                                       |
| substr(string, position, [, length])           | Returns substring from the position (`position`) with length (`length`) characters in the string (`string`).                                                                                                                             |
| translate(string, from, to)                    | Replaces the characters in `from` with the counterpart in `to`. Example: `translate('abcde', 'acd', '15') -> 1b5e`                                                                                                                       |
| trim(string)                                   | Removes all characters, space by default from the string (`string`)                                                                                                                                                                      |
| upper                                          | Converts all characters in the string into upper case. Example: `upper('hello') -> HELLO`                                                                                                                                                |

## Array Expressions

| Function                             | Notes                                                           |
| ------------------------------------ | --------------------------------------------------------------- |
| array_append(array, element)         | Appends an element to the end of an array.                      |
| array_concat(array[, ..., array_n])  | Concatenates arrays.                                            |
| array_dims(array)                    | Returns an array of the array's dimensions.                     |
| array_fill(element, array)           | Returns an array filled with copies of the given value.         |
| array_length(array, dimension)       | Returns the length of the array dimension.                      |
| array_ndims(array)                   | Returns the number of dimensions of the array.                  |
| array_position(array, element)       | Searches for an element in the array, returns first occurrence. |
| array_positions(array, element)      | Searches for an element in the array, returns all occurrences.  |
| array_prepend(array, element)        | Prepends an element to the beginning of an array.               |
| array_remove(array, element)         | Removes all elements equal to the given value from the array.   |
| array_replace(array, from, to)       | Replaces a specified element with another specified element.    |
| array_to_string(array, delimeter)    | Converts each element to its text representation.               |
| cardinality(array)                   | Returns the total number of elements in the array.              |
| make_array(value1, [value2 [, ...]]) | Returns an Arrow array using the specified input expressions.   |
| trim_array(array, n)                 | Removes the last n elements from the array.                     |

## Regular Expressions

| Function       | Notes                                                                         |
| -------------- | ----------------------------------------------------------------------------- |
| regexp_match   | Matches a regular expression against a string and returns matched substrings. |
| regexp_replace | Replaces strings that match a regular expression                              |

## Temporal Expressions

| Function             | Notes                                                  |
| -------------------- | ------------------------------------------------------ |
| date_part            | Extracts a subfield from the date.                     |
| date_trunc           | Truncates the date to a specified level of precision.  |
| from_unixtime        | Returns the unix time in format.                       |
| to_timestamp         | Converts a string to a `Timestamp(_, _)`               |
| to_timestamp_millis  | Converts a string to a `Timestamp(Milliseconds, None)` |
| to_timestamp_micros  | Converts a string to a `Timestamp(Microseconds, None)` |
| to_timestamp_seconds | Converts a string to a `Timestamp(Seconds, None)`      |
| now()                | Returns current time.                                  |

## Other Expressions

| Function                     | Notes                                                                                                      |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| array([value1, ...])         | Returns an array of fixed size with each argument (`[value1, ...]`) on it.                                 |
| in_list(expr, list, negated) | Returns `true` if (`expr`) belongs or not belongs (`negated`) to a list (`list`), otherwise returns false. |
| random()                     | Returns a random value from 0 (inclusive) to 1 (exclusive).                                                |
| sha224(text)                 | Computes the SHA224 hash of the argument (`text`).                                                         |
| sha256(text)                 | Computes the SHA256 hash of the argument (`text`).                                                         |
| sha384(text)                 | Computes the SHA384 hash of the argument (`text`).                                                         |
| sha512(text)                 | Computes the SHA512 hash of the argument (`text`).                                                         |
| to_hex(integer)              | Converts the integer (`integer`) to the corresponding hexadecimal string.                                  |

## Aggregate Functions

| Function                                                          | Notes                                                                                   |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| avg(expr)                                                         | Сalculates the average value for `expr`.                                                |
| approx_distinct(expr)                                             | Calculates an approximate count of the number of distinct values for `expr`.            |
| approx_median(expr)                                               | Calculates an approximation of the median for `expr`.                                   |
| approx_percentile_cont(expr, percentile)                          | Calculates an approximation of the specified `percentile` for `expr`.                   |
| approx_percentile_cont_with_weight(expr, weight_expr, percentile) | Calculates an approximation of the specified `percentile` for `expr` and `weight_expr`. |
| count(expr)                                                       | Returns the number of rows for `expr`.                                                  |
| count_distinct                                                    | Creates an expression to represent the count(distinct) aggregate function               |
| cube(exprs)                                                       | Creates a grouping set for all combination of `exprs`                                   |
| grouping_set(exprs)                                               | Create a grouping set.                                                                  |
| max(expr)                                                         | Finds the maximum value of `expr`.                                                      |
| median(expr)                                                      | Сalculates the median of `expr`.                                                        |
| min(expr)                                                         | Finds the minimum value of `expr`.                                                      |
| rollup(exprs)                                                     | Creates a grouping set for rollup sets.                                                 |
| sum(expr)                                                         | Сalculates the sum of `expr`.                                                           |

## Subquery Expressions

| Function        | Notes                                                                                         |
| --------------- | --------------------------------------------------------------------------------------------- |
| exists          | Creates an `EXISTS` subquery expression                                                       |
| in_subquery     | `df1.filter(in_subquery(col("foo"), df2))?` is the equivalent of the SQL `WHERE foo IN <df2>` |
| not_exists      | Creates a `NOT EXISTS` subquery expression                                                    |
| not_in_subquery | Creates a `NOT IN` subquery expression                                                        |
| scalar_subquery | Creates a scalar subquery expression                                                          |

## User-Defined Function Expressions

| Function    | Notes                                                                     |
| ----------- | ------------------------------------------------------------------------- |
| create_udf  | Creates a new UDF with a specific signature and specific return type.     |
| create_udaf | Creates a new UDAF with a specific signature, state type and return type. |
