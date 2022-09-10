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

# Expressions

DataFrame methods such as `select` and `filter` accept one or more logical expressions and there are many functions
available for creating logical expressions. These are documented below.

Expressions can be chained together using a fluent-style API:

```rust
// create the expression `(a > 5) AND (b < 7)`
col("a").gt(lit(5)).and(col("b").lt(lit(7)))
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
| asin(x)               | inverse sine                                      |
| atan(x)               | inverse tangent                                   |
| atan2(y, x)           | inverse tangent of y / x                          |
| ceil(x)               | nearest integer greater than or equal to argument |
| cos(x)                | cosine                                            |
| exp(x)                | exponential                                       |
| floor(x)              | nearest integer less than or equal to argument    |
| ln(x)                 | natural logarithm                                 |
| log10(x)              | base 10 logarithm                                 |
| log2(x)               | base 2 logarithm                                  |
| power(base, exponent) | base raised to the power of exponent              |
| round(x)              | round to nearest integer                          |
| signum(x)             | sign of the argument (-1, 0, +1)                  |
| sin(x)                | sine                                              |
| sqrt(x)               | square root                                       |
| tan(x)                | tangent                                           |
| trunc(x)              | truncate toward zero                              |

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

| Function         | Notes |
| ---------------- | ----- |
| ascii            |       |
| bit_length       |       |
| btrim            |       |
| char_length      |       |
| character_length |       |
| concat           |       |
| concat_ws        |       |
| chr              |       |
| initcap          |       |
| left             |       |
| length           |       |
| lower            |       |
| lpad             |       |
| ltrim            |       |
| md5              |       |
| octet_length     |       |
| repeat           |       |
| replace          |       |
| reverse          |       |
| right            |       |
| rpad             |       |
| rtrim            |       |
| digest           |       |
| split_part       |       |
| starts_with      |       |
| strpos           |       |
| substr           |       |
| translate        |       |
| trim             |       |
| upper            |       |

## Regular Expressions

| Function       | Notes |
| -------------- | ----- |
| regexp_match   |       |
| regexp_replace |       |

## Temporal Expressions

| Function             | Notes        |
| -------------------- | ------------ |
| date_part            |              |
| date_trunc           |              |
| from_unixtime        |              |
| to_timestamp         |              |
| to_timestamp_millis  |              |
| to_timestamp_micros  |              |
| to_timestamp_seconds |              |
| now()                | current time |

## Other Expressions

| Function | Notes |
| -------- | ----- |
| array    |       |
| in_list  |       |
| random   |       |
| sha224   |       |
| sha256   |       |
| sha384   |       |
| sha512   |       |
| struct   |       |
| to_hex   |       |

## Aggregate Functions

| Function                           | Notes |
| ---------------------------------- | ----- |
| avg                                |       |
| approx_distinct                    |       |
| approx_median                      |       |
| approx_percentile_cont             |       |
| approx_percentile_cont_with_weight |       |
| count                              |       |
| count_distinct                     |       |
| cube                               |       |
| grouping_set                       |       |
| max                                |       |
| median                             |       |
| min                                |       |
| rollup                             |       |
| sum                                |       |

## Subquery Expressions

| Function        | Notes                                                                                         |
| --------------- | --------------------------------------------------------------------------------------------- |
| exists          |                                                                                               |
| in_subquery     | `df1.filter(in_subquery(col("foo"), df2))?` is the equivalent of the SQL `WHERE foo IN <df2>` |
| not_exists      |                                                                                               |
| not_in_subquery |                                                                                               |
| scalar_subquery |                                                                                               |

## User-Defined Function Expressions

| Function    | Notes |
| ----------- | ----- |
| create_udf  |       |
| create_udaf |       |
