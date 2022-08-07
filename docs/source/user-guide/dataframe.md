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

# DataFrame API

DataFrame represents a logical set of rows with the same named columns.
Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
[Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)

DataFrames are typically created by calling a method on
[SessionContext](../execution/context/struct.SessionContext.html) such as `read_csv` and can then be modified
by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
to build up a query definition.

The query can be executed by calling the `collect` method.

The API is well documented at https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html

The DataFrame struct is part of DataFusion's prelude and can be imported with the following statement.

```rust
use datafusion::prelude::*;
```

Here is a minimal example showing the execution of a query using the DataFrame API.

```rust
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
let df = df.filter(col("a").lt_eq(col("b")))?
           .aggregate(vec![col("a")], vec![min(col("b"))])?
           .limit(None, Some(100))?;
let results = df.collect();
```

DataFrame methods such as `select` and `filter` accept one or more logical expressions and there are many functions
available for creating logical expressions. These are documented below.

# Expressions

## Identifiers

| Function | Comment                                      |
| -------- | -------------------------------------------- |
| col      | Reference a column in a dataframe `col("a")` |

## Literal Values

| Function | Comment                                            |
| -------- | -------------------------------------------------- |
| col      | Reference a column in a dataframe `col("a")`       |
| lit      | Literal value such as `lit(123)` or `lit("hello")` |

## Boolean Expressions

| Function | Comment                                   |
| -------- | ----------------------------------------- |
| and      | `and(expr1, expr2)` or `expr1.and(expr2)` |
| or       | `or(expr1, expr2)` or `expr1.or(expr2)`   |
| not      | `not(expr)` or `expr.not()`               |

## Comparison Expressions

| Function | Comment               |
| -------- | --------------------- |
| eq       | `expr1.eq(expr2)`     |
| gt       | `expr1.gt(expr2)`     |
| gt_eq    | `expr1.gt_eq(expr2)`  |
| lt       | `expr1.lt(expr2)`     |
| lt_eq    | `expr1.lt_eq(expr2)`  |
| not_eq   | `expr1.not_eq(expr2)` |

## Math Functions

| Function | Comment |
| -------- | ------- |
| abs      |         |
| acos     |         |
| asin     |         |
| atan     |         |
| atan2    |         |
| ceil     |         |
| exp      |         |
| floor    |         |
| ln       |         |
| log      |         |
| log10    |         |
| log2     |         |
| power    |         |
| round    |         |
| signum   |         |
| sin      |         |
| sqrt     |         |
| tan      |         |
| trunc    |         |

## Conditional Expressions

| Function | Comment |
| -------- | ------- |
| coalesce |         |
| case     |         |
| nullif   |         |
| when     |         |

## String Expressions

| Function         | Comment |
| ---------------- | ------- |
| ascii            |         |
| bit_length       |         |
| btrim            |         |
| char_length      |         |
| character_length |         |
| concat           |         |
| concat_ws        |         |
| chr              |         |
| initcap          |         |
| left             |         |
| length           |         |
| lower            |         |
| lpad             |         |
| ltrim            |         |
| md5              |         |
| octet_length     |         |
| repeat           |         |
| replace          |         |
| reverse          |         |
| right            |         |
| rpad             |         |
| rtrim            |         |
| digest           |         |
| split_part       |         |
| starts_with      |         |
| strpos           |         |
| substr           |         |
| translate        |         |
| trim             |         |
| upper            |         |

# Regular Expressions

| Function       | Comment |
| -------------- | ------- |
| regexp_match   |         |
| regexp_replace |         |

## Temporal Expressions

| Function             | Comment |
| -------------------- | ------- |
| date_part            |         |
| date_trunc           |         |
| to_timestamp         |         |
| to_timestamp_millis  |         |
| to_timestamp_micros  |         |
| to_timestamp_seconds |         |
| now                  |         |
| from_unixtime        |         |

## Misc Expressions

| Function | Comment |
| -------- | ------- |
| array    |         |
| in_list  |         |
| random   |         |
| sha224   |         |
| sha256   |         |
| sha384   |         |
| sha512   |         |
| struct   |         |
| to_hex   |         |

## Aggregate Functions

| Function                           | Comment |
| ---------------------------------- | ------- |
| approx_distinct                    |         |
| approx_percentile_cont             |         |
| approx_percentile_cont_with_weight |         |
| approx_median                      |         |
| median                             |         |
| avg                                |         |
| count                              |         |
| approx_distinct                    |         |
| count_distinct                     |         |
| cube                               |         |
| grouping_set                       |         |
| max                                |         |
| median                             |         |
| min                                |         |
| rollup                             |         |
| sum                                |         |

## Subquery Expressions

| Function        | Comment |
| --------------- | ------- |
| exists          |         |
| not_exists      |         |
| in_subquery     |         |
| not_in_subquery |         |
| scalar_subquery |         |

## User-Defined Function Expressions

| Function    | Comment |
| ----------- | ------- |
| create_udf  |         |
| create_udaf |         |
