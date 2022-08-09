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

A DataFrame represents a logical set of rows with the same named columns, similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
[Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html).

DataFrames are typically created by calling a method on
`SessionContext`, such as `read_csv`, and can then be modified
by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
to build up a query definition.

The query can be executed by calling the `collect` method.

The API is well documented in the [API reference on docs.rs](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html)

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
// Print results
df.show();
```

## DataFrame Transformations

These methods create a new DataFrame after applying a transformation to the logical plan that the DataFrame represents.

| Function            | Notes                                                                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| aggregate           | Perform an aggregate query with optional grouping expressions.                                                                             |
| distinct            | Filter out duplicate rows.                                                                                                                 |
| except              | Calculate the exception of two DataFrames. The two DataFrames must have exactly the same schema                                            |
| filter              | Filter a DataFrame to only include rows that match the specified filter expression.                                                        |
| intersect           | Calculate the intersection of two DataFrames. The two DataFrames must have exactly the same schema                                         |
| join                | Join this DataFrame with another DataFrame using the specified columns as join keys.                                                       |
| limit               | Limit the number of rows returned from this DataFrame.                                                                                     |
| repartition         | Repartition a DataFrame based on a logical partitioning scheme.                                                                            |
| sort                | Sort the DataFrame by the specified sorting expressions. Any expression can be turned into a sort expression by calling its `sort` method. |
| select              | Create a projection based on arbitrary expressions. Example: `df..select(vec![col("c1"), abs(col("c2"))])?`                                |
| select_columns      | Create a projection based on column names. Example: `df.select_columns(&["id", "name"])?`.                                                 |
| union               | Calculate the union of two DataFrames, preserving duplicate rows. The two DataFrames must have exactly the same schema.                    |
| union_distinct      | Calculate the distinct union of two DataFrames. The two DataFrames must have exactly the same schema.                                      |
| with_column         | Add an additional column to the DataFrame.                                                                                                 |
| with_column_renamed | Rename one column by applying a new projection.                                                                                            |

## DataFrame Actions

These methods execute the logical plan represented by the DataFrame and either collects the results into memory, prints them to stdout, or writes them to disk.

| Function                   | Notes                                                                                                                       |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| collect                    | Executes this DataFrame and collects all results into a vector of RecordBatch.                                              |
| collect_partitioned        | Executes this DataFrame and collects all results into a vector of vector of RecordBatch maintaining the input partitioning. |
| execute_stream             | Executes this DataFrame and returns a stream over a single partition.                                                       |
| execute_stream_partitioned | Executes this DataFrame and returns one stream per partition.                                                               |
| show                       | Execute this DataFrame and print the results to stdout.                                                                     |
| show_limit                 | Execute this DataFrame and print a subset of results to stdout.                                                             |
| write_csv                  | Execute this DataFrame and write the results to disk in CSV format.                                                         |
| write_json                 | Execute this DataFrame and write the results to disk in JSON format.                                                        |
| write_parquet              | Execute this DataFrame and write the results to disk in Parquet format.                                                     |

## Other DataFrame Methods

| Function        | Notes                                                                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| explain         | Return a DataFrame with the explanation of its plan so far.                                                                                                  |
| registry        | Return a `FunctionRegistry` used to plan udf's calls.                                                                                                        |
| schema          | Returns the schema describing the output of this DataFrame in terms of columns returned, where each column has a name, data type, and nullability attribute. |
| to_logical_plan | Return the logical plan represented by this DataFrame.                                                                                                       |

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

## Conditional Expressions

| Function | Notes                                                                                                                                                                                                    |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| coalesce | Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display. |
| case     | CASE expression. Example: `case(expr).when(expr, expr).when(expr, expr).otherwise(expr).end()`.                                                                                                          |
| nullif   | Returns a null value if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the `coalesce` expression.                                               |

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
