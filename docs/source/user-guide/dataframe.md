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
           .limit(0, Some(100))?;
// Print results
df.show();
```

The DataFrame API is well documented in the [API reference on docs.rs](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html).

Refer to the [Expressions Refence](expressions) for available functions for building logical expressions for use with the
DataFrame API.

## DataFrame Transformations

These methods create a new DataFrame after applying a transformation to the logical plan that the DataFrame represents.

DataFusion DataFrames use lazy evaluation, meaning that each transformation is just creating a new query plan and
not actually performing any transformations. This approach allows for the overall plan to be optimized before
execution. The plan is evaluated (executed) when an action method is invoked, such as `collect`.

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

| Function            | Notes                                                                                                                                                        |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| explain             | Return a DataFrame with the explanation of its plan so far.                                                                                                  |
| registry            | Return a `FunctionRegistry` used to plan udf's calls.                                                                                                        |
| schema              | Returns the schema describing the output of this DataFrame in terms of columns returned, where each column has a name, data type, and nullability attribute. |
| to_logical_plan     | Return the optimized logical plan represented by this DataFrame.                                                                                             |
| to_unoptimized_plan | Return the unoptimized logical plan represented by this DataFrame.                                                                                           |
