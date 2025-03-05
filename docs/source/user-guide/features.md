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

# Features

## General

- [x] SQL Parser
- [x] SQL Query Planner
- [x] DataFrame API
- [x] Parallel query execution
- [x] Streaming Execution

## Optimizations

- [x] Query Optimizer
- [x] Constant folding
- [x] Join Reordering
- [x] Limit Pushdown
- [x] Projection push down
- [x] Predicate push down

## SQL Support

- [x] Type coercion
- [x] Projection (`SELECT`)
- [x] Filter (`WHERE`)
- [x] Filter post-aggregate (`HAVING`)
- [x] Sorting (`ORDER BY`)
- [x] Limit (`LIMIT`
- [x] Aggregate (`GROUP BY`)
- [x] cast /try_cast
- [x] [`VALUES` lists](https://www.postgresql.org/docs/current/queries-values.html)
- [x] [String Functions](./sql/scalar_functions.md#string-functions)
- [x] [Conditional Functions](./sql/scalar_functions.md#conditional-functions)
- [x] [Time and Date Functions](./sql/scalar_functions.md#time-and-date-functions)
- [x] [Math Functions](./sql/scalar_functions.md#math-functions)
- [x] [Aggregate Functions](./sql/aggregate_functions.md) (`SUM`, `MEDIAN`, and many more)
- [x] Schema Queries
  - [x] `SHOW TABLES`
  - [x] `SHOW COLUMNS FROM <table/view>`
  - [x] `SHOW CREATE TABLE <view>`
  - [x] Basic SQL [Information Schema](./sql/information_schema.md) (`TABLES`, `VIEWS`, `COLUMNS`)
  - [ ] Full SQL [Information Schema](./sql/information_schema.md) support
- [x] Support for nested types (`ARRAY`/`LIST` and `STRUCT`.
  - [x] Read support
  - [x] Write support
  - [x] Field access (`col['field']` and [`col[1]`])
  - [x] [Array Functions](./sql/scalar_functions.md#array-functions)
  - [x] [Struct Functions](./sql/scalar_functions.md#struct-functions)
    - [x] `struct`
    - [ ] [Postgres JSON operators](https://github.com/apache/datafusion/issues/6631) (`->`, `->>`, etc.)
- [x] Subqueries
- [x] Common Table Expressions (CTE)
- [x] Set Operations (`UNION [ALL]`, `INTERSECT [ALL]`, `EXCEPT[ALL]`)
- [x] Joins (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`)
- [x] Window Functions
  - [x] Empty (`OVER()`)
  - [x] Partitioning and ordering: (`OVER(PARTITION BY <..> ORDER BY <..>)`)
  - [x] Custom Window (`ORDER BY time ROWS BETWEEN 2 PRECEDING AND 0 FOLLOWING)`)
  - [x] User Defined Window and Aggregate Functions
- [x] Catalogs
  - [x] Schemas (`CREATE / DROP SCHEMA`)
  - [x] Tables (`CREATE / DROP TABLE`, `CREATE TABLE AS SELECT`)
- [x] Data Insert
  - [x] `INSERT INTO`
  - [x] `COPY .. INTO ..`
  - [x] CSV
  - [x] JSON
  - [x] Parquet
  - [ ] Avro

## Runtime

- [x] Streaming Grouping
- [x] Streaming Window Evaluation
- [x] Memory limits enforced
- [x] Spilling (to disk) Sort
- [x] Spilling (to disk) Grouping
- [ ] Spilling (to disk) Joins

## Data Sources

In addition to allowing arbitrary datasources via the [`TableProvider`]
trait, DataFusion includes built in support for the following formats:

- [x] CSV
- [x] Parquet
  - [x] Primitive and Nested Types
  - [x] Row Group and Data Page pruning on min/max statistics
  - [x] Row Group pruning on Bloom Filters
  - [x] Predicate push down (late materialization) [not by default](https://github.com/apache/datafusion/issues/3463)
- [x] JSON
- [x] Avro
- [x] Arrow

[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
