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

# SQL Dialect

The included SQL supported in Apache DataFusion mostly follows the [PostgreSQL
SQL dialect], including:

- The sql parser and [SQL planner]
- Type checking, analyzer, and type coercions
- Semantics of functions bundled with DataFusion

Notable exceptions:

- Array/List functions and semantics follow the [DuckDB SQL dialect].
- DataFusion's type system is based on the [Apache Arrow type system], and the mapping to PostgrSQL types is not always 1:1.
- DataFusion has its own syntax (dialect) for certain operations (like [`CREATE EXTERNAL TABLE`])

As Apache DataFusion is designed to be fully customizable, systems built on
DataFusion can and do implement different SQL semantics. Using DataFusion's APs,
you can provide alternate function definitions, type rules, and/or SQL syntax
that matches other systems such as Apache Spark or MySQL or your own custom
semantics.

[postgresql sql dialect]: https://www.postgresql.org/docs/current/sql.html
[sql planner]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html
[duckdb sql dialect]: https://duckdb.org/docs/sql/functions/array
[apache arrow type system]: https://arrow.apache.org/docs/format/Columnar.html#data-types
[`create external table`]: ddl.md#create-external-table

## Rationale

SQL Engines have a choice to either use an existing SQL dialect or define their
own. Using an existing dialect may not fit perfectly as it is hard to match
semantics exactly (need bug-for-bug compatibility), and is likely not what all
users want. However, it avoids the (very significant) effort of defining
semantics as well as documenting and teaching users about them.
