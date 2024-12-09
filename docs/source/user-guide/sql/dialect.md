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

By default, DataFusion follows the [PostgreSQL SQL dialect]. 
For Array/List functions and semantics, it follows the [DuckDB SQL dialect].

[DuckDB SQL dialect]: https://duckdb.org/docs/sql/functions/array
[PostgreSQL SQL dialect]: https://www.postgresql.org/docs/current/sql.html


## Rationale

SQL Engines have a choice to either use an existing SQL dialect or define their
own. Using an existing dialect may not fit perfectly as it is hard to match
semantics exactly (need bug-for-bug compatibility), and is likely not what all
users want. However, it avoids the (very significant) effort of defining
semantics as well as documenting and teaching users about them.

As DataFusion is highly customizable, systems built on DataFusion can and do
update functions and SQL syntax to model other systems, such as Spark or
MySQL.