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

# Data Types

DataFusion uses Arrow, and thus the Arrow type system, for query
execution. The SQL types from
[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/ast/data_type.rs#L27)
are mapped to Arrow types according to the following table

| SQL Data Type | Arrow DataType                    |
| ------------- | --------------------------------- |
| `CHAR`        | `Utf8`                            |
| `VARCHAR`     | `Utf8`                            |
| `UUID`        | _Not yet supported_               |
| `CLOB`        | _Not yet supported_               |
| `BINARY`      | _Not yet supported_               |
| `VARBINARY`   | _Not yet supported_               |
| `DECIMAL`     | `Float64`                         |
| `FLOAT`       | `Float32`                         |
| `SMALLINT`    | `Int16`                           |
| `INT`         | `Int32`                           |
| `BIGINT`      | `Int64`                           |
| `REAL`        | `Float32`                         |
| `DOUBLE`      | `Float64`                         |
| `BOOLEAN`     | `Boolean`                         |
| `DATE`        | `Date32`                          |
| `TIME`        | `Time64(TimeUnit::Nanosecond)`   |
| `TIMESTAMP`   | `Timestamp(TimeUnit::Nanosecond)` |
| `INTERVAL`    | `Interval(YearMonth)` or `Interval(MonthDayNano)` or `Interval(DayTime)`  |
| `REGCLASS`    | _Not yet supported_               |
| `TEXT`        | `Utf8`               |
| `BYTEA`       | _Not yet supported_               |
| `CUSTOM`      | _Not yet supported_               |
| `ARRAY`       | _Not yet supported_               |
