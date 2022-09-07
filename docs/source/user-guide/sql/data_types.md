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
are mapped to [Arrow data types](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) according to the following table.
This mapping occurs when defining the schema in a `CREATE EXTERNAL TABLE` command or when performing a SQL `CAST` operation.

## Character Types

| SQL DataType | Arrow DataType |
| ------------ | -------------- |
| `CHAR`       | `Utf8`         |
| `VARCHAR`    | `Utf8`         |
| `TEXT`       | `Utf8`         |

## Numeric Types

| SQL DataType                         | Arrow DataType    |
| ------------------------------------ | :---------------- |
| `TINYINT`                            | `Int8`            |
| `SMALLINT`                           | `Int16`           |
| `INT` or `INTEGER`                   | `Int32`           |
| `BIGINT`                             | `Int64`           |
| `TINYINT UNSIGNED`                   | `UInt8`           |
| `SMALLINT UNSIGNED`                  | `UInt16`          |
| `INT UNSIGNED` or `INTEGER UNSIGNED` | `UInt32`          |
| `BIGINT UNSIGNED`                    | `UInt64`          |
| `FLOAT`                              | `Float32`         |
| `REAL`                               | `Float32`         |
| `DOUBLE`                             | `Float64`         |
| `DECIMAL(p,s)`                       | `Decimal128(p,s)` |

## Date/Time Types

| SQL DataType | Arrow DataType                          |
| ------------ | :-------------------------------------- |
| `DATE`       | `Date32`                                |
| `TIME`       | `Time64(TimeUnit::Nanosecond)`          |
| `TIMESTAMP`  | `Timestamp(TimeUnit::Nanosecond, None)` |

## Boolean Types

| SQL DataType | Arrow DataType |
| ------------ | :------------- |
| `BOOLEAN`    | `Boolean`      |

## Binary Types

| SQL DataType | Arrow DataType |
| ------------ | :------------- |
| `BYTEA`      | `Binary`       |

## Unsupported Types

| SQL Data Type | Arrow DataType      |
| ------------- | :------------------ |
| `UUID`        | _Not yet supported_ |
| `BLOB`        | _Not yet supported_ |
| `CLOB`        | _Not yet supported_ |
| `BINARY`      | _Not yet supported_ |
| `VARBINARY`   | _Not yet supported_ |
| `REGCLASS`    | _Not yet supported_ |
| `NVARCHAR`    | _Not yet supported_ |
| `STRING`      | _Not yet supported_ |
| `CUSTOM`      | _Not yet supported_ |
| `ARRAY`       | _Not yet supported_ |
| `ENUM`        | _Not yet supported_ |
| `SET`         | _Not yet supported_ |
| `INTERVAL`    | _Not yet supported_ |
| `DATETIME`    | _Not yet supported_ |
