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

# DataFusion Examples

This crate includes several examples of how to use various DataFusion APIs and help you on your way.

## Prerequisites:

Run `git submodule update --init` to init test files.

## Running Examples

To run the examples, use the `cargo run` command, such as:

```bash
git clone https://github.com/apache/arrow-datafusion
cd arrow-datafusion
# Download test data
git submodule update --init

# Run the `csv_sql` example:
# ... use the equivalent for other examples
cargo run --example csv_sql
```

## Single Process

- [`avro_sql.rs`](examples/avro_sql.rs): Build and run a query plan from a SQL statement against a local AVRO file
- [`csv_sql.rs`](examples/csv_sql.rs): Build and run a query plan from a SQL statement against a local CSV file
- [`custom_datasource.rs`](examples/custom_datasource.rs): Run queries against a custom datasource (TableProvider)
- [`dataframe.rs`](examples/dataframe.rs): Run a query using a DataFrame against a local parquet file
- [`dataframe_in_memory.rs`](examples/dataframe_in_memory.rs): Run a query using a DataFrame against data in memory
- [`deserialize_to_struct.rs`](examples/deserialize_to_struct.rs): Convert query results into rust structs using serde
- [`expr_api.rs`](examples/expr_api.rs): Use the `Expr` construction and simplification API
- [`flight_sql_server.rs`](examples/flight_sql_server.rs): Run DataFusion as a standalone process and execute SQL queries from JDBC clients
- [`memtable.rs`](examples/memtable.rs): Create an query data in memory using SQL and `RecordBatch`es
- [`parquet_sql.rs`](examples/parquet_sql.rs): Build and run a query plan from a SQL statement against a local Parquet file
- [`parquet_sql_multiple_files.rs`](examples/parquet_sql_multiple_files.rs): Build and run a query plan from a SQL statement against multiple local Parquet files
- [`query-aws-s3.rs`](examples/query-aws-s3.rs): Configure `object_store` and run a query against files stored in AWS S3
- [`rewrite_expr.rs`](examples/rewrite_expr.rs): Define and invoke a custom Query Optimizer pass
- [`simple_udaf.rs`](examples/simple_udaf.rs): Define and invoke a User Defined Aggregate Function (UDAF)
- [`simple_udf.rs`](examples/simple_udf.rs): Define and invoke a User Defined (scalar) Function (UDF)
- [`simple_udfw.rs`](examples/simple_udwf.rs): Define and invoke a User Defined Window Function (UDWF)

## Distributed

- [`flight_client.rs`](examples/flight_client.rs) and [`flight_server.rs`](examples/flight_server.rs): Run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol.
