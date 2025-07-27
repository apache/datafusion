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

This crate includes end to end, highly commented examples of how to use
various DataFusion APIs to help you get started.

## Prerequisites

Run `git submodule update --init` to init test files.

## Running Examples

To run an example, use the `cargo run` command, such as:

```bash
git clone https://github.com/apache/datafusion
cd datafusion
# Download test data
git submodule update --init

# Change to the examples directory
cd datafusion-examples/examples

# Run the `dataframe` example:
# ... use the equivalent for other examples
cargo run --example dataframe
```

## Single Process

- [`advanced_udaf.rs`](examples/advanced_udaf.rs): Define and invoke a more complicated User Defined Aggregate Function (UDAF)
- [`advanced_udf.rs`](examples/advanced_udf.rs): Define and invoke a more complicated User Defined Scalar Function (UDF)
- [`advanced_udwf.rs`](examples/advanced_udwf.rs): Define and invoke a more complicated User Defined Window Function (UDWF)
- [`advanced_parquet_index.rs`](examples/advanced_parquet_index.rs): Creates a detailed secondary index that covers the contents of several parquet files
- [`async_udf.rs`](examples/async_udf.rs): Define and invoke an asynchronous User Defined Scalar Function (UDF)
- [`analyzer_rule.rs`](examples/analyzer_rule.rs): Use a custom AnalyzerRule to change a query's semantics (row level access control)
- [`catalog.rs`](examples/catalog.rs): Register the table into a custom catalog
- [`composed_extension_codec`](examples/composed_extension_codec.rs): Example of using multiple extension codecs for serialization / deserialization
- [`csv_sql_streaming.rs`](examples/csv_sql_streaming.rs): Build and run a streaming query plan from a SQL statement against a local CSV file
- [`csv_json_opener.rs`](examples/csv_json_opener.rs): Use low level `FileOpener` APIs to read CSV/JSON into Arrow `RecordBatch`es
- [`custom_datasource.rs`](examples/custom_datasource.rs): Run queries against a custom datasource (TableProvider)
- [`custom_file_format.rs`](examples/custom_file_format.rs): Write data to a custom file format
- [`dataframe-to-s3.rs`](examples/external_dependency/dataframe-to-s3.rs): Run a query using a DataFrame against a parquet file from s3 and writing back to s3
- [`dataframe.rs`](examples/dataframe.rs): Run a query using a DataFrame API against parquet files, csv files, and in-memory data, including multiple subqueries. Also demonstrates the various methods to write out a DataFrame to a table, parquet file, csv file, and json file.
- [`deserialize_to_struct.rs`](examples/deserialize_to_struct.rs): Convert query results (Arrow ArrayRefs) into Rust structs
- [`expr_api.rs`](examples/expr_api.rs): Create, execute, simplify, analyze and coerce `Expr`s
- [`file_stream_provider.rs`](examples/file_stream_provider.rs): Run a query on `FileStreamProvider` which implements `StreamProvider` for reading and writing to arbitrary stream sources / sinks.
- [`flight_sql_server.rs`](examples/flight/flight_sql_server.rs): Run DataFusion as a standalone process and execute SQL queries from JDBC clients
- [`function_factory.rs`](examples/function_factory.rs): Register `CREATE FUNCTION` handler to implement SQL macros
- [`optimizer_rule.rs`](examples/optimizer_rule.rs): Use a custom OptimizerRule to replace certain predicates
- [`parquet_embedded_index.rs`](examples/parquet_embedded_index.rs): Store a custom index inside a Parquet file and use it to speed up queries
- [`parquet_encrypted.rs`](examples/parquet_encrypted.rs): Read and write encrypted Parquet files using DataFusion
- [`parquet_index.rs`](examples/parquet_index.rs): Create an secondary index over several parquet files and use it to speed up queries
- [`parquet_exec_visitor.rs`](examples/parquet_exec_visitor.rs): Extract statistics by visiting an ExecutionPlan after execution
- [`parse_sql_expr.rs`](examples/parse_sql_expr.rs): Parse SQL text into DataFusion `Expr`.
- [`plan_to_sql.rs`](examples/plan_to_sql.rs): Generate SQL from DataFusion `Expr` and `LogicalPlan`
- [`planner_api.rs`](examples/planner_api.rs) APIs to manipulate logical and physical plans
- [`pruning.rs`](examples/pruning.rs): Use pruning to rule out files based on statistics
- [`query-aws-s3.rs`](examples/external_dependency/query-aws-s3.rs): Configure `object_store` and run a query against files stored in AWS S3
- [`query-http-csv.rs`](examples/query-http-csv.rs): Configure `object_store` and run a query against files vi HTTP
- [`regexp.rs`](examples/regexp.rs): Examples of using regular expression functions
- [`remote_catalog.rs`](examples/regexp.rs): Examples of interfacing with a remote catalog (e.g. over a network)
- [`simple_udaf.rs`](examples/simple_udaf.rs): Define and invoke a User Defined Aggregate Function (UDAF)
- [`simple_udf.rs`](examples/simple_udf.rs): Define and invoke a User Defined Scalar Function (UDF)
- [`simple_udfw.rs`](examples/simple_udwf.rs): Define and invoke a User Defined Window Function (UDWF)
- [`sql_analysis.rs`](examples/sql_analysis.rs): Analyse SQL queries with DataFusion structures
- [`sql_frontend.rs`](examples/sql_frontend.rs): Create LogicalPlans (only) from sql strings
- [`sql_dialect.rs`](examples/sql_dialect.rs): Example of implementing a custom SQL dialect on top of `DFParser`
- [`sql_query.rs`](examples/memtable.rs): Query data using SQL (in memory `RecordBatches`, local Parquet files)
- [`date_time_function.rs`](examples/date_time_function.rs): Examples of date-time related functions and queries.

## Distributed

- [`flight_client.rs`](examples/flight/flight_client.rs) and [`flight_server.rs`](examples/flight/flight_server.rs): Run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol.
