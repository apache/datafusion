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
cargo run --example dataframe -- dataframe
```

## Single Process

- [`examples/udf/advanced_udaf.rs`](examples/udf/advanced_udaf.rs): Define and invoke a more complicated User Defined Aggregate Function (UDAF)
- [`examples/udf/advanced_udf.rs`](examples/udf/advanced_udf.rs): Define and invoke a more complicated User Defined Scalar Function (UDF)
- [`examples/udf/advanced_udwf.rs`](examples/udf/advanced_udwf.rs): Define and invoke a more complicated User Defined Window Function (UDWF)
- [`examples/data_io/parquet_advanced_index.rs`](examples/data_io/parquet_advanced_index.rs): Creates a detailed secondary index that covers the contents of several parquet files
- [`examples/udf/async_udf.rs`](examples/udf/async_udf.rs): Define and invoke an asynchronous User Defined Scalar Function (UDF)
- [`examples/query_planning/analyzer_rule.rs`](examples/query_planning/analyzer_rule.rs): Use a custom AnalyzerRule to change a query's semantics (row level access control)
- [`examples/data_io/catalog.rs`](examples/data_io/catalog.rs): Register the table into a custom catalog
- [`examples/data_io/json_shredding.rs`](examples/data_io/json_shredding.rs): Shows how to implement custom filter rewriting for JSON shredding
- [`examples/proto/composed_extension_codec`](examples/proto/composed_extension_codec.rs): Example of using multiple extension codecs for serialization / deserialization
- [`examples/custom_data_source/csv_sql_streaming.rs`](examples/custom_data_source/csv_sql_streaming.rs): Build and run a streaming query plan from a SQL statement against a local CSV file
- [`examples/custom_data_source/csv_json_opener.rs`](examples/custom_data_source/csv_json_opener.rs): Use low level `FileOpener` APIs to read CSV/JSON into Arrow `RecordBatch`es
- [`examples/custom_data_source/custom_datasource.rs`](examples/custom_data_source/custom_datasource.rs): Run queries against a custom datasource (TableProvider)
- [`examples/custom_data_source/custom_file_casts.rs`](examples/custom_data_source/custom_file_casts.rs): Implement custom casting rules to adapt file schemas
- [`examples/custom_data_source/custom_file_format.rs`](examples/custom_data_source/custom_file_format.rs): Write data to a custom file format
- [`examples/external_dependency/dataframe_to_s3.rs`](examples/external_dependency/dataframe_to_s3.rs): Run a query using a DataFrame against a parquet file from s3 and writing back to s3
- [`dataframe.rs`](examples/dataframe.rs): Run a query using a DataFrame API against parquet files, csv files, and in-memory data, including multiple subqueries. Also demonstrates the various methods to write out a DataFrame to a table, parquet file, csv file, and json file.
- [`examples/builtin_functions/date_time`](examples/builtin_functions/date_time.rs): Examples of date-time related functions and queries
- [`examples/custom_data_source/default_column_values.rs`](examples/custom_data_source/default_column_values.rs): Implement custom default value handling for missing columns using field metadata and PhysicalExprAdapter
- [`examples/dataframe/deserialize_to_struct.rs`](examples/dataframe/deserialize_to_struct.rs): Convert query results (Arrow ArrayRefs) into Rust structs
- [`examples/query_planning/expr_api.rs`](examples/query_planning/expr_api.rs): Create, execute, simplify, analyze and coerce `Expr`s
- [`examples/custom_data_source/file_stream_provider.rs`](examples/custom_data_source/file_stream_provider.rs): Run a query on `FileStreamProvider` which implements `StreamProvider` for reading and writing to arbitrary stream sources / sinks.
- [`flight/sql_server.rs`](examples/flight/sql_server.rs): Run DataFusion as a standalone process and execute SQL queries from Flight and and FlightSQL (e.g. JDBC) clients
- [`examples/builtin_functions/function_factory.rs`](examples/builtin_functions/function_factory.rs): Register `CREATE FUNCTION` handler to implement SQL macros
- [`examples/execution_monitoring/memory_pool_tracking.rs`](examples/execution_monitoring/memory_pool_tracking.rs): Demonstrates TrackConsumersPool for memory tracking and debugging with enhanced error messages
- [`examples/execution_monitoring/memory_pool_execution_plan.rs`](examples/execution_monitoring/memory_pool_execution_plan.rs): Shows how to implement memory-aware ExecutionPlan with memory reservation and spilling
- [`examples/execution_monitoring/tracing.rs`](examples/execution_monitoring/tracing.rs): Demonstrates the tracing injection feature for the DataFusion runtime
- [`examples/query_planning/optimizer_rule.rs`](examples/query_planning/optimizer_rule.rs): Use a custom OptimizerRule to replace certain predicates
- [`examples/data_io/parquet_embedded_index.rs`](examples/data_io/parquet_embedded_index.rs): Store a custom index inside a Parquet file and use it to speed up queries
- [`examples/data_io/parquet_encrypted.rs`](examples/data_io/parquet_encrypted.rs): Read and write encrypted Parquet files using DataFusion
- [`examples/data_io/parquet_encrypted_with_kms.rs`](examples/data_io/parquet_encrypted_with_kms.rs): Read and write encrypted Parquet files using an encryption factory
- [`examples/data_io/parquet_index.rs`](examples/data_io/parquet_index.rs): Create an secondary index over several parquet files and use it to speed up queries
- [`examples/data_io/parquet_exec_visitor.rs`](examples/data_io/parquet_exec_visitor.rs): Extract statistics by visiting an ExecutionPlan after execution
- [`examples/query_planning/parse_sql_expr.rs`](examples/query_planning/parse_sql_expr.rs): Parse SQL text into DataFusion `Expr`.
- [`examples/query_planning/plan_to_sql.rs`](examples/query_planning/plan_to_sql.rs): Generate SQL from DataFusion `Expr` and `LogicalPlan`
- [`examples/query_planning/planner_api.rs`](examples/query_planning/planner_api.rs) APIs to manipulate logical and physical plans
- [`examples/query_planning/pruning.rs`](examples/query_planning/pruning.rs): Use pruning to rule out files based on statistics
- [`examples/query_planning/thread_pools.rs`](examples/query_planning/thread_pools.rs): Demonstrates TrackConsumersPool for memory tracking and debugging with enhanced error messages and shows how to implement memory-aware ExecutionPlan with memory reservation and spilling
- [`examples/external_dependency/query_aws_s3.rs`](examples/external_dependency/query_aws_s3.rs): Configure `object_store` and run a query against files stored in AWS S3
- [`examples/data_io/query_http_csv.rs`](examples/data_io/query_http_csv.rs): Configure `object_store` and run a query against files via HTTP
- [`examples/builtin_functions/regexp.rs`](examples/builtin_functions/regexp.rs): Examples of using regular expression functions
- [`examples/data_io/remote_catalog.rs`](examples/data_io/remote_catalog.rs): Examples of interfacing with a remote catalog (e.g. over a network)
- [`examples/udf/simple_udaf.rs`](examples/udf/simple_udaf.rs): Define and invoke a User Defined Aggregate Function (UDAF)
- [`examples/udf/simple_udf.rs`](examples/udf/simple_udf.rs): Define and invoke a User Defined Scalar Function (UDF)
- [`examples/udf/simple_udtf.rs`](examples/udf/simple_udtf.rs): Define and invoke a User Defined Table Function (UDTF)
- [`examples/udf/simple_udfw.rs`](examples/udf/simple_udwf.rs): Define and invoke a User Defined Window Function (UDWF)
- [`examples/sql_ops/analysis.rs`](examples/sql_ops/analysis.rs): Analyse SQL queries with DataFusion structures
- [`examples/sql_ops/frontend.rs`](examples/sql_ops/frontend.rs): Create LogicalPlans (only) from sql strings
- [`examples/sql_ops/dialect.rs`](examples/sql_ops/dialect.rs): Example of implementing a custom SQL dialect on top of `DFParser`
- [`examples/sql_ops/query.rs`](examples/sql_ops/query.rs): Query data using SQL (in memory `RecordBatches`, local Parquet files)

## Distributed

- [`examples/flight/client.rs`](examples/flight/client.rs) and [`examples/flight/server.rs`](examples/flight/server.rs): Run DataFusion as a standalone process and execute SQL queries from a client using the Arrow Flight protocol.
