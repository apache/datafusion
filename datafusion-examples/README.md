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

# Run all examples in a group
cargo run --example <group> -- all

# Run a specific example within a group
cargo run --example <group> -- <subcommand>

# Run all examples in the `dataframe` group
cargo run --example dataframe -- all

# Run a single example from the `dataframe` group
# (apply the same pattern for any other group)
cargo run --example dataframe -- dataframe
```

## Builtin Functions Examples

### Group: `builtin_functions`

#### Category: Single Process

| Subcommand       | File Path                                                                                 | Description                                                |
| ---------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| date_time        | [`builtin_functions/date_time.rs`](examples/builtin_functions/date_time.rs)               | Examples of date-time related functions and queries        |
| function_factory | [`builtin_functions/function_factory.rs`](examples/builtin_functions/function_factory.rs) | Register `CREATE FUNCTION` handler to implement SQL macros |
| regexp           | [`builtin_functions/regexp.rs`](examples/builtin_functions/regexp.rs)                     | Examples of using regular expression functions             |

## Custom Data Source Examples

### Group: `custom_data_source`

#### Category: Single Process

| Subcommand            | File Path                                                                                             | Description                                   |
| --------------------- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| csv_sql_streaming     | [`custom_data_source/csv_sql_streaming.rs`](examples/custom_data_source/csv_sql_streaming.rs)         | Run a streaming SQL query against CSV data    |
| csv_json_opener       | [`custom_data_source/csv_json_opener.rs`](examples/custom_data_source/csv_json_opener.rs)             | Use low-level FileOpener APIs for CSV/JSON    |
| custom_datasource     | [`custom_data_source/custom_datasource.rs`](examples/custom_data_source/custom_datasource.rs)         | Query a custom TableProvider                  |
| custom_file_casts     | [`custom_data_source/custom_file_casts.rs`](examples/custom_data_source/custom_file_casts.rs)         | Implement custom casting rules                |
| custom_file_format    | [`custom_data_source/custom_file_format.rs`](examples/custom_data_source/custom_file_format.rs)       | Write to a custom file format                 |
| default_column_values | [`custom_data_source/default_column_values.rs`](examples/custom_data_source/default_column_values.rs) | Custom default values using metadata          |
| file_stream_provider  | [`custom_data_source/file_stream_provider.rs`](examples/custom_data_source/file_stream_provider.rs)   | Read/write via FileStreamProvider for streams |

## Data IO Examples

### Group: `data_io`

#### Category: Single Process

| Subcommand           | File Path                                                                                 | Description                                            |
| -------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| catalog              | [`data_io/catalog.rs`](examples/data_io/catalog.rs)                                       | Register tables into a custom catalog                  |
| json_shredding       | [`data_io/json_shredding.rs`](examples/data_io/json_shredding.rs)                         | Implement filter rewriting for JSON shredding          |
| parquet_adv_idx      | [`data_io/parquet_advanced_index.rs`](examples/data_io/parquet_advanced_index.rs)         | Create a secondary index across multiple parquet files |
| parquet_emb_idx      | [`data_io/parquet_embedded_index.rs`](examples/data_io/parquet_embedded_index.rs)         | Store a custom index inside Parquet files              |
| parquet_enc          | [`data_io/parquet_encrypted.rs`](examples/data_io/parquet_encrypted.rs)                   | Read & write encrypted Parquet files                   |
| parquet_enc_with_kms | [`data_io/parquet_encrypted_with_kms.rs`](examples/data_io/parquet_encrypted_with_kms.rs) | Encrypted Parquet I/O using a KMS-backed factory       |
| parquet_exec_visitor | [`data_io/parquet_exec_visitor.rs`](examples/data_io/parquet_exec_visitor.rs)             | Extract statistics by visiting an ExecutionPlan        |
| parquet_idx          | [`data_io/parquet_index.rs`](examples/data_io/parquet_index.rs)                           | Create a secondary index                               |
| query_http_csv       | [`data_io/query_http_csv.rs`](examples/data_io/query_http_csv.rs)                         | Query CSV files via HTTP                               |
| remote_catalog       | [`data_io/remote_catalog.rs`](examples/data_io/remote_catalog.rs)                         | Interact with a remote catalog                         |

## DataFrame Examples

### Group: `dataframe`

#### Category: Single Process

| Subcommand            | File Path                                                                           | Description                                             |
| --------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------- |
| cache_factory         | [`dataframe/cache_factory.rs`](examples/dataframe/cache_factory.rs)                 | Custom lazy caching for DataFrames using `CacheFactory` |
| dataframe             | [`dataframe/dataframe.rs`](examples/dataframe/dataframe.rs)                         | Query DataFrames from various sources and write output  |
| deserialize_to_struct | [`dataframe/deserialize_to_struct.rs`](examples/dataframe/deserialize_to_struct.rs) | Convert Arrow arrays into Rust structs                  |

## Execution Monitoring Examples

### Group: `execution_monitoring`

#### Category: Single Process

| Subcommand         | File Path                                                                                                           | Description                              |
| ------------------ | ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| mem_pool_exec_plan | [`execution_monitoring/memory_pool_execution_plan.rs`](examples/execution_monitoring/memory_pool_execution_plan.rs) | Memory-aware ExecutionPlan with spilling |
| mem_pool_tracking  | [`execution_monitoring/memory_pool_tracking.rs`](examples/execution_monitoring/memory_pool_tracking.rs)             | Demonstrates memory tracking             |
| tracing            | [`execution_monitoring/tracing.rs`](examples/execution_monitoring/tracing.rs)                                       | Demonstrates tracing integration         |

## External Dependency Examples

### Group: `external_dependency`

#### Category: Single Process

| Subcommand      | File Path                                                                                   | Description                              |
| --------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------- |
| dataframe_to_s3 | [`external_dependency/dataframe_to_s3.rs`](examples/external_dependency/dataframe_to_s3.rs) | Query DataFrames and write results to S3 |
| query_aws_s3    | [`external_dependency/query_aws_s3.rs`](examples/external_dependency/query_aws_s3.rs)       | Query S3-backed data using object_store  |

## Flight Examples

### Group: `flight`

#### Category: Distributed

| Subcommand | File Path                                               | Description                                            |
| ---------- | ------------------------------------------------------- | ------------------------------------------------------ |
| server     | [`flight/server.rs`](examples/flight/server.rs)         | Run DataFusion server accepting FlightSQL/JDBC queries |
| client     | [`flight/client.rs`](examples/flight/client.rs)         | Execute SQL queries via Arrow Flight protocol          |
| sql_server | [`flight/sql_server.rs`](examples/flight/sql_server.rs) | Standalone SQL server for JDBC clients                 |

## Proto Examples

### Group: `proto`

#### Category: Single Process

| Subcommand               | File Path                                                                         | Description                                                     |
| ------------------------ | --------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| composed_extension_codec | [`proto/composed_extension_codec.rs`](examples/proto/composed_extension_codec.rs) | Use multiple extension codecs for serialization/deserialization |

## Query Planning Examples

### Group: `query_planning`

#### Category: Single Process

| Subcommand     | File Path                                                                       | Description                                            |
| -------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------ |
| analyzer_rule  | [`query_planning/analyzer_rule.rs`](examples/query_planning/analyzer_rule.rs)   | Custom AnalyzerRule to change query semantics          |
| expr_api       | [`query_planning/expr_api.rs`](examples/query_planning/expr_api.rs)             | Create, execute, analyze, and coerce Exprs             |
| optimizer_rule | [`query_planning/optimizer_rule.rs`](examples/query_planning/optimizer_rule.rs) | Replace predicates via a custom OptimizerRule          |
| parse_sql_expr | [`query_planning/parse_sql_expr.rs`](examples/query_planning/parse_sql_expr.rs) | Parse SQL into DataFusion Expr                         |
| plan_to_sql    | [`query_planning/plan_to_sql.rs`](examples/query_planning/plan_to_sql.rs)       | Generate SQL from expressions or plans                 |
| planner_api    | [`query_planning/planner_api.rs`](examples/query_planning/planner_api.rs)       | APIs for logical and physical plan manipulation        |
| pruning        | [`query_planning/pruning.rs`](examples/query_planning/pruning.rs)               | Use pruning to skip irrelevant files                   |
| thread_pools   | [`query_planning/thread_pools.rs`](examples/query_planning/thread_pools.rs)     | Configure custom thread pools for DataFusion execution |

## Relation Planner Examples

### Group: `relation_planner`

#### Category: Single Process

| Subcommand      | File Path                                                                             | Description                                |
| --------------- | ------------------------------------------------------------------------------------- | ------------------------------------------ |
| match_recognize | [`relation_planner/match_recognize.rs`](examples/relation_planner/match_recognize.rs) | Implement MATCH_RECOGNIZE pattern matching |
| pivot_unpivot   | [`relation_planner/pivot_unpivot.rs`](examples/relation_planner/pivot_unpivot.rs)     | Implement PIVOT / UNPIVOT                  |
| table_sample    | [`relation_planner/table_sample.rs`](examples/relation_planner/table_sample.rs)       | Implement TABLESAMPLE                      |

## SQL Ops Examples

### Group: `sql_ops`

#### Category: Single Process

| Subcommand        | File Path                                                               | Description                                        |
| ----------------- | ----------------------------------------------------------------------- | -------------------------------------------------- |
| analysis          | [`sql_ops/analysis.rs`](examples/sql_ops/analysis.rs)                   | Analyze SQL queries                                |
| custom_sql_parser | [`sql_ops/custom_sql_parser.rs`](examples/sql_ops/custom_sql_parser.rs) | Implement a custom SQL parser to extend DataFusion |
| frontend          | [`sql_ops/frontend.rs`](examples/sql_ops/frontend.rs)                   | Build LogicalPlans from SQL                        |
| query             | [`sql_ops/query.rs`](examples/sql_ops/query.rs)                         | Query data using SQL                               |

## UDF Examples

### Group: `udf`

#### Category: Single Process

| Subcommand | File Path                                               | Description                                     |
| ---------- | ------------------------------------------------------- | ----------------------------------------------- |
| adv_udaf   | [`udf/advanced_udaf.rs`](examples/udf/advanced_udaf.rs) | Advanced User Defined Aggregate Function (UDAF) |
| adv_udf    | [`udf/advanced_udf.rs`](examples/udf/advanced_udf.rs)   | Advanced User Defined Scalar Function (UDF)     |
| adv_udwf   | [`udf/advanced_udwf.rs`](examples/udf/advanced_udwf.rs) | Advanced User Defined Window Function (UDWF)    |
| async_udf  | [`udf/async_udf.rs`](examples/udf/async_udf.rs)         | Asynchronous User Defined Scalar Function       |
| udaf       | [`udf/simple_udaf.rs`](examples/udf/simple_udaf.rs)     | Simple UDAF example                             |
| udf        | [`udf/simple_udf.rs`](examples/udf/simple_udf.rs)       | Simple UDF example                              |
| udtf       | [`udf/simple_udtf.rs`](examples/udf/simple_udtf.rs)     | Simple UDTF example                             |
| udwf       | [`udf/simple_udwf.rs`](examples/udf/simple_udwf.rs)     | Simple UDWF example                             |
