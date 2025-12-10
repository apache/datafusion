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

| Group             | Subcommand       | Category       | File Path                                        | Description                                                |
| ----------------- | ---------------- | -------------- | ------------------------------------------------ | ---------------------------------------------------------- |
| builtin_functions | date_time        | Single Process | `examples/builtin_functions/date_time.rs`        | Examples of date-time related functions and queries        |
| builtin_functions | function_factory | Single Process | `examples/builtin_functions/function_factory.rs` | Register `CREATE FUNCTION` handler to implement SQL macros |
| builtin_functions | regexp           | Single Process | `examples/builtin_functions/regexp.rs`           | Examples of using regular expression functions             |

## Custom Data Source Examples

| Group              | Subcommand            | Category       | File Path                                              | Description                                   |
| ------------------ | --------------------- | -------------- | ------------------------------------------------------ | --------------------------------------------- |
| custom_data_source | csv_sql_streaming     | Single Process | `examples/custom_data_source/csv_sql_streaming.rs`     | Run a streaming SQL query against CSV data    |
| custom_data_source | csv_json_opener       | Single Process | `examples/custom_data_source/csv_json_opener.rs`       | Use low-level FileOpener APIs for CSV/JSON    |
| custom_data_source | custom_datasource     | Single Process | `examples/custom_data_source/custom_datasource.rs`     | Query a custom TableProvider                  |
| custom_data_source | custom_file_casts     | Single Process | `examples/custom_data_source/custom_file_casts.rs`     | Implement custom casting rules for schemas    |
| custom_data_source | custom_file_format    | Single Process | `examples/custom_data_source/custom_file_format.rs`    | Write to a custom file format                 |
| custom_data_source | default_column_values | Single Process | `examples/custom_data_source/default_column_values.rs` | Custom default values using metadata          |
| custom_data_source | file_stream_provider  | Single Process | `examples/custom_data_source/file_stream_provider.rs`  | Read/write via FileStreamProvider for streams |

## Data IO Examples

| Group   | Subcommand                 | Category       | File Path                                        | Description                                                      |
| ------- | -------------------------- | -------------- | ------------------------------------------------ | ---------------------------------------------------------------- |
| data_io | catalog                    | Single Process | `examples/data_io/catalog.rs`                    | Register tables into a custom catalog                            |
| data_io | json_shredding             | Single Process | `examples/data_io/json_shredding.rs`             | Implement custom filter rewriting for JSON shredding             |
| data_io | parquet_adv_idx            | Single Process | `examples/data_io/parquet_advanced_index.rs`     | Creates a detailed secondary index across multiple parquet files |
| data_io | parquet_emb_idx            | Single Process | `examples/data_io/parquet_embedded_index.rs`     | Store a custom index inside Parquet files                        |
| data_io | parquet_enc                | Single Process | `examples/data_io/parquet_encrypted.rs`          | Read & write encrypted Parquet files                             |
| data_io | parquet_enc_with_kms       | Single Process | `examples/data_io/parquet_encrypted_with_kms.rs` | Encrypted Parquet I/O using a KMS-backed encryption factory      |
| data_io | parquet_exec_visitor       | Single Process | `examples/data_io/parquet_exec_visitor.rs`       | Extract statistics by visiting an ExecutionPlan                  |
| data_io | parquet_idx                | Single Process | `examples/data_io/parquet_index.rs`              | Create a secondary index over several parquet files              |
| data_io | query_http_csv             | Single Process | `examples/data_io/query_http_csv.rs`             | Query CSV files via HTTP using object_store                      |
| data_io | remote_catalog             | Single Process | `examples/data_io/remote_catalog.rs`             | Interact with a remote catalog                                   |

## DataFrame Examples

| Group     | Subcommand            | Category       | File Path                                     | Description                                                                   |
| --------- | --------------------- | -------------- | --------------------------------------------- | ----------------------------------------------------------------------------- |
| dataframe | dataframe             | Single Process | `examples/dataframe.rs`                       | Query DataFrames from Parquet/CSV/memory and write output to multiple formats |
| dataframe | deserialize_to_struct | Single Process | `examples/dataframe/deserialize_to_struct.rs` | Convert Arrow arrays into Rust structs                                        |

## Execution Monitoring Examples

| Group                | Subcommand                 | Category       | File Path                                                     | Description                                          |
| -------------------- | -------------------------- | -------------- | ------------------------------------------------------------- | ---------------------------------------------------- |
| execution_monitoring | mem_pool_exec_plan         | Single Process | `examples/execution_monitoring/memory_pool_execution_plan.rs` | Memory-aware ExecutionPlan with spilling             |
| execution_monitoring | mem_pool_tracking          | Single Process | `examples/execution_monitoring/memory_pool_tracking.rs`       | Demonstrates memory tracking with TrackConsumersPool |
| execution_monitoring | tracing                    | Single Process | `examples/execution_monitoring/tracing.rs`                    | Demonstrates tracing injection in DataFusion runtime |

## External Dependency Examples

| Group               | Subcommand      | Category       | File Path                                         | Description                              |
| ------------------- | --------------- | -------------- | ------------------------------------------------- | ---------------------------------------- |
| external_dependency | dataframe_to_s3 | Single Process | `examples/external_dependency/dataframe_to_s3.rs` | Query DataFrames and write results to S3 |
| external_dependency | query_aws_s3    | Single Process | `examples/external_dependency/query_aws_s3.rs`    | Query S3-backed data using object_store  |

## Flight Examples

| Group  | Subcommand    | Category    | File Path                   | Description                                            |
| ------ | ------------- | ----------- | --------------------------- | ------------------------------------------------------ |
| flight | server | Distributed | `examples/flight/server.rs` | Run DataFusion server accepting FlightSQL/JDBC queries |
| flight | client | Distributed | `examples/flight/client.rs` | Execute SQL queries using the Arrow Flight protocol    |
| flight | sql_server | Distributed | `examples/flight/sql_server.rs` | Run DataFusion as a standalone process and execute SQL queries from JDBC clients    |

## Proto Examples

| Group | Subcommand               | Category       | File Path                                    | Description                                                     |
| ----- | ------------------------ | -------------- | -------------------------------------------- | --------------------------------------------------------------- |
| proto | composed_extension_codec | Single Process | `examples/proto/composed_extension_codec.rs` | Use multiple extension codecs for serialization/deserialization |

## Query Planning Examples

| Group          | Subcommand     | Category       | File Path                                   | Description                                              |
| -------------- | -------------- | -------------- | ------------------------------------------- | -------------------------------------------------------- |
| query_planning | analyzer_rule  | Single Process | `examples/query_planning/analyzer_rule.rs`  | Custom AnalyzerRule to change query semantics            |
| query_planning | expr_api       | Single Process | `examples/query_planning/expr_api.rs`       | Create, execute, analyze, and coerce Exprs               |
| query_planning | optimizer_rule | Single Process | `examples/query_planning/optimizer_rule.rs` | Replace predicates via a custom OptimizerRule            |
| query_planning | parse_sql_expr | Single Process | `examples/query_planning/parse_sql_expr.rs` | Parse SQL text into DataFusion Expr                      |
| query_planning | plan_to_sql    | Single Process | `examples/query_planning/plan_to_sql.rs`    | Generate SQL from expressions or plans                   |
| query_planning | planner_api    | Single Process | `examples/query_planning/planner_api.rs`    | APIs for manipulating logical and physical plans         |
| query_planning | pruning        | Single Process | `examples/query_planning/pruning.rs`        | Use pruning to skip irrelevant files                     |
| query_planning | thread_pools   | Single Process | `examples/query_planning/thread_pools.rs`   | Demonstrates memory tracking, spilling & execution pools |

## Relation Planner Examples

| Group            | Subcommand      | Category       | File Path                                      | Description                                |
| ---------------- | --------------- | -------------- | ---------------------------------------------- | ------------------------------------------ |
| relation_planner | match_recognize | Single Process | `examples/relation_planner/match_recognize.rs` | Implement MATCH_RECOGNIZE pattern matching |
| relation_planner | pivot_unpivot   | Single Process | `examples/relation_planner/pivot_unpivot.rs`   | Implement PIVOT / UNPIVOT                  |
| relation_planner | table_sample    | Single Process | `examples/relation_planner/table_sample.rs`    | Implement TABLESAMPLE                      |

## SQL Ops Examples

| Group   | Subcommand | Category       | File Path                      | Description                                    |
| ------- | ---------- | -------------- | ------------------------------ | ---------------------------------------------- |
| sql_ops | analysis   | Single Process | `examples/sql_ops/analysis.rs` | Analyze SQL queries with DataFusion structures |
| sql_ops | dialect    | Single Process | `examples/sql_ops/dialect.rs`  | Implement a custom SQL dialect                 |
| sql_ops | frontend   | Single Process | `examples/sql_ops/frontend.rs` | Build LogicalPlans from SQL strings            |
| sql_ops | query      | Single Process | `examples/sql_ops/query.rs`    | Query data via SQL                             |


## UDF Examples

| Group | Subcommand    | Category       | File Path                       | Description                                                                  |
| ----- | ------------- | -------------- | ------------------------------- | ---------------------------------------------------------------------------- |
| udf   | adv_udaf      | Single Process | `examples/udf/advanced_udaf.rs` | Define and invoke a more complicated User Defined Aggregate Function (UDAF) |
| udf   | adv_udf       | Single Process | `examples/udf/advanced_udf.rs`  | Define and invoke a more complicated User Defined Scalar Function (UDF)     |
| udf   | adv_udwf      | Single Process | `examples/udf/advanced_udwf.rs` | Define and invoke a more complicated User Defined Window Function (UDWF)    |
| udf   | async_udf     | Single Process | `examples/udf/async_udf.rs`     | Define and invoke an asynchronous User Defined Scalar Function (UDF)        |
| udf   | udaf          | Single Process | `examples/udf/simple_udaf.rs`   | Define and invoke a User Defined Aggregate Function (UDAF)                  |
| udf   | udf           | Single Process | `examples/udf/simple_udf.rs`    | Define and invoke a User Defined Scalar Function (UDF)                      |
| udf   | udtf          | Single Process | `examples/udf/simple_udtf.rs`   | Define and invoke a User Defined Table Function (UDTF)                      |
| udf   | udfw          | Single Process | `examples/udf/simple_udfw.rs`   | Define and invoke a User Defined Window Function (UDWF)                     |
