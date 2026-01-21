// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! # These examples are all related to extending or defining how DataFusion reads data
//!
//! These examples demonstrate how DataFusion reads data.
//!
//! ## Usage
//! ```bash
//! cargo run --example custom_data_source -- [all|csv_json_opener|csv_sql_streaming|custom_datasource|custom_file_casts|custom_file_format|default_column_values|file_stream_provider]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `csv_json_opener` — use low level FileOpener APIs to read CSV/JSON into Arrow RecordBatches
//! - `csv_sql_streaming` — build and run a streaming query plan from a SQL statement against a local CSV file
//! - `custom_datasource` — run queries against a custom datasource (TableProvider)
//! - `custom_file_casts` — implement custom casting rules to adapt file schemas
//! - `custom_file_format` — write data to a custom file format
//! - `default_column_values` — implement custom default value handling for missing columns using field metadata and PhysicalExprAdapter
//! - `file_stream_provider` — run a query on FileStreamProvider which implements StreamProvider for reading and writing to arbitrary stream sources/sinks

mod csv_json_opener;
mod csv_sql_streaming;
mod custom_datasource;
mod custom_file_casts;
mod custom_file_format;
mod default_column_values;
mod file_stream_provider;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    CsvJsonOpener,
    CsvSqlStreaming,
    CustomDatasource,
    CustomFileCasts,
    CustomFileFormat,
    DefaultColumnValues,
    FileStreamProvider,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "custom_data_source";

    fn runnable() -> impl Iterator<Item = ExampleKind> {
        ExampleKind::iter().filter(|v| !matches!(v, ExampleKind::All))
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::runnable() {
                    println!("Running example: {example}");
                    Box::pin(example.run()).await?;
                }
            }
            ExampleKind::CsvJsonOpener => csv_json_opener::csv_json_opener().await?,
            ExampleKind::CsvSqlStreaming => {
                csv_sql_streaming::csv_sql_streaming().await?
            }
            ExampleKind::CustomDatasource => {
                custom_datasource::custom_datasource().await?
            }
            ExampleKind::CustomFileCasts => {
                custom_file_casts::custom_file_casts().await?
            }
            ExampleKind::CustomFileFormat => {
                custom_file_format::custom_file_format().await?
            }
            ExampleKind::DefaultColumnValues => {
                default_column_values::default_column_values().await?
            }
            ExampleKind::FileStreamProvider => {
                file_stream_provider::file_stream_provider().await?
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::VARIANTS.join("|")
    );

    let example: ExampleKind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ExampleKind::All.to_string())
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
