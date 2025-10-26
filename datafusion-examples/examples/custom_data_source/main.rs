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
//! This example demonstrates how DataFusion reads data.
//!
//! ## Usage
//! ```bash
//! cargo run --example custom_data_source -- [csv_json_opener|csv_sql_streaming|custom_datasource|custom_file_casts|custom_file_format|file_stream_provider]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `csv_json_opener` — use low level FileOpener APIs to read CSV/JSON into Arrow RecordBatches
//! - `csv_sql_streaming` — build and run a streaming query plan from a SQL statement against a local CSV file
//! - `custom_datasource` — run queries against a custom datasource (TableProvider)
//! - `custom_file_casts` — implement custom casting rules to adapt file schemas
//! - `custom_file_format` — write data to a custom file format
//! - `file_stream_provider` — run a query on FileStreamProvider which implements StreamProvider for reading and writing to arbitrary stream sources / sinks

mod csv_json_opener;
mod csv_sql_streaming;
mod custom_datasource;
mod custom_file_casts;
mod custom_file_format;
mod file_stream_provider;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    CsvJsonOpener,
    CsvSqlStreaming,
    CustomDatasource,
    CustomFileCasts,
    CustomFileFormat,
    FileFtreamProvider,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "csv_json_opener" => Ok(Self::CsvJsonOpener),
            "csv_sql_streaming" => Ok(Self::CsvSqlStreaming),
            "custom_datasource" => Ok(Self::CustomDatasource),
            "custom_file_casts" => Ok(Self::CustomFileCasts),
            "custom_file_format" => Ok(Self::CustomFileFormat),
            "file_stream_provider" => Ok(Self::FileFtreamProvider),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example custom_data_source -- [csv_json_opener|csv_sql_streaming|custom_datasource|custom_file_casts|custom_file_format|file_stream_provider]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::CsvJsonOpener => csv_json_opener::csv_json_opener().await?,
        ExampleKind::CsvSqlStreaming => csv_sql_streaming::csv_sql_streaming().await?,
        ExampleKind::CustomDatasource => custom_datasource::custom_data_source().await?,
        ExampleKind::CustomFileCasts => custom_file_casts::custom_file_casts().await?,
        ExampleKind::CustomFileFormat => custom_file_format::custom_file_format().await?,
        ExampleKind::FileFtreamProvider => {
            file_stream_provider::file_stream_provider().await?
        }
    }

    Ok(())
}
