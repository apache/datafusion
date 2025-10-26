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

//! # These examples of data formats and I/O
//!
//! This example demonstrates data formats and I/O.
//!
//! ## Usage
//! ```bash
//! cargo run --example data_io -- [catalog|json_shredding|query_http_csv|remote_catalog]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `catalog` — register the table into a custom catalog
//! - `json_shredding` — shows how to implement custom filter rewriting for JSON shredding
//! - `query_http_csv` — demonstrates executing a simple query against an Arrow data source (CSV) and fetching results
//! - `remote_catalog` — interfacing with a remote catalog (e.g. over a network)

mod catalog;
mod json_shredding;
mod query_http_csv;
mod remote_catalog;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Catalog,
    JsonShredding,
    QueryHttpCsv,
    RemoteCatalog,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "catalog" => Ok(Self::Catalog),
            "json_shredding" => Ok(Self::JsonShredding),
            "query_http_csv" => Ok(Self::QueryHttpCsv),
            "remote_catalog" => Ok(Self::RemoteCatalog),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example data_io -- [catalog|json_shredding|query_http_csv|remote_catalog]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::Catalog => catalog::catalog().await?,
        ExampleKind::JsonShredding => json_shredding::json_shredding().await?,
        ExampleKind::QueryHttpCsv => query_http_csv::query_http_csv().await?,
        ExampleKind::RemoteCatalog => remote_catalog::remote_catalog().await?,
    }

    Ok(())
}
