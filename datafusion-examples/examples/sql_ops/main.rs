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

//! # SQL Examples
//!
//! This example demonstrates SQL in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example sql_ops -- [analysis|dialect|frontend|query]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `analysis` — analyse SQL queries with DataFusion structures
//! - `dialect` — implementing a custom SQL dialect on top of DFParser
//! - `frontend` — create LogicalPlans (only) from sql strings
//! - `query` — query data using SQL (in memory RecordBatches, local Parquet files)

mod analysis;
mod dialect;
mod frontend;
mod query;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Analysis,
    Dialect,
    Frontend,
    Query,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "analysis" => Ok(Self::Analysis),
            "dialect" => Ok(Self::Dialect),
            "frontend" => Ok(Self::Frontend),
            "query" => Ok(Self::Query),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!(
            "Usage: cargo run --example sql_ops -- [analysis|dialect|frontend|query]"
        );
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::Analysis => analysis::analysis().await?,
        ExampleKind::Dialect => dialect::dialect().await?,
        ExampleKind::Frontend => frontend::frontend()?,
        ExampleKind::Query => query::query().await?,
    }

    Ok(())
}
