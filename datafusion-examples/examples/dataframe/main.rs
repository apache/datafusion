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

//! # These are core DataFrame API usage
//!
//! This example demonstrates core DataFrame API usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example dataframe -- [dataframe|default_column_values|deserialize_to_struct]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `dataframe` — run a query using a DataFrame API against parquet files, csv files, and in-memory data, including multiple subqueries
//! - `default_column_values` — implement custom default value handling for missing columns using field metadata and PhysicalExprAdapter
//! - `deserialize_to_struct` — convert query results (Arrow ArrayRefs) into Rust structs

mod dataframe;
mod default_column_values;
mod deserialize_to_struct;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Dataframe,
    DefaultColumnValues,
    DeserializeToStruct,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "dataframe" => Ok(Self::Dataframe),
            "default_column_values" => Ok(Self::DefaultColumnValues),
            "deserialize_to_struct" => Ok(Self::DeserializeToStruct),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example dataframe -- [dataframe|default_column_values|deserialize_to_struct]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::Dataframe => dataframe::dataframe().await?,
        ExampleKind::DefaultColumnValues => {
            default_column_values::default_column_values().await?
        }
        ExampleKind::DeserializeToStruct => {
            deserialize_to_struct::deserialize_to_struct().await?
        }
    }

    Ok(())
}
