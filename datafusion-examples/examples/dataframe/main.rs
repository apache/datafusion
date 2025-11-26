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
//! These examples demonstrate core DataFrame API usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example dataframe -- [all|dataframe|deserialize_to_struct]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `dataframe` — run a query using a DataFrame API against parquet files, csv files, and in-memory data, including multiple subqueries
//! - `deserialize_to_struct` — convert query results (Arrow ArrayRefs) into Rust structs

mod dataframe;
mod deserialize_to_struct;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    Dataframe,
    DeserializeToStruct,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::Dataframe => "dataframe",
            Self::DeserializeToStruct => "deserialize_to_struct",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "dataframe" => Ok(Self::Dataframe),
            "deserialize_to_struct" => Ok(Self::DeserializeToStruct),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 3] =
        [Self::All, Self::Dataframe, Self::DeserializeToStruct];

    const RUNNABLE_VARIANTS: [Self; 2] = [Self::Dataframe, Self::DeserializeToStruct];

    const EXAMPLE_NAME: &str = "dataframe";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::Dataframe => dataframe::dataframe_example().await?,
            ExampleKind::DeserializeToStruct => {
                deserialize_to_struct::deserialize_to_struct().await?
            }
            ExampleKind::All => unreachable!("`All` should be handled in main"),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::variants().join("|")
    );

    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("{usage}");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::All => {
            for example in ExampleKind::RUNNABLE_VARIANTS {
                println!("Running example: {}", example.as_ref());
                example.run().await?;
            }
        }
        example => example.run().await?,
    }

    Ok(())
}
