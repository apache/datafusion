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

//! # Relation Planner Examples
//!
//! These examples demonstrate how to use custom relation planners to extend
//! DataFusion's SQL syntax with custom table operators.
//!
//! ## Usage
//! ```bash
//! cargo run --example relation_planner -- [match_recognize|pivot_unpivot|table_sample]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `match_recognize` — MATCH_RECOGNIZE pattern matching on event streams
//! - `pivot_unpivot` — PIVOT and UNPIVOT operations for reshaping data
//! - `table_sample` — TABLESAMPLE clause for sampling rows from tables

mod match_recognize;
mod pivot_unpivot;
mod table_sample;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    MatchRecognize,
    PivotUnpivot,
    TableSample,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::MatchRecognize => "match_recognize",
            Self::PivotUnpivot => "pivot_unpivot",
            Self::TableSample => "table_sample",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "match_recognize" => Ok(Self::MatchRecognize),
            "pivot_unpivot" => Ok(Self::PivotUnpivot),
            "table_sample" => Ok(Self::TableSample),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL: [Self; 3] = [Self::MatchRecognize, Self::PivotUnpivot, Self::TableSample];

    const EXAMPLE_NAME: &str = "relation_planner";

    fn variants() -> Vec<&'static str> {
        Self::ALL.iter().map(|x| x.as_ref()).collect()
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
        ExampleKind::MatchRecognize => match_recognize::match_recognize().await?,
        ExampleKind::PivotUnpivot => pivot_unpivot::pivot_unpivot().await?,
        ExampleKind::TableSample => table_sample::table_sample().await?,
    }

    Ok(())
}
