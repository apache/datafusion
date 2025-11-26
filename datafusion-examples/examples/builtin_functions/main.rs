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

//! # These are miscellaneous function-related examples
//!
//! These examples demonstrate miscellaneous function-related features.
//!
//! ## Usage
//! ```bash
//! cargo run --example builtin_functions -- [all|date_time|function_factory|regexp]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `date_time` — examples of date-time related functions and queries
//! - `function_factory` — register `CREATE FUNCTION` handler to implement SQL macros
//! - `regexp` — examples of using regular expression functions

mod date_time;
mod function_factory;
mod regexp;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    DateTime,
    FunctionFactory,
    Regexp,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::DateTime => "date_time",
            Self::FunctionFactory => "function_factory",
            Self::Regexp => "regexp",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "date_time" => Ok(Self::DateTime),
            "function_factory" => Ok(Self::FunctionFactory),
            "regexp" => Ok(Self::Regexp),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 4] = [
        Self::All,
        Self::DateTime,
        Self::FunctionFactory,
        Self::Regexp,
    ];

    const RUNNABLE_VARIANTS: [Self; 3] =
        [Self::DateTime, Self::FunctionFactory, Self::Regexp];

    const EXAMPLE_NAME: &str = "builtin_functions";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::DateTime => date_time::date_time().await?,
            ExampleKind::FunctionFactory => function_factory::function_factory().await?,
            ExampleKind::Regexp => regexp::regexp().await?,
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
