// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//!
//! - `date_time`
//!   (file: date_time.rs, desc: Examples of date-time related functions and queries)
//!
//! - `function_factory`
//!   (file: function_factory.rs, desc: Register `CREATE FUNCTION` handler to implement SQL macros)
//!
//! - `regexp`
//!   (file: regexp.rs, desc: Examples of using regular expression functions)

mod date_time;
mod function_factory;
mod regexp;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    DateTime,
    FunctionFactory,
    Regexp,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "builtin_functions";

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
            ExampleKind::DateTime => date_time::date_time().await?,
            ExampleKind::FunctionFactory => function_factory::function_factory().await?,
            ExampleKind::Regexp => regexp::regexp().await?,
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
