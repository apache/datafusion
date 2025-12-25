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

//! # User-Defined Functions Examples
//!
//! These examples demonstrate user-defined functions in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example udf -- [all|adv_udaf|adv_udf|adv_udwf|async_udf|udaf|udf|udtf|udwf]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `adv_udaf` — user defined aggregate function example
//! - `adv_udf` — user defined scalar function example
//! - `adv_udwf` — user defined window function example
//! - `async_udf` — asynchronous user defined function example
//! - `udaf` — simple user defined aggregate function example
//! - `udf` — simple user defined scalar function example
//! - `udtf` — simple user defined table function example
//! - `udwf` — simple user defined window function example

mod advanced_udaf;
mod advanced_udf;
mod advanced_udwf;
mod async_udf;
mod simple_udaf;
mod simple_udf;
mod simple_udtf;
mod simple_udwf;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    AdvUdaf,
    AdvUdf,
    AdvUdwf,
    AsyncUdf,
    Udf,
    Udaf,
    Udwf,
    Udtf,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "udf";

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
            ExampleKind::AdvUdaf => advanced_udaf::advanced_udaf().await?,
            ExampleKind::AdvUdf => advanced_udf::advanced_udf().await?,
            ExampleKind::AdvUdwf => advanced_udwf::advanced_udwf().await?,
            ExampleKind::AsyncUdf => async_udf::async_udf().await?,
            ExampleKind::Udaf => simple_udaf::simple_udaf().await?,
            ExampleKind::Udf => simple_udf::simple_udf().await?,
            ExampleKind::Udtf => simple_udtf::simple_udtf().await?,
            ExampleKind::Udwf => simple_udwf::simple_udwf().await?,
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
        .ok_or_else(|| DataFusionError::Execution(format!("Missing argument. {usage}")))?
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
