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

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

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

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::AdvUdaf => "adv_udaf",
            Self::AdvUdf => "adv_udf",
            Self::AdvUdwf => "adv_udwf",
            Self::AsyncUdf => "async_udf",
            Self::Udf => "udf",
            Self::Udaf => "udaf",
            Self::Udwf => "udwt",
            Self::Udtf => "udtf",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "adv_udaf" => Ok(Self::AdvUdaf),
            "adv_udf" => Ok(Self::AdvUdf),
            "adv_udwf" => Ok(Self::AdvUdwf),
            "async_udf" => Ok(Self::AsyncUdf),
            "udaf" => Ok(Self::Udaf),
            "udf" => Ok(Self::Udf),
            "udtf" => Ok(Self::Udtf),
            "udwf" => Ok(Self::Udwf),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 9] = [
        Self::All,
        Self::AdvUdaf,
        Self::AdvUdf,
        Self::AdvUdwf,
        Self::AsyncUdf,
        Self::Udaf,
        Self::Udf,
        Self::Udtf,
        Self::Udwf,
    ];

    const RUNNABLE_VARIANTS: [Self; 8] = [
        Self::AdvUdaf,
        Self::AdvUdf,
        Self::AdvUdwf,
        Self::AsyncUdf,
        Self::Udaf,
        Self::Udf,
        Self::Udtf,
        Self::Udwf,
    ];

    const EXAMPLE_NAME: &str = "udf";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::AdvUdaf => advanced_udaf::advanced_udaf().await?,
            ExampleKind::AdvUdf => advanced_udf::advanced_udf().await?,
            ExampleKind::AdvUdwf => advanced_udwf::advanced_udwf().await?,
            ExampleKind::AsyncUdf => async_udf::async_udf().await?,
            ExampleKind::Udaf => simple_udaf::simple_udaf().await?,
            ExampleKind::Udf => simple_udf::simple_udf().await?,
            ExampleKind::Udtf => simple_udtf::simple_udtf().await?,
            ExampleKind::Udwf => simple_udwf::simple_udwf().await?,
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
