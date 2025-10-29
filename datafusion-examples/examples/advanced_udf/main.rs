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

//! # Advanced UDF/UDAF/UDWF/Asynchronous UDF Examples
//!
//! This example demonstrates advanced user-defined functions in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example advanced_udf -- [udf|udaf|udwf|async_udf]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `udf` — user defined scalar function example
//! - `udaf` — user defined aggregate function example
//! - `udwf` — user defined window function example
//! - `async_udf` — asynchronous user defined function example

mod async_udf;
mod udaf;
mod udf;
mod udwf;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Udf,
    Udaf,
    Udwf,
    AsyncUdf,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "udf" => Ok(Self::Udf),
            "udaf" => Ok(Self::Udaf),
            "udwf" => Ok(Self::Udwf),
            "async_udf" => Ok(Self::AsyncUdf),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example advanced_udf -- [udf|udaf|udwf|async_udf]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::Udf => udf::advanced_udf().await?,
        ExampleKind::Udaf => udaf::advanced_udaf().await?,
        ExampleKind::Udwf => udwf::advanced_udwf().await?,
        ExampleKind::AsyncUdf => async_udf::async_udf().await?,
    }

    Ok(())
}
