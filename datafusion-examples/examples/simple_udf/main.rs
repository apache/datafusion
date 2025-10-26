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

//! # Simple UDF/UDAF/UDWF/UDFT Examples
//!
//! This example demonstrates simple user-defined functions in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example simple_udf -- [udf|udaf|udwf|udtf]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `udf` — user defined scalar function example
//! - `udaf` — user defined aggregate function example
//! - `udwf` — user defined window function example
//! - `udtf` — user defined table function example

mod udaf;
mod udf;
mod udtf;
mod udwf;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Udf,
    Udaf,
    Udwf,
    Udtf,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "udf" => Ok(Self::Udf),
            "udaf" => Ok(Self::Udaf),
            "udwf" => Ok(Self::Udwf),
            "udtf" => Ok(Self::Udtf),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example simple_udf -- [udf|udaf|udwf|udtf]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::Udf => udf::simple_udf().await?,
        ExampleKind::Udaf => udaf::simple_udaf().await?,
        ExampleKind::Udtf => udtf::simple_udtf().await?,
        ExampleKind::Udwf => udwf::simple_udwf().await?,
    }

    Ok(())
}
