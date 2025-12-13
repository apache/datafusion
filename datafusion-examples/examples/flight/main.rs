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

//! # Arrow Flight Examples
//!
//! These examples demonstrate Arrow Flight usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example flight -- [all|client|server|sql_server]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!   Note: The Flight server must be started in a separate process
//!   before running the `client` example. Therefore, running `all` will
//!   not produce a full server+client workflow automatically.
//! - `client` — run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol
//! - `server` — run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol
//! - `sql_server` — run DataFusion as a standalone process and execute SQL queries from JDBC clients

mod client;
mod server;
mod sql_server;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

/// The `all` option cannot run all examples end-to-end because the
/// `server` example must run in a separate process before the `client`
/// example can connect.  
/// Therefore, `all` only iterates over individually runnable examples.
#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    Client,
    Server,
    SqlServer,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "flight";

    fn runnable() -> impl Iterator<Item = ExampleKind> {
        ExampleKind::iter().filter(|v| !matches!(v, ExampleKind::All))
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::runnable() {
                    println!("Running example: {example}");
                    Box::pin(example.run()).await?;
                }
            }
            ExampleKind::Client => client::client().await?,
            ExampleKind::Server => server::server().await?,
            ExampleKind::SqlServer => sql_server::sql_server().await?,
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
