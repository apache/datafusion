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
//! cargo run --example flight -- [client|server|sql_server]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `client` — run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol
//! - `server` — run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol
//! - `sql_server` — run DataFusion as a standalone process and execute SQL queries from JDBC clients

mod client;
mod server;
mod sql_server;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    Client,
    Server,
    SqlServer,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
            Self::SqlServer => "sql_server",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "client" => Ok(Self::Client),
            "server" => Ok(Self::Server),
            "sql_server" => Ok(Self::SqlServer),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL: [Self; 3] = [Self::Client, Self::Server, Self::SqlServer];

    const EXAMPLE_NAME: &str = "flight";

    fn variants() -> Vec<&'static str> {
        Self::ALL.iter().map(|x| x.as_ref()).collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        ExampleKind::Client => client::client().await?,
        ExampleKind::Server => server::server().await?,
        ExampleKind::SqlServer => sql_server::sql_server().await?,
    }

    Ok(())
}
