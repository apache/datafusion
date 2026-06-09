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
//!
//! - `client`
//!   (file: client.rs, desc: Execute SQL queries via Arrow Flight protocol)
//!
//! - `server`
//!   (file: server.rs, desc: Run DataFusion server accepting FlightSQL/JDBC queries)
//!
//! - `sql_server`
//!   (file: sql_server.rs, desc: Standalone SQL server for JDBC clients)
//!
//! ## OomGuard
//!
//! `sql_server` wraps the global allocator with [`oom_guard::OomGuard`]
//! to convert process-wide OOM kills into per-query panics. The guard
//! installs an `alloc_error_hook`, which is currently an unstable Rust
//! API (`#![feature(alloc_error_hook)]`); the workspace `.cargo/config.toml`
//! sets `RUSTC_BOOTSTRAP=1` so this builds on stable.
#![feature(alloc_error_hook)]

mod client;
mod oom_guard;
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the alloc-error hook BEFORE building the runtime so even
    // early allocations on the main thread benefit. When OomGuard returns
    // NULL on overdraft, the Rust runtime invokes this hook from a clean
    // safe-Rust frame *outside* the `unsafe impl GlobalAlloc` block.
    std::alloc::set_alloc_error_hook(|_layout| {
        if oom_guard::take_kill_pending() {
            let balance = oom_guard::balance();
            std::panic::panic_any(oom_guard::OomGuardPanic { balance });
        }
        std::process::abort();
    });

    // Install a global panic hook to log `OomGuardPanic` distinctly.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Some(g) = info.payload().downcast_ref::<oom_guard::OomGuardPanic>() {
            log::error!(
                "OomGuard panic on thread {:?}: balance={} bytes",
                std::thread::current().name().unwrap_or("?"),
                g.balance,
            );
        } else {
            default_hook(info);
        }
    }));

    // Manual runtime construction so we can `on_thread_start` to stamp
    // worker threads as eligible for the OomGuard overdraft panic.
    // Unstamped threads (control plane, the poll task itself) still debit
    // the bank but are exempt from the kill.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            oom_guard::stamp_current_thread();
        })
        .build()?;

    runtime.block_on(async {
        let usage = format!(
            "Usage: cargo run --example {} -- [{}]",
            ExampleKind::EXAMPLE_NAME,
            ExampleKind::VARIANTS.join("|")
        );

        let example: ExampleKind = std::env::args()
            .nth(1)
            .unwrap_or_else(|| ExampleKind::All.to_string())
            .parse()
            .map_err(|_| {
                DataFusionError::Execution(format!("Unknown example. {usage}"))
            })?;

        example.run().await
    })
}
