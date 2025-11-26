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

//! # Examples demonstrating DataFusion's plan serialization via the `datafusion-proto` crate
//!
//! These examples show how to use multiple extension codecs for serialization / deserialization.
//!
//! ## Usage
//! ```bash
//! cargo run --example proto -- [all|composed_extension_codec]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `composed_extension_codec` — example of using multiple extension codecs for serialization / deserialization

mod composed_extension_codec;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    ComposedExtensionCodec,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::ComposedExtensionCodec => "composed_extension_codec",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "composed_extension_codec" => Ok(Self::ComposedExtensionCodec),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 2] = [Self::All, Self::ComposedExtensionCodec];

    const RUNNABLE_VARIANTS: [Self; 1] = [Self::ComposedExtensionCodec];

    const EXAMPLE_NAME: &str = "proto";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::ComposedExtensionCodec => {
                composed_extension_codec::composed_extension_codec().await?
            }
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
