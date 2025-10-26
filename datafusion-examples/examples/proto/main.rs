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
//! cargo run --example proto -- [composed_extension_codec]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `composed_extension_codec` — example of using multiple extension codecs for serialization / deserialization

mod composed_extension_codec;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    ComposedExtensionCodec,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "composed_extension_codec" => Ok(Self::ComposedExtensionCodec),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example proto -- [composed_extension_codec]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::ComposedExtensionCodec => {
            composed_extension_codec::composed_extension_codec().await?
        }
    }

    Ok(())
}
