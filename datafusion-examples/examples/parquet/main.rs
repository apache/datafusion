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

//! # Parquet Examples
//!
//! This example demonstrates Parquet file in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example parquet -- [advanced_index|embedded_index|encrypted_with_kms|encrypted|exec_visitor|index]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `advanced_index` — create a detailed secondary index that covers the contents of several parquet files
//! - `embedded_index` — store a custom index inside a Parquet file and use it to speed up queries
//! - `encrypted_with_kms` — read and write encrypted Parquet files using an encryption factory
//! - `encrypted` — read and write encrypted Parquet files using DataFusion
//! - `exec_visitor` — extract statistics by visiting an ExecutionPlan after execution
//! - `index` — create an secondary index over several parquet files and use it to speed up queries

mod advanced_index;
mod embedded_index;
mod encrypted;
mod encrypted_with_kms;
mod exec_visitor;
mod index;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    AdvancedIndex,
    EmbeddedIndex,
    EncryptedWithKms,
    Encrypted,
    ExecVisitor,
    Index,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "advanced_index" => Ok(Self::AdvancedIndex),
            "embedded_index" => Ok(Self::EmbeddedIndex),
            "encrypted_with_kms" => Ok(Self::EncryptedWithKms),
            "encrypted" => Ok(Self::Encrypted),
            "exec_visitor" => Ok(Self::ExecVisitor),
            "index" => Ok(Self::Index),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example parquet -- [advanced_index|embedded_index|encrypted_with_kms|encrypted|exec_visitor|index]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::AdvancedIndex => advanced_index::advanced_index().await?,
        ExampleKind::EmbeddedIndex => embedded_index::embedded_index().await?,
        ExampleKind::EncryptedWithKms => encrypted_with_kms::encrypted_with_kms().await?,
        ExampleKind::Encrypted => encrypted::encrypted().await?,
        ExampleKind::ExecVisitor => exec_visitor::exec_visitor().await?,
        ExampleKind::Index => index::index().await?,
    }

    Ok(())
}
