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

//! # These examples of data formats and I/O
//!
//! These examples demonstrate data formats and I/O.
//!
//! ## Usage
//! ```bash
//! cargo run --example data_io -- [all|catalog|json_shredding|parquet_adv_idx|parquet_emb_idx|parquet_enc_with_kms|parquet_enc|parquet_exec_visitor|parquet_idx|query_http_csv|remote_catalog]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `catalog`
//!   (file: catalog.rs, desc: Register tables into a custom catalog)
//!
//! - `json_shredding`
//!   (file: json_shredding.rs, desc: Implement filter rewriting for JSON shredding)
//!
//! - `parquet_adv_idx`
//!   (file: parquet_advanced_index.rs, desc: Create a secondary index across multiple parquet files)
//!
//! - `parquet_emb_idx`
//!   (file: parquet_embedded_index.rs, desc: Store a custom index inside Parquet files)
//!
//! - `parquet_enc`  
//!   (file: parquet_encrypted.rs, desc: Read & write encrypted Parquet files)
//!
//! - `parquet_enc_with_kms`
//!   (file: parquet_encrypted_with_kms.rs, desc: Encrypted Parquet I/O using a KMS-backed factory)
//!
//! - `parquet_exec_visitor`
//!   (file: parquet_exec_visitor.rs, desc: Extract statistics by visiting an ExecutionPlan)
//!
//! - `parquet_idx`
//!   (file: parquet_index.rs, desc: Create a secondary index)
//!
//! - `query_http_csv`
//!   (file: query_http_csv.rs, desc: Query CSV files via HTTP)
//!
//! - `remote_catalog`
//!   (file: remote_catalog.rs, desc: Interact with a remote catalog)

mod catalog;
mod json_shredding;
mod parquet_advanced_index;
mod parquet_embedded_index;
mod parquet_encrypted;
mod parquet_encrypted_with_kms;
mod parquet_exec_visitor;
mod parquet_index;
mod query_http_csv;
mod remote_catalog;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    Catalog,
    JsonShredding,
    ParquetAdvIdx,
    ParquetEmbIdx,
    ParquetEnc,
    ParquetEncWithKms,
    ParquetExecVisitor,
    ParquetIdx,
    QueryHttpCsv,
    RemoteCatalog,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "data_io";

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
            ExampleKind::Catalog => catalog::catalog().await?,
            ExampleKind::JsonShredding => json_shredding::json_shredding().await?,
            ExampleKind::ParquetAdvIdx => {
                parquet_advanced_index::parquet_advanced_index().await?
            }
            ExampleKind::ParquetEmbIdx => {
                parquet_embedded_index::parquet_embedded_index().await?
            }
            ExampleKind::ParquetEncWithKms => {
                parquet_encrypted_with_kms::parquet_encrypted_with_kms().await?
            }
            ExampleKind::ParquetEnc => parquet_encrypted::parquet_encrypted().await?,
            ExampleKind::ParquetExecVisitor => {
                parquet_exec_visitor::parquet_exec_visitor().await?
            }
            ExampleKind::ParquetIdx => parquet_index::parquet_index().await?,
            ExampleKind::QueryHttpCsv => query_http_csv::query_http_csv().await?,
            ExampleKind::RemoteCatalog => remote_catalog::remote_catalog().await?,
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
        .unwrap_or_else(|| ExampleKind::All.to_string())
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
