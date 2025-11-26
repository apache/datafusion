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
//! - `catalog` — register the table into a custom catalog
//! - `json_shredding` — shows how to implement custom filter rewriting for JSON shredding
//! - `parquet_adv_idx` — create a detailed secondary index that covers the contents of several parquet files
//! - `parquet_emb_idx` — store a custom index inside a Parquet file and use it to speed up queries
//! - `parquet_enc_with_kms` — read and write encrypted Parquet files using an encryption factory
//! - `parquet_enc` — read and write encrypted Parquet files using DataFusion
//! - `parquet_exec_visitor` — extract statistics by visiting an ExecutionPlan after execution
//! - `parquet_idx` — create an secondary index over several parquet files and use it to speed up queries
//! - `query_http_csv` — configure `object_store` and run a query against files via HTTP
//! - `remote_catalog` — interfacing with a remote catalog (e.g. over a network)

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

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

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

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::Catalog => "catalog",
            Self::JsonShredding => "json_shredding",
            Self::ParquetAdvIdx => "parquet_adv_idx",
            Self::ParquetEmbIdx => "parquet_emb_idx",
            Self::ParquetEnc => "parquet_enc",
            Self::ParquetEncWithKms => "parquet_enc_with_kms",
            Self::ParquetExecVisitor => "parquet_exec_visitor",
            Self::ParquetIdx => "parquet_idx",
            Self::QueryHttpCsv => "query_http_csv",
            Self::RemoteCatalog => "remote_catalog",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "catalog" => Ok(Self::Catalog),
            "json_shredding" => Ok(Self::JsonShredding),
            "parquet_adv_idx" => Ok(Self::ParquetAdvIdx),
            "parquet_emb_idx" => Ok(Self::ParquetEmbIdx),
            "parquet_enc" => Ok(Self::ParquetEnc),
            "parquet_enc_with_kms" => Ok(Self::ParquetEncWithKms),
            "parquet_exec_visitor" => Ok(Self::ParquetExecVisitor),
            "parquet_idx" => Ok(Self::ParquetIdx),
            "query_http_csv" => Ok(Self::QueryHttpCsv),
            "remote_catalog" => Ok(Self::RemoteCatalog),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 11] = [
        Self::All,
        Self::Catalog,
        Self::JsonShredding,
        Self::ParquetAdvIdx,
        Self::ParquetEmbIdx,
        Self::ParquetEnc,
        Self::ParquetEncWithKms,
        Self::ParquetExecVisitor,
        Self::ParquetIdx,
        Self::QueryHttpCsv,
        Self::RemoteCatalog,
    ];

    const RUNNABLE_VARIANTS: [Self; 10] = [
        Self::Catalog,
        Self::JsonShredding,
        Self::ParquetAdvIdx,
        Self::ParquetEmbIdx,
        Self::ParquetEnc,
        Self::ParquetEncWithKms,
        Self::ParquetExecVisitor,
        Self::ParquetIdx,
        Self::QueryHttpCsv,
        Self::RemoteCatalog,
    ];

    const EXAMPLE_NAME: &str = "data_io";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
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
