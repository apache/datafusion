// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! # These are using data from Amazon S3 examples
//!
//! These examples demonstrate how to work with data from Amazon S3.
//!
//! ## Usage
//! ```bash
//! cargo run --example external_dependency -- [all|dataframe_to_s3|query_aws_s3]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `dataframe_to_s3`
//!   (file: dataframe_to_s3.rs, desc: Query DataFrames and write results to S3)
//!
//! - `query_aws_s3`
//!   (file: query_aws_s3.rs, desc: Query S3-backed data using object_store)

mod dataframe_to_s3;
mod query_aws_s3;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    DataframeToS3,
    QueryAwsS3,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "external_dependency";

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
            ExampleKind::DataframeToS3 => dataframe_to_s3::dataframe_to_s3().await?,
            ExampleKind::QueryAwsS3 => query_aws_s3::query_aws_s3().await?,
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
