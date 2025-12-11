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
//! - `dataframe_to_s3` — run a query using a DataFrame against a parquet file from AWS S3 and writing back to AWS S3
//! - `query_aws_s3` — configure `object_store` and run a query against files stored in AWS S3

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
        .ok_or_else(|| DataFusionError::Execution(format!("Missing argument. {usage}")))?
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
