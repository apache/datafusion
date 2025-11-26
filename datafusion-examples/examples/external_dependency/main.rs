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

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    DataframeToS3,
    QueryAwsS3,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::DataframeToS3 => "dataframe_to_s3",
            Self::QueryAwsS3 => "query_aws_s3",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "dataframe_to_s3" => Ok(Self::DataframeToS3),
            "query_aws_s3" => Ok(Self::QueryAwsS3),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 3] = [Self::All, Self::DataframeToS3, Self::QueryAwsS3];

    const RUNNABLE_VARIANTS: [Self; 2] = [Self::DataframeToS3, Self::QueryAwsS3];

    const EXAMPLE_NAME: &str = "external_dependency";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::DataframeToS3 => dataframe_to_s3::dataframe_to_s3().await?,
            ExampleKind::QueryAwsS3 => query_aws_s3::query_aws_s3().await?,
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
