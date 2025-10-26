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
//! This example demonstrates how to work with data from Amazon S3.
//!
//! ## Usage
//! ```bash
//! cargo run --example external_dependency -- [dataframe_to_s3|query_aws_s3]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `dataframe_to_s3` — run a query using a DataFrame against a parquet file from AWS S3 and writing back to AWS S3
//! - `query_aws_s3` — configure `object_store` and run a query against files stored in AWS S3

mod dataframe_to_s3;
mod query_aws_s3;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    DataframeToS3,
    QueryAwsS3,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "dataframe_to_s3" => Ok(Self::DataframeToS3),
            "query_aws_s3" => Ok(Self::QueryAwsS3),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example external_dependency -- [dataframe_to_s3|query_aws_s3]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::DataframeToS3 => dataframe_to_s3::dataframe_to_s3().await?,
        ExampleKind::QueryAwsS3 => query_aws_s3::query_aws_s3().await?,
    }

    Ok(())
}
