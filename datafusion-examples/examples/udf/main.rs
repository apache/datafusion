// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! # User-Defined Functions Examples
//!
//! These examples demonstrate user-defined functions in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example udf -- [all|adv_udaf|adv_udf|adv_udwf|async_udf|udaf|udf|udtf|udwf|table_list_udtf]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `adv_udaf`
//!   (file: advanced_udaf.rs, desc: Advanced User Defined Aggregate Function (UDAF))
//!
//! - `adv_udf`
//!   (file: advanced_udf.rs, desc: Advanced User Defined Scalar Function (UDF))
//!
//! - `adv_udwf`
//!   (file: advanced_udwf.rs, desc: Advanced User Defined Window Function (UDWF))
//!
//! - `async_udf`
//!   (file: async_udf.rs, desc: Asynchronous User Defined Scalar Function)
//!
//! - `struct_udaf`
//!   (file: struct_returning_udaf.rs, desc: Struct-returning UDAF with window metadata)
//!
//! - `udaf`
//!   (file: simple_udaf.rs, desc: Simple UDAF example)
//!
//! - `udf`
//!   (file: simple_udf.rs, desc: Simple UDF example)
//!
//! - `udtf`
//!   (file: simple_udtf.rs, desc: Simple UDTF example)
//!
//! - `udwf`
//!   (file: simple_udwf.rs, desc: Simple UDWF example)
//!
//! - `table_list_udtf`
//!   (file: table_list_udtf.rs, desc: Session-aware UDTF table list example)

mod advanced_udaf;
mod advanced_udf;
mod advanced_udwf;
mod async_udf;
mod simple_udaf;
mod simple_udf;
mod simple_udtf;
mod simple_udwf;
mod struct_returning_udaf;
mod table_list_udtf;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    AdvUdaf,
    AdvUdf,
    AdvUdwf,
    AsyncUdf,
    StructUdaf,
    Udf,
    Udaf,
    Udwf,
    Udtf,
    TableListUdtf,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "udf";

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
            ExampleKind::AdvUdaf => advanced_udaf::advanced_udaf().await?,
            ExampleKind::AdvUdf => advanced_udf::advanced_udf().await?,
            ExampleKind::AdvUdwf => advanced_udwf::advanced_udwf().await?,
            ExampleKind::AsyncUdf => async_udf::async_udf().await?,
            ExampleKind::StructUdaf => {
                struct_returning_udaf::struct_returning_udaf().await?
            }
            ExampleKind::Udaf => simple_udaf::simple_udaf().await?,
            ExampleKind::Udf => simple_udf::simple_udf().await?,
            ExampleKind::Udtf => simple_udtf::simple_udtf().await?,
            ExampleKind::Udwf => simple_udwf::simple_udwf().await?,
            ExampleKind::TableListUdtf => table_list_udtf::table_list_udtf().await?,
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
