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

use arrow::datatypes::DataType;
use clap::Parser;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::SessionContext;
use datafusion_cli::entry_point::{CliError, CliSession, CliArgs};
use datafusion_common::cast::as_string_array;
use mimalloc::MiMalloc;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct CustomArgs {
    #[command(flatten)]
    cli_args: CliArgs,
    #[clap(
        long,
        help = "Register the hello udf function",
        default_value = "true",
    )]
    register_hello: bool,
}

/// In this example we want to reuse the datafusion-cli binary argument, hen extend the `SessionContext` with custom udf.
///
/// 1. Declares a `hello`` udf function.
/// 2. Construct a `CliSession`
/// 3. Registers the udf function with the `SessionContext` so the user can input `select hello(1)` at the prompt.
/// 4. Runs the cli using [`dataframe_cli::CliSession::run`], printing any errors then exits.
#[tokio::main]
pub async fn main() -> Result<(), CliError> {
    let hello_udf = create_udf(
        "hello",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            assert_eq!(args.len(), 1);
            let args = ColumnarValue::values_to_arrays(args).unwrap();
            let vals = as_string_array(&args[0]).expect("cast failed");
            let array = vals
                .iter()
                .map(|v| v.map(|v| format!("hello {v}")))
                .collect::<StringArray>();
            Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
        }),
    );
    let args = CustomArgs::try_parse()?;
    let cli_session = CliSession::try_from_args(args.cli_args)?;
    let ctx: &SessionContext = cli_session.session_context();
    if args.register_hello {
        ctx.register_udf(hello_udf);
    }
    cli_session.run().await?;
    Ok(())
}
