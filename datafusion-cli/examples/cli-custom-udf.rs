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
use clap::{CommandFactory, FromArgMatches, Parser};
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::SessionContext;
use datafusion_cli::entry_point::{CliArgs, CliError, CliSession};
use datafusion_common::cast::as_string_array;
use mimalloc::MiMalloc;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct CustomArgs {
    // Shown is one way to avoid clashing arguments between datafusion and the custom command.
    #[arg(
        long,
        allow_hyphen_values = true,
        num_args = 0..,
        value_terminator = "--datafusion-cli-end",
        value_name = "DATAFUSION_ARGS",
        help = "Arguments between `--datafusion-cli-start` and `--datafusion-cli-end`"
    )]
    datafusion_cli_start: Option<Vec<String>>,

    #[clap(long, help = "Register the hello udf function", action = clap::ArgAction::Set, default_value_t = true)]
    register_hello: bool,
}

/// In this example we want to reuse the datafusion-cli binary argument, then extend the `SessionContext` with custom udf.
///
/// 1. Declares a `hello`` udf function.
/// 2. Handle argument parsing.
/// 3. Construct a `CliSession`
/// 4. Registers the udf function with the `SessionContext` so the user can input `select hello(1)` at the prompt.
/// 5. Runs the cli using [`dataframe_cli::CliSession::run`], printing any errors then exits.
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

    // Append the datafusion help text to ours
    let cli_args_help = CliArgs::command()
        .name("DATAFUSION_ARGS")
        .no_binary_name(true)
        .about("")
        .long_about("")
        .override_usage(
            "--datafusion-cli-start [<DATAFUSION_ARGS>...] --datafusion-cli-end",
        )
        .render_long_help();
    let matches = CustomArgs::command()
        .after_help(cli_args_help)
        .get_matches();
    let args = CustomArgs::from_arg_matches(&matches)?;
    // Pass the executable name along with the datafusion cli args.
    let mut cli_args_input = vec![std::env::args().next().unwrap()];
    if let Some(datafusion_args) = args.datafusion_cli_start {
        cli_args_input.extend(datafusion_args);
    }
    let cli_args = CliArgs::try_parse_from(cli_args_input)?;
    let cli_session = CliSession::builder().with_args(cli_args).build()?;
    let ctx: &SessionContext = cli_session.session_context();
    if args.register_hello {
        ctx.register_udf(hello_udf);
    }
    cli_session.run().await?;
    Ok(())
}
