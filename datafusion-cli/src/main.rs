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

use clap::Parser;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionConfig;
use datafusion_cli::{
    context::Context, exec, print_format::PrintFormat, print_options::PrintOptions,
    DATAFUSION_CLI_VERSION,
};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'c',
        long,
        help = "The batch size of each query, or use DataFusion default",
        validator(is_valid_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    file: Vec<String>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(long, help = "Ballista scheduler host")]
    host: Option<String>,

    #[clap(long, help = "Ballista scheduler port")]
    port: Option<u16>,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("DataFusion CLI v{}", DATAFUSION_CLI_VERSION);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(&p).unwrap();
    };

    let mut execution_config = ExecutionConfig::new().with_information_schema(true);

    if let Some(batch_size) = args.batch_size {
        execution_config = execution_config.with_batch_size(batch_size);
    };

    let mut ctx: Context = match (args.host, args.port) {
        (Some(ref h), Some(p)) => Context::new_remote(h, p)?,
        _ => Context::new_local(&execution_config),
    };

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    let files = args.file;
    if !files.is_empty() {
        let files = files
            .into_iter()
            .map(|file_path| File::open(file_path).unwrap())
            .collect::<Vec<_>>();
        for file in files {
            let mut reader = BufReader::new(file);
            exec::exec_from_lines(&mut ctx, &mut reader, &print_options).await;
        }
    } else {
        exec::exec_from_repl(&mut ctx, &mut print_options).await;
    }

    Ok(())
}

fn is_valid_file(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}
