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

#![allow(bare_trait_objects)]

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

use ballista::context::BallistaContext;
use ballista::prelude::BallistaConfig;
use clap::{crate_version, App, Arg};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion_cli::{
    print_format::{all_print_formats, PrintFormat},
    PrintOptions, DATAFUSION_CLI_VERSION,
};
use rustyline::Editor;

/// The CLI supports using a local DataFusion context or a distributed BallistaContext
enum Context {
    /// In-process execution with DataFusion
    Local(ExecutionContext),
    /// Distributed execution with Ballista
    Remote(BallistaContext),
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let matches = App::new("DataFusion")
        .version(crate_version!())
        .about(
            "DataFusion is an in-memory query engine that uses Apache Arrow \
             as the memory model. It supports executing SQL queries against CSV and \
             Parquet files as well as querying directly against in-memory data.",
        )
        .arg(
            Arg::with_name("data-path")
                .help("Path to your data, default to current directory")
                .short("p")
                .long("data-path")
                .validator(is_valid_data_dir)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch-size")
                .help("The batch size of each query, or use DataFusion default")
                .short("c")
                .long("batch-size")
                .validator(is_valid_batch_size)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("file")
                .help("Execute commands from file(s), then exit")
                .short("f")
                .long("file")
                .multiple(true)
                .validator(is_valid_file)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .long("format")
                .default_value("table")
                .possible_values(
                    &all_print_formats()
                        .iter()
                        .map(|format| format.to_string())
                        .collect::<Vec<_>>()
                        .iter()
                        .map(|i| i.as_str())
                        .collect::<Vec<_>>(),
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("host")
                .help("Ballista scheduler host")
                .long("host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .help("Ballista scheduler port")
                .long("port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("quiet")
                .help("Reduce printing other than the results and work quietly")
                .short("q")
                .long("quiet")
                .takes_value(false),
        )
        .get_matches();

    let quiet = matches.is_present("quiet");

    if !quiet {
        println!("DataFusion CLI v{}\n", DATAFUSION_CLI_VERSION);
    }

    let host = matches.value_of("host");
    let port = matches
        .value_of("port")
        .and_then(|port| port.parse::<u16>().ok());

    if let Some(path) = matches.value_of("data-path") {
        let p = Path::new(path);
        env::set_current_dir(&p).unwrap();
    };

    let mut execution_config = ExecutionConfig::new().with_information_schema(true);

    if let Some(batch_size) = matches
        .value_of("batch-size")
        .and_then(|size| size.parse::<usize>().ok())
    {
        execution_config = execution_config.with_batch_size(batch_size);
    };

    let ctx: Result<Context> = match (host, port) {
        (Some(h), Some(p)) => {
            let config: BallistaConfig = BallistaConfig::new()
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
            Ok(Context::Remote(BallistaContext::remote(h, p, &config)))
        }
        _ => Ok(Context::Local(ExecutionContext::with_config(
            execution_config.clone(),
        ))),
    };
    let mut ctx = ctx?;

    let format = matches
        .value_of("format")
        .expect("No format is specified")
        .parse::<PrintFormat>()
        .expect("Invalid format");

    let print_options = PrintOptions { format, quiet };

    if let Some(file_paths) = matches.values_of("file") {
        let files = file_paths
            .map(|file_path| File::open(file_path).unwrap())
            .collect::<Vec<_>>();
        for file in files {
            let mut reader = BufReader::new(file);
            exec_from_lines(&mut ctx, &mut reader, print_options.clone()).await;
        }
    } else {
        exec_from_repl(&mut ctx, print_options).await;
    }

    Ok(())
}

async fn exec_from_lines(
    ctx: &mut Context,
    reader: &mut BufReader<File>,
    print_options: PrintOptions,
) {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options.clone(), query).await {
                        Ok(_) => {}
                        Err(err) => println!("{:?}", err),
                    }
                    query = "".to_owned();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    if !query.is_empty() {
        match exec_and_print(ctx, print_options, query).await {
            Ok(_) => {}
            Err(err) => println!("{:?}", err),
        }
    }
}

async fn exec_from_repl(ctx: &mut Context, print_options: PrintOptions) {
    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        match rl.readline("> ") {
            Ok(ref line) if is_exit_command(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if line.starts_with("--") => {
                continue;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(ctx, print_options.clone(), query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push('\n');
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_valid_file(dir: String) -> std::result::Result<(), String> {
    if Path::new(&dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: String) -> std::result::Result<(), String> {
    if Path::new(&dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: String) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

async fn exec_and_print(
    ctx: &mut Context,
    print_options: PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();

    let df = match ctx {
        Context::Local(datafusion) => datafusion.sql(&sql)?,
        Context::Remote(ballista) => ballista.sql(&sql)?,
    };

    let results = df.collect().await?;
    print_options.print_batches(&results, now)?;

    Ok(())
}
