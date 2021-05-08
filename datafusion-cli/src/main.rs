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

mod format;

use clap::{crate_version, App, Arg};
use datafusion::error::Result;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use format::print_format::PrintFormat;
use rustyline::Editor;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

#[tokio::main]
pub async fn main() {
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
                .help("Execute commands from file, then exit")
                .short("f")
                .long("file")
                .validator(is_valid_file)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format (possible values: table, csv)")
                .long("format")
                .default_value("table")
                .validator(is_valid_format)
                .takes_value(true),
        )
        .get_matches();

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

    let print_format = matches
        .value_of("format")
        .expect("No format is specified")
        .parse::<PrintFormat>()
        .expect("Invalid format");

    if let Some(file_path) = matches.value_of("file") {
        let file = File::open(file_path)
            .unwrap_or_else(|err| panic!("cannot open file '{}': {}", file_path, err));
        let mut reader = BufReader::new(file);
        exec_from_lines(&mut reader, execution_config, print_format).await;
    } else {
        exec_from_repl(execution_config, print_format).await;
    }
}

async fn exec_from_lines(
    reader: &mut BufReader<File>,
    execution_config: ExecutionConfig,
    print_format: PrintFormat,
) {
    let mut ctx = ExecutionContext::with_config(execution_config);
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(&mut ctx, print_format.clone(), query).await {
                        Ok(_) => {}
                        Err(err) => println!("{:?}", err),
                    }
                    query = "".to_owned();
                } else {
                    query.push(' ');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    if !query.is_empty() {
        match exec_and_print(&mut ctx, print_format, query).await {
            Ok(_) => {}
            Err(err) => println!("{:?}", err),
        }
    }
}

async fn exec_from_repl(execution_config: ExecutionConfig, print_format: PrintFormat) {
    let mut ctx = ExecutionContext::with_config(execution_config);

    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        match rl.readline("> ") {
            Ok(ref line) if is_exit_command(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(&mut ctx, print_format.clone(), query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push(' ');
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_valid_format(format: String) -> std::result::Result<(), String> {
    match format.to_lowercase().as_str() {
        "csv" => Ok(()),
        "table" => Ok(()),
        _ => Err(format!("Format '{}' not supported", format)),
    }
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
    ctx: &mut ExecutionContext,
    print_format: PrintFormat,
    sql: String,
) -> Result<()> {
    let now = Instant::now();

    let df = ctx.sql(&sql)?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!(
            "0 rows in set. Query took {} seconds.",
            now.elapsed().as_secs()
        );
        return Ok(());
    }

    print_format.print_batches(&results)?;

    let row_count: usize = results.iter().map(|b| b.num_rows()).sum();

    if row_count > 1 {
        println!(
            "{} row in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    } else {
        println!(
            "{} rows in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    }

    Ok(())
}
