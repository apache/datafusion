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

use clap::{crate_version, App, Arg};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionConfig;
use datafusion_cli::{
    context::Context,
    exec,
    print_format::{all_print_formats, PrintFormat},
    print_options::PrintOptions,
    DATAFUSION_CLI_VERSION,
};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

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

    let mut ctx: Context = match (host, port) {
        (Some(h), Some(p)) => Context::new_remote(h, p)?,
        _ => Context::new_local(&execution_config),
    };

    let format = matches
        .value_of("format")
        .expect("No format is specified")
        .parse::<PrintFormat>()
        .expect("Invalid format");

    let mut print_options = PrintOptions { format, quiet };

    if let Some(file_paths) = matches.values_of("file") {
        let files = file_paths
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
