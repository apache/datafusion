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
    let owned_print_formats = all_print_formats()
        .iter()
        .map(|format| format.to_string())
        .collect::<Vec<_>>();
    let all_print_formats = owned_print_formats
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();
    let mut app = App::new("DataFusion")
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
                .possible_values(&all_print_formats)
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
        );
    if cfg!(feature = "experimental-tokomak") {
        app = app.arg(
            Arg::with_name("tokomak")
                .help("Enables the experimental tokomak optimizer")
                .short("t")
                .long("tokomak")
                .takes_value(false),
        );
    }
    let matches = app.get_matches();

    let quiet = matches.is_present("quiet");
    let tokomak = matches.is_present("tokomak");

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

    let mut execution_config = exec_context(tokomak);

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

#[cfg(feature = "experimental-tokomak")]
fn get_tokomak_optimizers() -> Vec<
    std::sync::Arc<
        dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync + 'static,
    >,
> {
    use datafusion::optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate,
        eliminate_limit::EliminateLimit, filter_push_down::FilterPushDown,
        limit_push_down::LimitPushDown, projection_push_down::ProjectionPushDown,
        simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
    };
    use std::sync::Arc;
    let mut settings = tokomak::RunnerSettings::new();
    settings
        .with_iter_limit(get_tokomak_iter_limit())
        .with_node_limit(get_tokomak_node_limit())
        .with_time_limit(std::time::Duration::from_secs_f64(get_tokomak_opt_seconds()));

    let tokomak_optimizer =
        tokomak::Tokomak::with_builtin_rules(settings, tokomak::ALL_RULES);
    vec![
        Arc::new(SimplifyExpressions::new()),
        Arc::new(tokomak_optimizer),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ProjectionPushDown::new()),
        Arc::new(FilterPushDown::new()),
        Arc::new(LimitPushDown::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
    ]
}

#[cfg(feature = "experimental-tokomak")]
fn get_tokomak_opt_seconds() -> f64 {
    const DEFAULT_TIME: f64 = 0.5;
    let str_time =
        std::env::var("TOKOMAK_OPT_TIME").unwrap_or_else(|_| format!("{}", DEFAULT_TIME));
    let opt_seconds: f64 = str_time.parse().unwrap_or(DEFAULT_TIME);
    opt_seconds
}
#[cfg(feature = "experimental-tokomak")]
fn get_tokomak_node_limit() -> usize {
    const DEFAULT_NODE_LIMIT: usize = 1_000_000;
    let str_lim = std::env::var("TOKOMAK_NODE_LIMIT")
        .unwrap_or_else(|_| format!("{}", DEFAULT_NODE_LIMIT));
    let lim: usize = str_lim.parse().unwrap_or(DEFAULT_NODE_LIMIT);
    lim
}
#[cfg(feature = "experimental-tokomak")]
fn get_tokomak_iter_limit() -> usize {
    const DEFAULT_ITER_LIMIT: usize = 1000;
    let str_lim = std::env::var("TOKOMAK_ITER_LIMIT")
        .unwrap_or_else(|_| format!("{}", DEFAULT_ITER_LIMIT));
    let lim: usize = str_lim.parse().unwrap_or(DEFAULT_ITER_LIMIT);
    lim
}

#[cfg(feature = "experimental-tokomak")]
fn exec_context(tokomak: bool) -> ExecutionConfig {
    let mut execution_config = ExecutionConfig::new().with_information_schema(true);
    if tokomak {
        execution_config =
            execution_config.with_optimizer_rules(get_tokomak_optimizers());
    }
    execution_config
}

#[cfg(not(feature = "experimental-tokomak"))]
fn exec_context(_tokomak: bool) -> ExecutionConfig {
    ExecutionConfig::new().with_information_schema(true)
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
