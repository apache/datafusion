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
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_cli::catalog::DynamicFileCatalog;
use datafusion_cli::{
    exec, print_format::PrintFormat, print_options::PrintOptions, DATAFUSION_CLI_VERSION,
};
use mimalloc::MiMalloc;
use std::env;
use std::path::Path;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
        short = 'b',
        long,
        help = "The batch size of each query, or use DataFusion default",
        validator(is_valid_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        short = 'c',
        long,
        multiple_values = true,
        help = "Execute the given command string(s), then exit"
    )]
    command: Vec<String>,

    #[clap(
        short = 'm',
        long,
        help = "The memory pool limitation, default to zero",
        validator(is_valid_memory_pool_size)
    )]
    memory_limit: Option<String>,

    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        multiple_values = true,
        help = "Run the provided files on startup instead of ~/.datafusionrc",
        validator(is_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "Specify the memory pool type 'greedy' or 'fair', default to 'greedy'",
        validator(is_valid_memory_pool_type)
    )]
    mem_pool_type: Option<String>,
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
        env::set_current_dir(p).unwrap();
    };

    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

    if let Some(batch_size) = args.batch_size {
        session_config = session_config.with_batch_size(batch_size);
    };

    let rn_config = RuntimeConfig::new();
    let rn_config =
        // set memory pool size
        if let Some(memory_limit) = args.memory_limit {
            let memory_limit = memory_limit[..memory_limit.len() - 1]
                .parse::<usize>()
                .unwrap();
            // set memory pool type
            if let Some(mem_pool_type) = args.mem_pool_type {
                match mem_pool_type.as_str() {
                    "greedy" => rn_config
                        .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_limit))),
                    "fair" => rn_config
                        .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit))),
                    _ => unreachable!(),
                }
            } else {
                rn_config
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_limit)))
            }
        } else {
            rn_config
        };

    let runtime_env = create_runtime_env(rn_config.clone())?;

    let mut ctx =
        SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));
    ctx.refresh_catalogs().await?;
    // install dynamic catalog provider that knows how to open files
    ctx.register_catalog_list(Arc::new(DynamicFileCatalog::new(
        ctx.state().catalog_list(),
        ctx.state_weak_ref(),
    )));

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    let commands = args.command;
    let files = args.file;
    let rc = match args.rc {
        Some(file) => file,
        None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".datafusionrc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };

    if commands.is_empty() && files.is_empty() {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &mut ctx, &print_options).await
        }
        // TODO maybe we can have thiserror for cli but for now let's keep it simple
        return exec::exec_from_repl(&mut ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !files.is_empty() {
        exec::exec_from_files(files, &mut ctx, &print_options).await;
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&mut ctx, &print_options, commands).await;
    }

    Ok(())
}

fn create_runtime_env(rn_config: RuntimeConfig) -> Result<RuntimeEnv> {
    RuntimeEnv::new(rn_config)
}

fn is_valid_file(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

fn is_valid_memory_pool_size(size: &str) -> Result<(), String> {
    if let Some(last_char) = size.chars().last() {
        if last_char != 'g' && last_char != 'G' {
            return Err(format!("Invalid memory pool size format '{}'", size));
        }
    }

    let size = &size[..size.len() - 1];
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid memory pool size '{}'", size)),
    }
}

fn is_valid_memory_pool_type(pool_type: &str) -> Result<(), String> {
    match pool_type {
        "greedy" | "fair" => Ok(()),
        _ => Err(format!(
            "Invalid memory pool type '{}', it should be 'fair' or 'greedy'",
            pool_type
        )),
    }
}
