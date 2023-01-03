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
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_cli::object_storage::DatafusionCliObjectStoreProvider;
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

    let runtime_env = create_runtime_env()?;
    let mut ctx =
        SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));
    ctx.refresh_catalogs().await?;

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

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

    if !files.is_empty() {
        exec::exec_from_files(files, &mut ctx, &print_options).await;
        Ok(())
    } else {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &mut ctx, &print_options).await
        }
        // TODO maybe we can have thiserror for cli but for now let's keep it simple
        exec::exec_from_repl(&mut ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

fn create_runtime_env() -> Result<RuntimeEnv> {
    let object_store_provider = DatafusionCliObjectStoreProvider {};
    let object_store_registry =
        ObjectStoreRegistry::new_with_provider(Some(Arc::new(object_store_provider)));
    let rn_config =
        RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry));
    RuntimeEnv::new(rn_config)
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
