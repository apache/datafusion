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

//! Execution functions

use crate::{
    command::{Command, OutputFormat},
    helper::CliHelper,
    print_options::PrintOptions,
};
use datafusion::{
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    logical_expr::CreateExternalTable,
};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;
use std::{fs::File, sync::Arc};
use url::Url;

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
    ctx: &mut SessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
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
                    match exec_and_print(ctx, print_options, query).await {
                        Ok(_) => {}
                        Err(err) => println!("{err}"),
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
            Err(err) => println!("{err}"),
        }
    }
}

pub async fn exec_from_files(
    files: Vec<String>,
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
) {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await;
    }
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(
    ctx: &mut SessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::<CliHelper>::new()?;
    rl.set_helper(Some(CliHelper::default()));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

    loop {
        match rl.readline("❯ ") {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end());
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) =
                                        command.execute(&mut print_options).await
                                    {
                                        eprintln!("{e}")
                                    }
                                } else {
                                    eprintln!(
                                        "'\\{}' is not a valid command",
                                        &line[1..]
                                    );
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        }
                        _ => {
                            if let Err(e) = cmd.execute(ctx, &mut print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }
            Ok(line) => {
                rl.add_history_entry(line.trim_end());
                match exec_and_print(ctx, &print_options, line).await {
                    Ok(_) => {}
                    Err(err) => eprintln!("{err}"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {:?}", err);
                break;
            }
        }
    }

    rl.save_history(".history")
}

async fn exec_and_print(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();

    let plan = ctx.state().create_logical_plan(&sql).await?;
    let df = match &plan {
        LogicalPlan::CreateExternalTable(cmd) => {
            create_external_table(&ctx, cmd)?;
            ctx.execute_logical_plan(plan).await?
        }
        _ => ctx.execute_logical_plan(plan).await?,
    };

    let results = df.collect().await?;
    print_options.print_batches(&results, now)?;

    Ok(())
}

fn create_external_table(ctx: &SessionContext, cmd: &CreateExternalTable) -> Result<()> {
    let table_path = ListingTableUrl::parse(&cmd.location)?;
    let scheme = table_path.scheme();
    let url: &Url = table_path.as_ref();

    // registering the cloud object store dynamically using cmd.options
    match scheme {
        "s3" => {
            let bucket_name = get_bucket_name(url)?;
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

            if let (Some(access_key_id), Some(secret_access_key)) = (
                cmd.options.get("access_key_id"),
                cmd.options.get("secret_access_key"),
            ) {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
            }

            if let Some(session_token) = cmd.options.get("session_token") {
                builder = builder.with_token(session_token);
            }

            if let Some(region) = cmd.options.get("region") {
                builder = builder.with_region(region);
            }

            let store = Arc::new(builder.build()?);

            ctx.runtime_env().register_object_store(url, store);
        }
        "oss" => {
            let bucket_name = get_bucket_name(url)?;
            let mut builder = AmazonS3Builder::from_env()
                .with_virtual_hosted_style_request(true)
                .with_bucket_name(bucket_name)
                // oss don't care about the "region" field
                .with_region("do_not_care");

            if let (Some(access_key_id), Some(secret_access_key)) = (
                cmd.options.get("access_key_id"),
                cmd.options.get("secret_access_key"),
            ) {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
            }

            if let Some(endpoint) = cmd.options.get("endpoint") {
                builder = builder.with_endpoint(endpoint);
            }

            let store = Arc::new(builder.build()?);

            ctx.runtime_env().register_object_store(url, store);
        }
        "gs" | "gcs" => {
            let bucket_name = get_bucket_name(url)?;
            let mut builder =
                GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);

            if let Some(service_account_path) = cmd.options.get("service_account_path") {
                builder = builder.with_service_account_path(service_account_path);
            }

            if let Some(service_account_key) = cmd.options.get("service_account_key") {
                builder = builder.with_service_account_key(service_account_key);
            }

            if let Some(application_credentials_path) =
                cmd.options.get("application_credentials_path")
            {
                builder =
                    builder.with_application_credentials(application_credentials_path);
            }

            let store = Arc::new(builder.build()?);

            ctx.runtime_env().register_object_store(url, store);
        }
        _ => {
            // for other types, try to get from the object_store_registry
            let store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Unsupported object store scheme: {}",
                        scheme
                    ))
                })?;
            ctx.runtime_env().register_object_store(url, store);
        }
    };

    Ok(())
}

fn get_bucket_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse bucket name from url: {}",
            url.as_str()
        ))
    })
}
