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
    context::Context,
    print_format::{all_print_formats, PrintFormat},
    print_options::PrintOptions,
};
use datafusion::error::{DataFusionError, Result};
use rustyline::Editor;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
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

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(ctx: &mut Context, print_options: PrintOptions) {
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

async fn exec_and_print(
    ctx: &mut Context,
    print_options: PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();
    let df = ctx.sql(&sql).await?;
    let results = df.collect().await?;
    print_options.print_batches(&results, now)?;

    Ok(())
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}
