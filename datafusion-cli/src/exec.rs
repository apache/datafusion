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
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;

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
pub async fn exec_from_repl(ctx: &mut SessionContext, print_options: &mut PrintOptions) {
    let mut rl = Editor::<CliHelper>::new();
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
                                        eprintln!("{}", e)
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
                                eprintln!("{}", e)
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
                    Err(err) => eprintln!("{:?}", err),
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

    rl.save_history(".history").ok();
}

async fn exec_and_print(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();
    let df = ctx.sql(&sql).await?;
    let results = df.collect().await?;
    print_options.print_batches(&results, now)?;

    Ok(())
}
