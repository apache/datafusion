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

//! Command within CLI

use crate::cli_context::CliSessionContext;
use crate::exec::{exec_and_print, exec_from_lines};
use crate::functions::{display_all_functions, Function};
use crate::print_format::PrintFormat;
use crate::print_options::PrintOptions;
use clap::ValueEnum;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::exec_err;
use datafusion::common::instant::Instant;
use datafusion::error::{DataFusionError, Result};
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug)]
pub enum Pset {
    Empty,
    Format(Option<String>),
    Pager(Option<String>),
    MaxRows(Option<String>),
    Quiet(Option<String>),
    Color(Option<String>),
}

/// Command
#[derive(Debug)]
pub enum Command {
    Quit,
    Help,
    ListTables,
    DescribeTableStmt(String),
    ListFunctions,
    Include(Option<String>),
    SearchFunctions(String),
    QuietMode(Option<bool>),
    Pset(Pset),
}

impl Command {
    pub async fn execute(
        &self,
        ctx: &dyn CliSessionContext,
        print_options: &mut PrintOptions,
    ) -> Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        match self {
            Self::Help => {
                let now = Instant::now();
                let command_batch = all_commands_info();
                let schema = command_batch.schema();
                let num_rows = command_batch.num_rows();
                let with_table_format = PrintOptions {
                    format: PrintFormat::Table,
                    quiet: true,
                    ..print_options.clone()
                };
                with_table_format.print_batches(
                    &mut writer,
                    schema,
                    &[command_batch],
                    now,
                    num_rows,
                )
            }
            Self::ListTables => {
                exec_and_print(ctx, &mut writer, print_options, "SHOW TABLES".into())
                    .await
            }
            Self::DescribeTableStmt(name) => {
                exec_and_print(
                    ctx,
                    &mut writer,
                    print_options,
                    format!("SHOW COLUMNS FROM {}", name),
                )
                .await
            }
            Self::Include(filename) => {
                if let Some(filename) = filename {
                    let file = File::open(filename).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Error opening {:?} {}",
                            filename, e
                        ))
                    })?;
                    exec_from_lines(ctx, &mut BufReader::new(file), print_options)
                        .await?;
                    Ok(())
                } else {
                    exec_err!("Required filename argument is missing")
                }
            }
            Self::QuietMode(quiet) => {
                if let Some(quiet) = quiet {
                    print_options.quiet = *quiet;
                    println!(
                        "Quiet mode set to {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                } else {
                    println!(
                        "Quiet mode is {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                }
                Ok(())
            }
            Self::Quit => exec_err!("Unexpected quit, this should be handled outside"),
            Self::ListFunctions => display_all_functions(),
            Self::SearchFunctions(function) => {
                if let Ok(func) = function.parse::<Function>() {
                    let details = func.function_details()?;
                    println!("{}", details);
                    Ok(())
                } else {
                    exec_err!("{function} is not a supported function")
                }
            }
            Self::Pset(_) => exec_err!("Unexpected pset, this should be handled outside"),
        }
    }

    fn get_name_and_description(&self) -> (&'static str, &'static str) {
        match self {
            Self::Quit => ("\\q", "quit datafusion-cli"),
            Self::ListTables => ("\\d", "list tables"),
            Self::DescribeTableStmt(_) => ("\\d name", "describe table"),
            Self::Help => ("\\?", "help"),
            Self::Include(_) => {
                ("\\i filename", "reads input from the specified filename")
            }
            Self::ListFunctions => ("\\h", "function list"),
            Self::SearchFunctions(_) => ("\\h function", "search function"),
            Self::QuietMode(_) => ("\\quiet (true|false)?", "print or set quiet mode"),
            Self::Pset(_) => ("\\pset [NAME [VALUE]]", "table display options"),
        }
    }
}

const ALL_COMMANDS: [Command; 9] = [
    Command::ListTables,
    Command::DescribeTableStmt(String::new()),
    Command::Quit,
    Command::Help,
    Command::Include(Some(String::new())),
    Command::ListFunctions,
    Command::SearchFunctions(String::new()),
    Command::QuietMode(None),
    Command::Pset(Pset::Empty),
];

fn all_commands_info() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Command", DataType::Utf8, false),
        Field::new("Description", DataType::Utf8, false),
    ]));
    let (names, description): (Vec<&str>, Vec<&str>) = ALL_COMMANDS
        .into_iter()
        .map(|c| c.get_name_and_description())
        .unzip();
    RecordBatch::try_new(
        schema,
        [names, description]
            .into_iter()
            .map(|i| Arc::new(StringArray::from(i)) as ArrayRef)
            .collect::<Vec<_>>(),
    )
    .expect("This should not fail")
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match crate::split_on_first_space(s) {
            ("q", None) => Self::Quit,
            ("d", None) => Self::ListTables,
            ("d", Some(name)) => Self::DescribeTableStmt(name.into()),
            ("?", None) => Self::Help,
            ("h", None) => Self::ListFunctions,
            ("h", Some(function)) => Self::SearchFunctions(function.into()),
            ("i", None) => Self::Include(None),
            ("i", Some(filename)) => Self::Include(Some(filename.to_owned())),
            ("quiet", Some("true" | "t" | "yes" | "y" | "on")) => {
                Self::QuietMode(Some(true))
            }
            ("quiet", Some("false" | "f" | "no" | "n" | "off")) => {
                Self::QuietMode(Some(false))
            }
            ("quiet", None) => Self::QuietMode(None),
            ("pset", None) => Self::Pset(Pset::Empty),
            ("pset", Some(rest)) => Self::Pset(rest.parse::<Pset>()?),
            _ => return Err(()),
        })
    }
}

impl FromStr for Pset {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match crate::split_on_first_space(s) {
            ("format", None) => Self::Format(None),
            ("format", Some(format)) => Self::Format(Some(format.into())),
            ("pager", None) => Self::Pager(None),
            ("pager", Some(cmd)) => Self::Pager(Some(cmd.into())),
            ("maxrows", None) => Self::MaxRows(None),
            ("maxrows", Some(mxr)) => Self::MaxRows(Some(mxr.into())),
            ("quiet", None) => Self::Quiet(None),
            ("quiet", Some(quiet)) => Self::Quiet(Some(quiet.into())),
            ("color", None) => Self::Color(None),
            ("color", Some(color)) => Self::Color(Some(color.into())),

            _ => return Err(()),
        })
    }
}

impl Pset {
    pub fn execute(&self, print_options: &mut PrintOptions) -> Result<()> {
        match self {
            Self::Empty => println!("{:#}", print_options),

            Self::Pager(None) => println!("pager is {}. Possible values: path [cmd-options] | yes | true | no | none | false", print_options.pager.as_ref().unwrap_or(&"not set".into())),
            Self::Pager(Some(pager)) => {
                match crate::pager::parse_pset_pager(pager) {
                    Ok(pager_cmd) => {
                        print_options.pager = pager_cmd;
                        Self::Pager(None).execute(print_options)?
                    }
                    Err(e) => return exec_err!("pager error {e}"),
                }
            },

            Self::Format(None) => println!("Output format is {:?}. Possible values: {:?}", print_options.format, PrintFormat::value_variants()),
            Self::Format(Some(format)) => {
                match format.parse::<PrintFormat>() {
                    Ok(format) => {
                        print_options.format = format;
                        Self::Format(None).execute(print_options)?
                    }
                    Err(_) => return exec_err!(
                        "{:?} is not a valid format type [possible values: {:?}]",
                        format,
                        PrintFormat::value_variants()
                    ),
                }
            },

            Self::MaxRows(None) => println!("maxrows is {}", print_options.maxrows),
            Self::MaxRows(Some(mxr)) => {
                match mxr.parse::<crate::print_options::MaxRows>() {
                    Ok(mxr) => {
                        print_options.maxrows = mxr;
                        Self::MaxRows(None).execute(print_options)?
                    }
                    Err(e) => return exec_err!("error in maxrows {e}"),
                }
            }

            Pset::Quiet(None) => println!("quiet is {}", print_options.quiet),
            Pset::Quiet(Some(v)) => {
                match v.parse::<bool>() {
                    Ok(v) => {
                        print_options.quiet = v;
                        Self::Quiet(None).execute(print_options)?
                    }
                    Err(e) => return exec_err!("error in quiet {e}"),
                }
            },

            Pset::Color(None) => println!("color is {}", print_options.quiet),
            Pset::Color(Some(v)) => {
                match v.parse::<bool>() {
                    Ok(v) => {
                        print_options.color = v;
                        Self::Color(None).execute(print_options)?
                    }
                    Err(e) => return exec_err!("error in quiet {e}"),
                }
            },
        };
        Ok(())
    }
}
