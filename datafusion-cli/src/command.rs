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

use crate::context::Context;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use std::str::FromStr;
use std::sync::Arc;

/// Command
#[derive(Debug)]
pub enum Command {
    Quit,
    Help,
    ListTables,
}

impl Command {
    pub async fn execute(&self, ctx: &mut Context) -> Result<()> {
        match self {
            Self::Help => pretty::print_batches(&[all_commands_info()])
                .map_err(|e| DataFusionError::Execution(e.to_string())),
            Self::ListTables => {
                let df = ctx.sql("SHOW TABLES").await?;
                let batches = df.collect().await?;
                pretty::print_batches(&batches)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))
            }
            Self::Quit => Err(DataFusionError::Execution(
                "Unexpected quit, this should be handled outside".into(),
            )),
        }
    }

    fn get_name_and_description(&self) -> (&str, &str) {
        match self {
            Self::Quit => ("\\q", "quit datafusion-cli"),
            Self::ListTables => ("\\d", "list tables"),
            Self::Help => ("\\?", "help"),
        }
    }
}

const ALL_COMMANDS: [Command; 3] = [Command::ListTables, Command::Quit, Command::Help];

fn all_commands_info() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Command", DataType::Utf8, false),
        Field::new("Description", DataType::Utf8, false),
    ]));
    let (names, description): (Vec<&str>, Vec<&str>) = ALL_COMMANDS
        .iter()
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

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "q" => Self::Quit,
            "d" => Self::ListTables,
            "?" => Self::Help,
            _ => return Err(()),
        })
    }
}
