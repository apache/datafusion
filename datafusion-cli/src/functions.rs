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
use datafusion::error::{DataFusionError, Result};
use std::str::FromStr;

#[derive(Debug)]
pub enum Function {
    SELECT,
    EXPLAIN,
}

const ALL_FUNCTIONS: [Function; 2] = [Function::SELECT, Function::EXPLAIN];

impl Function {
    pub fn function_details(&self) -> Result<()> {
        match self {
            Function::SELECT => {
                let details = "Description: Select rows from table
                Syntax: SELECT expression
                    [FROM from_item]";
                println!("{}", details)
            }
            Function::EXPLAIN => {
                let details = "Show execution plan of a statement";
                println!("{}", details)
            }
        }
        Ok(())
    }
}

impl FromStr for Function {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.to_uppercase().as_str() {
            "SELECT" => Self::SELECT,
            "EXPLAIN" => Self::EXPLAIN,
            _ => return Err(()),
        })
    }
}

pub fn display_all_functions() -> Result<()> {
    println!("Available help:");
    ALL_FUNCTIONS.iter().for_each(|f| println!("{:?}", f));
    Ok(())
}
