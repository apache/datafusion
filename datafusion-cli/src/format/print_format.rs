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

//! Print format variants
use arrow::csv::writer::WriterBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use std::str::FromStr;

/// Allow records to be printed in different formats
#[derive(Debug, Clone)]
pub enum PrintFormat {
    Csv,
    Table,
}

impl FromStr for PrintFormat {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<PrintFormat, ()> {
        match s {
            "csv" => Ok(PrintFormat::Csv),
            "table" => Ok(PrintFormat::Table),
            _ => Err(()),
        }
    }
}

impl PrintFormat {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        match self {
            PrintFormat::Csv => {
                let mut bytes = vec![];
                {
                    let builder = WriterBuilder::new().has_headers(true);
                    let mut writer = builder.build(&mut bytes);
                    for batch in batches {
                        writer.write(batch)?;
                    }
                }
                let csv = String::from_utf8(bytes)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                println!("{}", csv);
            }
            PrintFormat::Table => pretty::print_batches(batches)?,
        }
        Ok(())
    }
}
