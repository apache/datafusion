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
use datafusion::error::Result;
use std::io::{Read, Seek, SeekFrom};
use tempfile::tempfile;

/// Allow records to be printed in different formats
#[derive(Debug, Clone)]
pub enum PrintFormat {
    Csv,
    Aligned,
}

impl PrintFormat {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        match self {
            PrintFormat::Csv => {
                // utilizing a temp file so that we can leverage the arrow csv writer
                // ideally the upstream shall take a more generic trait
                let mut file = tempfile()?;
                {
                    let builder = WriterBuilder::new().has_headers(true);
                    let mut writer = builder.build(&file);
                    batches
                        .iter()
                        .for_each(|batch| writer.write(batch).unwrap());
                }
                let mut data = String::new();
                file.seek(SeekFrom::Start(0))?;
                file.read_to_string(&mut data)?;
                println!("{}", data);
            }
            PrintFormat::Aligned => pretty::print_batches(batches)?,
        }
        Ok(())
    }
}
