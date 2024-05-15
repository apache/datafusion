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

use async_trait::async_trait;

use crate::error::{DataFusionError, Result};
use arrow::json::{LineDelimitedWriter};
use arrow::record_batch::RecordBatch;

use super::sink::FranzSink;

pub struct StdoutSink {}

impl StdoutSink {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl FranzSink for StdoutSink {
    async fn write_record(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        // Write out JSON
        let mut writer = LineDelimitedWriter::new(std::io::stdout().lock());
        let _ = writer.write(&batch).map_err(|e| {
            DataFusionError::Execution(format!("Error writing batch: {}", e))
        })?;

        Ok(())
    }
}

pub struct PrettyPrinter {}

impl PrettyPrinter {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl FranzSink for PrettyPrinter {
    async fn write_record(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
