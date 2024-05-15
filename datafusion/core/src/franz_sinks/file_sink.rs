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

use crate::error::{DataFusionError, Result};

use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::fs::File;
use std::sync::Arc;

use super::sink::FranzSink;

pub struct FileSink {
    file: Arc<tokio::sync::Mutex<LineDelimitedWriter<File>>>,
}

// todo
impl FileSink {
    // todo
    pub fn new(fname: &str) -> Result<Self> {
        // todo
        let file = File::create(fname)?;
        let writer = LineDelimitedWriter::new(file);
        Ok(Self {
            file: Arc::new(tokio::sync::Mutex::new(writer)),
        })
    }
}

#[async_trait]
impl FranzSink for FileSink {
    async fn write_record(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        let mut file = self.file.lock().await;

        file.write(&batch).map_err(DataFusionError::from)?;
        file.finish().map_err(DataFusionError::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
