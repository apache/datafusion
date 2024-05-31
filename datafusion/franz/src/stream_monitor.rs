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

use std::time::Duration;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;

use arrow::record_batch::RecordBatch;

use datafusion::error::{DataFusionError, Result};
use datafusion::franz_sinks::FranzSink;

#[derive(Debug, Clone)]
pub struct StreamMonitorConfig {
}

impl Default for StreamMonitorConfig {
    fn default() -> Self {
        StreamMonitorConfig {
        }
    }
}

impl StreamMonitorConfig {
    pub fn new() -> Self {
        StreamMonitorConfig {
            ..Default::default()
        }
    }
}

pub struct StreamMonitor {
    config: StreamMonitorConfig,
    sink: Arc<Mutex<dyn FranzSink + Send + Sync>>,
}

impl StreamMonitor {
    pub fn new(config: &StreamMonitorConfig, sink: Arc<Mutex<dyn FranzSink + Send + Sync>>) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            sink,
        })
    }
}

#[async_trait]
impl FranzSink for StreamMonitor {
    async fn write_records(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        let mut sink = self.sink.lock().await;
        sink.write_records(batch).await
    }
}

#[cfg(test)]
mod tests {}
