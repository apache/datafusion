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
use datafusion::sql::sqlparser::ast::SetExpr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::instrument;

use arrow::json::ArrayWriter;
use arrow::record_batch::RecordBatch;

use datafusion::error::{DataFusionError, Result};
use datafusion::franz_sinks::FranzSink;

use serde::Serialize;
use serde_json::json;

use axum::{response::Json, routing::get, Router};

#[derive(Debug, Clone)]
struct CacheContent {
    cache: Vec<RecordBatch>,
}

#[derive(Debug, Clone)]
pub struct StreamMonitorConfig {}

impl Default for StreamMonitorConfig {
    fn default() -> Self {
        StreamMonitorConfig {}
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
    cache: Arc<RwLock<Vec<RecordBatch>>>,
}

impl StreamMonitor {
    pub fn new(
        config: &StreamMonitorConfig,
        sink: Arc<Mutex<dyn FranzSink + Send + Sync>>,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            sink,
            cache: Arc::new(RwLock::new(Vec::new())),
        })
    }

    pub async fn start_server(&self) {
        let cache = self.cache.clone();

        let _ = tokio::spawn(async {
            let app = Router::new().route(
                "/cache",
                get(move || {
                    let cache = cache.clone();
                    async move {
                        let batches = cache.read().await;

                        let batch_refs = &*batches.iter().collect::<Vec<_>>();

                        let buf = Vec::new();
                        let mut writer = ArrayWriter::new(buf);
                        writer.write_batches(batch_refs).unwrap();
                        writer.finish().unwrap();

                        // Get the underlying buffer back,
                        let buf = writer.into_inner();

                        let str1 = String::from_utf8(buf).unwrap();
                        let str2 = str1.as_str();
                        let json_str: serde_json::Value = serde_json::from_str(str2).unwrap();

                        Json(json!({ "data": json_str }))
                    }
                }),
            );

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    }
}

#[async_trait]
impl FranzSink for StreamMonitor {
    #[instrument(name = "StreamMonitor::write_records", skip(self, batch))]
    async fn write_records(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        {
            let mut cache = self.cache.write().await;
            cache.push(batch.clone());
            println!("cache size: {}", cache.len());
        }

        let mut sink = self.sink.lock().await;
        sink.write_records(batch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::test::mock_sink::MockSink;
    //
    // use tokio::sync::Mutex;
    // use arrow::array::{Int32Array, ArrayRef};
    // use arrow::record_batch::RecordBatch;
    // use arrow::datatypes::{Schema, Field, DataType};
    // use std::sync::Arc;
    //
    // #[tokio::test]
    // async fn test_write_records() {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("field1", DataType::Int32, false),
    //     ]));
    //     let batch = RecordBatch::try_new(
    //         schema.clone(),
    //         vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    //     ).unwrap();
    //
    //     let config = StreamMonitorConfig::new();
    //     let sink = Arc::new(Mutex::new(MockSink::new()));
    //     let mut monitor = StreamMonitor::new(&config, sink).unwrap();
    //
    //     monitor.write_records(batch.clone()).await.unwrap();
    //
    //     assert_eq!(monitor.cache.len(), 1);
    //     assert_eq!(monitor.cache[0], batch);
    // }
}
