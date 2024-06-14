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
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::instrument;

use crate::record_store::RecordStore;
use crate::rocksdb_backend::RocksDBBackend;

use arrow::json::ArrayWriter;
use arrow::record_batch::RecordBatch;

use datafusion::error::{DataFusionError, Result};
use datafusion::franz_sinks::FranzSink;

use serde_json::json;

use axum::{response::Json, routing::get, Router};

const NAMESPACE: &str = "_stream_monitor";

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
    record_store: RecordStore,
}

impl StreamMonitor {
    pub async fn new(
        config: &StreamMonitorConfig,
        sink: Arc<Mutex<dyn FranzSink + Send + Sync>>,
        state_store: Arc<Mutex<RocksDBBackend>>,
    ) -> Result<Self> {
        let record_store = RecordStore::new(state_store, NAMESPACE.to_string()).await;

        Ok(Self {
            config: config.clone(),
            sink,
            cache: Arc::new(RwLock::new(Vec::new())),
            record_store,
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
                        let json_str: serde_json::Value =
                            serde_json::from_str(str2).unwrap();

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
        // TODO
        // one key should serve as an index for keys that exist in the namespace state store
        // Hash the record batches as they come in and use that hash as the key to store the record
        // batch
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
mod tests {}
