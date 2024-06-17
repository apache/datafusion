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

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::franz_sinks::FranzSink;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct MockSink {
    // Store the number of times write_records is called
    pub write_count: AtomicUsize,
    // Store the batches written
    pub batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl MockSink {
    pub fn new() -> Self {
        MockSink {
            write_count: AtomicUsize::new(0),
            batches: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl FranzSink for MockSink {
    async fn write_records(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.write_count.fetch_add(1, Ordering::SeqCst);
        let mut batches = self.batches.lock().await;
        batches.push(batch);
        Ok(())
    }
}
