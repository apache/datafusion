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

use crate::rocksdb_backend::{RocksDBBackend, StateError};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

const INDEX_KEY: &str = "index";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordStoreIndex {}

impl RecordStoreIndex {}

pub struct RecordStore {
    state_store: Arc<Mutex<RocksDBBackend>>,
    namespace: String,
}

impl RecordStore {
    pub async fn new(state_store: Arc<Mutex<RocksDBBackend>>, namespace: String) -> Self {
        Self {
            state_store,
            namespace,
        }
    }

    pub async fn add_record(&self, record: &RecordBatch) -> Result<(), StateError> {
        Ok(())
    }

    pub async fn get(&self) -> Result<Option<RecordBatch>, StateError> {
        Ok(None)
    }

    async fn load_index(&self) -> Result<RecordStoreIndex, StateError> {
        if let Some(idx) = self
            .state_store
            .lock()
            .await
            .get_state::<_, RecordStoreIndex>(
                self.namespace.as_str(),
                INDEX_KEY.to_string(),
            )
            .await?
        {
            Ok(idx)
        } else {
            Ok(self.create_index().await?)
        }
    }

    async fn create_index(&self) -> Result<RecordStoreIndex, StateError> {
        Ok(RecordStoreIndex {})
    }
}
