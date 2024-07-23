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

use std::env;

use datafusion_common::DataFusionError;
use rocksdb::{BoundColumnFamily, DBWithThreadMode, MultiThreaded, Options, DB};
use std::sync::Arc;

pub struct RocksDBBackend {
    db: DBWithThreadMode<MultiThreaded>,
}

impl RocksDBBackend {
    pub fn new(path: &str) -> Result<Self, DataFusionError> {
        let dir = env::temp_dir();
        let mut db_opts: Options = Options::default();
        db_opts.create_if_missing(true);
        // ... potentially set other general DB options
        let db = DBWithThreadMode::<MultiThreaded>::open(
            &db_opts,
            format!("{}{}", dir.display(), path),
        )
        .map_err(|error| DataFusionError::Internal(error.into_string()))?;
        Ok(RocksDBBackend { db })
    }

    pub fn create_cf(&self, namespace: &str) -> Result<(), DataFusionError> {
        let cf_opts: Options = Options::default();
        DBWithThreadMode::<MultiThreaded>::create_cf(&self.db, namespace, &cf_opts)
            .map_err(|e| DataFusionError::Internal(e.into_string()))?;
        //self.namespaces.insert(namespace.to_string());
        Ok(())
    }

    async fn get_cf(
        &self,
        namespace: &str,
    ) -> Result<Arc<BoundColumnFamily>, DataFusionError> {
        self.db.cf_handle(namespace).ok_or_else(|| {
            DataFusionError::Internal("namespace does not exist.".to_string())
        })
    }

    fn namespaced_key(namespace: &str, key: &[u8]) -> Vec<u8> {
        let mut nk: Vec<u8> = namespace.as_bytes().to_vec();
        nk.push(b':');
        nk.extend_from_slice(key);
        nk
    }

    pub(crate) fn destroy(&self) {
        let ret = DB::destroy(&Options::default(), self.db.path());
    }

    pub async fn put_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DataFusionError> {
        /*         if !self.namespaces.contains(namespace) {
            return Err(StateBackendError {
                message: "Namespace does not exist.".into(),
            });
        } */
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &key);
        self.db
            .put_cf(&cf, namespaced_key, value)
            .map_err(|e| DataFusionError::Internal(e.to_string()))
    }

    pub async fn get_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DataFusionError> {
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &key);

        match self
            .db
            .get_cf(&cf, namespaced_key)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?
        {
            Some(serialized_value) => Ok(Some(serialized_value)),
            None => Ok(None),
        }
    }

    pub async fn delete_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &key);

        self.db
            .delete_cf(&cf, &namespaced_key)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        Ok(())
    }
}
