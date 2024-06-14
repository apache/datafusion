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

use std::{collections::HashSet, env};

use std::sync::Arc;
use rocksdb::{BoundColumnFamily, Error as RocksDBError, Options, DB, DBWithThreadMode, MultiThreaded, DBCommon};
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeJsonError;

#[derive(Debug)]
pub struct StateError {
    message: String,
}

impl From<RocksDBError> for StateError {
    fn from(error: RocksDBError) -> Self {
        StateError {
            message: error.to_string(),
        }
    }
}

impl From<SerdeJsonError> for StateError {
    fn from(error: SerdeJsonError) -> Self {
        StateError {
            message: error.to_string(),
        }
    }
}

pub struct RocksDBBackend {
    db: DBWithThreadMode::<MultiThreaded>,
    namespaces: HashSet<String>,
}

impl RocksDBBackend {
    pub fn new(path: &str) -> Result<Self, StateError> {
        let dir = env::temp_dir();
        let mut db_opts: Options = Options::default();
        db_opts.create_if_missing(true);
        tracing::info!("creating a backend at {}", dir.display());
        // ... potentially set other general DB options
        let db = DBWithThreadMode::<MultiThreaded>::open(&db_opts, format!("{}{}", dir.display(), path))?;
        Ok(RocksDBBackend {
            db,
            namespaces: HashSet::new(),
        })
    }

    async fn create_cf(&mut self, namespace: &str) -> Result<(), StateError> {
        let cf_opts: Options = Options::default();
        DBWithThreadMode::<MultiThreaded>::create_cf(&mut self.db, namespace, &cf_opts).map_err(|e| StateError::from(e))?;
        self.namespaces.insert(namespace.to_string());
        Ok(())
    }

    async fn get_cf(&self, namespace: &str) -> Result<Arc<BoundColumnFamily>, StateError> {
        self.db.cf_handle(namespace).ok_or_else(|| StateError {
            message: "namespace does not exist.".to_string(),
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
        tracing::info!("destroyed db {:?}", ret)
    }

    pub async fn put_state<K, V>(&mut self, namespace: &str, key: K, value: V) -> Result<(), StateError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Send,
        V: Serialize + for<'de> Deserialize<'de> + Send,
    {
        if !self.namespaces.contains(namespace) {
            let _ = self.create_cf(namespace).await?;
        }
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let serialized_key: Vec<u8> = serde_json::to_vec(&key)?;
        let serialized_value: Vec<u8> = serde_json::to_vec(&value)?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &serialized_key);
        self.db
            .put_cf(&cf, namespaced_key, serialized_value)
            .map_err(|e| StateError {
                message: e.to_string(),
            })
    }

    pub async fn get_state<K, V>(&mut self, namespace: &str, key: K) -> Result<Option<V>, StateError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Send + std::fmt::Debug,
        V: Serialize + for<'de> Deserialize<'de> + Send,
    {
        unsafe {
            tracing::info!("get_state called. for {} {:?}", namespace, key);
        }

        if !self.namespaces.contains(namespace) {
            let _ = self.create_cf(namespace).await?;
        }
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let serialized_key: Vec<u8> = serde_json::to_vec(&key)?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &serialized_key);

        match self.db.get_cf(&cf, namespaced_key)? {
            Some(serialized_value) => {
                let value: V = serde_json::from_slice(&serialized_value)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    // TODO Does not work
    // async fn put_state_with_ttl<K, V>(
    //     &self,
    //     namespace: &str,
    //     key: K,
    //     value: V,
    //     ttl: i64,
    // ) -> Result<(), StateError>
    // where
    //     K: Serialize + for<'de> Deserialize<'de> + Send,
    //     V: Serialize + for<'de> Deserialize<'de> + Send,
    // {
    //     let cf = self.get_cf(namespace).await?;
    //
    //     let serialized_key = serde_json::to_vec(&key).map_err(|e| StateError {
    //         message: e.to_string(),
    //     })?;
    //     let serialized_value: Vec<u8> = serde_json::to_vec(&value).map_err(|e| StateError {
    //         message: e.to_string(),
    //     })?;
    //     let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &serialized_key);
    //     self.db
    //         .put_cf(cf, namespaced_key, serialized_value)
    //         .map_err(|e| StateError {
    //             message: e.to_string(),
    //         })?;
    //     Ok(())
    // }

    pub async fn delete_state<K>(&self, namespace: &str, key: K) -> Result<(), StateError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Send,
    {
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace).await?;
        let serialized_key: Vec<u8> = serde_json::to_vec(&key)?;
        let namespaced_key: Vec<u8> = Self::namespaced_key(namespace, &serialized_key);

        self.db
            .delete_cf(&cf, &namespaced_key)
            .map_err(|e| StateError {
                message: e.to_string(),
            })?;
        Ok(())
    }
}
