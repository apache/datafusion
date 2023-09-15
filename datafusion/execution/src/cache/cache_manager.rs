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

use crate::cache::CacheAccessor;
use datafusion_common::{Result, Statistics};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::sync::Arc;

pub type FileStaticCache = Arc<dyn CacheAccessor<Path, Statistics, Extra=ObjectMeta>>;

#[derive(Default)]
pub struct CacheManager {
    file_statistic_cache: Option<FileStaticCache>,
}

impl CacheManager {
    pub fn try_new(config: &CacheManagerConfig) -> Result<Arc<Self>> {
        let mut manager = CacheManager::default();
        if let Some(cc) = &config.table_files_statistics_cache {
            manager.file_statistic_cache = Some(cc.clone())
        }
        Ok(Arc::new(manager))
    }

    pub fn get_table_statistic_cache(&self) -> Option<FileStaticCache> {
        self.file_statistic_cache.clone()
    }
}

#[derive(Clone, Default)]
pub struct CacheManagerConfig {
    /// Enable cache of files statistics when listing files.
    /// Avoid get same file statistics repeatedly in same datafusion session.
    /// Default is disable. Fow now only supports Parquet files.
    pub table_files_statistics_cache: Option<FileStaticCache>,
}

impl CacheManagerConfig {
    pub fn enable_table_files_statistics_cache(mut self, cache: FileStaticCache) -> Self {
        self.table_files_statistics_cache = Some(cache);
        self
    }
}
