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

use datafusion_common::{Result, Statistics};
use std::sync::Arc;
use object_store::ObjectMeta;
use object_store::path::Path;
use crate::cache::CacheAccessor;

pub type TableStaticCache = Arc<dyn CacheAccessor<Path, Statistics, Extra= ObjectMeta>>;

pub struct CacheManager {
    table_statistic_cache: Option<TableStaticCache>,
}

impl Default for CacheManager {
    fn default() -> Self {
        CacheManager {
            table_statistic_cache: None,
        }
    }
}

impl CacheManager {
    pub fn try_new(config: &CacheManagerConfig) -> Result<Arc<Self>> {
        let mut manager = CacheManager::default();
        if let Some(cc) = &config.table_files_statistics_cache {
            manager.table_statistic_cache = Some(cc.clone())
        }
        Ok(Arc::new(manager))
    }

    pub fn get_table_statistic_cache(&self) -> Option<TableStaticCache> {
        self.table_statistic_cache.clone()
    }
}

#[derive(Clone)]
pub struct CacheManagerConfig {
    /// Enable cache of files statistics when listing files.
    /// Avoid get same file statistics repeatedly in same datafusion session.
    /// Default is disable. Fow now only supports Parquet files.
    pub table_files_statistics_cache: Option<TableStaticCache>,
}

impl Default for CacheManagerConfig {
    fn default() -> Self {
        CacheManagerConfig {
            table_files_statistics_cache: None,
        }
    }
}

impl CacheManagerConfig {
    pub fn enable_table_files_statistics_cache(mut self, cache: TableStaticCache) -> Self {
        self.table_files_statistics_cache = Some(cache);
        self
    }
}