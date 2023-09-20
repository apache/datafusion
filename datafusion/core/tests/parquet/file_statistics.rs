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

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_execution::cache::cache_manager::CacheManagerConfig;
use datafusion_execution::cache::cache_unit;
use datafusion_execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use std::sync::Arc;

#[tokio::test]
async fn load_table_stats_with_session_level_cache() {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
    let table_path = ListingTableUrl::parse(filename).unwrap();

    let (cache1, state1) = get_cache_runtime_state();

    // Create a separate DefaultFileStatisticsCache
    let (cache2, state2) = get_cache_runtime_state();

    let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));

    let table1 = get_listing_with_cache(&table_path, cache1, &state1, &opt).await;
    let table2 = get_listing_with_cache(&table_path, cache2, &state2, &opt).await;

    //Session 1 first time list files
    assert_eq!(get_cache_size(&state1), 0);
    let exec1 = table1.scan(&state1, None, &[], None).await.unwrap();

    assert_eq!(exec1.statistics().unwrap().num_rows, Some(8));
    assert_eq!(exec1.statistics().unwrap().total_byte_size, Some(671));
    assert_eq!(get_cache_size(&state1), 1);

    //Session 2 first time list files
    //check session 1 cache result not show in session 2
    assert_eq!(
        state2
            .runtime_env()
            .cache_manager
            .get_file_statistic_cache()
            .unwrap()
            .len(),
        0
    );
    let exec2 = table2.scan(&state2, None, &[], None).await.unwrap();
    assert_eq!(exec2.statistics().unwrap().num_rows, Some(8));
    assert_eq!(exec2.statistics().unwrap().total_byte_size, Some(671));
    assert_eq!(get_cache_size(&state2), 1);

    //Session 1 second time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_cache_size(&state1), 1);
    let exec3 = table1.scan(&state1, None, &[], None).await.unwrap();
    assert_eq!(exec3.statistics().unwrap().num_rows, Some(8));
    assert_eq!(exec3.statistics().unwrap().total_byte_size, Some(671));
    // List same file no increase
    assert_eq!(get_cache_size(&state1), 1);
}

async fn get_listing_with_cache(
    table_path: &ListingTableUrl,
    cache1: Arc<DefaultFileStatisticsCache>,
    state1: &SessionState,
    opt: &ListingOptions,
) -> ListingTable {
    let schema = opt.infer_schema(state1, table_path).await.unwrap();
    let config1 = ListingTableConfig::new(table_path.clone())
        .with_listing_options(opt.clone())
        .with_schema(schema);
    ListingTable::try_new(config1)
        .unwrap()
        .with_cache(Some(cache1))
}

fn get_cache_runtime_state() -> (Arc<DefaultFileStatisticsCache>, SessionState) {
    let cache_config = CacheManagerConfig::default();
    let cache1 = Arc::new(cache_unit::DefaultFileStatisticsCache::default());
    let cache_config = cache_config.with_files_statistics_cache(Some(cache1.clone()));
    let rt = Arc::new(
        RuntimeEnv::new(RuntimeConfig::new().with_cache_manager(cache_config)).unwrap(),
    );
    let state = SessionContext::with_config_rt(SessionConfig::default(), rt).state();

    (cache1, state)
}

fn get_cache_size(state1: &SessionState) -> usize {
    state1
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .unwrap()
        .len()
}
