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

use std::fs;
use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::stats::Precision;
use datafusion_common::DFSchema;
use datafusion_execution::cache::cache_manager::CacheManagerConfig;
use datafusion_execution::cache::cache_unit::{
    DefaultFileStatisticsCache, DefaultListFilesCache,
};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::{col, lit, Expr};

use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::ExecutionPlan;
use tempfile::tempdir;

#[tokio::test]
async fn check_stats_precision_with_filter_pushdown() {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
    let table_path = ListingTableUrl::parse(filename).unwrap();

    let opt =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(true);
    let table = get_listing_table(&table_path, None, &opt).await;

    let (_, _, state) = get_cache_runtime_state();
    let mut options: ConfigOptions = state.config().options().as_ref().clone();
    options.execution.parquet.pushdown_filters = true;

    // Scan without filter, stats are exact
    let exec = table.scan(&state, None, &[], None).await.unwrap();
    assert_eq!(
        exec.partition_statistics(None).unwrap().num_rows,
        Precision::Exact(8),
        "Stats without filter should be exact"
    );

    // This is a filter that cannot be evaluated by the table provider scanning
    // (it is not a partition filter). Therefore; it will be pushed down to the
    // source operator after the appropriate optimizer pass.
    let filter_expr = Expr::gt(col("id"), lit(1));
    let exec_with_filter = table
        .scan(&state, None, std::slice::from_ref(&filter_expr), None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(table.schema()).unwrap();
    let physical_filter = ctx.create_physical_expr(filter_expr, &df_schema).unwrap();

    let filtered_exec =
        Arc::new(FilterExec::try_new(physical_filter, exec_with_filter).unwrap())
            as Arc<dyn ExecutionPlan>;

    let session_config = SessionConfig::from(options.clone());
    let optimizer_context = OptimizerContext::new(session_config);
    let optimized_exec = FilterPushdown::new()
        .optimize_plan(filtered_exec, &optimizer_context)
        .unwrap();

    assert!(
        optimized_exec.as_any().is::<DataSourceExec>(),
        "Sanity check that the pushdown did what we expected"
    );
    // Scan with filter pushdown, stats are inexact
    assert_eq!(
        optimized_exec.partition_statistics(None).unwrap().num_rows,
        Precision::Inexact(8),
        "Stats after filter pushdown should be inexact"
    );
}

#[tokio::test]
async fn load_table_stats_with_session_level_cache() {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
    let table_path = ListingTableUrl::parse(filename).unwrap();

    let (cache1, _, state1) = get_cache_runtime_state();

    // Create a separate DefaultFileStatisticsCache
    let (cache2, _, state2) = get_cache_runtime_state();

    let opt =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(true);

    let table1 = get_listing_table(&table_path, Some(cache1), &opt).await;
    let table2 = get_listing_table(&table_path, Some(cache2), &opt).await;

    //Session 1 first time list files
    assert_eq!(get_static_cache_size(&state1), 0);
    let exec1 = table1.scan(&state1, None, &[], None).await.unwrap();

    assert_eq!(
        exec1.partition_statistics(None).unwrap().num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        exec1.partition_statistics(None).unwrap().total_byte_size,
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        Precision::Exact(671),
    );
    assert_eq!(get_static_cache_size(&state1), 1);

    //Session 2 first time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_static_cache_size(&state2), 0);
    let exec2 = table2.scan(&state2, None, &[], None).await.unwrap();
    assert_eq!(
        exec2.partition_statistics(None).unwrap().num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        exec2.partition_statistics(None).unwrap().total_byte_size,
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        Precision::Exact(671),
    );
    assert_eq!(get_static_cache_size(&state2), 1);

    //Session 1 second time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_static_cache_size(&state1), 1);
    let exec3 = table1.scan(&state1, None, &[], None).await.unwrap();
    assert_eq!(
        exec3.partition_statistics(None).unwrap().num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        exec3.partition_statistics(None).unwrap().total_byte_size,
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        Precision::Exact(671),
    );
    // List same file no increase
    assert_eq!(get_static_cache_size(&state1), 1);
}

#[tokio::test]
async fn list_files_with_session_level_cache() {
    let p_name = "alltypes_plain.parquet";
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{testdata}/{p_name}");

    let temp_dir1 = tempdir().unwrap();
    let temp_path1 = temp_dir1.path().to_str().unwrap();
    let temp_filename1 = format!("{temp_path1}/{p_name}");

    let temp_dir2 = tempdir().unwrap();
    let temp_path2 = temp_dir2.path().to_str().unwrap();
    let temp_filename2 = format!("{temp_path2}/{p_name}");

    fs::copy(filename.clone(), temp_filename1).expect("panic");
    fs::copy(filename, temp_filename2).expect("panic");

    let table_path = ListingTableUrl::parse(temp_path1).unwrap();

    let (_, _, state1) = get_cache_runtime_state();

    // Create a separate DefaultFileStatisticsCache
    let (_, _, state2) = get_cache_runtime_state();

    let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));

    let table1 = get_listing_table(&table_path, None, &opt).await;
    let table2 = get_listing_table(&table_path, None, &opt).await;

    //Session 1 first time list files
    assert_eq!(get_list_file_cache_size(&state1), 0);
    let exec1 = table1.scan(&state1, None, &[], None).await.unwrap();
    let data_source_exec = exec1.as_any().downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet1 = data_source
        .as_any()
        .downcast_ref::<FileScanConfig>()
        .unwrap();

    assert_eq!(get_list_file_cache_size(&state1), 1);
    let fg = &parquet1.file_groups;
    assert_eq!(fg.len(), 1);
    assert_eq!(fg.first().unwrap().len(), 1);

    //Session 2 first time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_list_file_cache_size(&state2), 0);
    let exec2 = table2.scan(&state2, None, &[], None).await.unwrap();
    let data_source_exec = exec2.as_any().downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet2 = data_source
        .as_any()
        .downcast_ref::<FileScanConfig>()
        .unwrap();

    assert_eq!(get_list_file_cache_size(&state2), 1);
    let fg2 = &parquet2.file_groups;
    assert_eq!(fg2.len(), 1);
    assert_eq!(fg2.first().unwrap().len(), 1);

    //Session 1 second time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_list_file_cache_size(&state1), 1);
    let exec3 = table1.scan(&state1, None, &[], None).await.unwrap();
    let data_source_exec = exec3.as_any().downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet3 = data_source
        .as_any()
        .downcast_ref::<FileScanConfig>()
        .unwrap();

    assert_eq!(get_list_file_cache_size(&state1), 1);
    let fg = &parquet3.file_groups;
    assert_eq!(fg.len(), 1);
    assert_eq!(fg.first().unwrap().len(), 1);
    // List same file no increase
    assert_eq!(get_list_file_cache_size(&state1), 1);
}

async fn get_listing_table(
    table_path: &ListingTableUrl,
    static_cache: Option<Arc<DefaultFileStatisticsCache>>,
    opt: &ListingOptions,
) -> ListingTable {
    let schema = opt
        .infer_schema(
            &SessionStateBuilder::new().with_default_features().build(),
            table_path,
        )
        .await
        .unwrap();
    let config1 = ListingTableConfig::new(table_path.clone())
        .with_listing_options(opt.clone())
        .with_schema(schema);
    let table = ListingTable::try_new(config1).unwrap();
    if let Some(c) = static_cache {
        table.with_cache(Some(c))
    } else {
        table
    }
}

fn get_cache_runtime_state() -> (
    Arc<DefaultFileStatisticsCache>,
    Arc<DefaultListFilesCache>,
    SessionState,
) {
    let cache_config = CacheManagerConfig::default();
    let file_static_cache = Arc::new(DefaultFileStatisticsCache::default());
    let list_file_cache = Arc::new(DefaultListFilesCache::default());

    let cache_config = cache_config
        .with_files_statistics_cache(Some(file_static_cache.clone()))
        .with_list_files_cache(Some(list_file_cache.clone()));

    let rt = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_config)
        .build_arc()
        .expect("could not build runtime environment");

    let state = SessionContext::new_with_config_rt(SessionConfig::default(), rt).state();

    (file_static_cache, list_file_cache, state)
}

fn get_static_cache_size(state1: &SessionState) -> usize {
    state1
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .unwrap()
        .len()
}

fn get_list_file_cache_size(state1: &SessionState) -> usize {
    state1
        .runtime_env()
        .cache_manager
        .get_list_files_cache()
        .unwrap()
        .len()
}
