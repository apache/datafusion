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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::stats::Precision;
use datafusion_common::{DFSchema, TableReference};
use datafusion_execution::cache::cache_manager::{
    CacheManagerConfig, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
    DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT, FileStatisticsCache, ListFilesCache,
};
use datafusion_execution::cache::default_cache::DefaultCache;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::{Expr, col, lit};

use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use tempfile::tempdir;

fn write_statistics_keys(path: &std::path::Path, keys: Vec<Option<i64>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int64, false),
        Field::new("key", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0; keys.len()])),
            Arc::new(Int64Array::from(keys)),
        ],
    )
    .unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create(path).unwrap(),
        schema,
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn collected_statistics_deduplicate_across_files_and_row_groups() {
    let directory = tempdir().unwrap();
    write_statistics_keys(
        &directory.path().join("a.parquet"),
        vec![Some(1), Some(5), None, Some(9)],
    );
    write_statistics_keys(
        &directory.path().join("b.parquet"),
        vec![Some(1), Some(9), None],
    );
    let ctx = SessionContext::new();
    let table = get_listing_table(
        &ListingTableUrl::parse(directory.path().to_str().unwrap()).unwrap(),
        None,
        &ListingOptions::new(Arc::new(ParquetFormat::default())),
    )
    .await
    .with_collected_statistics(&ctx.state(), &["key", "payload", "key"])
    .await
    .unwrap();

    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.num_rows, Precision::Exact(7));
    assert_eq!(
        stats.column_statistics[0].distinct_count,
        Precision::Exact(1)
    );
    assert_eq!(
        stats.column_statistics[1].distinct_count,
        Precision::Exact(3)
    );
    assert_eq!(stats.column_statistics[1].null_count, Precision::Exact(2));

    let plan = table
        .scan(&ctx.state(), Some(&[1]), &[], None)
        .await
        .unwrap();
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.column_statistics.len(), 1);
    assert_eq!(
        stats.column_statistics[0].distinct_count,
        Precision::Exact(3)
    );
}

#[tokio::test]
async fn collected_statistics_handle_empty_and_all_null_columns() {
    for keys in [vec![], vec![None, None]] {
        let directory = tempdir().unwrap();
        let rows = keys.len();
        write_statistics_keys(&directory.path().join("a.parquet"), keys);
        let ctx = SessionContext::new();
        let table = get_listing_table(
            &ListingTableUrl::parse(directory.path().to_str().unwrap()).unwrap(),
            None,
            &ListingOptions::new(Arc::new(ParquetFormat::default())),
        )
        .await
        .with_collected_statistics(&ctx.state(), &["key"])
        .await
        .unwrap();
        let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
        let stats = StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(rows));
        assert_eq!(
            stats.column_statistics[1].distinct_count,
            Precision::Exact(0)
        );
        assert_eq!(
            stats.column_statistics[1].null_count,
            Precision::Exact(rows)
        );
    }
}

#[tokio::test]
async fn collected_statistics_improve_sparse_join_estimates() {
    let directory = tempdir().unwrap();
    write_statistics_keys(
        &directory.path().join("orders.parquet"),
        vec![Some(1), Some(5), Some(12)],
    );
    write_statistics_keys(
        &directory.path().join("lineitem.parquet"),
        [Some(1), Some(5), Some(12)].repeat(4),
    );
    let config = SessionConfig::new().with_target_partitions(1);
    for collect in [false, true] {
        let ctx = SessionContext::new_with_config(config.clone());
        for name in ["orders", "lineitem"] {
            let path = directory.path().join(format!("{name}.parquet"));
            let mut table = get_listing_table(
                &ListingTableUrl::parse(path.to_str().unwrap()).unwrap(),
                None,
                &ListingOptions::new(Arc::new(ParquetFormat::default())),
            )
            .await;
            if collect {
                table = table
                    .with_collected_statistics(&ctx.state(), &["key"])
                    .await
                    .unwrap();
            }
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
        let df = ctx
            .sql("SELECT l.key FROM orders o JOIN lineitem l ON o.key = l.key")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stats = StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(
            stats.num_rows,
            Precision::Inexact(if collect { 12 } else { 3 })
        );
        let batches = datafusion_physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 12);
    }
}

#[tokio::test]
async fn collected_statistics_are_not_reused_for_changed_files_or_pruned_subsets() {
    let directory = tempdir().unwrap();
    for partition in [1, 2] {
        let path = directory.path().join(format!("p={partition}"));
        fs::create_dir(&path).unwrap();
        write_statistics_keys(&path.join("a.parquet"), vec![Some(1), Some(5)]);
    }
    let ctx = SessionContext::new();
    let table = get_listing_table(
        &ListingTableUrl::parse(directory.path().to_str().unwrap()).unwrap(),
        None,
        &ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![("p".into(), DataType::Int32)]),
    )
    .await
    .with_collected_statistics(&ctx.state(), &["key", "p"])
    .await
    .unwrap();
    let plan = table
        .scan(&ctx.state(), None, &[col("p").eq(lit(1))], None)
        .await
        .unwrap();
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.num_rows, Precision::Exact(2));
    assert_eq!(stats.column_statistics[1].distinct_count, Precision::Absent);

    write_statistics_keys(
        &directory.path().join("p=1").join("new.parquet"),
        vec![Some(9)],
    );
    // Respect the existing listing-cache policy: make the new file visible
    // before checking that the collected NDV snapshot is invalidated.
    ctx.runtime_env()
        .cache_manager
        .get_list_files_cache()
        .unwrap()
        .clear();
    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.num_rows, Precision::Exact(5));
    assert_eq!(stats.column_statistics[1].distinct_count, Precision::Absent);
}

#[tokio::test]
async fn check_stats_precision_with_filter_pushdown() {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
    let table_path = ListingTableUrl::parse(filename).unwrap();

    let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let table = get_listing_table(&table_path, None, &opt).await;

    let (_, _, state) = get_cache_runtime_state();
    let mut options: ConfigOptions = state.config().options().as_ref().clone();
    options.execution.parquet.pushdown_filters = true;
    options.execution.collect_statistics = true;

    // Scan without filter, stats are exact
    let exec = table.scan(&state, None, &[], None).await.unwrap();
    assert_eq!(
        StatisticsContext::new()
            .compute(exec.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows,
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

    let optimized_exec = FilterPushdown::new()
        .optimize(filtered_exec, &options)
        .unwrap();

    assert!(
        optimized_exec.is::<DataSourceExec>(),
        "Sanity check that the pushdown did what we expected"
    );
    // Scan with filter pushdown, stats are inexact
    assert_eq!(
        StatisticsContext::new()
            .compute(optimized_exec.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows,
        Precision::Inexact(8),
        "Stats after filter pushdown should be inexact"
    );
}

#[tokio::test]
async fn load_table_stats_with_session_level_cache() {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
    let table_path = ListingTableUrl::parse(filename)
        .unwrap()
        .with_table_ref(TableReference::bare("alltypes_plain"));

    let (cache1, _, mut state1) = get_cache_runtime_state();
    let cfg_1 = state1.config_mut();
    cfg_1.options_mut().execution.collect_statistics = true;

    // Create a separate DefaultFileStatisticsCache
    let (cache2, _, mut state2) = get_cache_runtime_state();
    let cfg_2 = state2.config_mut();
    cfg_2.options_mut().execution.collect_statistics = true;

    let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));

    let table1 = get_listing_table(&table_path, Some(cache1), &opt).await;
    let table2 = get_listing_table(&table_path, Some(cache2), &opt).await;

    //Session 1 first time list files
    assert_eq!(get_static_cache_size(&state1), 0);
    let exec1 = table1.scan(&state1, None, &[], None).await.unwrap();

    assert_eq!(
        StatisticsContext::new()
            .compute(exec1.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        StatisticsContext::new()
            .compute(exec1.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .total_byte_size,
        // Byte size is absent because we cannot estimate the output size
        // of the Arrow data since there are variable length columns.
        Precision::Absent,
    );
    assert_eq!(get_static_cache_size(&state1), 1);

    //Session 2 first time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_static_cache_size(&state2), 0);
    let exec2 = table2.scan(&state2, None, &[], None).await.unwrap();
    assert_eq!(
        StatisticsContext::new()
            .compute(exec2.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        StatisticsContext::new()
            .compute(exec2.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .total_byte_size,
        // Absent because the data contains variable length columns
        Precision::Absent,
    );
    assert_eq!(get_static_cache_size(&state2), 1);

    //Session 1 second time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_static_cache_size(&state1), 1);
    let exec3 = table1.scan(&state1, None, &[], None).await.unwrap();
    assert_eq!(
        StatisticsContext::new()
            .compute(exec3.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows,
        Precision::Exact(8)
    );
    assert_eq!(
        StatisticsContext::new()
            .compute(exec3.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .total_byte_size,
        // Absent because the data contains variable length columns
        Precision::Absent,
    );
    // List same file no increase
    assert_eq!(get_static_cache_size(&state1), 1);
}

#[tokio::test]
async fn anonymous_parquet_stats_cache_with_explicit_wider_schema() {
    let temp_dir = tempdir().unwrap();
    let parquet_path = temp_dir.path().join("data.parquet");
    let parquet_path = parquet_path.to_string_lossy().to_string();

    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_collect_statistics(true),
    );
    let cache = ctx
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .unwrap();

    ctx.sql(&format!(
        "COPY (
            SELECT 1::BIGINT AS id, 1000::BIGINT AS population
        ) TO '{parquet_path}' STORED AS PARQUET"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    assert_eq!(cache.len(), 0);

    ctx.read_parquet(&parquet_path, ParquetReadOptions::default())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(cache.len(), 1);

    let wider_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("population", DataType::Int64, true),
        Field::new("extra", DataType::Int64, true),
    ]);

    let plan = ctx
        .read_parquet(
            &parquet_path,
            ParquetReadOptions::default().schema(&wider_schema),
        )
        .await
        .unwrap()
        .select_columns(&["id", "extra"])
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.column_statistics.len(), 2);
    assert_eq!(stats.column_statistics[1].null_count, Precision::Exact(1));

    // #23072: the cache now validates file_schema, so the wider read no
    // longer bypasses the cache (as in #22950), but it overwrites the existing
    // `{table, path}` entry instead of adding a schema-specific key.
    assert_eq!(cache.len(), 1);

    // Repeat the wider read: same path + same file_schema -> reuse (no new
    // entry) and a cache hit. Under #22950's bypass this read could never reuse.
    ctx.read_parquet(
        &parquet_path,
        ParquetReadOptions::default().schema(&wider_schema),
    )
    .await
    .unwrap()
    .create_physical_plan()
    .await
    .unwrap();
    assert_eq!(cache.len(), 1);
    let hits: usize = cache.list_entries().values().map(|e| e.hits).sum();
    assert_eq!(
        hits, 1,
        "expected a cache hit on the repeat read, got {hits}"
    );
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
    let data_source_exec = exec1.downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet1 = data_source.downcast_ref::<FileScanConfig>().unwrap();

    assert_eq!(get_list_file_cache_size(&state1), 1);
    let fg = &parquet1.file_groups;
    assert_eq!(fg.len(), 1);
    assert_eq!(fg.first().unwrap().len(), 1);

    //Session 2 first time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_list_file_cache_size(&state2), 0);
    let exec2 = table2.scan(&state2, None, &[], None).await.unwrap();
    let data_source_exec = exec2.downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet2 = data_source.downcast_ref::<FileScanConfig>().unwrap();

    assert_eq!(get_list_file_cache_size(&state2), 1);
    let fg2 = &parquet2.file_groups;
    assert_eq!(fg2.len(), 1);
    assert_eq!(fg2.first().unwrap().len(), 1);

    //Session 1 second time list files
    //check session 1 cache result not show in session 2
    assert_eq!(get_list_file_cache_size(&state1), 1);
    let exec3 = table1.scan(&state1, None, &[], None).await.unwrap();
    let data_source_exec = exec3.downcast_ref::<DataSourceExec>().unwrap();
    let data_source = data_source_exec.data_source();
    let parquet3 = data_source.downcast_ref::<FileScanConfig>().unwrap();

    assert_eq!(get_list_file_cache_size(&state1), 1);
    let fg = &parquet3.file_groups;
    assert_eq!(fg.len(), 1);
    assert_eq!(fg.first().unwrap().len(), 1);
    // List same file no increase
    assert_eq!(get_list_file_cache_size(&state1), 1);
}

async fn get_listing_table(
    table_path: &ListingTableUrl,
    static_cache: Option<Arc<FileStatisticsCache>>,
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
    ListingTable::try_new(config1)
        .unwrap()
        .with_cache(static_cache)
}

fn get_cache_runtime_state()
-> (Arc<FileStatisticsCache>, Arc<ListFilesCache>, SessionState) {
    let cache_config = CacheManagerConfig::default();
    let file_static_cache =
        Arc::new(DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT));
    let list_file_cache =
        Arc::new(DefaultCache::new(DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT));

    let cache_config = cache_config
        .with_file_statistics_cache(Some(file_static_cache.clone()))
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
