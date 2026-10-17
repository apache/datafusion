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

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_catalog::TableProvider;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{Result, stats::Precision};
use datafusion_datasource::{
    ListingTableUrl, file_scan_config::FileScanConfig, source::DataSourceExec,
};
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_expr::{Expr, Partitioning, col, lit};
use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion_physical_plan::{ExecutionPlan, displayable, empty::EmptyExec};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

fn fixture(values: Vec<Vec<Option<i64>>>, hive: bool, statistics: bool) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Int64, true)]));
    for (i, values) in values.into_iter().enumerate() {
        let parent = if hive {
            dir.path().join(format!("p={}", i % 2))
        } else {
            dir.path().to_owned()
        };
        std::fs::create_dir_all(&parent).unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(if statistics {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            })
            .build();
        let mut writer = ArrowWriter::try_new(
            File::create(parent.join(format!("f{i}.parquet"))).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
    }
    dir
}
fn eight_files() -> TempDir {
    fixture(
        (0..8)
            .map(|i| (i * 10..i * 10 + 10).map(Some).collect())
            .collect(),
        false,
        true,
    )
}
fn table(
    dir: &TempDir,
    partitioning: Option<Partitioning>,
    hive: bool,
    adapter: bool,
) -> ListingTable {
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Int64, true)]));
    let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_output_partitioning(partitioning);
    if hive {
        options = options.with_table_partition_cols(vec![("p".into(), DataType::Int64)]);
    }
    let mut config = ListingTableConfig::new(
        ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap(),
    )
    .with_listing_options(options)
    .with_schema(schema);
    if adapter {
        config =
            config.with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));
    }
    ListingTable::try_new(config).unwrap()
}
fn context(statistics: bool) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_collect_statistics(statistics),
    )
}
fn scan_config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .unwrap()
}
fn names(config: &FileScanConfig) -> Vec<Vec<String>> {
    config
        .file_groups
        .iter()
        .map(|g| {
            g.iter()
                .map(|f| f.object_meta.location.filename().unwrap().to_string())
                .collect()
        })
        .collect()
}
fn predicate() -> Expr {
    col("d").gt_eq(lit(20_i64)).and(col("d").lt(lit(40_i64)))
}
async fn rows(ctx: &SessionContext, query: &str) -> Result<usize> {
    Ok(ctx
        .sql(query)
        .await?
        .collect()
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum())
}

#[tokio::test]
async fn prunes_before_grouping_and_aggregates_survivor_statistics() -> Result<()> {
    let dir = eight_files();
    let ctx = context(true);
    let table = table(&dir, None, false, false);
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    let config = scan_config(&plan);
    assert_eq!(names(config), vec![vec!["f2.parquet"], vec!["f3.parquet"]]);
    assert_eq!(config.statistics().num_rows, Precision::Exact(20));
    assert_eq!(
        config.statistics().column_statistics[0].min_value,
        Precision::Exact(20_i64.into())
    );
    assert_eq!(
        config.statistics().column_statistics[0].max_value,
        Precision::Exact(39_i64.into())
    );
    println!(
        "AFTER raw scan: {}",
        displayable(plan.as_ref()).indent(true)
    );
    println!("AFTER raw statistics: {:?}", config.statistics());
    ctx.register_table("t", Arc::new(table))?;
    let optimized = ctx
        .sql("select * from t where d >= 20 and d < 40")
        .await?
        .create_physical_plan()
        .await?;
    let filtered_statistics =
        datafusion_physical_plan::statistics::StatisticsContext::new().compute(
            optimized.as_ref(),
            &datafusion_physical_plan::statistics::StatisticsArgs::new(),
        )?;
    assert!(
        matches!(filtered_statistics.num_rows, Precision::Inexact(20)),
        "{filtered_statistics:?}"
    );
    println!(
        "AFTER optimized plan: {}",
        displayable(optimized.as_ref()).indent(true)
    );
    assert_eq!(
        rows(&ctx, "select * from t where d >= 20 and d < 40").await?,
        20
    );
    assert_eq!(
        rows(&ctx, "select * from t where d >= 25 and d < 35").await?,
        10
    );
    Ok(())
}
#[tokio::test]
async fn disabled_statistics_keep_all_files() -> Result<()> {
    let dir = eight_files();
    let ctx = context(false);
    let table = table(&dir, None, false, false);
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    assert_eq!(
        scan_config(&plan)
            .file_groups
            .iter()
            .map(|g| g.len())
            .sum::<usize>(),
        8
    );
    ctx.register_table("t", Arc::new(table))?;
    assert_eq!(
        rows(&ctx, "select * from t where d >= 20 and d < 40").await?,
        20
    );
    Ok(())
}
#[tokio::test]
async fn missing_statistics_keep_files() -> Result<()> {
    let dir = fixture(vec![vec![Some(1)], vec![Some(25)]], false, false);
    let ctx = context(true);
    let table = table(&dir, None, false, false);
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    assert_eq!(
        scan_config(&plan)
            .file_groups
            .iter()
            .map(|g| g.len())
            .sum::<usize>(),
        2
    );
    ctx.register_table("t", Arc::new(table))?;
    assert_eq!(
        rows(&ctx, "select * from t where d >= 20 and d < 40").await?,
        1
    );
    Ok(())
}
#[tokio::test]
async fn null_semantics_and_empty_scan() -> Result<()> {
    let dir = fixture(
        vec![
            vec![None, None],
            vec![Some(5), Some(6)],
            vec![None, Some(25)],
        ],
        false,
        true,
    );
    let ctx = context(true);
    let table = table(&dir, None, false, false);
    let plan = table
        .scan(&ctx.state(), None, &[col("d").gt(lit(100_i64))], None)
        .await?;
    assert!(plan.downcast_ref::<EmptyExec>().is_some());
    ctx.register_table("t", Arc::new(table))?;
    assert_eq!(rows(&ctx, "select * from t where d is null").await?, 3);
    assert_eq!(rows(&ctx, "select * from t where d is not null").await?, 3);
    assert_eq!(
        rows(&ctx, "select * from t where d >= 20 or d is null").await?,
        4
    );
    assert_eq!(
        rows(&ctx, "select * from t where d not in (5, NULL)").await?,
        0
    );
    Ok(())
}
#[tokio::test]
async fn declared_partitioning_keeps_groups_and_empty_trailing_groups() -> Result<()> {
    let dir = fixture(vec![vec![Some(1)], vec![Some(25)]], false, true);
    let ctx = context(true);
    let table = table(
        &dir,
        Some(Partitioning::Hash(vec![col("d")], 4)),
        false,
        false,
    );
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    assert_eq!(
        names(scan_config(&plan)),
        vec![vec!["f0.parquet"], vec!["f1.parquet"], vec![], vec![]]
    );
    Ok(())
}
#[tokio::test]
async fn limits_offsets_and_projection_keep_row_filter() -> Result<()> {
    let dir = eight_files();
    let ctx = context(true);
    let table = table(&dir, None, false, false);
    ctx.register_table("t", Arc::new(table))?;
    assert_eq!(
        rows(
            &ctx,
            "select d from t where d >= 25 and d < 35 limit 3 offset 2"
        )
        .await?,
        3
    );
    let batches = ctx
        .sql("select d from t where d >= 25 and d < 35 order by d limit 3 offset 2")
        .await?
        .collect()
        .await?;
    let values: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, [27, 28, 29]);
    Ok(())
}
#[tokio::test]
async fn custom_adapter_disables_statistics_pruning() -> Result<()> {
    let dir = eight_files();
    let ctx = context(true);
    let table = table(&dir, None, false, true);
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    assert_eq!(
        scan_config(&plan)
            .file_groups
            .iter()
            .map(|g| g.len())
            .sum::<usize>(),
        8
    );
    Ok(())
}
#[tokio::test]
async fn hive_and_data_predicates_preserve_partition_groups() -> Result<()> {
    let dir = fixture(
        (0..8)
            .map(|i| (i * 10..i * 10 + 10).map(Some).collect())
            .collect(),
        true,
        true,
    );
    let mut config = SessionConfig::new()
        .with_target_partitions(4)
        .with_collect_statistics(true);
    config.options_mut().optimizer.preserve_file_partitions = 1;
    let ctx = SessionContext::new_with_config(config);
    let table = table(&dir, None, true, false);
    let filters = [col("p").eq(lit(0_i64)), predicate()];
    let plan = table.scan(&ctx.state(), None, &filters, None).await?;
    assert_eq!(names(scan_config(&plan)), vec![vec!["f2.parquet"]]);
    ctx.register_table("t", Arc::new(table))?;
    assert_eq!(
        rows(&ctx, "select * from t where p = 0 and d >= 20 and d < 40").await?,
        10
    );
    assert_eq!(rows(&ctx, "select * from t where p = d").await?, 1);
    Ok(())
}

#[tokio::test]
async fn volatile_predicate_is_not_evaluated_while_listing() -> Result<()> {
    let dir = eight_files();
    let ctx = context(true);
    let table = table(&dir, None, false, false);
    use datafusion_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let udf = create_udf(
        "volatile_threshold",
        vec![],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(1000_i64.into()))
        }),
    );
    let filters = [col("d").gt(udf.call(vec![]))];
    let plan = table.scan(&ctx.state(), None, &filters, None).await?;
    assert_eq!(
        scan_config(&plan)
            .file_groups
            .iter()
            .map(|g| g.len())
            .sum::<usize>(),
        8
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn declared_range_partitioning_preserves_groups() -> Result<()> {
    use datafusion_common::SplitPoint;
    use datafusion_expr::RangePartitioning;
    let dir = fixture(vec![vec![Some(1)], vec![Some(25)]], false, true);
    let ctx = context(true);
    let partitioning = Partitioning::Range(RangePartitioning::try_new(
        vec![col("d").sort(true, true)],
        vec![
            SplitPoint::new(vec![10_i64.into()]),
            SplitPoint::new(vec![30_i64.into()]),
            SplitPoint::new(vec![40_i64.into()]),
        ],
    )?);
    let table = table(&dir, Some(partitioning), false, false);
    let plan = table.scan(&ctx.state(), None, &[predicate()], None).await?;
    assert_eq!(
        names(scan_config(&plan)),
        vec![vec!["f0.parquet"], vec!["f1.parquet"], vec![], vec![]]
    );
    Ok(())
}
