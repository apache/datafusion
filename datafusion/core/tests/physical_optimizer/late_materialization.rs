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

use std::sync::Arc;

use crate::physical_optimizer::test_utils::{parquet_exec, schema};

use arrow::array::{Int64Array, StringArray, UInt16Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, assert_contains};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::late_materialization::LateMaterialization;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::{ExecutionPlan, displayable};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

#[test]
fn rewrites_simple_parquet_topk_by_default() -> Result<()> {
    let plan = topk_plan()?;
    let optimized = LateMaterialization::new().optimize(plan, &ConfigOptions::new())?;
    let display = displayable(optimized.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");
    assert_contains!(&display, "projection=[c]");
    assert_contains!(&display, "projection=[a, b, c, d, e]");
    Ok(())
}

#[test]
fn skips_rewrite_when_disabled() -> Result<()> {
    let plan = topk_plan()?;
    let mut config = ConfigOptions::new();
    config.optimizer.enable_row_number_topk_late_materialization = false;

    let optimized = LateMaterialization::new().optimize(plan, &config)?;
    let display = displayable(optimized.as_ref()).indent(false).to_string();

    assert_contains!(&display, "SortExec: TopK(fetch=10)");
    assert!(!display.contains("LateTopKMaterializationExec"));
    Ok(())
}

#[test]
fn rewrites_filtered_topk_when_output_is_wide() -> Result<()> {
    let plan = filtered_topk_plan()?;
    let optimized = LateMaterialization::new().optimize(plan, &ConfigOptions::new())?;
    let display = displayable(optimized.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");
    assert_contains!(&display, "FilterExec: e@1");
    assert_contains!(&display, "RowNumberExec");
    Ok(())
}

#[tokio::test]
async fn sql_parquet_select_star_order_by_limit_uses_late_materialization() -> Result<()>
{
    let test_file = make_topk_parquet()?;
    let ctx = topk_context(&test_file, true).await?;

    let dataframe = ctx.sql("SELECT * FROM t ORDER BY c LIMIT 10").await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");
    assert_contains!(&display, "projection=[c]");

    let batches = dataframe.collect().await?;
    assert_batches_eq!(
        [
            "+----+---+---------+",
            "| id | c | payload |",
            "+----+---+---------+",
            "| 19 | 0 | p19     |",
            "| 18 | 1 | p18     |",
            "| 17 | 2 | p17     |",
            "| 16 | 3 | p16     |",
            "| 15 | 4 | p15     |",
            "| 14 | 5 | p14     |",
            "| 13 | 6 | p13     |",
            "| 12 | 7 | p12     |",
            "| 11 | 8 | p11     |",
            "| 10 | 9 | p10     |",
            "+----+---+---------+",
        ],
        &batches
    );

    Ok(())
}

#[tokio::test]
async fn sql_parquet_filtered_select_star_order_by_limit_uses_late_materialization()
-> Result<()> {
    let test_file = make_topk_parquet()?;
    let ctx = topk_context(&test_file, true).await?;

    let dataframe = ctx
        .sql("SELECT * FROM t WHERE payload <> 'p19' ORDER BY c LIMIT 10")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");
    assert_contains!(&display, "FilterExec:");

    let batches = dataframe.collect().await?;
    assert_filtered_topk_batches(&batches);

    Ok(())
}

#[tokio::test]
async fn sql_parquet_filtered_byte_range_select_star_order_by_limit_uses_late_materialization()
-> Result<()> {
    let test_file = make_topk_parquet()?;
    let config = topk_config(true)
        .with_target_partitions(2)
        .with_repartition_file_min_size(1);
    let ctx = topk_context_with_config(&test_file, config).await?;

    let dataframe = ctx
        .sql("SELECT * FROM t WHERE payload <> 'p19' ORDER BY c LIMIT 10")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");
    assert_contains!(&display, "PartitionColumnExec");
    assert!(!display.contains("RowNumberExec"));

    let batches = dataframe.collect().await?;
    assert_filtered_topk_batches(&batches);

    Ok(())
}

#[tokio::test]
async fn sql_parquet_filtered_projection_rebinds_to_full_schema() -> Result<()> {
    let test_file = make_topk_parquet_with_date()?;
    let ctx = topk_context(&test_file, true).await?;

    let dataframe = ctx
        .sql("SELECT payload, id, c FROM t WHERE payload <> 'p19' ORDER BY c LIMIT 10")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");

    let batches = dataframe.collect().await?;
    assert_batches_eq!(
        [
            "+---------+----+----+",
            "| payload | id | c  |",
            "+---------+----+----+",
            "| p18     | 18 | 1  |",
            "| p17     | 17 | 2  |",
            "| p16     | 16 | 3  |",
            "| p15     | 15 | 4  |",
            "| p14     | 14 | 5  |",
            "| p13     | 13 | 6  |",
            "| p12     | 12 | 7  |",
            "| p11     | 11 | 8  |",
            "| p10     | 10 | 9  |",
            "| p9      | 9  | 10 |",
            "+---------+----+----+",
        ],
        &batches
    );

    Ok(())
}

#[tokio::test]
async fn sql_parquet_filtered_view_cast_order_by_limit_uses_late_materialization()
-> Result<()> {
    let test_file = make_topk_parquet_with_date()?;
    let ctx = topk_context(&test_file, true).await?;
    ctx.sql(
        "CREATE VIEW v AS \
         SELECT * EXCEPT(event_date), \
         CAST(CAST(event_date AS INTEGER) AS DATE) AS event_date FROM t",
    )
    .await?
    .collect()
    .await?;

    let dataframe = ctx
        .sql("SELECT * FROM v WHERE payload <> 'p19' ORDER BY c LIMIT 10")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "LateTopKMaterializationExec: fetch=10");

    let batches = dataframe.collect().await?;
    let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(row_count, 10);

    Ok(())
}

#[tokio::test]
async fn sql_parquet_select_star_order_by_limit_respects_disabled_config() -> Result<()> {
    let test_file = make_topk_parquet()?;
    let ctx = topk_context(&test_file, false).await?;

    let dataframe = ctx.sql("SELECT * FROM t ORDER BY c LIMIT 10").await?;
    let plan = dataframe.create_physical_plan().await?;
    let display = displayable(plan.as_ref()).indent(false).to_string();

    assert_contains!(&display, "SortExec: TopK(fetch=10)");
    assert!(!display.contains("LateTopKMaterializationExec"));

    Ok(())
}

fn topk_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let schema = schema();
    let scan = parquet_exec(Arc::clone(&schema));
    let sort_exprs = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }])
    .unwrap();

    Ok(Arc::new(
        SortExec::new(sort_exprs, scan).with_fetch(Some(10)),
    ))
}

fn filtered_topk_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let schema = schema();
    let scan = parquet_exec(Arc::clone(&schema));
    let filter = Arc::new(FilterExec::try_new(col("e", &schema)?, scan)?);
    let sort_exprs = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }])
    .unwrap();

    Ok(Arc::new(
        SortExec::new(sort_exprs, filter).with_fetch(Some(10)),
    ))
}

async fn topk_context(
    test_file: &NamedTempFile,
    late_materialization_enabled: bool,
) -> Result<SessionContext> {
    topk_context_with_config(test_file, topk_config(late_materialization_enabled)).await
}

fn topk_config(late_materialization_enabled: bool) -> SessionConfig {
    SessionConfig::new().with_collect_statistics(true).set_bool(
        "datafusion.optimizer.enable_row_number_topk_late_materialization",
        late_materialization_enabled,
    )
}

async fn topk_context_with_config(
    test_file: &NamedTempFile,
    config: SessionConfig,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "t",
        test_file.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
}

fn assert_filtered_topk_batches(batches: &[RecordBatch]) {
    assert_batches_eq!(
        [
            "+----+----+---------+",
            "| id | c  | payload |",
            "+----+----+---------+",
            "| 18 | 1  | p18     |",
            "| 17 | 2  | p17     |",
            "| 16 | 3  | p16     |",
            "| 15 | 4  | p15     |",
            "| 14 | 5  | p14     |",
            "| 13 | 6  | p13     |",
            "| 12 | 7  | p12     |",
            "| 11 | 8  | p11     |",
            "| 10 | 9  | p10     |",
            "| 9  | 10 | p9      |",
            "+----+----+---------+",
        ],
        batches
    );
}

fn make_topk_parquet() -> Result<NamedTempFile> {
    let mut test_file = tempfile::Builder::new().suffix(".parquet").tempfile()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = (0..20).collect::<Vec<_>>();
    let sort_values = (0..20).rev().collect::<Vec<_>>();
    let payloads = (0..20).map(|value| format!("p{value}")).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(sort_values)),
            Arc::new(StringArray::from(payloads)),
        ],
    )?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build();
    let mut writer = ArrowWriter::try_new(&mut test_file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(test_file)
}

fn make_topk_parquet_with_date() -> Result<NamedTempFile> {
    let mut test_file = tempfile::Builder::new().suffix(".parquet").tempfile()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("event_date", DataType::UInt16, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = (0..20).collect::<Vec<_>>();
    let sort_values = (0..20).rev().collect::<Vec<_>>();
    let dates = (0..20).map(|value| value as u16).collect::<Vec<_>>();
    let payloads = (0..20).map(|value| format!("p{value}")).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(sort_values)),
            Arc::new(UInt16Array::from(dates)),
            Arc::new(StringArray::from(payloads)),
        ],
    )?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build();
    let mut writer = ArrowWriter::try_new(&mut test_file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(test_file)
}
