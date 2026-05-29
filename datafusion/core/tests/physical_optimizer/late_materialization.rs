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

use arrow::array::{Int64Array, StringArray};
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

async fn topk_context(
    test_file: &NamedTempFile,
    late_materialization_enabled: bool,
) -> Result<SessionContext> {
    let config = SessionConfig::new().with_collect_statistics(true).set_bool(
        "datafusion.optimizer.enable_row_number_topk_late_materialization",
        late_materialization_enabled,
    );
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "t",
        test_file.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
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
