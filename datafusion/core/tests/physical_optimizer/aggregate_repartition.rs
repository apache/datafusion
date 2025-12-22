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

//! Regression test for ensuring repartitioning between chained aggregates.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_expr::col;
use datafusion_functions_aggregate::expr_fn::count;
use datafusion_physical_plan::{collect, displayable};

#[tokio::test]
async fn repartition_between_chained_aggregates() -> Result<()> {
    // Build a two-partition, empty MemTable with the expected schema to mimic the
    // reported failing plan: Sort -> Aggregate(ts, region) -> Sort -> Aggregate(ts).
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let empty_batch = RecordBatch::new_empty(schema.clone());
    let partitions = vec![vec![empty_batch.clone()], vec![empty_batch]];
    let mem_table = MemTable::try_new(schema, partitions)?;

    let config = SessionConfig::new().with_target_partitions(2);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("metrics", Arc::new(mem_table))?;

    let df = ctx
        .table("metrics")
        .await?
        .sort(vec![
            col("ts").sort(true, true),
            col("region").sort(true, true),
        ])?
        .aggregate(vec![col("ts"), col("region")], vec![count(col("value"))])?
        .sort(vec![
            col("ts").sort(true, true),
            col("region").sort(true, true),
        ])?
        .aggregate(vec![col("ts")], vec![count(col("region"))])?;

    let physical_plan = df.create_physical_plan().await?;

    // The optimizer should either keep the stream single-partitioned via the
    // sort-preserving merge, or insert a repartition between the two aggregates
    // so that the second grouping sees a consistent hash distribution. Either
    // path protects against the panic that was previously reported for this
    // plan shape.
    let plan_display = displayable(physical_plan.as_ref()).indent(true).to_string();
    let has_repartition =
        plan_display.contains("RepartitionExec: partitioning=Hash([ts@0], 2)");
    assert!(
        has_repartition || plan_display.contains("SortPreservingMergeExec"),
        "Expected either a repartition between aggregates or a sort-preserving merge chain"
    );

    // Execute the optimized plan to ensure the empty, multi-partition pipeline does not panic.
    let batches = collect(physical_plan, ctx.task_ctx()).await?;
    assert!(batches.is_empty());

    Ok(())
}
