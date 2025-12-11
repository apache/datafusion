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

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::assert_batches_eq;
use datafusion_physical_plan::displayable;

#[tokio::test]
async fn utf8_grouping_min_max_limit_fallbacks() -> datafusion_common::Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().optimizer.enable_topk_aggregation = true;
    let ctx = SessionContext::new_with_config(config);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("val_str", DataType::Utf8, false),
            Field::new("val_num", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
            Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    // Supported path: numeric min/max with UTF-8 grouping should still use TopK aggregation
    // and return correct results.
    let supported_df = ctx
        .sql("SELECT g, max(val_num) AS m FROM t GROUP BY g ORDER BY m DESC LIMIT 1")
        .await?;
    let supported_batches = supported_df.collect().await?;
    assert_batches_eq!(
        &[
            "+---+---+",
            "| g | m |",
            "+---+---+",
            "| a | 3 |",
            "+---+---+"
        ],
        &supported_batches
    );

    // Unsupported TopK value type: string min/max should fall back without panicking.
    let unsupported_df = ctx
        .sql("SELECT g, max(val_str) AS s FROM t GROUP BY g ORDER BY s DESC LIMIT 1")
        .await?;
    let unsupported_plan = unsupported_df.clone().create_physical_plan().await?;
    let unsupported_batches = unsupported_df.collect().await?;

    // Ensure the plan avoided the TopK-specific stream implementation.
    let plan_display = displayable(unsupported_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !plan_display.contains("GroupedTopKAggregateStream"),
        "Unsupported UTF-8 aggregate value should not use TopK: {plan_display}"
    );

    assert_batches_eq!(
        &[
            "+---+---------+",
            "| g | s       |",
            "+---+---------+",
            "| a | charlie |",
            "+---+---------+"
        ],
        &unsupported_batches
    );

    Ok(())
}
