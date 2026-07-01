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

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_scheduler::{SchedulerConfig, run_distributed};
use std::sync::Arc;

#[tokio::test]
async fn single_stage_scan_filter_matches_collect() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx.sql("SELECT a FROM t WHERE a > 2").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    let expected = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
        .await
        .unwrap();

    let config = SchedulerConfig::in_memory(&ctx);
    let actual = run_distributed(&ctx, plan, config).await.unwrap();

    // order-insensitive compare
    assert_eq!(total_rows(&expected), total_rows(&actual));
    assert_eq!(sorted_values(&expected), sorted_values(&actual));
}

fn total_rows(b: &[RecordBatch]) -> usize {
    b.iter().map(|b| b.num_rows()).sum()
}
fn sorted_values(b: &[RecordBatch]) -> Vec<i32> {
    let mut v: Vec<i32> = b
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    v.sort();
    v
}
