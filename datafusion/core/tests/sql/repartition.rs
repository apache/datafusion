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

use arrow::array::UInt32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::test_util::UnboundedExec;
use datafusion_common::Result;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExpr;
use futures::StreamExt;
use std::sync::Arc;

/// See <https://github.com/apache/arrow-datafusion/issues/5278>
#[tokio::test]
async fn unbounded_repartition() -> Result<()> {
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);
    let task = ctx.task_ctx();
    let schema = Arc::new(Schema::new(vec![Field::new("a2", DataType::UInt32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt32Array::from(vec![1]))],
    )?;
    let input = Arc::new(UnboundedExec::new(None, batch.clone(), 1));
    let on: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("a2", 0))];
    let plan = Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(on, 3))?);
    let plan = Arc::new(CoalescePartitionsExec::new(plan.clone()));
    let mut stream = plan.execute(0, task)?;

    // Note: `tokio::time::timeout` does NOT help here because in the mentioned issue, the whole runtime is blocked by a
    // CPU-spinning thread. Using a multithread runtime with multiple threads is NOT a solution since this would not
    // trigger the bug (the bug is not specific to a single-thread RT though, it's just the only way to trigger it reliably).
    let batch_actual = stream
        .next()
        .await
        .expect("not terminated")
        .expect("no error in stream");
    assert_eq!(batch_actual, batch);
    Ok(())
}
