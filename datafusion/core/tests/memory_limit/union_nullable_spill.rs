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

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::sort_batch;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, Partitioning};
use futures::StreamExt;

const NUM_BATCHES: usize = 200;
const ROWS_PER_BATCH: usize = 10;

fn non_nullable_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn nullable_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]))
}

fn non_nullable_batches() -> Vec<RecordBatch> {
    (0..NUM_BATCHES)
        .map(|i| {
            let start = (i * ROWS_PER_BATCH) as i64;
            let keys: Vec<i64> = (start..start + ROWS_PER_BATCH as i64).collect();
            RecordBatch::try_new(
                non_nullable_schema(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(vec![0i64; ROWS_PER_BATCH])),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn nullable_batches() -> Vec<RecordBatch> {
    (0..NUM_BATCHES)
        .map(|i| {
            let start = (i * ROWS_PER_BATCH) as i64;
            let keys: Vec<i64> = (start..start + ROWS_PER_BATCH as i64).collect();
            let vals: Vec<Option<i64>> = (0..ROWS_PER_BATCH)
                .map(|j| if j % 3 == 1 { None } else { Some(j as i64) })
                .collect();
            RecordBatch::try_new(
                nullable_schema(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(vals)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn build_task_ctx(pool_size: usize) -> Arc<datafusion_execution::TaskContext> {
    let session_config = SessionConfig::new().with_batch_size(2);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(pool_size)))
        .build_arc()
        .unwrap();
    Arc::new(
        datafusion_execution::TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(runtime),
    )
}

/// Exercises spilling through UnionExec -> RepartitionExec where union children
/// have mismatched nullability (one child's `val` is non-nullable, the other's
/// is nullable with NULLs). A tiny FairSpillPool forces all batches to spill.
///
/// UnionExec returns child streams without schema coercion, so batches from
/// different children carry different per-field nullability into the shared
/// SpillPool. The IPC writer must use the SpillManager's canonical (nullable)
/// schema — not the first batch's schema — so readback batches are valid.
///
/// Otherwise, sort_batch will panic with
/// `Column 'val' is declared as non-nullable but contains null values`
#[tokio::test]
async fn test_sort_union_repartition_spill_mixed_nullability() {
    let non_nullable_exec = MemorySourceConfig::try_new_exec(
        &[non_nullable_batches()],
        non_nullable_schema(),
        None,
    )
    .unwrap();

    let nullable_exec =
        MemorySourceConfig::try_new_exec(&[nullable_batches()], nullable_schema(), None)
            .unwrap();

    let union_exec = UnionExec::try_new(vec![non_nullable_exec, nullable_exec]).unwrap();
    assert!(union_exec.schema().field(1).is_nullable());

    let repartition = Arc::new(
        RepartitionExec::try_new(union_exec, Partitioning::RoundRobinBatch(1)).unwrap(),
    );

    let task_ctx = build_task_ctx(200);
    let mut stream = repartition.execute(0, task_ctx).unwrap();

    let sort_expr = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("key", &nullable_schema()).unwrap(),
        options: SortOptions::default(),
    }])
    .unwrap();

    let mut total_rows = 0usize;
    let mut total_nulls = 0usize;
    while let Some(result) = stream.next().await {
        let batch = result.unwrap();

        let batch = sort_batch(&batch, &sort_expr, None).unwrap();

        total_rows += batch.num_rows();
        total_nulls += batch.column(1).null_count();
    }

    assert_eq!(
        total_rows,
        NUM_BATCHES * ROWS_PER_BATCH * 2,
        "All rows from both UNION branches should be present"
    );
    assert!(
        total_nulls > 0,
        "Expected some null values in output (i.e. nullable batches were processed)"
    );
}
