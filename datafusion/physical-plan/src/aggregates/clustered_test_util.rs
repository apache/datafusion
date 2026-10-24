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

//! Test helpers for the clustered aggregate streams under memory pressure.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::test_util::batches_to_string;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::StreamExt;

use super::{AggregateExec, AggregateMode, GroupClusteringMode, PhysicalGroupBy};
use crate::ExecutionPlan;
use crate::test::TestMemoryExec;

/// Key columns of two batches sorted on `k` that share the boundary key 256, so
/// groups complete within each batch under full group clustering.
pub(super) fn full_batches() -> Vec<Vec<Vec<i64>>> {
    vec![vec![(0..=256).collect()], vec![(256..=512).collect()]]
}

/// Key columns `(a, b)` of batches sorted on `a` only, with 256 groups for each
/// `a` spread over two batches. The groups of one `a` complete together when the
/// next `a` starts.
pub(super) fn partial_batches() -> Vec<Vec<Vec<i64>>> {
    [0, 0, 1, 1, 2, 2]
        .into_iter()
        .map(|a| vec![vec![a; 256], (0..256).collect()])
        .collect()
}

/// Key column of two batches that hold the same single key, so no group
/// completes before the input ends.
pub(super) fn single_key_batches() -> Vec<Vec<Vec<i64>>> {
    vec![vec![vec![0; 512]]; 2]
}

/// Builds `SUM(v) GROUP BY <keys>` in `mode` over `batches` of `(keys..., v)`
/// columns, where `v` equals the last key. The input is declared sorted on the
/// first key only, so one key gives full group clustering and more keys give
/// partial group clustering.
///
/// For `Final` mode, the batches are the partial states, since the state of
/// `SUM(v)` is a single `Int64` column.
pub(super) fn sum_over_sorted_input(
    mode: AggregateMode,
    keys: &[&str],
    batches: Vec<Vec<Vec<i64>>>,
) -> Result<(Arc<dyn ExecutionPlan>, GroupClusteringMode)> {
    let fields = keys
        .iter()
        .chain(&["v"])
        .map(|name| Field::new(*name, DataType::Int64, false))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let group_by = PhysicalGroupBy::new_single(
        keys.iter()
            .map(|key| Ok((col(key, &schema)?, key.to_string())))
            .collect::<Result<_>>()?,
    );
    let aggregate_exprs = vec![Arc::new(
        AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("sum")
            .build()?,
    )];

    let (input_schema, group_by) = if mode == AggregateMode::Final {
        let empty = TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregate_exprs.clone(),
            vec![None],
            empty,
            Arc::clone(&schema),
        )?;
        (partial.schema(), group_by.as_final())
    } else {
        (Arc::clone(&schema), group_by)
    };

    let batches = batches
        .into_iter()
        .map(|columns| {
            let v = columns.last().cloned().unwrap_or_default();
            let columns = columns
                .into_iter()
                .chain([v])
                .map(|values| Arc::new(Int64Array::from(values)) as _)
                .collect();
            RecordBatch::try_new(Arc::clone(&input_schema), columns)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ordering =
        LexOrdering::new([PhysicalSortExpr::new_default(col(keys[0], &input_schema)?)])
            .unwrap();
    let input = TestMemoryExec::try_new(&[batches], Arc::clone(&input_schema), None)?
        .try_with_sort_information(vec![ordering])?;
    let aggregate = AggregateExec::try_new(
        mode,
        group_by,
        aggregate_exprs,
        vec![None],
        Arc::new(TestMemoryExec::update_cache(&Arc::new(input))),
        schema,
    )?;
    let group_clustering_mode = aggregate.group_clustering_mode().clone();
    Ok((Arc::new(aggregate), group_clustering_mode))
}

/// Executes partition 0 of `plan` on the clustered aggregate path, with `pool`
/// and temporary files disabled. Returns the output and the most memory the
/// plan held when it handed off a batch, including its initial reservation.
pub(super) async fn collect_with_held_memory(
    plan: &Arc<dyn ExecutionPlan>,
    pool: Arc<dyn MemoryPool>,
) -> Result<(Vec<RecordBatch>, usize)> {
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .set_bool("datafusion.execution.enable_migration_aggregate", true),
            )
            .with_runtime(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::clone(&pool))
                    .with_disk_manager_builder(
                        DiskManagerBuilder::default()
                            .with_mode(DiskManagerMode::Disabled),
                    )
                    .build_arc()?,
            ),
    );
    let mut stream = plan.execute(0, context)?;
    // Each output batch is a single slice, so the stream releases it before
    // handing it off and holds only the table.
    let mut held = pool.reserved();
    let mut output = vec![];
    while let Some(batch) = stream.next().await.transpose()? {
        held = held.max(pool.reserved());
        output.push(batch);
    }
    Ok((output, held))
}

/// Returns the memory `plan` holds when handing off output without a limit.
pub(super) async fn held_memory(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    let (_, held) =
        collect_with_held_memory(plan, Arc::new(UnboundedMemoryPool::default())).await?;
    Ok(held)
}

/// Runs `plan` without a memory limit, then with a limit that fits the table
/// with only the active groups, but not the table before completed groups are
/// emitted. The result must not change.
pub(super) async fn assert_completed_groups_emitted_under_limit(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<()> {
    let (expected, held) =
        collect_with_held_memory(plan, Arc::new(UnboundedMemoryPool::default())).await?;
    let (actual, _) =
        collect_with_held_memory(plan, Arc::new(GreedyMemoryPool::new(held))).await?;
    assert_eq!(batches_to_string(&actual), batches_to_string(&expected));
    Ok(())
}

/// Runs `plan` under `limit`, where emitting completed groups cannot bring the
/// table under the limit, so the refused reservation must be returned.
pub(super) async fn assert_resources_exhausted_under_limit(
    plan: &Arc<dyn ExecutionPlan>,
    limit: usize,
) -> Result<()> {
    let result =
        collect_with_held_memory(plan, Arc::new(GreedyMemoryPool::new(limit))).await;
    assert!(
        matches!(result, Err(DataFusionError::ResourcesExhausted(_))),
        "expected ResourcesExhausted, got {result:?}"
    );
    Ok(())
}
