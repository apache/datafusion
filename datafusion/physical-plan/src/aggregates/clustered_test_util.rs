// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Test helpers for the clustered aggregate streams under memory pressure.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use datafusion_expr::AggregateUDF;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::StreamExt;

use super::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::ExecutionPlan;
use crate::test::TestMemoryExec;

/// Key columns of two batches sorted on `k` that share the boundary key 256, so
/// groups complete within each batch under full group clustering.
pub(super) fn full_batches() -> Vec<Vec<Vec<i64>>> {
    vec![vec![(0..=256).collect()], vec![(256..=512).collect()]]
}

/// Key columns of three batches of 8192 distinct keys sorted on `k`, where each
/// batch shares its first key with the previous one. Aggregating a batch grows
/// the table for about 8192 groups, while the active group needs a few KB.
pub(super) fn distinct_key_batches() -> Vec<Vec<Vec<i64>>> {
    vec![
        vec![(0..8193).collect()],
        vec![(8192..16385).collect()],
        vec![(16384..24576).collect()],
    ]
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
) -> Result<AggregateExec> {
    aggregate_over_sorted_input(mode, keys, batches, sum_udaf())
}

/// Like [`sum_over_sorted_input`], but aggregates `v` with `udaf`. For `Final`
/// mode, the state of `udaf` must be a single `Int64` column.
pub(super) fn aggregate_over_sorted_input(
    mode: AggregateMode,
    keys: &[&str],
    batches: Vec<Vec<Vec<i64>>>,
    udaf: Arc<AggregateUDF>,
) -> Result<AggregateExec> {
    aggregate_expr_over_sorted_input(mode, keys, batches, udaf, |builder, _| Ok(builder))
}

/// Like [`aggregate_over_sorted_input`], but `configure` can change the
/// aggregate expression, given the input schema. For example, it can add
/// `ORDER BY v`.
pub(super) fn aggregate_expr_over_sorted_input(
    mode: AggregateMode,
    keys: &[&str],
    batches: Vec<Vec<Vec<i64>>>,
    udaf: Arc<AggregateUDF>,
    configure: impl FnOnce(AggregateExprBuilder, &SchemaRef) -> Result<AggregateExprBuilder>,
) -> Result<AggregateExec> {
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
        configure(
            AggregateExprBuilder::new(Arc::clone(&udaf), vec![col("v", &schema)?]),
            &schema,
        )?
        .schema(Arc::clone(&schema))
        .alias(udaf.name())
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
            let mut columns = columns
                .into_iter()
                .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
                .collect::<Vec<_>>();
            columns.push(Arc::clone(columns.last().unwrap()));
            RecordBatch::try_new(Arc::clone(&input_schema), columns)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ordering =
        LexOrdering::new([PhysicalSortExpr::new_default(col(keys[0], &input_schema)?)])
            .unwrap();
    let input = TestMemoryExec::try_new(&[batches], Arc::clone(&input_schema), None)?
        .try_with_sort_information(vec![ordering])?;
    AggregateExec::try_new(
        mode,
        group_by,
        aggregate_exprs,
        vec![None],
        Arc::new(TestMemoryExec::update_cache(&Arc::new(input))),
        schema,
    )
}

/// Executes partition 0 of `plan` on the clustered aggregate path, with `pool`
/// and temporary files disabled. Returns the output and the most memory the
/// plan held when it handed off a batch, including its initial reservation.
async fn collect_with_held_memory(
    plan: &AggregateExec,
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

/// Runs `plan` under `limit`.
pub(super) async fn collect_under_limit(
    plan: &AggregateExec,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    let (output, _) =
        collect_with_held_memory(plan, Arc::new(GreedyMemoryPool::new(limit))).await?;
    Ok(output)
}

/// Returns the memory `plan` holds when handing off output without a limit.
pub(super) async fn held_memory(plan: &AggregateExec) -> Result<usize> {
    let (_, held) =
        collect_with_held_memory(plan, Arc::new(UnboundedMemoryPool::default())).await?;
    Ok(held)
}

/// Runs `plan` without a memory limit, then with a limit that fits the table
/// with only the active groups, but not the table before completed groups are
/// emitted. The result must not change.
pub(super) async fn assert_completed_groups_emitted_under_limit(
    plan: &AggregateExec,
) -> Result<()> {
    let (expected, held) =
        collect_with_held_memory(plan, Arc::new(UnboundedMemoryPool::default())).await?;
    let (actual, _) =
        collect_with_held_memory(plan, Arc::new(GreedyMemoryPool::new(held))).await?;
    assert_eq!(batches_to_string(&actual), batches_to_string(&expected));
    Ok(())
}

/// Runs `plan` without a memory limit, then under `limit`. The result must not
/// change.
pub(super) async fn assert_output_unchanged_under_limit(
    plan: &AggregateExec,
    limit: usize,
) -> Result<()> {
    let (expected, _) =
        collect_with_held_memory(plan, Arc::new(UnboundedMemoryPool::default())).await?;
    let (actual, _) =
        collect_with_held_memory(plan, Arc::new(GreedyMemoryPool::new(limit))).await?;
    assert_eq!(batches_to_string(&actual), batches_to_string(&expected));
    Ok(())
}

/// Runs `plan` under `limit`, where emitting completed groups cannot bring the
/// table under the limit, so the refused reservation must be returned.
pub(super) async fn assert_resources_exhausted_under_limit(
    plan: &AggregateExec,
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
