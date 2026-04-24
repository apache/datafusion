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

//! The [`JoinSelection`] rule tries to modify a given plan so that it can
//! accommodate infinite sources and utilize statistical information (if there
//! is any) to obtain more performant plans. To achieve the first goal, it
//! tries to transform a non-runnable query (with the given infinite sources)
//! into a runnable query by replacing pipeline-breaking join operations with
//! pipeline-friendly ones. To achieve the second goal, it selects the proper
//! `PartitionMode` and the build side using the available statistics for hash joins.

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::Statistics;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{JoinSide, JoinType, NullEquality, internal_err};
use datafusion_expr_common::sort_properties::SortProperties;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{LexOrdering, PhysicalExprRef};
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::joins::utils::ColumnIndex;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, JoinOn, NestedLoopJoinExec, PartitionMode,
    StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

const MAX_DP_JOIN_REORDER_INPUTS: usize = 8;
const UNKNOWN_JOIN_REORDER_COST: f64 = 1_000_000_000_000.0;

/// The [`JoinSelection`] rule tries to modify a given plan so that it can
/// accommodate infinite sources and optimize joins in the plan according to
/// available statistical information, if there is any.
#[derive(Default, Debug)]
pub struct JoinSelection {}

impl JoinSelection {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Get statistics for a plan node, using the registry if available.
fn get_stats(
    plan: &dyn ExecutionPlan,
    registry: Option<&StatisticsRegistry>,
) -> Result<Arc<Statistics>> {
    if let Some(reg) = registry {
        reg.compute(plan)
            .map(|s| Arc::<Statistics>::clone(s.base_arc()))
    } else {
        plan.partition_statistics(None)
    }
}

// TODO: We need some performance test for Right Semi/Right Join swap to Left Semi/Left Join in case that the right side is smaller but not much smaller.
// TODO: In PrestoSQL, the optimizer flips join sides only if one side is much smaller than the other by more than SIZE_DIFFERENCE_THRESHOLD times, by default is 8 times.
/// Checks whether join inputs should be swapped using available statistics.
///
/// It follows these steps:
/// 1. If a [`StatisticsRegistry`] is provided, use it for cross-operator estimates
///    (e.g., intermediate join outputs that would otherwise have `Absent` statistics).
/// 2. Compare the in-memory sizes of both sides, and place the smaller side on
///    the left (build) side.
/// 3. If in-memory byte sizes are unavailable, fall back to row counts.
/// 4. Do not reorder the join if neither statistic is available, or if
///    `datafusion.optimizer.join_reordering` is disabled.
///
/// Used configurations inside arg `config`
/// - `config.optimizer.join_reordering`: allows or forbids statistics-driven join swapping
pub(crate) fn should_swap_join_order(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
    config: &ConfigOptions,
    registry: Option<&StatisticsRegistry>,
) -> Result<bool> {
    if !config.optimizer.join_reordering {
        return Ok(false);
    }

    let left_stats = get_stats(left, registry)?;
    let right_stats = get_stats(right, registry)?;

    // First compare total_byte_size, then fall back to num_rows if byte
    // sizes are unavailable.
    match (
        left_stats.total_byte_size.get_value(),
        right_stats.total_byte_size.get_value(),
    ) {
        (Some(l), Some(r)) => Ok(l > r),
        _ => match (
            left_stats.num_rows.get_value(),
            right_stats.num_rows.get_value(),
        ) {
            (Some(l), Some(r)) => Ok(l > r),
            _ => Ok(false),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct LeafColumn {
    input: usize,
    index: usize,
}

#[derive(Clone)]
struct JoinInput {
    plan: Arc<dyn ExecutionPlan>,
    columns: Vec<LeafColumn>,
}

#[derive(Clone, Copy, Debug)]
struct JoinEdge {
    left: LeafColumn,
    right: LeafColumn,
}

struct JoinIsland {
    inputs: Vec<JoinInput>,
    edges: Vec<JoinEdge>,
    output_columns: Vec<LeafColumn>,
    output_schema: SchemaRef,
    null_equality: NullEquality,
}

#[derive(Clone)]
struct JoinDpEntry {
    plan: Arc<dyn ExecutionPlan>,
    columns: Vec<LeafColumn>,
    rows: Option<f64>,
    bytes: Option<f64>,
    cost: f64,
}

fn reorder_inner_hash_join_island_subrule(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    registry: Option<&StatisticsRegistry>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if !config.optimizer.join_reordering {
        return Ok(Transformed::no(plan));
    }

    let Some(island) = collect_join_island(&plan) else {
        return Ok(Transformed::no(plan));
    };

    if island.inputs.len() < 3 || island.inputs.len() > MAX_DP_JOIN_REORDER_INPUTS {
        return Ok(Transformed::no(plan));
    }

    let Some(best) = reorder_join_island_with_dp(&island, registry)? else {
        return Ok(Transformed::no(plan));
    };

    let original_cost = estimate_existing_join_cost(&plan, registry)?;
    if best.cost >= original_cost * 0.99 {
        return Ok(Transformed::no(plan));
    }

    let projected = project_to_original_schema(
        best.plan,
        &best.columns,
        &island.output_columns,
        &island.output_schema,
    )?;

    Ok(Transformed::new(projected, true, TreeNodeRecursion::Jump))
}

fn collect_join_island(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinIsland> {
    let root = plan.downcast_ref::<HashJoinExec>()?;
    if !is_reorderable_hash_join(root, None) {
        return None;
    }

    let null_equality = root.null_equality();
    let mut inputs = vec![];
    let mut edges = vec![];
    let output_columns =
        collect_join_island_input(plan, null_equality, &mut inputs, &mut edges)?;

    // Join schema metadata is merged from child schemas and can be sensitive to
    // join tree shape. ProjectionExec preserves input metadata, so skip these
    // rare cases rather than changing the output schema metadata.
    if inputs
        .iter()
        .any(|input| !input.plan.schema().metadata().is_empty())
    {
        return None;
    }

    // Every input must participate in at least one join predicate. This avoids
    // introducing cross joins as part of join reordering.
    if inputs.iter().enumerate().any(|(idx, _)| {
        !edges
            .iter()
            .any(|edge| edge.left.input == idx || edge.right.input == idx)
    }) {
        return None;
    }

    Some(JoinIsland {
        inputs,
        edges,
        output_columns,
        output_schema: plan.schema(),
        null_equality,
    })
}

fn collect_join_island_input(
    plan: &Arc<dyn ExecutionPlan>,
    null_equality: NullEquality,
    inputs: &mut Vec<JoinInput>,
    edges: &mut Vec<JoinEdge>,
) -> Option<Vec<LeafColumn>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && is_reorderable_hash_join(join, Some(null_equality))
    {
        let left_columns =
            collect_join_island_input(join.left(), null_equality, inputs, edges)?;
        let right_columns =
            collect_join_island_input(join.right(), null_equality, inputs, edges)?;

        for (left, right) in join.on() {
            let left = left.downcast_ref::<Column>()?;
            let right = right.downcast_ref::<Column>()?;
            edges.push(JoinEdge {
                left: *left_columns.get(left.index())?,
                right: *right_columns.get(right.index())?,
            });
        }

        let joined_columns: Vec<_> =
            left_columns.into_iter().chain(right_columns).collect();
        return match &join.projection {
            Some(projection) => projection
                .iter()
                .map(|idx| joined_columns.get(*idx).copied())
                .collect(),
            None => Some(joined_columns),
        };
    }

    let input = inputs.len();
    let columns = (0..plan.schema().fields().len())
        .map(|index| LeafColumn { input, index })
        .collect::<Vec<_>>();
    inputs.push(JoinInput {
        plan: Arc::clone(plan),
        columns: columns.clone(),
    });
    Some(columns)
}

fn is_reorderable_hash_join(
    join: &HashJoinExec,
    null_equality: Option<NullEquality>,
) -> bool {
    matches!(join.join_type(), JoinType::Inner)
        && join.filter().is_none()
        && !join.null_aware
        && !join.on().is_empty()
        && null_equality.is_none_or(|expected| join.null_equality() == expected)
        && join.on().iter().all(|(left, right)| {
            left.downcast_ref::<Column>().is_some()
                && right.downcast_ref::<Column>().is_some()
        })
}

fn reorder_join_island_with_dp(
    island: &JoinIsland,
    registry: Option<&StatisticsRegistry>,
) -> Result<Option<JoinDpEntry>> {
    let input_count = island.inputs.len();
    let full_mask = (1usize << input_count) - 1;
    let mut best: Vec<Option<JoinDpEntry>> = vec![None; full_mask + 1];

    for (idx, input) in island.inputs.iter().enumerate() {
        let mask = 1usize << idx;
        best[mask] = Some(JoinDpEntry::leaf(
            Arc::clone(&input.plan),
            input.columns.clone(),
            registry,
        )?);
    }

    for mask in 1usize..=full_mask {
        if mask.count_ones() < 2 {
            continue;
        }

        let first_bit = 1usize << mask.trailing_zeros();
        let mut left_mask = (mask - 1) & mask;
        while left_mask > 0 {
            let right_mask = mask ^ left_mask;
            // Keep enumeration deterministic and avoid considering both
            // equivalent subset partitions. Join orientation is considered
            // separately below.
            if left_mask & first_bit == 0 || right_mask == 0 {
                left_mask = (left_mask - 1) & mask;
                continue;
            }

            if let (Some(left), Some(right)) =
                (best[left_mask].clone(), best[right_mask].clone())
            {
                if let Some(candidate) =
                    build_join_dp_entry(&left, &right, island, registry)?
                {
                    update_best_join(&mut best[mask], candidate);
                }

                if let Some(candidate) =
                    build_join_dp_entry(&right, &left, island, registry)?
                {
                    update_best_join(&mut best[mask], candidate);
                }
            }

            left_mask = (left_mask - 1) & mask;
        }
    }

    Ok(best[full_mask].clone())
}

fn update_best_join(best: &mut Option<JoinDpEntry>, candidate: JoinDpEntry) {
    if best
        .as_ref()
        .is_none_or(|current| candidate.cost < current.cost)
    {
        *best = Some(candidate);
    }
}

fn build_join_dp_entry(
    left: &JoinDpEntry,
    right: &JoinDpEntry,
    island: &JoinIsland,
    registry: Option<&StatisticsRegistry>,
) -> Result<Option<JoinDpEntry>> {
    let on = join_keys_between(left, right, &island.edges);
    if on.is_empty() {
        return Ok(None);
    }

    let plan = Arc::new(HashJoinExec::try_new(
        Arc::clone(&left.plan),
        Arc::clone(&right.plan),
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Auto,
        island.null_equality,
        false,
    )?) as Arc<dyn ExecutionPlan>;

    let columns = left
        .columns
        .iter()
        .chain(right.columns.iter())
        .copied()
        .collect::<Vec<_>>();
    let (rows, bytes) = estimate_join_rows_and_bytes(&plan, left, right, registry)?;
    let left_bytes = entry_bytes(left);
    let right_bytes = entry_bytes(right);
    let output_bytes = bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST);
    let build_bytes = left_bytes;
    let cost =
        left.cost + right.cost + left_bytes + right_bytes + build_bytes + output_bytes;

    Ok(Some(JoinDpEntry {
        plan,
        columns,
        rows,
        bytes: Some(output_bytes),
        cost,
    }))
}

fn join_keys_between(
    left: &JoinDpEntry,
    right: &JoinDpEntry,
    edges: &[JoinEdge],
) -> JoinOn {
    edges
        .iter()
        .filter_map(|edge| {
            if let (Some(left_idx), Some(right_idx)) = (
                column_position(&left.columns, edge.left),
                column_position(&right.columns, edge.right),
            ) {
                Some(make_join_key(left, right, left_idx, right_idx))
            } else if let (Some(left_idx), Some(right_idx)) = (
                column_position(&left.columns, edge.right),
                column_position(&right.columns, edge.left),
            ) {
                Some(make_join_key(left, right, left_idx, right_idx))
            } else {
                None
            }
        })
        .collect()
}

fn make_join_key(
    left: &JoinDpEntry,
    right: &JoinDpEntry,
    left_idx: usize,
    right_idx: usize,
) -> (PhysicalExprRef, PhysicalExprRef) {
    let left_schema = left.plan.schema();
    let right_schema = right.plan.schema();
    (
        Arc::new(Column::new(left_schema.field(left_idx).name(), left_idx)),
        Arc::new(Column::new(right_schema.field(right_idx).name(), right_idx)),
    )
}

fn column_position(columns: &[LeafColumn], needle: LeafColumn) -> Option<usize> {
    columns.iter().position(|column| *column == needle)
}

impl JoinDpEntry {
    fn leaf(
        plan: Arc<dyn ExecutionPlan>,
        columns: Vec<LeafColumn>,
        registry: Option<&StatisticsRegistry>,
    ) -> Result<Self> {
        let (rows, bytes) = estimate_plan_rows_and_bytes(&*plan, registry)?;
        let bytes =
            bytes.or_else(|| rows.map(|rows| rows * schema_row_width(&plan.schema())));
        Ok(Self {
            plan,
            columns,
            rows,
            bytes,
            cost: bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST),
        })
    }
}

fn estimate_join_rows_and_bytes(
    plan: &Arc<dyn ExecutionPlan>,
    left: &JoinDpEntry,
    right: &JoinDpEntry,
    registry: Option<&StatisticsRegistry>,
) -> Result<(Option<f64>, Option<f64>)> {
    let (rows, bytes) = estimate_plan_rows_and_bytes(&**plan, registry)?;
    let rows = rows.or_else(|| match (left.rows, right.rows) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(rows), None) | (None, Some(rows)) => Some(rows),
        (None, None) => None,
    });
    let bytes =
        bytes.or_else(|| rows.map(|rows| rows * schema_row_width(&plan.schema())));
    Ok((rows, bytes))
}

fn estimate_plan_rows_and_bytes(
    plan: &dyn ExecutionPlan,
    registry: Option<&StatisticsRegistry>,
) -> Result<(Option<f64>, Option<f64>)> {
    let stats = get_stats(plan, registry)?;
    let rows = stats.num_rows.get_value().map(|rows| *rows as f64);
    let bytes = stats.total_byte_size.get_value().map(|bytes| *bytes as f64);
    let row_width = schema_row_width(&plan.schema());
    let rows = rows.or_else(|| bytes.map(|bytes| bytes / row_width.max(1.0)));
    let bytes = bytes.or_else(|| rows.map(|rows| rows * row_width));
    Ok((rows, bytes))
}

fn estimate_existing_join_cost(
    plan: &Arc<dyn ExecutionPlan>,
    registry: Option<&StatisticsRegistry>,
) -> Result<f64> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && is_reorderable_hash_join(join, None)
    {
        let left_cost = estimate_existing_join_cost(join.left(), registry)?;
        let right_cost = estimate_existing_join_cost(join.right(), registry)?;
        let (_, bytes) = estimate_plan_rows_and_bytes(&**plan, registry)?;
        let (_, left_bytes) = estimate_plan_rows_and_bytes(&**join.left(), registry)?;
        let (_, right_bytes) = estimate_plan_rows_and_bytes(&**join.right(), registry)?;
        let left_bytes = left_bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST);
        let right_bytes = right_bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST);
        let build_bytes = left_bytes;
        return Ok(left_cost
            + right_cost
            + left_bytes
            + right_bytes
            + build_bytes
            + bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST));
    }

    let (_, bytes) = estimate_plan_rows_and_bytes(&**plan, registry)?;
    Ok(bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST))
}

fn entry_bytes(entry: &JoinDpEntry) -> f64 {
    entry.bytes.unwrap_or(UNKNOWN_JOIN_REORDER_COST)
}

fn project_to_original_schema(
    plan: Arc<dyn ExecutionPlan>,
    columns: &[LeafColumn],
    output_columns: &[LeafColumn],
    output_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if columns == output_columns && plan.schema().as_ref() == output_schema.as_ref() {
        return Ok(plan);
    }

    let input_schema = plan.schema();
    let mut projection = Vec::with_capacity(output_columns.len());
    for (output_idx, output_column) in output_columns.iter().enumerate() {
        let Some(input_idx) = column_position(columns, *output_column) else {
            return internal_err!(
                "Unable to preserve join output column during join reordering"
            );
        };
        projection.push(ProjectionExpr {
            expr: Arc::new(Column::new(input_schema.field(input_idx).name(), input_idx))
                as PhysicalExprRef,
            alias: output_schema.field(output_idx).name().clone(),
        });
    }

    Ok(Arc::new(ProjectionExec::try_new(projection, plan)?))
}

fn schema_row_width(schema: &Schema) -> f64 {
    schema
        .fields()
        .iter()
        .map(|field| data_type_width(field.data_type()) as f64)
        .sum::<f64>()
        .max(1.0)
}

fn data_type_width(data_type: &DataType) -> usize {
    if let Some(width) = data_type.primitive_width() {
        return width;
    }

    match data_type {
        DataType::Boolean => 1,
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => 16,
        DataType::FixedSizeBinary(size) => *size as usize,
        DataType::Dictionary(_, value_type) => data_type_width(value_type),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| data_type_width(field.data_type()))
            .sum::<usize>()
            .max(1),
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => 16,
        DataType::Null => 1,
        _ => 8,
    }
}

fn supports_collect_by_thresholds(
    plan: &dyn ExecutionPlan,
    threshold_byte_size: usize,
    threshold_num_rows: usize,
    registry: Option<&StatisticsRegistry>,
) -> bool {
    let Ok(stats) = get_stats(plan, registry) else {
        return false;
    };

    // Stats use `Precision<T>` to represent stats, where `Absent` means unknown.
    // `Exact(0)` and `Inexact(0)` are both valid stats, and we should not treat
    // them as unknown, `Absent` will return None (this is in regards to why
    // `!=0` is not checked)
    if let Some(byte_size) = stats.total_byte_size.get_value() {
        *byte_size < threshold_byte_size
    } else if let Some(num_rows) = stats.num_rows.get_value() {
        *num_rows < threshold_num_rows
    } else {
        false
    }
}

impl PhysicalOptimizerRule for JoinSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.config_options();
        let mut default_registry = None;
        let registry: Option<&StatisticsRegistry> =
            if config.optimizer.use_statistics_registry {
                Some(context.statistics_registry().unwrap_or_else(|| {
                    default_registry
                        .insert(StatisticsRegistry::default_with_builtin_providers())
                }))
            } else {
                None
            };
        let new_plan = plan
            .transform_down(|plan| {
                reorder_inner_hash_join_island_subrule(plan, config, registry)
            })
            .data()?;
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let new_plan = new_plan
            .transform_up(|p| apply_subrules(p, &subrules, config))
            .data()?;
        new_plan
            .transform_up(|plan| {
                statistical_join_selection_subrule(plan, config, registry)
            })
            .data()
    }

    fn name(&self) -> &str {
        "join_selection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to create a [`HashJoinExec`] in [`PartitionMode::CollectLeft`] when possible.
///
/// This function will first consider the given join type and check whether the
/// `CollectLeft` mode is applicable. Otherwise, it will try to swap the join sides.
/// When the `ignore_threshold` is false, this function will also check left
/// and right sizes in bytes or rows.
///
/// Used configurations inside arg `config`
/// - `config.optimizer.hash_join_single_partition_threshold`: byte threshold for `CollectLeft`
/// - `config.optimizer.hash_join_single_partition_threshold_rows`: row threshold for `CollectLeft`
/// - `config.optimizer.join_reordering`: allows or forbids input swapping
pub(crate) fn try_collect_left(
    hash_join: &HashJoinExec,
    ignore_threshold: bool,
    config: &ConfigOptions,
    registry: Option<&StatisticsRegistry>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left = hash_join.left();
    let right = hash_join.right();
    let optimizer_config = &config.optimizer;

    let left_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**left,
            optimizer_config.hash_join_single_partition_threshold,
            optimizer_config.hash_join_single_partition_threshold_rows,
            registry,
        );
    let right_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**right,
            optimizer_config.hash_join_single_partition_threshold,
            optimizer_config.hash_join_single_partition_threshold_rows,
            registry,
        );

    match (left_can_collect, right_can_collect) {
        (true, true) => {
            // Don't swap null-aware anti joins as they have specific side requirements
            if hash_join.join_type().supports_swap()
                && !hash_join.null_aware
                && should_swap_join_order(&**left, &**right, config, registry)?
            {
                Ok(Some(hash_join.swap_inputs(PartitionMode::CollectLeft)?))
            } else {
                Ok(Some(Arc::new(
                    hash_join
                        .builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .build()?,
                )))
            }
        }
        (true, false) => Ok(Some(Arc::new(
            hash_join
                .builder()
                .with_partition_mode(PartitionMode::CollectLeft)
                .build()?,
        ))),
        (false, true) => {
            // Don't swap null-aware anti joins as they have specific side requirements
            if optimizer_config.join_reordering
                && hash_join.join_type().supports_swap()
                && !hash_join.null_aware
            {
                hash_join.swap_inputs(PartitionMode::CollectLeft).map(Some)
            } else {
                Ok(None)
            }
        }
        (false, false) => Ok(None),
    }
}

/// Creates a partitioned hash join execution plan, swapping inputs if beneficial.
///
/// Checks if the join order should be swapped based on the join type and input statistics.
/// If swapping is optimal and supported, creates a swapped partitioned hash join; otherwise,
/// creates a standard partitioned hash join.
///
/// Used configurations inside arg `config`
/// - `config.optimizer.join_reordering`: allows or forbids statistics-driven join swapping
pub(crate) fn partitioned_hash_join(
    hash_join: &HashJoinExec,
    config: &ConfigOptions,
    registry: Option<&StatisticsRegistry>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    // Don't swap null-aware anti joins as they have specific side requirements
    if hash_join.join_type().supports_swap()
        && !hash_join.null_aware
        && should_swap_join_order(&**left, &**right, config, registry)?
    {
        hash_join.swap_inputs(PartitionMode::Partitioned)
    } else {
        // Null-aware anti joins must use CollectLeft mode because they track probe-side state
        // (probe_side_non_empty, probe_side_has_null) per-partition, but need global knowledge
        // for correct null handling. With partitioning, a partition might not see probe rows
        // even if the probe side is globally non-empty, leading to incorrect NULL row handling.
        let partition_mode = if hash_join.null_aware {
            PartitionMode::CollectLeft
        } else {
            PartitionMode::Partitioned
        };

        Ok(Arc::new(
            hash_join
                .builder()
                .with_partition_mode(partition_mode)
                .build()?,
        ))
    }
}

/// This subrule tries to modify a given plan so that it can
/// optimize hash and cross joins in the plan according to available statistical
/// information.
///
/// Used configurations inside arg `config`
/// - `config.optimizer.hash_join_single_partition_threshold`: byte threshold for `CollectLeft`
/// - `config.optimizer.hash_join_single_partition_threshold_rows`: row threshold for `CollectLeft`
/// - `config.optimizer.join_reordering`: allows or forbids input swapping
fn statistical_join_selection_subrule(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    registry: Option<&StatisticsRegistry>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed = if let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() {
        match hash_join.partition_mode() {
            PartitionMode::Auto => try_collect_left(hash_join, false, config, registry)?
                .map_or_else(
                    || partitioned_hash_join(hash_join, config, registry).map(Some),
                    |v| Ok(Some(v)),
                )?,
            PartitionMode::CollectLeft => {
                try_collect_left(hash_join, true, config, registry)?.map_or_else(
                    || partitioned_hash_join(hash_join, config, registry).map(Some),
                    |v| Ok(Some(v)),
                )?
            }
            PartitionMode::Partitioned => {
                let left = hash_join.left();
                let right = hash_join.right();
                // Don't swap null-aware anti joins as they have specific side requirements
                if hash_join.join_type().supports_swap()
                    && !hash_join.null_aware
                    && should_swap_join_order(&**left, &**right, config, registry)?
                {
                    hash_join
                        .swap_inputs(PartitionMode::Partitioned)
                        .map(Some)?
                } else {
                    None
                }
            }
        }
    } else if let Some(cross_join) = plan.downcast_ref::<CrossJoinExec>() {
        let left = cross_join.left();
        let right = cross_join.right();
        if should_swap_join_order(&**left, &**right, config, registry)? {
            cross_join.swap_inputs().map(Some)?
        } else {
            None
        }
    } else if let Some(nl_join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        let left = nl_join.left();
        let right = nl_join.right();
        if nl_join.join_type().supports_swap()
            && should_swap_join_order(&**left, &**right, config, registry)?
        {
            nl_join.swap_inputs().map(Some)?
        } else {
            None
        }
    } else {
        None
    };

    Ok(if let Some(transformed) = transformed {
        Transformed::yes(transformed)
    } else {
        Transformed::no(plan)
    })
}

/// Pipeline-fixing join selection subrule.
pub type PipelineFixerSubrule =
    dyn Fn(Arc<dyn ExecutionPlan>, &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>>;

/// Converts a hash join to a symmetric hash join if both its inputs are
/// unbounded and incremental.
///
/// This subrule checks if a hash join can be replaced with a symmetric hash join when dealing
/// with unbounded (infinite) inputs on both sides. This replacement avoids pipeline breaking and
/// preserves query runnability. If the replacement is applicable, this subrule makes this change;
/// otherwise, it leaves the input unchanged.
///
/// # Arguments
/// * `input` - The current state of the pipeline, including the execution plan.
/// * `config_options` - Configuration options that might affect the transformation logic.
///
/// # Returns
/// An `Option` that contains the `Result` of the transformation. If the transformation is not applicable,
/// it returns `None`. If applicable, it returns `Some(Ok(...))` with the modified pipeline state,
/// or `Some(Err(...))` if an error occurs during the transformation.
fn hash_join_convert_symmetric_subrule(
    input: Arc<dyn ExecutionPlan>,
    config_options: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Check if the current plan node is a HashJoinExec.
    if let Some(hash_join) = input.downcast_ref::<HashJoinExec>() {
        let left_unbounded = hash_join.left.boundedness().is_unbounded();
        let left_incremental = matches!(
            hash_join.left.pipeline_behavior(),
            EmissionType::Incremental | EmissionType::Both
        );
        let right_unbounded = hash_join.right.boundedness().is_unbounded();
        let right_incremental = matches!(
            hash_join.right.pipeline_behavior(),
            EmissionType::Incremental | EmissionType::Both
        );
        // Process only if both left and right sides are unbounded and incrementally emit.
        if left_unbounded && right_unbounded & left_incremental & right_incremental {
            // Determine the partition mode based on configuration.
            let mode = if config_options.optimizer.repartition_joins {
                StreamJoinPartitionMode::Partitioned
            } else {
                StreamJoinPartitionMode::SinglePartition
            };
            // A closure to determine the required sort order for each side of the join in the SymmetricHashJoinExec.
            // This function checks if the columns involved in the filter have any specific ordering requirements.
            // If the child nodes (left or right side of the join) already have a defined order and the columns used in the
            // filter predicate are ordered, this function captures that ordering requirement. The identified order is then
            // used in the SymmetricHashJoinExec to maintain bounded memory during join operations.
            // However, if the child nodes do not have an inherent order, or if the filter columns are unordered,
            // the function concludes that no specific order is required for the SymmetricHashJoinExec. This approach
            // ensures that the symmetric hash join operation only imposes ordering constraints when necessary,
            // based on the properties of the child nodes and the filter condition.
            let determine_order = |side: JoinSide| -> Option<LexOrdering> {
                hash_join
                    .filter()
                    .map(|filter| {
                        filter.column_indices().iter().any(
                            |ColumnIndex {
                                 index,
                                 side: column_side,
                             }| {
                                // Skip if column side does not match the join side.
                                if *column_side != side {
                                    return false;
                                }
                                // Retrieve equivalence properties and schema based on the side.
                                let (equivalence, schema) = match side {
                                    JoinSide::Left => (
                                        hash_join.left().equivalence_properties(),
                                        hash_join.left().schema(),
                                    ),
                                    JoinSide::Right => (
                                        hash_join.right().equivalence_properties(),
                                        hash_join.right().schema(),
                                    ),
                                    JoinSide::None => return false,
                                };

                                let name = schema.field(*index).name();
                                let col = Arc::new(Column::new(name, *index)) as _;
                                // Check if the column is ordered.
                                equivalence.get_expr_properties(col).sort_properties
                                    != SortProperties::Unordered
                            },
                        )
                    })
                    .unwrap_or(false)
                    .then(|| {
                        match side {
                            JoinSide::Left => hash_join.left().output_ordering(),
                            JoinSide::Right => hash_join.right().output_ordering(),
                            JoinSide::None => unreachable!(),
                        }
                        .cloned()
                    })
                    .flatten()
            };

            // Determine the sort order for both left and right sides.
            let left_order = determine_order(JoinSide::Left);
            let right_order = determine_order(JoinSide::Right);

            return SymmetricHashJoinExec::try_new(
                Arc::clone(hash_join.left()),
                Arc::clone(hash_join.right()),
                hash_join.on().to_vec(),
                hash_join.filter().cloned(),
                hash_join.join_type(),
                hash_join.null_equality(),
                left_order,
                right_order,
                mode,
            )
            .map(|exec| Arc::new(exec) as _);
        }
    }
    Ok(input)
}

/// This subrule will swap build/probe sides of a hash join depending on whether
/// one of its inputs may produce an infinite stream of records. The rule ensures
/// that the left (build) side of the hash join always operates on an input stream
/// that will produce a finite set of records. If the left side can not be chosen
/// to be "finite", the join sides stay the same as the original query.
/// ```text
/// For example, this rule makes the following transformation:
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Infinite     |    true      | Hash         |\true
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              |       |              |
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Finite       |    false     | Hash         |/false
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Finite       |    false     | Hash         |\false
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              | true  |              | true
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |-----
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Infinite     |    true      | Hash         |/true
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
/// ```
pub fn hash_join_swap_subrule(
    mut input: Arc<dyn ExecutionPlan>,
    _config_options: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(hash_join) = input.downcast_ref::<HashJoinExec>()
        && hash_join.left.boundedness().is_unbounded()
        && !hash_join.right.boundedness().is_unbounded()
        && !hash_join.null_aware // Don't swap null-aware anti joins
        && matches!(
            *hash_join.join_type(),
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti
        )
    {
        input = swap_join_according_to_unboundedness(hash_join)?;
    }
    Ok(input)
}

/// This function swaps sides of a hash join to make it runnable even if one of
/// its inputs are infinite. Note that this is not always possible; i.e.
/// [`JoinType::Full`], [`JoinType::Right`], [`JoinType::RightAnti`] and
/// [`JoinType::RightSemi`] can not run with an unbounded left side, even if
/// we swap join sides. Therefore, we do not consider them here.
/// This function is crate public as it is useful for downstream projects
/// to implement, or experiment with, their own join selection rules.
pub(crate) fn swap_join_according_to_unboundedness(
    hash_join: &HashJoinExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (
            _,
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
            | JoinType::Full,
        ) => internal_err!("{join_type} join cannot be swapped for unbounded input."),
        (PartitionMode::Partitioned, _) => {
            hash_join.swap_inputs(PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, _) => {
            hash_join.swap_inputs(PartitionMode::CollectLeft)
        }
        (PartitionMode::Auto, _) => {
            // Use `PartitionMode::Partitioned` as default if `Auto` is selected.
            hash_join.swap_inputs(PartitionMode::Partitioned)
        }
    }
}

/// Apply given `PipelineFixerSubrule`s to a given plan. This plan, along with
/// auxiliary boundedness information, is in the `PipelineStatePropagator` object.
fn apply_subrules(
    mut input: Arc<dyn ExecutionPlan>,
    subrules: &Vec<Box<PipelineFixerSubrule>>,
    config_options: &ConfigOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let original = Arc::clone(&input);
    for subrule in subrules {
        input = subrule(input, config_options)?;
    }

    let transformed = !Arc::ptr_eq(&original, &input);

    Ok(Transformed::new_transformed(input, transformed))
}

// See tests in datafusion/core/tests/physical_optimizer
