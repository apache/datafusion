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
//! `PartitionMode` and the build side using the available statistics for hash
//! joins. The shape of the join tree is chosen before this, by
//! [`JoinEnumeration`](crate::join_enumeration::JoinEnumeration).

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};
use datafusion_common::Statistics;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, internal_err};
use datafusion_expr_common::sort_properties::SortProperties;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::Distribution;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::execution_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion_physical_plan::joins::utils::ColumnIndex;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
    StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

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
        StatisticsContext::new().compute(plan, &StatisticsArgs::new())
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
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let new_plan = plan
            .transform_up(|p| apply_subrules(p, &subrules, config))
            .data()?;
        let new_plan = new_plan
            .transform_up(|plan| {
                statistical_join_selection_subrule(plan, config, registry)
            })
            .data()?;
        new_plan
            .transform_down(|plan| keep_partitioning_needed_above(plan, registry))
            .data()
    }

    fn name(&self) -> &str {
        "join_selection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// How far above a join a hash requirement still counts as that join's, enough to reach
/// through the partial aggregate a group-by is planned as.
const PARTITIONING_LOOKAHEAD: usize = 3;

/// Restores a partitioned join whose partitioning an operator above needs. Collecting the
/// build side saves its exchanges but discards the partitioning, which is then rebuilt
/// above, at more than collecting saved.
fn keep_partitioning_needed_above(
    plan: Arc<dyn ExecutionPlan>,
    registry: Option<&StatisticsRegistry>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let mut children: Vec<_> = plan.children().into_iter().map(Arc::clone).collect();
    let mut changed = false;
    let requirements = plan.input_distribution_requirements();
    for (idx, required) in requirements.per_child_distributions().enumerate() {
        #[expect(
            deprecated,
            reason = "HashPartitioned is still planned during the KeyPartitioned migration"
        )]
        let required_exprs = match required {
            Distribution::KeyPartitioned(exprs)
            | Distribution::HashPartitioned(exprs) => exprs,
            _ => continue,
        };
        if let Some(rewritten) =
            repartition_collected_join(&children[idx], required_exprs, registry)?
        {
            children[idx] = rewritten;
            changed = true;
        }
    }
    if changed {
        Ok(Transformed::yes(plan.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?))
    } else {
        Ok(Transformed::no(plan))
    }
}

/// Rebuilds the collected join under `plan` as partitioned when its keys are what
/// `required_exprs` asks for, looking through the operators that pass a partitioning up.
fn repartition_collected_join(
    plan: &Arc<dyn ExecutionPlan>,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    registry: Option<&StatisticsRegistry>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(required) = column_names(required_exprs) else {
        return Ok(None);
    };
    let mut node = Arc::clone(plan);
    for depth in 0..PARTITIONING_LOOKAHEAD {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            if *join.partition_mode() != PartitionMode::CollectLeft
                || join.on().is_empty()
            {
                return Ok(None);
            }
            // The join is not hash partitioned yet, its inputs still being single
            // partitions, so compare against the keys it would be partitioned on.
            let keys: Vec<_> =
                join.on().iter().map(|(left, _)| Arc::clone(left)).collect();
            if column_names(&keys).is_none_or(|keys| keys != required) {
                return Ok(None);
            }
            if !worth_partitioning(join, registry)? {
                return Ok(None);
            }
            let partitioned: Arc<dyn ExecutionPlan> = Arc::new(
                join.builder()
                    .with_partition_mode(PartitionMode::Partitioned)
                    .build()?,
            );
            return Ok(Some(rebuild_above(plan, &node, partitioned, depth)?));
        }
        // Only a single-input operator with no requirement of its own passes one up.
        let children = node.children();
        let [child] = children.as_slice() else {
            return Ok(None);
        };
        if !matches!(
            node.input_distribution_requirements().child_distribution(0),
            Some(Distribution::UnspecifiedDistribution)
        ) {
            return Ok(None);
        }
        let child = Arc::clone(child);
        drop(children);
        node = child;
    }
    Ok(None)
}

/// Whether partitioning the join moves no more rows than collecting it does. Both move
/// the build side; partitioning then moves the probe side, collecting the output above.
fn worth_partitioning(
    join: &HashJoinExec,
    registry: Option<&StatisticsRegistry>,
) -> Result<bool> {
    let rows = |plan: &dyn ExecutionPlan| -> Result<Option<usize>> {
        Ok(get_stats(plan, registry)?.num_rows.get_value().copied())
    };
    let (Some(probe), Some(output)) = (rows(join.right().as_ref())?, rows(join)?) else {
        return Ok(false);
    };
    Ok(output >= probe)
}

/// The names the expressions partition on, or `None` if any is not a column.
fn column_names(exprs: &[Arc<dyn PhysicalExpr>]) -> Option<Vec<&str>> {
    exprs
        .iter()
        .map(|expr| expr.downcast_ref::<Column>().map(Column::name))
        .collect()
}

/// Puts `replacement` back under the `depth` single-input operators above it.
fn rebuild_above(
    top: &Arc<dyn ExecutionPlan>,
    old: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
    depth: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if depth == 0 {
        return Ok(replacement);
    }
    let mut rebuilt = replacement;
    let mut chain = vec![];
    let mut node = Arc::clone(top);
    while !Arc::ptr_eq(&node, old) {
        chain.push(Arc::clone(&node));
        let child = Arc::clone(node.children()[0]);
        node = child;
    }
    for parent in chain.into_iter().rev() {
        rebuilt = parent.replace_children(
            vec![rebuilt],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
    }
    Ok(rebuilt)
}

/// Determines whether it is possible to swap inputs of a hash join - for null-aware joins, we can only swap `LeftAnti` with no filters
fn can_swap_hash_join(hash_join: &HashJoinExec) -> bool {
    hash_join.join_type().supports_swap()
        && (!hash_join.null_aware
            || (*hash_join.join_type() == JoinType::LeftAnti
                && hash_join.filter().is_none()))
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
            // For null-aware joins, we only swap `LeftAnti` joins where the left side is > right side
            if can_swap_hash_join(hash_join)
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
            if optimizer_config.join_reordering && can_swap_hash_join(hash_join) {
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
    let partition_mode = if hash_join.null_aware {
        PartitionMode::CollectLeft
    } else {
        PartitionMode::Partitioned
    };
    if can_swap_hash_join(hash_join)
        && should_swap_join_order(&**left, &**right, config, registry)?
    {
        hash_join.swap_inputs(partition_mode)
    } else {
        // Null-aware anti joins must use CollectLeft mode because they track probe-side state
        // (probe_side_non_empty, probe_side_has_null) per-partition, but need global knowledge
        // for correct null handling. With partitioning, a partition might not see probe rows
        // even if the probe side is globally non-empty, leading to incorrect NULL row handling.

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
                if can_swap_hash_join(hash_join)
                    && should_swap_join_order(&**left, &**right, config, registry)?
                {
                    // Null-aware RightAnti only supports CollectLeft
                    let partition_mode = if hash_join.null_aware {
                        PartitionMode::CollectLeft
                    } else {
                        PartitionMode::Partitioned
                    };
                    hash_join.swap_inputs(partition_mode).map(Some)?
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
