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

use crate::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_err, JoinSide, JoinType};
use datafusion_expr_common::sort_properties::SortProperties;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::joins::utils::ColumnIndex;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
    StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// The [`JoinSelection`] rule tries to modify a given plan so that it can
/// accommodate infinite sources and optimize joins in the plan according to
/// available statistical information, if there is any.
#[derive(Default, Debug)]
pub struct JoinSelection {}

impl JoinSelection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

// TODO: We need some performance test for Right Semi/Right Join swap to Left Semi/Left Join in case that the right side is smaller but not much smaller.
// TODO: In PrestoSQL, the optimizer flips join sides only if one side is much smaller than the other by more than SIZE_DIFFERENCE_THRESHOLD times, by default is 8 times.
/// Checks statistics for join swap.
pub(crate) fn should_swap_join_order(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
) -> Result<bool> {
    // Get the left and right table's total bytes
    // If both the left and right tables contain total_byte_size statistics,
    // use `total_byte_size` to determine `should_swap_join_order`, else use `num_rows`
    let left_stats = left.partition_statistics(None)?;
    let right_stats = right.partition_statistics(None)?;
    // First compare `total_byte_size` of left and right side,
    // if information in this field is insufficient fallback to the `num_rows`
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
) -> bool {
    // Currently we do not trust the 0 value from stats, due to stats collection might have bug
    // TODO check the logic in datasource::get_statistics_with_limit()
    let Ok(stats) = plan.partition_statistics(None) else {
        return false;
    };

    if let Some(byte_size) = stats.total_byte_size.get_value() {
        *byte_size != 0 && *byte_size < threshold_byte_size
    } else if let Some(num_rows) = stats.num_rows.get_value() {
        *num_rows != 0 && *num_rows < threshold_num_rows
    } else {
        false
    }
}

impl PhysicalOptimizerRule for JoinSelection {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        // First, we make pipeline-fixing modifications to joins so as to accommodate
        // unbounded inputs. Each pipeline-fixing subrule, which is a function
        // of type `PipelineFixerSubrule`, takes a single [`PipelineStatePropagator`]
        // argument storing state variables that indicate the unboundedness status
        // of the current [`ExecutionPlan`] as we traverse the plan tree.
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let new_plan = plan
            .transform_up(|p| apply_subrules(p, &subrules, config))
            .data()?;
        // Next, we apply another subrule that tries to optimize joins using any
        // statistics their inputs might have.
        // - For a hash join with partition mode [`PartitionMode::Auto`], we will
        //   make a cost-based decision to select which `PartitionMode` mode
        //   (`Partitioned`/`CollectLeft`) is optimal. If the statistics information
        //   is not available, we will fall back to [`PartitionMode::Partitioned`].
        // - We optimize/swap join sides so that the left (build) side of the join
        //   is the small side. If the statistics information is not available, we
        //   do not modify join sides.
        // - We will also swap left and right sides for cross joins so that the left
        //   side is the small side.
        let config = &config.optimizer;
        let collect_threshold_byte_size = config.hash_join_single_partition_threshold;
        let collect_threshold_num_rows = config.hash_join_single_partition_threshold_rows;
        new_plan
            .transform_up(|plan| {
                statistical_join_selection_subrule(
                    plan,
                    collect_threshold_byte_size,
                    collect_threshold_num_rows,
                )
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
pub(crate) fn try_collect_left(
    hash_join: &HashJoinExec,
    ignore_threshold: bool,
    threshold_byte_size: usize,
    threshold_num_rows: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left = hash_join.left();
    let right = hash_join.right();

    let left_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**left,
            threshold_byte_size,
            threshold_num_rows,
        );
    let right_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**right,
            threshold_byte_size,
            threshold_num_rows,
        );

    match (left_can_collect, right_can_collect) {
        (true, true) => {
            if hash_join.join_type().supports_swap()
                && should_swap_join_order(&**left, &**right)?
            {
                Ok(Some(hash_join.swap_inputs(PartitionMode::CollectLeft)?))
            } else {
                Ok(Some(Arc::new(HashJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    hash_join.on().to_vec(),
                    hash_join.filter().cloned(),
                    hash_join.join_type(),
                    hash_join.projection.clone(),
                    PartitionMode::CollectLeft,
                    hash_join.null_equality(),
                )?)))
            }
        }
        (true, false) => Ok(Some(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            hash_join.projection.clone(),
            PartitionMode::CollectLeft,
            hash_join.null_equality(),
        )?))),
        (false, true) => {
            if hash_join.join_type().supports_swap() {
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
pub(crate) fn partitioned_hash_join(
    hash_join: &HashJoinExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    if hash_join.join_type().supports_swap() && should_swap_join_order(&**left, &**right)?
    {
        hash_join.swap_inputs(PartitionMode::Partitioned)
    } else {
        Ok(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            hash_join.projection.clone(),
            PartitionMode::Partitioned,
            hash_join.null_equality(),
        )?))
    }
}

/// This subrule tries to modify a given plan so that it can
/// optimize hash and cross joins in the plan according to available statistical information.
fn statistical_join_selection_subrule(
    plan: Arc<dyn ExecutionPlan>,
    collect_threshold_byte_size: usize,
    collect_threshold_num_rows: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed =
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            match hash_join.partition_mode() {
                PartitionMode::Auto => try_collect_left(
                    hash_join,
                    false,
                    collect_threshold_byte_size,
                    collect_threshold_num_rows,
                )?
                .map_or_else(
                    || partitioned_hash_join(hash_join).map(Some),
                    |v| Ok(Some(v)),
                )?,
                PartitionMode::CollectLeft => try_collect_left(hash_join, true, 0, 0)?
                    .map_or_else(
                        || partitioned_hash_join(hash_join).map(Some),
                        |v| Ok(Some(v)),
                    )?,
                PartitionMode::Partitioned => {
                    let left = hash_join.left();
                    let right = hash_join.right();
                    if hash_join.join_type().supports_swap()
                        && should_swap_join_order(&**left, &**right)?
                    {
                        hash_join
                            .swap_inputs(PartitionMode::Partitioned)
                            .map(Some)?
                    } else {
                        None
                    }
                }
            }
        } else if let Some(cross_join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left = cross_join.left();
            let right = cross_join.right();
            if should_swap_join_order(&**left, &**right)? {
                cross_join.swap_inputs().map(Some)?
            } else {
                None
            }
        } else if let Some(nl_join) = plan.as_any().downcast_ref::<NestedLoopJoinExec>() {
            let left = nl_join.left();
            let right = nl_join.right();
            if nl_join.join_type().supports_swap()
                && should_swap_join_order(&**left, &**right)?
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
    if let Some(hash_join) = input.as_any().downcast_ref::<HashJoinExec>() {
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
    if let Some(hash_join) = input.as_any().downcast_ref::<HashJoinExec>() {
        if hash_join.left.boundedness().is_unbounded()
            && !hash_join.right.boundedness().is_unbounded()
            && matches!(
                *hash_join.join_type(),
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
            )
        {
            input = swap_join_according_to_unboundedness(hash_join)?;
        }
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
    for subrule in subrules {
        input = subrule(input, config_options)?;
    }
    Ok(Transformed::yes(input))
}

// See tests in datafusion/core/tests/physical_optimizer
