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

//! EnforceDistribution optimizer rule inspects the physical plan with respect
//! to distribution requirements and adds [`RepartitionExec`]s to satisfy them
//! when necessary. If increasing parallelism is beneficial (and also desirable
//! according to the configuration), this rule increases partition counts in
//! the physical plan.

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::datasource::physical_plan::{CsvExec, ParquetExec};
use crate::error::{DataFusionError, Result};
use crate::physical_optimizer::sort_enforcement::ExecTree;
use crate::physical_optimizer::utils::get_plan_string;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortOptions;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::{can_interleave, InterleaveExec, UnionExec};
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::Partitioning;
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};

use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_expr::logical_plan::JoinType;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_expr::expressions::{Column, NoOp};
use datafusion_physical_expr::utils::{
    map_columns_before_projection, ordering_satisfy, ordering_satisfy_requirement,
    ordering_satisfy_requirement_concrete,
};
use datafusion_physical_expr::{
    expr_list_eq_strict_order, normalize_expr_with_equivalence_properties, PhysicalExpr,
    PhysicalSortRequirement,
};

use itertools::izip;

/// The `EnforceDistribution` rule ensures that distribution requirements are
/// met. In doing so, this rule will increase the parallelism in the plan by
/// introducing repartitioning operators to the physical plan.
///
/// For example, given an input such as:
///
///
/// ```text
/// ┌─────────────────────────────────┐
/// │                                 │
/// │          ExecutionPlan          │
/// │                                 │
/// └─────────────────────────────────┘
///             ▲         ▲
///             │         │
///       ┌─────┘         └─────┐
///       │                     │
///       │                     │
///       │                     │
/// ┌───────────┐         ┌───────────┐
/// │           │         │           │
/// │ batch A1  │         │ batch B1  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A2  │         │ batch B2  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A3  │         │ batch B3  │
/// │           │         │           │
/// └───────────┘         └───────────┘
///
///      Input                 Input
///        A                     B
/// ```
///
/// This rule will attempt to add a `RepartitionExec` to increase parallelism
/// (to 3, in this case) and create the following arrangement:
///
/// ```text
///     ┌─────────────────────────────────┐
///     │                                 │
///     │          ExecutionPlan          │
///     │                                 │
///     └─────────────────────────────────┘
///               ▲      ▲       ▲            Input now has 3
///               │      │       │             partitions
///       ┌───────┘      │       └───────┐
///       │              │               │
///       │              │               │
/// ┌───────────┐  ┌───────────┐   ┌───────────┐
/// │           │  │           │   │           │
/// │ batch A1  │  │ batch A3  │   │ batch B3  │
/// │           │  │           │   │           │
/// ├───────────┤  ├───────────┤   ├───────────┤
/// │           │  │           │   │           │
/// │ batch B2  │  │ batch B1  │   │ batch A2  │
/// │           │  │           │   │           │
/// └───────────┘  └───────────┘   └───────────┘
///       ▲              ▲               ▲
///       │              │               │
///       └─────────┐    │    ┌──────────┘
///                 │    │    │
///                 │    │    │
///     ┌─────────────────────────────────┐   batches are
///     │       RepartitionExec(3)        │   repartitioned
///     │           RoundRobin            │
///     │                                 │
///     └─────────────────────────────────┘
///                 ▲         ▲
///                 │         │
///           ┌─────┘         └─────┐
///           │                     │
///           │                     │
///           │                     │
///     ┌───────────┐         ┌───────────┐
///     │           │         │           │
///     │ batch A1  │         │ batch B1  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A2  │         │ batch B2  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A3  │         │ batch B3  │
///     │           │         │           │
///     └───────────┘         └───────────┘
///
///
///      Input                 Input
///        A                     B
/// ```
///
/// The `EnforceDistribution` rule
/// - is idempotent; i.e. it can be applied multiple times, each time producing
///   the same result.
/// - always produces a valid plan in terms of distribution requirements. Its
///   input plan can be valid or invalid with respect to distribution requirements,
///   but the output plan will always be valid.
/// - produces a valid plan in terms of ordering requirements, *if* its input is
///   a valid plan in terms of ordering requirements. If the input plan is invalid,
///   this rule does not attempt to fix it as doing so is the responsibility of the
///   `EnforceSorting` rule.
///
/// Note that distribution requirements are met in the strictest way. This may
/// result in more than strictly necessary [`RepartitionExec`]s in the plan, but
/// meeting the requirements in the strictest way may help avoid possible data
/// skew in joins.
///
/// For example for a hash join with keys (a, b, c), the required Distribution(a, b, c)
/// can be satisfied by several alternative partitioning ways: (a, b, c), (a, b),
/// (a, c), (b, c), (a), (b), (c) and ( ).
///
/// This rule only chooses the exact match and satisfies the Distribution(a, b, c)
/// by a HashPartition(a, b, c).
#[derive(Default)]
pub struct EnforceDistribution {}

impl EnforceDistribution {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnforceDistribution {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.execution.target_partitions;
        // When `false`, round robin repartition will not be added to increase parallelism
        let enable_roundrobin = config.optimizer.enable_round_robin_repartition;
        let repartition_file_scans = config.optimizer.repartition_file_scans;
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let top_down_join_key_reordering = config.optimizer.top_down_join_key_reordering;

        let adjusted = if top_down_join_key_reordering {
            // Run a top-down process to adjust input key ordering recursively
            let plan_requirements = PlanWithKeyRequirements::new(plan);
            let adjusted =
                plan_requirements.transform_down(&adjust_input_keys_ordering)?;
            adjusted.plan
        } else {
            // Run a bottom-up process
            plan.transform_up(&|plan| {
                Ok(Transformed::Yes(reorder_join_keys_to_inputs(plan)?))
            })?
        };

        let repartition_context = RepartitionContext::new(adjusted);
        // Distribution enforcement needs to be applied bottom-up.
        let updated_plan = repartition_context.transform_up(&|repartition_context| {
            ensure_distribution(
                repartition_context,
                target_partitions,
                enable_roundrobin,
                repartition_file_scans,
                repartition_file_min_size,
            )
        })?;

        Ok(updated_plan.plan)
    }

    fn name(&self) -> &str {
        "EnforceDistribution"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// A Top-Down process will use this method to adjust children's output partitioning based on the parent key reordering requirements:
///
/// Example:
///     TopJoin on (a, b, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
///  Will be adjusted to:
///     TopJoin on (a, b, c)
///         bottom left join on(a, b, c)
///         bottom right join on(a, b, c)
///
/// Example:
///     TopJoin on (a, b, c)
///         Agg1 group by (b, a, c)
///         Agg2 group by (c, b, a)
///
/// Will be adjusted to:
///     TopJoin on (a, b, c)
///          Projection(b, a, c)
///             Agg1 group by (a, b, c)
///          Projection(c, b, a)
///             Agg2 group by (a, b, c)
///
/// Following is the explanation of the reordering process:
///
/// 1) If the current plan is Partitioned HashJoin, SortMergeJoin, check whether the requirements can be satisfied by adjusting join keys ordering:
///    Requirements can not be satisfied, clear the current requirements, generate new requirements(to pushdown) based on the current join keys, return the unchanged plan.
///    Requirements is already satisfied, clear the current requirements, generate new requirements(to pushdown) based on the current join keys, return the unchanged plan.
///    Requirements can be satisfied by adjusting keys ordering, clear the current requiements, generate new requirements(to pushdown) based on the adjusted join keys, return the changed plan.
///
/// 2) If the current plan is Aggregation, check whether the requirements can be satisfied by adjusting group by keys ordering:
///    Requirements can not be satisfied, clear all the requirements, return the unchanged plan.
///    Requirements is already satisfied, clear all the requirements, return the unchanged plan.
///    Requirements can be satisfied by adjusting keys ordering, clear all the requirements, return the changed plan.
///
/// 3) If the current plan is RepartitionExec, CoalescePartitionsExec or WindowAggExec, clear all the requirements, return the unchanged plan
/// 4) If the current plan is Projection, transform the requirements to the columns before the Projection and push down requirements
/// 5) For other types of operators, by default, pushdown the parent requirements to children.
///
fn adjust_input_keys_ordering(
    requirements: PlanWithKeyRequirements,
) -> Result<Transformed<PlanWithKeyRequirements>> {
    let parent_required = requirements.required_key_ordering.clone();
    let plan_any = requirements.plan.as_any();
    let transformed = if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        mode,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        match mode {
            PartitionMode::Partitioned => {
                let join_constructor =
                    |new_conditions: (Vec<(Column, Column)>, Vec<SortOptions>)| {
                        Ok(Arc::new(HashJoinExec::try_new(
                            left.clone(),
                            right.clone(),
                            new_conditions.0,
                            filter.clone(),
                            join_type,
                            PartitionMode::Partitioned,
                            *null_equals_null,
                        )?) as Arc<dyn ExecutionPlan>)
                    };
                Some(reorder_partitioned_join_keys(
                    requirements.plan.clone(),
                    &parent_required,
                    on,
                    vec![],
                    &join_constructor,
                )?)
            }
            PartitionMode::CollectLeft => {
                let new_right_request = match join_type {
                    JoinType::Inner | JoinType::Right => shift_right_required(
                        &parent_required,
                        left.schema().fields().len(),
                    ),
                    JoinType::RightSemi | JoinType::RightAnti => {
                        Some(parent_required.clone())
                    }
                    JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::Full => None,
                };

                // Push down requirements to the right side
                Some(PlanWithKeyRequirements {
                    plan: requirements.plan.clone(),
                    required_key_ordering: vec![],
                    request_key_ordering: vec![None, new_right_request],
                })
            }
            PartitionMode::Auto => {
                // Can not satisfy, clear the current requirements and generate new empty requirements
                Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
            }
        }
    } else if let Some(CrossJoinExec { left, .. }) =
        plan_any.downcast_ref::<CrossJoinExec>()
    {
        let left_columns_len = left.schema().fields().len();
        // Push down requirements to the right side
        Some(PlanWithKeyRequirements {
            plan: requirements.plan.clone(),
            required_key_ordering: vec![],
            request_key_ordering: vec![
                None,
                shift_right_required(&parent_required, left_columns_len),
            ],
        })
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        join_type,
        sort_options,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<SortMergeJoinExec>()
    {
        let join_constructor =
            |new_conditions: (Vec<(Column, Column)>, Vec<SortOptions>)| {
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left.clone(),
                    right.clone(),
                    new_conditions.0,
                    *join_type,
                    new_conditions.1,
                    *null_equals_null,
                )?) as Arc<dyn ExecutionPlan>)
            };
        Some(reorder_partitioned_join_keys(
            requirements.plan.clone(),
            &parent_required,
            on,
            sort_options.clone(),
            &join_constructor,
        )?)
    } else if let Some(aggregate_exec) = plan_any.downcast_ref::<AggregateExec>() {
        if !parent_required.is_empty() {
            match aggregate_exec.mode {
                AggregateMode::FinalPartitioned => Some(reorder_aggregate_keys(
                    requirements.plan.clone(),
                    &parent_required,
                    aggregate_exec,
                )?),
                _ => Some(PlanWithKeyRequirements::new(requirements.plan.clone())),
            }
        } else {
            // Keep everything unchanged
            None
        }
    } else if let Some(ProjectionExec { expr, .. }) =
        plan_any.downcast_ref::<ProjectionExec>()
    {
        // For Projection, we need to transform the requirements to the columns before the Projection
        // And then to push down the requirements
        // Construct a mapping from new name to the the orginal Column
        let new_required = map_columns_before_projection(&parent_required, expr);
        if new_required.len() == parent_required.len() {
            Some(PlanWithKeyRequirements {
                plan: requirements.plan.clone(),
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(new_required.clone())],
            })
        } else {
            // Can not satisfy, clear the current requirements and generate new empty requirements
            Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
        }
    } else if plan_any.downcast_ref::<RepartitionExec>().is_some()
        || plan_any.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan_any.downcast_ref::<WindowAggExec>().is_some()
    {
        Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
    } else {
        // By default, push down the parent requirements to children
        let children_len = requirements.plan.children().len();
        Some(PlanWithKeyRequirements {
            plan: requirements.plan.clone(),
            required_key_ordering: vec![],
            request_key_ordering: vec![Some(parent_required.clone()); children_len],
        })
    };
    Ok(if let Some(transformed) = transformed {
        Transformed::Yes(transformed)
    } else {
        Transformed::No(requirements)
    })
}

fn reorder_partitioned_join_keys<F>(
    join_plan: Arc<dyn ExecutionPlan>,
    parent_required: &[Arc<dyn PhysicalExpr>],
    on: &[(Column, Column)],
    sort_options: Vec<SortOptions>,
    join_constructor: &F,
) -> Result<PlanWithKeyRequirements>
where
    F: Fn((Vec<(Column, Column)>, Vec<SortOptions>)) -> Result<Arc<dyn ExecutionPlan>>,
{
    let join_key_pairs = extract_join_keys(on);
    if let Some((
        JoinKeyPairs {
            left_keys,
            right_keys,
        },
        new_positions,
    )) = try_reorder(
        join_key_pairs.clone(),
        parent_required,
        &join_plan.equivalence_properties(),
    ) {
        if !new_positions.is_empty() {
            let new_join_on = new_join_conditions(&left_keys, &right_keys);
            let mut new_sort_options: Vec<SortOptions> = vec![];
            for idx in 0..sort_options.len() {
                new_sort_options.push(sort_options[new_positions[idx]])
            }

            Ok(PlanWithKeyRequirements {
                plan: join_constructor((new_join_on, new_sort_options))?,
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(left_keys), Some(right_keys)],
            })
        } else {
            Ok(PlanWithKeyRequirements {
                plan: join_plan,
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(left_keys), Some(right_keys)],
            })
        }
    } else {
        Ok(PlanWithKeyRequirements {
            plan: join_plan,
            required_key_ordering: vec![],
            request_key_ordering: vec![
                Some(join_key_pairs.left_keys),
                Some(join_key_pairs.right_keys),
            ],
        })
    }
}

fn reorder_aggregate_keys(
    agg_plan: Arc<dyn ExecutionPlan>,
    parent_required: &[Arc<dyn PhysicalExpr>],
    agg_exec: &AggregateExec,
) -> Result<PlanWithKeyRequirements> {
    let out_put_columns = agg_exec
        .group_by
        .expr()
        .iter()
        .enumerate()
        .map(|(index, (_col, name))| Column::new(name, index))
        .collect::<Vec<_>>();

    let out_put_exprs = out_put_columns
        .iter()
        .map(|c| Arc::new(c.clone()) as Arc<dyn PhysicalExpr>)
        .collect::<Vec<_>>();

    if parent_required.len() != out_put_exprs.len()
        || !agg_exec.group_by.null_expr().is_empty()
        || expr_list_eq_strict_order(&out_put_exprs, parent_required)
    {
        Ok(PlanWithKeyRequirements::new(agg_plan))
    } else {
        let new_positions = expected_expr_positions(&out_put_exprs, parent_required);
        match new_positions {
            None => Ok(PlanWithKeyRequirements::new(agg_plan)),
            Some(positions) => {
                let new_partial_agg = if let Some(AggregateExec {
                    mode,
                    group_by,
                    aggr_expr,
                    filter_expr,
                    order_by_expr,
                    input,
                    input_schema,
                    ..
                }) =
                    agg_exec.input.as_any().downcast_ref::<AggregateExec>()
                {
                    if matches!(mode, AggregateMode::Partial) {
                        let mut new_group_exprs = vec![];
                        for idx in positions.iter() {
                            new_group_exprs.push(group_by.expr()[*idx].clone());
                        }
                        let new_partial_group_by =
                            PhysicalGroupBy::new_single(new_group_exprs);
                        // new Partial AggregateExec
                        Some(Arc::new(AggregateExec::try_new(
                            AggregateMode::Partial,
                            new_partial_group_by,
                            aggr_expr.clone(),
                            filter_expr.clone(),
                            order_by_expr.clone(),
                            input.clone(),
                            input_schema.clone(),
                        )?))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(partial_agg) = new_partial_agg {
                    // Build new group expressions that correspond to the output of partial_agg
                    let new_final_group: Vec<Arc<dyn PhysicalExpr>> =
                        partial_agg.output_group_expr();
                    let new_group_by = PhysicalGroupBy::new_single(
                        new_final_group
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| {
                                (
                                    expr.clone(),
                                    partial_agg.group_expr().expr()[i].1.clone(),
                                )
                            })
                            .collect(),
                    );

                    let new_final_agg = Arc::new(AggregateExec::try_new(
                        AggregateMode::FinalPartitioned,
                        new_group_by,
                        agg_exec.aggr_expr.to_vec(),
                        agg_exec.filter_expr.to_vec(),
                        agg_exec.order_by_expr.to_vec(),
                        partial_agg,
                        agg_exec.input_schema.clone(),
                    )?);

                    // Need to create a new projection to change the expr ordering back
                    let mut proj_exprs = out_put_columns
                        .iter()
                        .map(|col| {
                            (
                                Arc::new(Column::new(
                                    col.name(),
                                    new_final_agg.schema().index_of(col.name()).unwrap(),
                                ))
                                    as Arc<dyn PhysicalExpr>,
                                col.name().to_owned(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let agg_schema = new_final_agg.schema();
                    let agg_fields = agg_schema.fields();
                    for (idx, field) in
                        agg_fields.iter().enumerate().skip(out_put_columns.len())
                    {
                        proj_exprs.push((
                            Arc::new(Column::new(field.name().as_str(), idx))
                                as Arc<dyn PhysicalExpr>,
                            field.name().clone(),
                        ))
                    }
                    // TODO merge adjacent Projections if there are
                    Ok(PlanWithKeyRequirements::new(Arc::new(
                        ProjectionExec::try_new(proj_exprs, new_final_agg)?,
                    )))
                } else {
                    Ok(PlanWithKeyRequirements::new(agg_plan))
                }
            }
        }
    }
}

fn shift_right_required(
    parent_required: &[Arc<dyn PhysicalExpr>],
    left_columns_len: usize,
) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let new_right_required: Vec<Arc<dyn PhysicalExpr>> = parent_required
        .iter()
        .filter_map(|r| {
            if let Some(col) = r.as_any().downcast_ref::<Column>() {
                if col.index() >= left_columns_len {
                    Some(
                        Arc::new(Column::new(col.name(), col.index() - left_columns_len))
                            as Arc<dyn PhysicalExpr>,
                    )
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // if the parent required are all comming from the right side, the requirements can be pushdown
    if new_right_required.len() != parent_required.len() {
        None
    } else {
        Some(new_right_required)
    }
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// This method will try to change the ordering of the join keys to match with the
/// partitioning of the join nodes' children. If it can not match with both sides, it will try to
/// match with one, either the left side or the right side.
///
/// Example:
///     TopJoin on (a, b, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
///  Will be adjusted to:
///     TopJoin on (b, a, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
/// Compared to the Top-Down reordering process, this Bottom-Up approach is much simpler, but might not reach a best result.
/// The Bottom-Up approach will be useful in future if we plan to support storage partition-wised Joins.
/// In that case, the datasources/tables might be pre-partitioned and we can't adjust the key ordering of the datasources
/// and then can't apply the Top-Down reordering process.
pub(crate) fn reorder_join_keys_to_inputs(
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        mode,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        match mode {
            PartitionMode::Partitioned => {
                let join_key_pairs = extract_join_keys(on);
                if let Some((
                    JoinKeyPairs {
                        left_keys,
                        right_keys,
                    },
                    new_positions,
                )) = reorder_current_join_keys(
                    join_key_pairs,
                    Some(left.output_partitioning()),
                    Some(right.output_partitioning()),
                    &left.equivalence_properties(),
                    &right.equivalence_properties(),
                ) {
                    if !new_positions.is_empty() {
                        let new_join_on = new_join_conditions(&left_keys, &right_keys);
                        Ok(Arc::new(HashJoinExec::try_new(
                            left.clone(),
                            right.clone(),
                            new_join_on,
                            filter.clone(),
                            join_type,
                            PartitionMode::Partitioned,
                            *null_equals_null,
                        )?))
                    } else {
                        Ok(plan)
                    }
                } else {
                    Ok(plan)
                }
            }
            _ => Ok(plan),
        }
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        join_type,
        sort_options,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<SortMergeJoinExec>()
    {
        let join_key_pairs = extract_join_keys(on);
        if let Some((
            JoinKeyPairs {
                left_keys,
                right_keys,
            },
            new_positions,
        )) = reorder_current_join_keys(
            join_key_pairs,
            Some(left.output_partitioning()),
            Some(right.output_partitioning()),
            &left.equivalence_properties(),
            &right.equivalence_properties(),
        ) {
            if !new_positions.is_empty() {
                let new_join_on = new_join_conditions(&left_keys, &right_keys);
                let mut new_sort_options = vec![];
                for idx in 0..sort_options.len() {
                    new_sort_options.push(sort_options[new_positions[idx]])
                }
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left.clone(),
                    right.clone(),
                    new_join_on,
                    *join_type,
                    new_sort_options,
                    *null_equals_null,
                )?))
            } else {
                Ok(plan)
            }
        } else {
            Ok(plan)
        }
    } else {
        Ok(plan)
    }
}

/// Reorder the current join keys ordering based on either left partition or right partition
fn reorder_current_join_keys(
    join_keys: JoinKeyPairs,
    left_partition: Option<Partitioning>,
    right_partition: Option<Partitioning>,
    left_equivalence_properties: &EquivalenceProperties,
    right_equivalence_properties: &EquivalenceProperties,
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    match (left_partition, right_partition.clone()) {
        (Some(Partitioning::Hash(left_exprs, _)), _) => {
            try_reorder(join_keys.clone(), &left_exprs, left_equivalence_properties)
                .or_else(|| {
                    reorder_current_join_keys(
                        join_keys,
                        None,
                        right_partition,
                        left_equivalence_properties,
                        right_equivalence_properties,
                    )
                })
        }
        (_, Some(Partitioning::Hash(right_exprs, _))) => {
            try_reorder(join_keys, &right_exprs, right_equivalence_properties)
        }
        _ => None,
    }
}

fn try_reorder(
    join_keys: JoinKeyPairs,
    expected: &[Arc<dyn PhysicalExpr>],
    equivalence_properties: &EquivalenceProperties,
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    let mut normalized_expected = vec![];
    let mut normalized_left_keys = vec![];
    let mut normalized_right_keys = vec![];
    if join_keys.left_keys.len() != expected.len() {
        return None;
    }
    if expr_list_eq_strict_order(expected, &join_keys.left_keys)
        || expr_list_eq_strict_order(expected, &join_keys.right_keys)
    {
        return Some((join_keys, vec![]));
    } else if !equivalence_properties.classes().is_empty() {
        normalized_expected = expected
            .iter()
            .map(|e| {
                normalize_expr_with_equivalence_properties(
                    e.clone(),
                    equivalence_properties.classes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(normalized_expected.len(), expected.len());

        normalized_left_keys = join_keys
            .left_keys
            .iter()
            .map(|e| {
                normalize_expr_with_equivalence_properties(
                    e.clone(),
                    equivalence_properties.classes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(join_keys.left_keys.len(), normalized_left_keys.len());

        normalized_right_keys = join_keys
            .right_keys
            .iter()
            .map(|e| {
                normalize_expr_with_equivalence_properties(
                    e.clone(),
                    equivalence_properties.classes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(join_keys.right_keys.len(), normalized_right_keys.len());

        if expr_list_eq_strict_order(&normalized_expected, &normalized_left_keys)
            || expr_list_eq_strict_order(&normalized_expected, &normalized_right_keys)
        {
            return Some((join_keys, vec![]));
        }
    }

    let new_positions = expected_expr_positions(&join_keys.left_keys, expected)
        .or_else(|| expected_expr_positions(&join_keys.right_keys, expected))
        .or_else(|| expected_expr_positions(&normalized_left_keys, &normalized_expected))
        .or_else(|| {
            expected_expr_positions(&normalized_right_keys, &normalized_expected)
        });

    if let Some(positions) = new_positions {
        let mut new_left_keys = vec![];
        let mut new_right_keys = vec![];
        for pos in positions.iter() {
            new_left_keys.push(join_keys.left_keys[*pos].clone());
            new_right_keys.push(join_keys.right_keys[*pos].clone());
        }
        Some((
            JoinKeyPairs {
                left_keys: new_left_keys,
                right_keys: new_right_keys,
            },
            positions,
        ))
    } else {
        None
    }
}

/// Return the expected expressions positions.
/// For example, the current expressions are ['c', 'a', 'a', b'], the expected expressions are ['b', 'c', 'a', 'a'],
///
/// This method will return a Vec [3, 0, 1, 2]
fn expected_expr_positions(
    current: &[Arc<dyn PhysicalExpr>],
    expected: &[Arc<dyn PhysicalExpr>],
) -> Option<Vec<usize>> {
    if current.is_empty() || expected.is_empty() {
        return None;
    }
    let mut indexes: Vec<usize> = vec![];
    let mut current = current.to_vec();
    for expr in expected.iter() {
        // Find the position of the expected expr in the current expressions
        if let Some(expected_position) = current.iter().position(|e| e.eq(expr)) {
            current[expected_position] = Arc::new(NoOp::new());
            indexes.push(expected_position);
        } else {
            return None;
        }
    }
    Some(indexes)
}

fn extract_join_keys(on: &[(Column, Column)]) -> JoinKeyPairs {
    let (left_keys, right_keys) = on
        .iter()
        .map(|(l, r)| {
            (
                Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
            )
        })
        .unzip();
    JoinKeyPairs {
        left_keys,
        right_keys,
    }
}

fn new_join_conditions(
    new_left_keys: &[Arc<dyn PhysicalExpr>],
    new_right_keys: &[Arc<dyn PhysicalExpr>],
) -> Vec<(Column, Column)> {
    let new_join_on = new_left_keys
        .iter()
        .zip(new_right_keys.iter())
        .map(|(l_key, r_key)| {
            (
                l_key.as_any().downcast_ref::<Column>().unwrap().clone(),
                r_key.as_any().downcast_ref::<Column>().unwrap().clone(),
            )
        })
        .collect::<Vec<_>>();
    new_join_on
}

/// Adds RoundRobin repartition operator to increase parallelism.
fn add_roundrobin_on_top(
    input: Arc<dyn ExecutionPlan>,
    n_target: usize,
    repartition_onward: &mut Option<ExecTree>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let should_preserve_ordering = input.output_ordering().is_some();
    if input.output_partitioning().partition_count() < n_target {
        let new_plan = Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(n_target))?
                .with_preserve_order(should_preserve_ordering),
        ) as Arc<dyn ExecutionPlan>;
        if let Some(exec_tree) = repartition_onward {
            return Err(DataFusionError::Internal(format!(
                "ExecTree should have been empty, but got:{:?} ",
                exec_tree
            )));
        }
        // Initialize the new onward, keep track of the operators starting from
        // the RepartitionExec(RoundRobin).
        *repartition_onward = Some(ExecTree::new(new_plan.clone(), 0, vec![]));
        Ok(new_plan)
    } else {
        // Partition is not helpful, we already have desired number of partitions.
        Ok(input)
    }
}

/// Adds a hash repartition operator:
/// - to increase parallelism, and/or
/// - to satisfy requirements of the subsequent operators.
/// Repartition(Hash) is added on top of operator `input`.
fn add_hash_on_top(
    input: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    // Repartition(Hash) will have `n_target` partitions at the output.
    n_target: usize,
    // Stores executors starting from Repartition(RoundRobin) until
    // current executor. When Repartition(Hash) is added, `repartition_onward`
    // is updated such that it stores connection from Repartition(RoundRobin)
    // until Repartition(Hash).
    repartition_onward: &mut Option<ExecTree>,
    // If `true` SortPreservingRepartitionExec(Hash) is used.
    should_preserve_order: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Since hashing benefits from partitioning, add a round-robin repartition
    // before it:
    let mut new_plan = add_roundrobin_on_top(input, n_target, repartition_onward)?;
    new_plan = Arc::new(
        RepartitionExec::try_new(new_plan, Partitioning::Hash(hash_exprs, n_target))?
            .with_preserve_order(should_preserve_order),
    ) as _;

    // Update the onward tree if there is a branch connected to the round-robin
    // repartition:
    if let Some(exec_tree) = repartition_onward {
        *exec_tree = ExecTree::new(new_plan.clone(), 0, vec![exec_tree.clone()]);
    }
    Ok(new_plan)
}

/// Add SortPreservingMergeExec operator on top of input executor
fn add_spm_on_top(
    input: Arc<dyn ExecutionPlan>,
    repartition_onward: &mut Option<ExecTree>,
    should_preserve_ordering: bool,
) -> Arc<dyn ExecutionPlan> {
    // Add SortPreservingMerge only when partition count is larger than 1.
    if input.output_partitioning().partition_count() > 1 {
        let new_plan: Arc<dyn ExecutionPlan> = if should_preserve_ordering {
            let existing_ordering = input.output_ordering().unwrap_or(&[]);
            Arc::new(SortPreservingMergeExec::new(
                existing_ordering.to_vec(),
                input,
            )) as _
        } else {
            Arc::new(CoalescePartitionsExec::new(input)) as _
        };
        if let Some(repartition_onward_inner) = repartition_onward {
            *repartition_onward = Some(ExecTree {
                plan: new_plan.clone(),
                idx: 0,
                children: vec![repartition_onward_inner.clone()],
            })
        }
        new_plan
    } else {
        input
    }
}

/// Updates the physical plan inside `repartition_context` if having a
/// `RepartitionExec(RoundRobin)` is not helpful.
///
/// Assume that it is given the following plan:
/// ```text
/// "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "    ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
///
/// `RepartitionExec` at the top is unnecessary. Since it doesn't help with increasing parallelism.
/// This function removes top repartition, and return following plan.
///
/// ```text
/// "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "  ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
fn remove_unnecessary_repartition(
    repartition_context: RepartitionContext,
) -> Result<RepartitionContext> {
    let RepartitionContext {
        mut plan,
        mut repartition_onwards,
    } = repartition_context;

    // Remove any redundant RoundRobin at the start:
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        if let Partitioning::RoundRobinBatch(n_out) = repartition.partitioning() {
            // Repartition is useless:
            if *n_out <= repartition.input().output_partitioning().partition_count() {
                let mut new_repartition_onwards =
                    vec![None; repartition.input().children().len()];
                if let Some(exec_tree) = &repartition_onwards[0] {
                    for child in &exec_tree.children {
                        new_repartition_onwards[child.idx] = Some(child.clone());
                    }
                }
                plan = repartition.input().clone();
                repartition_onwards = new_repartition_onwards;
            }
        }
    }

    // Create a plan with the updated children:
    Ok(RepartitionContext {
        plan,
        repartition_onwards,
    })
}

fn print_plan(plan: &Arc<dyn ExecutionPlan>) -> () {
    let formatted = crate::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    println!("{:#?}", actual);
}

fn remove_order_preservation(exec_tree: &ExecTree) -> Result<Arc<dyn ExecutionPlan>> {
    let mut updated_children = exec_tree.plan.children();
    for child in &exec_tree.children {
        updated_children[child.idx] = remove_order_preservation(child)?;
    }
    if let Some(repartition) = exec_tree.plan.as_any().downcast_ref::<RepartitionExec>() {
        if repartition.preserve_order() {
            return Ok(Arc::new(
                RepartitionExec::try_new(
                    repartition.input().clone(),
                    repartition.partitioning().clone(),
                )?
                .with_preserve_order(false),
            ));
        }
    }
    exec_tree.plan.clone().with_new_children(updated_children)
}

/// This function checks whether we need to add additional data exchange
/// operators to satisfy distribution requirements. Since this function
/// takes care of such requirements, we should avoid manually adding data
/// exchange operators in other places.
fn ensure_distribution(
    repartition_context: RepartitionContext,
    target_partitions: usize,
    enable_round_robin: bool,
    repartition_file_scans: bool,
    repartition_file_min_size: usize,
) -> Result<Transformed<RepartitionContext>> {
    if repartition_context.plan.children().is_empty() {
        return Ok(Transformed::No(repartition_context));
    }
    // Don't need to apply when the returned row count is not greater than 1:
    let stats = repartition_context.plan.statistics();
    let mut repartition_beneficial_stat = true;
    if stats.is_exact {
        repartition_beneficial_stat =
            stats.num_rows.map(|num_rows| num_rows > 1).unwrap_or(true);
    }

    // Remove unnecessary repartition from the physical plan if any
    let RepartitionContext {
        plan,
        mut repartition_onwards,
    } = remove_unnecessary_repartition(repartition_context)?;

    // This loop iterates over all the children to:
    // - Increase parallelism for every child if it is beneficial.
    // - Satisfy the distribution requirements of every child, if it is not
    //   already satisfied.
    // We store the updated children in `new_children`.
    let new_children = izip!(
        plan.children().into_iter(),
        plan.required_input_distribution().iter(),
        plan.required_input_ordering().iter(),
        repartition_onwards.iter_mut(),
        plan.benefits_from_input_partitioning(),
        plan.maintains_input_order(),
    )
    .map(
        |(
            mut child,
            requirement,
            required_input_ordering,
            repartition_onward,
            would_benefit,
            maintains,
        )| {
            if enable_round_robin
                // Operator benefits from partitioning (e.g. filter):
                && (would_benefit && repartition_beneficial_stat)
                // Unless partitioning doesn't increase the partition count, it is not beneficial:
                && child.output_partitioning().partition_count() < target_partitions
            {
                let should_preserve_ordering =
                    should_preserve_ordering(&child, required_input_ordering.as_deref())
                        || maintains;

                // println!("should_preserve_ordering:{:?}", should_preserve_ordering);
                // print_plan(&child);

                // When `repartition_file_scans` is set, leverage source operators
                // (`ParquetExec`, `CsvExec` etc.) to increase parallelism at the source.
                if repartition_file_scans {
                    if let Some(parquet_exec) =
                        child.as_any().downcast_ref::<ParquetExec>()
                    {
                        child = Arc::new(parquet_exec.get_repartitioned(
                            target_partitions,
                            repartition_file_min_size,
                        ));
                    } else if let Some(csv_exec) =
                        child.as_any().downcast_ref::<CsvExec>()
                    {
                        if let Some(csv_exec) = csv_exec.get_repartitioned(
                            target_partitions,
                            repartition_file_min_size,
                        ) {
                            child = Arc::new(csv_exec);
                        }
                    }
                }
                // Increase parallelism by adding round-robin repartitioning
                // on top of the operator. Note that we only do this if the
                // partition count is not already equal to the desired partition
                // count.
                child =
                    add_roundrobin_on_top(child, target_partitions, repartition_onward)?;
            }

            if !child
                .output_partitioning()
                .satisfy(requirement.clone(), || child.equivalence_properties())
            {
                // Satisfy the distribution requirement if it is unmet.

                // Stores whether we should preserve ordering while adding
                // repartitioning:
                let should_preserve_ordering =
                    should_preserve_ordering(&child, required_input_ordering.as_deref())
                        || (maintains && child.output_ordering().is_some());

                match requirement {
                    Distribution::SinglePartition => {
                        child = add_spm_on_top(
                            child,
                            repartition_onward,
                            should_preserve_ordering,
                        );
                    }
                    Distribution::HashPartitioned(exprs) => {
                        child = add_hash_on_top(
                            child,
                            exprs.to_vec(),
                            target_partitions,
                            repartition_onward,
                            should_preserve_ordering,
                        )?;
                    }
                    Distribution::UnspecifiedDistribution => {}
                };
            }
            // Preserving ordering through RepartitionExec is meaningless if any one of the conditions is true
            // - Ordering is not maintained in the current operator and operator doesn't require ordering itself such as FilterExec
            // - Ordering requirement of the operator cannot be satisfied with existing ordering

            // println!("child.output_ordering(): {:?}", child.output_ordering());
            // println!("required_input_ordering: {:?}", required_input_ordering);
            // println!("maintains(): {:?}", maintains);
            // print_plan(&child);

            // if !ordering_satisfy_requirement(
            //     child.output_ordering(),
            //     required_input_ordering.as_deref(),
            //     || child.equivalence_properties(),
            //     || child.ordering_equivalence_properties(),
            // ) || (!maintains && required_input_ordering.is_none())
            // {
            //     if let Some(repartition_onward) = repartition_onward {
            //         println!("before");
            //         print_plan(&child);
            //         child = remove_order_preservation(repartition_onward)?;
            //         print_plan(&child);
            //         println!("after");
            //     }
            //     *repartition_onward = None;
            // }
            match requirement {
                Distribution::SinglePartition | Distribution::HashPartitioned(_) => {
                    if !ordering_satisfy_requirement(
                        child.output_ordering(),
                        required_input_ordering.as_deref(),
                        || child.equivalence_properties(),
                        || child.ordering_equivalence_properties(),
                    ) {
                        if let Some(repartition_onward) = repartition_onward {
                            // println!("before");
                            // print_plan(&child);
                            child = remove_order_preservation(repartition_onward)?;
                            // print_plan(&child);
                            // println!("after");
                        }
                    }
                    *repartition_onward = None;
                }
                _ => {
                    if !ordering_satisfy_requirement(
                        child.output_ordering(),
                        required_input_ordering.as_deref(),
                        || child.equivalence_properties(),
                        || child.ordering_equivalence_properties(),
                    ) || !maintains
                    {
                        if let Some(repartition_onward) = repartition_onward {
                            // println!("before");
                            // print_plan(&child);
                            child = remove_order_preservation(repartition_onward)?;
                            // print_plan(&child);
                            // println!("after");
                        }
                        *repartition_onward = None;
                    }
                }
            }
            Ok(child)
        },
    )
    .collect::<Result<Vec<_>>>()?;

    let new_repartition_context = RepartitionContext {
        plan: if plan.as_any().is::<UnionExec>() && can_interleave(&new_children) {
            // Add a special case for [`UnionExec`] since we want to "bubble up"
            // hash-partitioned data. So instead of
            //
            // Agg:
            //   Repartition (hash):
            //     Union:
            //       - Agg:
            //           Repartition (hash):
            //             Data
            //       - Agg:
            //           Repartition (hash):
            //             Data
            //
            // we can use:
            //
            // Agg:
            //   Interleave:
            //     - Agg:
            //         Repartition (hash):
            //           Data
            //     - Agg:
            //         Repartition (hash):
            //           Data
            Arc::new(InterleaveExec::try_new(new_children)?)
        } else {
            plan.clone().with_new_children(new_children)?
        },
        repartition_onwards,
    };
    Ok(Transformed::Yes(new_repartition_context))
}

/// Decide whether we should preserve ordering while adding repartition.
fn should_preserve_ordering(
    input: &Arc<dyn ExecutionPlan>,
    required_input_ordering: Option<&[PhysicalSortRequirement]>,
) -> bool {
    match (input.output_ordering(), required_input_ordering) {
        (Some(existing_ordering), Some(required_ordering)) => {
            // We have to preserve ordering when adding repartition if the
            // existing ordering satisfies the ordering requirement.
            ordering_satisfy_requirement_concrete(
                existing_ordering,
                required_ordering,
                || input.equivalence_properties(),
                || input.ordering_equivalence_properties(),
            )
        }
        (_, _) => false,
    }
}

/// A struct to keep track of repartitions, and their associated parents
/// inside `plan`. Using this information, we can remove repartitions from
/// the plan if/when necessary.
#[derive(Debug, Clone)]
struct RepartitionContext {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Keep track of associations for each child of the plan. If `None`,
    /// there is no `RepartitionExec` in its descendants.
    repartition_onwards: Vec<Option<ExecTree>>,
}

impl RepartitionContext {
    /// Creates an empty context.
    fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        RepartitionContext {
            plan,
            repartition_onwards: vec![None; length],
        }
    }

    /// Constructs a new context from children contexts.
    fn new_from_children_nodes(
        children_nodes: Vec<RepartitionContext>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let repartition_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, context)| {
                let RepartitionContext {
                    plan,
                    // The `repartition_onwards` tree keeps track of operators
                    // that increase repartitioning, or preserves the existing
                    // partitioning (starting from a round-robin repartition).
                    repartition_onwards,
                } = context;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if repartition_onwards[0].is_none() {
                    if let Some(repartition) =
                        plan.as_any().downcast_ref::<RepartitionExec>()
                    {
                        if let Partitioning::RoundRobinBatch(_) =
                            repartition.partitioning()
                        {
                            // Start tracking operators starting from this RR repartition:
                            return Some(ExecTree::new(plan, idx, vec![]));
                        }
                    }
                    None
                } else {
                    // Propagate children repartition tracking to the above
                    let new_repartition_onwards = izip!(
                        plan.required_input_distribution().iter(),
                        repartition_onwards.into_iter()
                    )
                    .flat_map(|(required_dist, repartition_onwards)| {
                        if let Some(repartition_onwards) = repartition_onwards {
                            // Operator can safely propagate the repartition above.
                            // This is similar to maintaining order in the EnforceSorting rule.
                            if let Distribution::UnspecifiedDistribution = required_dist {
                                return Some(repartition_onwards);
                            }
                        }
                        None
                    })
                    .collect::<Vec<_>>();
                    // Either:
                    // - None of the children has a connected repartition, or
                    // - The current operator uses repartitioning at its input,
                    //   so doesn't propagate it above.
                    if new_repartition_onwards.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, new_repartition_onwards))
                    }
                }
            })
            .collect();
        Ok(RepartitionContext {
            plan: with_new_children_if_necessary(parent_plan, children_plans)?.into(),
            repartition_onwards,
        })
    }

    /// Computes repartition tracking contexts for every child of the plan.
    fn children(&self) -> Vec<RepartitionContext> {
        self.plan
            .children()
            .into_iter()
            .map(|child| RepartitionContext::new(child))
            .collect()
    }
}

impl TreeNode for RepartitionContext {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.children() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            RepartitionContext::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}

/// implement Display method for RepartitionContext struct.
impl fmt::Display for RepartitionContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let plan_string = get_plan_string(&self.plan);
        write!(f, "plan: {:?}", plan_string)?;
        for (idx, child) in self.repartition_onwards.iter().enumerate() {
            if let Some(child) = child {
                write!(f, "idx:{:?}, exec_tree:{}", idx, child)?;
            }
        }
        write!(f, "")
    }
}

#[derive(Debug, Clone)]
struct JoinKeyPairs {
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
}

#[derive(Debug, Clone)]
struct PlanWithKeyRequirements {
    pub plan: Arc<dyn ExecutionPlan>,
    /// Parent required key ordering
    required_key_ordering: Vec<Arc<dyn PhysicalExpr>>,
    /// The request key ordering to children
    request_key_ordering: Vec<Option<Vec<Arc<dyn PhysicalExpr>>>>,
}

impl PlanWithKeyRequirements {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let children_len = plan.children().len();
        PlanWithKeyRequirements {
            plan,
            required_key_ordering: vec![],
            request_key_ordering: vec![None; children_len],
        }
    }

    pub fn children(&self) -> Vec<PlanWithKeyRequirements> {
        let plan_children = self.plan.children();
        assert_eq!(plan_children.len(), self.request_key_ordering.len());
        plan_children
            .into_iter()
            .zip(self.request_key_ordering.clone().into_iter())
            .map(|(child, required)| {
                let from_parent = required.unwrap_or_default();
                let length = child.children().len();
                PlanWithKeyRequirements {
                    plan: child,
                    required_key_ordering: from_parent.clone(),
                    request_key_ordering: vec![None; length],
                }
            })
            .collect()
    }
}

impl TreeNode for PlanWithKeyRequirements {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = self.children();
        for child in children {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if !children.is_empty() {
            let new_children: Result<Vec<_>> =
                children.into_iter().map(transform).collect();

            let children_plans = new_children?
                .into_iter()
                .map(|child| child.plan)
                .collect::<Vec<_>>();
            let new_plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithKeyRequirements {
                plan: new_plan.into(),
                required_key_ordering: self.required_key_ordering,
                request_key_ordering: self.request_key_ordering,
            })
        } else {
            Ok(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use crate::physical_optimizer::sort_enforcement::EnforceSorting;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::{
        utils::JoinOn, HashJoinExec, PartitionMode, SortMergeJoinExec,
    };
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::{displayable, Statistics};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::{
        expressions, expressions::binary, expressions::lit, expressions::Column,
        PhysicalExpr, PhysicalSortExpr,
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]))
    }

    fn parquet_exec() -> Arc<ParquetExec> {
        parquet_exec_with_sort(vec![])
    }

    fn parquet_exec_with_sort(
        output_ordering: Vec<Vec<PhysicalSortExpr>>,
    ) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    // Created a sorted parquet exec with multiple files
    fn parquet_exec_multiple_sorted(
        output_ordering: Vec<Vec<PhysicalSortExpr>>,
    ) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![
                    vec![PartitionedFile::new("x".to_string(), 100)],
                    vec![PartitionedFile::new("y".to_string(), 100)],
                ],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn projection_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut exprs = vec![];
        for (column, alias) in alias_pairs.iter() {
            exprs.push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
        for (column, alias) in alias_pairs.iter() {
            group_by_expr
                .push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        let group_by = PhysicalGroupBy::new_single(group_by_expr.clone());

        let final_group_by_expr = group_by_expr
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                (
                    Arc::new(expressions::Column::new(name, index))
                        as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let final_grouping = PhysicalGroupBy::new_single(final_group_by_expr);

        Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                final_grouping,
                vec![],
                vec![],
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        group_by,
                        vec![],
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn hash_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                None,
                join_type,
                PartitionMode::Partitioned,
                false,
            )
            .unwrap(),
        )
    }

    fn sort_merge_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                *join_type,
                vec![SortOptions::default(); join_on.len()],
                false,
            )
            .unwrap(),
        )
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn ensure_distribution_helper(
        plan: Arc<dyn ExecutionPlan>,
        target_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let repartition_context = RepartitionContext::new(plan);
        ensure_distribution(repartition_context, target_partitions, false, false, 1024)
            .map(|item| item.into().plan)
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            let mut config = ConfigOptions::new();
            config.execution.target_partitions = 10;

            // NOTE: These tests verify the joint `EnforceDistribution` + `EnforceSorting` cascade
            //       because they were written prior to the separation of `BasicEnforcement` into
            //       `EnforceSorting` and `EnforceDistribution`.
            // TODO: Orthogonalize the tests here just to verify `EnforceDistribution` and create
            //       new tests for the cascade.
            let optimized = if $FIRST_ENFORCE_DIST {
                // Run enforce distribution rule first:
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize($PLAN.clone(), &config)?;
                // The rule should be idempotent.
                // Re-running this rule shouldn't introduce unnecessary operators.
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // Run the enforce sorting rule:
                let optimizer = EnforceSorting::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                optimized
            } else {
                // Run the enforce sorting rule first:
                let optimizer = EnforceSorting::new();
                let optimized = optimizer.optimize($PLAN.clone(), &config)?;
                // Run enforce distribution rule:
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // The rule should be idempotent.
                // Re-running this rule shouldn't introduce unnecessary operators.
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                optimized
            };

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    macro_rules! assert_plan_txt {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();
            // Now format correctly
            let plan = displayable($PLAN.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn multi_hash_joins() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
            ("d".to_string(), "d1".to_string()),
            ("e".to_string(), "e1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let join_plan = format!(
                "HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, b1@1)]"
            );

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    // Join on (a == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("a", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];
                    let top_join = hash_join_exec(
                        join.clone(),
                        parquet_exec(),
                        &top_join_on,
                        &join_type,
                    );
                    let top_join_plan =
                        format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, c@2)]");

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs
                        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 4 RepartitionExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected, top_join.clone(), true);
                    assert_optimized!(expected, top_join, false);
                }
                JoinType::RightSemi | JoinType::RightAnti => {}
            }

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::RightSemi
                | JoinType::RightAnti => {
                    // This time we use (b1 == c) for top join
                    // Join on (b1 == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("b1", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];

                    let top_join =
                        hash_join_exec(join, parquet_exec(), &top_join_on, &join_type);
                    let top_join_plan = match join_type {
                        JoinType::RightSemi | JoinType::RightAnti =>
                            format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@1, c@2)]"),
                        _ =>
                            format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@6, c@2)]"),
                    };

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs
                        JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti =>
                            vec![
                                top_join_plan.as_str(),
                                join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            ],
                        // Should include 4 RepartitionExecs
                        _ =>
                            vec![
                                top_join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                                join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            ],
                    };
                    assert_optimized!(expected, top_join.clone(), true);
                    assert_optimized!(expected, top_join, false);
                }
                JoinType::LeftSemi | JoinType::LeftAnti => {}
            }
        }

        Ok(())
    }

    #[test]
    fn multi_joins_after_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(a as a1, a as a2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("a".to_string(), "a2".to_string()),
        ];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Join on (a1 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a1", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(
            projection.clone(),
            right.clone(),
            &top_join_on,
            &JoinType::Inner,
        );

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, c@2)]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);

        // Join on (a2 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a2", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a2@1, c@2)]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);
        Ok(())
    }

    #[test]
    fn multi_joins_after_multi_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];

        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(c as c1)
        let alias_pairs: Vec<(String, String)> =
            vec![("c".to_string(), "c1".to_string())];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Projection(c1 as a)
        let alias_pairs: Vec<(String, String)> =
            vec![("c1".to_string(), "a".to_string())];
        let projection2 = projection_exec_with_alias(projection, alias_pairs);

        // Join on (a == c)
        let top_join_on = vec![(
            Column::new_with_schema("a", &projection2.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection2, right, &top_join_on, &JoinType::Inner);

        // The Column 'a' has different meaning now after the two Projections
        // The original Output partition can not satisfy the Join requirements and need to add an additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, c@2)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "ProjectionExec: expr=[c1@0 as a]",
            "ProjectionExec: expr=[c@2 as c1]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);
        Ok(())
    }

    #[test]
    fn join_after_agg_alias() -> Result<()> {
        // group by (a as a1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );
        // group by (a as a2)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a2".to_string())],
        );

        // Join on (a1 == a2)
        let join_on = vec![(
            Column::new_with_schema("a1", &left.schema()).unwrap(),
            Column::new_with_schema("a2", &right.schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, a2@0)]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true);
        assert_optimized!(expected, join, false);
        Ok(())
    }

    #[test]
    fn hash_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        // Join on (b1 == b && a1 == a)
        let join_on = vec![
            (
                Column::new_with_schema("b1", &left.schema()).unwrap(),
                Column::new_with_schema("b", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a1", &left.schema()).unwrap(),
                Column::new_with_schema("a", &right.schema()).unwrap(),
            ),
        ];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b1@1, b@0), (a1@0, a@1)]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true);
        assert_optimized!(expected, join, false);
        Ok(())
    }

    #[test]
    fn multi_hash_join_key_ordering() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1 and c == c1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_left_join =
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner);

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join =
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let top_join = hash_join_exec(
            bottom_left_projection.clone(),
            bottom_right_join,
            &top_join_on,
            &JoinType::Inner,
        );

        let predicate: Arc<dyn PhysicalExpr> = binary(
            col("c", top_join.schema().deref())?,
            Operator::Gt,
            lit(1i64),
            top_join.schema().deref(),
        )?;

        let filter_top_join: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, top_join)?);

        // The bottom joins' join key ordering is adjusted based on the top join. And the top join should not introduce additional RepartitionExec
        let expected = &[
            "FilterExec: c@6 > 1",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(B@2, b1@6), (C@3, c@2), (AA@1, a1@5)]",
            "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
            "RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
            "RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, filter_top_join.clone(), true);
        assert_optimized!(expected, filter_top_join, false);
        Ok(())
    }

    #[test]
    fn reorder_join_keys_to_left_input() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1 and c == c1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
        ];

        let bottom_left_join = ensure_distribution_helper(
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
            10,
        )?;

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join = ensure_distribution_helper(
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
            10,
        )?;

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let top_join = hash_join_exec(
                bottom_left_projection.clone(),
                bottom_right_join.clone(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(AA@1, a1@5), (B@2, b1@6), (C@3, c@2)]", &join_type);

            let reordered = reorder_join_keys_to_inputs(top_join)?;

            // The top joins' join key ordering is adjusted based on the children inputs.
            let expected = &[
                top_join_plan.as_str(),
                "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1), (c@2, c1@2)]",
                "RepartitionExec: partitioning=Hash([a@0, b@1, c@2], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([a1@0, b1@1, c1@2], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
                "RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            ];

            assert_plan_txt!(expected, reordered);
        }

        Ok(())
    }

    #[test]
    fn reorder_join_keys_to_right_input() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_left_join = ensure_distribution_helper(
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
            10,
        )?;

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join = ensure_distribution_helper(
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
            10,
        )?;

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let top_join = hash_join_exec(
                bottom_left_projection.clone(),
                bottom_right_join.clone(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(C@3, c@2), (B@2, b1@6), (AA@1, a1@5)]", &join_type);

            let reordered = reorder_join_keys_to_inputs(top_join)?;

            // The top joins' join key ordering is adjusted based on the children inputs.
            let expected = &[
                top_join_plan.as_str(),
                "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1)]",
                "RepartitionExec: partitioning=Hash([a@0, b@1], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([a1@0, b1@1], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
                "RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            ];

            assert_plan_txt!(expected, reordered);
        }

        Ok(())
    }

    #[test]
    fn multi_smj_joins() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
            ("d".to_string(), "d1".to_string()),
            ("e".to_string(), "e1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // SortMergeJoin does not support RightSemi and RightAnti join now
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(a@0, b1@1)]");

            // Top join on (a == c)
            let top_join_on = vec![(
                Column::new_with_schema("a", &join.schema()).unwrap(),
                Column::new_with_schema("c", &schema()).unwrap(),
            )];
            let top_join = sort_merge_join_exec(
                join.clone(),
                parquet_exec(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(a@0, c@2)]");

            let expected = match join_type {
                // Should include 6 RepartitionExecs 3 SortExecs
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                    vec![
                        top_join_plan.as_str(),
                        join_plan.as_str(),
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[b1@1 ASC]",
                        "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[c@2 ASC]",
                        "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    ],
                // Should include 7 RepartitionExecs
                _ => vec![
                        top_join_plan.as_str(),
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        join_plan.as_str(),
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[b1@1 ASC]",
                        "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[c@2 ASC]",
                        "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join.clone(), true);

            let expected_first_sort_enforcement = match join_type {
                // Should include 3 RepartitionExecs 3 SortExecs
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                    vec![
                        top_join_plan.as_str(),
                        join_plan.as_str(),
                        "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[a@0 ASC]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[b1@1 ASC]",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[c@2 ASC]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    ],
                // Should include 8 RepartitionExecs (4 of them preserves ordering)
                _ => vec![
                    top_join_plan.as_str(),
                    "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[a@0 ASC]",
                    "CoalescePartitionsExec",
                    join_plan.as_str(),
                    "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[a@0 ASC]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[b1@1 ASC]",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[c@2 ASC]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected_first_sort_enforcement, top_join, false);

            match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    // This time we use (b1 == c) for top join
                    // Join on (b1 == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("b1", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];
                    let top_join = sort_merge_join_exec(
                        join,
                        parquet_exec(),
                        &top_join_on,
                        &join_type,
                    );
                    let top_join_plan =
                        format!("SortMergeJoin: join_type={join_type}, on=[(b1@6, c@2)]");

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs and 3 SortExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "SortExec: expr=[a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 4 RepartitionExecs and 4 SortExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "SortExec: expr=[b1@6 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                            join_plan.as_str(),
                            "SortExec: expr=[a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected, top_join.clone(), true);

                    let expected_first_sort_enforcement = match join_type {
                        // Should include 6 RepartitionExecs (3 of them preserves order) and 3 SortExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[a@0 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@1 ASC]",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[c@2 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 8 RepartitionExecs (4 of them preserves order) and 4 SortExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@6 ASC]",
                            "CoalescePartitionsExec",
                            join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[a@0 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@1 ASC]",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[c@2 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected_first_sort_enforcement, top_join, false);
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[test]
    fn smj_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        //Projection(a1 as a3, b1 as b3)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a1".to_string(), "a3".to_string()),
            ("b1".to_string(), "b3".to_string()),
        ];
        let left = projection_exec_with_alias(left, alias_pairs);

        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        //Projection(a as a2, b as b2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a2".to_string()),
            ("b".to_string(), "b2".to_string()),
        ];
        let right = projection_exec_with_alias(right, alias_pairs);

        // Join on (b3 == b2 && a3 == a2)
        let join_on = vec![
            (
                Column::new_with_schema("b3", &left.schema()).unwrap(),
                Column::new_with_schema("b2", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a3", &left.schema()).unwrap(),
                Column::new_with_schema("a2", &right.schema()).unwrap(),
            ),
        ];
        let join = sort_merge_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
            "SortExec: expr=[b3@1 ASC,a3@0 ASC]",
            "ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "SortExec: expr=[b2@1 ASC,a2@0 ASC]",
            "ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
            "SortPreservingRepartitionExec: partitioning=Hash([b3@1, a3@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[b3@1 ASC,a3@0 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "SortPreservingRepartitionExec: partitioning=Hash([b2@1, a2@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[b2@1 ASC,a2@0 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected_first_sort_enforcement, join, false);
        Ok(())
    }

    #[test]
    fn merge_does_not_need_sort() -> Result<()> {
        // see https://github.com/apache/arrow-datafusion/issues/4331
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        // Scan some sorted parquet files
        let exec = parquet_exec_multiple_sorted(vec![sort_key.clone()]);

        // CoalesceBatchesExec to mimic behavior after a filter
        let exec = Arc::new(CoalesceBatchesExec::new(exec, 4096));

        // Merge from multiple parquet files and keep the data sorted
        let exec = Arc::new(SortPreservingMergeExec::new(sort_key, exec));

        // The optimizer should not add an additional SortExec as the
        // data is already sorted
        let expected = &[
            "SortPreservingMergeExec: [a@0 ASC]",
            "CoalesceBatchesExec: target_batch_size=4096",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        // assert_optimized!(expected, exec, true);
        assert_optimized!(expected, exec, false);
        Ok(())
    }

    #[test]
    fn union_to_interleave() -> Result<()> {
        // group by (a as a1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );
        // group by (a as a2)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );

        //  Union
        let plan = Arc::new(UnionExec::new(vec![left, right]));

        // final agg
        let plan =
            aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

        // Only two RepartitionExecs added, no final RepartionExec required
        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]",
            "InterleaveExec",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }
}
