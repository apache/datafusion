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

use std::fmt::Debug;
use std::sync::Arc;

use crate::optimizer::PhysicalOptimizerRule;
use crate::output_requirements::OutputRequirementExec;
use crate::utils::{
    add_sort_above_with_check, is_coalesce_partitions, is_repartition,
    is_sort_preserving_merge,
};
use crate::OptimizerContext;

use arrow::compute::SortOptions;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::logical_plan::JoinType;
use datafusion_physical_expr::expressions::{Column, NoOp};
use datafusion_physical_expr::utils::map_columns_before_projection;
use datafusion_physical_expr::{
    physical_exprs_equal, EquivalenceProperties, PhysicalExpr, PhysicalExprRef,
};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, PartitionMode, SortMergeJoinExec,
};
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::union::{can_interleave, InterleaveExec, UnionExec};
use datafusion_physical_plan::windows::WindowAggExec;
use datafusion_physical_plan::windows::{get_best_fitting_window, BoundedWindowAggExec};
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::{Distribution, ExecutionPlan, Partitioning};

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
#[derive(Default, Debug)]
pub struct EnforceDistribution {}

impl EnforceDistribution {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnforceDistribution {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        let top_down_join_key_reordering = config.optimizer.top_down_join_key_reordering;

        let adjusted = if top_down_join_key_reordering {
            // Run a top-down process to adjust input key ordering recursively
            let plan_requirements = PlanWithKeyRequirements::new_default(plan);
            let adjusted = plan_requirements
                .transform_down(adjust_input_keys_ordering)
                .data()?;
            adjusted.plan
        } else {
            // Run a bottom-up process
            plan.transform_up(|plan| {
                Ok(Transformed::yes(reorder_join_keys_to_inputs(plan)?))
            })
            .data()?
        };

        let distribution_context = DistributionContext::new_default(adjusted);
        // Distribution enforcement needs to be applied bottom-up.
        let distribution_context = distribution_context
            .transform_up(|distribution_context| {
                ensure_distribution(distribution_context, config)
            })
            .data()?;
        Ok(distribution_context.plan)
    }

    fn name(&self) -> &str {
        "EnforceDistribution"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct JoinKeyPairs {
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
}

/// Keeps track of parent required key orderings.
pub type PlanWithKeyRequirements = PlanContext<Vec<Arc<dyn PhysicalExpr>>>;

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
///    Requirements can be satisfied by adjusting keys ordering, clear the current requirements, generate new requirements(to pushdown) based on the adjusted join keys, return the changed plan.
///
/// 2) If the current plan is Aggregation, check whether the requirements can be satisfied by adjusting group by keys ordering:
///    Requirements can not be satisfied, clear all the requirements, return the unchanged plan.
///    Requirements is already satisfied, clear all the requirements, return the unchanged plan.
///    Requirements can be satisfied by adjusting keys ordering, clear all the requirements, return the changed plan.
///
/// 3) If the current plan is RepartitionExec, CoalescePartitionsExec or WindowAggExec, clear all the requirements, return the unchanged plan
/// 4) If the current plan is Projection, transform the requirements to the columns before the Projection and push down requirements
/// 5) For other types of operators, by default, pushdown the parent requirements to children.
pub fn adjust_input_keys_ordering(
    mut requirements: PlanWithKeyRequirements,
) -> Result<Transformed<PlanWithKeyRequirements>> {
    let plan = Arc::clone(&requirements.plan);

    if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        projection,
        mode,
        null_equality,
        ..
    }) = plan.as_any().downcast_ref::<HashJoinExec>()
    {
        match mode {
            PartitionMode::Partitioned => {
                let join_constructor = |new_conditions: (
                    Vec<(PhysicalExprRef, PhysicalExprRef)>,
                    Vec<SortOptions>,
                )| {
                    HashJoinExec::try_new(
                        Arc::clone(left),
                        Arc::clone(right),
                        new_conditions.0,
                        filter.clone(),
                        join_type,
                        // TODO: although projection is not used in the join here, because projection pushdown is after enforce_distribution. Maybe we need to handle it later. Same as filter.
                        projection.clone(),
                        PartitionMode::Partitioned,
                        *null_equality,
                    )
                    .map(|e| Arc::new(e) as _)
                };
                return reorder_partitioned_join_keys(
                    requirements,
                    on,
                    &[],
                    &join_constructor,
                )
                .map(Transformed::yes);
            }
            PartitionMode::CollectLeft => {
                // Push down requirements to the right side
                requirements.children[1].data = match join_type {
                    JoinType::Inner | JoinType::Right => shift_right_required(
                        &requirements.data,
                        left.schema().fields().len(),
                    )
                    .unwrap_or_default(),
                    JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                        requirements.data.clone()
                    }
                    JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::Full
                    | JoinType::LeftMark => vec![],
                };
            }
            PartitionMode::Auto => {
                // Can not satisfy, clear the current requirements and generate new empty requirements
                requirements.data.clear();
            }
        }
    } else if let Some(CrossJoinExec { left, .. }) =
        plan.as_any().downcast_ref::<CrossJoinExec>()
    {
        let left_columns_len = left.schema().fields().len();
        // Push down requirements to the right side
        requirements.children[1].data =
            shift_right_required(&requirements.data, left_columns_len)
                .unwrap_or_default();
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        sort_options,
        null_equality,
        ..
    }) = plan.as_any().downcast_ref::<SortMergeJoinExec>()
    {
        let join_constructor = |new_conditions: (
            Vec<(PhysicalExprRef, PhysicalExprRef)>,
            Vec<SortOptions>,
        )| {
            SortMergeJoinExec::try_new(
                Arc::clone(left),
                Arc::clone(right),
                new_conditions.0,
                filter.clone(),
                *join_type,
                new_conditions.1,
                *null_equality,
            )
            .map(|e| Arc::new(e) as _)
        };
        return reorder_partitioned_join_keys(
            requirements,
            on,
            sort_options,
            &join_constructor,
        )
        .map(Transformed::yes);
    } else if let Some(aggregate_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        if !requirements.data.is_empty() {
            if aggregate_exec.mode() == &AggregateMode::FinalPartitioned {
                return reorder_aggregate_keys(requirements, aggregate_exec)
                    .map(Transformed::yes);
            } else {
                requirements.data.clear();
            }
        } else {
            // Keep everything unchanged
            return Ok(Transformed::no(requirements));
        }
    } else if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let expr = proj.expr();
        // For Projection, we need to transform the requirements to the columns before the Projection
        // And then to push down the requirements
        // Construct a mapping from new name to the original Column
        let proj_exprs: Vec<_> = expr
            .iter()
            .map(|p| (Arc::clone(&p.expr), p.alias.clone()))
            .collect();
        let new_required = map_columns_before_projection(&requirements.data, &proj_exprs);
        if new_required.len() == requirements.data.len() {
            requirements.children[0].data = new_required;
        } else {
            // Can not satisfy, clear the current requirements and generate new empty requirements
            requirements.data.clear();
        }
    } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
        || plan.as_any().downcast_ref::<WindowAggExec>().is_some()
    {
        requirements.data.clear();
    } else {
        // By default, push down the parent requirements to children
        for child in requirements.children.iter_mut() {
            child.data.clone_from(&requirements.data);
        }
    }
    Ok(Transformed::yes(requirements))
}

pub fn reorder_partitioned_join_keys<F>(
    mut join_plan: PlanWithKeyRequirements,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    sort_options: &[SortOptions],
    join_constructor: &F,
) -> Result<PlanWithKeyRequirements>
where
    F: Fn(
        (Vec<(PhysicalExprRef, PhysicalExprRef)>, Vec<SortOptions>),
    ) -> Result<Arc<dyn ExecutionPlan>>,
{
    let parent_required = &join_plan.data;
    let join_key_pairs = extract_join_keys(on);
    let eq_properties = join_plan.plan.equivalence_properties();

    let (
        JoinKeyPairs {
            left_keys,
            right_keys,
        },
        positions,
    ) = try_reorder(join_key_pairs, parent_required, eq_properties);

    if let Some(positions) = positions {
        if !positions.is_empty() {
            let new_join_on = new_join_conditions(&left_keys, &right_keys);
            let new_sort_options = (0..sort_options.len())
                .map(|idx| sort_options[positions[idx]])
                .collect();
            join_plan.plan = join_constructor((new_join_on, new_sort_options))?;
        }
    }

    join_plan.children[0].data = left_keys;
    join_plan.children[1].data = right_keys;
    Ok(join_plan)
}

pub fn reorder_aggregate_keys(
    mut agg_node: PlanWithKeyRequirements,
    agg_exec: &AggregateExec,
) -> Result<PlanWithKeyRequirements> {
    let parent_required = &agg_node.data;
    let output_columns = agg_exec
        .group_expr()
        .expr()
        .iter()
        .enumerate()
        .map(|(index, (_, name))| Column::new(name, index))
        .collect::<Vec<_>>();

    let output_exprs = output_columns
        .iter()
        .map(|c| Arc::new(c.clone()) as _)
        .collect::<Vec<_>>();

    if parent_required.len() == output_exprs.len()
        && agg_exec.group_expr().null_expr().is_empty()
        && !physical_exprs_equal(&output_exprs, parent_required)
    {
        if let Some(positions) = expected_expr_positions(&output_exprs, parent_required) {
            if let Some(agg_exec) =
                agg_exec.input().as_any().downcast_ref::<AggregateExec>()
            {
                if matches!(agg_exec.mode(), &AggregateMode::Partial) {
                    let group_exprs = agg_exec.group_expr().expr();
                    let new_group_exprs = positions
                        .into_iter()
                        .map(|idx| group_exprs[idx].clone())
                        .collect();
                    let partial_agg = Arc::new(AggregateExec::try_new(
                        AggregateMode::Partial,
                        PhysicalGroupBy::new_single(new_group_exprs),
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        Arc::clone(agg_exec.input()),
                        Arc::clone(&agg_exec.input_schema),
                    )?);
                    // Build new group expressions that correspond to the output
                    // of the "reordered" aggregator:
                    let group_exprs = partial_agg.group_expr().expr();
                    let new_group_by = PhysicalGroupBy::new_single(
                        partial_agg
                            .output_group_expr()
                            .into_iter()
                            .enumerate()
                            .map(|(idx, expr)| (expr, group_exprs[idx].1.clone()))
                            .collect(),
                    );
                    let new_final_agg = Arc::new(AggregateExec::try_new(
                        AggregateMode::FinalPartitioned,
                        new_group_by,
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        Arc::clone(&partial_agg) as _,
                        agg_exec.input_schema(),
                    )?);

                    agg_node.plan = Arc::clone(&new_final_agg) as _;
                    agg_node.data.clear();
                    agg_node.children = vec![PlanWithKeyRequirements::new(
                        partial_agg as _,
                        vec![],
                        agg_node.children.swap_remove(0).children,
                    )];

                    // Need to create a new projection to change the expr ordering back
                    let agg_schema = new_final_agg.schema();
                    let mut proj_exprs = output_columns
                        .iter()
                        .map(|col| {
                            let name = col.name();
                            let index = agg_schema.index_of(name)?;
                            Ok(ProjectionExpr {
                                expr: Arc::new(Column::new(name, index)) as _,
                                alias: name.to_owned(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let agg_fields = agg_schema.fields();
                    for (idx, field) in
                        agg_fields.iter().enumerate().skip(output_columns.len())
                    {
                        let name = field.name();
                        let plan = Arc::new(Column::new(name, idx)) as _;
                        proj_exprs.push(ProjectionExpr {
                            expr: plan,
                            alias: name.clone(),
                        })
                    }
                    return ProjectionExec::try_new(proj_exprs, new_final_agg).map(|p| {
                        PlanWithKeyRequirements::new(Arc::new(p), vec![], vec![agg_node])
                    });
                }
            }
        }
    }
    Ok(agg_node)
}

fn shift_right_required(
    parent_required: &[Arc<dyn PhysicalExpr>],
    left_columns_len: usize,
) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let new_right_required = parent_required
        .iter()
        .filter_map(|r| {
            r.as_any().downcast_ref::<Column>().and_then(|col| {
                col.index()
                    .checked_sub(left_columns_len)
                    .map(|index| Arc::new(Column::new(col.name(), index)) as _)
            })
        })
        .collect::<Vec<_>>();

    // if the parent required are all coming from the right side, the requirements can be pushdown
    (new_right_required.len() == parent_required.len()).then_some(new_right_required)
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
pub fn reorder_join_keys_to_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        projection,
        mode,
        null_equality,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        if matches!(mode, PartitionMode::Partitioned) {
            let (join_keys, positions) = reorder_current_join_keys(
                extract_join_keys(on),
                Some(left.output_partitioning()),
                Some(right.output_partitioning()),
                left.equivalence_properties(),
                right.equivalence_properties(),
            );
            if positions.is_some_and(|idxs| !idxs.is_empty()) {
                let JoinKeyPairs {
                    left_keys,
                    right_keys,
                } = join_keys;
                let new_join_on = new_join_conditions(&left_keys, &right_keys);
                return Ok(Arc::new(HashJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    new_join_on,
                    filter.clone(),
                    join_type,
                    projection.clone(),
                    PartitionMode::Partitioned,
                    *null_equality,
                )?));
            }
        }
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        sort_options,
        null_equality,
        ..
    }) = plan_any.downcast_ref::<SortMergeJoinExec>()
    {
        let (join_keys, positions) = reorder_current_join_keys(
            extract_join_keys(on),
            Some(left.output_partitioning()),
            Some(right.output_partitioning()),
            left.equivalence_properties(),
            right.equivalence_properties(),
        );
        if let Some(positions) = positions {
            if !positions.is_empty() {
                let JoinKeyPairs {
                    left_keys,
                    right_keys,
                } = join_keys;
                let new_join_on = new_join_conditions(&left_keys, &right_keys);
                let new_sort_options = (0..sort_options.len())
                    .map(|idx| sort_options[positions[idx]])
                    .collect();
                return SortMergeJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    new_join_on,
                    filter.clone(),
                    *join_type,
                    new_sort_options,
                    *null_equality,
                )
                .map(|smj| Arc::new(smj) as _);
            }
        }
    }
    Ok(plan)
}

/// Reorder the current join keys ordering based on either left partition or right partition
fn reorder_current_join_keys(
    join_keys: JoinKeyPairs,
    left_partition: Option<&Partitioning>,
    right_partition: Option<&Partitioning>,
    left_equivalence_properties: &EquivalenceProperties,
    right_equivalence_properties: &EquivalenceProperties,
) -> (JoinKeyPairs, Option<Vec<usize>>) {
    match (left_partition, right_partition) {
        (Some(Partitioning::Hash(left_exprs, _)), _) => {
            match try_reorder(join_keys, left_exprs, left_equivalence_properties) {
                (join_keys, None) => reorder_current_join_keys(
                    join_keys,
                    None,
                    right_partition,
                    left_equivalence_properties,
                    right_equivalence_properties,
                ),
                result => result,
            }
        }
        (_, Some(Partitioning::Hash(right_exprs, _))) => {
            try_reorder(join_keys, right_exprs, right_equivalence_properties)
        }
        _ => (join_keys, None),
    }
}

fn try_reorder(
    join_keys: JoinKeyPairs,
    expected: &[Arc<dyn PhysicalExpr>],
    equivalence_properties: &EquivalenceProperties,
) -> (JoinKeyPairs, Option<Vec<usize>>) {
    let eq_groups = equivalence_properties.eq_group();
    let mut normalized_expected = vec![];
    let mut normalized_left_keys = vec![];
    let mut normalized_right_keys = vec![];
    if join_keys.left_keys.len() != expected.len() {
        return (join_keys, None);
    }
    if physical_exprs_equal(expected, &join_keys.left_keys)
        || physical_exprs_equal(expected, &join_keys.right_keys)
    {
        return (join_keys, Some(vec![]));
    } else if !equivalence_properties.eq_group().is_empty() {
        normalized_expected = expected
            .iter()
            .map(|e| eq_groups.normalize_expr(Arc::clone(e)))
            .collect();

        normalized_left_keys = join_keys
            .left_keys
            .iter()
            .map(|e| eq_groups.normalize_expr(Arc::clone(e)))
            .collect();

        normalized_right_keys = join_keys
            .right_keys
            .iter()
            .map(|e| eq_groups.normalize_expr(Arc::clone(e)))
            .collect();

        if physical_exprs_equal(&normalized_expected, &normalized_left_keys)
            || physical_exprs_equal(&normalized_expected, &normalized_right_keys)
        {
            return (join_keys, Some(vec![]));
        }
    }

    let Some(positions) = expected_expr_positions(&join_keys.left_keys, expected)
        .or_else(|| expected_expr_positions(&join_keys.right_keys, expected))
        .or_else(|| expected_expr_positions(&normalized_left_keys, &normalized_expected))
        .or_else(|| {
            expected_expr_positions(&normalized_right_keys, &normalized_expected)
        })
    else {
        return (join_keys, None);
    };

    let mut new_left_keys = vec![];
    let mut new_right_keys = vec![];
    for pos in positions.iter() {
        new_left_keys.push(Arc::clone(&join_keys.left_keys[*pos]));
        new_right_keys.push(Arc::clone(&join_keys.right_keys[*pos]));
    }
    let pairs = JoinKeyPairs {
        left_keys: new_left_keys,
        right_keys: new_right_keys,
    };

    (pairs, Some(positions))
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

fn extract_join_keys(on: &[(PhysicalExprRef, PhysicalExprRef)]) -> JoinKeyPairs {
    let (left_keys, right_keys) = on
        .iter()
        .map(|(l, r)| (Arc::clone(l) as _, Arc::clone(r) as _))
        .unzip();
    JoinKeyPairs {
        left_keys,
        right_keys,
    }
}

fn new_join_conditions(
    new_left_keys: &[Arc<dyn PhysicalExpr>],
    new_right_keys: &[Arc<dyn PhysicalExpr>],
) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
    new_left_keys
        .iter()
        .zip(new_right_keys.iter())
        .map(|(l_key, r_key)| (Arc::clone(l_key), Arc::clone(r_key)))
        .collect()
}

/// Adds RoundRobin repartition operator to the plan increase parallelism.
///
/// # Arguments
///
/// * `input`: Current node.
/// * `n_target`: desired target partition number, if partition number of the
///   current executor is less than this value. Partition number will be increased.
///
/// # Returns
///
/// A [`Result`] object that contains new execution plan where the desired
/// partition number is achieved by adding a RoundRobin repartition.
fn add_roundrobin_on_top(
    input: DistributionContext,
    n_target: usize,
) -> Result<DistributionContext> {
    // Adding repartition is helpful:
    if input.plan.output_partitioning().partition_count() < n_target {
        // When there is an existing ordering, we preserve ordering
        // during repartition. This will be un-done in the future
        // If any of the following conditions is true
        // - Preserving ordering is not helpful in terms of satisfying ordering requirements
        // - Usage of order preserving variants is not desirable
        // (determined by flag `config.optimizer.prefer_existing_sort`)
        let partitioning = Partitioning::RoundRobinBatch(n_target);
        let repartition =
            RepartitionExec::try_new(Arc::clone(&input.plan), partitioning)?
                .with_preserve_order();

        let new_plan = Arc::new(repartition) as _;

        Ok(DistributionContext::new(new_plan, true, vec![input]))
    } else {
        // Partition is not helpful, we already have desired number of partitions.
        Ok(input)
    }
}

/// Adds a hash repartition operator:
/// - to increase parallelism, and/or
/// - to satisfy requirements of the subsequent operators.
///
/// Repartition(Hash) is added on top of operator `input`.
///
/// # Arguments
///
/// * `input`: Current node.
/// * `hash_exprs`: Stores Physical Exprs that are used during hashing.
/// * `n_target`: desired target partition number, if partition number of the
///   current executor is less than this value. Partition number will be increased.
///
/// # Returns
///
/// A [`Result`] object that contains new execution plan where the desired
/// distribution is satisfied by adding a Hash repartition.
fn add_hash_on_top(
    input: DistributionContext,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    n_target: usize,
) -> Result<DistributionContext> {
    // Early return if hash repartition is unnecessary
    // `RepartitionExec: partitioning=Hash([...], 1), input_partitions=1` is unnecessary.
    if n_target == 1 && input.plan.output_partitioning().partition_count() == 1 {
        return Ok(input);
    }

    let dist = Distribution::HashPartitioned(hash_exprs);
    let satisfied = input
        .plan
        .output_partitioning()
        .satisfy(&dist, input.plan.equivalence_properties());

    // Add hash repartitioning when:
    // - The hash distribution requirement is not satisfied, or
    // - We can increase parallelism by adding hash partitioning.
    if !satisfied || n_target > input.plan.output_partitioning().partition_count() {
        // When there is an existing ordering, we preserve ordering during
        // repartition. This will be rolled back in the future if any of the
        // following conditions is true:
        // - Preserving ordering is not helpful in terms of satisfying ordering
        //   requirements.
        // - Usage of order preserving variants is not desirable (per the flag
        //   `config.optimizer.prefer_existing_sort`).
        let partitioning = dist.create_partitioning(n_target);
        let repartition =
            RepartitionExec::try_new(Arc::clone(&input.plan), partitioning)?
                .with_preserve_order();
        let plan = Arc::new(repartition) as _;

        return Ok(DistributionContext::new(plan, true, vec![input]));
    }

    Ok(input)
}

/// Adds a [`SortPreservingMergeExec`] or a [`CoalescePartitionsExec`] operator
/// on top of the given plan node to satisfy a single partition requirement
/// while preserving ordering constraints.
///
/// # Parameters
///
/// * `input`: Current node.
///
/// # Returns
///
/// Updated node with an execution plan, where the desired single distribution
/// requirement is satisfied.
fn add_merge_on_top(input: DistributionContext) -> DistributionContext {
    // Apply only when the partition count is larger than one.
    if input.plan.output_partitioning().partition_count() > 1 {
        // When there is an existing ordering, we preserve ordering
        // when decreasing partitions. This will be un-done in the future
        // if any of the following conditions is true
        // - Preserving ordering is not helpful in terms of satisfying ordering requirements
        // - Usage of order preserving variants is not desirable
        // (determined by flag `config.optimizer.prefer_existing_sort`)
        let new_plan = if let Some(req) = input.plan.output_ordering() {
            Arc::new(SortPreservingMergeExec::new(
                req.clone(),
                Arc::clone(&input.plan),
            )) as _
        } else {
            // If there is no input order, we can simply coalesce partitions:
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&input.plan))) as _
        };

        DistributionContext::new(new_plan, true, vec![input])
    } else {
        input
    }
}

/// Updates the physical plan inside [`DistributionContext`] so that distribution
/// changing operators are removed from the top. If they are necessary, they will
/// be added in subsequent stages.
///
/// Assume that following plan is given:
/// ```text
/// "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "    DataSourceExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC], file_type=parquet",
/// ```
///
/// Since `RepartitionExec`s change the distribution, this function removes
/// them and returns following plan:
///
/// ```text
/// "DataSourceExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC], file_type=parquet",
/// ```
fn remove_dist_changing_operators(
    mut distribution_context: DistributionContext,
) -> Result<DistributionContext> {
    while is_repartition(&distribution_context.plan)
        || is_coalesce_partitions(&distribution_context.plan)
        || is_sort_preserving_merge(&distribution_context.plan)
    {
        // All of above operators have a single child. First child is only child.
        // Remove any distribution changing operators at the beginning:
        distribution_context = distribution_context.children.swap_remove(0);
        // Note that they will be re-inserted later on if necessary or helpful.
    }

    Ok(distribution_context)
}

/// Updates the [`DistributionContext`] if preserving ordering while changing partitioning is not helpful or desirable.
///
/// Assume that following plan is given:
/// ```text
/// "SortPreservingMergeExec: \[a@0 ASC]"
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10, preserve_order=true",
/// "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true",
/// "      DataSourceExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC], file_type=parquet",
/// ```
///
/// This function converts plan above to the following:
///
/// ```text
/// "CoalescePartitionsExec"
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "      DataSourceExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC], file_type=parquet",
/// ```
pub fn replace_order_preserving_variants(
    mut context: DistributionContext,
) -> Result<DistributionContext> {
    context.children = context
        .children
        .into_iter()
        .map(|child| {
            if child.data {
                replace_order_preserving_variants(child)
            } else {
                Ok(child)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    if is_sort_preserving_merge(&context.plan) {
        let child_plan = Arc::clone(&context.children[0].plan);
        context.plan = Arc::new(
            CoalescePartitionsExec::new(child_plan).with_fetch(context.plan.fetch()),
        );
        return Ok(context);
    } else if let Some(repartition) =
        context.plan.as_any().downcast_ref::<RepartitionExec>()
    {
        if repartition.preserve_order() {
            context.plan = Arc::new(RepartitionExec::try_new(
                Arc::clone(&context.children[0].plan),
                repartition.partitioning().clone(),
            )?);
            return Ok(context);
        }
    }

    context.update_plan_from_children()
}

/// A struct to keep track of repartition requirements for each child node.
struct RepartitionRequirementStatus {
    /// The distribution requirement for the node.
    requirement: Distribution,
    /// Designates whether round robin partitioning is theoretically beneficial;
    /// i.e. the operator can actually utilize parallelism.
    roundrobin_beneficial: bool,
    /// Designates whether round robin partitioning is beneficial according to
    /// the statistical information we have on the number of rows.
    roundrobin_beneficial_stats: bool,
    /// Designates whether hash partitioning is necessary.
    hash_necessary: bool,
}

/// Calculates the `RepartitionRequirementStatus` for each children to generate
/// consistent and sensible (in terms of performance) distribution requirements.
/// As an example, a hash join's left (build) child might produce
///
/// ```text
/// RepartitionRequirementStatus {
///     ..,
///     hash_necessary: true
/// }
/// ```
///
/// while its right (probe) child might have very few rows and produce:
///
/// ```text
/// RepartitionRequirementStatus {
///     ..,
///     hash_necessary: false
/// }
/// ```
///
/// These statuses are not consistent as all children should agree on hash
/// partitioning. This function aligns the statuses to generate consistent
/// hash partitions for each children. After alignment, the right child's
/// status would turn into:
///
/// ```text
/// RepartitionRequirementStatus {
///     ..,
///     hash_necessary: true
/// }
/// ```
fn get_repartition_requirement_status(
    plan: &Arc<dyn ExecutionPlan>,
    batch_size: usize,
    should_use_estimates: bool,
) -> Result<Vec<RepartitionRequirementStatus>> {
    let mut needs_alignment = false;
    let children = plan.children();
    let rr_beneficial = plan.benefits_from_input_partitioning();
    let requirements = plan.required_input_distribution();
    let mut repartition_status_flags = vec![];
    for (child, requirement, roundrobin_beneficial) in
        izip!(children.into_iter(), requirements, rr_beneficial)
    {
        // Decide whether adding a round robin is beneficial depending on
        // the statistical information we have on the number of rows:
        let roundrobin_beneficial_stats = match child.partition_statistics(None)?.num_rows
        {
            Precision::Exact(n_rows) => n_rows > batch_size,
            Precision::Inexact(n_rows) => !should_use_estimates || (n_rows > batch_size),
            Precision::Absent => true,
        };
        let is_hash = matches!(requirement, Distribution::HashPartitioned(_));
        // Hash re-partitioning is necessary when the input has more than one
        // partitions:
        let multi_partitions = child.output_partitioning().partition_count() > 1;
        let roundrobin_sensible = roundrobin_beneficial && roundrobin_beneficial_stats;
        needs_alignment |= is_hash && (multi_partitions || roundrobin_sensible);
        repartition_status_flags.push((
            is_hash,
            RepartitionRequirementStatus {
                requirement,
                roundrobin_beneficial,
                roundrobin_beneficial_stats,
                hash_necessary: is_hash && multi_partitions,
            },
        ));
    }
    // Align hash necessary flags for hash partitions to generate consistent
    // hash partitions at each children:
    if needs_alignment {
        // When there is at least one hash requirement that is necessary or
        // beneficial according to statistics, make all children require hash
        // repartitioning:
        for (is_hash, status) in &mut repartition_status_flags {
            if *is_hash {
                status.hash_necessary = true;
            }
        }
    }
    Ok(repartition_status_flags
        .into_iter()
        .map(|(_, status)| status)
        .collect())
}

/// This function checks whether we need to add additional data exchange
/// operators to satisfy distribution requirements. Since this function
/// takes care of such requirements, we should avoid manually adding data
/// exchange operators in other places.
///
/// This function is intended to be used in a bottom up traversal, as it
/// can first repartition (or newly partition) at the datasources -- these
/// source partitions may be later repartitioned with additional data exchange operators.
pub fn ensure_distribution(
    dist_context: DistributionContext,
    config: &ConfigOptions,
) -> Result<Transformed<DistributionContext>> {
    let dist_context = update_children(dist_context)?;

    if dist_context.plan.children().is_empty() {
        return Ok(Transformed::no(dist_context));
    }

    let target_partitions = config.execution.target_partitions;
    // When `false`, round robin repartition will not be added to increase parallelism
    let enable_round_robin = config.optimizer.enable_round_robin_repartition;
    let repartition_file_scans = config.optimizer.repartition_file_scans;
    let batch_size = config.execution.batch_size;
    let should_use_estimates = config
        .execution
        .use_row_number_estimates_to_optimize_partitioning;
    let unbounded_and_pipeline_friendly = dist_context.plan.boundedness().is_unbounded()
        && matches!(
            dist_context.plan.pipeline_behavior(),
            EmissionType::Incremental | EmissionType::Both
        );
    // Use order preserving variants either of the conditions true
    // - it is desired according to config
    // - when plan is unbounded
    // - when it is pipeline friendly (can incrementally produce results)
    let order_preserving_variants_desirable =
        unbounded_and_pipeline_friendly || config.optimizer.prefer_existing_sort;

    // Remove unnecessary repartition from the physical plan if any
    let DistributionContext {
        mut plan,
        data,
        children,
    } = remove_dist_changing_operators(dist_context)?;

    if let Some(exec) = plan.as_any().downcast_ref::<WindowAggExec>() {
        if let Some(updated_window) = get_best_fitting_window(
            exec.window_expr(),
            exec.input(),
            &exec.partition_keys(),
        )? {
            plan = updated_window;
        }
    } else if let Some(exec) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() {
        if let Some(updated_window) = get_best_fitting_window(
            exec.window_expr(),
            exec.input(),
            &exec.partition_keys(),
        )? {
            plan = updated_window;
        }
    };

    let repartition_status_flags =
        get_repartition_requirement_status(&plan, batch_size, should_use_estimates)?;
    // This loop iterates over all the children to:
    // - Increase parallelism for every child if it is beneficial.
    // - Satisfy the distribution requirements of every child, if it is not
    //   already satisfied.
    // We store the updated children in `new_children`.
    let children = izip!(
        children.into_iter(),
        plan.required_input_ordering(),
        plan.maintains_input_order(),
        repartition_status_flags.into_iter()
    )
    .map(
        |(
            mut child,
            required_input_ordering,
            maintains,
            RepartitionRequirementStatus {
                requirement,
                roundrobin_beneficial,
                roundrobin_beneficial_stats,
                hash_necessary,
            },
        )| {
            let add_roundrobin = enable_round_robin
                // Operator benefits from partitioning (e.g. filter):
                && roundrobin_beneficial
                && roundrobin_beneficial_stats
                // Unless partitioning increases the partition count, it is not beneficial:
                && child.plan.output_partitioning().partition_count() < target_partitions;

            // When `repartition_file_scans` is set, attempt to increase
            // parallelism at the source.
            //
            // If repartitioning is not possible (a.k.a. None is returned from `ExecutionPlan::repartitioned`)
            // then no repartitioning will have occurred. As the default implementation returns None, it is only
            // specific physical plan nodes, such as certain datasources, which are repartitioned.
            if repartition_file_scans && roundrobin_beneficial_stats {
                if let Some(new_child) =
                    child.plan.repartitioned(target_partitions, config)?
                {
                    child.plan = new_child;
                }
            }

            // Satisfy the distribution requirement if it is unmet.
            match &requirement {
                Distribution::SinglePartition => {
                    child = add_merge_on_top(child);
                }
                Distribution::HashPartitioned(exprs) => {
                    // See https://github.com/apache/datafusion/issues/18341#issuecomment-3503238325 for background
                    if add_roundrobin && !hash_necessary {
                        // Add round-robin repartitioning on top of the operator
                        // to increase parallelism.
                        child = add_roundrobin_on_top(child, target_partitions)?;
                    }
                    // When inserting hash is necessary to satisfy hash requirement, insert hash repartition.
                    if hash_necessary {
                        child =
                            add_hash_on_top(child, exprs.to_vec(), target_partitions)?;
                    }
                }
                Distribution::UnspecifiedDistribution => {
                    if add_roundrobin {
                        // Add round-robin repartitioning on top of the operator
                        // to increase parallelism.
                        child = add_roundrobin_on_top(child, target_partitions)?;
                    }
                }
            };

            // There is an ordering requirement of the operator:
            if let Some(required_input_ordering) = required_input_ordering {
                // Either:
                // - Ordering requirement cannot be satisfied by preserving ordering through repartitions, or
                // - using order preserving variant is not desirable.
                let sort_req = required_input_ordering.into_single();
                let ordering_satisfied = child
                    .plan
                    .equivalence_properties()
                    .ordering_satisfy_requirement(sort_req.clone())?;

                if (!ordering_satisfied || !order_preserving_variants_desirable)
                    && child.data
                {
                    child = replace_order_preserving_variants(child)?;
                    // If ordering requirements were satisfied before repartitioning,
                    // make sure ordering requirements are still satisfied after.
                    if ordering_satisfied {
                        // Make sure to satisfy ordering requirement:
                        child = add_sort_above_with_check(
                            child,
                            sort_req,
                            plan.as_any()
                                .downcast_ref::<OutputRequirementExec>()
                                .map(|output| output.fetch())
                                .unwrap_or(None),
                        )?;
                    }
                }
                // Stop tracking distribution changing operators
                child.data = false;
            } else {
                // no ordering requirement
                match requirement {
                    // Operator requires specific distribution.
                    Distribution::SinglePartition | Distribution::HashPartitioned(_) => {
                        // Since there is no ordering requirement, preserving ordering is pointless
                        child = replace_order_preserving_variants(child)?;
                    }
                    Distribution::UnspecifiedDistribution => {
                        // Since ordering is lost, trying to preserve ordering is pointless
                        if !maintains || plan.as_any().is::<OutputRequirementExec>() {
                            child = replace_order_preserving_variants(child)?;
                        }
                    }
                }
            }
            Ok(child)
        },
    )
    .collect::<Result<Vec<_>>>()?;

    let children_plans = children
        .iter()
        .map(|c| Arc::clone(&c.plan))
        .collect::<Vec<_>>();

    plan = if plan.as_any().is::<UnionExec>()
        && !config.optimizer.prefer_existing_union
        && can_interleave(children_plans.iter())
    {
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
        Arc::new(InterleaveExec::try_new(children_plans)?)
    } else {
        plan.with_new_children(children_plans)?
    };

    Ok(Transformed::yes(DistributionContext::new(
        plan, data, children,
    )))
}

/// Keeps track of distribution changing operators (like `RepartitionExec`,
/// `SortPreservingMergeExec`, `CoalescePartitionsExec`) and their ancestors.
/// Using this information, we can optimize distribution of the plan if/when
/// necessary.
pub type DistributionContext = PlanContext<bool>;

fn update_children(mut dist_context: DistributionContext) -> Result<DistributionContext> {
    for child_context in dist_context.children.iter_mut() {
        let child_plan_any = child_context.plan.as_any();
        child_context.data =
            if let Some(repartition) = child_plan_any.downcast_ref::<RepartitionExec>() {
                !matches!(
                    repartition.partitioning(),
                    Partitioning::UnknownPartitioning(_)
                )
            } else {
                child_plan_any.is::<SortPreservingMergeExec>()
                    || child_plan_any.is::<CoalescePartitionsExec>()
                    || child_context.plan.children().is_empty()
                    || child_context.children[0].data
                    || child_context
                        .plan
                        .required_input_distribution()
                        .iter()
                        .zip(child_context.children.iter())
                        .any(|(required_dist, child_context)| {
                            child_context.data
                                && matches!(
                                    required_dist,
                                    Distribution::UnspecifiedDistribution
                                )
                        })
            }
    }

    dist_context.data = false;
    Ok(dist_context)
}

// See tests in datafusion/core/tests/physical_optimizer
