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

//! Enforcement optimizer rules are used to make sure the plan's Distribution and Ordering
//! requirements are met by inserting necessary [[RepartitionExec]] and [[SortExec]].
//!
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use crate::physical_plan::{Partitioning, TreeNodeRewritable};
use crate::prelude::SessionConfig;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::expressions::NoOp;
use datafusion_physical_expr::{
    expr_list_eq_strict_order, normalize_expr_with_equivalence_properties,
    normalize_sort_expr_with_equivalence_properties, PhysicalExpr, PhysicalSortExpr,
};
use std::collections::HashMap;
use std::sync::Arc;

/// BasicEnforcement rule, it ensures the Distribution and Ordering requirements are met
/// in the strictest way. It might add additional [[RepartitionExec]] to the plan tree
/// and give a non-optimal plan, but it can avoid the possible data skew in joins
///
/// For example for a HashJoin with keys(a, b, c), the required Distribution(a, b, c) can be satisfied by
/// several alternative partitioning ways: [(a, b, c), (a, b), (a, c), (b, c), (a), (b), (c), ( )].
///
/// This rule only chooses the exactly match and satisfies the Distribution(a, b, c) by a HashPartition(a, b, c).
#[derive(Default)]
pub struct BasicEnforcement {}

impl BasicEnforcement {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for BasicEnforcement {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.target_partitions;
        let top_down_join_key_reordering = config.top_down_join_key_reordering;
        let new_plan = if top_down_join_key_reordering {
            // Run a top-down process to adjust input key ordering recursively
            adjust_input_keys_down_recursively(plan, vec![])?
        } else {
            plan
        };
        // Distribution and Ordering enforcement need to be applied bottom-up.
        new_plan.transform_up(&{
            |plan| {
                let adjusted = if !top_down_join_key_reordering {
                    reorder_join_keys_to_inputs(plan)
                } else {
                    plan
                };
                Some(ensure_distribution_and_ordering(
                    adjusted,
                    target_partitions,
                ))
            }
        })
    }

    fn name(&self) -> &str {
        "BasicEnforcement"
    }
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// This method run a top-down process and try to adjust the output partitionging of the children
/// if children themselves are joins or aggregations.
fn adjust_input_keys_down_recursively(
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
    parent_required: Vec<Arc<dyn PhysicalExpr>>,
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
                )) = try_reorder(
                    join_key_pairs.clone(),
                    parent_required,
                    &plan.equivalence_properties(),
                ) {
                    let new_join_on = if !new_positions.is_empty() {
                        new_join_conditions(&left_keys, &right_keys)
                    } else {
                        on.clone()
                    };
                    let new_left =
                        adjust_input_keys_down_recursively(left.clone(), left_keys)?;
                    let new_right =
                        adjust_input_keys_down_recursively(right.clone(), right_keys)?;
                    Ok(Arc::new(HashJoinExec::try_new(
                        new_left,
                        new_right,
                        new_join_on,
                        filter.clone(),
                        join_type,
                        PartitionMode::Partitioned,
                        null_equals_null,
                    )?))
                } else {
                    let new_left = adjust_input_keys_down_recursively(
                        left.clone(),
                        join_key_pairs.left_keys,
                    )?;
                    let new_right = adjust_input_keys_down_recursively(
                        right.clone(),
                        join_key_pairs.right_keys,
                    )?;
                    Ok(Arc::new(HashJoinExec::try_new(
                        new_left,
                        new_right,
                        on.clone(),
                        filter.clone(),
                        join_type,
                        PartitionMode::Partitioned,
                        null_equals_null,
                    )?))
                }
            }
            PartitionMode::CollectLeft => {
                let new_right =
                    adjust_input_keys_down_recursively(right.clone(), parent_required)?;
                Ok(Arc::new(HashJoinExec::try_new(
                    left.clone(),
                    new_right,
                    on.clone(),
                    filter.clone(),
                    join_type,
                    PartitionMode::CollectLeft,
                    null_equals_null,
                )?))
            }
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
        )) = try_reorder(
            join_key_pairs.clone(),
            parent_required,
            &plan.equivalence_properties(),
        ) {
            let new_join_on = if !new_positions.is_empty() {
                new_join_conditions(&left_keys, &right_keys)
            } else {
                on.clone()
            };
            let new_options = if !new_positions.is_empty() {
                let mut new_sort_options = vec![];
                for idx in 0..sort_options.len() {
                    new_sort_options.push(sort_options[new_positions[idx]])
                }
                new_sort_options
            } else {
                sort_options.clone()
            };

            let new_left = adjust_input_keys_down_recursively(left.clone(), left_keys)?;
            let new_right =
                adjust_input_keys_down_recursively(right.clone(), right_keys)?;

            Ok(Arc::new(SortMergeJoinExec::try_new(
                new_left,
                new_right,
                new_join_on,
                *join_type,
                new_options,
                *null_equals_null,
            )?))
        } else {
            let new_left = adjust_input_keys_down_recursively(
                left.clone(),
                join_key_pairs.left_keys,
            )?;
            let new_right = adjust_input_keys_down_recursively(
                right.clone(),
                join_key_pairs.right_keys,
            )?;
            Ok(Arc::new(SortMergeJoinExec::try_new(
                new_left,
                new_right,
                on.clone(),
                *join_type,
                sort_options.clone(),
                *null_equals_null,
            )?))
        }
    } else if let Some(AggregateExec {
        mode,
        group_by,
        aggr_expr,
        input,
        input_schema,
        ..
    }) = plan_any.downcast_ref::<AggregateExec>()
    {
        if parent_required.is_empty() {
            Ok(plan)
        } else {
            match mode {
                AggregateMode::FinalPartitioned | AggregateMode::Partial => {
                    let out_put_columns = group_by
                        .expr()
                        .iter()
                        .enumerate()
                        .map(|(index, (_col, name))| Column::new(name, index))
                        .collect::<Vec<_>>();

                    let out_put_exprs = out_put_columns
                        .iter()
                        .map(|c| Arc::new(c.clone()) as Arc<dyn PhysicalExpr>)
                        .collect::<Vec<_>>();

                    // Check whether the requirements can be satisfied by the Aggregation
                    if parent_required.len() != out_put_exprs.len()
                        || expr_list_eq_strict_order(&out_put_exprs, &parent_required)
                        || !group_by.null_expr().is_empty()
                    {
                        Ok(plan)
                    } else {
                        let new_positions =
                            expected_expr_positions(&out_put_exprs, &parent_required);
                        match new_positions {
                            Some(positions) => {
                                let mut new_group_exprs = vec![];
                                for idx in positions.into_iter() {
                                    new_group_exprs.push(group_by.expr()[idx].clone());
                                }
                                let new_group_by =
                                    PhysicalGroupBy::new_single(new_group_exprs);
                                match mode {
                                    AggregateMode::FinalPartitioned => {
                                        let new_input =
                                            adjust_input_keys_down_recursively(
                                                input.clone(),
                                                parent_required,
                                            )?;
                                        let new_agg = Arc::new(AggregateExec::try_new(
                                            AggregateMode::FinalPartitioned,
                                            new_group_by,
                                            aggr_expr.clone(),
                                            new_input,
                                            input_schema.clone(),
                                        )?);

                                        // Need to create a new projection to change the expr ordering back
                                        let mut proj_exprs = out_put_columns
                                            .iter()
                                            .map(|col| {
                                                (
                                                    Arc::new(Column::new(
                                                        col.name(),
                                                        new_agg
                                                            .schema()
                                                            .index_of(col.name())
                                                            .unwrap(),
                                                    ))
                                                        as Arc<dyn PhysicalExpr>,
                                                    col.name().to_owned(),
                                                )
                                            })
                                            .collect::<Vec<_>>();
                                        let agg_schema = new_agg.schema();
                                        let agg_fields = agg_schema.fields();
                                        for (idx, field) in agg_fields
                                            .iter()
                                            .enumerate()
                                            .skip(out_put_columns.len())
                                        {
                                            proj_exprs.push((
                                                Arc::new(Column::new(
                                                    field.name().as_str(),
                                                    idx,
                                                ))
                                                    as Arc<dyn PhysicalExpr>,
                                                field.name().clone(),
                                            ))
                                        }
                                        // TODO merge adjacent Projections if there are
                                        Ok(Arc::new(ProjectionExec::try_new(
                                            proj_exprs, new_agg,
                                        )?))
                                    }
                                    AggregateMode::Partial => {
                                        Ok(Arc::new(AggregateExec::try_new(
                                            AggregateMode::Partial,
                                            new_group_by,
                                            aggr_expr.clone(),
                                            input.clone(),
                                            input_schema.clone(),
                                        )?))
                                    }
                                    _ => Ok(plan),
                                }
                            }
                            _ => Ok(plan),
                        }
                    }
                }
                _ => Ok(plan),
            }
        }
    } else if let Some(ProjectionExec { expr, .. }) =
        plan_any.downcast_ref::<ProjectionExec>()
    {
        // For Projection, we need to transform the columns to the columns before the Projection
        // And then to push down the requirements
        let mut column_mapping = HashMap::new();
        for (expression, name) in expr.iter() {
            if let Some(column) = expression.as_any().downcast_ref::<Column>() {
                column_mapping.insert(name.clone(), column.clone());
            };
        }
        let new_required: Vec<Arc<dyn PhysicalExpr>> = parent_required
            .iter()
            .filter_map(|r| {
                if let Some(column) = r.as_any().downcast_ref::<Column>() {
                    column_mapping.get(column.name())
                } else {
                    None
                }
            })
            .map(|e| Arc::new(e.clone()) as Arc<dyn PhysicalExpr>)
            .collect::<Vec<_>>();
        if new_required.len() == parent_required.len() {
            plan.map_children(|plan| {
                adjust_input_keys_down_recursively(plan, new_required.clone())
            })
        } else {
            Ok(plan)
        }
    } else if let Some(WindowAggExec { input: _, .. }) =
        plan_any.downcast_ref::<WindowAggExec>()
    {
        // TODO
        Ok(plan)
    } else if parent_required.is_empty() {
        Ok(plan)
    } else {
        plan.map_children(|plan| {
            adjust_input_keys_down_recursively(plan, parent_required.clone())
        })
    }
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// This method will try to change the ordering of the join keys to match with the
/// partitioning of the join nodes' children.
/// If it can not match with both sides, it will try to match with one, either left side or right side.
fn reorder_join_keys_to_inputs(
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
) -> Arc<dyn crate::physical_plan::ExecutionPlan> {
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
                    &plan.equivalence_properties(),
                ) {
                    if !new_positions.is_empty() {
                        let new_join_on = new_join_conditions(&left_keys, &right_keys);
                        Arc::new(
                            HashJoinExec::try_new(
                                left.clone(),
                                right.clone(),
                                new_join_on,
                                filter.clone(),
                                join_type,
                                PartitionMode::Partitioned,
                                null_equals_null,
                            )
                            .unwrap(),
                        )
                    } else {
                        plan
                    }
                } else {
                    plan
                }
            }
            _ => plan,
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
            &plan.equivalence_properties(),
        ) {
            if !new_positions.is_empty() {
                let new_join_on = new_join_conditions(&left_keys, &right_keys);
                let mut new_sort_options = vec![];
                for idx in 0..sort_options.len() {
                    new_sort_options.push(sort_options[new_positions[idx]])
                }
                Arc::new(
                    SortMergeJoinExec::try_new(
                        left.clone(),
                        right.clone(),
                        new_join_on,
                        *join_type,
                        new_sort_options,
                        *null_equals_null,
                    )
                    .unwrap(),
                )
            } else {
                plan
            }
        } else {
            plan
        }
    } else {
        plan
    }
}

/// Reorder the current join keys ordering based on either left partition or right partition.
fn reorder_current_join_keys(
    join_keys: JoinKeyPairs,
    left_partition: Option<Partitioning>,
    right_partition: Option<Partitioning>,
    equivalence_properties: &[Vec<Column>],
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    match (left_partition.clone(), right_partition.clone()) {
        (Some(Partitioning::Hash(left_exprs, _)), _) => {
            try_reorder(join_keys.clone(), left_exprs, equivalence_properties).or_else(
                || {
                    reorder_current_join_keys(
                        join_keys,
                        None,
                        right_partition,
                        equivalence_properties,
                    )
                },
            )
        }
        (_, Some(Partitioning::Hash(right_exprs, _))) => {
            try_reorder(join_keys.clone(), right_exprs, equivalence_properties).or_else(
                || {
                    reorder_current_join_keys(
                        join_keys,
                        left_partition,
                        None,
                        equivalence_properties,
                    )
                },
            )
        }
        _ => None,
    }
}

fn try_reorder(
    join_keys: JoinKeyPairs,
    expected: Vec<Arc<dyn PhysicalExpr>>,
    equivalence_properties: &[Vec<Column>],
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    if join_keys.left_keys.len() != expected.len() {
        return None;
    }
    if expr_list_eq_strict_order(&expected, &join_keys.left_keys) {
        return Some((join_keys, vec![]));
    }
    let new_positions = expected_expr_positions(&join_keys.left_keys, &expected);
    match new_positions {
        Some(positions) => {
            let mut new_right_keys = vec![];
            for pos in positions.iter() {
                new_right_keys.push(join_keys.right_keys[*pos].clone());
            }
            Some((
                JoinKeyPairs {
                    left_keys: expected,
                    right_keys: new_right_keys,
                },
                positions,
            ))
        }
        None => {
            if !equivalence_properties.is_empty() {
                let normalized_expected = expected
                    .iter()
                    .map(|e| {
                        normalize_expr_with_equivalence_properties(
                            e.clone(),
                            equivalence_properties,
                        )
                    })
                    .collect::<Vec<_>>();
                let normalized_left_keys = join_keys
                    .left_keys
                    .iter()
                    .map(|e| {
                        normalize_expr_with_equivalence_properties(
                            e.clone(),
                            equivalence_properties,
                        )
                    })
                    .collect::<Vec<_>>();
                if expr_list_eq_strict_order(&normalized_expected, &normalized_left_keys)
                {
                    Some((join_keys, vec![]))
                } else {
                    let new_positions = expected_expr_positions(
                        &normalized_left_keys,
                        &normalized_expected,
                    );
                    match new_positions {
                        Some(positions) => {
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
                        }
                        None => None,
                    }
                }
            } else {
                None
            }
        }
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

fn ensure_distribution_and_ordering(
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
    target_partitions: usize,
) -> Arc<dyn crate::physical_plan::ExecutionPlan> {
    if plan.children().is_empty() {
        return plan;
    }
    let required_input_distributions = plan.required_input_distribution();
    let required_input_orderings = plan.required_input_ordering();
    let children: Vec<Arc<dyn ExecutionPlan>> = plan.children();
    assert_eq!(children.len(), required_input_distributions.len());
    assert_eq!(children.len(), required_input_orderings.len());

    // Add RepartitionExec to guarantee output partitioning
    let children = children
        .into_iter()
        .zip(required_input_distributions.into_iter())
        .map(|(child, required)| {
            if child
                .output_partitioning()
                .satisfy(required.clone(), || child.equivalence_properties())
            {
                child
            } else {
                let new_child: Arc<dyn ExecutionPlan> = match required {
                    Distribution::SinglePartition
                        if child.output_partitioning().partition_count() > 1 =>
                    {
                        Arc::new(CoalescePartitionsExec::new(child.clone()))
                    }
                    _ => {
                        let partition = required.create_partitioning(target_partitions);
                        Arc::new(RepartitionExec::try_new(child, partition).unwrap())
                    }
                };
                new_child
            }
        });

    // Add SortExec to guarantee output ordering
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .zip(required_input_orderings.into_iter())
        .map(|(child, required)| {
            if ordering_satisfy(child.output_ordering(), required, || {
                child.equivalence_properties()
            }) {
                child
            } else {
                let sort_expr = required.unwrap().to_vec();
                if child.output_partitioning().partition_count() > 1 {
                    Arc::new(SortExec::new_with_partitioning(
                        sort_expr, child, true, None,
                    ))
                } else {
                    Arc::new(SortExec::try_new(sort_expr, child, None).unwrap())
                }
            }
        })
        .collect::<Vec<_>>();

    with_new_children_if_necessary(plan, new_children).unwrap()
}

/// DynamicEnforcement rule
///
///
#[derive(Default)]
pub struct DynamicEnforcement {}

// TODO
impl DynamicEnforcement {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Check the required ordering requirements are satisfied by the provided PhysicalSortExprs.
fn ordering_satisfy<F: FnOnce() -> Vec<Vec<Column>>>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortExpr]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            if required.len() > provided.len() {
                false
            } else {
                let fast_match = required
                    .iter()
                    .zip(provided.iter())
                    .all(|(order1, order2)| order1.eq(order2));

                if !fast_match {
                    let eq_properties = equal_properties();
                    if !eq_properties.is_empty() {
                        let normalized_required_exprs = required
                            .iter()
                            .map(|e| {
                                normalize_sort_expr_with_equivalence_properties(
                                    e.clone(),
                                    &eq_properties,
                                )
                            })
                            .collect::<Vec<_>>();
                        let normalized_provided_exprs = provided
                            .iter()
                            .map(|e| {
                                normalize_sort_expr_with_equivalence_properties(
                                    e.clone(),
                                    &eq_properties,
                                )
                            })
                            .collect::<Vec<_>>();
                        normalized_required_exprs
                            .iter()
                            .zip(normalized_provided_exprs.iter())
                            .all(|(order1, order2)| order1.eq(order2))
                    } else {
                        fast_match
                    }
                } else {
                    fast_match
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct JoinKeyPairs {
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{expressions, PhysicalExpr};

    use super::*;
    use crate::config::ConfigOptions;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::joins::{
        utils::JoinOn, HashJoinExec, PartitionMode, SortMergeJoinExec,
    };
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::{displayable, Statistics};

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
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                config_options: ConfigOptions::new().into_shareable(),
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
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        group_by,
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
                &false,
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

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = BasicEnforcement {};
            let optimized = optimizer
                .optimize($PLAN, &SessionConfig::new().with_target_partitions(10))?;

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent().to_string();
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
            JoinType::Semi,
            JoinType::Anti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            // Join on (a == c)
            let top_join_on = vec![(
                Column::new_with_schema("a", &join.schema()).unwrap(),
                Column::new_with_schema("c", &schema()).unwrap(),
            )];

            let top_join =
                hash_join_exec(join.clone(), parquet_exec(), &top_join_on, &join_type);

            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"c\", index: 2 }})]", join_type);
            let join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"b1\", index: 1 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs
                JoinType::Inner | JoinType::Left => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);

            match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    // This time we use (b1 == c) for top join
                    // Join on (b1 == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("b1", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];

                    let top_join =
                        hash_join_exec(join, parquet_exec(), &top_join_on, &join_type);
                    let top_join_plan =
                        format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"b1\", index: 6 }}, Column {{ name: \"c\", index: 2 }})]", join_type);

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                        ],
                        // Should include 4 RepartitionExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 6 }], 10)",
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected, top_join);
                }
                _ => {}
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
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a1\", index: 0 }, Column { name: \"c\", index: 2 })]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, top_join);

        // Join on (a2 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a2", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a2\", index: 1 }, Column { name: \"c\", index: 2 })]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join);
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
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"c\", index: 2 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ProjectionExec: expr=[c1@0 as a]",
            "ProjectionExec: expr=[c@2 as c1]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join);
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
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a1\", index: 0 }, Column { name: \"a2\", index: 0 })]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a1\", index: 0 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 0 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
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
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"b1\", index: 1 }, Column { name: \"b\", index: 0 }), (Column { name: \"a1\", index: 0 }, Column { name: \"a\", index: 1 })]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@1 as b1, a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 0 }, Column { name: \"a1\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 0 }, Column { name: \"a\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
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
        let top_left_join =
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner);

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let projection = projection_exec_with_alias(top_left_join, alias_pairs);

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
        let top_right_join =
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &projection.schema()).unwrap(),
                Column::new_with_schema("b1", &top_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &projection.schema()).unwrap(),
                Column::new_with_schema("c", &top_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &projection.schema()).unwrap(),
                Column::new_with_schema("a1", &top_right_join.schema()).unwrap(),
            ),
        ];

        let top_join = hash_join_exec(
            projection.clone(),
            top_right_join,
            &top_join_on,
            &JoinType::Inner,
        );

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"B\", index: 2 }, Column { name: \"b1\", index: 6 }), (Column { name: \"C\", index: 3 }, Column { name: \"c\", index: 2 }), (Column { name: \"AA\", index: 1 }, Column { name: \"a1\", index: 5 })]",
            "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"b\", index: 1 }, Column { name: \"b1\", index: 1 }), (Column { name: \"c\", index: 2 }, Column { name: \"c1\", index: 2 }), (Column { name: \"a\", index: 0 }, Column { name: \"a1\", index: 0 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }, Column { name: \"c\", index: 2 }, Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }, Column { name: \"c1\", index: 2 }, Column { name: \"a1\", index: 0 }], 10)",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"b\", index: 1 }, Column { name: \"b1\", index: 1 }), (Column { name: \"c\", index: 2 }, Column { name: \"c1\", index: 2 }), (Column { name: \"a\", index: 0 }, Column { name: \"a1\", index: 0 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }, Column { name: \"c\", index: 2 }, Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }, Column { name: \"c1\", index: 2 }, Column { name: \"a1\", index: 0 }], 10)",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, top_join);
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
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);

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
                format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"c\", index: 2 }})]", join_type);
            let join_plan =
                format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"b1\", index: 1 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs 3 SortExecs
                JoinType::Inner | JoinType::Left => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b1@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b1@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);

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
                        format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"b1\", index: 6 }}, Column {{ name: \"c\", index: 2 }})]", join_type);

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs and 3 SortExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "SortExec: [a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "SortExec: [b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "SortExec: [c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                        ],
                        // Should include 4 RepartitionExecs and 4 SortExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "SortExec: [b1@6 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 6 }], 10)",
                            join_plan.as_str(),
                            "SortExec: [a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "SortExec: [b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 1 }], 10)",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                            "SortExec: [c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected, top_join);
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
            "SortMergeJoin: join_type=Inner, on=[(Column { name: \"b3\", index: 1 }, Column { name: \"b2\", index: 1 }), (Column { name: \"a3\", index: 0 }, Column { name: \"a2\", index: 0 })]",
            "SortExec: [b3@1 ASC,a3@0 ASC]",
            "ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@1 as b1, a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b1\", index: 0 }, Column { name: \"a1\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "SortExec: [b2@1 ASC,a2@0 ASC]",
            "ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 0 }, Column { name: \"a\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
        Ok(())
    }
}
