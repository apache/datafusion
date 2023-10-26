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

//! Optimizer rule to replace `where false` on a plan with an empty relation.
//! This saves time in planning and executing the query.
//! Note that this rule should be applied after simplify expressions optimizer rule.
use crate::optimizer::ApplyOrder;
use datafusion_common::{
    get_required_group_by_exprs_indices, plan_err, Column, DFSchema, DFSchemaRef,
    DataFusionError, JoinType, Result,
};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    logical_plan::LogicalPlan, Aggregate, Analyze, Explain, Expr, Partitioning,
    Projection, TableScan, Window,
};
use itertools::izip;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that eliminate the scalar value (true/false) filter with an [LogicalPlan::EmptyRelation]
#[derive(Default)]
pub struct RemoveUnusedColumns {}

impl RemoveUnusedColumns {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RemoveUnusedColumns {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let indices = require_all_indices(plan);
        let res = try_optimize_internal(plan, config, indices)?;
        Ok(res)
    }

    fn name(&self) -> &str {
        "RemoveUnusedColumns"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

fn is_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarSubquery(_) => true,
        Expr::Alias(alias) => is_subquery(&alias.expr),
        Expr::BinaryExpr(binary) => {
            is_subquery(&binary.left) | is_subquery(&binary.right)
        }
        _ => false,
    }
}

fn get_required_exprs(input_schema: &Arc<DFSchema>, indices: &[usize]) -> Vec<Expr> {
    let fields = input_schema.fields();
    indices
        .iter()
        .map(|&idx| Expr::Column(fields[idx].qualified_column()))
        .collect()
}

fn get_referred_indices(input: &LogicalPlan, exprs: &[Expr]) -> Result<Vec<usize>> {
    if exprs.iter().any(is_subquery) {
        Ok(require_all_indices(input))
    } else {
        let mut new_indices = vec![];
        for expr in exprs {
            let cols = expr.to_columns()?;
            for col in cols {
                if input.schema().has_column(&col) {
                    let idx = input.schema().index_of_column(&col)?;
                    if !new_indices.contains(&idx) {
                        new_indices.push(idx);
                    }
                }
            }
        }
        new_indices.sort();
        Ok(new_indices)
    }
}

fn get_at_indices(exprs: &[Expr], indices: &[usize]) -> Vec<Expr> {
    indices
        .iter()
        .filter_map(|&idx| {
            if idx < exprs.len() {
                Some(exprs[idx].clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}

fn merge_vectors(lhs: &[usize], rhs: &[usize]) -> Vec<usize> {
    let mut merged = lhs.to_vec();
    merged.extend(rhs);
    // Make sure to run sort before dedup.
    // Dedup removes consecutive same entries
    // If sort is run before it, all duplicates are removed.
    merged.sort();
    merged.dedup();
    merged
}

fn join_child_requirement(
    child: &LogicalPlan,
    on: &[Expr],
    filter: &[Expr],
) -> Result<Vec<usize>> {
    let on_indices = get_referred_indices(child, on)?;
    let filter_indices = get_referred_indices(child, filter)?;
    Ok(merge_vectors(&on_indices, &filter_indices))
}

fn split_join_requirement_indices_to_children(
    left_len: usize,
    indices: &[usize],
    join_type: &JoinType,
) -> (Vec<usize>, Vec<usize>) {
    match join_type {
        // In these cases split
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {}
        JoinType::LeftAnti | JoinType::LeftSemi => return (indices.to_vec(), vec![]),
        JoinType::RightSemi | JoinType::RightAnti => return (vec![], indices.to_vec()),
    }
    let left_requirements_from_parent = indices
        .iter()
        .filter_map(|&idx| if idx < left_len { Some(idx) } else { None })
        .collect::<Vec<_>>();
    let right_requirements_from_parent = indices
        .iter()
        .filter_map(|&idx| {
            if idx >= left_len {
                Some(idx - left_len)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (
        left_requirements_from_parent,
        right_requirements_from_parent,
    )
}

/// Get the projection exprs from columns in the order of the schema
fn get_expr(columns: &HashSet<Column>, schema: &DFSchemaRef) -> Result<Vec<Expr>> {
    let expr = schema
        .fields()
        .iter()
        .flat_map(|field| {
            let qc = field.qualified_column();
            let uqc = field.unqualified_column();
            if columns.contains(&qc) || columns.contains(&uqc) {
                Some(Expr::Column(qc))
            } else {
                None
            }
        })
        .collect::<Vec<Expr>>();
    if columns.len() != expr.len() {
        plan_err!("required columns can't push down, columns: {columns:?}")
    } else {
        Ok(expr)
    }
}

// TODO: Add is_changed flag
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
    projection_beneficial: bool,
) -> Result<LogicalPlan> {
    if !projection_beneficial {
        return Ok(plan);
    }
    let projection_beneficial =
        projection_beneficial && project_exprs.len() < plan.schema().fields().len();
    if projection_beneficial {
        Projection::try_new(project_exprs, Arc::new(plan)).map(LogicalPlan::Projection)
    } else {
        Ok(plan)
    }
}

/// Requires all indices for the plan
fn require_all_indices(plan: &LogicalPlan) -> Vec<usize> {
    (0..plan.schema().fields().len()).collect()
}

fn try_optimize_internal(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: Vec<usize>,
) -> Result<Option<LogicalPlan>> {
    let child_required_indices: Option<Vec<(Vec<usize>, bool)>> = match plan {
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, &indices);
            let required_indices = get_referred_indices(&proj.input, &exprs_used)?;
            if let Some(input) =
                try_optimize_internal(&proj.input, _config, required_indices)?
            {
                let new_proj = Projection::try_new(exprs_used, Arc::new(input))?;
                let new_proj = LogicalPlan::Projection(new_proj);
                return Ok(Some(new_proj));
            } else {
                return Ok(None);
            };
        }
        LogicalPlan::Aggregate(aggregate) => {
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;
            let group_bys_used = if let Some(simplest_groupby_indices) =
                get_required_group_by_exprs_indices(
                    aggregate.input.schema(),
                    &group_by_expr_existing,
                ) {
                let required_indices = merge_vectors(&simplest_groupby_indices, &indices);
                get_at_indices(&aggregate.group_expr, &required_indices)
            } else {
                aggregate.group_expr.clone()
            };
            let new_aggr_expr = aggregate
                .aggr_expr
                .iter()
                .enumerate()
                .filter_map(|(idx, aggr_expr)| {
                    if indices.contains(&(idx + aggregate.group_expr.len())) {
                        Some(aggr_expr.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let mut all_exprs = group_bys_used.clone();
            all_exprs.extend(new_aggr_expr.clone());
            let mut necessary_indices =
                get_referred_indices(&aggregate.input, &all_exprs)?;
            if necessary_indices.is_empty()
                && !aggregate.input.schema().fields().is_empty()
            {
                // When aggregate doesn't require any column, require at least one column
                // This can arise when group by Count(*)
                necessary_indices.push(0);
                let col =
                    Expr::Column(aggregate.input.schema().fields()[0].qualified_column());
                all_exprs.push(col);
            }

            let aggregate_input = if let Some(input) =
                try_optimize_internal(&aggregate.input, _config, necessary_indices)?
            {
                input
            } else {
                aggregate.input.as_ref().clone()
            };
            let mut required_columns = HashSet::new();
            for e in all_exprs.iter() {
                expr_to_columns(e, &mut required_columns)?
            }
            let new_expr = get_expr(&required_columns, aggregate.input.schema())?;
            let aggregate_input =
                add_projection_on_top_if_helpful(aggregate_input, new_expr, true)?;
            // At the input of the aggregate project schema, so that we do not use unnecessary sections.
            let aggregate = Aggregate::try_new(
                Arc::new(aggregate_input),
                group_bys_used,
                new_aggr_expr,
            )?;
            return Ok(Some(LogicalPlan::Aggregate(aggregate)));
        }
        LogicalPlan::Sort(sort) => {
            let indices_referred_by_sort = get_referred_indices(&sort.input, &sort.expr)?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_sort);
            Some(vec![(required_indices, true)])
        }
        LogicalPlan::Filter(filter) => {
            let indices_referred_by_filter =
                get_referred_indices(&filter.input, &[filter.predicate.clone()])?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_filter);
            Some(vec![(required_indices, true)])
        }
        LogicalPlan::Window(window) => {
            let n_input_fields = window.input.schema().fields().len();
            let new_window_expr = window
                .window_expr
                .iter()
                .enumerate()
                .filter_map(|(idx, window_expr)| {
                    if indices.contains(&(idx + n_input_fields)) {
                        Some(window_expr.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let indices_referred_by_window =
                get_referred_indices(&window.input, &new_window_expr)?;
            let n_input_fields = window.input.schema().fields().len();
            let window_child_indices = indices
                .iter()
                .filter_map(|idx| {
                    if *idx < n_input_fields {
                        Some(*idx)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let required_indices =
                merge_vectors(&window_child_indices, &indices_referred_by_window);
            let window_child = if let Some(new_window_child) =
                try_optimize_internal(&window.input, _config, required_indices.clone())?
            {
                new_window_child
            } else {
                window.input.as_ref().clone()
            };
            if new_window_expr.is_empty() {
                return Ok(Some(window_child));
            } else {
                // Calculated required expression according to old child, otherwise indices are not valid.
                let required_exprs =
                    get_required_exprs(window.input.schema(), &required_indices);
                let window_child =
                    add_projection_on_top_if_helpful(window_child, required_exprs, true)?;
                let window = Window::try_new(new_window_expr, Arc::new(window_child))?;
                return Ok(Some(LogicalPlan::Window(window)));
            }
        }
        LogicalPlan::Join(join) => {
            let left_len = join.left.schema().fields().len();
            let (left_requirements_from_parent, right_requirements_from_parent) =
                split_join_requirement_indices_to_children(
                    left_len,
                    &indices,
                    &join.join_type,
                );
            let (left_on, right_on): (Vec<_>, Vec<_>) = join.on.iter().cloned().unzip();
            let join_filter = &join
                .filter
                .as_ref()
                .map(|item| vec![item.clone()])
                .unwrap_or_default();
            let left_indices = join_child_requirement(&join.left, &left_on, join_filter)?;
            let left_indices =
                merge_vectors(&left_requirements_from_parent, &left_indices);

            let right_indices =
                join_child_requirement(&join.right, &right_on, join_filter)?;
            let right_indices =
                merge_vectors(&right_requirements_from_parent, &right_indices);
            Some(vec![(left_indices, true), (right_indices, true)])
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (mut left_child_indices, mut right_child_indices) =
                split_join_requirement_indices_to_children(
                    left_len,
                    &indices,
                    &JoinType::Inner,
                );
            if left_child_indices.is_empty() {
                left_child_indices.push(0);
            }
            if right_child_indices.is_empty() {
                right_child_indices.push(0);
            }
            Some(vec![
                (left_child_indices, true),
                (right_child_indices, true),
            ])
        }
        LogicalPlan::Repartition(repartition) => {
            // Repartition refers to these indices
            let exprs_used =
                if let Partitioning::Hash(exprs, _) = &repartition.partitioning_scheme {
                    exprs.as_slice()
                } else {
                    &[]
                };
            let referred_indices = get_referred_indices(&repartition.input, exprs_used)?;
            // required indices from parent propagated directly.
            let required_indices = merge_vectors(&indices, &referred_indices);
            Some(vec![(required_indices, false)])
        }
        LogicalPlan::Union(union) => {
            // Union directly re-routes requirements from its parents.
            Some(vec![(indices, true); union.inputs.len()])
        }
        LogicalPlan::TableScan(table_scan) => {
            // let filter_referred_indices =
            //     get_referred_indices(plan, &table_scan.filters)?;
            // let indices = merge_vectors(&indices, &filter_referred_indices);
            let indices = if indices.is_empty() {
                // // Use at least 1 column if not empty
                // if table_scan.projected_schema.fields().is_empty() {
                //     vec![]
                // } else {
                //     vec![0]
                // }
                vec![]
            } else {
                indices
            };
            let projection_fields = table_scan.projected_schema.fields();
            let schema = table_scan.source.schema();
            let fields_used = indices
                .iter()
                .map(|idx| projection_fields[*idx].clone())
                .collect::<Vec<_>>();
            let projection = fields_used
                .iter()
                .map(|field_proj| {
                    schema
                        .fields()
                        .iter()
                        .position(|field_source| field_proj.field() == field_source)
                })
                .collect::<Option<Vec<_>>>();

            return Ok(Some(LogicalPlan::TableScan(TableScan::try_new(
                table_scan.table_name.clone(),
                table_scan.source.clone(),
                projection,
                table_scan.filters.clone(),
                table_scan.fetch,
            )?)));
        }
        // SubqueryAlias alias can route requirement for its parent to its child
        LogicalPlan::SubqueryAlias(_sub_query_alias) => Some(vec![(indices, true)]),
        LogicalPlan::Subquery(_sub_query) => {
            // Subquery may use additional fields other than requirement from above.
            // Additionally, it is not trivial how to find all indices that may be used by subquery
            // Hence, we stop iteration here to be in the safe side.
            Some(vec![(indices, true)])
        }
        LogicalPlan::EmptyRelation(_empty_relation) => {
            // Empty Relation has no children
            None
        }
        LogicalPlan::Limit(_limit) => Some(vec![(indices, false)]),
        LogicalPlan::Statement(_statement) => {
            // TODO: Add support for statement.
            None
        }
        LogicalPlan::Values(_values) => {
            // Values has no children
            None
        }
        LogicalPlan::Explain(Explain {
            plan,
            verbose,
            stringified_plans,
            schema,
            ..
        }) => {
            let indices = require_all_indices(plan.as_ref());
            if let Some(new_plan) = try_optimize_internal(plan, _config, indices)? {
                return Ok(Some(LogicalPlan::Explain(Explain {
                    verbose: *verbose,
                    plan: Arc::new(new_plan),
                    stringified_plans: stringified_plans.to_vec(),
                    schema: schema.clone(),
                    logical_optimization_succeeded: true,
                })));
            } else {
                return Ok(None);
            }
        }
        LogicalPlan::Analyze(analyze) => {
            let input = &analyze.input;
            let indices = require_all_indices(input.as_ref());
            if let Some(new_input) = try_optimize_internal(input, _config, indices)? {
                return Ok(Some(LogicalPlan::Analyze(Analyze {
                    verbose: analyze.verbose,
                    input: Arc::new(new_input),
                    schema: analyze.schema.clone(),
                })));
            } else {
                return Ok(None);
            }
        }
        LogicalPlan::Distinct(_distinct) => {
            // Direct plan to its child.
            Some(vec![(indices, true)])
        }
        LogicalPlan::Prepare(_prepare) => {
            // Direct plan to its child.
            Some(vec![(indices, false)])
        }
        LogicalPlan::Extension(_extension) => {
            // It is not known how to direct requirements to children.
            // Safest behaviour is to stop propagation.
            None
        }
        LogicalPlan::Dml(dml_statement) => {
            // Require all of the fields of the dml input. Otherwise dml schema may not match with input.
            let required_indices = require_all_indices(&dml_statement.input);
            Some(vec![(required_indices, false)])
        }
        LogicalPlan::Ddl(ddl_statement) => {
            // Require all of the fields in the ddl statement input (if any)
            let inputs = ddl_statement.inputs();
            let res = inputs
                .into_iter()
                .map(|input| {
                    let required_indices = require_all_indices(input);
                    (required_indices, false)
                })
                .collect::<Vec<_>>();
            Some(res)
        }
        LogicalPlan::Copy(_copy_to) => {
            // Direct plan to its child.
            Some(vec![(indices, false)])
        }
        LogicalPlan::DescribeTable(_desc_table) => {
            // Describe table has no children.
            None
        }
        // TODO :Add an uneest test
        LogicalPlan::Unnest(unnest) => {
            let referred_indices = get_referred_indices(
                &unnest.input,
                &[Expr::Column(unnest.column.clone())],
            )?;
            let required_indices = merge_vectors(&indices, &referred_indices);
            Some(vec![(required_indices, false)])
        }
    };
    if let Some(child_required_indices) = child_required_indices {
        let new_inputs = izip!(child_required_indices, plan.inputs().into_iter())
            .map(|((required_indices, projection_beneficial), child)| {
                let input = if let Some(new_input) =
                    try_optimize_internal(child, _config, required_indices.clone())?
                {
                    let project_exprs =
                        get_required_exprs(child.schema(), &required_indices);
                    Some(add_projection_on_top_if_helpful(
                        new_input,
                        project_exprs,
                        projection_beneficial,
                    )?)
                } else {
                    let project_exprs =
                        get_required_exprs(child.schema(), &required_indices);
                    Some(add_projection_on_top_if_helpful(
                        child.clone(),
                        project_exprs,
                        projection_beneficial,
                    )?)
                };
                Ok(input)
            })
            .collect::<Result<Vec<Option<_>>>>()?;
        // All of the children are same in this case, no need to change plan
        if new_inputs.iter().all(|child| child.is_none()) {
            Ok(None)
        } else {
            // At least one of the children is changed.
            let new_inputs = izip!(new_inputs, plan.inputs())
                .map(|(new_input, old_child)| new_input.unwrap_or(old_child.clone()))
                .collect::<Vec<_>>();
            let res = plan.with_new_inputs(&new_inputs)?;
            Ok(Some(res))
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::Result;
    #[test]
    fn dummy() -> Result<()> {
        Ok(())
    }
}
