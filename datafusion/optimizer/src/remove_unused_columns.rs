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

//! Optimizer rule to prune unnecessary Columns from the intermediate schemas inside the [LogicalPlan].
//! This rule
//! - Removes unnecessary columns that are not showed at the output, and that are not used during computation.
//! - Adds projection to decrease table column size before operators that benefits from less memory at its input.
//! - Removes unnecessary [LogicalPlan::Projection] from the [LogicalPlan].
use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::{Transformed, TreeNode};
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
        // All of the fields at the output are necessary.
        let indices = require_all_indices(plan);
        let unnecessary_columns_removed =
            remove_unnecessary_columns(plan, config, indices)?;
        let projections_eliminated = unnecessary_columns_removed
            .map(|plan| plan.transform_up(&eliminate_projection))
            .transpose()?;
        Ok(projections_eliminated)
    }

    fn name(&self) -> &str {
        "RemoveUnusedColumns"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

/// Check whether expression is (or contains) a subquery
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

/// Get Column expressions of the input_schema at indices.
fn get_required_exprs(input_schema: &Arc<DFSchema>, indices: &[usize]) -> Vec<Expr> {
    let fields = input_schema.fields();
    indices
        .iter()
        .map(|&idx| Expr::Column(fields[idx].qualified_column()))
        .collect()
}

/// Get indices of the column necessary for the referred expressions among input LogicalPlan.
fn get_referred_indices(input: &LogicalPlan, exprs: &[Expr]) -> Result<Vec<usize>> {
    // If any of the expressions is sub-query require all of the fields of the input.
    // Because currently we cannot calculate definitely which fields sub-query use
    if exprs.iter().any(is_subquery) {
        Ok(require_all_indices(input))
    } else {
        let mut new_indices = vec![];
        for expr in exprs {
            let cols = expr.to_columns()?;
            for col in cols {
                if input.schema().has_column(&col) {
                    let idx = input.schema().index_of_column(&col)?;
                    new_indices.push(idx);
                }
            }
        }
        new_indices.sort();
        new_indices.dedup();
        Ok(new_indices)
    }
}

/// Get expressions at the indices among `exprs`.
fn get_at_indices(exprs: &[Expr], indices: &[usize]) -> Vec<Expr> {
    indices
        .iter()
        // Indices may point to further places than `exprs` len.
        .filter_map(|&idx| exprs.get(idx).cloned())
        .collect()
}

/// Merge two vectors,
/// Result is ordered and doesn't contain duplicate entries.
/// As an example merge of [3, 2, 4] and [3, 6, 1] will produce [1, 2, 3, 6]
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

/// Find indices of columns referred by `on` and `filter` expressions
/// in the child schema.
fn join_child_requirement(
    child: &LogicalPlan,
    on: &[Expr],
    filter: &[Expr],
) -> Result<Vec<usize>> {
    let on_indices = get_referred_indices(child, on)?;
    let filter_indices = get_referred_indices(child, filter)?;
    Ok(merge_vectors(&on_indices, &filter_indices))
}

/// Calculate children requirement indices for the join for the given requirement `indices` for the join.
/// Returns required indices for left and right children.
fn split_join_requirement_indices_to_children(
    left_len: usize,
    indices: &[usize],
    join_type: &JoinType,
) -> (Vec<usize>, Vec<usize>) {
    match join_type {
        // In these cases requirements split to left and right child.
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {}
        // All requirements can be re-routed to left child directly.
        JoinType::LeftAnti | JoinType::LeftSemi => return (indices.to_vec(), vec![]),
        // All requirements can be re-routed to right side directly.
        JoinType::RightSemi | JoinType::RightAnti => return (vec![], indices.to_vec()),
    }
    // Required indices that are smaller than `left_len` belongs to left side.
    // Other belong to the right side.
    let (left_requirements_from_parent, right_requirements_from_parent): (
        Vec<_>,
        Vec<_>,
    ) = indices
        .iter()
        .map(|&idx| {
            if idx < left_len {
                (Some(idx), None)
            } else {
                // Decrease right side index by `left_len` so that they point to valid positions in the right child.
                (None, Some(idx - left_len))
            }
        })
        .unzip();

    // Filter out the `None` values in both collections.
    let left_requirements_from_parent: Vec<_> = left_requirements_from_parent
        .into_iter()
        .flatten()
        .collect();
    let right_requirements_from_parent: Vec<_> = right_requirements_from_parent
        .into_iter()
        .flatten()
        .collect();
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

/// Adds projection to the top of the plan. If projection decreases the table column size
/// and beneficial for the parent operator.
/// Returns new LogicalPlan and flag
/// flag `true` means that projection is added, `false` means that no projection is added.
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
    projection_beneficial: bool,
) -> Result<(LogicalPlan, bool)> {
    // If not beneficial return immediately without projection
    if !projection_beneficial {
        return Ok((plan, false));
    }
    // Make sure projection decreases table column size, otherwise it is unnecessary.
    let projection_beneficial =
        projection_beneficial && project_exprs.len() < plan.schema().fields().len();
    if projection_beneficial {
        Projection::try_new(project_exprs, Arc::new(plan))
            .map(LogicalPlan::Projection)
            .map(|elem| (elem, true))
    } else {
        Ok((plan, false))
    }
}

/// Requires all indices for the plan
fn require_all_indices(plan: &LogicalPlan) -> Vec<usize> {
    (0..plan.schema().fields().len()).collect()
}

/// This function to prunes `plan` according to requirement `indices` given.
fn remove_unnecessary_columns(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: Vec<usize>,
) -> Result<Option<LogicalPlan>> {
    // `child_required_indices` stores indices of the columns required for each child
    // and a flag indicating whether putting a projection above children is beneficial for the parent.
    // As an example filter benefits from small tables. Hence for filter child this flag would be `true`.
    let child_required_indices: Option<Vec<(Vec<usize>, bool)>> = match plan {
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, &indices);
            let required_indices = get_referred_indices(&proj.input, &exprs_used)?;
            if let Some(input) =
                remove_unnecessary_columns(&proj.input, _config, required_indices)?
            {
                let new_proj = Projection::try_new(exprs_used, Arc::new(input))?;
                let new_proj = LogicalPlan::Projection(new_proj);
                return Ok(Some(new_proj));
            } else if exprs_used.len() < proj.expr.len() {
                // Projection expression used is different than the existing projection
                // In this case, even if child doesn't change we should update projection to use less columns.
                let new_proj = Projection::try_new(exprs_used, proj.input.clone())?;
                let new_proj = LogicalPlan::Projection(new_proj);
                return Ok(Some(new_proj));
            } else {
                // Projection doesn't change.
                return Ok(None);
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;
            // Use the absolutely necessary group expressions according to functional dependencies and
            // Expressions used after aggregation.
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
            // Only use absolutely necessary aggregate expressions required by parent.
            let mut new_aggr_expr = vec![];
            for idx in indices {
                let group_expr_len = aggregate.group_expr_len()?;
                if let Some(aggr_expr_idx) = idx.checked_sub(group_expr_len) {
                    new_aggr_expr.push(aggregate.aggr_expr[aggr_expr_idx].clone())
                }
            }
            let mut all_exprs = group_bys_used.clone();
            all_exprs.extend(new_aggr_expr.clone());
            let necessary_indices = get_referred_indices(&aggregate.input, &all_exprs)?;

            let aggregate_input = if let Some(input) =
                remove_unnecessary_columns(&aggregate.input, _config, necessary_indices)?
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
            // Simplify input of the aggregation by adding a projection so that its input only contains
            // absolutely necessary columns for the aggregate expressions.
            let (aggregate_input, _is_added) =
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
            // Re-route required indices from the parent + column indices referred by sort expression
            // to the sort child. Sort benefits from small column numbers. Hence projection_beneficial flag is `true`.
            let indices_referred_by_sort = get_referred_indices(&sort.input, &sort.expr)?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_sort);
            Some(vec![(required_indices, true)])
        }
        LogicalPlan::Filter(filter) => {
            // Re-route required indices from the parent + column indices referred by filter expression
            // to the filter child. Filter benefits from small column numbers. Hence projection_beneficial flag is `true`.
            let indices_referred_by_filter =
                get_referred_indices(&filter.input, &[filter.predicate.clone()])?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_filter);
            Some(vec![(required_indices, true)])
        }
        LogicalPlan::Window(window) => {
            let n_input_fields = window.input.schema().fields().len();
            // Only use window expressions that are absolutely necessary by parent requirements.
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
            // Find column indices window expressions refer at the window input.
            let indices_referred_by_window =
                get_referred_indices(&window.input, &new_window_expr)?;
            let n_input_fields = window.input.schema().fields().len();
            // Find necessary child indices according to parent requirements.
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
            // All of the required column indices at the input of the window by parent, and window expression requirements.
            let required_indices =
                merge_vectors(&window_child_indices, &indices_referred_by_window);
            let window_child = if let Some(new_window_child) = remove_unnecessary_columns(
                &window.input,
                _config,
                required_indices.clone(),
            )? {
                new_window_child
            } else {
                window.input.as_ref().clone()
            };
            // When no window expression is necessary, just use window input. (Remove window operator)
            if new_window_expr.is_empty() {
                return Ok(Some(window_child));
            } else {
                // Calculated required expression according to old child, otherwise indices are not valid.
                let required_exprs =
                    get_required_exprs(window.input.schema(), &required_indices);
                let (window_child, _is_added) =
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
            // Join benefits from small columns numbers at its input (decreases memory usage)
            // Hence each child benefits from projection.
            Some(vec![(left_indices, true), (right_indices, true)])
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (left_child_indices, right_child_indices) =
                split_join_requirement_indices_to_children(
                    left_len,
                    &indices,
                    &JoinType::Inner,
                );
            // Join benefits from small columns numbers at its input (decreases memory usage)
            // Hence each child benefits from projection.
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
            // Also if benefits from projection (decreases column number)
            Some(vec![(indices, true); union.inputs.len()])
        }
        LogicalPlan::TableScan(table_scan) => {
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
            None
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
            if let Some(new_plan) = remove_unnecessary_columns(plan, _config, indices)? {
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
            if let Some(new_input) = remove_unnecessary_columns(input, _config, indices)?
            {
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
        LogicalPlan::Copy(copy_to) => {
            // Require all of the fields of the copy input.
            let required_indices = require_all_indices(&copy_to.input);
            Some(vec![(required_indices, false)])
        }
        LogicalPlan::DescribeTable(_desc_table) => {
            // Describe table has no children.
            None
        }
        // TODO :Add support for unnest
        LogicalPlan::Unnest(_unnest) => None,
    };
    if let Some(child_required_indices) = child_required_indices {
        let new_inputs = izip!(child_required_indices, plan.inputs().into_iter())
            .map(|((required_indices, projection_beneficial), child)| {
                let input = if let Some(new_input) =
                    remove_unnecessary_columns(child, _config, required_indices.clone())?
                {
                    let project_exprs =
                        get_required_exprs(child.schema(), &required_indices);
                    let (new_input, _is_added) = add_projection_on_top_if_helpful(
                        new_input,
                        project_exprs,
                        projection_beneficial,
                    )?;
                    Some(new_input)
                } else {
                    let project_exprs =
                        get_required_exprs(child.schema(), &required_indices);
                    let child = child.clone();
                    let (child, is_added) = add_projection_on_top_if_helpful(
                        child,
                        project_exprs,
                        projection_beneficial,
                    )?;
                    is_added.then_some(child)
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

/// This function removes unnecessary [LogicalPlan::Projection] from the [LogicalPlan].
fn eliminate_projection(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(ref projection) => {
            let child_plan = projection.input.as_ref();
            if plan.schema() == child_plan.schema() {
                // If child schema and schema of the projection is same
                // Projection can be removed.
                Ok(Transformed::Yes(child_plan.clone()))
            } else {
                Ok(Transformed::No(plan))
            }
        }
        _ => Ok(Transformed::No(plan)),
    }
}
