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
use arrow::datatypes::SchemaRef;
use datafusion_common::{
    get_required_group_by_exprs_indices, DFField, DFSchemaRef, JoinType,
    OwnedTableReference, Result, ToDFSchema,
};
use datafusion_expr::{
    logical_plan::LogicalPlan, Aggregate, Analyze, Explain, Expr, Partitioning,
    Projection, TableScan,
};
use itertools::izip;
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
        let indices = (0..plan.schema().fields().len()).collect();
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

fn get_referred_indices(input: &LogicalPlan, exprs: &[Expr]) -> Result<Vec<usize>> {
    if exprs.iter().any(|expr| is_subquery(expr)) {
        Ok((0..input.schema().fields().len()).collect())
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

fn get_projected_schema(
    indices: &[usize],
    table_name: &OwnedTableReference,
    schema: &SchemaRef,
) -> Result<DFSchemaRef> {
    // create the projected schema
    let projected_fields: Vec<DFField> = indices
        .iter()
        .map(|i| DFField::from_qualified(table_name.clone(), schema.fields()[*i].clone()))
        .collect();

    projected_fields.to_dfschema_ref()
}

fn try_optimize_internal(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: Vec<usize>,
) -> Result<Option<LogicalPlan>> {
    let child_required_indices: Option<Vec<Vec<usize>>> = match plan {
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, &indices);
            let required_indices = get_referred_indices(&proj.input, &exprs_used)?;
            let projection_input = if let Some(input) =
                try_optimize_internal(&proj.input, _config, required_indices)?
            {
                Arc::new(input)
            } else {
                proj.input.clone()
            };
            let new_proj = Projection::try_new(exprs_used, projection_input)?;
            return Ok(Some(LogicalPlan::Projection(new_proj)));
        }
        LogicalPlan::Aggregate(aggregate) => {
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;
            if let Some(simplest_groupby_indices) = get_required_group_by_exprs_indices(
                aggregate.input.schema(),
                &group_by_expr_existing,
            ) {
                let required_indices = merge_vectors(&simplest_groupby_indices, &indices);
                let group_bys_used =
                    get_at_indices(&aggregate.group_expr, &required_indices);

                let mut all_exprs = group_bys_used.clone();
                all_exprs.extend(aggregate.aggr_expr.clone());
                let necessary_indices =
                    get_referred_indices(&aggregate.input, &all_exprs)?;

                let aggregate_input = if let Some(input) =
                    try_optimize_internal(&aggregate.input, _config, necessary_indices)?
                {
                    Arc::new(input)
                } else {
                    aggregate.input.clone()
                };
                return Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate_input,
                    group_bys_used,
                    aggregate.aggr_expr.clone(),
                )?)));
            }
            None
        }
        LogicalPlan::Sort(sort) => {
            let indices_referred_by_sort = get_referred_indices(&sort.input, &sort.expr)?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_sort);
            Some(vec![required_indices])
        }
        LogicalPlan::Filter(filter) => {
            let indices_referred_by_filter =
                get_referred_indices(&filter.input, &[filter.predicate.clone()])?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_filter);
            Some(vec![required_indices])
        }
        LogicalPlan::Window(window) => {
            let indices_referred_by_window =
                get_referred_indices(&window.input, &window.window_expr)?;
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
            Some(vec![required_indices])
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
            Some(vec![left_indices, right_indices])
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (left_child_indices, right_child_indices) =
                split_join_requirement_indices_to_children(
                    left_len,
                    &indices,
                    &JoinType::Inner,
                );
            Some(vec![left_child_indices, right_child_indices])
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
            Some(vec![required_indices])
        }
        LogicalPlan::Union(union) => {
            // Union directly re-routes requirements from its parents.
            Some(vec![indices; union.inputs.len()])
        }
        LogicalPlan::TableScan(table_scan) => {
            let filter_referred_indices =
                get_referred_indices(plan, &table_scan.filters)?;
            let indices = merge_vectors(&indices, &filter_referred_indices);
            let indices = if indices.is_empty() {
                // Use at least 1 column if not empty
                if table_scan.projected_schema.fields().is_empty() {
                    vec![]
                } else {
                    vec![0]
                }
            } else {
                indices
            };
            let projection_fields = table_scan.projected_schema.fields();
            let schema = table_scan.source.schema();
            let fields_used = indices
                .iter()
                .map(|idx| projection_fields[*idx].clone())
                .collect::<Vec<_>>();
            let mut projection = fields_used
                .iter()
                .map(|field_proj| {
                    schema
                        .fields()
                        .iter()
                        .position(|field_source| field_proj.field() == field_source)
                })
                .collect::<Option<Vec<_>>>();
            // // TODO: Remove this check.
            // if table_scan.projection.is_none() {
            //     projection = None;
            // }
            let projected_schema = if let Some(indices) = &projection {
                get_projected_schema(
                    indices,
                    &table_scan.table_name,
                    &table_scan.source.schema(),
                )?
            } else {
                // Use existing projected schema.
                table_scan.projected_schema.clone()
            };
            return Ok(Some(LogicalPlan::TableScan(TableScan {
                table_name: table_scan.table_name.clone(),
                source: table_scan.source.clone(),
                projection,
                projected_schema,
                filters: table_scan.filters.clone(),
                fetch: table_scan.fetch,
            })));
        }
        // SubqueryAlias alias can route requirement for its parent to its child
        LogicalPlan::SubqueryAlias(sub_query_alias) => {
            // let referred_indices = get_referred_indices(&sub_query_alias.input)
            Some(vec![indices])
            // None
        }
        LogicalPlan::Subquery(sub_query) => {
            // let referred_indices =
            //     get_referred_indices(&sub_query.subquery, &sub_query.outer_ref_columns)?;
            // let required_indices = merge_vectors(&indices, &referred_indices);
            // Some(vec![required_indices])
            None
        }
        LogicalPlan::EmptyRelation(_empty_relation) => {
            // Empty Relation has no children
            None
        }
        LogicalPlan::Limit(_limit) => Some(vec![indices]),
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
            let indices = (0..plan.schema().fields().len()).collect();
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
            let indices = (0..input.schema().fields().len()).collect();
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
            Some(vec![indices])
        }
        LogicalPlan::Prepare(_prepare) => {
            // Direct plan to its child.
            Some(vec![indices])
        }
        LogicalPlan::Extension(_extension) => {
            // It is not known how to direct requirements to children.
            // Safest behaviour is to stop propagation.
            None
        }
        LogicalPlan::Dml(_dml_statement) => {
            // Direct plan to its child.
            Some(vec![indices])
        }
        LogicalPlan::Ddl(_ddl_statement) => {
            // Ddl has no children
            None
        }
        LogicalPlan::Copy(_copy_to) => {
            // Direct plan to its child.
            Some(vec![indices])
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
            Some(vec![required_indices])
        }
    };
    if let Some(child_required_indices) = child_required_indices {
        let new_inputs = izip!(child_required_indices, plan.inputs())
            .map(|(required_indices, child)| {
                Ok(
                    if let Some(child) =
                        try_optimize_internal(child, _config, required_indices)?
                    {
                        child
                    } else {
                        // If child is not changed use existing child
                        child.clone()
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let res = plan.with_new_inputs(&new_inputs)?;
        Ok(Some(res))
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
