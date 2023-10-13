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
use std::sync::Arc;
use crate::optimizer::ApplyOrder;
use datafusion_common::{get_target_functional_dependencies, Result};
use datafusion_expr::{logical_plan::LogicalPlan, Aggregate, Expr};
use itertools::izip;

use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that eliminate the scalar value (true/false) filter with an [LogicalPlan::EmptyRelation]
#[derive(Default)]
pub struct SimplifyAggregate {}

impl SimplifyAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SimplifyAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let indices = (0..plan.schema().fields().len()).collect();
        try_optimize_internal(plan, config, indices)
    }

    fn name(&self) -> &str {
        "SimplifyAggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

fn get_required_indices(input: &LogicalPlan, exprs: &[Expr]) -> Result<Vec<usize>> {
    let mut new_indices = vec![];
    for expr in exprs {
        let cols = expr.to_columns()?;
        for col in cols {
            let idx = input.schema().index_of_column(&col)?;
            if !new_indices.contains(&idx) {
                new_indices.push(idx);
            }
        }
    }
    Ok(new_indices)
}

fn get_at_indices(exprs: &[Expr], indices: Vec<usize>) -> Vec<Expr> {
    indices
        .into_iter()
        .filter_map(|idx| {
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

fn try_optimize_internal(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: Vec<usize>,
) -> Result<Option<LogicalPlan>> {
    let child_required_indices: Option<Vec<Vec<usize>>> = match plan {
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, indices);
            let new_indices = get_required_indices(&proj.input, &exprs_used)?;
            Some(vec![new_indices])
        }
        LogicalPlan::Aggregate(aggregate) => {
            let group_bys_used = get_at_indices(&aggregate.group_expr, indices);
            let group_by_expr_names_used = group_bys_used
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;
            let used_target_indices = get_target_functional_dependencies(
                aggregate.input.schema(),
                &group_by_expr_names_used,
            );
            let existing_target_indices = get_target_functional_dependencies(
                aggregate.input.schema(),
                &group_by_expr_existing,
            );
            // Can simplify aggregate group by
            if (used_target_indices == existing_target_indices)
                && used_target_indices.is_some()
            {
                let new_indices = (0..(group_by_expr_names_used.len()+aggregate.aggr_expr.len())).collect::<Vec<_>>();
                let aggregate_input = if let Some(input ) = try_optimize_internal(&aggregate.input, _config, new_indices)?{
                    Arc::new(input)
                } else{
                    aggregate.input.clone()
                };
                // TODO: Continue to recursion for double aggregates
                return Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate_input,
                    group_bys_used,
                    aggregate.aggr_expr.clone(),
                )?)));
            }
            None
        }
        LogicalPlan::Sort(sort) => {
            let indices_referred_by_sort = get_required_indices(&sort.input, &sort.expr)?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_sort);
            Some(vec![required_indices])
        }
        LogicalPlan::Filter(filter) => {
            let indices_referred_by_filter =
                get_required_indices(&filter.input, &[filter.predicate.clone()])?;
            let required_indices = merge_vectors(&indices, &indices_referred_by_filter);
            Some(vec![required_indices])
        }
        _ => None,
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
