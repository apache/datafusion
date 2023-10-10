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
use datafusion_common::{get_target_functional_dependencies, Result};
use datafusion_expr::{logical_plan::LogicalPlan, Aggregate, Expr};

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

fn try_optimize_internal(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: Vec<usize>,
) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, indices);
            let new_indices = get_required_indices(&proj.input, &exprs_used)?;

            if let Some(new_input) =
                try_optimize_internal(&proj.input, _config, new_indices)?
            {
                return Ok(Some(plan.with_new_inputs(&[new_input])?));
            }
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
            if (used_target_indices == existing_target_indices)
                && used_target_indices.is_some()
            {
                // TODO: Continue to recursion for double aggregates
                return Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate.input.clone(),
                    group_bys_used,
                    aggregate.aggr_expr.clone(),
                )?)));
            }
        }
        LogicalPlan::Sort(sort) => {
            if let Some(new_input) = try_optimize_internal(&sort.input, _config, indices)?
            {
                let res = plan.with_new_inputs(&[new_input])?;
                return Ok(Some(res));
            }
        }
        _ => {}
    }
    Ok(Some(plan.clone()))
}

#[cfg(test)]
mod tests {
    use datafusion_common::Result;
    #[test]
    fn dummy() -> Result<()> {
        Ok(())
    }
}
