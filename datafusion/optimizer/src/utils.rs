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

//! Utility functions leveraged by the query optimizer rules

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::{logical_plan::LogicalPlan, Expr};

use log::{debug, trace};

/// Re-export of `NamesPreserver` for backwards compatibility,
/// as it was initially placed here and then moved elsewhere.
pub use datafusion_expr::expr_rewriter::NamePreserver;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
///
/// Returning `Ok(None)` indicates that the plan can't be optimized by the `optimizer`.
#[deprecated(
    since = "40.0.0",
    note = "please use OptimizerRule::apply_order with ApplyOrder::BottomUp instead"
)]
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        if optimizer.supports_rewrite() {
            let new_input = optimizer.rewrite(input.clone(), config)?;
            plan_is_changed = plan_is_changed || new_input.transformed;
            new_inputs.push(new_input.data);
        } else {
            #[allow(deprecated)]
            let new_input = optimizer.try_optimize(input, config)?;
            plan_is_changed = plan_is_changed || new_input.is_some();
            new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
        }
    }
    if plan_is_changed {
        let exprs = plan.expressions();
        plan.with_new_exprs(exprs, new_inputs).map(Some)
    } else {
        Ok(None)
    }
}

/// Returns true if `expr` contains all columns in `schema_cols`
pub(crate) fn has_all_column_refs(expr: &Expr, schema_cols: &HashSet<Column>) -> bool {
    let column_refs = expr.column_refs();
    // note can't use HashSet::intersect because of different types (owned vs References)
    schema_cols
        .iter()
        .filter(|c| column_refs.contains(c))
        .count()
        == column_refs.len()
}

pub(crate) fn collect_subquery_cols(
    exprs: &[Expr],
    subquery_schema: &DFSchema,
) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.column_refs().into_iter() {
            if subquery_schema.has_column(col) {
                using_cols.push(col.clone());
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

pub(crate) fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| Column::new(Some(subquery_alias), &col.name))
        .collect();
    let replace_map: HashMap<&Column, &Column> =
        cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
pub fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}
