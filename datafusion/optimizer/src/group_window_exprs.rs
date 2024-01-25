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

//! Unwrap-cast binary comparison rule can be used to the binary/inlist comparison expr now, and other type
//! of expr can be added if needed.
//! This rule can reduce adding the `Expr::Cast` the expr instead of adding the `Expr::Cast` to literal expr.
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{
    DataType, TimeUnit, MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::tree_node::{RewriteRecursion, TreeNodeRewriter};
use datafusion_common::{
    internal_err, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{BinaryExpr, Cast, InList, TryCast};
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::utils::{
    compare_sort_expr, group_window_expr_by_sort_keys, merge_schema,
};
use datafusion_expr::{
    binary_expr, in_list, lit, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
    Operator,
};
use std::cmp::Ordering;
use std::sync::Arc;

/// [`crate::unwrap_cast_in_comparison::UnwrapCastInComparison`] attempts to remove casts from
/// comparisons to literals ([`ScalarValue`]s) by applying the casts
/// to the literals if possible. It is inspired by the optimizer rule
/// `UnwrapCastInBinaryComparison` of Spark.
///
/// Removing casts often improves performance because:
/// 1. The cast is done once (to the literal) rather than to every value
/// 2. Can enable other optimizations such as predicate pushdown that
///    don't support casting
///
/// The rule is applied to expressions of the following forms:
///
/// 1. `cast(left_expr as data_type) comparison_op literal_expr`
/// 2. `literal_expr comparison_op cast(left_expr as data_type)`
/// 3. `cast(literal_expr) IN (expr1, expr2, ...)`
/// 4. `literal_expr IN (cast(expr1) , cast(expr2), ...)`
///
/// If the expression matches one of the forms above, the rule will
/// ensure the value of `literal` is in range(min, max) of the
/// expr's data_type, and if the scalar is within range, the literal
/// will be casted to the data type of expr on the other side, and the
/// cast will be removed from the other side.
///
/// # Example
///
/// If the DataType of c1 is INT32. Given the filter
///
/// ```text
/// Filter: cast(c1 as INT64) > INT64(10)`
/// ```
///
/// This rule will remove the cast and rewrite the expression to:
///
/// ```text
/// Filter: c1 > INT32(10)
/// ```
///
#[derive(Default)]
pub struct GroupWindowExprs {}

impl GroupWindowExprs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for GroupWindowExprs {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Window(window) = plan {
            println!("at the start: ");
            println!("plan: {:#?}", plan);
            // for expr in &window.window_expr{
            //     println!("expr: {:?}", expr);
            // }
            // return Ok(None);
            let window_exprs = window.window_expr.to_vec();
            let input = &window.input;

            let mut groups = group_window_expr_by_sort_keys(window_exprs)?;
            // To align with the behavior of PostgreSQL, we want the sort_keys sorted as same rule as PostgreSQL that first
            // we compare the sort key themselves and if one window's sort keys are a prefix of another
            // put the window with more sort keys first. so more deeply sorted plans gets nested further down as children.
            // The sort_by() implementation here is a stable sort.
            // Note that by this rule if there's an empty over, it'll be at the top level
            groups.sort_by(|(key_a, _), (key_b, _)| {
                for ((first, _), (second, _)) in key_a.iter().zip(key_b.iter()) {
                    let key_ordering = compare_sort_expr(first, second, plan.schema());
                    match key_ordering {
                        Ordering::Less => {
                            return Ordering::Less;
                        }
                        Ordering::Greater => {
                            return Ordering::Greater;
                        }
                        Ordering::Equal => {}
                    }
                }
                key_b.len().cmp(&key_a.len())
            });
            let mut plan = input.as_ref().clone();
            for (_, exprs) in groups {
                let window_exprs = exprs.into_iter().collect::<Vec<_>>();
                // Partition and sorting is done at physical level, see the EnforceDistribution
                // and EnforceSorting rules.
                plan = LogicalPlanBuilder::from(plan)
                    .window(window_exprs)?
                    .build()?;
            }
            println!("at the end: ");
            println!("plan: {:#?}", plan);
            return Ok(Some(plan));
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "group_window_exprs"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}
