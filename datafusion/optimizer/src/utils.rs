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

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_rewriter::{replace_col, replace_expr_with_null};
use datafusion_expr::{logical_plan::LogicalPlan, Expr, ExprSchemable};
use log::{debug, trace};
use std::sync::Arc;

use crate::simplify_expressions::ExprSimplifier;
/// Re-export of `NamesPreserver` for backwards compatibility,
/// as it was initially placed here and then moved elsewhere.
pub use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::simplify::SimplifyContext;

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

/// Determine whether a predicate can restrict NULLs. e.g.
/// `c0 > 8` return true;
/// `c0 IS NULL` return false.
pub fn is_restrict_null_predicate<'a>(
    input_schema: &DFSchema,
    predicate: Expr,
    cols_of_predicate: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    let execution_props = ExecutionProps::default();
    let replace_columns = cols_of_predicate.into_iter().collect();
    let replaced_predicate = replace_expr_with_null(predicate, &replace_columns)?;
    let coerced_predicate = coerce(replaced_predicate, input_schema)?;

    let info = SimplifyContext::new(&execution_props)
        .with_schema(Arc::new(input_schema.clone()));
    let simplifier = ExprSimplifier::new(info).with_canonicalize(false);
    let expr = simplifier.simplify(coerced_predicate)?;

    let ret = match &expr {
        Expr::Literal(scalar) if scalar.is_null() => true,
        Expr::Literal(ScalarValue::Boolean(Some(b))) => !b,
        _ if matches!(expr.get_type(input_schema)?, DataType::Null) => true,
        _ => false,
    };

    Ok(ret)
}

fn coerce(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let mut expr_rewrite = TypeCoercionRewriter { schema };
    expr.rewrite(&mut expr_rewrite).data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_expr::{binary_expr, case, col, in_list, is_null, lit, Operator};

    #[test]
    fn expr_is_restrict_null_predicate() -> Result<()> {
        let test_cases = vec![
            // a
            (col("a"), true),
            // a IS NULL
            (is_null(col("a")), false),
            // a IS NOT NULL
            (Expr::IsNotNull(Box::new(col("a"))), true),
            // a = NULL
            (
                binary_expr(col("a"), Operator::Eq, Expr::Literal(ScalarValue::Null)),
                true,
            ),
            // a > 8
            (binary_expr(col("a"), Operator::Gt, lit(8i64)), true),
            // a <= 8
            (binary_expr(col("a"), Operator::LtEq, lit(8i32)), true),
            // CASE a WHEN 1 THEN true WHEN 0 THEN false ELSE NULL END
            (
                case(col("a"))
                    .when(lit(1i64), lit(true))
                    .when(lit(0i64), lit(false))
                    .otherwise(lit(ScalarValue::Null))?,
                true,
            ),
            // CASE a WHEN 1 THEN true ELSE false END
            (
                case(col("a"))
                    .when(lit(1i64), lit(true))
                    .otherwise(lit(false))?,
                true,
            ),
            // CASE a WHEN 0 THEN false ELSE true END
            (
                case(col("a"))
                    .when(lit(0i64), lit(false))
                    .otherwise(lit(true))?,
                false,
            ),
            // (CASE a WHEN 0 THEN false ELSE true END) OR false
            (
                binary_expr(
                    case(col("a"))
                        .when(lit(0i64), lit(false))
                        .otherwise(lit(true))?,
                    Operator::Or,
                    lit(false),
                ),
                false,
            ),
            // (CASE a WHEN 0 THEN true ELSE false END) OR false
            (
                binary_expr(
                    case(col("a"))
                        .when(lit(0i64), lit(true))
                        .otherwise(lit(false))?,
                    Operator::Or,
                    lit(false),
                ),
                true,
            ),
            // a IN (1, 2, 3)
            (
                in_list(col("a"), vec![lit(1i64), lit(2i64), lit(3i64)], false),
                true,
            ),
            // a NOT IN (1, 2, 3)
            (
                in_list(col("a"), vec![lit(1i64), lit(2i64), lit(3i64)], true),
                true,
            ),
            // a IN (NULL)
            (
                in_list(col("a"), vec![Expr::Literal(ScalarValue::Null)], false),
                true,
            ),
            // a NOT IN (NULL)
            (
                in_list(col("a"), vec![Expr::Literal(ScalarValue::Null)], true),
                true,
            ),
            // new
            (col("a").gt(col("b")), true),
        ];

        let column_a = Column::from_name("a");
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::UInt64, true),
        ]);
        let df_schema = DFSchema::try_from(schema)?;

        for (predicate, expected) in test_cases {
            let join_cols_of_predicate = std::iter::once(&column_a);
            let actual = is_restrict_null_predicate(
                &df_schema,
                predicate.clone(),
                join_cols_of_predicate,
            )?;
            assert_eq!(actual, expected, "{}", predicate);
        }

        Ok(())
    }
}
