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

mod null_restriction;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
use std::sync::Mutex;

use crate::analyzer::type_coercion::TypeCoercionRewriter;

/// Null restriction evaluation mode for optimizer tests.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum NullRestrictionEvalMode {
    Auto,
    AuthoritativeOnly,
}

#[cfg(test)]
static NULL_RESTRICTION_EVAL_MODE: Mutex<NullRestrictionEvalMode> =
    Mutex::new(NullRestrictionEvalMode::Auto);

#[cfg(test)]
pub(crate) fn set_null_restriction_eval_mode_for_test(mode: NullRestrictionEvalMode) {
    *NULL_RESTRICTION_EVAL_MODE.lock().unwrap() = mode;
}

fn null_restriction_eval_mode() -> NullRestrictionEvalMode {
    #[cfg(test)]
    {
        *NULL_RESTRICTION_EVAL_MODE.lock().unwrap()
    }
    #[cfg(not(test))]
    {
        NullRestrictionEvalMode::Auto
    }
}
use arrow::array::{Array, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::{ColumnarValue, Expr, logical_plan::LogicalPlan};
use datafusion_physical_expr::create_physical_expr;
use log::{debug, trace};

/// Re-export of `NamesPreserver` for backwards compatibility,
/// as it was initially placed here and then moved elsewhere.
pub use datafusion_expr::expr_rewriter::NamePreserver;

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
    predicate: Expr,
    join_cols_of_predicate: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    // Collect join columns so they can be used in both the fast-path check and the
    // fallback evaluation path below.
    let join_cols: HashSet<&Column> = join_cols_of_predicate.into_iter().collect();

    // Fast path: if the predicate references columns outside the join key set,
    // `evaluate_expr_with_null_column` would fail because the null schema only
    // contains a placeholder for the join key columns. Callers treat such errors as
    // non-restricting (false) via `matches!(_, Ok(true))`, so we return false early
    // and avoid the expensive physical-expression compilation pipeline entirely.
    if !null_restriction::predicate_uses_only_columns(&predicate, &join_cols) {
        return Ok(false);
    }

    let mode = null_restriction_eval_mode();

    match mode {
        NullRestrictionEvalMode::AuthoritativeOnly => {
            authoritative_restrict_null_predicate(predicate, join_cols)
        }
        NullRestrictionEvalMode::Auto => {
            if let Some(is_restricting) =
                null_restriction::syntactic_restrict_null_predicate(
                    &predicate, &join_cols,
                )
            {
                #[cfg(debug_assertions)]
                {
                    let authoritative = authoritative_restrict_null_predicate(
                        predicate.clone(),
                        join_cols.iter().copied(),
                    )?;
                    debug_assert_eq!(
                        is_restricting, authoritative,
                        "syntactic fast path disagrees with authoritative null-restriction evaluation for predicate: {predicate}"
                    );
                }
                Ok(is_restricting)
            } else {
                authoritative_restrict_null_predicate(predicate, join_cols)
            }
        }
    }
}

/// Determines if an expression will always evaluate to null.
/// `c0 + 8` return true
/// `c0 IS NULL` return false
/// `CASE WHEN c0 > 1 then 0 else 1` return false
pub fn evaluates_to_null<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    Ok(
        match evaluate_expr_with_null_column(predicate, null_columns)? {
            ColumnarValue::Array(_) => false,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        },
    )
}

fn evaluate_expr_with_null_column<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<ColumnarValue> {
    static DUMMY_COL_NAME: &str = "?";
    let schema = Arc::new(Schema::new(vec![Field::new(
        DUMMY_COL_NAME,
        DataType::Null,
        true,
    )]));
    let input_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let column = new_null_array(&DataType::Null, 1);
    let input_batch = RecordBatch::try_new(schema, vec![column])?;
    let execution_props = ExecutionProps::default();
    let null_column = Column::from_name(DUMMY_COL_NAME);

    let join_cols_to_replace = null_columns
        .into_iter()
        .map(|column| (column, &null_column))
        .collect::<HashMap<_, _>>();

    let replaced_predicate = replace_col(predicate, &join_cols_to_replace)?;
    let coerced_predicate = coerce(replaced_predicate, &input_schema)?;
    create_physical_expr(&coerced_predicate, &input_schema, &execution_props)?
        .evaluate(&input_batch)
}

fn authoritative_restrict_null_predicate<'a>(
    predicate: Expr,
    join_cols_of_predicate: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    Ok(
        match evaluate_expr_with_null_column(predicate, join_cols_of_predicate)? {
            ColumnarValue::Array(array) => {
                if array.len() == 1 {
                    let boolean_array = as_boolean_array(&array)?;
                    boolean_array.is_null(0) || !boolean_array.value(0)
                } else {
                    false
                }
            }
            ColumnarValue::Scalar(scalar) => matches!(
                scalar,
                ScalarValue::Boolean(None) | ScalarValue::Boolean(Some(false))
            ),
        },
    )
}

fn coerce(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let mut expr_rewrite = TypeCoercionRewriter { schema };
    expr.rewrite(&mut expr_rewrite).data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{
        Operator, binary_expr, case, col, in_list, is_null, lit, when,
    };

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
                binary_expr(
                    col("a"),
                    Operator::Eq,
                    Expr::Literal(ScalarValue::Null, None),
                ),
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
            // CASE 1 WHEN 1 THEN true ELSE false END
            (
                case(lit(1i64))
                    .when(lit(1i64), lit(true))
                    .otherwise(lit(false))?,
                false,
            ),
            // CASE 1 WHEN 1 THEN NULL ELSE false END
            (
                case(lit(1i64))
                    .when(lit(1i64), lit(ScalarValue::Null))
                    .otherwise(lit(false))?,
                true,
            ),
            // CASE true WHEN true THEN false ELSE true END
            (
                case(lit(true))
                    .when(lit(true), lit(false))
                    .otherwise(lit(true))?,
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
                in_list(
                    col("a"),
                    vec![Expr::Literal(ScalarValue::Null, None)],
                    false,
                ),
                true,
            ),
            // a NOT IN (NULL)
            (
                in_list(col("a"), vec![Expr::Literal(ScalarValue::Null, None)], true),
                true,
            ),
            // CASE WHEN a IS NOT NULL THEN a ELSE b END > 2
            (
                binary_expr(
                    when(Expr::IsNotNull(Box::new(col("a"))), col("a"))
                        .otherwise(col("b"))?,
                    Operator::Gt,
                    lit(2i64),
                ),
                true,
            ),
        ];

        for (predicate, expected) in test_cases {
            let join_cols_of_predicate = predicate.column_refs();
            let actual = is_restrict_null_predicate(
                predicate.clone(),
                join_cols_of_predicate.iter().copied(),
            )?;
            assert_eq!(actual, expected, "{predicate}");
        }

        // Keep coverage for the fast path that rejects predicates referencing
        // columns outside the provided join key set.
        let predicate = binary_expr(col("a"), Operator::Gt, col("b"));
        let column_a = Column::from_name("a");
        let actual =
            is_restrict_null_predicate(predicate.clone(), std::iter::once(&column_a))?;
        assert!(!actual, "{predicate}");

        Ok(())
    }

    #[test]
    fn syntactic_fast_path_matches_authoritative_evaluator() -> Result<()> {
        let test_cases = vec![
            is_null(col("a")),
            Expr::IsNotNull(Box::new(col("a"))),
            binary_expr(col("a"), Operator::Gt, lit(8i64)),
            binary_expr(col("a"), Operator::Eq, lit(ScalarValue::Null)),
            binary_expr(col("a"), Operator::And, lit(true)),
            binary_expr(col("a"), Operator::Or, lit(false)),
            Expr::Not(Box::new(col("a").is_true())),
            col("a").is_true(),
            col("a").is_false(),
            col("a").is_unknown(),
            col("a").is_not_true(),
            col("a").is_not_false(),
            col("a").is_not_unknown(),
            col("a").between(lit(1i64), lit(10i64)),
            binary_expr(
                when(Expr::IsNotNull(Box::new(col("a"))), col("a"))
                    .otherwise(col("b"))?,
                Operator::Gt,
                lit(2i64),
            ),
            case(col("a"))
                .when(lit(1i64), lit(true))
                .otherwise(lit(false))?,
            case(col("a"))
                .when(lit(0i64), lit(false))
                .otherwise(lit(true))?,
            binary_expr(
                case(col("a"))
                    .when(lit(0i64), lit(true))
                    .otherwise(lit(false))?,
                Operator::Or,
                lit(false),
            ),
            binary_expr(
                case(lit(1i64))
                    .when(lit(1i64), lit(ScalarValue::Null))
                    .otherwise(lit(false))?,
                Operator::IsNotDistinctFrom,
                lit(true),
            ),
        ];

        for predicate in test_cases {
            let join_cols = predicate.column_refs();
            if let Some(syntactic) = null_restriction::syntactic_restrict_null_predicate(
                &predicate, &join_cols,
            ) {
                let authoritative = authoritative_restrict_null_predicate(
                    predicate.clone(),
                    join_cols.iter().copied(),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "authoritative evaluator failed for predicate `{predicate}`: {error}"
                    )
                });
                assert_eq!(
                    syntactic, authoritative,
                    "syntactic fast path disagrees with authoritative evaluator for predicate: {predicate}",
                );
            }
        }

        Ok(())
    }

    #[test]
    fn null_restriction_eval_mode_auto_vs_authoritative_only() -> Result<()> {
        let predicate = binary_expr(col("a"), Operator::Gt, lit(8i64));
        let join_cols_of_predicate = predicate.column_refs();

        set_null_restriction_eval_mode_for_test(NullRestrictionEvalMode::Auto);
        let auto_result = is_restrict_null_predicate(
            predicate.clone(),
            join_cols_of_predicate.iter().copied(),
        )?;

        set_null_restriction_eval_mode_for_test(
            NullRestrictionEvalMode::AuthoritativeOnly,
        );
        let authoritative_result = is_restrict_null_predicate(
            predicate.clone(),
            join_cols_of_predicate.iter().copied(),
        )?;

        assert_eq!(auto_result, authoritative_result);

        Ok(())
    }
}
