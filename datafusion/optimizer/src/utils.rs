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
use std::sync::Arc;

use crate::analyzer::type_coercion::TypeCoercionRewriter;
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
    if !predicate_uses_only_columns(&predicate, &join_cols) {
        return Ok(false);
    }

    if let Some(is_restricting) =
        syntactic_restrict_null_predicate(&predicate, &join_cols)
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
        return Ok(is_restricting);
    }

    // If result is single `true`, return false;
    // If result is single `NULL` or `false`, return true;
    authoritative_restrict_null_predicate(predicate, join_cols)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NullSubstitutionValue {
    Null,
    NonNull,
    Boolean(bool),
}

fn syntactic_restrict_null_predicate(
    predicate: &Expr,
    join_cols: &HashSet<&Column>,
) -> Option<bool> {
    match syntactic_null_substitution_value(predicate, join_cols) {
        Some(NullSubstitutionValue::Boolean(true)) => Some(false),
        Some(NullSubstitutionValue::Boolean(false) | NullSubstitutionValue::Null) => {
            Some(true)
        }
        Some(NullSubstitutionValue::NonNull) | None => None,
    }
}

fn predicate_uses_only_columns(
    predicate: &Expr,
    allowed_columns: &HashSet<&Column>,
) -> bool {
    predicate
        .column_refs()
        .iter()
        .all(|column| allowed_columns.contains(*column))
}

fn contains_null(
    values: impl IntoIterator<Item = Option<NullSubstitutionValue>>,
) -> bool {
    values
        .into_iter()
        .any(|value| matches!(value, Some(NullSubstitutionValue::Null)))
}

fn null_check_value(
    value: Option<NullSubstitutionValue>,
    when_non_null: bool,
) -> Option<NullSubstitutionValue> {
    match value {
        Some(NullSubstitutionValue::Null) => Some(NullSubstitutionValue::Boolean(false)),
        Some(NullSubstitutionValue::NonNull | NullSubstitutionValue::Boolean(_)) => {
            Some(NullSubstitutionValue::Boolean(when_non_null))
        }
        None => None,
    }
}

fn syntactic_null_substitution_value(
    expr: &Expr,
    join_cols: &HashSet<&Column>,
) -> Option<NullSubstitutionValue> {
    match expr {
        Expr::Alias(alias) => {
            syntactic_null_substitution_value(alias.expr.as_ref(), join_cols)
        }
        Expr::Column(column) => {
            if join_cols.contains(column) {
                Some(NullSubstitutionValue::Null)
            } else {
                None
            }
        }
        Expr::Literal(value, _) => Some(scalar_to_null_substitution_value(value)),
        Expr::BinaryExpr(binary_expr) => syntactic_binary_value(binary_expr, join_cols),
        Expr::Not(expr) => {
            sql_not(syntactic_null_substitution_value(expr.as_ref(), join_cols))
        }
        Expr::IsNull(expr) => sql_not(null_check_value(
            syntactic_null_substitution_value(expr.as_ref(), join_cols),
            true,
        )),
        Expr::IsNotNull(expr) => null_check_value(
            syntactic_null_substitution_value(expr.as_ref(), join_cols),
            true,
        ),
        Expr::Between(between) => {
            if contains_null([
                syntactic_null_substitution_value(between.expr.as_ref(), join_cols),
                syntactic_null_substitution_value(between.low.as_ref(), join_cols),
                syntactic_null_substitution_value(between.high.as_ref(), join_cols),
            ]) {
                Some(NullSubstitutionValue::Null)
            } else {
                None
            }
        }
        Expr::Cast(cast) => strict_null_passthrough(cast.expr.as_ref(), join_cols),
        Expr::TryCast(try_cast) => {
            strict_null_passthrough(try_cast.expr.as_ref(), join_cols)
        }
        Expr::Negative(expr) => strict_null_passthrough(expr.as_ref(), join_cols),
        Expr::Like(like) | Expr::SimilarTo(like) => {
            if contains_null([
                syntactic_null_substitution_value(like.expr.as_ref(), join_cols),
                syntactic_null_substitution_value(like.pattern.as_ref(), join_cols),
            ]) {
                Some(NullSubstitutionValue::Null)
            } else {
                None
            }
        }
        Expr::Exists { .. }
        | Expr::InList(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _)
        | Expr::Unnest(_)
        | Expr::GroupingSet(_)
        | Expr::WindowFunction(_)
        | Expr::ScalarFunction(_)
        | Expr::Case(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_) => None,
        Expr::AggregateFunction(_) => None,
        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        Expr::Wildcard { .. } => None,
    }
}

fn scalar_to_null_substitution_value(value: &ScalarValue) -> NullSubstitutionValue {
    if value.is_null() {
        NullSubstitutionValue::Null
    } else if let ScalarValue::Boolean(Some(value)) = value {
        NullSubstitutionValue::Boolean(*value)
    } else {
        NullSubstitutionValue::NonNull
    }
}

fn strict_null_passthrough(
    expr: &Expr,
    join_cols: &HashSet<&Column>,
) -> Option<NullSubstitutionValue> {
    if matches!(
        syntactic_null_substitution_value(expr, join_cols),
        Some(NullSubstitutionValue::Null)
    ) {
        Some(NullSubstitutionValue::Null)
    } else {
        None
    }
}

fn sql_not(value: Option<NullSubstitutionValue>) -> Option<NullSubstitutionValue> {
    match value {
        Some(NullSubstitutionValue::Boolean(value)) => {
            Some(NullSubstitutionValue::Boolean(!value))
        }
        Some(NullSubstitutionValue::Null) => Some(NullSubstitutionValue::Null),
        Some(NullSubstitutionValue::NonNull) | None => None,
    }
}

fn sql_and(
    left: Option<NullSubstitutionValue>,
    right: Option<NullSubstitutionValue>,
) -> Option<NullSubstitutionValue> {
    if matches!(left, Some(NullSubstitutionValue::Boolean(false)))
        || matches!(right, Some(NullSubstitutionValue::Boolean(false)))
    {
        return Some(NullSubstitutionValue::Boolean(false));
    }

    match (left, right) {
        (Some(NullSubstitutionValue::Boolean(true)), value)
        | (value, Some(NullSubstitutionValue::Boolean(true))) => value,
        (Some(NullSubstitutionValue::Null), Some(NullSubstitutionValue::Null)) => {
            Some(NullSubstitutionValue::Null)
        }
        (Some(NullSubstitutionValue::NonNull), _)
        | (_, Some(NullSubstitutionValue::NonNull))
        | (None, _)
        | (_, None) => None,
        (left, right) => {
            debug_assert_eq!(left, right);
            left
        }
    }
}

fn sql_or(
    left: Option<NullSubstitutionValue>,
    right: Option<NullSubstitutionValue>,
) -> Option<NullSubstitutionValue> {
    if matches!(left, Some(NullSubstitutionValue::Boolean(true)))
        || matches!(right, Some(NullSubstitutionValue::Boolean(true)))
    {
        return Some(NullSubstitutionValue::Boolean(true));
    }

    match (left, right) {
        (Some(NullSubstitutionValue::Boolean(false)), value)
        | (value, Some(NullSubstitutionValue::Boolean(false))) => value,
        (Some(NullSubstitutionValue::Null), Some(NullSubstitutionValue::Null)) => {
            Some(NullSubstitutionValue::Null)
        }
        (Some(NullSubstitutionValue::NonNull), _)
        | (_, Some(NullSubstitutionValue::NonNull))
        | (None, _)
        | (_, None) => None,
        (left, right) => {
            debug_assert_eq!(left, right);
            left
        }
    }
}

fn syntactic_binary_value(
    binary_expr: &datafusion_expr::BinaryExpr,
    join_cols: &HashSet<&Column>,
) -> Option<NullSubstitutionValue> {
    let left = syntactic_null_substitution_value(binary_expr.left.as_ref(), join_cols);
    let right = syntactic_null_substitution_value(binary_expr.right.as_ref(), join_cols);

    match binary_expr.op {
        datafusion_expr::Operator::And => sql_and(left, right),
        datafusion_expr::Operator::Or => sql_or(left, right),
        datafusion_expr::Operator::Eq
        | datafusion_expr::Operator::NotEq
        | datafusion_expr::Operator::Lt
        | datafusion_expr::Operator::LtEq
        | datafusion_expr::Operator::Gt
        | datafusion_expr::Operator::GtEq
        | datafusion_expr::Operator::Plus
        | datafusion_expr::Operator::Minus
        | datafusion_expr::Operator::Multiply
        | datafusion_expr::Operator::Divide
        | datafusion_expr::Operator::Modulo
        | datafusion_expr::Operator::RegexMatch
        | datafusion_expr::Operator::RegexIMatch
        | datafusion_expr::Operator::RegexNotMatch
        | datafusion_expr::Operator::RegexNotIMatch
        | datafusion_expr::Operator::LikeMatch
        | datafusion_expr::Operator::ILikeMatch
        | datafusion_expr::Operator::NotLikeMatch
        | datafusion_expr::Operator::NotILikeMatch
        | datafusion_expr::Operator::BitwiseAnd
        | datafusion_expr::Operator::BitwiseOr
        | datafusion_expr::Operator::BitwiseXor
        | datafusion_expr::Operator::BitwiseShiftRight
        | datafusion_expr::Operator::BitwiseShiftLeft
        | datafusion_expr::Operator::StringConcat
        | datafusion_expr::Operator::AtArrow
        | datafusion_expr::Operator::ArrowAt
        | datafusion_expr::Operator::Arrow
        | datafusion_expr::Operator::LongArrow
        | datafusion_expr::Operator::HashArrow
        | datafusion_expr::Operator::HashLongArrow
        | datafusion_expr::Operator::AtAt
        | datafusion_expr::Operator::IntegerDivide
        | datafusion_expr::Operator::HashMinus
        | datafusion_expr::Operator::AtQuestion
        | datafusion_expr::Operator::Question
        | datafusion_expr::Operator::QuestionAnd
        | datafusion_expr::Operator::QuestionPipe
        | datafusion_expr::Operator::Colon => {
            if contains_null([left, right]) {
                Some(NullSubstitutionValue::Null)
            } else {
                None
            }
        }
        datafusion_expr::Operator::IsDistinctFrom
        | datafusion_expr::Operator::IsNotDistinctFrom => None,
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
            if let Some(syntactic) =
                syntactic_restrict_null_predicate(&predicate, &join_cols)
            {
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
}
