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
use std::sync::Arc;

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

    // If result is single `true`, return false;
    // If result is single `NULL` or `false`, return true;
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

fn coerce(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let mut expr_rewrite = TypeCoercionRewriter { schema };
    expr.rewrite(&mut expr_rewrite).data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{Operator, binary_expr, case, col, in_list, is_null, lit};

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
        ];

        let column_a = Column::from_name("a");
        for (predicate, expected) in test_cases {
            let join_cols_of_predicate = std::iter::once(&column_a);
            let actual =
                is_restrict_null_predicate(predicate.clone(), join_cols_of_predicate)?;
            assert_eq!(actual, expected, "{predicate}");
        }

        Ok(())
    }
}
