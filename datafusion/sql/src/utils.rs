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

//! SQL Utility Functions

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::vec;

use arrow_schema::{
    DataType, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    exec_err, internal_err, plan_err, Column, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::builder::get_struct_unnested_columns;
use datafusion_expr::expr::{Alias, GroupingSet, Unnest, WindowFunction};
use datafusion_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion_expr::{expr_vec_fmt, Expr, ExprSchemable, LogicalPlan};
use datafusion_expr::{ColumnUnnestList, ColumnUnnestType};
use sqlparser::ast::Ident;
use sqlparser::ast::Value;

/// Make a best-effort attempt at resolving all columns in the expression tree
pub(crate) fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.clone()
        .transform_up(|nested_expr| {
            match nested_expr {
                Expr::Column(col) => {
                    let (qualifier, field) =
                        plan.schema().qualified_field_from_column(&col)?;
                    Ok(Transformed::yes(Expr::Column(Column::from((
                        qualifier, field,
                    )))))
                }
                _ => {
                    // keep recursing
                    Ok(Transformed::no(nested_expr))
                }
            }
        })
        .data()
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub(crate) fn rebase_expr(
    expr: &Expr,
    base_exprs: &[Expr],
    plan: &LogicalPlan,
) -> Result<Expr> {
    expr.clone()
        .transform_down(|nested_expr| {
            if base_exprs.contains(&nested_expr) {
                Ok(Transformed::yes(expr_as_column_expr(&nested_expr, plan)?))
            } else {
                Ok(Transformed::no(nested_expr))
            }
        })
        .data()
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
pub(crate) fn check_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
    message_prefix: &str,
) -> Result<()> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => internal_err!("Expr::Column are required"),
    })?;
    let column_exprs = find_column_exprs(exprs);
    for e in &column_exprs {
        match e {
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                for exprs in lists_of_exprs {
                    for e in exprs {
                        check_column_satisfies_expr(columns, e, message_prefix)?;
                    }
                }
            }
            _ => check_column_satisfies_expr(columns, e, message_prefix)?,
        }
    }
    Ok(())
}

fn check_column_satisfies_expr(
    columns: &[Expr],
    expr: &Expr,
    message_prefix: &str,
) -> Result<()> {
    if !columns.contains(expr) {
        return plan_err!(
            "{}: Expression {} could not be resolved from available columns: {}",
            message_prefix,
            expr,
            expr_vec_fmt!(columns)
        );
    }
    Ok(())
}

/// Returns mapping of each alias (`String`) to the expression (`Expr`) it is
/// aliasing.
pub(crate) fn extract_aliases(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(Alias { expr, name, .. }) => Some((name.clone(), *expr.clone())),
            _ => None,
        })
        .collect::<HashMap<String, Expr>>()
}

/// Given an expression that's literal int encoding position, lookup the corresponding expression
/// in the select_exprs list, if the index is within the bounds and it is indeed a position literal,
/// otherwise, returns planning error.
/// If input expression is not an int literal, returns expression as-is.
pub(crate) fn resolve_positions_to_exprs(
    expr: Expr,
    select_exprs: &[Expr],
) -> Result<Expr> {
    match expr {
        // sql_expr_to_logical_expr maps number to i64
        // https://github.com/apache/datafusion/blob/8d175c759e17190980f270b5894348dc4cff9bbf/datafusion/src/sql/planner.rs#L882-L887
        Expr::Literal(ScalarValue::Int64(Some(position)))
            if position > 0_i64 && position <= select_exprs.len() as i64 =>
        {
            let index = (position - 1) as usize;
            let select_expr = &select_exprs[index];
            Ok(match select_expr {
                Expr::Alias(Alias { expr, .. }) => *expr.clone(),
                _ => select_expr.clone(),
            })
        }
        Expr::Literal(ScalarValue::Int64(Some(position))) => plan_err!(
            "Cannot find column with position {} in SELECT clause. Valid columns: 1 to {}",
            position, select_exprs.len()
        ),
        _ => Ok(expr),
    }
}

/// Rebuilds an `Expr` with columns that refer to aliases replaced by the
/// alias' underlying `Expr`.
pub(crate) fn resolve_aliases_to_exprs(
    expr: Expr,
    aliases: &HashMap<String, Expr>,
) -> Result<Expr> {
    expr.transform_up(|nested_expr| match nested_expr {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Transformed::yes(aliased_expr.clone()))
            } else {
                Ok(Transformed::no(Expr::Column(c)))
            }
        }
        _ => Ok(Transformed::no(nested_expr)),
    })
    .data()
}

/// given a slice of window expressions sharing the same sort key, find their common partition
/// keys.
pub fn window_expr_common_partition_keys(window_exprs: &[Expr]) -> Result<&[Expr]> {
    let all_partition_keys = window_exprs
        .iter()
        .map(|expr| match expr {
            Expr::WindowFunction(WindowFunction { partition_by, .. }) => Ok(partition_by),
            Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
                Expr::WindowFunction(WindowFunction { partition_by, .. }) => {
                    Ok(partition_by)
                }
                expr => exec_err!("Impossibly got non-window expr {expr:?}"),
            },
            expr => exec_err!("Impossibly got non-window expr {expr:?}"),
        })
        .collect::<Result<Vec<_>>>()?;
    let result = all_partition_keys
        .iter()
        .min_by_key(|s| s.len())
        .ok_or_else(|| {
            DataFusionError::Execution("No window expressions found".to_owned())
        })?;
    Ok(result)
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return plan_err!("Cannot specify only scale for decimal data type")
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    if precision == 0
        || precision > DECIMAL256_MAX_PRECISION
        || scale.unsigned_abs() > precision
    {
        plan_err!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 76`, and `scale <= precision`."
        )
    } else if precision > DECIMAL128_MAX_PRECISION
        && precision <= DECIMAL256_MAX_PRECISION
    {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

// Normalize an owned identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}

pub(crate) fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(s) => Some(s.to_string()),
        Value::DollarQuotedString(s) => Some(s.to_string()),
        Value::Number(_, _) | Value::Boolean(_) => Some(value.to_string()),
        Value::DoubleQuotedString(_)
        | Value::EscapedStringLiteral(_)
        | Value::NationalStringLiteral(_)
        | Value::SingleQuotedByteStringLiteral(_)
        | Value::DoubleQuotedByteStringLiteral(_)
        | Value::TripleSingleQuotedString(_)
        | Value::TripleDoubleQuotedString(_)
        | Value::TripleSingleQuotedByteStringLiteral(_)
        | Value::TripleDoubleQuotedByteStringLiteral(_)
        | Value::SingleQuotedRawStringLiteral(_)
        | Value::DoubleQuotedRawStringLiteral(_)
        | Value::TripleSingleQuotedRawStringLiteral(_)
        | Value::TripleDoubleQuotedRawStringLiteral(_)
        | Value::HexStringLiteral(_)
        | Value::Null
        | Value::Placeholder(_) => None,
    }
}

pub(crate) fn transform_bottom_unnests(
    input: &LogicalPlan,
    unnest_placeholder_columns: &mut Vec<(Column, ColumnUnnestType)>,
    inner_projection_exprs: &mut Vec<Expr>,
    memo: &mut HashMap<String, Vec<Column>>,
    original_exprs: &[Expr],
) -> Result<Vec<Expr>> {
    Ok(original_exprs
        .iter()
        .map(|expr| {
            transform_bottom_unnest(
                input,
                unnest_placeholder_columns,
                inner_projection_exprs,
                memo,
                expr,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>())
}

/// Explain me
/// The context is we want to rewrite unnest() into InnerProjection->Unnest->OuterProjection
/// Given an expression which contains unnest expr as one of its children,
/// Try transform depends on unnest type
/// - For list column: unnest(col) with type list -> unnest(col) with type list::item
/// - For struct column: unnest(struct(field1, field2)) -> unnest(struct).field1, unnest(struct).field2
///
/// The transformed exprs will be used in the outer projection
/// If along the path from root to bottom, there are multiple unnest expressions, the transformation
/// is done only for the bottom expression
pub(crate) fn transform_bottom_unnest(
    input: &LogicalPlan,
    unnest_placeholder_columns: &mut Vec<(Column, ColumnUnnestType)>,
    inner_projection_exprs: &mut Vec<Expr>,
    memo: &mut HashMap<String, Vec<Column>>,
    original_expr: &Expr,
) -> Result<Vec<Expr>> {
    let mut transform = |level: usize,
                         expr_in_unnest: &Expr,
                         struct_allowed: bool,
                         inner_projection_exprs: &mut Vec<Expr>|
     -> Result<Vec<Expr>> {
        let inner_expr_name = expr_in_unnest.schema_name().to_string();
        // let col = match expr_in_unnest {
        //     Expr::Column(col) => col,
        //     _ => {
        //         // TODO: this failed
        //         return internal_err!("unnesting on non-column expr is not supported");
        //     }
        // };

        // Full context, we are trying to plan the execution as InnerProjection->Unnest->OuterProjection
        // inside unnest execution, each column inside the inner projection
        // will be transformed into new columns. Thus we need to keep track of these placeholding column names
        // let placeholder_name = unnest_expr.display_name()?;
        let placeholder_name = format!("unnest_placeholder({})", inner_expr_name);
        let post_unnest_name =
            format!("unnest_placeholder({},depth={})", inner_expr_name, level);
        let placeholder_column = Column::from_name(placeholder_name.clone());
        let schema = input.schema();

        let (data_type, _) = expr_in_unnest.data_type_and_nullable(schema)?;

        match data_type {
            DataType::Struct(inner_fields) => {
                if !struct_allowed {
                    return internal_err!("unnest on struct can only be applied at the root level of select expression");
                }
                inner_projection_exprs
                    .push(expr_in_unnest.clone().alias(placeholder_name.clone()));
                unnest_placeholder_columns.push((
                    Column::from_name(placeholder_name.clone()),
                    ColumnUnnestType::Struct,
                ));
                return Ok(
                    get_struct_unnested_columns(&placeholder_name, &inner_fields)
                        .into_iter()
                        .map(|c| Expr::Column(c))
                        .collect(),
                );
            }
            DataType::List(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field) => {
                // TODO: this memo only needs to be a hashset
                let (already_projected, transformed_cols) =
                    match memo.get_mut(&inner_expr_name) {
                        Some(vec) => (true, vec),
                        _ => {
                            memo.insert(inner_expr_name.clone(), vec![]);
                            (false, memo.get_mut(&inner_expr_name).unwrap())
                        }
                    };
                if !already_projected {
                    inner_projection_exprs
                        .push(expr_in_unnest.clone().alias(placeholder_name.clone()));
                }

                let post_unnest_column = Column::from_name(post_unnest_name);
                match unnest_placeholder_columns
                    .iter_mut()
                    .find(|(inner_col, _)| inner_col == &placeholder_column)
                {
                    None => {
                        unnest_placeholder_columns.push((
                            placeholder_column.clone(),
                            ColumnUnnestType::List(vec![ColumnUnnestList {
                                output_column: post_unnest_column.clone(),
                                depth: level,
                            }]),
                        ));
                    }
                    Some((col, unnesting)) => match unnesting {
                        ColumnUnnestType::List(list) => {
                            let unnesting = ColumnUnnestList {
                                output_column: post_unnest_column.clone(),
                                depth: level,
                            };
                            if !list.contains(&unnesting) {
                                list.push(unnesting);
                            }
                        }
                        _ => {
                            return internal_err!("expr_in_unnest is a list type, while previous unnesting on this column is not a list type");
                        }
                    },
                }
                return Ok(vec![Expr::Column(post_unnest_column)]);
            }
            _ => {
                return internal_err!(
                    "unnest on non-list or struct type is not supported"
                );
            }
        }
    };
    let latest_visited_unnest = RefCell::new(None);
    let exprs_under_unnest = RefCell::new(HashSet::new());
    let ancestor_unnest = RefCell::new(None);

    let consecutive_unnest = RefCell::new(Vec::<Option<Expr>>::new());
    // we need to mark only the latest unnest expr that was visitted during the down traversal
    let transform_down = |expr: Expr| -> Result<Transformed<Expr>> {
        if let Expr::Unnest(Unnest {
            expr: ref inner_expr,
        }) = expr
        {
            let mut consecutive_unnest_mut = consecutive_unnest.borrow_mut();
            consecutive_unnest_mut.push(Some(expr.clone()));

            let mut maybe_ancestor = ancestor_unnest.borrow_mut();
            if maybe_ancestor.is_none() {
                *maybe_ancestor = Some(expr.clone());
            }

            exprs_under_unnest.borrow_mut().insert(inner_expr.clone());
            *latest_visited_unnest.borrow_mut() = Some(expr.clone());
            Ok(Transformed::no(expr))
        } else {
            consecutive_unnest.borrow_mut().push(None);
            Ok(Transformed::no(expr))
        }
    };
    let mut transformed_root_exprs = None;
    let transform_up = |expr: Expr| -> Result<Transformed<Expr>> {
        // From the bottom up, we know the latest consecutive unnest sequence
        // we only do the transformation at the top unnest node
        // For example given this complex expr
        // - unnest(array_concat(unnest([[1,2,3]]),unnest([[4,5,6]]))) + unnest(unnest([[7,8,9]))
        // traversal will be like this:
        // down[binary_add]
        //  ->down[unnest(...)]->down[array_concat]->down/up[unnest([[1,2,3]])]->down/up[unnest([[4,5,6]])]
        // ->up[array_concat]->up[unnest(...)]->down[unnest(unnest(...))]->down[unnest([[7,8,9]])]
        // ->up[unnest([[7,8,9]])]->up[unnest(unnest(...))]->up[binary_add]
        // the transformation only happens for unnest([[1,2,3]]), unnest([[4,5,6]]) and unnest(unnest([[7,8,9]]))
        // and the complex expr will be rewritten into:
        // unnest(array_concat(place_holder_col_1, place_holder_col_2)) + place_holder_col_3
        if let Expr::Unnest(Unnest { .. }) = expr {
            let mut down_unnest_mut = ancestor_unnest.borrow_mut();
            // upward traversal has reached the top unnest expr again
            // reset it to None
            if *down_unnest_mut == Some(expr.clone()) {
                down_unnest_mut.take();
            }
            // find inside consecutive_unnest, the sequence of continous unnest exprs
            let mut found_first_unnest = false;
            let mut unnest_stack = vec![];
            for item in consecutive_unnest.borrow().iter().rev() {
                if let Some(expr) = item {
                    found_first_unnest = true;
                    unnest_stack.push(expr.clone());
                } else {
                    if !found_first_unnest {
                        continue;
                    }
                    break;
                }
            }

            // this is the top most unnest expr inside the consecutive unnest exprs
            // e.g unnest(unnest(some_col))
            if expr == *unnest_stack.last().unwrap() {
                let most_inner = unnest_stack.first().unwrap();
                if let Expr::Unnest(Unnest { expr: ref arg }) = most_inner {
                    let depth = unnest_stack.len();
                    let struct_allowed = (&expr == original_expr) && depth == 1;

                    let mut transformed_exprs =
                        transform(depth, arg, struct_allowed, inner_projection_exprs)?;
                    if struct_allowed {
                        transformed_root_exprs = Some(transformed_exprs.clone());
                    }
                    return Ok(Transformed::new(
                        transformed_exprs.swap_remove(0),
                        true,
                        TreeNodeRecursion::Continue,
                    ));
                } else {
                    return internal_err!("not reached");
                }

                // }
            }
        } else {
            consecutive_unnest.borrow_mut().push(None);
        }

        // For column exprs that are not descendants of any unnest node
        // retain their projection
        // e.g given expr tree unnest(col_a) + col_b, we have to retain projection of col_b
        // down_unnest is non means current upward traversal is not descendant of any unnest
        if matches!(&expr, Expr::Column(_)) && ancestor_unnest.borrow().is_none() {
            inner_projection_exprs.push(expr.clone());
        }

        Ok(Transformed::no(expr))
    };

    // This transformation is only done for list unnest
    // struct unnest is done at the root level, and at the later stage
    // because the syntax of TreeNode only support transform into 1 Expr, while
    // Unnest struct will be transformed into multiple Exprs
    // TODO: This can be resolved after this issue is resolved: https://github.com/apache/datafusion/issues/10102
    //
    // The transformation looks like:
    // - unnest(array_col) will be transformed into unnest(array_col)
    // - unnest(array_col) + 1 will be transformed into unnest(array_col) + 1
    let Transformed {
        data: transformed_expr,
        transformed,
        tnr: _,
    } = original_expr
        .clone()
        .transform_down_up(transform_down, transform_up)?;

    if !transformed {
        if matches!(&transformed_expr, Expr::Column(_)) {
            inner_projection_exprs.push(transformed_expr.clone());
            Ok(vec![transformed_expr])
        } else {
            // We need to evaluate the expr in the inner projection,
            // outer projection just select its name
            let column_name = transformed_expr.schema_name().to_string();
            inner_projection_exprs.push(transformed_expr);
            Ok(vec![Expr::Column(Column::from_name(column_name))])
        }
    } else {
        if let Some(transformed_root_exprs) = transformed_root_exprs {
            return Ok(transformed_root_exprs);
        }
        Ok(vec![transformed_expr])
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ops::Add, sync::Arc};

    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow_schema::Fields;
    use datafusion_common::{Column, DFSchema, Result};
    use datafusion_expr::{
        col, lit, unnest, ColumnUnnestType, EmptyRelation, LogicalPlan,
    };
    use datafusion_functions::core::expr_ext::FieldAccessor;
    use datafusion_functions_aggregate::expr_fn::count;

    use crate::utils::{resolve_positions_to_exprs, transform_bottom_unnest};
    fn column_unnests_eq(l: Vec<&str>, r: &[(Column, ColumnUnnestType)]) {
        let formatted: Vec<String> =
            r.iter().map(|i| format!("{}|{}", i.0, i.1)).collect();
        assert_eq!(l, formatted)
    }

    #[test]
    fn test_transform_bottom_unnest_recursive_memoization_struct() -> Result<()> {
        let three_d_dtype = ArrowDataType::List(Arc::new(Field::new(
            "2d_col",
            ArrowDataType::List(Arc::new(Field::new(
                "elements",
                ArrowDataType::Int64,
                true,
            ))),
            true,
        )));
        let schema = Schema::new(vec![
            // list[struct(3d_data)] [([[1,2,3]])]
            Field::new(
                "struct_arr_col",
                ArrowDataType::List(Arc::new(Field::new(
                    "struct",
                    ArrowDataType::Struct(Fields::from(vec![Field::new(
                        "field1",
                        three_d_dtype,
                        true,
                    )])),
                    true,
                ))),
                true,
            ),
            Field::new(
                "3d_col",
                ArrowDataType::List(Arc::new(Field::new(
                    "2d_col",
                    ArrowDataType::List(Arc::new(Field::new(
                        "elements",
                        ArrowDataType::Int64,
                        true,
                    ))),
                    true,
                ))),
                true,
            ),
            Field::new("i64_col", ArrowDataType::Int64, true),
        ]);

        let dfschema = DFSchema::try_from(schema)?;

        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(dfschema),
        });

        let mut unnest_placeholder_columns = vec![];
        let mut inner_projection_exprs = vec![];

        // unnest(unnest(3d_col)) + unnest(unnest(3d_col))
        let original_expr = unnest(unnest(col("3d_col")))
            .add(unnest(unnest(col("3d_col"))))
            .add(col("i64_col"));
        let mut memo = HashMap::new();
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr,
        )?;
        // only the bottom most unnest exprs are transformed
        assert_eq!(
            transformed_exprs,
            vec![col("unnest_placeholder(3d_col,depth=2)")
                .add(col("unnest_placeholder(3d_col,depth=2)"))
                .add(col("i64_col"))]
        );
        // memoization only contains 1 transformation
        assert_eq!(memo.len(), 1);
        assert!(memo.get("3d_col").is_some());
        column_unnests_eq(
            vec!["unnest_placeholder(3d_col)"],
            &unnest_placeholder_columns,
        );

        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("unnest_placeholder(3d_col)"),
                col("i64_col")
            ]
        );

        // unnest(3d_col) as 2d_col
        let original_expr_2 = unnest(col("3d_col")).alias("2d_col");
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr_2,
        )?;

        assert_eq!(
            transformed_exprs,
            vec![col("unnest_placeholder(3d_col,depth=1)").alias("2d_col")]
        );
        // memoization still contains 1 transformation
        // and the previous transformation is reused
        assert_eq!(memo.len(), 1);
        assert!(memo.get("3d_col").is_some());
        column_unnests_eq(
            vec!["unnest_placeholder(3d_col)"],
            &mut unnest_placeholder_columns,
        );
        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("unnest_placeholder(3d_col)"),
                col("i64_col")
            ]
        );

        // unnest(unnset(unnest(struct_arr_col)['field1'])) as fully_unnested_struct_arr
        let original_expr_3 =
            unnest(unnest(unnest(col("struct_arr_col")).field("field1")))
                .alias("fully_unnested_struct_arr");
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr_3,
        )?;

        assert_eq!(
            transformed_exprs,
            vec![unnest(unnest(
                col("unnest_placeholder(struct_arr_col,depth=1)").field("field1")
            ))
            .alias("fully_unnested_struct_arr")]
        );
        // memoization still contains 1 transformation
        // and the previous transformation is reused
        assert_eq!(memo.len(), 2);

        assert!(memo.get("struct_arr_col").is_some());
        column_unnests_eq(
            vec![
                "unnest_placeholder(3d_col)",
                "unnest_placeholder(struct_arr_col)",
            ],
            &mut unnest_placeholder_columns,
        );
        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("unnest_placeholder(3d_col)"),
                col("i64_col"),
                col("struct_arr_col").alias("unnest_placeholder(struct_arr_col)")
            ]
        );

        Ok(())
    }

    #[test]
    fn test_transform_bottom_unnest_recursive_memoization() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new(
                "3d_col",
                ArrowDataType::List(Arc::new(Field::new(
                    "2d_col",
                    ArrowDataType::List(Arc::new(Field::new(
                        "elements",
                        ArrowDataType::Int64,
                        true,
                    ))),
                    true,
                ))),
                true,
            ),
            Field::new("i64_col", ArrowDataType::Int64, true),
        ]);

        let dfschema = DFSchema::try_from(schema)?;

        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(dfschema),
        });

        let mut unnest_placeholder_columns = vec![];
        let mut inner_projection_exprs = vec![];

        // unnest(unnest(3d_col)) + unnest(unnest(3d_col))
        let original_expr = unnest(unnest(col("3d_col")))
            .add(unnest(unnest(col("3d_col"))))
            .add(col("i64_col"));
        let mut memo = HashMap::new();
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr,
        )?;
        // only the bottom most unnest exprs are transformed
        assert_eq!(
            transformed_exprs,
            vec![col("unnest_placeholder(3d_col,depth=2)")
                .add(col("unnest_placeholder(3d_col,depth=2)"))
                .add(col("i64_col"))]
        );
        // memoization only contains 1 transformation
        assert_eq!(memo.len(), 1);
        assert!(memo.get("3d_col").is_some());
        column_unnests_eq(
            vec!["unnest_placeholder(3d_col)|List([unnest_placeholder(3d_col,depth=2)|depth=2])"],
            &unnest_placeholder_columns,
        );

        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("unnest_placeholder(3d_col)"),
                col("i64_col")
            ]
        );

        // unnest(3d_col) as 2d_col
        let original_expr_2 = unnest(col("3d_col")).alias("2d_col");
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr_2,
        )?;

        assert_eq!(
            transformed_exprs,
            vec![col("unnest_placeholder(3d_col,depth=1)").alias("2d_col")]
        );
        // memoization still contains 1 transformation
        // and the for the same column, depth = 1 needs to be performed aside from depth = 2
        assert_eq!(memo.len(), 1);
        assert!(memo.get("3d_col").is_some());
        column_unnests_eq(
            vec!["unnest_placeholder(3d_col)|List([unnest_placeholder(3d_col,depth=2)|depth=2, unnest_placeholder(3d_col,depth=1)|depth=1])"],
            &unnest_placeholder_columns,
        );
        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("unnest_placeholder(3d_col)"),
                col("i64_col")
            ]
        );

        Ok(())
    }

    #[test]
    fn test_transform_bottom_unnest() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new(
                "struct_col",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("field1", ArrowDataType::Int32, false),
                    Field::new("field2", ArrowDataType::Int32, false),
                ])),
                false,
            ),
            Field::new(
                "array_col",
                ArrowDataType::List(Arc::new(Field::new(
                    "item",
                    ArrowDataType::Int64,
                    true,
                ))),
                true,
            ),
            Field::new("int_col", ArrowDataType::Int32, false),
        ]);

        let dfschema = DFSchema::try_from(schema)?;

        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(dfschema),
        });

        let mut unnest_placeholder_columns = vec![];
        let mut inner_projection_exprs = vec![];

        let mut memo = HashMap::new();
        // unnest(struct_col)
        let original_expr = unnest(col("struct_col"));
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr,
        )?;
        assert_eq!(
            transformed_exprs,
            vec![
                col("UNNEST(struct_col).field1"),
                col("UNNEST(struct_col).field2"),
            ]
        );
        column_unnests_eq(
            vec!["unnest_placeholder(struct_col)"],
            &mut unnest_placeholder_columns,
        );
        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_col").alias("UNNEST(struct_col)"),]
        );

        memo.clear();
        // unnest(array_col) + 1
        let original_expr = unnest(col("array_col")).add(lit(1i64));
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr,
        )?;
        column_unnests_eq(
            vec![
                "unnest_placeholder(struct_col)",
                "unnest_placeholder(array_col)",
            ],
            &mut unnest_placeholder_columns,
        );
        // only transform the unnest children
        assert_eq!(
            transformed_exprs,
            vec![col("UNNEST(array_col)").add(lit(1i64))]
        );

        // keep appending to the current vector
        // still reference array_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("struct_col").alias("UNNEST(struct_col)"),
                col("array_col").alias("UNNEST(array_col)")
            ]
        );

        // a nested structure struct[[]]
        let schema = Schema::new(vec![
            Field::new(
                "struct_col", // {array_col: [1,2,3]}
                ArrowDataType::Struct(Fields::from(vec![Field::new(
                    "matrix",
                    ArrowDataType::List(Arc::new(Field::new(
                        "matrix_row",
                        ArrowDataType::List(Arc::new(Field::new(
                            "item",
                            ArrowDataType::Int64,
                            true,
                        ))),
                        true,
                    ))),
                    true,
                )])),
                false,
            ),
            Field::new("int_col", ArrowDataType::Int32, false),
        ]);

        let dfschema = DFSchema::try_from(schema)?;

        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(dfschema),
        });

        let mut unnest_placeholder_columns = vec![];
        let mut inner_projection_exprs = vec![];
        memo.clear();

        // An expr with multiple unnest
        let original_expr = unnest(unnest(col("struct_col").field("matrix")));
        let transformed_exprs = transform_bottom_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &mut memo,
            &original_expr,
        )?;
        // Only the inner most/ bottom most unnest is transformed
        assert_eq!(
            transformed_exprs,
            vec![unnest(col("UNNEST(struct_col[matrix])"))]
        );

        column_unnests_eq(
            vec!["unnest_placeholder(struct_col[matrix])"],
            &mut unnest_placeholder_columns,
        );

        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_col")
                .field("matrix")
                .alias("UNNEST(struct_col[matrix])"),]
        );

        Ok(())
    }

    #[test]
    fn test_resolve_positions_to_exprs() -> Result<()> {
        let select_exprs = vec![col("c1"), col("c2"), count(lit(1))];

        // Assert 1 resolved as first column in select list
        let resolved = resolve_positions_to_exprs(lit(1i64), &select_exprs)?;
        assert_eq!(resolved, col("c1"));

        // Assert error if index out of select clause bounds
        let resolved = resolve_positions_to_exprs(lit(-1i64), &select_exprs);
        assert!(resolved.is_err_and(|e| e.message().contains(
            "Cannot find column with position -1 in SELECT clause. Valid columns: 1 to 3"
        )));

        let resolved = resolve_positions_to_exprs(lit(5i64), &select_exprs);
        assert!(resolved.is_err_and(|e| e.message().contains(
            "Cannot find column with position 5 in SELECT clause. Valid columns: 1 to 3"
        )));

        // Assert expression returned as-is
        let resolved = resolve_positions_to_exprs(lit("text"), &select_exprs)?;
        assert_eq!(resolved, lit("text"));

        let resolved = resolve_positions_to_exprs(col("fake"), &select_exprs)?;
        assert_eq!(resolved, col("fake"));

        Ok(())
    }
}
