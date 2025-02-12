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

use std::vec;

use arrow::datatypes::{
    DataType, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{
    exec_err, internal_err, plan_err, Column, DFSchemaRef, DataFusionError, Diagnostic,
    HashMap, Result, ScalarValue,
};
use datafusion_expr::builder::get_struct_unnested_columns;
use datafusion_expr::expr::{Alias, GroupingSet, Unnest, WindowFunction};
use datafusion_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion_expr::{
    col, expr_vec_fmt, ColumnUnnestList, Expr, ExprSchemable, LogicalPlan,
};

use indexmap::IndexMap;
use sqlparser::ast::{Ident, Value};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckColumnsSatisfyExprsPurpose {
    ProjectionMustReferenceAggregate,
    HavingMustReferenceAggregate,
}

impl CheckColumnsSatisfyExprsPurpose {
    fn message_prefix(&self) -> &'static str {
        match self {
            CheckColumnsSatisfyExprsPurpose::ProjectionMustReferenceAggregate => {
                "Projection references non-aggregate values"
            }
            CheckColumnsSatisfyExprsPurpose::HavingMustReferenceAggregate => {
                "HAVING clause references non-aggregate values"
            }
        }
    }

    fn diagnostic_message(&self, expr: &Expr) -> String {
        format!("'{expr}' must appear in GROUP BY clause because it's not an aggregate expression")
    }
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
pub(crate) fn check_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
    purpose: CheckColumnsSatisfyExprsPurpose,
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
                    check_column_satisfies_expr(columns, e, purpose)?;
                }
            }
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, purpose)?;
                }
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                for exprs in lists_of_exprs {
                    for e in exprs {
                        check_column_satisfies_expr(columns, e, purpose)?;
                    }
                }
            }
            _ => check_column_satisfies_expr(columns, e, purpose)?,
        }
    }
    Ok(())
}

fn check_column_satisfies_expr(
    columns: &[Expr],
    expr: &Expr,
    purpose: CheckColumnsSatisfyExprsPurpose,
) -> Result<()> {
    if !columns.contains(expr) {
        return plan_err!(
            "{}: Expression {} could not be resolved from available columns: {}",
            purpose.message_prefix(),
            expr,
            expr_vec_fmt!(columns)
        )
        .map_err(|err| {
            let diagnostic = Diagnostic::new_error(
                purpose.diagnostic_message(expr),
                expr.spans().and_then(|spans| spans.first()),
            )
            .with_help(format!("add '{expr}' to GROUP BY clause"), None);
            err.with_diagnostic(diagnostic)
        });
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

/// Given a slice of window expressions sharing the same sort key, find their common partition
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

/// Normalize an owned identifier to a lowercase string, unless the identifier is quoted.
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
        Value::UnicodeStringLiteral(s) => Some(s.to_string()),
        Value::EscapedStringLiteral(s) => Some(s.to_string()),
        Value::DoubleQuotedString(_)
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

pub(crate) fn rewrite_recursive_unnests_bottom_up(
    input: &LogicalPlan,
    unnest_placeholder_columns: &mut IndexMap<Column, Option<Vec<ColumnUnnestList>>>,
    inner_projection_exprs: &mut Vec<Expr>,
    original_exprs: &[Expr],
) -> Result<Vec<Expr>> {
    Ok(original_exprs
        .iter()
        .map(|expr| {
            rewrite_recursive_unnest_bottom_up(
                input,
                unnest_placeholder_columns,
                inner_projection_exprs,
                expr,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>())
}

pub const UNNEST_PLACEHOLDER: &str = "__unnest_placeholder";

/*
This is only usedful when used with transform down up
A full example of how the transformation works:
 */
struct RecursiveUnnestRewriter<'a> {
    input_schema: &'a DFSchemaRef,
    root_expr: &'a Expr,
    // Useful to detect which child expr is a part of/ not a part of unnest operation
    top_most_unnest: Option<Unnest>,
    consecutive_unnest: Vec<Option<Unnest>>,
    inner_projection_exprs: &'a mut Vec<Expr>,
    columns_unnestings: &'a mut IndexMap<Column, Option<Vec<ColumnUnnestList>>>,
    transformed_root_exprs: Option<Vec<Expr>>,
}
impl RecursiveUnnestRewriter<'_> {
    /// This struct stores the history of expr
    /// during its tree-traversal with a notation of
    /// \[None,**Unnest(exprA)**,**Unnest(exprB)**,None,None\]
    /// then this function will returns \[**Unnest(exprA)**,**Unnest(exprB)**\]
    ///
    /// The first item will be the inner most expr
    fn get_latest_consecutive_unnest(&self) -> Vec<Unnest> {
        self.consecutive_unnest
            .iter()
            .rev()
            .skip_while(|item| item.is_none())
            .take_while(|item| item.is_some())
            .to_owned()
            .cloned()
            .map(|item| item.unwrap())
            .collect()
    }

    fn transform(
        &mut self,
        level: usize,
        alias_name: String,
        expr_in_unnest: &Expr,
        struct_allowed: bool,
    ) -> Result<Vec<Expr>> {
        let inner_expr_name = expr_in_unnest.schema_name().to_string();

        // Full context, we are trying to plan the execution as InnerProjection->Unnest->OuterProjection
        // inside unnest execution, each column inside the inner projection
        // will be transformed into new columns. Thus we need to keep track of these placeholding column names
        let placeholder_name = format!("{UNNEST_PLACEHOLDER}({})", inner_expr_name);
        let post_unnest_name =
            format!("{UNNEST_PLACEHOLDER}({},depth={})", inner_expr_name, level);
        // This is due to the fact that unnest transformation should keep the original
        // column name as is, to comply with group by and order by
        let placeholder_column = Column::from_name(placeholder_name.clone());

        let (data_type, _) = expr_in_unnest.data_type_and_nullable(self.input_schema)?;

        match data_type {
            DataType::Struct(inner_fields) => {
                if !struct_allowed {
                    return internal_err!("unnest on struct can only be applied at the root level of select expression");
                }
                push_projection_dedupl(
                    self.inner_projection_exprs,
                    expr_in_unnest.clone().alias(placeholder_name.clone()),
                );
                self.columns_unnestings
                    .insert(Column::from_name(placeholder_name.clone()), None);
                Ok(
                    get_struct_unnested_columns(&placeholder_name, &inner_fields)
                        .into_iter()
                        .map(Expr::Column)
                        .collect(),
                )
            }
            DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_) => {
                push_projection_dedupl(
                    self.inner_projection_exprs,
                    expr_in_unnest.clone().alias(placeholder_name.clone()),
                );

                let post_unnest_expr = col(post_unnest_name.clone()).alias(alias_name);
                let list_unnesting = self
                    .columns_unnestings
                    .entry(placeholder_column)
                    .or_insert(Some(vec![]));
                let unnesting = ColumnUnnestList {
                    output_column: Column::from_name(post_unnest_name),
                    depth: level,
                };
                let list_unnestings = list_unnesting.as_mut().unwrap();
                if !list_unnestings.contains(&unnesting) {
                    list_unnestings.push(unnesting);
                }
                Ok(vec![post_unnest_expr])
            }
            _ => {
                internal_err!("unnest on non-list or struct type is not supported")
            }
        }
    }
}

impl TreeNodeRewriter for RecursiveUnnestRewriter<'_> {
    type Node = Expr;

    /// This downward traversal needs to keep track of:
    /// - Whether or not some unnest expr has been visited from the top util the current node
    /// - If some unnest expr has been visited, maintain a stack of such information, this
    ///   is used to detect if some recursive unnest expr exists (e.g **unnest(unnest(unnest(3d column))))**
    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::Unnest(ref unnest_expr) = expr {
            let (data_type, _) =
                unnest_expr.expr.data_type_and_nullable(self.input_schema)?;
            self.consecutive_unnest.push(Some(unnest_expr.clone()));
            // if expr inside unnest is a struct, do not consider
            // the next unnest as consecutive unnest (if any)
            // meaning unnest(unnest(struct_arr_col)) can't
            // be interpreted as unnest(struct_arr_col, depth:=2)
            // but has to be split into multiple unnest logical plan instead
            // a.k.a:
            // - unnest(struct_col)
            //      unnest(struct_arr_col) as struct_col

            if let DataType::Struct(_) = data_type {
                self.consecutive_unnest.push(None);
            }
            if self.top_most_unnest.is_none() {
                self.top_most_unnest = Some(unnest_expr.clone());
            }

            Ok(Transformed::no(expr))
        } else {
            self.consecutive_unnest.push(None);
            Ok(Transformed::no(expr))
        }
    }

    /// The rewriting only happens when the traversal has reached the top-most unnest expr
    /// within a sequence of consecutive unnest exprs node
    ///
    /// For example an expr of **unnest(unnest(column1)) + unnest(unnest(unnest(column2)))**
    /// ```text
    ///                         ┌──────────────────┐           
    ///                         │    binaryexpr    │           
    ///                         │                  │           
    ///                         └──────────────────┘           
    ///                f_down  / /            │ │              
    ///                       / / f_up        │ │              
    ///                      / /        f_down│ │f_up          
    ///                  unnest               │ │              
    ///                                       │ │              
    ///       f_down  / / f_up(rewriting)     │ │              
    ///              / /                                       
    ///             / /                      unnest            
    ///         unnest                                         
    ///                           f_down  / / f_up(rewriting)  
    /// f_down / /f_up                   / /                   
    ///       / /                       / /                    
    ///      / /                    unnest                     
    ///   column1                                              
    ///                     f_down / /f_up                     
    ///                           / /                          
    ///                          / /                           
    ///                       column2                          
    /// ```
    ///         
    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::Unnest(ref traversing_unnest) = expr {
            if traversing_unnest == self.top_most_unnest.as_ref().unwrap() {
                self.top_most_unnest = None;
            }
            // Find inside consecutive_unnest, the sequence of continuous unnest exprs

            // Get the latest consecutive unnest exprs
            // and check if current upward traversal is the returning to the root expr
            // for example given a expr `unnest(unnest(col))` then the traversal happens like:
            // down(unnest) -> down(unnest) -> down(col) -> up(col) -> up(unnest) -> up(unnest)
            // the result of such traversal is unnest(col, depth:=2)
            let unnest_stack = self.get_latest_consecutive_unnest();

            // This traversal has reached the top most unnest again
            // e.g Unnest(top) -> Unnest(2nd) -> Column(bottom)
            // -> Unnest(2nd) -> Unnest(top) a.k.a here
            // Thus
            // Unnest(Unnest(some_col)) is rewritten into Unnest(some_col, depth:=2)
            if traversing_unnest == unnest_stack.last().unwrap() {
                let most_inner = unnest_stack.first().unwrap();
                let inner_expr = most_inner.expr.as_ref();
                // unnest(unnest(struct_arr_col)) is not allow to be done recursively
                // it needs to be splitted into multiple unnest logical plan
                // unnest(struct_arr)
                //  unnest(struct_arr_col) as struct_arr
                // instead of unnest(struct_arr_col, depth = 2)

                let unnest_recursion = unnest_stack.len();
                let struct_allowed = (&expr == self.root_expr) && unnest_recursion == 1;

                let mut transformed_exprs = self.transform(
                    unnest_recursion,
                    expr.schema_name().to_string(),
                    inner_expr,
                    struct_allowed,
                )?;
                if struct_allowed {
                    self.transformed_root_exprs = Some(transformed_exprs.clone());
                }
                return Ok(Transformed::new(
                    transformed_exprs.swap_remove(0),
                    true,
                    TreeNodeRecursion::Continue,
                ));
            }
        } else {
            self.consecutive_unnest.push(None);
        }

        // For column exprs that are not descendants of any unnest node
        // retain their projection
        // e.g given expr tree unnest(col_a) + col_b, we have to retain projection of col_b
        // this condition can be checked by maintaining an Option<top most unnest>
        if matches!(&expr, Expr::Column(_)) && self.top_most_unnest.is_none() {
            push_projection_dedupl(self.inner_projection_exprs, expr.clone());
        }

        Ok(Transformed::no(expr))
    }
}

fn push_projection_dedupl(projection: &mut Vec<Expr>, expr: Expr) {
    let schema_name = expr.schema_name().to_string();
    if !projection
        .iter()
        .any(|e| e.schema_name().to_string() == schema_name)
    {
        projection.push(expr);
    }
}
/// The context is we want to rewrite unnest() into InnerProjection->Unnest->OuterProjection
/// Given an expression which contains unnest expr as one of its children,
/// Try transform depends on unnest type
/// - For list column: unnest(col) with type list -> unnest(col) with type list::item
/// - For struct column: unnest(struct(field1, field2)) -> unnest(struct).field1, unnest(struct).field2
///
/// The transformed exprs will be used in the outer projection
/// If along the path from root to bottom, there are multiple unnest expressions, the transformation
/// is done only for the bottom expression
pub(crate) fn rewrite_recursive_unnest_bottom_up(
    input: &LogicalPlan,
    unnest_placeholder_columns: &mut IndexMap<Column, Option<Vec<ColumnUnnestList>>>,
    inner_projection_exprs: &mut Vec<Expr>,
    original_expr: &Expr,
) -> Result<Vec<Expr>> {
    let mut rewriter = RecursiveUnnestRewriter {
        input_schema: input.schema(),
        root_expr: original_expr,
        top_most_unnest: None,
        consecutive_unnest: vec![],
        inner_projection_exprs,
        columns_unnestings: unnest_placeholder_columns,
        transformed_root_exprs: None,
    };

    // This transformation is only done for list unnest
    // struct unnest is done at the root level, and at the later stage
    // because the syntax of TreeNode only support transform into 1 Expr, while
    // Unnest struct will be transformed into multiple Exprs
    // TODO: This can be resolved after this issue is resolved: https://github.com/apache/datafusion/issues/10102
    //
    // The transformation looks like:
    // - unnest(array_col) will be transformed into Column("unnest_place_holder(array_col)")
    // - unnest(array_col) + 1 will be transformed into Column("unnest_place_holder(array_col) + 1")
    let Transformed {
        data: transformed_expr,
        transformed,
        tnr: _,
    } = original_expr.clone().rewrite(&mut rewriter)?;

    if !transformed {
        if matches!(&transformed_expr, Expr::Column(_))
            || matches!(&transformed_expr, Expr::Wildcard { .. })
        {
            push_projection_dedupl(inner_projection_exprs, transformed_expr.clone());
            Ok(vec![transformed_expr])
        } else {
            // We need to evaluate the expr in the inner projection,
            // outer projection just select its name
            let column_name = transformed_expr.schema_name().to_string();
            push_projection_dedupl(inner_projection_exprs, transformed_expr);
            Ok(vec![Expr::Column(Column::from_name(column_name))])
        }
    } else {
        if let Some(transformed_root_exprs) = rewriter.transformed_root_exprs {
            return Ok(transformed_root_exprs);
        }
        Ok(vec![transformed_expr])
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Add, sync::Arc};

    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
    use datafusion_common::{Column, DFSchema, Result};
    use datafusion_expr::{
        col, lit, unnest, ColumnUnnestList, EmptyRelation, LogicalPlan,
    };
    use datafusion_functions::core::expr_ext::FieldAccessor;
    use datafusion_functions_aggregate::expr_fn::count;

    use crate::utils::{resolve_positions_to_exprs, rewrite_recursive_unnest_bottom_up};
    use indexmap::IndexMap;

    fn column_unnests_eq(
        l: Vec<&str>,
        r: &IndexMap<Column, Option<Vec<ColumnUnnestList>>>,
    ) {
        let r_formatted: Vec<String> = r
            .iter()
            .map(|i| match i.1 {
                None => format!("{}", i.0),
                Some(vec) => format!(
                    "{}=>[{}]",
                    i.0,
                    vec.iter()
                        .map(|i| format!("{}", i))
                        .collect::<Vec<String>>()
                        .join(", ")
                ),
            })
            .collect();
        let l_formatted: Vec<String> = l.iter().map(|i| i.to_string()).collect();
        assert_eq!(l_formatted, r_formatted);
    }

    #[test]
    fn test_transform_bottom_unnest_recursive() -> Result<()> {
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

        let mut unnest_placeholder_columns = IndexMap::new();
        let mut inner_projection_exprs = vec![];

        // unnest(unnest(3d_col)) + unnest(unnest(3d_col))
        let original_expr = unnest(unnest(col("3d_col")))
            .add(unnest(unnest(col("3d_col"))))
            .add(col("i64_col"));
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &original_expr,
        )?;
        // Only the bottom most unnest exprs are transformed
        assert_eq!(
            transformed_exprs,
            vec![col("__unnest_placeholder(3d_col,depth=2)")
                .alias("UNNEST(UNNEST(3d_col))")
                .add(
                    col("__unnest_placeholder(3d_col,depth=2)")
                        .alias("UNNEST(UNNEST(3d_col))")
                )
                .add(col("i64_col"))]
        );
        column_unnests_eq(
            vec![
                "__unnest_placeholder(3d_col)=>[__unnest_placeholder(3d_col,depth=2)|depth=2]",
            ],
            &unnest_placeholder_columns,
        );

        // Still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("__unnest_placeholder(3d_col)"),
                col("i64_col")
            ]
        );

        // unnest(3d_col) as 2d_col
        let original_expr_2 = unnest(col("3d_col")).alias("2d_col");
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &original_expr_2,
        )?;

        assert_eq!(
            transformed_exprs,
            vec![
                (col("__unnest_placeholder(3d_col,depth=1)").alias("UNNEST(3d_col)"))
                    .alias("2d_col")
            ]
        );
        column_unnests_eq(
            vec!["__unnest_placeholder(3d_col)=>[__unnest_placeholder(3d_col,depth=2)|depth=2, __unnest_placeholder(3d_col,depth=1)|depth=1]"],
            &unnest_placeholder_columns,
        );
        // Still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("3d_col").alias("__unnest_placeholder(3d_col)"),
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
                ArrowDataType::List(Arc::new(Field::new_list_field(
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

        let mut unnest_placeholder_columns = IndexMap::new();
        let mut inner_projection_exprs = vec![];

        // unnest(struct_col)
        let original_expr = unnest(col("struct_col"));
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &original_expr,
        )?;
        assert_eq!(
            transformed_exprs,
            vec![
                col("__unnest_placeholder(struct_col).field1"),
                col("__unnest_placeholder(struct_col).field2"),
            ]
        );
        column_unnests_eq(
            vec!["__unnest_placeholder(struct_col)"],
            &unnest_placeholder_columns,
        );
        // Still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_col").alias("__unnest_placeholder(struct_col)"),]
        );

        // unnest(array_col) + 1
        let original_expr = unnest(col("array_col")).add(lit(1i64));
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &original_expr,
        )?;
        column_unnests_eq(
            vec![
                "__unnest_placeholder(struct_col)",
                "__unnest_placeholder(array_col)=>[__unnest_placeholder(array_col,depth=1)|depth=1]",
            ],
            &unnest_placeholder_columns,
        );
        // Only transform the unnest children
        assert_eq!(
            transformed_exprs,
            vec![col("__unnest_placeholder(array_col,depth=1)")
                .alias("UNNEST(array_col)")
                .add(lit(1i64))]
        );

        // Keep appending to the current vector
        // Still reference array_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("struct_col").alias("__unnest_placeholder(struct_col)"),
                col("array_col").alias("__unnest_placeholder(array_col)")
            ]
        );

        Ok(())
    }

    // Unnest -> field access -> unnest
    #[test]
    fn test_transform_non_consecutive_unnests() -> Result<()> {
        // List of struct
        // [struct{'subfield1':list(i64), 'subfield2':list(utf8)}]
        let schema = Schema::new(vec![
            Field::new(
                "struct_list",
                ArrowDataType::List(Arc::new(Field::new(
                    "element",
                    ArrowDataType::Struct(Fields::from(vec![
                        Field::new(
                            // list of i64
                            "subfield1",
                            ArrowDataType::List(Arc::new(Field::new(
                                "i64_element",
                                ArrowDataType::Int64,
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            // list of utf8
                            "subfield2",
                            ArrowDataType::List(Arc::new(Field::new(
                                "utf8_element",
                                ArrowDataType::Utf8,
                                true,
                            ))),
                            true,
                        ),
                    ])),
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

        let mut unnest_placeholder_columns = IndexMap::new();
        let mut inner_projection_exprs = vec![];

        // An expr with multiple unnest
        let select_expr1 = unnest(unnest(col("struct_list")).field("subfield1"));
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &select_expr1,
        )?;
        // Only the inner most/ bottom most unnest is transformed
        assert_eq!(
            transformed_exprs,
            vec![unnest(
                col("__unnest_placeholder(struct_list,depth=1)")
                    .alias("UNNEST(struct_list)")
                    .field("subfield1")
            )]
        );

        column_unnests_eq(
            vec![
                "__unnest_placeholder(struct_list)=>[__unnest_placeholder(struct_list,depth=1)|depth=1]",
            ],
            &unnest_placeholder_columns,
        );

        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_list").alias("__unnest_placeholder(struct_list)")]
        );

        // continue rewrite another expr in select
        let select_expr2 = unnest(unnest(col("struct_list")).field("subfield2"));
        let transformed_exprs = rewrite_recursive_unnest_bottom_up(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            &select_expr2,
        )?;
        // Only the inner most/ bottom most unnest is transformed
        assert_eq!(
            transformed_exprs,
            vec![unnest(
                col("__unnest_placeholder(struct_list,depth=1)")
                    .alias("UNNEST(struct_list)")
                    .field("subfield2")
            )]
        );

        // unnest place holder columns remain the same
        // because expr1 and expr2 derive from the same unnest result
        column_unnests_eq(
            vec![
                "__unnest_placeholder(struct_list)=>[__unnest_placeholder(struct_list,depth=1)|depth=1]",
            ],
            &unnest_placeholder_columns,
        );

        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_list").alias("__unnest_placeholder(struct_list)")]
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
