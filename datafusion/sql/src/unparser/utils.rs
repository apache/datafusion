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

use datafusion_common::{
    internal_err,
    tree_node::{Transformed, TreeNode},
    Column, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    utils::grouping_set_to_exprlist, Aggregate, Expr, LogicalPlan, Projection, SortExpr,
    Window,
};
use sqlparser::ast;

use super::{dialect::DateFieldExtractStyle, Unparser};

/// Recursively searches children of [LogicalPlan] to find an Aggregate node if exists
/// prior to encountering a Join, TableScan, or a nested subquery (derived table factor).
/// If an Aggregate or node is not found prior to this or at all before reaching the end
/// of the tree, None is returned.
pub(crate) fn find_agg_node_within_select(
    plan: &LogicalPlan,
    already_projected: bool,
) -> Option<&Aggregate> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return None;
    } else {
        input.first()?
    };
    // Agg nodes explicitly return immediately with a single node
    if let LogicalPlan::Aggregate(agg) = input {
        Some(agg)
    } else if let LogicalPlan::TableScan(_) = input {
        None
    } else if let LogicalPlan::Projection(_) = input {
        if already_projected {
            None
        } else {
            find_agg_node_within_select(input, true)
        }
    } else {
        find_agg_node_within_select(input, already_projected)
    }
}

/// Recursively searches children of [LogicalPlan] to find Window nodes if exist
/// prior to encountering a Join, TableScan, or a nested subquery (derived table factor).
/// If Window node is not found prior to this or at all before reaching the end
/// of the tree, None is returned.
pub(crate) fn find_window_nodes_within_select<'a>(
    plan: &'a LogicalPlan,
    mut prev_windows: Option<Vec<&'a Window>>,
    already_projected: bool,
) -> Option<Vec<&'a Window>> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return prev_windows;
    } else {
        input.first()?
    };

    // Window nodes accumulate in a vec until encountering a TableScan or 2nd projection
    match input {
        LogicalPlan::Window(window) => {
            prev_windows = match &mut prev_windows {
                Some(windows) => {
                    windows.push(window);
                    prev_windows
                }
                _ => Some(vec![window]),
            };
            find_window_nodes_within_select(input, prev_windows, already_projected)
        }
        LogicalPlan::Projection(_) => {
            if already_projected {
                prev_windows
            } else {
                find_window_nodes_within_select(input, prev_windows, true)
            }
        }
        LogicalPlan::TableScan(_) => prev_windows,
        _ => find_window_nodes_within_select(input, prev_windows, already_projected),
    }
}

/// Recursively identify all Column expressions and transform them into the appropriate
/// aggregate expression contained in agg.
///
/// For example, if expr contains the column expr "COUNT(*)" it will be transformed
/// into an actual aggregate expression COUNT(*) as identified in the aggregate node.
pub(crate) fn unproject_agg_exprs(
    expr: &Expr,
    agg: &Aggregate,
    windows: Option<&[&Window]>,
) -> Result<Expr> {
    expr.clone()
        .transform(|sub_expr| {
            if let Expr::Column(c) = sub_expr {
                if let Some(unprojected_expr) = find_agg_expr(agg, &c)? {
                    Ok(Transformed::yes(unprojected_expr.clone()))
                } else if let Some(mut unprojected_expr) =
                    windows.and_then(|w| find_window_expr(w, &c.name).cloned())
                {
                    if let Expr::WindowFunction(func) = &mut unprojected_expr {
                        // Window function can contain an aggregation column, e.g., 'avg(sum(ss_sales_price)) over ...' that needs to be unprojected
                        for arg in &mut func.args {
                            if let Expr::Column(c) = arg {
                                if let Some(expr) = find_agg_expr(agg, c)? {
                                    *arg = expr.clone();
                                }
                            }
                        }
                    }
                    Ok(Transformed::yes(unprojected_expr))
                } else {
                    internal_err!(
                        "Tried to unproject agg expr for column '{}' that was not found in the provided Aggregate!", &c.name
                    )
                }
            } else {
                Ok(Transformed::no(sub_expr))
            }
        })
        .map(|e| e.data)
}

/// Recursively identify all Column expressions and transform them into the appropriate
/// window expression contained in window.
///
/// For example, if expr contains the column expr "COUNT(*) PARTITION BY id" it will be transformed
/// into an actual window expression as identified in the window node.
pub(crate) fn unproject_window_exprs(expr: &Expr, windows: &[&Window]) -> Result<Expr> {
    expr.clone()
        .transform(|sub_expr| {
            if let Expr::Column(c) = sub_expr {
                if let Some(unproj) = find_window_expr(windows, &c.name) {
                    Ok(Transformed::yes(unproj.clone()))
                } else {
                    Ok(Transformed::no(Expr::Column(c)))
                }
            } else {
                Ok(Transformed::no(sub_expr))
            }
        })
        .map(|e| e.data)
}

fn find_agg_expr<'a>(agg: &'a Aggregate, column: &Column) -> Result<Option<&'a Expr>> {
    if let Ok(index) = agg.schema.index_of_column(column) {
        if matches!(agg.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
            // For grouping set expr, we must operate by expression list from the grouping set
            let grouping_expr = grouping_set_to_exprlist(agg.group_expr.as_slice())?;
            return Ok(grouping_expr
                .into_iter()
                .chain(agg.aggr_expr.iter())
                .nth(index));
        } else {
            return Ok(agg.group_expr.iter().chain(agg.aggr_expr.iter()).nth(index));
        };
    } else {
        Ok(None)
    }
}

fn find_window_expr<'a>(
    windows: &'a [&'a Window],
    column_name: &'a str,
) -> Option<&'a Expr> {
    windows
        .iter()
        .flat_map(|w| w.window_expr.iter())
        .find(|expr| expr.schema_name().to_string() == column_name)
}

/// Transforms a Column expression into the actual expression from aggregation or projection if found.
/// This is required because if an ORDER BY expression is present in an Aggregate or Select, it is replaced
/// with a Column expression (e.g., "sum(catalog_returns.cr_net_loss)"). We need to transform it back to
/// the actual expression, such as sum("catalog_returns"."cr_net_loss").
pub(crate) fn unproject_sort_expr(
    sort_expr: &SortExpr,
    agg: Option<&Aggregate>,
    input: &LogicalPlan,
) -> Result<SortExpr> {
    let mut sort_expr = sort_expr.clone();

    // Remove alias if present, because ORDER BY cannot use aliases
    if let Expr::Alias(alias) = &sort_expr.expr {
        sort_expr.expr = *alias.expr.clone();
    }

    let Expr::Column(ref col_ref) = sort_expr.expr else {
        return Ok::<_, DataFusionError>(sort_expr);
    };

    if col_ref.relation.is_some() {
        return Ok::<_, DataFusionError>(sort_expr);
    };

    // In case of aggregation there could be columns containing aggregation functions we need to unproject
    if let Some(agg) = agg {
        if agg.schema.is_column_from_schema(col_ref) {
            let new_expr = unproject_agg_exprs(&sort_expr.expr, agg, None)?;
            sort_expr.expr = new_expr;
            return Ok::<_, DataFusionError>(sort_expr);
        }
    }

    // If SELECT and ORDER BY contain the same expression with a scalar function, the ORDER BY expression will
    // be replaced by a Column expression (e.g., "substr(customer.c_last_name, Int64(0), Int64(5))"), and we need
    // to transform it back to the actual expression.
    if let LogicalPlan::Projection(Projection { expr, schema, .. }) = input {
        if let Ok(idx) = schema.index_of_column(col_ref) {
            if let Some(Expr::ScalarFunction(scalar_fn)) = expr.get(idx) {
                sort_expr.expr = Expr::ScalarFunction(scalar_fn.clone());
            }
        }
        return Ok::<_, DataFusionError>(sort_expr);
    }

    Ok::<_, DataFusionError>(sort_expr)
}

/// Converts a date_part function to SQL, tailoring it to the supported date field extraction style.
pub(crate) fn date_part_to_sql(
    unparser: &Unparser,
    style: DateFieldExtractStyle,
    date_part_args: &[Expr],
) -> Result<Option<ast::Expr>> {
    match (style, date_part_args.len()) {
        (DateFieldExtractStyle::Extract, 2) => {
            let date_expr = unparser.expr_to_sql(&date_part_args[1])?;
            if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &date_part_args[0] {
                let field = match field.to_lowercase().as_str() {
                    "year" => ast::DateTimeField::Year,
                    "month" => ast::DateTimeField::Month,
                    "day" => ast::DateTimeField::Day,
                    "hour" => ast::DateTimeField::Hour,
                    "minute" => ast::DateTimeField::Minute,
                    "second" => ast::DateTimeField::Second,
                    _ => return Ok(None),
                };

                return Ok(Some(ast::Expr::Extract {
                    field,
                    expr: Box::new(date_expr),
                    syntax: ast::ExtractSyntax::From,
                }));
            }
        }
        (DateFieldExtractStyle::Strftime, 2) => {
            let column = unparser.expr_to_sql(&date_part_args[1])?;

            if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &date_part_args[0] {
                let field = match field.to_lowercase().as_str() {
                    "year" => "%Y",
                    "month" => "%m",
                    "day" => "%d",
                    "hour" => "%H",
                    "minute" => "%M",
                    "second" => "%S",
                    _ => return Ok(None),
                };

                return Ok(Some(ast::Expr::Function(ast::Function {
                    name: ast::ObjectName(vec![ast::Ident {
                        value: "strftime".to_string(),
                        quote_style: None,
                    }]),
                    args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                ast::Expr::Value(ast::Value::SingleQuotedString(
                                    field.to_string(),
                                )),
                            )),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(column)),
                        ],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    parameters: ast::FunctionArguments::None,
                })));
            }
        }
        (DateFieldExtractStyle::DatePart, _) => {
            return Ok(Some(
                unparser.scalar_function_to_sql("date_part", date_part_args)?,
            ));
        }
        _ => {}
    };

    Ok(None)
}
