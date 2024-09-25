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

use datafusion_expr::ScalarUDF;
use sqlparser::ast::Value::SingleQuotedString;
use sqlparser::ast::{
    self, BinaryOperator, Expr as AstExpr, Function, FunctionArg, Ident, Interval,
    ObjectName, TimezoneInfo, UnaryOperator,
};
use std::sync::Arc;
use std::vec;

use super::dialect::{DateFieldExtractStyle, IntervalStyle};
use super::Unparser;
use arrow::datatypes::{Decimal128Type, Decimal256Type, DecimalType};
use arrow::util::display::array_value_to_string;
use arrow_array::types::{
    ArrowTemporalType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Date32Array, Date64Array, PrimitiveArray};
use arrow_schema::DataType;
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, plan_err, Column, Result,
    ScalarValue,
};
use datafusion_expr::{
    expr::{Alias, Exists, InList, ScalarFunction, Sort, WindowFunction},
    Between, BinaryExpr, Case, Cast, Expr, GroupingSet, Like, Operator, TryCast,
};

/// Convert a DataFusion [`Expr`] to [`ast::Expr`]
///
/// This function is the opposite of [`SqlToRel::sql_to_expr`] and can be used
/// to, among other things, convert [`Expr`]s to SQL strings. Such strings could
/// be used to pass filters or other expressions to another SQL engine.
///
/// # Errors
///
/// Throws an error if [`Expr`] can not be represented by an [`ast::Expr`]
///
/// # See Also
///
/// * [`Unparser`] for more control over the conversion to SQL
/// * [`plan_to_sql`] for converting a [`LogicalPlan`] to SQL
///
/// # Example
/// ```
/// use datafusion_expr::{col, lit};
/// use datafusion_sql::unparser::expr_to_sql;
/// let expr = col("a").gt(lit(4)); // form an expression `a > 4`
/// let sql = expr_to_sql(&expr).unwrap(); // convert to ast::Expr
/// // use the Display impl to convert to SQL text
/// assert_eq!(sql.to_string(), "(a > 4)")
/// ```
///
/// [`SqlToRel::sql_to_expr`]: crate::planner::SqlToRel::sql_to_expr
/// [`plan_to_sql`]: crate::unparser::plan_to_sql
/// [`LogicalPlan`]: datafusion_expr::logical_plan::LogicalPlan
pub fn expr_to_sql(expr: &Expr) -> Result<ast::Expr> {
    let unparser = Unparser::default();
    unparser.expr_to_sql(expr)
}

pub fn sort_to_sql(sort: &Sort) -> Result<ast::OrderByExpr> {
    let unparser = Unparser::default();
    unparser.sort_to_sql(sort)
}

const LOWEST: &BinaryOperator = &BinaryOperator::Or;
// closest precedence we have to IS operator is BitwiseAnd (any other) in PG docs
// (https://www.postgresql.org/docs/7.2/sql-precedence.html)
const IS: &BinaryOperator = &BinaryOperator::BitwiseAnd;

impl Unparser<'_> {
    pub fn expr_to_sql(&self, expr: &Expr) -> Result<ast::Expr> {
        let mut root_expr = self.expr_to_sql_inner(expr)?;
        if self.pretty {
            root_expr = self.remove_unnecessary_nesting(root_expr, LOWEST, LOWEST);
        }
        Ok(root_expr)
    }

    fn expr_to_sql_inner(&self, expr: &Expr) -> Result<ast::Expr> {
        match expr {
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let list_expr = list
                    .iter()
                    .map(|e| self.expr_to_sql_inner(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast::Expr::InList {
                    expr: Box::new(self.expr_to_sql_inner(expr)?),
                    list: list_expr,
                    negated: *negated,
                })
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let func_name = func.name();

                if let Some(expr) =
                    self.scalar_function_to_sql_overrides(func_name, func, args)
                {
                    return Ok(expr);
                }

                let args = args
                    .iter()
                    .map(|e| {
                        if matches!(
                            e,
                            Expr::Wildcard {
                                qualifier: None,
                                ..
                            }
                        ) {
                            Ok(FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                        } else {
                            self.expr_to_sql_inner(e).map(|e| {
                                FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                            })
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(ast::Expr::Function(Function {
                    name: ast::ObjectName(vec![Ident {
                        value: func_name.to_string(),
                        quote_style: None,
                    }]),
                    args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args,
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    parameters: ast::FunctionArguments::None,
                }))
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let sql_parser_expr = self.expr_to_sql_inner(expr)?;
                let sql_low = self.expr_to_sql_inner(low)?;
                let sql_high = self.expr_to_sql_inner(high)?;
                Ok(ast::Expr::Nested(Box::new(self.between_op_to_sql(
                    sql_parser_expr,
                    *negated,
                    sql_low,
                    sql_high,
                ))))
            }
            Expr::Column(col) => self.col_to_sql(col),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.expr_to_sql_inner(left.as_ref())?;
                let r = self.expr_to_sql_inner(right.as_ref())?;
                let op = self.op_to_sql(op)?;

                Ok(ast::Expr::Nested(Box::new(self.binary_op_to_sql(l, r, op))))
            }
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let conditions = when_then_expr
                    .iter()
                    .map(|(w, _)| self.expr_to_sql_inner(w))
                    .collect::<Result<Vec<_>>>()?;
                let results = when_then_expr
                    .iter()
                    .map(|(_, t)| self.expr_to_sql_inner(t))
                    .collect::<Result<Vec<_>>>()?;
                let operand = match expr.as_ref() {
                    Some(e) => match self.expr_to_sql_inner(e) {
                        Ok(sql_expr) => Some(Box::new(sql_expr)),
                        Err(_) => None,
                    },
                    None => None,
                };
                let else_result = match else_expr.as_ref() {
                    Some(e) => match self.expr_to_sql_inner(e) {
                        Ok(sql_expr) => Some(Box::new(sql_expr)),
                        Err(_) => None,
                    },
                    None => None,
                };

                Ok(ast::Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                })
            }
            Expr::Cast(Cast { expr, data_type }) => {
                let inner_expr = self.expr_to_sql_inner(expr)?;
                match data_type {
                    DataType::Dictionary(_, _) => match inner_expr {
                        // Dictionary values don't need to be cast to other types when rewritten back to sql
                        ast::Expr::Value(_) => Ok(inner_expr),
                        _ => Ok(ast::Expr::Cast {
                            kind: ast::CastKind::Cast,
                            expr: Box::new(inner_expr),
                            data_type: self.arrow_dtype_to_ast_dtype(data_type)?,
                            format: None,
                        }),
                    },
                    _ => Ok(ast::Expr::Cast {
                        kind: ast::CastKind::Cast,
                        expr: Box::new(inner_expr),
                        data_type: self.arrow_dtype_to_ast_dtype(data_type)?,
                        format: None,
                    }),
                }
            }
            Expr::Literal(value) => Ok(self.scalar_to_sql(value)?),
            Expr::Alias(Alias { expr, name: _, .. }) => self.expr_to_sql_inner(expr),
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment: _,
            }) => {
                let func_name = fun.name();

                let args = self.function_args_to_sql(args)?;

                let units = match window_frame.units {
                    datafusion_expr::window_frame::WindowFrameUnits::Rows => {
                        ast::WindowFrameUnits::Rows
                    }
                    datafusion_expr::window_frame::WindowFrameUnits::Range => {
                        ast::WindowFrameUnits::Range
                    }
                    datafusion_expr::window_frame::WindowFrameUnits::Groups => {
                        ast::WindowFrameUnits::Groups
                    }
                };
                let order_by: Vec<ast::OrderByExpr> = order_by
                    .iter()
                    .map(sort_to_sql)
                    .collect::<Result<Vec<_>>>()?;

                let start_bound = self.convert_bound(&window_frame.start_bound)?;
                let end_bound = self.convert_bound(&window_frame.end_bound)?;
                let over = Some(ast::WindowType::WindowSpec(ast::WindowSpec {
                    window_name: None,
                    partition_by: partition_by
                        .iter()
                        .map(|e| self.expr_to_sql_inner(e))
                        .collect::<Result<Vec<_>>>()?,
                    order_by,
                    window_frame: Some(ast::WindowFrame {
                        units,
                        start_bound,
                        end_bound: Option::from(end_bound),
                    }),
                }));

                Ok(ast::Expr::Function(Function {
                    name: ast::ObjectName(vec![Ident {
                        value: func_name.to_string(),
                        quote_style: None,
                    }]),
                    args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args,
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over,
                    within_group: vec![],
                    parameters: ast::FunctionArguments::None,
                }))
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            })
            | Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => Ok(ast::Expr::Like {
                negated: *negated,
                expr: Box::new(self.expr_to_sql_inner(expr)?),
                pattern: Box::new(self.expr_to_sql_inner(pattern)?),
                escape_char: escape_char.map(|c| c.to_string()),
            }),
            Expr::AggregateFunction(agg) => {
                let func_name = agg.func.name();

                let args = self.function_args_to_sql(&agg.args)?;
                let filter = match &agg.filter {
                    Some(filter) => Some(Box::new(self.expr_to_sql_inner(filter)?)),
                    None => None,
                };
                Ok(ast::Expr::Function(Function {
                    name: ast::ObjectName(vec![Ident {
                        value: func_name.to_string(),
                        quote_style: None,
                    }]),
                    args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                        duplicate_treatment: agg
                            .distinct
                            .then_some(ast::DuplicateTreatment::Distinct),
                        args,
                        clauses: vec![],
                    }),
                    filter,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    parameters: ast::FunctionArguments::None,
                }))
            }
            Expr::ScalarSubquery(subq) => {
                let sub_statement = self.plan_to_sql(subq.subquery.as_ref())?;
                let sub_query = if let ast::Statement::Query(inner_query) = sub_statement
                {
                    inner_query
                } else {
                    return plan_err!(
                        "Subquery must be a Query, but found {sub_statement:?}"
                    );
                };
                Ok(ast::Expr::Subquery(sub_query))
            }
            Expr::InSubquery(insubq) => {
                let inexpr = Box::new(self.expr_to_sql_inner(insubq.expr.as_ref())?);
                let sub_statement =
                    self.plan_to_sql(insubq.subquery.subquery.as_ref())?;
                let sub_query = if let ast::Statement::Query(inner_query) = sub_statement
                {
                    inner_query
                } else {
                    return plan_err!(
                        "Subquery must be a Query, but found {sub_statement:?}"
                    );
                };
                Ok(ast::Expr::InSubquery {
                    expr: inexpr,
                    subquery: sub_query,
                    negated: insubq.negated,
                })
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let sub_statement = self.plan_to_sql(subquery.subquery.as_ref())?;
                let sub_query = if let ast::Statement::Query(inner_query) = sub_statement
                {
                    inner_query
                } else {
                    return plan_err!(
                        "Subquery must be a Query, but found {sub_statement:?}"
                    );
                };
                Ok(ast::Expr::Exists {
                    subquery: sub_query,
                    negated: *negated,
                })
            }
            Expr::IsNull(expr) => {
                Ok(ast::Expr::IsNull(Box::new(self.expr_to_sql_inner(expr)?)))
            }
            Expr::IsNotNull(expr) => Ok(ast::Expr::IsNotNull(Box::new(
                self.expr_to_sql_inner(expr)?,
            ))),
            Expr::IsTrue(expr) => {
                Ok(ast::Expr::IsTrue(Box::new(self.expr_to_sql_inner(expr)?)))
            }
            Expr::IsNotTrue(expr) => Ok(ast::Expr::IsNotTrue(Box::new(
                self.expr_to_sql_inner(expr)?,
            ))),
            Expr::IsFalse(expr) => {
                Ok(ast::Expr::IsFalse(Box::new(self.expr_to_sql_inner(expr)?)))
            }
            Expr::IsNotFalse(expr) => Ok(ast::Expr::IsNotFalse(Box::new(
                self.expr_to_sql_inner(expr)?,
            ))),
            Expr::IsUnknown(expr) => Ok(ast::Expr::IsUnknown(Box::new(
                self.expr_to_sql_inner(expr)?,
            ))),
            Expr::IsNotUnknown(expr) => Ok(ast::Expr::IsNotUnknown(Box::new(
                self.expr_to_sql_inner(expr)?,
            ))),
            Expr::Not(expr) => {
                let sql_parser_expr = self.expr_to_sql_inner(expr)?;
                Ok(AstExpr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(sql_parser_expr),
                })
            }
            Expr::Negative(expr) => {
                let sql_parser_expr = self.expr_to_sql_inner(expr)?;
                Ok(AstExpr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(sql_parser_expr),
                })
            }
            Expr::ScalarVariable(_, ids) => {
                if ids.is_empty() {
                    return internal_err!("Not a valid ScalarVariable");
                }

                Ok(if ids.len() == 1 {
                    ast::Expr::Identifier(
                        self.new_ident_without_quote_style(ids[0].to_string()),
                    )
                } else {
                    ast::Expr::CompoundIdentifier(
                        ids.iter()
                            .map(|i| self.new_ident_without_quote_style(i.to_string()))
                            .collect(),
                    )
                })
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                let inner_expr = self.expr_to_sql_inner(expr)?;
                Ok(ast::Expr::Cast {
                    kind: ast::CastKind::TryCast,
                    expr: Box::new(inner_expr),
                    data_type: self.arrow_dtype_to_ast_dtype(data_type)?,
                    format: None,
                })
            }
            // TODO: unparsing wildcard addition options
            Expr::Wildcard { qualifier, .. } => {
                if let Some(qualifier) = qualifier {
                    let idents: Vec<Ident> =
                        qualifier.to_vec().into_iter().map(Ident::new).collect();
                    Ok(ast::Expr::QualifiedWildcard(ObjectName(idents)))
                } else {
                    Ok(ast::Expr::Wildcard)
                }
            }
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::GroupingSets(grouping_sets) => {
                    let expr_ast_sets = grouping_sets
                        .iter()
                        .map(|set| {
                            set.iter()
                                .map(|e| self.expr_to_sql_inner(e))
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(ast::Expr::GroupingSets(expr_ast_sets))
                }
                GroupingSet::Cube(cube) => {
                    let expr_ast_sets = cube
                        .iter()
                        .map(|e| {
                            let sql = self.expr_to_sql_inner(e)?;
                            Ok(vec![sql])
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ast::Expr::Cube(expr_ast_sets))
                }
                GroupingSet::Rollup(rollup) => {
                    let expr_ast_sets: Vec<Vec<AstExpr>> = rollup
                        .iter()
                        .map(|e| {
                            let sql = self.expr_to_sql_inner(e)?;
                            Ok(vec![sql])
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ast::Expr::Rollup(expr_ast_sets))
                }
            },
            Expr::Placeholder(p) => {
                Ok(ast::Expr::Value(ast::Value::Placeholder(p.id.to_string())))
            }
            Expr::OuterReferenceColumn(_, col) => self.col_to_sql(col),
            Expr::Unnest(_) => not_impl_err!("Unsupported Expr conversion: {expr:?}"),
        }
    }

    pub fn sort_to_sql(&self, sort: &Sort) -> Result<ast::OrderByExpr> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let sql_parser_expr = self.expr_to_sql(expr)?;

        let nulls_first = if self.dialect.supports_nulls_first_in_sort() {
            Some(*nulls_first)
        } else {
            None
        };

        Ok(ast::OrderByExpr {
            expr: sql_parser_expr,
            asc: Some(*asc),
            nulls_first,
            with_fill: None,
        })
    }

    fn scalar_function_to_sql_overrides(
        &self,
        func_name: &str,
        _func: &Arc<ScalarUDF>,
        args: &[Expr],
    ) -> Option<ast::Expr> {
        if func_name.to_lowercase() == "date_part" {
            match (self.dialect.date_field_extract_style(), args.len()) {
                (DateFieldExtractStyle::Extract, 2) => {
                    let date_expr = self.expr_to_sql(&args[1]).ok()?;

                    if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &args[0] {
                        let field = match field.to_lowercase().as_str() {
                            "year" => ast::DateTimeField::Year,
                            "month" => ast::DateTimeField::Month,
                            "day" => ast::DateTimeField::Day,
                            "hour" => ast::DateTimeField::Hour,
                            "minute" => ast::DateTimeField::Minute,
                            "second" => ast::DateTimeField::Second,
                            _ => return None,
                        };

                        return Some(ast::Expr::Extract {
                            field,
                            expr: Box::new(date_expr),
                            syntax: ast::ExtractSyntax::From,
                        });
                    }
                }
                (DateFieldExtractStyle::Strftime, 2) => {
                    let column = self.expr_to_sql(&args[1]).ok()?;

                    if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &args[0] {
                        let field = match field.to_lowercase().as_str() {
                            "year" => "%Y",
                            "month" => "%m",
                            "day" => "%d",
                            "hour" => "%H",
                            "minute" => "%M",
                            "second" => "%S",
                            _ => return None,
                        };

                        return Some(ast::Expr::Function(ast::Function {
                            name: ast::ObjectName(vec![ast::Ident {
                                value: "strftime".to_string(),
                                quote_style: None,
                            }]),
                            args: ast::FunctionArguments::List(
                                ast::FunctionArgumentList {
                                    duplicate_treatment: None,
                                    args: vec![
                                        ast::FunctionArg::Unnamed(
                                            ast::FunctionArgExpr::Expr(ast::Expr::Value(
                                                ast::Value::SingleQuotedString(
                                                    field.to_string(),
                                                ),
                                            )),
                                        ),
                                        ast::FunctionArg::Unnamed(
                                            ast::FunctionArgExpr::Expr(column),
                                        ),
                                    ],
                                    clauses: vec![],
                                },
                            ),
                            filter: None,
                            null_treatment: None,
                            over: None,
                            within_group: vec![],
                            parameters: ast::FunctionArguments::None,
                        }));
                    }
                }
                _ => {} // no overrides for DateFieldExtractStyle::DatePart, because it's already a date_part
            }
        }

        None
    }

    fn ast_type_for_date64_in_cast(&self) -> ast::DataType {
        if self.dialect.use_timestamp_for_date64() {
            ast::DataType::Timestamp(None, ast::TimezoneInfo::None)
        } else {
            ast::DataType::Datetime(None)
        }
    }

    fn col_to_sql(&self, col: &Column) -> Result<ast::Expr> {
        if let Some(table_ref) = &col.relation {
            let mut id = table_ref.to_vec();
            id.push(col.name.to_string());
            return Ok(ast::Expr::CompoundIdentifier(
                id.iter()
                    .map(|i| self.new_ident_quoted_if_needs(i.to_string()))
                    .collect(),
            ));
        }
        Ok(ast::Expr::Identifier(
            self.new_ident_quoted_if_needs(col.name.to_string()),
        ))
    }

    fn convert_bound(
        &self,
        bound: &datafusion_expr::window_frame::WindowFrameBound,
    ) -> Result<ast::WindowFrameBound> {
        match bound {
            datafusion_expr::window_frame::WindowFrameBound::Preceding(val) => {
                Ok(ast::WindowFrameBound::Preceding({
                    let val = self.scalar_to_sql(val)?;
                    if let ast::Expr::Value(ast::Value::Null) = &val {
                        None
                    } else {
                        Some(Box::new(val))
                    }
                }))
            }
            datafusion_expr::window_frame::WindowFrameBound::Following(val) => {
                Ok(ast::WindowFrameBound::Following({
                    let val = self.scalar_to_sql(val)?;
                    if let ast::Expr::Value(ast::Value::Null) = &val {
                        None
                    } else {
                        Some(Box::new(val))
                    }
                }))
            }
            datafusion_expr::window_frame::WindowFrameBound::CurrentRow => {
                Ok(ast::WindowFrameBound::CurrentRow)
            }
        }
    }

    fn function_args_to_sql(&self, args: &[Expr]) -> Result<Vec<ast::FunctionArg>> {
        args.iter()
            .map(|e| {
                if matches!(
                    e,
                    Expr::Wildcard {
                        qualifier: None,
                        ..
                    }
                ) {
                    Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                } else {
                    self.expr_to_sql(e)
                        .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    /// This function can create an identifier with or without quotes based on the dialect rules
    pub(super) fn new_ident_quoted_if_needs(&self, ident: String) -> ast::Ident {
        let quote_style = self.dialect.identifier_quote_style(&ident);
        ast::Ident {
            value: ident,
            quote_style,
        }
    }

    pub(super) fn new_ident_without_quote_style(&self, str: String) -> ast::Ident {
        ast::Ident {
            value: str,
            quote_style: None,
        }
    }

    pub(super) fn binary_op_to_sql(
        &self,
        lhs: ast::Expr,
        rhs: ast::Expr,
        op: ast::BinaryOperator,
    ) -> ast::Expr {
        ast::Expr::BinaryOp {
            left: Box::new(lhs),
            op,
            right: Box::new(rhs),
        }
    }

    /// Given an expression of the form `((a + b) * (c * d))`,
    /// the parenthesing is redundant if the precedence of the nested expression is already higher
    /// than the surrounding operators' precedence. The above expression would become
    /// `(a + b) * c * d`.
    ///
    /// Also note that when fetching the precedence of a nested expression, we ignore other nested
    /// expressions, so precedence of expr `(a * (b + c))` equals `*` and not `+`.
    fn remove_unnecessary_nesting(
        &self,
        expr: ast::Expr,
        left_op: &BinaryOperator,
        right_op: &BinaryOperator,
    ) -> ast::Expr {
        match expr {
            ast::Expr::Nested(nested) => {
                let surrounding_precedence = self
                    .sql_op_precedence(left_op)
                    .max(self.sql_op_precedence(right_op));

                let inner_precedence = self.inner_precedence(&nested);

                let not_associative =
                    matches!(left_op, BinaryOperator::Minus | BinaryOperator::Divide);

                if inner_precedence == surrounding_precedence && not_associative {
                    ast::Expr::Nested(Box::new(
                        self.remove_unnecessary_nesting(*nested, LOWEST, LOWEST),
                    ))
                } else if inner_precedence >= surrounding_precedence {
                    self.remove_unnecessary_nesting(*nested, left_op, right_op)
                } else {
                    ast::Expr::Nested(Box::new(
                        self.remove_unnecessary_nesting(*nested, LOWEST, LOWEST),
                    ))
                }
            }
            ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
                left: Box::new(self.remove_unnecessary_nesting(*left, left_op, &op)),
                right: Box::new(self.remove_unnecessary_nesting(*right, &op, right_op)),
                op,
            },
            ast::Expr::IsTrue(expr) => ast::Expr::IsTrue(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsNotTrue(expr) => ast::Expr::IsNotTrue(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsFalse(expr) => ast::Expr::IsFalse(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsNotFalse(expr) => ast::Expr::IsNotFalse(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsNull(expr) => ast::Expr::IsNull(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsNotNull(expr) => ast::Expr::IsNotNull(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsUnknown(expr) => ast::Expr::IsUnknown(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            ast::Expr::IsNotUnknown(expr) => ast::Expr::IsNotUnknown(Box::new(
                self.remove_unnecessary_nesting(*expr, left_op, IS),
            )),
            _ => expr,
        }
    }

    fn inner_precedence(&self, expr: &ast::Expr) -> u8 {
        match expr {
            ast::Expr::Nested(_) | ast::Expr::Identifier(_) | ast::Expr::Value(_) => 100,
            ast::Expr::BinaryOp { op, .. } => self.sql_op_precedence(op),
            // closest precedence we currently have to Between is PGLikeMatch
            // (https://www.postgresql.org/docs/7.2/sql-precedence.html)
            ast::Expr::Between { .. } => {
                self.sql_op_precedence(&ast::BinaryOperator::PGLikeMatch)
            }
            _ => 0,
        }
    }

    pub(super) fn between_op_to_sql(
        &self,
        expr: ast::Expr,
        negated: bool,
        low: ast::Expr,
        high: ast::Expr,
    ) -> ast::Expr {
        ast::Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    fn sql_op_precedence(&self, op: &BinaryOperator) -> u8 {
        match self.sql_to_op(op) {
            Ok(op) => op.precedence(),
            Err(_) => 0,
        }
    }

    fn sql_to_op(&self, op: &BinaryOperator) -> Result<Operator> {
        match op {
            ast::BinaryOperator::Eq => Ok(Operator::Eq),
            ast::BinaryOperator::NotEq => Ok(Operator::NotEq),
            ast::BinaryOperator::Lt => Ok(Operator::Lt),
            ast::BinaryOperator::LtEq => Ok(Operator::LtEq),
            ast::BinaryOperator::Gt => Ok(Operator::Gt),
            ast::BinaryOperator::GtEq => Ok(Operator::GtEq),
            ast::BinaryOperator::Plus => Ok(Operator::Plus),
            ast::BinaryOperator::Minus => Ok(Operator::Minus),
            ast::BinaryOperator::Multiply => Ok(Operator::Multiply),
            ast::BinaryOperator::Divide => Ok(Operator::Divide),
            ast::BinaryOperator::Modulo => Ok(Operator::Modulo),
            ast::BinaryOperator::And => Ok(Operator::And),
            ast::BinaryOperator::Or => Ok(Operator::Or),
            ast::BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            ast::BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            ast::BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            ast::BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            ast::BinaryOperator::PGILikeMatch => Ok(Operator::ILikeMatch),
            ast::BinaryOperator::PGNotLikeMatch => Ok(Operator::NotLikeMatch),
            ast::BinaryOperator::PGLikeMatch => Ok(Operator::LikeMatch),
            ast::BinaryOperator::PGNotILikeMatch => Ok(Operator::NotILikeMatch),
            ast::BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            ast::BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            ast::BinaryOperator::BitwiseXor => Ok(Operator::BitwiseXor),
            ast::BinaryOperator::PGBitwiseShiftRight => Ok(Operator::BitwiseShiftRight),
            ast::BinaryOperator::PGBitwiseShiftLeft => Ok(Operator::BitwiseShiftLeft),
            ast::BinaryOperator::StringConcat => Ok(Operator::StringConcat),
            ast::BinaryOperator::AtArrow => Ok(Operator::AtArrow),
            ast::BinaryOperator::ArrowAt => Ok(Operator::ArrowAt),
            _ => not_impl_err!("unsupported operation: {op:?}"),
        }
    }

    fn op_to_sql(&self, op: &Operator) -> Result<ast::BinaryOperator> {
        match op {
            Operator::Eq => Ok(ast::BinaryOperator::Eq),
            Operator::NotEq => Ok(ast::BinaryOperator::NotEq),
            Operator::Lt => Ok(ast::BinaryOperator::Lt),
            Operator::LtEq => Ok(ast::BinaryOperator::LtEq),
            Operator::Gt => Ok(ast::BinaryOperator::Gt),
            Operator::GtEq => Ok(ast::BinaryOperator::GtEq),
            Operator::Plus => Ok(ast::BinaryOperator::Plus),
            Operator::Minus => Ok(ast::BinaryOperator::Minus),
            Operator::Multiply => Ok(ast::BinaryOperator::Multiply),
            Operator::Divide => Ok(ast::BinaryOperator::Divide),
            Operator::Modulo => Ok(ast::BinaryOperator::Modulo),
            Operator::And => Ok(ast::BinaryOperator::And),
            Operator::Or => Ok(ast::BinaryOperator::Or),
            Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::RegexMatch => Ok(ast::BinaryOperator::PGRegexMatch),
            Operator::RegexIMatch => Ok(ast::BinaryOperator::PGRegexIMatch),
            Operator::RegexNotMatch => Ok(ast::BinaryOperator::PGRegexNotMatch),
            Operator::RegexNotIMatch => Ok(ast::BinaryOperator::PGRegexNotIMatch),
            Operator::ILikeMatch => Ok(ast::BinaryOperator::PGILikeMatch),
            Operator::NotLikeMatch => Ok(ast::BinaryOperator::PGNotLikeMatch),
            Operator::LikeMatch => Ok(ast::BinaryOperator::PGLikeMatch),
            Operator::NotILikeMatch => Ok(ast::BinaryOperator::PGNotILikeMatch),
            Operator::BitwiseAnd => Ok(ast::BinaryOperator::BitwiseAnd),
            Operator::BitwiseOr => Ok(ast::BinaryOperator::BitwiseOr),
            Operator::BitwiseXor => Ok(ast::BinaryOperator::BitwiseXor),
            Operator::BitwiseShiftRight => Ok(ast::BinaryOperator::PGBitwiseShiftRight),
            Operator::BitwiseShiftLeft => Ok(ast::BinaryOperator::PGBitwiseShiftLeft),
            Operator::StringConcat => Ok(ast::BinaryOperator::StringConcat),
            Operator::AtArrow => not_impl_err!("unsupported operation: {op:?}"),
            Operator::ArrowAt => not_impl_err!("unsupported operation: {op:?}"),
        }
    }

    fn handle_timestamp<T: ArrowTemporalType>(
        &self,
        v: &ScalarValue,
        tz: &Option<Arc<str>>,
    ) -> Result<ast::Expr>
    where
        i64: From<T::Native>,
    {
        let ts = if let Some(tz) = tz {
            v.to_array()?
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or(internal_datafusion_err!(
                    "Failed to downcast type {v:?} to arrow array"
                ))?
                .value_as_datetime_with_tz(0, tz.parse()?)
                .ok_or(internal_datafusion_err!(
                    "Unable to convert {v:?} to DateTime"
                ))?
                .to_string()
        } else {
            v.to_array()?
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or(internal_datafusion_err!(
                    "Failed to downcast type {v:?} to arrow array"
                ))?
                .value_as_datetime(0)
                .ok_or(internal_datafusion_err!(
                    "Unable to convert {v:?} to DateTime"
                ))?
                .to_string()
        };
        Ok(ast::Expr::Cast {
            kind: ast::CastKind::Cast,
            expr: Box::new(ast::Expr::Value(SingleQuotedString(ts))),
            data_type: ast::DataType::Timestamp(None, TimezoneInfo::None),
            format: None,
        })
    }

    fn handle_time<T: ArrowTemporalType>(&self, v: &ScalarValue) -> Result<ast::Expr>
    where
        i64: From<T::Native>,
    {
        let time = v
            .to_array()?
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or(internal_datafusion_err!(
                "Failed to downcast type {v:?} to arrow array"
            ))?
            .value_as_time(0)
            .ok_or(internal_datafusion_err!("Unable to convert {v:?} to Time"))?
            .to_string();
        Ok(ast::Expr::Cast {
            kind: ast::CastKind::Cast,
            expr: Box::new(ast::Expr::Value(SingleQuotedString(time))),
            data_type: ast::DataType::Time(None, TimezoneInfo::None),
            format: None,
        })
    }

    /// DataFusion ScalarValues sometimes require a ast::Expr to construct.
    /// For example ScalarValue::Date32(d) corresponds to the ast::Expr CAST('datestr' as DATE)
    fn scalar_to_sql(&self, v: &ScalarValue) -> Result<ast::Expr> {
        match v {
            ScalarValue::Null => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Boolean(Some(b)) => {
                Ok(ast::Expr::Value(ast::Value::Boolean(b.to_owned())))
            }
            ScalarValue::Boolean(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Float16(Some(f)) => {
                Ok(ast::Expr::Value(ast::Value::Number(f.to_string(), false)))
            }
            ScalarValue::Float16(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Float32(Some(f)) => {
                let f_val = match f.fract() {
                    0.0 => format!("{:.1}", f),
                    _ => format!("{}", f),
                };
                Ok(ast::Expr::Value(ast::Value::Number(f_val, false)))
            }
            ScalarValue::Float32(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Float64(Some(f)) => {
                let f_val = match f.fract() {
                    0.0 => format!("{:.1}", f),
                    _ => format!("{}", f),
                };
                Ok(ast::Expr::Value(ast::Value::Number(f_val, false)))
            }
            ScalarValue::Float64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Decimal128(Some(value), precision, scale) => {
                Ok(ast::Expr::Value(ast::Value::Number(
                    Decimal128Type::format_decimal(*value, *precision, *scale),
                    false,
                )))
            }
            ScalarValue::Decimal128(None, ..) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Decimal256(Some(value), precision, scale) => {
                Ok(ast::Expr::Value(ast::Value::Number(
                    Decimal256Type::format_decimal(*value, *precision, *scale),
                    false,
                )))
            }
            ScalarValue::Decimal256(None, ..) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Int8(Some(i)) => {
                Ok(ast::Expr::Value(ast::Value::Number(i.to_string(), false)))
            }
            ScalarValue::Int8(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Int16(Some(i)) => {
                Ok(ast::Expr::Value(ast::Value::Number(i.to_string(), false)))
            }
            ScalarValue::Int16(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Int32(Some(i)) => {
                Ok(ast::Expr::Value(ast::Value::Number(i.to_string(), false)))
            }
            ScalarValue::Int32(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Int64(Some(i)) => {
                Ok(ast::Expr::Value(ast::Value::Number(i.to_string(), false)))
            }
            ScalarValue::Int64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::UInt8(Some(ui)) => {
                Ok(ast::Expr::Value(ast::Value::Number(ui.to_string(), false)))
            }
            ScalarValue::UInt8(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::UInt16(Some(ui)) => {
                Ok(ast::Expr::Value(ast::Value::Number(ui.to_string(), false)))
            }
            ScalarValue::UInt16(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::UInt32(Some(ui)) => {
                Ok(ast::Expr::Value(ast::Value::Number(ui.to_string(), false)))
            }
            ScalarValue::UInt32(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::UInt64(Some(ui)) => {
                Ok(ast::Expr::Value(ast::Value::Number(ui.to_string(), false)))
            }
            ScalarValue::UInt64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Utf8(Some(str)) => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(str.to_string()),
            )),
            ScalarValue::Utf8(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Utf8View(Some(str)) => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(str.to_string()),
            )),
            ScalarValue::Utf8View(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::LargeUtf8(Some(str)) => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(str.to_string()),
            )),
            ScalarValue::LargeUtf8(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Binary(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::BinaryView(Some(_)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::BinaryView(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::FixedSizeBinary(..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::LargeBinary(Some(_)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::LargeBinary(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::FixedSizeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::List(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::LargeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Date32(Some(_)) => {
                let date = v
                    .to_array()?
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or(internal_datafusion_err!(
                        "Unable to downcast to Date32 from Date32 scalar"
                    ))?
                    .value_as_date(0)
                    .ok_or(internal_datafusion_err!(
                        "Unable to convert Date32 to NaiveDate"
                    ))?;

                Ok(ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        date.to_string(),
                    ))),
                    data_type: ast::DataType::Date,
                    format: None,
                })
            }
            ScalarValue::Date32(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Date64(Some(_)) => {
                let datetime = v
                    .to_array()?
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .ok_or(internal_datafusion_err!(
                        "Unable to downcast to Date64 from Date64 scalar"
                    ))?
                    .value_as_datetime(0)
                    .ok_or(internal_datafusion_err!(
                        "Unable to convert Date64 to NaiveDateTime"
                    ))?;

                Ok(ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        datetime.to_string(),
                    ))),
                    data_type: self.ast_type_for_date64_in_cast(),
                    format: None,
                })
            }
            ScalarValue::Date64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Time32Second(Some(_t)) => {
                self.handle_time::<Time32SecondType>(v)
            }
            ScalarValue::Time32Second(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Time32Millisecond(Some(_t)) => {
                self.handle_time::<Time32MillisecondType>(v)
            }
            ScalarValue::Time32Millisecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::Time64Microsecond(Some(_t)) => {
                self.handle_time::<Time64MicrosecondType>(v)
            }
            ScalarValue::Time64Microsecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::Time64Nanosecond(Some(_t)) => {
                self.handle_time::<Time64NanosecondType>(v)
            }
            ScalarValue::Time64Nanosecond(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::TimestampSecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampSecondType>(v, tz)
            }
            ScalarValue::TimestampSecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampMillisecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampMillisecondType>(v, tz)
            }
            ScalarValue::TimestampMillisecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampMicrosecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampMicrosecondType>(v, tz)
            }
            ScalarValue::TimestampMicrosecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampNanosecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampNanosecondType>(v, tz)
            }
            ScalarValue::TimestampNanosecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::IntervalYearMonth(Some(_))
            | ScalarValue::IntervalDayTime(Some(_))
            | ScalarValue::IntervalMonthDayNano(Some(_)) => {
                self.interval_scalar_to_sql(v)
            }
            ScalarValue::IntervalYearMonth(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::IntervalDayTime(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::IntervalMonthDayNano(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::DurationSecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationSecond(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::DurationMillisecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMillisecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::DurationMicrosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMicrosecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::DurationNanosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationNanosecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::Struct(_) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Map(_) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Union(..) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
        }
    }

    /// MySQL requires INTERVAL sql to be in the format: INTERVAL 1 YEAR + INTERVAL 1 MONTH + INTERVAL 1 DAY etc
    /// `<https://dev.mysql.com/doc/refman/8.4/en/expressions.html#temporal-intervals>`
    /// Interval sequence can't be wrapped in brackets - (INTERVAL 1 YEAR + INTERVAL 1 MONTH ...) so we need to generate
    /// a single INTERVAL expression so it works correct for interval substraction cases
    /// MySQL supports the DAY_MICROSECOND unit type (format is DAYS HOURS:MINUTES:SECONDS.MICROSECONDS), but it is not supported by sqlparser
    /// so we calculate the best single interval to represent the provided duration
    fn interval_to_mysql_expr(
        &self,
        months: i32,
        days: i32,
        microseconds: i64,
    ) -> Result<ast::Expr> {
        // MONTH only
        if months != 0 && days == 0 && microseconds == 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    months.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Month),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        } else if months != 0 {
            return not_impl_err!("Unsupported Interval scalar with both Month and DayTime for IntervalStyle::MySQL");
        }

        // DAY only
        if microseconds == 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    days.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Day),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        }

        // calculate the best single interval to represent the provided days and microseconds

        let microseconds = microseconds + (days as i64 * 24 * 60 * 60 * 1_000_000);

        if microseconds % 1_000_000 != 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    microseconds.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Microsecond),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        }

        let secs = microseconds / 1_000_000;

        if secs % 60 != 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    secs.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Second),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        }

        let mins = secs / 60;

        if mins % 60 != 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    mins.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Minute),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        }

        let hours = mins / 60;

        if hours % 24 != 0 {
            let interval = Interval {
                value: Box::new(ast::Expr::Value(ast::Value::Number(
                    hours.to_string(),
                    false,
                ))),
                leading_field: Some(ast::DateTimeField::Hour),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            return Ok(ast::Expr::Interval(interval));
        }

        let days = hours / 24;

        let interval = Interval {
            value: Box::new(ast::Expr::Value(ast::Value::Number(
                days.to_string(),
                false,
            ))),
            leading_field: Some(ast::DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        };
        Ok(ast::Expr::Interval(interval))
    }

    fn interval_scalar_to_sql(&self, v: &ScalarValue) -> Result<ast::Expr> {
        match self.dialect.interval_style() {
            IntervalStyle::PostgresVerbose => {
                let wrap_array = v.to_array()?;
                let Some(result) = array_value_to_string(&wrap_array, 0).ok() else {
                    return internal_err!(
                        "Unable to convert interval scalar value to string"
                    );
                };
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(SingleQuotedString(
                        result.to_uppercase(),
                    ))),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                };
                Ok(ast::Expr::Interval(interval))
            }
            // If the interval standard is SQLStandard, implement a simple unparse logic
            IntervalStyle::SQLStandard => match v {
                ScalarValue::IntervalYearMonth(Some(v)) => {
                    let interval = Interval {
                        value: Box::new(ast::Expr::Value(
                            ast::Value::SingleQuotedString(v.to_string()),
                        )),
                        leading_field: Some(ast::DateTimeField::Month),
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None,
                    };
                    Ok(ast::Expr::Interval(interval))
                }
                ScalarValue::IntervalDayTime(Some(v)) => {
                    let days = v.days;
                    let secs = v.milliseconds / 1_000;
                    let mins = secs / 60;
                    let hours = mins / 60;

                    let secs = secs - (mins * 60);
                    let mins = mins - (hours * 60);

                    let millis = v.milliseconds % 1_000;
                    let interval = Interval {
                        value: Box::new(ast::Expr::Value(
                            ast::Value::SingleQuotedString(format!(
                                "{days} {hours}:{mins}:{secs}.{millis:3}"
                            )),
                        )),
                        leading_field: Some(ast::DateTimeField::Day),
                        leading_precision: None,
                        last_field: Some(ast::DateTimeField::Second),
                        fractional_seconds_precision: None,
                    };
                    Ok(ast::Expr::Interval(interval))
                }
                ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    if v.months >= 0 && v.days == 0 && v.nanoseconds == 0 {
                        let interval = Interval {
                            value: Box::new(ast::Expr::Value(
                                ast::Value::SingleQuotedString(v.months.to_string()),
                            )),
                            leading_field: Some(ast::DateTimeField::Month),
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        };
                        Ok(ast::Expr::Interval(interval))
                    } else if v.months == 0 && v.nanoseconds % 1_000_000 == 0 {
                        let days = v.days;
                        let secs = v.nanoseconds / 1_000_000_000;
                        let mins = secs / 60;
                        let hours = mins / 60;

                        let secs = secs - (mins * 60);
                        let mins = mins - (hours * 60);

                        let millis = (v.nanoseconds % 1_000_000_000) / 1_000_000;

                        let interval = Interval {
                            value: Box::new(ast::Expr::Value(
                                ast::Value::SingleQuotedString(format!(
                                    "{days} {hours}:{mins}:{secs}.{millis:03}"
                                )),
                            )),
                            leading_field: Some(ast::DateTimeField::Day),
                            leading_precision: None,
                            last_field: Some(ast::DateTimeField::Second),
                            fractional_seconds_precision: None,
                        };
                        Ok(ast::Expr::Interval(interval))
                    } else {
                        not_impl_err!("Unsupported IntervalMonthDayNano scalar with both Month and DayTime for IntervalStyle::SQLStandard")
                    }
                }
                _ => not_impl_err!(
                    "Unsupported ScalarValue for Interval conversion: {v:?}"
                ),
            },
            IntervalStyle::MySQL => match v {
                ScalarValue::IntervalYearMonth(Some(v)) => {
                    self.interval_to_mysql_expr(*v, 0, 0)
                }
                ScalarValue::IntervalDayTime(Some(v)) => {
                    self.interval_to_mysql_expr(0, v.days, v.milliseconds as i64 * 1_000)
                }
                ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    if v.nanoseconds % 1_000 != 0 {
                        return not_impl_err!(
                            "Unsupported IntervalMonthDayNano scalar with nanoseconds precision for IntervalStyle::MySQL"
                        );
                    }
                    self.interval_to_mysql_expr(v.months, v.days, v.nanoseconds / 1_000)
                }
                _ => not_impl_err!(
                    "Unsupported ScalarValue for Interval conversion: {v:?}"
                ),
            },
        }
    }

    fn arrow_dtype_to_ast_dtype(&self, data_type: &DataType) -> Result<ast::DataType> {
        match data_type {
            DataType::Null => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Boolean => Ok(ast::DataType::Bool),
            DataType::Int8 => Ok(ast::DataType::TinyInt(None)),
            DataType::Int16 => Ok(ast::DataType::SmallInt(None)),
            DataType::Int32 => Ok(ast::DataType::Integer(None)),
            DataType::Int64 => Ok(self.dialect.int64_cast_dtype()),
            DataType::UInt8 => Ok(ast::DataType::UnsignedTinyInt(None)),
            DataType::UInt16 => Ok(ast::DataType::UnsignedSmallInt(None)),
            DataType::UInt32 => Ok(ast::DataType::UnsignedInteger(None)),
            DataType::UInt64 => Ok(ast::DataType::UnsignedBigInt(None)),
            DataType::Float16 => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Float32 => Ok(ast::DataType::Float(None)),
            DataType::Float64 => Ok(self.dialect.float64_ast_dtype()),
            DataType::Timestamp(time_unit, tz) => {
                Ok(self.dialect.timestamp_cast_dtype(time_unit, tz))
            }
            DataType::Date32 => Ok(self.dialect.date32_cast_dtype()),
            DataType::Date64 => Ok(self.ast_type_for_date64_in_cast()),
            DataType::Time32(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Time64(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Duration(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Interval(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Binary => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::FixedSizeBinary(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::LargeBinary => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::BinaryView => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Utf8 => Ok(self.dialect.utf8_cast_dtype()),
            DataType::LargeUtf8 => Ok(self.dialect.large_utf8_cast_dtype()),
            DataType::Utf8View => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::List(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::FixedSizeList(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::LargeList(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::ListView(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::LargeListView(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Struct(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Union(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Dictionary(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => {
                let mut new_precision = *precision as u64;
                let mut new_scale = *scale as u64;
                if *scale < 0 {
                    new_precision = (*precision as i16 - *scale as i16) as u64;
                    new_scale = 0
                }

                Ok(ast::DataType::Decimal(
                    ast::ExactNumberInfo::PrecisionAndScale(new_precision, new_scale),
                ))
            }
            DataType::Map(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::RunEndEncoded(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, Sub};
    use std::{any::Any, sync::Arc, vec};

    use arrow::datatypes::TimeUnit;
    use arrow::datatypes::{Field, Schema};
    use arrow_schema::DataType::Int8;
    use ast::ObjectName;
    use datafusion_common::TableReference;
    use datafusion_expr::expr::WildcardOptions;
    use datafusion_expr::{
        case, col, cube, exists, grouping_set, interval_datetime_lit,
        interval_year_month_lit, lit, not, not_exists, out_ref_col, placeholder, rollup,
        table_scan, try_cast, when, wildcard, ColumnarValue, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility, WindowFrame, WindowFunctionDefinition,
    };
    use datafusion_expr::{interval_month_day_nano_lit, ExprFunctionExt};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::sum;
    use datafusion_functions_window::row_number::row_number_udwf;

    use crate::unparser::dialect::{CustomDialect, CustomDialectBuilder};

    use super::*;

    /// Mocked UDF
    #[derive(Debug)]
    struct DummyUDF {
        signature: Signature,
    }

    impl DummyUDF {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for DummyUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "dummy_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unimplemented!("DummyUDF::invoke")
        }
    }
    // See sql::tests for E2E tests.

    #[test]
    fn expr_to_sql_ok() -> Result<()> {
        let dummy_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let dummy_logical_plan = table_scan(Some("t"), &dummy_schema, None)?
            .project(vec![Expr::Wildcard {
                qualifier: None,
                options: WildcardOptions::default(),
            }])?
            .filter(col("a").eq(lit(1)))?
            .build()?;

        let tests: Vec<(Expr, &str)> = vec![
            ((col("a") + col("b")).gt(lit(4)), r#"((a + b) > 4)"#),
            (
                Expr::Column(Column {
                    relation: Some(TableReference::partial("a", "b")),
                    name: "c".to_string(),
                })
                .gt(lit(4)),
                r#"(a.b.c > 4)"#,
            ),
            (
                case(col("a"))
                    .when(lit(1), lit(true))
                    .when(lit(0), lit(false))
                    .otherwise(lit(ScalarValue::Null))?,
                r#"CASE a WHEN 1 THEN true WHEN 0 THEN false ELSE NULL END"#,
            ),
            (
                when(col("a").is_null(), lit(true)).otherwise(lit(false))?,
                r#"CASE WHEN a IS NULL THEN true ELSE false END"#,
            ),
            (
                when(col("a").is_not_null(), lit(true)).otherwise(lit(false))?,
                r#"CASE WHEN a IS NOT NULL THEN true ELSE false END"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::Date64,
                }),
                r#"CAST(a AS DATETIME)"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::Timestamp(
                        TimeUnit::Nanosecond,
                        Some("+08:00".into()),
                    ),
                }),
                r#"CAST(a AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
                }),
                r#"CAST(a AS TIMESTAMP)"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::UInt32,
                }),
                r#"CAST(a AS INTEGER UNSIGNED)"#,
            ),
            (
                col("a").in_list(vec![lit(1), lit(2), lit(3)], false),
                r#"a IN (1, 2, 3)"#,
            ),
            (
                col("a").in_list(vec![lit(1), lit(2), lit(3)], true),
                r#"a NOT IN (1, 2, 3)"#,
            ),
            (
                ScalarUDF::new_from_impl(DummyUDF::new()).call(vec![col("a"), col("b")]),
                r#"dummy_udf(a, b)"#,
            ),
            (
                ScalarUDF::new_from_impl(DummyUDF::new())
                    .call(vec![col("a"), col("b")])
                    .is_null(),
                r#"dummy_udf(a, b) IS NULL"#,
            ),
            (
                ScalarUDF::new_from_impl(DummyUDF::new())
                    .call(vec![col("a"), col("b")])
                    .is_not_null(),
                r#"dummy_udf(a, b) IS NOT NULL"#,
            ),
            (
                Expr::Like(Like {
                    negated: true,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: true,
                }),
                r#"a NOT LIKE 'foo' ESCAPE 'o'"#,
            ),
            (
                Expr::SimilarTo(Like {
                    negated: false,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: true,
                }),
                r#"a LIKE 'foo' ESCAPE 'o'"#,
            ),
            (
                Expr::Literal(ScalarValue::Date64(Some(0))),
                r#"CAST('1970-01-01 00:00:00' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date64(Some(10000))),
                r#"CAST('1970-01-01 00:00:10' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date64(Some(-10000))),
                r#"CAST('1969-12-31 23:59:50' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(0))),
                r#"CAST('1970-01-01' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(10))),
                r#"CAST('1970-01-11' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(-1))),
                r#"CAST('1969-12-31' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampSecond(Some(10001), None)),
                r#"CAST('1970-01-01 02:46:41' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampSecond(
                    Some(10001),
                    Some("+08:00".into()),
                )),
                r#"CAST('1970-01-01 10:46:41 +08:00' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMillisecond(Some(10001), None)),
                r#"CAST('1970-01-01 00:00:10.001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(10001),
                    Some("+08:00".into()),
                )),
                r#"CAST('1970-01-01 08:00:10.001 +08:00' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMicrosecond(Some(10001), None)),
                r#"CAST('1970-01-01 00:00:00.010001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMicrosecond(
                    Some(10001),
                    Some("+08:00".into()),
                )),
                r#"CAST('1970-01-01 08:00:00.010001 +08:00' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampNanosecond(Some(10001), None)),
                r#"CAST('1970-01-01 00:00:00.000010001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampNanosecond(
                    Some(10001),
                    Some("+08:00".into()),
                )),
                r#"CAST('1970-01-01 08:00:00.000010001 +08:00' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time32Second(Some(10001))),
                r#"CAST('02:46:41' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time32Millisecond(Some(10001))),
                r#"CAST('00:00:10.001' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time64Microsecond(Some(10001))),
                r#"CAST('00:00:00.010001' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time64Nanosecond(Some(10001))),
                r#"CAST('00:00:00.000010001' AS TIME)"#,
            ),
            (sum(col("a")), r#"sum(a)"#),
            (
                count_udaf()
                    .call(vec![Expr::Wildcard {
                        qualifier: None,
                        options: WildcardOptions::default(),
                    }])
                    .distinct()
                    .build()
                    .unwrap(),
                "count(DISTINCT *)",
            ),
            (
                count_udaf()
                    .call(vec![Expr::Wildcard {
                        qualifier: None,
                        options: WildcardOptions::default(),
                    }])
                    .filter(lit(true))
                    .build()
                    .unwrap(),
                "count(*) FILTER (WHERE true)",
            ),
            (
                Expr::WindowFunction(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                    args: vec![col("col")],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: WindowFrame::new(None),
                    null_treatment: None,
                }),
                r#"row_number(col) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"#,
            ),
            (
                Expr::WindowFunction(WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    args: vec![wildcard()],
                    partition_by: vec![],
                    order_by: vec![Sort::new(col("a"), false, true)],
                    window_frame: WindowFrame::new_bounds(
                        datafusion_expr::WindowFrameUnits::Range,
                        datafusion_expr::WindowFrameBound::Preceding(
                            ScalarValue::UInt32(Some(6)),
                        ),
                        datafusion_expr::WindowFrameBound::Following(
                            ScalarValue::UInt32(Some(2)),
                        ),
                    ),
                    null_treatment: None,
                }),
                r#"count(*) OVER (ORDER BY a DESC NULLS FIRST RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING)"#,
            ),
            (col("a").is_not_null(), r#"a IS NOT NULL"#),
            (col("a").is_null(), r#"a IS NULL"#),
            (
                (col("a") + col("b")).gt(lit(4)).is_true(),
                r#"((a + b) > 4) IS TRUE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_not_true(),
                r#"((a + b) > 4) IS NOT TRUE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_false(),
                r#"((a + b) > 4) IS FALSE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_not_false(),
                r#"((a + b) > 4) IS NOT FALSE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_unknown(),
                r#"((a + b) > 4) IS UNKNOWN"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_not_unknown(),
                r#"((a + b) > 4) IS NOT UNKNOWN"#,
            ),
            (not(col("a")), r#"NOT a"#),
            (
                Expr::between(col("a"), lit(1), lit(7)),
                r#"(a BETWEEN 1 AND 7)"#,
            ),
            (Expr::Negative(Box::new(col("a"))), r#"-a"#),
            (
                exists(Arc::new(dummy_logical_plan.clone())),
                r#"EXISTS (SELECT * FROM t WHERE (t.a = 1))"#,
            ),
            (
                not_exists(Arc::new(dummy_logical_plan)),
                r#"NOT EXISTS (SELECT * FROM t WHERE (t.a = 1))"#,
            ),
            (
                try_cast(col("a"), DataType::Date64),
                r#"TRY_CAST(a AS DATETIME)"#,
            ),
            (
                try_cast(col("a"), DataType::UInt32),
                r#"TRY_CAST(a AS INTEGER UNSIGNED)"#,
            ),
            (
                Expr::ScalarVariable(Int8, vec![String::from("@a")]),
                r#"@a"#,
            ),
            (
                Expr::ScalarVariable(
                    Int8,
                    vec![String::from("@root"), String::from("foo")],
                ),
                r#"@root.foo"#,
            ),
            (col("x").eq(placeholder("$1")), r#"(x = $1)"#),
            (
                out_ref_col(DataType::Int32, "t.a").gt(lit(1)),
                r#"(t.a > 1)"#,
            ),
            (
                grouping_set(vec![vec![col("a"), col("b")], vec![col("a")]]),
                r#"GROUPING SETS ((a, b), (a))"#,
            ),
            (cube(vec![col("a"), col("b")]), r#"CUBE (a, b)"#),
            (rollup(vec![col("a"), col("b")]), r#"ROLLUP (a, b)"#),
            (col("table").eq(lit(1)), r#"("table" = 1)"#),
            (
                col("123_need_quoted").eq(lit(1)),
                r#"("123_need_quoted" = 1)"#,
            ),
            (col("need-quoted").eq(lit(1)), r#"("need-quoted" = 1)"#),
            (col("need quoted").eq(lit(1)), r#"("need quoted" = 1)"#),
            // See test_interval_scalar_to_expr for interval literals
            (
                (col("a") + col("b")).gt(Expr::Literal(ScalarValue::Decimal128(
                    Some(100123),
                    28,
                    3,
                ))),
                r#"((a + b) > 100.123)"#,
            ),
            (
                (col("a") + col("b")).gt(Expr::Literal(ScalarValue::Decimal256(
                    Some(100123.into()),
                    28,
                    3,
                ))),
                r#"((a + b) > 100.123)"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::Decimal128(10, -2),
                }),
                r#"CAST(a AS DECIMAL(12,0))"#,
            ),
        ];

        for (expr, expected) in tests {
            let ast = expr_to_sql(&expr)?;

            let actual = format!("{}", ast);

            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn custom_dialect_with_identifier_quote_style() -> Result<()> {
        let dialect = CustomDialectBuilder::new()
            .with_identifier_quote_style('\'')
            .build();
        let unparser = Unparser::new(&dialect);

        let expr = col("a").gt(lit(4));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{}", ast);

        let expected = r#"('a' > 4)"#;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn custom_dialect_without_identifier_quote_style() -> Result<()> {
        let dialect = CustomDialect::default();
        let unparser = Unparser::new(&dialect);

        let expr = col("a").gt(lit(4));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{}", ast);

        let expected = r#"(a > 4)"#;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn custom_dialect_use_timestamp_for_date64() -> Result<()> {
        for (use_timestamp_for_date64, identifier) in
            [(false, "DATETIME"), (true, "TIMESTAMP")]
        {
            let dialect = CustomDialectBuilder::new()
                .with_use_timestamp_for_date64(use_timestamp_for_date64)
                .build();
            let unparser = Unparser::new(&dialect);

            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Date64,
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);

            let expected = format!(r#"CAST(a AS {identifier})"#);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_float64_ast_dtype() -> Result<()> {
        for (float64_ast_dtype, identifier) in [
            (sqlparser::ast::DataType::Double, "DOUBLE"),
            (
                sqlparser::ast::DataType::DoublePrecision,
                "DOUBLE PRECISION",
            ),
        ] {
            let dialect = CustomDialectBuilder::new()
                .with_float64_ast_dtype(float64_ast_dtype)
                .build();
            let unparser = Unparser::new(&dialect);

            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);

            let expected = format!(r#"CAST(a AS {identifier})"#);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn customer_dialect_support_nulls_first_in_ort() -> Result<()> {
        let tests: Vec<(Sort, &str, bool)> = vec![
            (col("a").sort(true, true), r#"a ASC NULLS FIRST"#, true),
            (col("a").sort(true, true), r#"a ASC"#, false),
        ];

        for (expr, expected, supports_nulls_first_in_sort) in tests {
            let dialect = CustomDialectBuilder::new()
                .with_supports_nulls_first_in_sort(supports_nulls_first_in_sort)
                .build();
            let unparser = Unparser::new(&dialect);
            let ast = unparser.sort_to_sql(&expr)?;

            let actual = format!("{}", ast);

            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_interval_scalar_to_expr() {
        let tests = [
            (
                interval_month_day_nano_lit("1 MONTH"),
                IntervalStyle::SQLStandard,
                "INTERVAL '1' MONTH",
            ),
            (
                interval_month_day_nano_lit("1.5 DAY"),
                IntervalStyle::SQLStandard,
                "INTERVAL '1 12:0:0.000' DAY TO SECOND",
            ),
            (
                interval_month_day_nano_lit("-1.5 DAY"),
                IntervalStyle::SQLStandard,
                "INTERVAL '-1 -12:0:0.000' DAY TO SECOND",
            ),
            (
                interval_month_day_nano_lit("1.51234 DAY"),
                IntervalStyle::SQLStandard,
                "INTERVAL '1 12:17:46.176' DAY TO SECOND",
            ),
            (
                interval_datetime_lit("1.51234 DAY"),
                IntervalStyle::SQLStandard,
                "INTERVAL '1 12:17:46.176' DAY TO SECOND",
            ),
            (
                interval_year_month_lit("1 YEAR"),
                IntervalStyle::SQLStandard,
                "INTERVAL '12' MONTH",
            ),
            (
                interval_month_day_nano_lit(
                    "1 YEAR 1 MONTH 1 DAY 3 HOUR 10 MINUTE 20 SECOND",
                ),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '13 MONS 1 DAYS 3 HOURS 10 MINS 20.000000000 SECS'"#,
            ),
            (
                interval_month_day_nano_lit("1.5 MONTH"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '1 MONS 15 DAYS'"#,
            ),
            (
                interval_month_day_nano_lit("-3 MONTH"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '-3 MONS'"#,
            ),
            (
                interval_month_day_nano_lit("1 MONTH")
                    .add(interval_month_day_nano_lit("1 DAY")),
                IntervalStyle::PostgresVerbose,
                r#"(INTERVAL '1 MONS' + INTERVAL '1 DAYS')"#,
            ),
            (
                interval_month_day_nano_lit("1 MONTH")
                    .sub(interval_month_day_nano_lit("1 DAY")),
                IntervalStyle::PostgresVerbose,
                r#"(INTERVAL '1 MONS' - INTERVAL '1 DAYS')"#,
            ),
            (
                interval_datetime_lit("10 DAY 1 HOUR 10 MINUTE 20 SECOND"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '10 DAYS 1 HOURS 10 MINS 20.000 SECS'"#,
            ),
            (
                interval_datetime_lit("10 DAY 1.5 HOUR 10 MINUTE 20 SECOND"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '10 DAYS 1 HOURS 40 MINS 20.000 SECS'"#,
            ),
            (
                interval_year_month_lit("1 YEAR 1 MONTH"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '1 YEARS 1 MONS'"#,
            ),
            (
                interval_year_month_lit("1.5 YEAR 1 MONTH"),
                IntervalStyle::PostgresVerbose,
                r#"INTERVAL '1 YEARS 7 MONS'"#,
            ),
            (
                interval_year_month_lit("1 YEAR 1 MONTH"),
                IntervalStyle::MySQL,
                r#"INTERVAL 13 MONTH"#,
            ),
            (
                interval_month_day_nano_lit("1 YEAR -1 MONTH"),
                IntervalStyle::MySQL,
                r#"INTERVAL 11 MONTH"#,
            ),
            (
                interval_month_day_nano_lit("15 DAY"),
                IntervalStyle::MySQL,
                r#"INTERVAL 15 DAY"#,
            ),
            (
                interval_month_day_nano_lit("-40 HOURS"),
                IntervalStyle::MySQL,
                r#"INTERVAL -40 HOUR"#,
            ),
            (
                interval_datetime_lit("-1.5 DAY 1 HOUR"),
                IntervalStyle::MySQL,
                "INTERVAL -35 HOUR",
            ),
            (
                interval_datetime_lit("1000000 DAY 1.5 HOUR 10 MINUTE 20 SECOND"),
                IntervalStyle::MySQL,
                r#"INTERVAL 86400006020 SECOND"#,
            ),
            (
                interval_year_month_lit("0 DAY 0 HOUR"),
                IntervalStyle::MySQL,
                r#"INTERVAL 0 DAY"#,
            ),
            (
                interval_month_day_nano_lit("-1296000000 SECOND"),
                IntervalStyle::MySQL,
                r#"INTERVAL -15000 DAY"#,
            ),
        ];

        for (value, style, expected) in tests {
            let dialect = CustomDialectBuilder::new()
                .with_interval_style(style)
                .build();
            let unparser = Unparser::new(&dialect);

            let ast = unparser.expr_to_sql(&value).expect("to be unparsed");

            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_float_scalar_to_expr() {
        let tests = [
            (Expr::Literal(ScalarValue::Float64(Some(3f64))), "3.0"),
            (Expr::Literal(ScalarValue::Float64(Some(3.1f64))), "3.1"),
            (Expr::Literal(ScalarValue::Float32(Some(-2f32))), "-2.0"),
            (
                Expr::Literal(ScalarValue::Float32(Some(-2.989f32))),
                "-2.989",
            ),
        ];
        for (value, expected) in tests {
            let dialect = CustomDialectBuilder::new().build();
            let unparser = Unparser::new(&dialect);

            let ast = unparser.expr_to_sql(&value).expect("to be unparsed");
            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn custom_dialect_use_char_for_utf8_cast() -> Result<()> {
        let default_dialect = CustomDialectBuilder::default().build();
        let mysql_custom_dialect = CustomDialectBuilder::new()
            .with_utf8_cast_dtype(ast::DataType::Char(None))
            .with_large_utf8_cast_dtype(ast::DataType::Char(None))
            .build();

        for (dialect, data_type, identifier) in [
            (&default_dialect, DataType::Utf8, "VARCHAR"),
            (&default_dialect, DataType::LargeUtf8, "TEXT"),
            (&mysql_custom_dialect, DataType::Utf8, "CHAR"),
            (&mysql_custom_dialect, DataType::LargeUtf8, "CHAR"),
        ] {
            let unparser = Unparser::new(dialect);

            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type,
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_date_field_extract_style() -> Result<()> {
        for (extract_style, unit, expected) in [
            (
                DateFieldExtractStyle::DatePart,
                "YEAR",
                "date_part('YEAR', x)",
            ),
            (
                DateFieldExtractStyle::Extract,
                "YEAR",
                "EXTRACT(YEAR FROM x)",
            ),
            (DateFieldExtractStyle::Strftime, "YEAR", "strftime('%Y', x)"),
            (
                DateFieldExtractStyle::DatePart,
                "MONTH",
                "date_part('MONTH', x)",
            ),
            (
                DateFieldExtractStyle::Extract,
                "MONTH",
                "EXTRACT(MONTH FROM x)",
            ),
            (
                DateFieldExtractStyle::Strftime,
                "MONTH",
                "strftime('%m', x)",
            ),
            (
                DateFieldExtractStyle::DatePart,
                "DAY",
                "date_part('DAY', x)",
            ),
            (DateFieldExtractStyle::Strftime, "DAY", "strftime('%d', x)"),
            (DateFieldExtractStyle::Extract, "DAY", "EXTRACT(DAY FROM x)"),
        ] {
            let dialect = CustomDialectBuilder::new()
                .with_date_field_extract_style(extract_style)
                .build();

            let unparser = Unparser::new(&dialect);
            let expr = ScalarUDF::new_from_impl(
                datafusion_functions::datetime::date_part::DatePartFunc::new(),
            )
            .call(vec![Expr::Literal(ScalarValue::new_utf8(unit)), col("x")]);

            let ast = unparser.expr_to_sql(&expr)?;
            let actual = format!("{}", ast);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_int64_cast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_int64_cast_dtype(ast::DataType::Custom(
                ObjectName(vec![Ident::new("SIGNED")]),
                vec![],
            ))
            .build();

        for (dialect, identifier) in
            [(default_dialect, "BIGINT"), (mysql_dialect, "SIGNED")]
        {
            let unparser = Unparser::new(&dialect);
            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Int64,
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_timestamp_cast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_timestamp_cast_dtype(
                ast::DataType::Datetime(None),
                ast::DataType::Datetime(None),
            )
            .build();

        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let timestamp_with_tz =
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into()));

        for (dialect, data_type, identifier) in [
            (&default_dialect, &timestamp, "TIMESTAMP"),
            (
                &default_dialect,
                &timestamp_with_tz,
                "TIMESTAMP WITH TIME ZONE",
            ),
            (&mysql_dialect, &timestamp, "DATETIME"),
            (&mysql_dialect, &timestamp_with_tz, "DATETIME"),
        ] {
            let unparser = Unparser::new(dialect);
            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type: data_type.clone(),
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_date32_ast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::default().build();
        let sqlite_custom_dialect = CustomDialectBuilder::new()
            .with_date32_cast_dtype(ast::DataType::Text)
            .build();

        for (dialect, data_type, identifier) in [
            (&default_dialect, DataType::Date32, "DATE"),
            (&sqlite_custom_dialect, DataType::Date32, "TEXT"),
        ] {
            let unparser = Unparser::new(dialect);

            let expr = Expr::Cast(Cast {
                expr: Box::new(col("a")),
                data_type,
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{}", ast);
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_cast_value_to_dict_expr() {
        let tests = [(
            Expr::Cast(Cast {
                expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some(
                    "variation".to_string(),
                )))),
                data_type: DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                ),
            }),
            "'variation'",
        )];
        for (value, expected) in tests {
            let dialect = CustomDialectBuilder::new().build();
            let unparser = Unparser::new(&dialect);

            let ast = unparser.expr_to_sql(&value).expect("to be unparsed");
            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
    }
}
