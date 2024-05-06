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

use core::fmt;
use std::fmt::Display;

use arrow_array::{Date32Array, Date64Array};
use arrow_schema::DataType;
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, plan_err, Column, Result,
    ScalarValue,
};
use datafusion_expr::{
    expr::{Alias, Exists, InList, ScalarFunction, Sort, WindowFunction},
    Between, BinaryExpr, Case, Cast, Expr, Like, Operator,
};
use sqlparser::ast::{
    self, Expr as AstExpr, Function, FunctionArg, Ident, UnaryOperator,
};

use super::Unparser;

/// DataFusion's Exprs can represent either an `Expr` or an `OrderByExpr`
pub enum Unparsed {
    // SQL Expression
    Expr(ast::Expr),
    // SQL ORDER BY expression (e.g. `col ASC NULLS FIRST`)
    OrderByExpr(ast::OrderByExpr),
}

impl Unparsed {
    pub fn into_order_by_expr(self) -> Result<ast::OrderByExpr> {
        if let Unparsed::OrderByExpr(order_by_expr) = self {
            Ok(order_by_expr)
        } else {
            internal_err!("Expected Sort expression to be converted an OrderByExpr")
        }
    }
}

impl Display for Unparsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unparsed::Expr(expr) => write!(f, "{}", expr),
            Unparsed::OrderByExpr(order_by_expr) => write!(f, "{}", order_by_expr),
        }
    }
}

/// Convert a DataFusion [`Expr`] to `sqlparser::ast::Expr`
///
/// This function is the opposite of `SqlToRel::sql_to_expr` and can
/// be used to, among other things, convert [`Expr`]s to strings.
/// Throws an error if [`Expr`] can not be represented by an `sqlparser::ast::Expr`
///
/// # Example
/// ```
/// use datafusion_expr::{col, lit};
/// use datafusion_sql::unparser::expr_to_sql;
/// let expr = col("a").gt(lit(4));
/// let sql = expr_to_sql(&expr).unwrap();
///
/// assert_eq!(format!("{}", sql), "(\"a\" > 4)")
/// ```
pub fn expr_to_sql(expr: &Expr) -> Result<ast::Expr> {
    let unparser = Unparser::default();
    unparser.expr_to_sql(expr)
}

/// Convert a DataFusion [`Expr`] to [`Unparsed`]
///
/// This function is similar to expr_to_sql, but it supports converting more [`Expr`] types like
/// `Sort` expressions to `OrderByExpr` expressions.
pub fn expr_to_unparsed(expr: &Expr) -> Result<Unparsed> {
    let unparser = Unparser::default();
    unparser.expr_to_unparsed(expr)
}

impl Unparser<'_> {
    pub fn expr_to_sql(&self, expr: &Expr) -> Result<ast::Expr> {
        match expr {
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let list_expr = list
                    .iter()
                    .map(|e| self.expr_to_sql(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast::Expr::InList {
                    expr: Box::new(self.expr_to_sql(expr)?),
                    list: list_expr,
                    negated: *negated,
                })
            }
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => {
                let func_name = func_def.name();

                let args = args
                    .iter()
                    .map(|e| {
                        if matches!(e, Expr::Wildcard { qualifier: None }) {
                            Ok(FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                        } else {
                            self.expr_to_sql(e).map(|e| {
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
                }))
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let sql_parser_expr = self.expr_to_sql(expr)?;
                let sql_low = self.expr_to_sql(low)?;
                let sql_high = self.expr_to_sql(high)?;
                Ok(ast::Expr::Nested(Box::new(self.between_op_to_sql(
                    sql_parser_expr,
                    *negated,
                    sql_low,
                    sql_high,
                ))))
            }
            Expr::Column(col) => self.col_to_sql(col),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.expr_to_sql(left.as_ref())?;
                let r = self.expr_to_sql(right.as_ref())?;
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
                    .map(|(w, _)| self.expr_to_sql(w))
                    .collect::<Result<Vec<_>>>()?;
                let results = when_then_expr
                    .iter()
                    .map(|(_, t)| self.expr_to_sql(t))
                    .collect::<Result<Vec<_>>>()?;
                let operand = match expr.as_ref() {
                    Some(e) => match self.expr_to_sql(e) {
                        Ok(sql_expr) => Some(Box::new(sql_expr)),
                        Err(_) => None,
                    },
                    None => None,
                };
                let else_result = match else_expr.as_ref() {
                    Some(e) => match self.expr_to_sql(e) {
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
                let inner_expr = self.expr_to_sql(expr)?;
                Ok(ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(inner_expr),
                    data_type: self.arrow_dtype_to_ast_dtype(data_type)?,
                    format: None,
                })
            }
            Expr::Literal(value) => Ok(self.scalar_to_sql(value)?),
            Expr::Alias(Alias { expr, name: _, .. }) => self.expr_to_sql(expr),
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
                    .map(|expr| expr_to_unparsed(expr)?.into_order_by_expr())
                    .collect::<Result<Vec<_>>>()?;

                let start_bound = self.convert_bound(&window_frame.start_bound);
                let end_bound = self.convert_bound(&window_frame.end_bound);
                let over = Some(ast::WindowType::WindowSpec(ast::WindowSpec {
                    window_name: None,
                    partition_by: partition_by
                        .iter()
                        .map(|e| self.expr_to_sql(e))
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
                expr: Box::new(self.expr_to_sql(expr)?),
                pattern: Box::new(self.expr_to_sql(pattern)?),
                escape_char: escape_char.map(|c| c.to_string()),
            }),
            Expr::AggregateFunction(agg) => {
                let func_name = agg.func_def.name();

                let args = self.function_args_to_sql(&agg.args)?;
                let filter = match &agg.filter {
                    Some(filter) => Some(Box::new(self.expr_to_sql(filter)?)),
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
                let inexpr = Box::new(self.expr_to_sql(insubq.expr.as_ref())?);
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
            Expr::Sort(Sort {
                expr: _,
                asc: _,
                nulls_first: _,
            }) => plan_err!("Sort expression should be handled by expr_to_unparsed"),
            Expr::IsNotNull(expr) => {
                Ok(ast::Expr::IsNotNull(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::IsTrue(expr) => {
                Ok(ast::Expr::IsTrue(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::IsNotTrue(expr) => {
                Ok(ast::Expr::IsNotTrue(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::IsFalse(expr) => {
                Ok(ast::Expr::IsFalse(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::IsUnknown(expr) => {
                Ok(ast::Expr::IsUnknown(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::IsNotUnknown(expr) => {
                Ok(ast::Expr::IsNotUnknown(Box::new(self.expr_to_sql(expr)?)))
            }
            Expr::Not(expr) => {
                let sql_parser_expr = self.expr_to_sql(expr)?;
                Ok(AstExpr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(sql_parser_expr),
                })
            }
            Expr::Negative(expr) => {
                let sql_parser_expr = self.expr_to_sql(expr)?;
                Ok(AstExpr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(sql_parser_expr),
                })
            }
            Expr::ScalarVariable(_, _) => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::IsNull(_) => not_impl_err!("Unsupported Expr conversion: {expr:?}"),
            Expr::IsNotFalse(_) => not_impl_err!("Unsupported Expr conversion: {expr:?}"),
            Expr::GetIndexedField(_) => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::TryCast(_) => not_impl_err!("Unsupported Expr conversion: {expr:?}"),
            Expr::Wildcard { qualifier: _ } => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::GroupingSet(_) => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::Placeholder(_) => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::OuterReferenceColumn(_, _) => {
                not_impl_err!("Unsupported Expr conversion: {expr:?}")
            }
            Expr::Unnest(_) => not_impl_err!("Unsupported Expr conversion: {expr:?}"),
        }
    }

    /// This function can convert more [`Expr`] types than `expr_to_sql`, returning an [`Unparsed`]
    /// like `Sort` expressions to `OrderByExpr` expressions.
    pub fn expr_to_unparsed(&self, expr: &Expr) -> Result<Unparsed> {
        match expr {
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => {
                let sql_parser_expr = self.expr_to_sql(expr)?;
                Ok(Unparsed::OrderByExpr(ast::OrderByExpr {
                    expr: sql_parser_expr,
                    asc: Some(*asc),
                    nulls_first: Some(*nulls_first),
                }))
            }
            _ => {
                let sql_parser_expr = self.expr_to_sql(expr)?;
                Ok(Unparsed::Expr(sql_parser_expr))
            }
        }
    }

    fn col_to_sql(&self, col: &Column) -> Result<ast::Expr> {
        if let Some(table_ref) = &col.relation {
            let mut id = table_ref.to_vec();
            id.push(col.name.to_string());
            return Ok(ast::Expr::CompoundIdentifier(
                id.iter().map(|i| self.new_ident(i.to_string())).collect(),
            ));
        }
        Ok(ast::Expr::Identifier(self.new_ident(col.name.to_string())))
    }

    fn convert_bound(
        &self,
        bound: &datafusion_expr::window_frame::WindowFrameBound,
    ) -> ast::WindowFrameBound {
        match bound {
            datafusion_expr::window_frame::WindowFrameBound::Preceding(val) => {
                ast::WindowFrameBound::Preceding(
                    self.scalar_to_sql(val).map(Box::new).ok(),
                )
            }
            datafusion_expr::window_frame::WindowFrameBound::Following(val) => {
                ast::WindowFrameBound::Following(
                    self.scalar_to_sql(val).map(Box::new).ok(),
                )
            }
            datafusion_expr::window_frame::WindowFrameBound::CurrentRow => {
                ast::WindowFrameBound::CurrentRow
            }
        }
    }

    fn function_args_to_sql(&self, args: &[Expr]) -> Result<Vec<ast::FunctionArg>> {
        args.iter()
            .map(|e| {
                if matches!(e, Expr::Wildcard { qualifier: None }) {
                    Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                } else {
                    self.expr_to_sql(e)
                        .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    pub(super) fn new_ident(&self, str: String) -> ast::Ident {
        ast::Ident {
            value: str,
            quote_style: self.dialect.identifier_quote_style(),
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

    /// DataFusion ScalarValues sometimes require a ast::Expr to construct.
    /// For example ScalarValue::Date32(d) corresponds to the ast::Expr CAST('datestr' as DATE)
    fn scalar_to_sql(&self, v: &ScalarValue) -> Result<ast::Expr> {
        match v {
            ScalarValue::Null => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Boolean(Some(b)) => {
                Ok(ast::Expr::Value(ast::Value::Boolean(b.to_owned())))
            }
            ScalarValue::Boolean(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Float32(Some(f)) => {
                Ok(ast::Expr::Value(ast::Value::Number(f.to_string(), false)))
            }
            ScalarValue::Float32(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Float64(Some(f)) => {
                Ok(ast::Expr::Value(ast::Value::Number(f.to_string(), false)))
            }
            ScalarValue::Float64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Decimal128(Some(_), ..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Decimal128(None, ..) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Decimal256(Some(_), ..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
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
            ScalarValue::LargeUtf8(Some(str)) => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(str.to_string()),
            )),
            ScalarValue::LargeUtf8(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Binary(None) => Ok(ast::Expr::Value(ast::Value::Null)),
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
                    data_type: ast::DataType::Datetime(None),
                    format: None,
                })
            }
            ScalarValue::Date64(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Time32Second(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time32Second(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::Time32Millisecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time32Millisecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::Time64Microsecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time64Microsecond(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::Time64Nanosecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time64Nanosecond(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::TimestampSecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampSecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampMillisecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampMillisecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampMicrosecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampMicrosecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::TimestampNanosecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampNanosecond(None, _) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::IntervalYearMonth(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::IntervalYearMonth(None) => {
                Ok(ast::Expr::Value(ast::Value::Null))
            }
            ScalarValue::IntervalDayTime(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::IntervalDayTime(None) => Ok(ast::Expr::Value(ast::Value::Null)),
            ScalarValue::IntervalMonthDayNano(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
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
            ScalarValue::Union(..) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
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
            DataType::Int64 => Ok(ast::DataType::BigInt(None)),
            DataType::UInt8 => Ok(ast::DataType::UnsignedTinyInt(None)),
            DataType::UInt16 => Ok(ast::DataType::UnsignedSmallInt(None)),
            DataType::UInt32 => Ok(ast::DataType::UnsignedInteger(None)),
            DataType::UInt64 => Ok(ast::DataType::UnsignedBigInt(None)),
            DataType::Float16 => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Float32 => Ok(ast::DataType::Float(None)),
            DataType::Float64 => Ok(ast::DataType::Double),
            DataType::Timestamp(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Date32 => Ok(ast::DataType::Date),
            DataType::Date64 => Ok(ast::DataType::Datetime(None)),
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
            DataType::Utf8 => Ok(ast::DataType::Varchar(None)),
            DataType::LargeUtf8 => Ok(ast::DataType::Text),
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
            DataType::Decimal128(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
            }
            DataType::Decimal256(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
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
    use std::{any::Any, sync::Arc, vec};

    use arrow::datatypes::{Field, Schema};
    use datafusion_common::TableReference;
    use datafusion_expr::{
        case, col, exists,
        expr::{AggregateFunction, AggregateFunctionDefinition},
        lit, not, not_exists, table_scan, wildcard, ColumnarValue, ScalarUDF,
        ScalarUDFImpl, Signature, Volatility, WindowFrame, WindowFunctionDefinition,
    };

    use crate::unparser::dialect::CustomDialect;

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
            .project(vec![Expr::Wildcard { qualifier: None }])?
            .filter(col("a").eq(lit(1)))?
            .build()?;

        let tests: Vec<(Expr, &str)> = vec![
            ((col("a") + col("b")).gt(lit(4)), r#"(("a" + "b") > 4)"#),
            (
                Expr::Column(Column {
                    relation: Some(TableReference::partial("a", "b")),
                    name: "c".to_string(),
                })
                .gt(lit(4)),
                r#"("a"."b"."c" > 4)"#,
            ),
            (
                case(col("a"))
                    .when(lit(1), lit(true))
                    .when(lit(0), lit(false))
                    .otherwise(lit(ScalarValue::Null))?,
                r#"CASE "a" WHEN 1 THEN true WHEN 0 THEN false ELSE NULL END"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::Date64,
                }),
                r#"CAST("a" AS DATETIME)"#,
            ),
            (
                Expr::Cast(Cast {
                    expr: Box::new(col("a")),
                    data_type: DataType::UInt32,
                }),
                r#"CAST("a" AS INTEGER UNSIGNED)"#,
            ),
            (
                col("a").in_list(vec![lit(1), lit(2), lit(3)], false),
                r#""a" IN (1, 2, 3)"#,
            ),
            (
                col("a").in_list(vec![lit(1), lit(2), lit(3)], true),
                r#""a" NOT IN (1, 2, 3)"#,
            ),
            (
                ScalarUDF::new_from_impl(DummyUDF::new()).call(vec![col("a"), col("b")]),
                r#"dummy_udf("a", "b")"#,
            ),
            (
                Expr::Like(Like {
                    negated: true,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: true,
                }),
                r#""a" NOT LIKE 'foo' ESCAPE 'o'"#,
            ),
            (
                Expr::SimilarTo(Like {
                    negated: false,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: true,
                }),
                r#""a" LIKE 'foo' ESCAPE 'o'"#,
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
                Expr::AggregateFunction(AggregateFunction {
                    func_def: AggregateFunctionDefinition::BuiltIn(
                        datafusion_expr::AggregateFunction::Sum,
                    ),
                    args: vec![col("a")],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                }),
                r#"SUM("a")"#,
            ),
            (
                Expr::AggregateFunction(AggregateFunction {
                    func_def: AggregateFunctionDefinition::BuiltIn(
                        datafusion_expr::AggregateFunction::Count,
                    ),
                    args: vec![Expr::Wildcard { qualifier: None }],
                    distinct: true,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                }),
                "COUNT(DISTINCT *)",
            ),
            (
                Expr::AggregateFunction(AggregateFunction {
                    func_def: AggregateFunctionDefinition::BuiltIn(
                        datafusion_expr::AggregateFunction::Count,
                    ),
                    args: vec![Expr::Wildcard { qualifier: None }],
                    distinct: false,
                    filter: Some(Box::new(lit(true))),
                    order_by: None,
                    null_treatment: None,
                }),
                "COUNT(*) FILTER (WHERE true)",
            ),
            (
                Expr::WindowFunction(WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        datafusion_expr::BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![col("col")],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: WindowFrame::new(None),
                    null_treatment: None,
                }),
                r#"ROW_NUMBER("col") OVER (ROWS BETWEEN NULL PRECEDING AND NULL FOLLOWING)"#,
            ),
            (
                Expr::WindowFunction(WindowFunction {
                    fun: WindowFunctionDefinition::AggregateFunction(
                        datafusion_expr::AggregateFunction::Count,
                    ),
                    args: vec![wildcard()],
                    partition_by: vec![],
                    order_by: vec![Expr::Sort(Sort::new(
                        Box::new(col("a")),
                        false,
                        true,
                    ))],
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
                r#"COUNT(*) OVER (ORDER BY "a" DESC NULLS FIRST RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING)"#,
            ),
            (col("a").is_not_null(), r#""a" IS NOT NULL"#),
            (
                (col("a") + col("b")).gt(lit(4)).is_true(),
                r#"(("a" + "b") > 4) IS TRUE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_not_true(),
                r#"(("a" + "b") > 4) IS NOT TRUE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_false(),
                r#"(("a" + "b") > 4) IS FALSE"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_unknown(),
                r#"(("a" + "b") > 4) IS UNKNOWN"#,
            ),
            (
                (col("a") + col("b")).gt(lit(4)).is_not_unknown(),
                r#"(("a" + "b") > 4) IS NOT UNKNOWN"#,
            ),
            (not(col("a")), r#"NOT "a""#),
            (
                Expr::between(col("a"), lit(1), lit(7)),
                r#"("a" BETWEEN 1 AND 7)"#,
            ),
            (Expr::Negative(Box::new(col("a"))), r#"-"a""#),
            (
                exists(Arc::new(dummy_logical_plan.clone())),
                r#"EXISTS (SELECT "t"."a" FROM "t" WHERE ("t"."a" = 1))"#,
            ),
            (
                not_exists(Arc::new(dummy_logical_plan.clone())),
                r#"NOT EXISTS (SELECT "t"."a" FROM "t" WHERE ("t"."a" = 1))"#,
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
    fn expr_to_unparsed_ok() -> Result<()> {
        let tests: Vec<(Expr, &str)> = vec![
            ((col("a") + col("b")).gt(lit(4)), r#"(("a" + "b") > 4)"#),
            (col("a").sort(true, true), r#""a" ASC NULLS FIRST"#),
        ];

        for (expr, expected) in tests {
            let ast = expr_to_unparsed(&expr)?;

            let actual = format!("{}", ast);

            assert_eq!(actual, expected);
        }

        Ok(())
    }
    #[test]
    fn custom_dialect() -> Result<()> {
        let dialect = CustomDialect::new(Some('\''));
        let unparser = Unparser::new(&dialect);

        let expr = col("a").gt(lit(4));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{}", ast);

        let expected = r#"('a' > 4)"#;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn custom_dialect_none() -> Result<()> {
        let dialect = CustomDialect::new(None);
        let unparser = Unparser::new(&dialect);

        let expr = col("a").gt(lit(4));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{}", ast);

        let expected = r#"(a > 4)"#;
        assert_eq!(actual, expected);

        Ok(())
    }
}
