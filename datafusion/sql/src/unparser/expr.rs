// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::datatype::DataTypeExt;
use datafusion_expr::expr::{
    AggregateFunctionParams, HigherOrderFunction, NullTreatment, WindowFunctionParams,
};
use datafusion_expr::expr::{Lambda, Unnest};
use sqlparser::dialect::clickhouse::ClickHouseOrderByItem;
use sqlparser::dialect::duckdb::DuckDbExpr;
use std::borrow::Cow;
use std::sync::Arc;
use std::vec;

use super::Unparser;
use super::ast::{
    binary, data_type, data_type_with_args, function, ident, name_expr, nested, null,
    number, object_name, string,
};
use super::dialect::{DistinctFromStyle, IntervalStyle};
use crate::ast::{
    self, BinaryOp, BuiltinExpr, CaseExpr, CaseWhen, CastStyle, ExprKind, FrameBound,
    FrameUnits, FunctionArg, FunctionOptions, GroupingKind, Ident, Interval, IsTest,
    LikeKind, Literal, OrderByItem, Quantifier, SpecialExpr, UnaryOp, WindowFrame,
    WindowOver, WindowSpec,
};
use arrow::array::{
    ArrayRef, Date32Array, Date64Array, PrimitiveArray,
    types::{
        ArrowTemporalType, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    FieldRef,
};
use arrow::util::display::array_value_to_string;
use datafusion_common::{
    Column, Result, ScalarValue, assert_eq_or_internal_err, assert_or_internal_err,
    internal_datafusion_err, internal_err, not_impl_err, plan_err,
};
use datafusion_expr::{
    Between, BinaryExpr, Case, Cast, Expr, GroupingSet, Like, Operator, TryCast,
    expr::{Alias, Exists, InList, ScalarFunction, SetQuantifier, Sort, WindowFunction},
};

/// The SQL syntax tree of an expression, as the unparser builds it.
type SqlExpr = ast::Expr<'static>;

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
/// let sql = expr_to_sql(&expr).unwrap(); // convert to ast::Expr, using
/// assert_eq!(sql.to_string(), "(a > 4)"); // use Display impl for SQL text
/// ```
///
/// [`SqlToRel::sql_to_expr`]: crate::planner::SqlToRel::sql_to_expr
/// [`plan_to_sql`]: crate::unparser::plan_to_sql
/// [`LogicalPlan`]: datafusion_expr::logical_plan::LogicalPlan
pub fn expr_to_sql(expr: &Expr) -> Result<ast::Expr<'static>> {
    let unparser = Unparser::default();
    unparser.expr_to_sql(expr)
}

const LOWEST: &BinaryOp<'static> = &BinaryOp::Or;
// Closest precedence we have to IS operator is BitwiseAnd (any other) in PG docs
// (https://www.postgresql.org/docs/7.2/sql-precedence.html)
const IS: &BinaryOp<'static> = &BinaryOp::BitAnd;

fn null_treatment_to_sql(null_treatment: NullTreatment) -> ast::NullTreatment {
    match null_treatment {
        NullTreatment::IgnoreNulls => ast::NullTreatment::Ignore,
        NullTreatment::RespectNulls => ast::NullTreatment::Respect,
    }
}

/// The clauses of a call that has none of them.
fn function_options() -> FunctionOptions<'static> {
    FunctionOptions {
        parameters: Vec::new(),
        quantifier: ast::SetQuantifier::None,
        order_by: Vec::new(),
        within_group: Vec::new(),
        filter: None,
        null_treatment: None,
        inner_null_treatment: None,
        separator: None,
        on_overflow: None,
        over: None,
    }
}

/// A call of `name`, with the less common clauses in `options`.
fn function_with_options(
    name: &str,
    args: Vec<FunctionArg<'static>>,
    options: FunctionOptions<'static>,
) -> SqlExpr {
    ExprKind::Function {
        name: object_name(vec![ident(name)]),
        args,
        options: Some(Box::new(options)),
    }
    .into()
}

fn special(expr: SpecialExpr<'static>) -> SqlExpr {
    ExprKind::Special(Box::new(expr)).into()
}

fn unary(op: UnaryOp, expr: SqlExpr) -> SqlExpr {
    ExprKind::Unary {
        op,
        expr: Box::new(expr),
    }
    .into()
}

fn is(expr: SqlExpr, test: IsTest<'static>, negated: bool) -> SqlExpr {
    ExprKind::Is {
        expr: Box::new(expr),
        test,
        negated,
    }
    .into()
}

fn cast(expr: SqlExpr, ty: ast::DataType<'static>, style: CastStyle) -> SqlExpr {
    ExprKind::Cast {
        expr: Box::new(expr),
        ty: Box::new(ty),
        style,
    }
    .into()
}

fn interval(
    value: SqlExpr,
    leading_field: Option<&'static str>,
    last_field: Option<&'static str>,
) -> SqlExpr {
    ExprKind::Interval(Box::new(Interval {
        value,
        leading_field: leading_field.map(Cow::Borrowed),
        leading_precision: None,
        last_field: last_field.map(Cow::Borrowed),
        fractional_seconds_precision: None,
    }))
    .into()
}

/// The elements of an array literal, in any of the forms the unparser writes.
fn array_elements(expr: SqlExpr) -> Option<Vec<SqlExpr>> {
    match expr.kind {
        ExprKind::Special(special) => match *special {
            SpecialExpr::Array(items) => Some(items),
            _ => None,
        },
        ExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::List(items))) => Some(items),
        _ => None,
    }
}

impl Unparser<'_> {
    pub fn expr_to_sql(&self, expr: &Expr) -> Result<ast::Expr<'static>> {
        // Unparsing recurses once per nesting level. The function-argument and
        // dialect scalar-function-override paths cost more per level than the
        // default `recursive` red zone, so without raising the minimum stack
        // size the stack-growing trampoline engages too late and the OS stack
        // overflows on deeply nested expressions (issue #23056). The size
        // mirrors the planner's stack-growth usage in `query.rs`.
        crate::stack::maybe_grow(|| self.expr_to_sql_with_nesting(expr))
    }

    /// Recursive entry point shared by the public [`Self::expr_to_sql`] and the
    /// internal recursion sites (scalar-function arguments, arrays, maps, and
    /// dialect scalar-function overrides).
    ///
    /// This is a stack-growth checkpoint. Internal recursion must call this
    /// rather than the public [`Self::expr_to_sql`]: the public entry point
    /// would re-enter the public stack-growth boundary on every level.
    pub(crate) fn expr_to_sql_with_nesting(&self, expr: &Expr) -> Result<SqlExpr> {
        crate::stack::maybe_grow(|| {
            let mut root_expr = self.expr_to_sql_inner(expr)?;
            if self.pretty {
                root_expr = self.remove_unnecessary_nesting(root_expr, LOWEST, LOWEST);
            }
            Ok(root_expr)
        })
    }

    fn distinct_from_to_sql(
        &self,
        left: SqlExpr,
        right: SqlExpr,
        is_distinct: bool,
    ) -> Result<SqlExpr> {
        match self.dialect.distinct_from_style() {
            DistinctFromStyle::FullText => {
                let expr = is(left, IsTest::DistinctFrom(Box::new(right)), !is_distinct);
                Ok(nested(expr))
            }
            DistinctFromStyle::Spaceship => {
                let expr = nested(binary(left, BinaryOp::Named("<=>".into()), right));
                if is_distinct {
                    Ok(nested(unary(UnaryOp::Not, expr)))
                } else {
                    Ok(expr)
                }
            }
        }
    }

    fn expr_to_sql_inner(&self, expr: &Expr) -> Result<SqlExpr> {
        crate::stack::maybe_grow(|| match expr {
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let list_expr = list
                    .iter()
                    .map(|e| self.expr_to_sql_inner(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ExprKind::InList {
                    expr: Box::new(self.expr_to_sql_inner(expr)?),
                    list: list_expr,
                    negated: *negated,
                }
                .into())
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let func_name = func.name();

                if let Some(expr) = self
                    .dialect
                    .scalar_function_to_sql_overrides(self, func_name, args)?
                {
                    return Ok(expr);
                }

                self.scalar_function_to_sql(func_name, args)
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
                Ok(nested(self.between_op_to_sql(
                    sql_parser_expr,
                    *negated,
                    sql_low,
                    sql_high,
                )))
            }
            Expr::Column(col) => self.col_to_sql(col),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::IsDistinctFrom,
                right,
            }) => {
                let l = self.expr_to_sql_inner(left.as_ref())?;
                let r = self.expr_to_sql_inner(right.as_ref())?;
                self.distinct_from_to_sql(l, r, true)
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::IsNotDistinctFrom,
                right,
            }) => {
                let l = self.expr_to_sql_inner(left.as_ref())?;
                let r = self.expr_to_sql_inner(right.as_ref())?;
                self.distinct_from_to_sql(l, r, false)
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.expr_to_sql_inner(left.as_ref())?;
                let r = self.expr_to_sql_inner(right.as_ref())?;
                let op = self.op_to_sql(op)?;

                Ok(nested(self.binary_op_to_sql(l, r, op)))
            }
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let branches = when_then_expr
                    .iter()
                    .map(|(cond, result)| {
                        Ok(CaseWhen {
                            condition: self.expr_to_sql_inner(cond)?,
                            result: self.expr_to_sql_inner(result)?,
                        })
                    })
                    .collect::<Result<Vec<CaseWhen>>>()?;

                let operand = expr
                    .as_ref()
                    .map(|e| self.expr_to_sql_inner(e))
                    .transpose()?;
                let else_result = else_expr
                    .as_ref()
                    .map(|e| self.expr_to_sql_inner(e))
                    .transpose()?;

                Ok(ExprKind::Case(Box::new(CaseExpr {
                    operand,
                    branches,
                    else_result,
                }))
                .into())
            }
            Expr::Cast(Cast { expr, field }) => Ok(self.cast_to_sql(expr, field)?),
            Expr::Literal(value, _) => Ok(self.scalar_to_sql(value)?),
            Expr::Alias(Alias { expr, .. }) => self.expr_to_sql_inner(expr),
            Expr::WindowFunction(window_fun) => {
                let WindowFunction {
                    fun,
                    params:
                        WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            filter,
                            distinct,
                            null_treatment,
                        },
                } = window_fun.as_ref();
                let func_name = fun.name();

                let args = self.function_args_to_sql(args)?;

                let units = match window_frame.units {
                    datafusion_expr::window_frame::WindowFrameUnits::Rows => {
                        FrameUnits::Rows
                    }
                    datafusion_expr::window_frame::WindowFrameUnits::Range => {
                        FrameUnits::Range
                    }
                    datafusion_expr::window_frame::WindowFrameUnits::Groups => {
                        FrameUnits::Groups
                    }
                };

                let order_by = order_by
                    .iter()
                    .map(|sort_expr| self.sort_to_sql(sort_expr))
                    .collect::<Result<Vec<_>>>()?;

                let start_bound = self.convert_bound(&window_frame.start_bound)?;
                let end_bound = self.convert_bound(&window_frame.end_bound)?;

                let frame = if self.dialect.window_func_support_window_frame(
                    func_name,
                    &start_bound,
                    &end_bound,
                ) {
                    Some(WindowFrame {
                        units,
                        start: start_bound,
                        end: Some(end_bound),
                    })
                } else {
                    None
                };

                let over = Some(WindowOver::Spec(WindowSpec {
                    base: None,
                    partition_by: partition_by
                        .iter()
                        .map(|e| self.expr_to_sql_inner(e))
                        .collect::<Result<Vec<_>>>()?,
                    order_by,
                    frame,
                }));

                Ok(function_with_options(
                    func_name,
                    args,
                    FunctionOptions {
                        quantifier: if *distinct {
                            ast::SetQuantifier::Distinct
                        } else {
                            ast::SetQuantifier::None
                        },
                        filter: filter
                            .as_ref()
                            .map(|f| self.expr_to_sql_inner(f))
                            .transpose()?,
                        null_treatment: null_treatment.map(null_treatment_to_sql),
                        over,
                        ..function_options()
                    },
                ))
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => Ok(ExprKind::Like {
                negated: *negated,
                expr: Box::new(self.expr_to_sql_inner(expr)?),
                pattern: Box::new(self.expr_to_sql_inner(pattern)?),
                kind: LikeKind::Like,
                escape: escape_char.map(|c| Box::new(string(&c.to_string()))),
            }
            .into()),
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Ok(ExprKind::Like {
                negated: *negated,
                expr: Box::new(self.expr_to_sql_inner(expr)?),
                pattern: Box::new(self.expr_to_sql_inner(pattern)?),
                kind: if *case_insensitive {
                    LikeKind::ILike
                } else {
                    LikeKind::Like
                },
                escape: escape_char.map(|c| Box::new(string(&c.to_string()))),
            }
            .into()),

            Expr::AggregateFunction(agg) => {
                let func_name = agg.func.name();
                let AggregateFunctionParams {
                    distinct,
                    args,
                    filter,
                    order_by,
                    null_treatment,
                } = &agg.params;

                // An aggregate's `order_by` is spelled one of two ways in SQL:
                // `WITHIN GROUP (ORDER BY ..)` for ordered-set aggregates, and an
                // `ORDER BY` clause inside the argument list for every other
                // aggregate. Both map onto `params.order_by`.
                let (args_to_use, within_group, order_by) = if order_by.is_empty() {
                    (self.function_args_to_sql(args)?, vec![], vec![])
                } else {
                    let order_by_sql = order_by
                        .iter()
                        .map(|sort_expr| self.sort_to_sql(sort_expr))
                        .collect::<Result<Vec<_>>>()?;
                    if agg.func.supports_within_group_clause() {
                        // Ordered-set aggregates carry the ordered value as their
                        // first argument, so skip it.
                        (self.function_args_to_sql(&args[1..])?, order_by_sql, vec![])
                    } else {
                        (self.function_args_to_sql(args)?, vec![], order_by_sql)
                    }
                };

                let filter = match filter {
                    Some(filter) => Some(self.expr_to_sql_inner(filter)?),
                    None => None,
                };
                Ok(function_with_options(
                    func_name,
                    args_to_use,
                    FunctionOptions {
                        quantifier: if *distinct {
                            ast::SetQuantifier::Distinct
                        } else {
                            ast::SetQuantifier::None
                        },
                        order_by,
                        within_group,
                        filter,
                        null_treatment: null_treatment.map(null_treatment_to_sql),
                        ..function_options()
                    },
                ))
            }
            Expr::ScalarSubquery(subq) => {
                let sub_query = self.subquery_to_sql(subq.subquery.as_ref())?;
                Ok(ExprKind::Subquery(sub_query).into())
            }
            Expr::InSubquery(insubq) => {
                let inexpr = Box::new(self.expr_to_sql_inner(insubq.expr.as_ref())?);
                let sub_query =
                    self.subquery_to_sql(insubq.subquery.subquery.as_ref())?;
                Ok(ExprKind::InSubquery {
                    expr: inexpr,
                    subquery: sub_query,
                    negated: insubq.negated,
                }
                .into())
            }
            Expr::SetComparison(set_cmp) => {
                let left = self.expr_to_sql_inner(set_cmp.expr.as_ref())?;
                let sub_query =
                    self.subquery_to_sql(set_cmp.subquery.subquery.as_ref())?;
                let op = self.op_to_sql(&set_cmp.op)?;
                let quantifier = match set_cmp.quantifier {
                    SetQuantifier::Any => Quantifier::Any,
                    SetQuantifier::All => Quantifier::All,
                };
                Ok(special(SpecialExpr::Quantified {
                    left,
                    op,
                    quantifier,
                    right: ExprKind::Subquery(sub_query).into(),
                }))
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let sub_query = self.subquery_to_sql(subquery.subquery.as_ref())?;
                let exists = ExprKind::Exists(sub_query).into();
                if *negated {
                    Ok(unary(UnaryOp::Not, exists))
                } else {
                    Ok(exists)
                }
            }
            Expr::IsNull(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::Null, false))
            }
            Expr::IsNotNull(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::Null, true))
            }
            Expr::IsTrue(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::True, false))
            }
            Expr::IsNotTrue(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::True, true))
            }
            Expr::IsFalse(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::False, false))
            }
            Expr::IsNotFalse(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::False, true))
            }
            Expr::IsUnknown(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::Unknown, false))
            }
            Expr::IsNotUnknown(expr) => {
                Ok(is(self.expr_to_sql_inner(expr)?, IsTest::Unknown, true))
            }
            Expr::Not(expr) => Ok(unary(UnaryOp::Not, self.expr_to_sql_inner(expr)?)),
            Expr::Negative(expr) => {
                Ok(unary(UnaryOp::Neg, self.expr_to_sql_inner(expr)?))
            }
            Expr::ScalarVariable(_, ids) => {
                assert_or_internal_err!(!ids.is_empty(), "Not a valid ScalarVariable");

                Ok(name_expr(
                    ids.iter()
                        .map(|i| self.new_ident_without_quote_style(i.to_string()))
                        .collect(),
                ))
            }
            Expr::TryCast(TryCast { expr, field }) => {
                let inner_expr = self.expr_to_sql_inner(expr)?;
                Ok(cast(
                    inner_expr,
                    self.arrow_dtype_to_ast_dtype(field)?,
                    CastStyle::TryFunction,
                ))
            }
            // TODO: unparsing wildcard addition options
            // A wildcard is not an expression in SQL, so it is written as a
            // name whose last part is `*`.
            #[expect(deprecated)]
            Expr::Wildcard { qualifier, .. } => {
                let mut parts: Vec<Ident> = qualifier
                    .iter()
                    .flat_map(|qualifier| qualifier.to_vec())
                    .map(ident)
                    .collect();
                parts.push(ident("*"));
                Ok(name_expr(parts))
            }
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::GroupingSets(grouping_sets) => {
                    let sets = grouping_sets
                        .iter()
                        .map(|set| {
                            set.iter()
                                .map(|e| self.expr_to_sql_inner(e))
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(special(SpecialExpr::Grouping {
                        kind: GroupingKind::Sets,
                        sets,
                    }))
                }
                GroupingSet::Cube(cube) => {
                    let sets = cube
                        .iter()
                        .map(|e| Ok(vec![self.expr_to_sql_inner(e)?]))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(special(SpecialExpr::Grouping {
                        kind: GroupingKind::Cube,
                        sets,
                    }))
                }
                GroupingSet::Rollup(rollup) => {
                    let sets = rollup
                        .iter()
                        .map(|e| Ok(vec![self.expr_to_sql_inner(e)?]))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(special(SpecialExpr::Grouping {
                        kind: GroupingKind::Rollup,
                        sets,
                    }))
                }
            },
            Expr::Placeholder(p) => {
                Ok(ExprKind::Placeholder(Cow::Owned(p.id.to_string())).into())
            }
            Expr::OuterReferenceColumn(_, col) => self.col_to_sql(col),
            Expr::Unnest(unnest) => self.unnest_to_sql(unnest),
            Expr::HigherOrderFunction(HigherOrderFunction { func, args }) => {
                let func_name = func.name();

                if let Some(expr) = self
                    .dialect
                    .higher_order_function_to_sql_overrides(self, func_name, args)?
                {
                    return Ok(expr);
                }

                self.function_to_sql_internal(func_name, args)
            }
            Expr::Lambda(Lambda { params, body }) => Ok(special(SpecialExpr::Lambda {
                params: params
                    .iter()
                    .map(|param| self.new_ident_quoted_if_needs(param.clone()))
                    .collect(),
                parenthesized: true,
                body: self.expr_to_sql_inner(body)?,
            })),
            Expr::LambdaVariable(l) => Ok(ExprKind::Ident(
                self.new_ident_quoted_if_needs(l.name.clone()),
            )
            .into()),
        })
    }

    /// Unparses the plan of a subquery, which must be a query.
    fn subquery_to_sql(
        &self,
        plan: &datafusion_expr::LogicalPlan,
    ) -> Result<Box<ast::Query<'static>>> {
        let sub_statement = self.plan_to_sql(plan)?;
        let ast::StatementKind::Query(sub_query) = sub_statement.kind else {
            return plan_err!("Subquery must be a Query, but found {sub_statement:?}");
        };
        Ok(sub_query)
    }

    pub fn scalar_function_to_sql(
        &self,
        func_name: &str,
        args: &[Expr],
    ) -> Result<ast::Expr<'static>> {
        match func_name {
            "make_array" => self.make_array_to_sql(args),
            "array_element" => self.array_element_to_sql(args),
            "named_struct" => self.named_struct_to_sql(args),
            "get_field" => self.get_field_to_sql(args),
            "map" => self.map_to_sql(args),
            // TODO: support for the construct and access functions of the `map` type
            _ => self.function_to_sql_internal(func_name, args),
        }
    }

    fn function_to_sql_internal(
        &self,
        func_name: &str,
        args: &[Expr],
    ) -> Result<SqlExpr> {
        let args = self.function_args_to_sql(args)?;
        Ok(function(func_name, args))
    }

    /// An array literal: `ARRAY[...]`, or `[...]` in dialects without the keyword.
    fn array_literal(&self, elements: Vec<SqlExpr>) -> SqlExpr {
        if self.dialect.use_array_keyword_for_array_literals() {
            special(SpecialExpr::Array(elements))
        } else {
            ExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::List(elements))).into()
        }
    }

    fn make_array_to_sql(&self, args: &[Expr]) -> Result<SqlExpr> {
        let args = args
            .iter()
            .map(|e| self.expr_to_sql_with_nesting(e))
            .collect::<Result<Vec<_>>>()?;
        Ok(self.array_literal(args))
    }

    fn scalar_value_list_to_sql(&self, array: &ArrayRef) -> Result<SqlExpr> {
        let mut elem = Vec::with_capacity(array.len());
        for i in 0..array.len() {
            let value = ScalarValue::try_from_array(&array, i)?;
            elem.push(self.scalar_to_sql(&value)?);
        }

        Ok(self.array_literal(elem))
    }

    fn array_element_to_sql(&self, args: &[Expr]) -> Result<SqlExpr> {
        assert_eq_or_internal_err!(
            args.len(),
            2,
            "array_element must have exactly 2 arguments"
        );
        let array = self.expr_to_sql_with_nesting(&args[0])?;
        let index = self.expr_to_sql_with_nesting(&args[1])?;
        Ok(special(SpecialExpr::Subscript { expr: array, index }))
    }

    fn named_struct_to_sql(&self, args: &[Expr]) -> Result<SqlExpr> {
        assert_or_internal_err!(
            args.len().is_multiple_of(2),
            "named_struct must have an even number of arguments"
        );

        let args = args
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[name, value]| {
                let key = match name {
                    Expr::Literal(ScalarValue::Utf8(Some(s)), _) => self.new_ident_quoted_if_needs(s.to_string()),
                    _ => return internal_err!("named_struct expects even arguments to be strings, but received: {name:?}")
                };

                Ok((key, self.expr_to_sql_with_nesting(value)?))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::Struct(args))).into())
    }

    fn get_field_to_sql(&self, args: &[Expr]) -> Result<SqlExpr> {
        if args.len() < 2 {
            return internal_err!(
                "get_field must have at least 2 arguments, got {}",
                args.len()
            );
        }

        // Extract all field names (args[1..])
        let mut fields = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            let field = match arg {
                Expr::Literal(lit, _) => self.new_ident_quoted_if_needs(lit.to_string()),
                _ => {
                    return internal_err!(
                        "get_field expects field arguments to be strings, but received: {:?}",
                        arg
                    );
                }
            };
            fields.push(field);
        }

        match &args[0] {
            Expr::Column(col) => {
                let mut id = match self.col_to_sql(col)?.kind {
                    ExprKind::Ident(ident) => vec![ident],
                    ExprKind::Compound(name) => name.parts,
                    other => {
                        return internal_err!(
                            "expected col_to_sql to return an Identifier or CompoundIdentifier, but received: {:?}",
                            other
                        );
                    }
                };
                id.extend(fields);
                Ok(name_expr(id))
            }
            Expr::ScalarFunction(struct_expr) => {
                let root = self
                    .scalar_function_to_sql(struct_expr.func.name(), &struct_expr.args)?;
                Ok(fields.into_iter().fold(root, |expr, field| {
                    special(SpecialExpr::FieldAccess { expr, field })
                }))
            }
            _ => {
                internal_err!(
                    "get_field expects first argument to be column or scalar function, but received: {:?}",
                    &args[0]
                )
            }
        }
    }

    fn map_to_sql(&self, args: &[Expr]) -> Result<SqlExpr> {
        assert_eq_or_internal_err!(args.len(), 2, "map must have exactly 2 arguments");

        let Some(keys) = array_elements(self.expr_to_sql_with_nesting(&args[0])?) else {
            return internal_err!(
                "map expects first argument to be an array, but received: {:?}",
                &args[0]
            );
        };

        let Some(values) = array_elements(self.expr_to_sql_with_nesting(&args[1])?)
        else {
            return internal_err!(
                "map expects second argument to be an array, but received: {:?}",
                &args[1]
            );
        };

        let entries = keys.into_iter().zip(values).collect();

        Ok(ExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::Map(entries))).into())
    }

    pub fn sort_to_sql(&self, sort: &Sort) -> Result<OrderByItem<'static>> {
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

        Ok(OrderByItem {
            expr: sql_parser_expr,
            asc: Some(*asc),
            nulls_first,
            using: None,
            ext: ClickHouseOrderByItem::default(),
        })
    }

    fn ast_type_for_date64_in_cast(&self) -> ast::DataType<'static> {
        if self.dialect.use_timestamp_for_date64() {
            data_type("TIMESTAMP")
        } else {
            data_type("DATETIME")
        }
    }

    pub fn col_to_sql(&self, col: &Column) -> Result<ast::Expr<'static>> {
        // Replace the column name if the dialect has an override
        let col_name =
            if let Some(rewritten_name) = self.dialect.col_alias_overrides(&col.name)? {
                rewritten_name
            } else {
                col.name.to_string()
            };

        if let Some(table_ref) = &col.relation {
            let mut id = if self.dialect.full_qualified_col() {
                table_ref.to_vec()
            } else {
                vec![table_ref.table().to_string()]
            };
            id.push(col_name);
            return Ok(ExprKind::Compound(object_name(
                id.into_iter()
                    .map(|i| self.new_ident_quoted_if_needs(i))
                    .collect(),
            ))
            .into());
        }
        Ok(ExprKind::Ident(self.new_ident_quoted_if_needs(col_name)).into())
    }

    fn convert_bound(
        &self,
        bound: &datafusion_expr::window_frame::WindowFrameBound,
    ) -> Result<FrameBound<'static>> {
        match bound {
            datafusion_expr::window_frame::WindowFrameBound::Preceding(val) => {
                let val = self.scalar_to_sql(val)?;
                if matches!(val.kind, ExprKind::Literal(Literal::Null)) {
                    Ok(FrameBound::UnboundedPreceding)
                } else {
                    Ok(FrameBound::Preceding(val))
                }
            }
            datafusion_expr::window_frame::WindowFrameBound::Following(val) => {
                let val = self.scalar_to_sql(val)?;
                if matches!(val.kind, ExprKind::Literal(Literal::Null)) {
                    Ok(FrameBound::UnboundedFollowing)
                } else {
                    Ok(FrameBound::Following(val))
                }
            }
            datafusion_expr::window_frame::WindowFrameBound::CurrentRow => {
                Ok(FrameBound::CurrentRow)
            }
        }
    }

    pub(crate) fn function_args_to_sql(
        &self,
        args: &[Expr],
    ) -> Result<Vec<FunctionArg<'static>>> {
        args.iter()
            .map(|e| {
                #[expect(deprecated)]
                if matches!(
                    e,
                    Expr::Wildcard {
                        qualifier: None,
                        ..
                    }
                ) {
                    Ok(FunctionArg::Wildcard)
                } else {
                    self.expr_to_sql_with_nesting(e).map(FunctionArg::Expr)
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    /// This function can create an identifier with or without quotes based on the dialect rules
    pub(super) fn new_ident_quoted_if_needs(&self, ident: String) -> Ident<'static> {
        let quote_style = self.dialect.identifier_quote_style(&ident);
        super::ast::quoted_ident(ident, quote_style)
    }

    pub(super) fn new_ident_without_quote_style(&self, str: String) -> Ident<'static> {
        ident(str)
    }

    pub(super) fn binary_op_to_sql(
        &self,
        lhs: SqlExpr,
        rhs: SqlExpr,
        op: BinaryOp<'static>,
    ) -> SqlExpr {
        binary(lhs, op, rhs)
    }

    /// Given an expression of the form `((a + b) * (c * d))`,
    /// the parenthesis is redundant if the precedence of the nested expression is already higher
    /// than the surrounding operators' precedence. The above expression would become
    /// `(a + b) * c * d`.
    ///
    /// Also note that when fetching the precedence of a nested expression, we ignore other nested
    /// expressions, so precedence of expr `(a * (b + c))` equals `*` and not `+`.
    fn remove_unnecessary_nesting(
        &self,
        expr: SqlExpr,
        left_op: &BinaryOp<'static>,
        right_op: &BinaryOp<'static>,
    ) -> SqlExpr {
        crate::stack::maybe_grow(|| match expr.kind {
            ExprKind::Nested(nested_expr) => {
                let surrounding_precedence = self
                    .sql_op_precedence(left_op)
                    .max(self.sql_op_precedence(right_op));

                let inner_precedence = self.inner_precedence(&nested_expr);

                let not_associative = matches!(left_op, BinaryOp::Sub | BinaryOp::Div);

                if inner_precedence == surrounding_precedence && not_associative {
                    nested(self.remove_unnecessary_nesting(*nested_expr, LOWEST, LOWEST))
                } else if inner_precedence >= surrounding_precedence {
                    self.remove_unnecessary_nesting(*nested_expr, left_op, right_op)
                } else {
                    nested(self.remove_unnecessary_nesting(*nested_expr, LOWEST, LOWEST))
                }
            }
            ExprKind::Binary { left, op, right } => {
                let left = self.remove_unnecessary_nesting(*left, left_op, &op);
                let right = self.remove_unnecessary_nesting(*right, &op, right_op);
                binary(left, op, right)
            }
            ExprKind::Is {
                expr: inner,
                test:
                    test @ (IsTest::True | IsTest::False | IsTest::Null | IsTest::Unknown),
                negated,
            } => is(
                self.remove_unnecessary_nesting(*inner, left_op, IS),
                test,
                negated,
            ),
            kind => kind.into(),
        })
    }

    fn inner_precedence(&self, expr: &SqlExpr) -> u8 {
        match &expr.kind {
            ExprKind::Nested(_) | ExprKind::Ident(_) | ExprKind::Literal(_) => 100,
            ExprKind::Binary { op, .. } => self.sql_op_precedence(op),
            // Closest precedence we currently have to Between is PGLikeMatch
            // (https://www.postgresql.org/docs/7.2/sql-precedence.html)
            ExprKind::Between { .. } => Operator::LikeMatch.precedence(),
            _ => 0,
        }
    }

    pub(super) fn between_op_to_sql(
        &self,
        expr: SqlExpr,
        negated: bool,
        low: SqlExpr,
        high: SqlExpr,
    ) -> SqlExpr {
        ExprKind::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }
        .into()
    }

    fn sql_op_precedence(&self, op: &BinaryOp) -> u8 {
        match self.sql_to_op(op) {
            Ok(op) => op.precedence(),
            Err(_) => 0,
        }
    }

    fn sql_to_op(&self, op: &BinaryOp) -> Result<Operator> {
        match op {
            BinaryOp::Eq => Ok(Operator::Eq),
            BinaryOp::NotEq => Ok(Operator::NotEq),
            BinaryOp::Lt => Ok(Operator::Lt),
            BinaryOp::LtEq => Ok(Operator::LtEq),
            BinaryOp::Gt => Ok(Operator::Gt),
            BinaryOp::GtEq => Ok(Operator::GtEq),
            BinaryOp::Add => Ok(Operator::Plus),
            BinaryOp::Sub => Ok(Operator::Minus),
            BinaryOp::Mul => Ok(Operator::Multiply),
            BinaryOp::Div => Ok(Operator::Divide),
            BinaryOp::Mod => Ok(Operator::Modulo),
            BinaryOp::And => Ok(Operator::And),
            BinaryOp::Or => Ok(Operator::Or),
            BinaryOp::BitAnd => Ok(Operator::BitwiseAnd),
            BinaryOp::BitOr => Ok(Operator::BitwiseOr),
            BinaryOp::BitXor => Ok(Operator::BitwiseXor),
            BinaryOp::ShiftRight => Ok(Operator::BitwiseShiftRight),
            BinaryOp::ShiftLeft => Ok(Operator::BitwiseShiftLeft),
            BinaryOp::StringConcat => Ok(Operator::StringConcat),
            BinaryOp::Named(name) => match name.as_ref() {
                "~" => Ok(Operator::RegexMatch),
                "~*" => Ok(Operator::RegexIMatch),
                "!~" => Ok(Operator::RegexNotMatch),
                "!~*" => Ok(Operator::RegexNotIMatch),
                "~~*" => Ok(Operator::ILikeMatch),
                "!~~" => Ok(Operator::NotLikeMatch),
                "~~" => Ok(Operator::LikeMatch),
                "!~~*" => Ok(Operator::NotILikeMatch),
                "@>" => Ok(Operator::AtArrow),
                "<@" => Ok(Operator::ArrowAt),
                "->" => Ok(Operator::Arrow),
                "->>" => Ok(Operator::LongArrow),
                "#>" => Ok(Operator::HashArrow),
                "#>>" => Ok(Operator::HashLongArrow),
                "@@" => Ok(Operator::AtAt),
                "//" | "DIV" => Ok(Operator::IntegerDivide),
                "#-" => Ok(Operator::HashMinus),
                "@?" => Ok(Operator::AtQuestion),
                "?" => Ok(Operator::Question),
                "?&" => Ok(Operator::QuestionAnd),
                "?|" => Ok(Operator::QuestionPipe),
                _ => not_impl_err!("unsupported operation: {op:?}"),
            },
        }
    }

    fn op_to_sql(&self, op: &Operator) -> Result<BinaryOp<'static>> {
        let named = |name: &'static str| Ok(BinaryOp::Named(Cow::Borrowed(name)));
        match op {
            Operator::Eq => Ok(BinaryOp::Eq),
            Operator::NotEq => Ok(BinaryOp::NotEq),
            Operator::Lt => Ok(BinaryOp::Lt),
            Operator::LtEq => Ok(BinaryOp::LtEq),
            Operator::Gt => Ok(BinaryOp::Gt),
            Operator::GtEq => Ok(BinaryOp::GtEq),
            Operator::Plus => Ok(BinaryOp::Add),
            Operator::Minus => Ok(BinaryOp::Sub),
            Operator::Multiply => Ok(BinaryOp::Mul),
            Operator::Divide => Ok(self.dialect.division_operator()),
            Operator::Modulo => Ok(BinaryOp::Mod),
            Operator::And => Ok(BinaryOp::And),
            Operator::Or => Ok(BinaryOp::Or),
            Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::RegexMatch => named("~"),
            Operator::RegexIMatch => named("~*"),
            Operator::RegexNotMatch => named("!~"),
            Operator::RegexNotIMatch => named("!~*"),
            Operator::ILikeMatch => named("~~*"),
            Operator::NotLikeMatch => named("!~~"),
            Operator::LikeMatch => named("~~"),
            Operator::NotILikeMatch => named("!~~*"),
            Operator::BitwiseAnd => Ok(BinaryOp::BitAnd),
            Operator::BitwiseOr => Ok(BinaryOp::BitOr),
            Operator::BitwiseXor => Ok(BinaryOp::BitXor),
            Operator::BitwiseShiftRight => Ok(BinaryOp::ShiftRight),
            Operator::BitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
            Operator::StringConcat => Ok(BinaryOp::StringConcat),
            Operator::AtArrow => named("@>"),
            Operator::ArrowAt => named("<@"),
            Operator::Arrow => named("->"),
            Operator::LongArrow => named("->>"),
            Operator::HashArrow => named("#>"),
            Operator::HashLongArrow => named("#>>"),
            Operator::AtAt => named("@@"),
            Operator::IntegerDivide => named("//"),
            Operator::HashMinus => named("#-"),
            Operator::AtQuestion => named("@?"),
            Operator::Question => named("?"),
            Operator::QuestionAnd => named("?&"),
            Operator::QuestionPipe => named("?|"),
            Operator::Colon => named(":"),
        }
    }

    fn handle_timestamp<T: ArrowTemporalType>(
        &self,
        v: &ScalarValue,
        tz: Option<&Arc<str>>,
    ) -> Result<SqlExpr>
    where
        i64: From<T::Native>,
    {
        let DataType::Timestamp(time_unit, _) = T::DATA_TYPE else {
            return Err(internal_datafusion_err!(
                "Expected Timestamp, got {:?}",
                T::DATA_TYPE
            ));
        };

        let ts = if let Some(tz) = tz {
            let dt = v
                .to_array()?
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or(internal_datafusion_err!(
                    "Failed to downcast type {v:?} to arrow array"
                ))?
                .value_as_datetime_with_tz(0, tz.parse()?)
                .ok_or(internal_datafusion_err!(
                    "Unable to convert {v:?} to DateTime"
                ))?;
            self.dialect.timestamp_with_tz_to_string(dt, time_unit)
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

        Ok(cast(
            string(&ts),
            self.dialect.timestamp_cast_dtype(&time_unit, &tz.cloned()),
            CastStyle::Function,
        ))
    }

    fn handle_time<T: ArrowTemporalType>(&self, v: &ScalarValue) -> Result<SqlExpr>
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
        Ok(cast(string(&time), data_type("TIME"), CastStyle::Function))
    }

    // Explicit type cast on ast::Expr::Value is not needed by underlying engine for certain types
    // For example: CAST(Utf8("binary_value") AS Binary) and  CAST(Utf8("dictionary_value") AS Dictionary)
    fn cast_to_sql(&self, expr: &Expr, field: &FieldRef) -> Result<SqlExpr> {
        let inner_expr = self.expr_to_sql_inner(expr)?;
        let data_type = field.data_type();
        match inner_expr.kind {
            ExprKind::Literal(_) => match data_type {
                DataType::Dictionary(_, _) | DataType::Binary | DataType::BinaryView
                    if field.metadata().is_empty() =>
                {
                    Ok(inner_expr)
                }
                _ => Ok(cast(
                    inner_expr,
                    self.arrow_dtype_to_ast_dtype(field)?,
                    CastStyle::Function,
                )),
            },
            _ => Ok(cast(
                inner_expr,
                self.arrow_dtype_to_ast_dtype(field)?,
                CastStyle::Function,
            )),
        }
    }

    /// DataFusion ScalarValues sometimes require a ast::Expr to construct.
    /// For example ScalarValue::Date32(d) corresponds to the ast::Expr CAST('datestr' as DATE)
    fn scalar_to_sql(&self, v: &ScalarValue) -> Result<SqlExpr> {
        match v {
            ScalarValue::Null => Ok(null()),
            ScalarValue::Boolean(Some(b)) => {
                Ok(ExprKind::Literal(Literal::Boolean(*b)).into())
            }
            ScalarValue::Boolean(None) => Ok(null()),
            ScalarValue::Float16(Some(f)) => Ok(number(f.to_string())),
            ScalarValue::Float16(None) => Ok(null()),
            ScalarValue::Float32(Some(f)) => {
                let f_val = match f.fract() {
                    0.0 => format!("{f:.1}"),
                    _ => format!("{f}"),
                };
                Ok(number(f_val))
            }
            ScalarValue::Float32(None) => Ok(null()),
            ScalarValue::Float64(Some(f)) => {
                let f_val = match f.fract() {
                    0.0 => format!("{f:.1}"),
                    _ => format!("{f}"),
                };
                Ok(number(f_val))
            }
            ScalarValue::Float64(None) => Ok(null()),
            ScalarValue::Decimal32(Some(value), precision, scale) => Ok(number(
                Decimal32Type::format_decimal(*value, *precision, *scale),
            )),
            ScalarValue::Decimal32(None, ..) => Ok(null()),
            ScalarValue::Decimal64(Some(value), precision, scale) => Ok(number(
                Decimal64Type::format_decimal(*value, *precision, *scale),
            )),
            ScalarValue::Decimal64(None, ..) => Ok(null()),
            ScalarValue::Decimal128(Some(value), precision, scale) => Ok(number(
                Decimal128Type::format_decimal(*value, *precision, *scale),
            )),
            ScalarValue::Decimal128(None, ..) => Ok(null()),
            ScalarValue::Decimal256(Some(value), precision, scale) => Ok(number(
                Decimal256Type::format_decimal(*value, *precision, *scale),
            )),
            ScalarValue::Decimal256(None, ..) => Ok(null()),
            ScalarValue::Int8(Some(i)) => Ok(number(i.to_string())),
            ScalarValue::Int8(None) => Ok(null()),
            ScalarValue::Int16(Some(i)) => Ok(number(i.to_string())),
            ScalarValue::Int16(None) => Ok(null()),
            ScalarValue::Int32(Some(i)) => Ok(number(i.to_string())),
            ScalarValue::Int32(None) => Ok(null()),
            ScalarValue::Int64(Some(i)) => Ok(number(i.to_string())),
            ScalarValue::Int64(None) => Ok(null()),
            ScalarValue::UInt8(Some(ui)) => Ok(number(ui.to_string())),
            ScalarValue::UInt8(None) => Ok(null()),
            ScalarValue::UInt16(Some(ui)) => Ok(number(ui.to_string())),
            ScalarValue::UInt16(None) => Ok(null()),
            ScalarValue::UInt32(Some(ui)) => Ok(number(ui.to_string())),
            ScalarValue::UInt32(None) => Ok(null()),
            ScalarValue::UInt64(Some(ui)) => Ok(number(ui.to_string())),
            ScalarValue::UInt64(None) => Ok(null()),
            ScalarValue::Utf8(Some(str))
            | ScalarValue::Utf8View(Some(str))
            | ScalarValue::LargeUtf8(Some(str)) => {
                if let Some(expr) = self.dialect.string_literal_to_sql(str) {
                    return Ok(expr);
                }
                Ok(string(str))
            }
            ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None) => Ok(null()),
            ScalarValue::Binary(Some(bin))
            | ScalarValue::BinaryView(Some(bin))
            | ScalarValue::LargeBinary(Some(bin))
            | ScalarValue::FixedSizeBinary(_, Some(bin)) => {
                let hex = bin
                    .iter()
                    .flat_map(|x| {
                        const HEX: [char; 16] = [
                            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
                            'c', 'd', 'e', 'f',
                        ];
                        let (hi, lo) = (((*x >> 4) & 0xfu8), (*x & 0xfu8));
                        [HEX[hi as usize], HEX[lo as usize]]
                    })
                    .collect::<String>();
                Ok(ExprKind::Literal(Literal::HexString(Cow::Owned(hex))).into())
            }
            ScalarValue::Binary(None)
            | ScalarValue::BinaryView(None)
            | ScalarValue::FixedSizeBinary(_, None)
            | ScalarValue::LargeBinary(None) => Ok(null()),
            ScalarValue::FixedSizeList(a) => self.scalar_value_list_to_sql(a.values()),
            ScalarValue::List(a) => self.scalar_value_list_to_sql(a.values()),
            ScalarValue::LargeList(a) => self.scalar_value_list_to_sql(a.values()),
            ScalarValue::ListView(a) => self.scalar_value_list_to_sql(a.values()),
            ScalarValue::LargeListView(a) => self.scalar_value_list_to_sql(a.values()),
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

                Ok(cast(
                    string(&date.to_string()),
                    data_type("DATE"),
                    CastStyle::Function,
                ))
            }
            ScalarValue::Date32(None) => Ok(null()),
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

                Ok(cast(
                    string(&datetime.to_string()),
                    self.ast_type_for_date64_in_cast(),
                    CastStyle::Function,
                ))
            }
            ScalarValue::Date64(None) => Ok(null()),
            ScalarValue::Time32Second(Some(_t)) => {
                self.handle_time::<Time32SecondType>(v)
            }
            ScalarValue::Time32Second(None) => Ok(null()),
            ScalarValue::Time32Millisecond(Some(_t)) => {
                self.handle_time::<Time32MillisecondType>(v)
            }
            ScalarValue::Time32Millisecond(None) => Ok(null()),
            ScalarValue::Time64Microsecond(Some(_t)) => {
                self.handle_time::<Time64MicrosecondType>(v)
            }
            ScalarValue::Time64Microsecond(None) => Ok(null()),
            ScalarValue::Time64Nanosecond(Some(_t)) => {
                self.handle_time::<Time64NanosecondType>(v)
            }
            ScalarValue::Time64Nanosecond(None) => Ok(null()),
            ScalarValue::TimestampSecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampSecondType>(v, tz.as_ref())
            }
            ScalarValue::TimestampSecond(None, _) => Ok(null()),
            ScalarValue::TimestampMillisecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampMillisecondType>(v, tz.as_ref())
            }
            ScalarValue::TimestampMillisecond(None, _) => Ok(null()),
            ScalarValue::TimestampMicrosecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampMicrosecondType>(v, tz.as_ref())
            }
            ScalarValue::TimestampMicrosecond(None, _) => Ok(null()),
            ScalarValue::TimestampNanosecond(Some(_ts), tz) => {
                self.handle_timestamp::<TimestampNanosecondType>(v, tz.as_ref())
            }
            ScalarValue::TimestampNanosecond(None, _) => Ok(null()),
            ScalarValue::IntervalYearMonth(Some(_))
            | ScalarValue::IntervalDayTime(Some(_))
            | ScalarValue::IntervalMonthDayNano(Some(_)) => {
                self.interval_scalar_to_sql(v)
            }
            ScalarValue::IntervalYearMonth(None) => Ok(null()),
            ScalarValue::IntervalDayTime(None) => Ok(null()),
            ScalarValue::IntervalMonthDayNano(None) => Ok(null()),
            ScalarValue::DurationSecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationSecond(None) => Ok(null()),
            ScalarValue::DurationMillisecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMillisecond(None) => Ok(null()),
            ScalarValue::DurationMicrosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMicrosecond(None) => Ok(null()),
            ScalarValue::DurationNanosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationNanosecond(None) => Ok(null()),
            ScalarValue::Struct(_) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Map(_) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Union(..) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Dictionary(_k, v) => self.scalar_to_sql(v),
            ScalarValue::RunEndEncoded(_, _, v) => self.scalar_to_sql(v),
        }
    }

    /// MySQL requires INTERVAL sql to be in the format: INTERVAL 1 YEAR + INTERVAL 1 MONTH + INTERVAL 1 DAY etc
    /// `<https://dev.mysql.com/doc/refman/8.4/en/expressions.html#temporal-intervals>`
    /// Interval sequence can't be wrapped in brackets - (INTERVAL 1 YEAR + INTERVAL 1 MONTH ...) so we need to generate
    /// a single INTERVAL expression so it works correct for interval subtraction cases
    /// MySQL supports the DAY_MICROSECOND unit type (format is DAYS HOURS:MINUTES:SECONDS.MICROSECONDS), but it is not supported by sqlparser
    /// so we calculate the best single interval to represent the provided duration
    fn interval_to_mysql_expr(
        &self,
        months: i32,
        days: i32,
        microseconds: i64,
    ) -> Result<SqlExpr> {
        // MONTH only
        if months != 0 && days == 0 && microseconds == 0 {
            return Ok(interval(number(months.to_string()), Some("MONTH"), None));
        } else if months != 0 {
            return not_impl_err!(
                "Unsupported Interval scalar with both Month and DayTime for IntervalStyle::MySQL"
            );
        }

        // DAY only
        if microseconds == 0 {
            return Ok(interval(number(days.to_string()), Some("DAY"), None));
        }

        // Calculate the best single interval to represent the provided days and microseconds

        let microseconds = microseconds + (days as i64 * 24 * 60 * 60 * 1_000_000);

        if microseconds % 1_000_000 != 0 {
            return Ok(interval(
                number(microseconds.to_string()),
                Some("MICROSECOND"),
                None,
            ));
        }

        let secs = microseconds / 1_000_000;

        if secs % 60 != 0 {
            return Ok(interval(number(secs.to_string()), Some("SECOND"), None));
        }

        let mins = secs / 60;

        if mins % 60 != 0 {
            return Ok(interval(number(mins.to_string()), Some("MINUTE"), None));
        }

        let hours = mins / 60;

        if hours % 24 != 0 {
            return Ok(interval(number(hours.to_string()), Some("HOUR"), None));
        }

        let days = hours / 24;

        Ok(interval(number(days.to_string()), Some("DAY"), None))
    }

    fn interval_scalar_to_sql(&self, v: &ScalarValue) -> Result<SqlExpr> {
        match self.dialect.interval_style() {
            IntervalStyle::PostgresVerbose => {
                let wrap_array = v.to_array()?;
                let Some(result) = array_value_to_string(&wrap_array, 0).ok() else {
                    return internal_err!(
                        "Unable to convert interval scalar value to string"
                    );
                };
                Ok(interval(string(&result.to_uppercase()), None, None))
            }
            // If the interval standard is SQLStandard, implement a simple unparse logic
            IntervalStyle::SQLStandard => match v {
                ScalarValue::IntervalYearMonth(Some(v)) => {
                    Ok(interval(string(&v.to_string()), Some("MONTH"), None))
                }
                ScalarValue::IntervalDayTime(Some(v)) => {
                    let days = v.days;
                    let secs = v.milliseconds / 1_000;
                    let mins = secs / 60;
                    let hours = mins / 60;

                    let secs = secs - (mins * 60);
                    let mins = mins - (hours * 60);

                    let millis = v.milliseconds % 1_000;
                    Ok(interval(
                        string(&format!("{days} {hours}:{mins}:{secs}.{millis:3}")),
                        Some("DAY"),
                        Some("SECOND"),
                    ))
                }
                ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    if v.months >= 0 && v.days == 0 && v.nanoseconds == 0 {
                        Ok(interval(string(&v.months.to_string()), Some("MONTH"), None))
                    } else if v.months == 0 && v.nanoseconds % 1_000_000 == 0 {
                        let days = v.days;
                        let secs = v.nanoseconds / 1_000_000_000;
                        let mins = secs / 60;
                        let hours = mins / 60;

                        let secs = secs - (mins * 60);
                        let mins = mins - (hours * 60);

                        let millis = (v.nanoseconds % 1_000_000_000) / 1_000_000;

                        Ok(interval(
                            string(&format!("{days} {hours}:{mins}:{secs}.{millis:03}")),
                            Some("DAY"),
                            Some("SECOND"),
                        ))
                    } else {
                        not_impl_err!(
                            "Unsupported IntervalMonthDayNano scalar with both Month and DayTime for IntervalStyle::SQLStandard"
                        )
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

    /// Converts an UNNEST operation to an AST expression by wrapping it as a function call,
    /// since there is no direct representation for UNNEST in the AST.
    fn unnest_to_sql(&self, unnest: &Unnest) -> Result<SqlExpr> {
        let args = self.function_args_to_sql(std::slice::from_ref(&unnest.expr))?;
        Ok(function("UNNEST", args))
    }

    fn arrow_dtype_to_ast_dtype(
        &self,
        field: &FieldRef,
    ) -> Result<ast::DataType<'static>> {
        let data_type = field.data_type();
        match data_type {
            DataType::Null => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Boolean => Ok(super::ast::data_type("BOOL")),
            DataType::Int8 => Ok(self.dialect.int8_cast_dtype()),
            DataType::Int16 => Ok(self.dialect.int16_cast_dtype()),
            DataType::Int32 => Ok(self.dialect.int32_cast_dtype()),
            DataType::Int64 => Ok(self.dialect.int64_cast_dtype()),
            DataType::UInt8 => Ok(self.dialect.uint8_cast_dtype()),
            DataType::UInt16 => Ok(self.dialect.uint16_cast_dtype()),
            DataType::UInt32 => Ok(self.dialect.uint32_cast_dtype()),
            DataType::UInt64 => Ok(self.dialect.uint64_cast_dtype()),
            DataType::Float16 => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Float32 => Ok(super::ast::data_type("FLOAT")),
            DataType::Float64 => Ok(self.dialect.float64_ast_dtype()),
            DataType::Timestamp(time_unit, tz) => {
                Ok(self.dialect.timestamp_cast_dtype(time_unit, tz))
            }
            DataType::Date32 => Ok(self.dialect.date32_cast_dtype()),
            DataType::Date64 => Ok(self.ast_type_for_date64_in_cast()),
            DataType::Time32(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Time64(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Duration(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Interval(_) => Ok(super::ast::data_type("INTERVAL")),
            DataType::Binary => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::FixedSizeBinary(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::LargeBinary => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::BinaryView => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Utf8 => Ok(self.dialect.utf8_cast_dtype()),
            DataType::LargeUtf8 => Ok(self.dialect.large_utf8_cast_dtype()),
            DataType::Utf8View => Ok(self.dialect.utf8_cast_dtype()),
            DataType::List(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::FixedSizeList(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::LargeList(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::ListView(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::LargeListView(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Struct(_) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Union(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
            DataType::Dictionary(_, val) => {
                self.arrow_dtype_to_ast_dtype(&val.clone().into_nullable_field_ref())
            }
            DataType::RunEndEncoded(_, val) => self.arrow_dtype_to_ast_dtype(val),
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => {
                let mut new_precision = *precision as u64;
                let mut new_scale = *scale as u64;
                if *scale < 0 {
                    new_precision = (*precision as i16 - *scale as i16) as u64;
                    new_scale = 0
                }

                Ok(data_type_with_args("DECIMAL", &[new_precision, new_scale]))
            }
            DataType::Map(_, _) => {
                not_impl_err!("Unsupported DataType: conversion: {data_type}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, Sub};
    use std::{sync::Arc, vec};

    use crate::ast::DataTypeSuffix;
    use crate::unparser::ast::{data_type, data_type_with_args, data_type_with_suffix};
    use crate::unparser::dialect::{MySqlDialect, SnowflakeDialect, SqliteDialect};
    use arrow::array::{LargeListArray, LargeListViewArray, ListArray, ListViewArray};
    use arrow::datatypes::{DataType::Int8, Field, Int32Type, Schema, TimeUnit};
    use datafusion_common::datatype::DataTypeExt;
    use datafusion_common::{Spans, TableReference};
    use datafusion_expr::expr::WildcardOptions;
    use datafusion_expr::{
        ColumnarValue, HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress,
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, ValueOrLambda,
        Volatility, WindowFrame, WindowFunctionDefinition, case, cast, col, cube, exists,
        grouping_set, interval_datetime_lit, interval_year_month_lit, lambda, lambda_var,
        lit, not, not_exists, out_ref_col, placeholder, rollup, table_scan, try_cast,
        when,
    };
    use datafusion_expr::{ExprFunctionExt, interval_month_day_nano_lit};
    use datafusion_functions::datetime::from_unixtime::FromUnixtimeFunc;
    use datafusion_functions::expr_fn::{get_field, named_struct};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::sum;
    use datafusion_functions_nested::expr_fn::{array_element, array_has, make_array};
    use datafusion_functions_nested::map::map;
    use datafusion_functions_window::rank::rank_udwf;
    use datafusion_functions_window::row_number::row_number_udwf;

    use crate::unparser::dialect::{
        BigQueryDialect, CharacterLengthStyle, CustomDialect, CustomDialectBuilder,
        DateFieldExtractStyle, DefaultDialect, Dialect, DuckDBDialect, PostgreSqlDialect,
        ScalarFnToSqlHandler,
    };

    use super::*;

    /// Mocked UDF
    #[derive(Debug, PartialEq, Eq, Hash)]
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
        fn name(&self) -> &str {
            "dummy_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("dummy - not implemented")
        }
    }
    // See sql::tests for E2E tests.

    #[derive(Debug, Hash, Eq, PartialEq)]
    struct DummyHigherOrderUDF;

    impl HigherOrderUDFImpl for DummyHigherOrderUDF {
        fn name(&self) -> &str {
            "dummy_higher_order_function"
        }

        fn signature(&self) -> &datafusion_expr::HigherOrderSignature {
            unimplemented!()
        }

        fn lambda_parameters(
            &self,
            _step: usize,
            _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
        ) -> Result<LambdaParametersProgress> {
            unimplemented!()
        }

        fn return_field_from_args(
            &self,
            _args: datafusion_expr::HigherOrderReturnFieldArgs,
        ) -> Result<FieldRef> {
            unimplemented!()
        }

        fn invoke_with_args(
            &self,
            _args: datafusion_expr::HigherOrderFunctionArgs,
        ) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn expr_to_sql_ok() -> Result<()> {
        let dummy_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        #[expect(deprecated)]
        let dummy_logical_plan = table_scan(Some("t"), &dummy_schema, None)?
            .project(vec![Expr::Wildcard {
                qualifier: None,
                options: Box::new(WildcardOptions::default()),
            }])?
            .filter(col("a").eq(lit(1)))?
            .build()?;

        let tests: Vec<(Expr, &str)> = vec![
            ((col("a") + col("b")).gt(lit(4)), r#"((a + b) > 4)"#),
            (
                Expr::Column(Column {
                    relation: Some(TableReference::partial("a", "b")),
                    name: "c".to_string(),
                    spans: Spans::new(),
                })
                .gt(lit(4)),
                r#"(b.c > 4)"#,
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
                Expr::Cast(Cast::new(Box::new(col("a")), DataType::Date64)),
                r#"CAST(a AS DATETIME)"#,
            ),
            (
                Expr::Cast(Cast::new(
                    Box::new(col("a")),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into())),
                )),
                r#"CAST(a AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Cast(Cast::new(
                    Box::new(col("a")),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                )),
                r#"CAST(a AS TIMESTAMP)"#,
            ),
            (
                Expr::Cast(Cast::new(Box::new(col("a")), DataType::UInt32)),
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
                Expr::HigherOrderFunction(HigherOrderFunction::new(
                    Arc::new(HigherOrderUDF::new_from_impl(DummyHigherOrderUDF)),
                    vec![col("a"), lambda(["v"], -lambda_var("v"))],
                )),
                r#"dummy_higher_order_function(a, (v) -> -v)"#,
            ),
            (
                Expr::Like(Like {
                    negated: true,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: false,
                }),
                r#"a NOT LIKE 'foo' ESCAPE 'o'"#,
            ),
            (
                Expr::Like(Like {
                    negated: true,
                    expr: Box::new(col("a")),
                    pattern: Box::new(lit("foo")),
                    escape_char: Some('o'),
                    case_insensitive: true,
                }),
                r#"a NOT ILIKE 'foo' ESCAPE 'o'"#,
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
                Expr::Literal(ScalarValue::Date64(Some(0)), None),
                r#"CAST('1970-01-01 00:00:00' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date64(Some(10000)), None),
                r#"CAST('1970-01-01 00:00:10' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date64(Some(-10000)), None),
                r#"CAST('1969-12-31 23:59:50' AS DATETIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(0)), None),
                r#"CAST('1970-01-01' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(10)), None),
                r#"CAST('1970-01-11' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::Date32(Some(-1)), None),
                r#"CAST('1969-12-31' AS DATE)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampSecond(Some(10001), None), None),
                r#"CAST('1970-01-01 02:46:41' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(
                    ScalarValue::TimestampSecond(Some(10001), Some("+08:00".into())),
                    None,
                ),
                r#"CAST('1970-01-01T10:46:41+08:00' AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMillisecond(Some(10001), None), None),
                r#"CAST('1970-01-01 00:00:10.001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(
                    ScalarValue::TimestampMillisecond(Some(10001), Some("+08:00".into())),
                    None,
                ),
                r#"CAST('1970-01-01T08:00:10.001+08:00' AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampMicrosecond(Some(10001), None), None),
                r#"CAST('1970-01-01 00:00:00.010001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(
                    ScalarValue::TimestampMicrosecond(Some(10001), Some("+08:00".into())),
                    None,
                ),
                r#"CAST('1970-01-01T08:00:00.010001+08:00' AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Literal(ScalarValue::TimestampNanosecond(Some(10001), None), None),
                r#"CAST('1970-01-01 00:00:00.000010001' AS TIMESTAMP)"#,
            ),
            (
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(Some(10001), Some("+08:00".into())),
                    None,
                ),
                r#"CAST('1970-01-01T08:00:00.000010001+08:00' AS TIMESTAMP WITH TIME ZONE)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time32Second(Some(10001)), None),
                r#"CAST('02:46:41' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time32Millisecond(Some(10001)), None),
                r#"CAST('00:00:10.001' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time64Microsecond(Some(10001)), None),
                r#"CAST('00:00:00.010001' AS TIME)"#,
            ),
            (
                Expr::Literal(ScalarValue::Time64Nanosecond(Some(10001)), None),
                r#"CAST('00:00:00.000010001' AS TIME)"#,
            ),
            (sum(col("a")), r#"sum(a)"#),
            (
                #[expect(deprecated)]
                count_udaf()
                    .call(vec![Expr::Wildcard {
                        qualifier: None,
                        options: Box::new(WildcardOptions::default()),
                    }])
                    .distinct()
                    .build()
                    .unwrap(),
                "count(DISTINCT *)",
            ),
            (
                #[expect(deprecated)]
                count_udaf()
                    .call(vec![Expr::Wildcard {
                        qualifier: None,
                        options: Box::new(WildcardOptions::default()),
                    }])
                    .filter(lit(true))
                    .build()
                    .unwrap(),
                "count(*) FILTER (WHERE true)",
            ),
            (
                Expr::from(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                    params: WindowFunctionParams {
                        args: vec![col("col")],
                        partition_by: vec![],
                        order_by: vec![],
                        window_frame: WindowFrame::new(None),
                        null_treatment: None,
                        distinct: false,
                        filter: None,
                    },
                }),
                r#"row_number(col) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"#,
            ),
            (
                #[expect(deprecated)]
                Expr::from(WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    params: WindowFunctionParams {
                        args: vec![Expr::Wildcard {
                            qualifier: None,
                            options: Box::new(WildcardOptions::default()),
                        }],
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
                        distinct: false,
                        filter: Some(Box::new(col("a").gt(lit(100)))),
                    },
                }),
                r#"count(*) FILTER (WHERE (a > 100)) OVER (ORDER BY a DESC NULLS FIRST RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING)"#,
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
                Expr::ScalarVariable(
                    Int8.into_nullable_field_ref(),
                    vec![String::from("@a")],
                ),
                r#"@a"#,
            ),
            (
                Expr::ScalarVariable(
                    Int8.into_nullable_field_ref(),
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
                (col("a") + col("b")).gt(Expr::Literal(
                    ScalarValue::Decimal32(Some(1123), 4, 3),
                    None,
                )),
                r#"((a + b) > 1.123)"#,
            ),
            (
                (col("a") + col("b")).gt(Expr::Literal(
                    ScalarValue::Decimal64(Some(1123), 4, 3),
                    None,
                )),
                r#"((a + b) > 1.123)"#,
            ),
            (
                (col("a") + col("b")).gt(Expr::Literal(
                    ScalarValue::Decimal128(Some(100123), 28, 3),
                    None,
                )),
                r#"((a + b) > 100.123)"#,
            ),
            (
                (col("a") + col("b")).gt(Expr::Literal(
                    ScalarValue::Decimal256(Some(100123.into()), 28, 3),
                    None,
                )),
                r#"((a + b) > 100.123)"#,
            ),
            (
                Expr::Cast(Cast::new(Box::new(col("a")), DataType::Decimal128(10, -2))),
                r#"CAST(a AS DECIMAL(12,0))"#,
            ),
            (
                Expr::Unnest(Unnest {
                    expr: Box::new(Expr::Column(Column {
                        relation: Some(TableReference::partial("schema", "table")),
                        name: "array_col".to_string(),
                        spans: Spans::new(),
                    })),
                    outer: false,
                }),
                r#"UNNEST("table".array_col)"#,
            ),
            (make_array(vec![lit(1), lit(2), lit(3)]), "[1, 2, 3]"),
            (array_element(col("array_col"), lit(1)), "array_col[1]"),
            (
                array_element(make_array(vec![lit(1), lit(2), lit(3)]), lit(1)),
                "[1, 2, 3][1]",
            ),
            (
                named_struct(vec![lit("a"), lit("1"), lit("b"), lit(2)]),
                "{a: '1', b: 2}",
            ),
            (get_field(col("a.b"), "c"), "a.b.c"),
            (
                map(vec![lit("a"), lit("b")], vec![lit(1), lit(2)]),
                "MAP {'a': 1, 'b': 2}",
            ),
            (
                Expr::Literal(
                    ScalarValue::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(ScalarValue::Utf8(Some("foo".into()))),
                    ),
                    None,
                ),
                "'foo'",
            ),
            (
                Expr::Literal(
                    ScalarValue::RunEndEncoded(
                        Field::new("run_ends", DataType::Int32, false).into(),
                        Field::new("values", DataType::Utf8, true).into(),
                        Box::new(ScalarValue::Utf8(Some("foo".into()))),
                    ),
                    None,
                ),
                "'foo'",
            ),
            (
                Expr::Literal(
                    ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<
                        Int32Type,
                        _,
                        _,
                    >(vec![Some(vec![
                        Some(1),
                        Some(2),
                        Some(3),
                    ])]))),
                    None,
                ),
                "[1, 2, 3]",
            ),
            (
                Expr::Literal(
                    ScalarValue::LargeList(Arc::new(
                        LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                            Some(vec![Some(1), Some(2), Some(3)]),
                        ]),
                    )),
                    None,
                ),
                "[1, 2, 3]",
            ),
            (
                Expr::Literal(
                    ScalarValue::ListView(Arc::new(
                        ListViewArray::from_iter_primitive::<Int32Type, _, _>(vec![
                            Some(vec![Some(1), Some(2), Some(3)]),
                        ]),
                    )),
                    None,
                ),
                "[1, 2, 3]",
            ),
            (
                Expr::Literal(
                    ScalarValue::LargeListView(Arc::new(
                        LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(vec![
                            Some(vec![Some(1), Some(2), Some(3)]),
                        ]),
                    )),
                    None,
                ),
                "[1, 2, 3]",
            ),
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("a")),
                    op: Operator::ArrowAt,
                    right: Box::new(col("b")),
                }),
                "(a <@ b)",
            ),
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("a")),
                    op: Operator::AtArrow,
                    right: Box::new(col("b")),
                }),
                "(a @> b)",
            ),
        ];

        for (expr, expected) in tests {
            let ast = expr_to_sql(&expr)?;

            let actual = format!("{ast}");

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

        let actual = format!("{ast}");

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

        let actual = format!("{ast}");

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

            let expr = Expr::Cast(Cast::new(Box::new(col("a")), DataType::Date64));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");

            let expected = format!(r#"CAST(a AS {identifier})"#);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_float64_ast_dtype() -> Result<()> {
        for (float64_ast_dtype, identifier) in [
            (data_type("DOUBLE"), "DOUBLE"),
            (
                data_type_with_suffix("DOUBLE", DataTypeSuffix::Precision),
                "DOUBLE PRECISION",
            ),
        ] {
            let dialect = CustomDialectBuilder::new()
                .with_float64_ast_dtype(float64_ast_dtype)
                .build();
            let unparser = Unparser::new(&dialect);

            let expr = Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");

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

            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_character_length_scalar_to_expr() {
        let tests = [
            (CharacterLengthStyle::Length, "length(x)"),
            (CharacterLengthStyle::CharacterLength, "character_length(x)"),
        ];

        for (style, expected) in tests {
            let dialect = CustomDialectBuilder::new()
                .with_character_length_style(style)
                .build();
            let unparser = Unparser::new(&dialect);

            let expr = ScalarUDF::new_from_impl(
                datafusion_functions::unicode::character_length::CharacterLengthFunc::new(
                ),
            )
            .call(vec![col("x")]);

            let ast = unparser.expr_to_sql(&expr).expect("to be unparsed");

            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
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
            (Expr::Literal(ScalarValue::Float64(Some(3f64)), None), "3.0"),
            (
                Expr::Literal(ScalarValue::Float64(Some(3.1f64)), None),
                "3.1",
            ),
            (
                Expr::Literal(ScalarValue::Float32(Some(-2f32)), None),
                "-2.0",
            ),
            (
                Expr::Literal(ScalarValue::Float32(Some(-2.989f32)), None),
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
    fn test_cast_value_to_binary_expr() {
        let tests = [
            (
                Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(
                        ScalarValue::Utf8(Some("blah".to_string())),
                        None,
                    )),
                    DataType::Binary,
                )),
                "'blah'",
            ),
            (
                Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(
                        ScalarValue::Utf8(Some("blah".to_string())),
                        None,
                    )),
                    DataType::BinaryView,
                )),
                "'blah'",
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
            .with_utf8_cast_dtype(data_type("CHAR"))
            .with_large_utf8_cast_dtype(data_type("CHAR"))
            .build();

        for (dialect, data_type, identifier) in [
            (&default_dialect, DataType::Utf8, "VARCHAR"),
            (&default_dialect, DataType::LargeUtf8, "TEXT"),
            (&mysql_custom_dialect, DataType::Utf8, "CHAR"),
            (&mysql_custom_dialect, DataType::LargeUtf8, "CHAR"),
        ] {
            let unparser = Unparser::new(dialect);

            let expr = Expr::Cast(Cast::new(Box::new(col("a")), data_type));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
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
                DateFieldExtractStyle::Extract,
                "MONS",
                "EXTRACT(MONTH FROM x)",
            ),
            (
                DateFieldExtractStyle::Strftime,
                "MONTH",
                "strftime('%m', x)",
            ),
            (DateFieldExtractStyle::Strftime, "YRS", "strftime('%Y', x)"),
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
            .call(vec![
                Expr::Literal(ScalarValue::new_utf8(unit), None),
                col("x"),
            ]);

            let ast = unparser.expr_to_sql(&expr)?;
            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_int64_cast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_int64_cast_dtype(data_type("SIGNED"))
            .build();

        for (dialect, identifier) in
            [(default_dialect, "BIGINT"), (mysql_dialect, "SIGNED")]
        {
            let unparser = Unparser::new(&dialect);
            let expr = Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int64));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_int32_cast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_int32_cast_dtype(data_type("SIGNED"))
            .build();

        for (dialect, identifier) in
            [(default_dialect, "INTEGER"), (mysql_dialect, "SIGNED")]
        {
            let unparser = Unparser::new(&dialect);
            let expr = Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int32));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_timestamp_cast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_timestamp_cast_dtype(data_type("DATETIME"), data_type("DATETIME"))
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
            let expr = Expr::Cast(Cast::new(Box::new(col("a")), data_type.clone()));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_with_timestamp_cast_dtype_scalar_expr() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let mysql_dialect = CustomDialectBuilder::new()
            .with_timestamp_cast_dtype(data_type("DATETIME"), data_type("DATETIME"))
            .build();

        let timestamp = ScalarValue::TimestampMillisecond(Some(1738285549123), None);
        let timestamp_with_tz =
            ScalarValue::TimestampMillisecond(Some(1738285549123), Some("+08:00".into()));

        for (dialect, scalar, expected) in [
            (
                &default_dialect,
                &timestamp,
                "CAST('2025-01-31 01:05:49.123' AS TIMESTAMP)",
            ),
            (
                &default_dialect,
                &timestamp_with_tz,
                "CAST('2025-01-31T09:05:49.123+08:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                &mysql_dialect,
                &timestamp,
                "CAST('2025-01-31 01:05:49.123' AS DATETIME)",
            ),
            (
                &mysql_dialect,
                &timestamp_with_tz,
                "CAST('2025-01-31T09:05:49.123+08:00' AS DATETIME)",
            ),
        ] {
            let unparser = Unparser::new(dialect);
            let expr = Expr::Literal(scalar.clone(), None);
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    // MySQL `DATETIME` without precision is `DATETIME(0)` and rounds the
    // literal to whole seconds, which breaks equality and keyset seeks on
    // sub-second timestamps. The MySQL dialect must carry the unit's precision.
    #[test]
    fn mysql_timestamp_cast_keeps_fractional_precision() -> Result<()> {
        let unparser = Unparser::new(&MySqlDialect {});

        for (scalar, expected) in [
            (
                ScalarValue::TimestampSecond(Some(1789980940), None),
                "CAST('2026-09-21 08:55:40' AS DATETIME)",
            ),
            (
                ScalarValue::TimestampMillisecond(Some(1789980940006), None),
                "CAST('2026-09-21 08:55:40.006' AS DATETIME(3))",
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(1789980940006000), None),
                "CAST('2026-09-21 08:55:40.006' AS DATETIME(6))",
            ),
            (
                ScalarValue::TimestampNanosecond(Some(1789980940006000000), None),
                "CAST('2026-09-21 08:55:40.006' AS DATETIME(6))",
            ),
        ] {
            let ast = unparser.expr_to_sql(&Expr::Literal(scalar, None))?;
            assert_eq!(format!("{ast}"), expected);
        }

        let expr = Expr::Cast(Cast::new(
            Box::new(col("a")),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
        let ast = unparser.expr_to_sql(&expr)?;
        assert_eq!(format!("{ast}"), "CAST(`a` AS DATETIME(6))");
        Ok(())
    }

    // MySQL's `CAST` only accepts `SIGNED` and `UNSIGNED` as integer target types.
    #[test]
    fn mysql_integer_casts() -> Result<()> {
        let unparser = Unparser::new(&MySqlDialect {});
        for (data_type, expected) in [
            (Int8, "CAST(`a` AS SIGNED)"),
            (DataType::Int16, "CAST(`a` AS SIGNED)"),
            (DataType::Int32, "CAST(`a` AS SIGNED)"),
            (DataType::Int64, "CAST(`a` AS SIGNED)"),
            (DataType::UInt8, "CAST(`a` AS UNSIGNED)"),
            (DataType::UInt16, "CAST(`a` AS UNSIGNED)"),
            (DataType::UInt32, "CAST(`a` AS UNSIGNED)"),
            (DataType::UInt64, "CAST(`a` AS UNSIGNED)"),
        ] {
            let expr = Expr::Cast(Cast::new(Box::new(col("a")), data_type));
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(format!("{ast}"), expected);
        }
        Ok(())
    }

    #[test]
    fn default_and_custom_dialect_integer_casts() -> Result<()> {
        let custom_dialect = CustomDialectBuilder::new()
            .with_int8_cast_dtype(data_type("SMALLINT"))
            .with_int16_cast_dtype(data_type("INTEGER"))
            .with_uint8_cast_dtype(data_type("SMALLINT"))
            .with_uint16_cast_dtype(data_type("INTEGER"))
            .with_uint32_cast_dtype(data_type("BIGINT"))
            .with_uint64_cast_dtype(data_type("NUMERIC"))
            .build();
        let default_unparser = Unparser::new(&DefaultDialect {});
        let custom_unparser = Unparser::new(&custom_dialect);
        for (data_type, default_expected, custom_expected) in [
            (Int8, "CAST(a AS TINYINT)", "CAST(a AS SMALLINT)"),
            (DataType::Int16, "CAST(a AS SMALLINT)", "CAST(a AS INTEGER)"),
            (
                DataType::UInt8,
                "CAST(a AS TINYINT UNSIGNED)",
                "CAST(a AS SMALLINT)",
            ),
            (
                DataType::UInt16,
                "CAST(a AS SMALLINT UNSIGNED)",
                "CAST(a AS INTEGER)",
            ),
            (
                DataType::UInt32,
                "CAST(a AS INTEGER UNSIGNED)",
                "CAST(a AS BIGINT)",
            ),
            (
                DataType::UInt64,
                "CAST(a AS BIGINT UNSIGNED)",
                "CAST(a AS NUMERIC)",
            ),
        ] {
            let expr = Expr::Cast(Cast::new(Box::new(col("a")), data_type));
            let ast = default_unparser.expr_to_sql(&expr)?;
            assert_eq!(format!("{ast}"), default_expected);
            let ast = custom_unparser.expr_to_sql(&expr)?;
            assert_eq!(format!("{ast}"), custom_expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_date32_ast_dtype() -> Result<()> {
        let default_dialect = CustomDialectBuilder::default().build();
        let sqlite_custom_dialect = CustomDialectBuilder::new()
            .with_date32_cast_dtype(data_type("TEXT"))
            .build();

        for (dialect, data_type, identifier) in [
            (&default_dialect, DataType::Date32, "DATE"),
            (&sqlite_custom_dialect, DataType::Date32, "TEXT"),
        ] {
            let unparser = Unparser::new(dialect);

            let expr = Expr::Cast(Cast::new(Box::new(col("a")), data_type));
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = format!(r#"CAST(a AS {identifier})"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn custom_dialect_division_operator() -> Result<()> {
        let default_dialect = CustomDialectBuilder::new().build();
        let duckdb_dialect = CustomDialectBuilder::new()
            .with_division_operator(BinaryOp::Named("//".into()))
            .build();

        for (dialect, expected) in
            [(default_dialect, "(a / b)"), (duckdb_dialect, "(a // b)")]
        {
            let unparser = Unparser::new(&dialect);
            let expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("a")),
                op: Operator::Divide,
                right: Box::new(col("b")),
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = expected.to_string();

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_mssql_dialect_national_literal() -> Result<()> {
        struct MsSqlDialect;

        impl Dialect for MsSqlDialect {
            fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
                Some('[')
            }

            fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr<'static>> {
                if !s.is_ascii() {
                    Some(
                        ExprKind::Literal(Literal::String(ast::StringLiteral {
                            national: true,
                            ..crate::unparser::ast::string_literal(s)
                        }))
                        .into(),
                    )
                } else {
                    None
                }
            }
        }

        let dialect = MsSqlDialect;
        let unparser = Unparser::new(&dialect);

        // Get nation string literal for the custom mssql dialect
        for (s, expected) in [
            ("national string", "'national string'"),
            ("datafusion資料融合", "N'datafusion資料融合'"),
        ] {
            let expr = Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);

            let expr = Expr::Literal(ScalarValue::Utf8View(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);

            let expr = Expr::Literal(ScalarValue::LargeUtf8(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);
        }

        let dialect = DefaultDialect {};
        let unparser = Unparser::new(&dialect);

        // Get normal string literal for default dialect
        for (s, expected) in [
            ("national string", "'national string'"),
            ("datafusion資料融合", "'datafusion資料融合'"),
        ] {
            let expr = Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);

            let expr = Expr::Literal(ScalarValue::Utf8View(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);

            let expr = Expr::Literal(ScalarValue::LargeUtf8(Some(s.to_string())), None);
            let ast = unparser.expr_to_sql(&expr)?;
            assert_eq!(ast.to_string(), expected);
        }
        Ok(())
    }

    #[test]
    fn test_cast_value_to_dict_expr() {
        let tests = [(
            Expr::Cast(Cast::new(
                Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("variation".to_string())),
                    None,
                )),
                DataType::Dictionary(Box::new(Int8), Box::new(DataType::Utf8)),
            )),
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

    #[test]
    fn test_array_literal_scalar_value_to_sql_postgres() -> Result<()> {
        let dialect: Arc<dyn Dialect> = Arc::new(PostgreSqlDialect {});
        let unparser = Unparser::new(dialect.as_ref());

        let expr = Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(
                &[
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(2)),
                    ScalarValue::Int32(Some(3)),
                ],
                &DataType::Int32,
            )),
            None,
        );

        let ast = unparser.expr_to_sql(&expr)?;
        assert_eq!(ast.to_string(), "ARRAY[1, 2, 3]");

        Ok(())
    }

    #[test]
    fn test_nested_array_literal_scalar_value_to_sql_postgres() -> Result<()> {
        let dialect: Arc<dyn Dialect> = Arc::new(PostgreSqlDialect {});
        let unparser = Unparser::new(dialect.as_ref());

        let inner_type = DataType::Int32;
        let nested_type =
            DataType::List(Arc::new(Field::new_list_field(inner_type.clone(), true)));

        let expr = Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(
                &[
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))],
                        &inner_type,
                    )),
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Int32(Some(3)), ScalarValue::Int32(Some(4))],
                        &inner_type,
                    )),
                ],
                &nested_type,
            )),
            None,
        );

        let ast = unparser.expr_to_sql(&expr)?;
        assert_eq!(ast.to_string(), "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]");

        Ok(())
    }

    #[test]
    fn test_round_scalar_fn_to_expr() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> = Arc::new(
            CustomDialectBuilder::new()
                .with_identifier_quote_style('"')
                .build(),
        );
        let postgres_dialect: Arc<dyn Dialect> = Arc::new(PostgreSqlDialect {});

        for (dialect, identifier) in
            [(default_dialect, "DOUBLE"), (postgres_dialect, "NUMERIC")]
        {
            let unparser = Unparser::new(dialect.as_ref());
            let expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(
                    datafusion_functions::math::round::RoundFunc::new(),
                )),
                args: vec![
                    Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
                    Expr::Literal(ScalarValue::Int64(Some(2)), None),
                ],
            });
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = format!("{ast}");
            let expected = format!(r#"round(CAST("a" AS {identifier}), 2)"#);

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_postgres_array_has_to_any() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> = Arc::new(DefaultDialect {});
        let postgres_dialect: Arc<dyn Dialect> = Arc::new(PostgreSqlDialect {});
        let expr = array_has(col("items"), lit(1));

        for (dialect, expected) in [
            (default_dialect, "array_has(\"items\", 1)"),
            (postgres_dialect, "1 = ANY(\"items\")"),
        ] {
            let unparser = Unparser::new(dialect.as_ref());
            let actual = format!("{}", unparser.expr_to_sql(&expr)?);
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    /// Regression test for https://github.com/apache/datafusion/issues/23056
    ///
    /// Deeply-nested expressions whose unparse path routes through scalar
    /// function arguments and dialect scalar-function overrides used to
    /// overflow the OS stack even with `recursive_protection` enabled,
    /// because the per-level stack cost of those paths exceeds the default
    /// `recursive` red zone and the unparser installed no [`StackGuard`].
    ///
    /// This test only asserts the protected behavior, so it is gated on the
    /// `recursive_protection` feature. Without that feature the unparser is
    /// not stack-safe by design and a deep enough expression will overflow.
    #[cfg(feature = "recursive_protection")]
    #[test]
    fn test_deeply_nested_expr_does_not_overflow_stack() {
        // Far deeper than the ~60 levels that overflow without protection, but
        // bounded so the trampoline's heap stacks stay reasonable in debug.
        const DEPTH: usize = 2_000;

        // Run on an explicit, realistically-sized thread stack. The work is
        // performed on a spawned thread so an overflow (in the unfixed code)
        // aborts the process and fails the test deterministically rather than
        // depending on the harness thread's stack size.
        let handle = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                // 1. Linear chain through a dialect scalar-function override:
                //    array_has(array_has(... array_has(col, 'x') ...), 'x').
                //    PostgreSqlDialect unparses array_has via array_has_to_sql_any,
                //    which recurses back into the unparser for each argument.
                let mut nested_fn: Expr = col("c");
                for _ in 0..DEPTH {
                    nested_fn = array_has(nested_fn, lit("x"));
                }
                let pg = PostgreSqlDialect {};
                Unparser::new(&pg)
                    .expr_to_sql(&nested_fn)
                    .expect("deeply nested scalar function should unparse");

                // 2. Linear chain of plain binary operators, exercising the
                //    inner -> inner recursion on the default dialect.
                let mut nested_binary: Expr = col("c");
                for _ in 0..DEPTH {
                    nested_binary = nested_binary + lit(1);
                }
                Unparser::default()
                    .expr_to_sql(&nested_binary)
                    .expect("deeply nested binary expression should unparse");

                // 3. Same binary chain in pretty mode. Pretty mode runs
                //    `remove_unnecessary_nesting` at every level, which recurses
                //    alongside the unparse itself; this locks down that second
                //    recursion site fixed by this PR.
                Unparser::default()
                    .with_pretty(true)
                    .expr_to_sql(&nested_binary)
                    .expect(
                        "deeply nested binary expression should unparse in pretty mode",
                    );
            })
            .unwrap();

        // If the unparser overflows, the process aborts and this join is never
        // reached; otherwise the spawned thread returns cleanly.
        handle.join().expect("unparsing thread should not panic");
    }

    #[cfg(feature = "recursive_protection")]
    #[test]
    fn test_expr_to_sql_does_not_mutate_recursive_minimum_stack_size() -> Result<()> {
        const DEFAULT_RECURSIVE_RED_ZONE: usize = 128 * 1024;

        let previous_minimum = recursive::get_minimum_stack_size();
        recursive::set_minimum_stack_size(DEFAULT_RECURSIVE_RED_ZONE);

        let observed_minimum = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
        let dialect = DuckDBDialect::new().with_custom_scalar_overrides(vec![(
            "dummy_udf",
            Box::new({
                let observed_minimum = Arc::clone(&observed_minimum);
                move |unparser: &Unparser, args: &[Expr]| {
                    observed_minimum.store(
                        recursive::get_minimum_stack_size(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    unparser.scalar_function_to_sql("dummy_udf", args).map(Some)
                }
            }) as ScalarFnToSqlHandler,
        )]);
        let expr = ScalarUDF::new_from_impl(DummyUDF::new()).call(vec![col("a")]);

        let result = Unparser::new(&dialect).expr_to_sql(&expr);
        let final_minimum = recursive::get_minimum_stack_size();
        recursive::set_minimum_stack_size(previous_minimum);

        result?;
        assert_eq!(
            observed_minimum.load(std::sync::atomic::Ordering::Relaxed),
            DEFAULT_RECURSIVE_RED_ZONE
        );
        assert_eq!(final_minimum, DEFAULT_RECURSIVE_RED_ZONE);

        Ok(())
    }

    #[test]
    fn case_propagates_operand_and_else_errors() {
        let unsupported = Expr::Literal(ScalarValue::DurationSecond(Some(1)), None);
        let expressions = [
            Expr::Case(Case::new(
                Some(Box::new(unsupported.clone())),
                vec![(Box::new(lit(1)), Box::new(lit(2)))],
                None,
            )),
            Expr::Case(Case::new(
                None,
                vec![(Box::new(lit(true)), Box::new(lit(1)))],
                Some(Box::new(unsupported)),
            )),
        ];

        for expr in expressions {
            let error = expr_to_sql(&expr).expect_err("unsupported CASE child");
            assert!(error.to_string().contains("Unsupported scalar"));
        }
    }

    #[test]
    fn test_window_func_support_window_frame() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> =
            Arc::new(CustomDialectBuilder::new().build());

        let test_dialect: Arc<dyn Dialect> = Arc::new(
            CustomDialectBuilder::new()
                .with_window_func_support_window_frame(false)
                .build(),
        );

        for (dialect, expected) in [
            (
                default_dialect,
                "rank() OVER (ORDER BY a ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            ),
            (test_dialect, "rank() OVER (ORDER BY a ASC NULLS FIRST)"),
        ] {
            let unparser = Unparser::new(dialect.as_ref());
            let func = WindowFunctionDefinition::WindowUDF(rank_udwf());
            let mut window_func = WindowFunction::new(func, vec![]);
            window_func.params.order_by = vec![Sort::new(col("a"), true, true)];
            let expr = Expr::from(window_func);
            let ast = unparser.expr_to_sql(&expr)?;

            let actual = ast.to_string();
            let expected = expected.to_string();

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_from_unixtime() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> = Arc::new(DefaultDialect {});
        let sqlite_dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect {});

        for (dialect, expected) in [
            (default_dialect, "from_unixtime(date_col)"),
            (sqlite_dialect, "datetime(`date_col`, 'unixepoch')"),
        ] {
            let unparser = Unparser::new(dialect.as_ref());
            let expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(FromUnixtimeFunc::default())),
                args: vec![col("date_col")],
            });

            let ast = unparser.expr_to_sql(&expr)?;

            let actual = ast.to_string();
            let expected = expected.to_string();

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_date_trunc() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> = Arc::new(DefaultDialect {});
        let sqlite_dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect {});

        for (dialect, precision, expected) in [
            (
                Arc::clone(&default_dialect),
                "YEAR",
                "date_trunc('YEAR', date_col)",
            ),
            (
                Arc::clone(&sqlite_dialect),
                "YEAR",
                "strftime('%Y', `date_col`)",
            ),
            (
                Arc::clone(&default_dialect),
                "MONTH",
                "date_trunc('MONTH', date_col)",
            ),
            (
                Arc::clone(&sqlite_dialect),
                "MONTH",
                "strftime('%Y-%m', `date_col`)",
            ),
            (
                Arc::clone(&default_dialect),
                "DAY",
                "date_trunc('DAY', date_col)",
            ),
            (
                Arc::clone(&sqlite_dialect),
                "DAY",
                "strftime('%Y-%m-%d', `date_col`)",
            ),
            (
                Arc::clone(&default_dialect),
                "HOUR",
                "date_trunc('HOUR', date_col)",
            ),
            (
                Arc::clone(&sqlite_dialect),
                "HOUR",
                "strftime('%Y-%m-%d %H', `date_col`)",
            ),
            (
                Arc::clone(&default_dialect),
                "MINUTE",
                "date_trunc('MINUTE', date_col)",
            ),
            (
                Arc::clone(&sqlite_dialect),
                "MINUTE",
                "strftime('%Y-%m-%d %H:%M', `date_col`)",
            ),
            (default_dialect, "SECOND", "date_trunc('SECOND', date_col)"),
            (
                sqlite_dialect,
                "SECOND",
                "strftime('%Y-%m-%d %H:%M:%S', `date_col`)",
            ),
        ] {
            let unparser = Unparser::new(dialect.as_ref());
            let expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(
                    datafusion_functions::datetime::date_trunc::DateTruncFunc::new(),
                )),
                args: vec![
                    Expr::Literal(ScalarValue::Utf8(Some(precision.to_string())), None),
                    col("date_col"),
                ],
            });

            let ast = unparser.expr_to_sql(&expr)?;

            let actual = ast.to_string();
            let expected = expected.to_string();

            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_dictionary_to_sql() -> Result<()> {
        let dialect = CustomDialectBuilder::new().build();

        let unparser = Unparser::new(&dialect);

        let arrow_field = Arc::new(Field::new(
            "",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ));
        let ast_dtype = unparser.arrow_dtype_to_ast_dtype(&arrow_field)?;

        assert_eq!(ast_dtype, data_type("VARCHAR"));

        Ok(())
    }

    #[test]
    fn test_run_end_encoded_to_sql() -> Result<()> {
        let dialect = CustomDialectBuilder::new().build();

        let unparser = Unparser::new(&dialect);

        let ast_dtype = unparser.arrow_dtype_to_ast_dtype(
            &DataType::RunEndEncoded(
                Field::new("run_ends", DataType::Int32, false).into(),
                Field::new("values", DataType::Utf8, true).into(),
            )
            .into_nullable_field_ref(),
        )?;

        assert_eq!(ast_dtype, data_type("VARCHAR"));

        Ok(())
    }

    #[test]
    fn test_utf8_view_to_sql() -> Result<()> {
        let dialect = CustomDialectBuilder::new()
            .with_utf8_cast_dtype(data_type("CHAR"))
            .build();
        let unparser = Unparser::new(&dialect);

        let arrow_field = Arc::new(Field::new("", DataType::Utf8View, true));
        let ast_dtype = unparser.arrow_dtype_to_ast_dtype(&arrow_field)?;

        assert_eq!(ast_dtype, data_type("CHAR"));

        let expr = cast(col("a"), DataType::Utf8View);
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{ast}");
        let expected = r#"CAST(a AS CHAR)"#.to_string();

        assert_eq!(actual, expected);

        let expr = col("a").eq(lit(ScalarValue::Utf8View(Some("hello".to_string()))));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{ast}");
        let expected = r#"(a = 'hello')"#.to_string();

        assert_eq!(actual, expected);

        let expr = col("a").is_not_null();

        let ast = unparser.expr_to_sql(&expr)?;
        let actual = format!("{ast}");
        let expected = r#"a IS NOT NULL"#.to_string();

        assert_eq!(actual, expected);

        let expr = col("a").is_null();

        let ast = unparser.expr_to_sql(&expr)?;
        let actual = format!("{ast}");
        let expected = r#"a IS NULL"#.to_string();

        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_custom_scalar_overrides_duckdb() -> Result<()> {
        let duckdb_default = DuckDBDialect::new();
        let duckdb_extended = DuckDBDialect::new().with_custom_scalar_overrides(vec![(
            "dummy_udf",
            Box::new(|unparser: &Unparser, args: &[Expr]| {
                unparser.scalar_function_to_sql("smart_udf", args).map(Some)
            }) as ScalarFnToSqlHandler,
        )]);

        for (dialect, expected) in [
            (duckdb_default, r#"dummy_udf("a", "b")"#),
            (duckdb_extended, r#"smart_udf("a", "b")"#),
        ] {
            let unparser = Unparser::new(&dialect);
            let expr =
                ScalarUDF::new_from_impl(DummyUDF::new()).call(vec![col("a"), col("b")]);
            let actual = format!("{}", unparser.expr_to_sql(&expr)?);
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_cast_timestamp_sqlite() -> Result<()> {
        let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect {});

        let unparser = Unparser::new(dialect.as_ref());
        let expr = Expr::Cast(Cast::new(
            Box::new(col("a")),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ));

        let ast = unparser.expr_to_sql(&expr)?;

        let actual = ast.to_string();
        let expected = "CAST(`a` AS TEXT)".to_string();

        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_timestamp_with_tz_format() -> Result<()> {
        let default_dialect: Arc<dyn Dialect> =
            Arc::new(CustomDialectBuilder::new().build());

        let duckdb_dialect: Arc<dyn Dialect> = Arc::new(DuckDBDialect::new());
        let bigquery_dialect: Arc<dyn Dialect> = Arc::new(BigQueryDialect::new());
        let snowflake_dialect: Arc<dyn Dialect> = Arc::new(SnowflakeDialect::new());

        for (dialect, scalar, expected) in [
            (
                Arc::clone(&default_dialect),
                ScalarValue::TimestampSecond(Some(1757934000), Some("+00:00".into())),
                "CAST('2025-09-15T11:00:00+00:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&default_dialect),
                ScalarValue::TimestampMillisecond(
                    Some(1757934000123),
                    Some("+01:00".into()),
                ),
                "CAST('2025-09-15T12:00:00.123+01:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&default_dialect),
                ScalarValue::TimestampMicrosecond(
                    Some(1757934000123456),
                    Some("-01:00".into()),
                ),
                "CAST('2025-09-15T10:00:00.123456-01:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&default_dialect),
                ScalarValue::TimestampNanosecond(
                    Some(1757934000123456789),
                    Some("+00:00".into()),
                ),
                "CAST('2025-09-15T11:00:00.123456789+00:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&duckdb_dialect),
                ScalarValue::TimestampSecond(Some(1757934000), Some("+00:00".into())),
                "CAST('2025-09-15T11:00:00+00:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&duckdb_dialect),
                ScalarValue::TimestampMillisecond(
                    Some(1757934000123),
                    Some("+01:00".into()),
                ),
                "CAST('2025-09-15T12:00:00.123+01:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&duckdb_dialect),
                ScalarValue::TimestampMicrosecond(
                    Some(1757934000123456),
                    Some("-01:00".into()),
                ),
                "CAST('2025-09-15T10:00:00.123456-01:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&duckdb_dialect),
                ScalarValue::TimestampNanosecond(
                    Some(1757934000123456789),
                    Some("+00:00".into()),
                ),
                "CAST('2025-09-15T11:00:00.123456789+00:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
            (
                Arc::clone(&bigquery_dialect),
                ScalarValue::TimestampSecond(Some(1757934000), Some("+00:00".into())),
                "CAST('2025-09-15T11:00:00+00:00' AS TIMESTAMP)",
            ),
            (
                Arc::clone(&bigquery_dialect),
                ScalarValue::TimestampMillisecond(
                    Some(1757934000123),
                    Some("+01:00".into()),
                ),
                "CAST('2025-09-15T12:00:00.123+01:00' AS TIMESTAMP)",
            ),
            (
                Arc::clone(&bigquery_dialect),
                ScalarValue::TimestampMicrosecond(
                    Some(1757934000123456),
                    Some("-01:00".into()),
                ),
                "CAST('2025-09-15T10:00:00.123456-01:00' AS TIMESTAMP)",
            ),
            (
                Arc::clone(&bigquery_dialect),
                ScalarValue::TimestampNanosecond(
                    Some(1757934000123456789),
                    Some("+00:00".into()),
                ),
                "CAST('2025-09-15T11:00:00.123456789+00:00' AS TIMESTAMP)",
            ),
            (
                Arc::clone(&snowflake_dialect),
                ScalarValue::TimestampMillisecond(
                    Some(1757934000123),
                    Some("+01:00".into()),
                ),
                "CAST('2025-09-15T12:00:00.123+01:00' AS TIMESTAMP WITH TIME ZONE)",
            ),
        ] {
            let unparser = Unparser::new(dialect.as_ref());

            let expr = Expr::Literal(scalar, None);

            let actual = format!("{}", unparser.expr_to_sql(&expr)?);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_timestamp_literal_passes_tz_to_dialect() -> Result<()> {
        // Puts the time zone it receives into the cast type, so the generated
        // SQL shows which zone the unparser passed to the dialect.
        struct TzEchoDialect;

        impl Dialect for TzEchoDialect {
            fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
                None
            }

            fn timestamp_cast_dtype(
                &self,
                _time_unit: &TimeUnit,
                tz: &Option<Arc<str>>,
            ) -> ast::DataType<'static> {
                match tz {
                    Some(tz) => data_type_with_args("TIMESTAMPTZ", &[tz]),
                    None => data_type("TIMESTAMP"),
                }
            }
        }

        let unparser = Unparser::new(&TzEchoDialect);

        let literal = Expr::Literal(
            ScalarValue::TimestampSecond(Some(1704078000), Some("Asia/Tokyo".into())),
            None,
        );
        let cast_expr = cast(
            col("a"),
            DataType::Timestamp(TimeUnit::Second, Some("Asia/Tokyo".into())),
        );

        for (expr, expected) in [
            (
                literal,
                "CAST('2024-01-01T12:00:00+09:00' AS TIMESTAMPTZ(Asia/Tokyo))",
            ),
            (cast_expr, "CAST(a AS TIMESTAMPTZ(Asia/Tokyo))"),
        ] {
            let actual = format!("{}", unparser.expr_to_sql(&expr)?);
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn test_bigquery_dialect_overrides() -> Result<()> {
        let bigquery_dialect: Arc<dyn Dialect> = Arc::new(BigQueryDialect::new());
        let unparser = Unparser::new(bigquery_dialect.as_ref());

        // date_field_extract_style: EXTRACT instead of date_part
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(
                datafusion_functions::datetime::date_part::DatePartFunc::new(),
            )),
            args: vec![lit("YEAR"), col("date_col")],
        });
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "EXTRACT(YEAR FROM `date_col`)");

        // interval_style: SQL standard instead of PostgresVerbose
        let expr = interval_year_month_lit("3 months");
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "INTERVAL '3' MONTH");

        // float64_ast_dtype: FLOAT64 instead of DOUBLE
        let expr = cast(col("a"), DataType::Float64);
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "CAST(`a` AS FLOAT64)");

        // supports_column_alias_in_table_alias: false
        assert!(!bigquery_dialect.supports_column_alias_in_table_alias());

        // utf8_cast_dtype: STRING instead of VARCHAR
        let expr = cast(col("a"), DataType::Utf8);
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "CAST(`a` AS STRING)");

        // large_utf8_cast_dtype: STRING instead of TEXT
        let expr = cast(col("a"), DataType::LargeUtf8);
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "CAST(`a` AS STRING)");

        // timestamp_cast_dtype: TIMESTAMP (no WITH TIME ZONE)
        let expr = cast(
            col("a"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        );
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, "CAST(`a` AS TIMESTAMP)");

        Ok(())
    }

    #[test]
    fn test_is_distinct_from() {
        let mysql_unparser = Unparser::new(&MySqlDialect {});

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("c1")),
            Operator::IsDistinctFrom,
            Box::new(lit(true)),
        ));

        let sql = expr_to_sql(&expr).unwrap().to_string();
        assert_eq!(sql, "(c1 IS DISTINCT FROM true)");
        let sql = mysql_unparser.expr_to_sql(&expr).unwrap().to_string();
        assert_eq!(sql, "(NOT (`c1` <=> true))");

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("c1")),
            Operator::IsNotDistinctFrom,
            Box::new(lit(true)),
        ));

        let sql = expr_to_sql(&expr).unwrap().to_string();
        assert_eq!(sql, "(c1 IS NOT DISTINCT FROM true)");
        let sql = mysql_unparser.expr_to_sql(&expr).unwrap().to_string();
        assert_eq!(sql, "(`c1` <=> true)");
    }

    #[test]
    fn test_binary_literal() {
        let value = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let expected_hex = "X'deadbeef'";

        assert_eq!(
            expr_to_sql(&Expr::Literal(
                ScalarValue::Binary(Some(value.clone())),
                None
            ))
            .unwrap()
            .to_string(),
            expected_hex
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(
                ScalarValue::BinaryView(Some(value.clone())),
                None
            ))
            .unwrap()
            .to_string(),
            expected_hex
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(
                ScalarValue::FixedSizeBinary(4, Some(value.clone())),
                None
            ))
            .unwrap()
            .to_string(),
            expected_hex
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(
                ScalarValue::LargeBinary(Some(value.clone())),
                None
            ))
            .unwrap()
            .to_string(),
            expected_hex
        );

        assert_eq!(
            expr_to_sql(&Expr::Literal(ScalarValue::Binary(None), None))
                .unwrap()
                .to_string(),
            "NULL"
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(ScalarValue::BinaryView(None), None))
                .unwrap()
                .to_string(),
            "NULL"
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(ScalarValue::FixedSizeBinary(1, None), None))
                .unwrap()
                .to_string(),
            "NULL"
        );
        assert_eq!(
            expr_to_sql(&Expr::Literal(ScalarValue::LargeBinary(None), None))
                .unwrap()
                .to_string(),
            "NULL"
        );
    }

    #[test]
    fn test_random_function() -> Result<()> {
        let duckdb_dialect = DuckDBDialect::new();
        let dialects = vec![
            (Unparser::new(&DefaultDialect {}), "random()"),
            (Unparser::new(&PostgreSqlDialect {}), "random()"),
            (Unparser::new(&duckdb_dialect), "random()"),
            (Unparser::new(&MySqlDialect {}), "rand()"),
            (Unparser::new(&SqliteDialect {}), "random()"),
            (Unparser::new(&BigQueryDialect {}), "rand()"),
            (Unparser::new(&SnowflakeDialect {}), "random()"),
        ];

        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(
                datafusion_functions::math::random::RandomFunc::new(),
            )),
            args: vec![],
        });

        for (unparser, result) in dialects {
            assert_eq!(unparser.expr_to_sql(&expr)?.to_string(), result);
        }

        Ok(())
    }

    #[test]
    fn test_unparse_null_treatment_window_and_aggregate() -> Result<()> {
        use datafusion_expr::expr::NullTreatment;
        use datafusion_functions_aggregate::first_last::first_value_udaf;
        use datafusion_functions_window::nth_value::first_value_udwf;

        // Window function with IgnoreNulls
        let window_ignore = Expr::from(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            params: WindowFunctionParams {
                args: vec![col("a")],
                partition_by: vec![],
                order_by: vec![Sort::new(col("b"), true, true)],
                window_frame: WindowFrame::new(None),
                null_treatment: Some(NullTreatment::IgnoreNulls),
                distinct: false,
                filter: None,
            },
        });

        // Window function with RespectNulls
        let window_respect = Expr::from(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            params: WindowFunctionParams {
                args: vec![col("a")],
                partition_by: vec![],
                order_by: vec![Sort::new(col("b"), true, true)],
                window_frame: WindowFrame::new(None),
                null_treatment: Some(NullTreatment::RespectNulls),
                distinct: false,
                filter: None,
            },
        });

        // Window function with None (Control)
        let window_none = Expr::from(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            params: WindowFunctionParams {
                args: vec![col("a")],
                partition_by: vec![],
                order_by: vec![Sort::new(col("b"), true, true)],
                window_frame: WindowFrame::new(None),
                null_treatment: None,
                distinct: false,
                filter: None,
            },
        });

        // Aggregate function with IgnoreNulls
        let agg_ignore =
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                first_value_udaf(),
                vec![col("a")],
                false,
                None,
                vec![],
                Some(NullTreatment::IgnoreNulls),
            ));

        // Aggregate function with RespectNulls
        let agg_respect =
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                first_value_udaf(),
                vec![col("a")],
                false,
                None,
                vec![],
                Some(NullTreatment::RespectNulls),
            ));

        // Aggregate function with None (Control)
        let agg_none =
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                first_value_udaf(),
                vec![col("a")],
                false,
                None,
                vec![],
                None,
            ));

        let actual_window_ignore = expr_to_sql(&window_ignore)?.to_string();
        let actual_window_respect = expr_to_sql(&window_respect)?.to_string();
        let actual_window_none = expr_to_sql(&window_none)?.to_string();
        let actual_agg_ignore = expr_to_sql(&agg_ignore)?.to_string();
        let actual_agg_respect = expr_to_sql(&agg_respect)?.to_string();
        let actual_agg_none = expr_to_sql(&agg_none)?.to_string();

        assert_eq!(
            actual_window_ignore,
            "first_value(a) IGNORE NULLS OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        );
        assert_eq!(
            actual_window_respect,
            "first_value(a) RESPECT NULLS OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        );
        assert_eq!(
            actual_window_none,
            "first_value(a) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        );
        assert_eq!(actual_agg_ignore, "first_value(a) IGNORE NULLS");
        assert_eq!(actual_agg_respect, "first_value(a) RESPECT NULLS");
        assert_eq!(actual_agg_none, "first_value(a)");

        Ok(())
    }
}
