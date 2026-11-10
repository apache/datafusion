// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::planner::{
    PlannerResult, RawBinaryExpr, RawDictionaryExpr, RawFieldAccessExpr,
};
use sqlparser::ast::{Visit, Visitor};
use sqlparser::dialect::bigquery::BigQueryExpr;
use sqlparser::dialect::duckdb::DuckDbExpr;
use sqlparser::dialect::mysql::MySqlExpr;
use sqlparser::dialect::postgres::PostgresExpr;
use sqlparser::dialect::snowflake::{PathElement, SnowflakeExpr};

use crate::ast::{
    BinaryOp, Builtin, BuiltinExpr, CastStyle, DataType as SQLDataType, Expr as SQLExpr,
    ExprKind, ExprWithAlias as SQLExprWithAlias, GroupingKind, Ident, IsTest, LikeKind,
    Literal as SQLLiteral, ObjectName, Quantifier, Query, Span as SQLSpan, SpecialExpr,
    TrimSide,
};

use datafusion_common::{
    DFSchema, Diagnostic, HashMap, Result, ScalarValue, Span, internal_datafusion_err,
    internal_err, not_impl_err, plan_err,
};

use datafusion_expr::expr::InList;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr::SetQuantifier;
use datafusion_expr::utils::{find_window_exprs, window_function_not_allowed_err};
use datafusion_expr::{
    Between, BinaryExpr, Cast, Expr, ExprSchemable, GetFieldAccess, Like, Literal,
    Operator, TryCast, lit, when,
};

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_functions_nested::expr_fn::{
    array_has, array_max, array_min, array_position, cardinality,
};

mod binary_op;
mod function;
mod grouping_set;
mod identifier;
mod order_by;
mod subquery;
mod substring;
mod unary_op;
mod value;

pub(crate) use binary_op::binary_op_to_static;

/// Returns `None` if `expr` is not a NULL literal, and `Some(span)` where
/// `span` is the literal's span, if it has one.
#[expect(
    clippy::option_option,
    reason = "The two levels mean different things, see above"
)]
fn null_value_span(
    expr: &SQLExpr<'_>,
    to_span: &dyn Fn(SQLSpan) -> Option<Span>,
) -> Option<Option<Span>> {
    if expr.kind == ExprKind::Literal(SQLLiteral::Null) {
        Some(to_span(expr.span))
    } else {
        None
    }
}

fn null_equality_warning(
    expr: &SQLExpr<'_>,
    to_span: &dyn Fn(SQLSpan) -> Option<Span>,
) -> Option<Diagnostic> {
    let ExprKind::Binary { left, op, right } = &expr.kind else {
        return None;
    };

    let null_span =
        null_value_span(left, to_span).or_else(|| null_value_span(right, to_span))?;

    let (message, help) = match op {
        BinaryOp::Eq => (
            "comparison with NULL using `=` always evaluates to NULL",
            "use `IS NULL` to check for NULL values",
        ),
        BinaryOp::NotEq => (
            "comparison with NULL using `<>` always evaluates to NULL",
            "use `IS NOT NULL` to check for non-NULL values",
        ),
        _ => return None,
    };

    Some(Diagnostic::new_warning(message, to_span(expr.span)).with_help(help, null_span))
}

struct NullEqualityPredicateVisitor<'a, 'b, S: ContextProvider> {
    sql_to_rel: &'a SqlToRel<'b, S>,
    subquery_depth: usize,
}

impl<'a, 'b, S: ContextProvider> NullEqualityPredicateVisitor<'a, 'b, S> {
    fn new(sql_to_rel: &'a SqlToRel<'b, S>) -> Self {
        Self {
            sql_to_rel,
            subquery_depth: 0,
        }
    }
}

impl<'s, S: ContextProvider> Visitor<'s, Builtin>
    for NullEqualityPredicateVisitor<'_, '_, S>
{
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query<'s>) -> ControlFlow<Self::Break> {
        self.subquery_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query<'s>) -> ControlFlow<Self::Break> {
        self.subquery_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &SQLExpr<'s>) -> ControlFlow<Self::Break> {
        if self.subquery_depth == 0
            && let Some(warning) =
                null_equality_warning(expr, &|span| self.sql_to_rel.sql_span(span))
        {
            self.sql_to_rel.add_warning(warning);
        }
        ControlFlow::Continue(())
    }
}

/// Finds the location of the first window function call in a SQL expression.
/// An identifier that is an alias of a `SELECT` expression containing a window
/// function call counts as well, since aliases are resolved before the window
/// function check (`HAVING total > 0` where `total` is `sum(x) OVER ()`).
/// Subqueries are skipped: window functions are legal there.
struct WindowFunctionSpanVisitor<'a, 'b, S: ContextProvider> {
    sql_to_rel: &'a SqlToRel<'b, S>,
    aliases: &'a HashMap<String, Expr>,
    subquery_depth: usize,
    span: Option<SQLSpan>,
}

impl<'s, S: ContextProvider> Visitor<'s, Builtin>
    for WindowFunctionSpanVisitor<'_, '_, S>
{
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query<'s>) -> ControlFlow<Self::Break> {
        self.subquery_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query<'s>) -> ControlFlow<Self::Break> {
        self.subquery_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &SQLExpr<'s>) -> ControlFlow<Self::Break> {
        if self.subquery_depth > 0 {
            return ControlFlow::Continue(());
        }
        let found = match &expr.kind {
            ExprKind::Function {
                options: Some(options),
                ..
            } => options.over.is_some(),
            ExprKind::Ident(ident) => {
                let name = self.sql_to_rel.ident_normalizer.normalize(ident.clone());
                self.aliases
                    .get(&name)
                    .is_some_and(|aliased| !find_window_exprs([aliased]).is_empty())
            }
            _ => false,
        };
        if found {
            self.span = Some(expr.span);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

/// Help for a window function in `WHERE` or `HAVING`: `QUALIFY` is the clause
/// that filters on window function results.
pub(crate) const QUALIFY_HELP: &str = "Move the condition that uses this window function to a QUALIFY clause, which is evaluated after window functions are computed";

/// Returns an error if `expr`, planned from a clause where window functions
/// cannot be evaluated, contains a window function call. `clause` names it in
/// the message, `help` says how to rewrite the query and `span` is the location
/// of the call in the SQL text, see [`SqlToRel::window_function_span`].
///
/// The physical planner rejects such expressions in every position, but
/// without a span or a clause-specific hint. This check exists to give a
/// better error for the common mistakes.
pub(crate) fn reject_window_functions(
    expr: &Expr,
    clause: &str,
    help: &str,
    span: Option<Span>,
) -> Result<()> {
    match find_window_exprs([expr]).into_iter().next() {
        None => Ok(()),
        Some(window) => Err(window_function_not_allowed_err(&window, clause, span, help)),
    }
}

/// Names that an earlier version of `sqlparser` parsed as calls to functions
/// without arguments when written as bare keywords, such as `current_date`.
fn is_keyword_function(ident: &Ident<'_>) -> bool {
    const NAMES: [&str; 10] = [
        "current_catalog",
        "current_date",
        "current_role",
        "current_time",
        "current_timestamp",
        "current_user",
        "localtime",
        "localtimestamp",
        "session_user",
        "user",
    ];
    ident.quote.is_none()
        && NAMES
            .iter()
            .any(|name| ident.text.eq_ignore_ascii_case(name))
}

/// One step after the root of `a.b[1]['c'][2:3]`.
enum Access<'s> {
    /// `.name`
    Field(Ident<'s>),
    /// `[index]`
    Index(SQLExpr<'s>),
    /// `[lower:upper:step]`
    Slice {
        lower: Option<SQLExpr<'s>>,
        upper: Option<SQLExpr<'s>>,
        step: Option<SQLExpr<'s>>,
    },
}

/// The parts of a binary operation, or the expression back if it is not one.
type BinaryParts<'s> = (Box<SQLExpr<'s>>, BinaryOp<'static>, Box<SQLExpr<'s>>);

/// Splits a binary operation, including those that dialects write as their
/// own nodes, into its operands and operator. Anything else is handed back.
#[expect(
    clippy::result_large_err,
    reason = "the expression is handed back, not built"
)]
fn into_binary_parts(
    expr: SQLExpr<'_>,
) -> std::result::Result<BinaryParts<'_>, SQLExpr<'_>> {
    let span = expr.span;
    match expr.kind {
        ExprKind::Binary { left, op, right } => {
            Ok((left, binary_op_to_static(op), right))
        }
        ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Binary(binary)))
            if binary.qualified.is_none() =>
        {
            let binary = *binary;
            Ok((
                Box::new(binary.left),
                BinaryOp::Named(binary.op.into_owned().into()),
                Box::new(binary.right),
            ))
        }
        ExprKind::Ext(BuiltinExpr::MySql(MySqlExpr::IntDiv { left, right })) => {
            Ok((left, BinaryOp::Named("DIV".into()), right))
        }
        kind => Err(SQLExpr::new(kind, span)),
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn warn_on_null_equality_predicate(&self, predicate: &SQLExpr<'_>) {
        let mut visitor = NullEqualityPredicateVisitor::new(self);
        let _ = predicate.visit(&mut visitor);
    }

    /// The location in the SQL text of the first window function call in
    /// `expr`, or of the first identifier that is an alias (in `aliases`) of a
    /// `SELECT` expression containing one. Falls back to the location of the
    /// whole of `expr`, and returns `None` only if the parser recorded no
    /// spans.
    ///
    /// Used to point the [`Diagnostic`] for a window function in `WHERE` or
    /// `HAVING` at the offending call, which the planned [`Expr`] cannot do:
    /// only columns carry spans there.
    pub(crate) fn window_function_span(
        &self,
        expr: &SQLExpr<'_>,
        aliases: &HashMap<String, Expr>,
    ) -> Option<Span> {
        let mut visitor = WindowFunctionSpanVisitor {
            sql_to_rel: self,
            aliases,
            subquery_depth: 0,
            span: None,
        };
        let _ = expr.visit(&mut visitor);
        self.sql_span(visitor.span.unwrap_or(expr.span))
    }

    pub(crate) fn sql_expr_to_logical_expr_with_alias(
        &self,
        sql: SQLExprWithAlias<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr =
            self.sql_expr_to_logical_expr(sql.expr, schema, planner_context)?;
        if let Some(alias) = sql.alias {
            expr = expr.alias(alias.value().into_owned());
        }
        Ok(expr)
    }
    pub(crate) fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        enum StackEntry<'s> {
            SQLExpr(Box<SQLExpr<'s>>),
            Operator(BinaryOp<'static>),
        }

        // Virtual stack machine to convert SQLExpr to Expr
        // This allows visiting the expr tree in a depth-first manner which
        // produces expressions in postfix notations, i.e. `a + b` => `a b +`.
        // See https://github.com/apache/datafusion/issues/1444
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sql))];
        let mut eval_stack = vec![];

        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sql_expr) => {
                    match into_binary_parts(*sql_expr) {
                        Ok((left, op, right)) => {
                            // Note the order that we push the entries to the stack
                            // is important. We want to visit the left node first.
                            stack.push(StackEntry::Operator(op));
                            stack.push(StackEntry::SQLExpr(right));
                            stack.push(StackEntry::SQLExpr(left));
                        }
                        Err(sql_expr) => {
                            let expr = self.sql_expr_to_logical_expr_internal(
                                sql_expr,
                                schema,
                                planner_context,
                            )?;
                            eval_stack.push(expr);
                        }
                    }
                }
                StackEntry::Operator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let expr = self.build_logical_expr(op, left, right, schema)?;
                    eval_stack.push(expr);
                }
            }
        }

        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    fn build_logical_expr(
        &self,
        op: BinaryOp<'static>,
        left: Expr,
        right: Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let binary_expr = RawBinaryExpr { op, left, right };
        match self.try_plan_binary_op(binary_expr, schema)? {
            PlannerResult::Planned(expr) => Ok(expr),
            PlannerResult::Original(RawBinaryExpr { op, left, right }) => {
                self.build_binary_expr(&op, left, right)
            }
        }
    }

    fn try_plan_binary_op(
        &self,
        mut binary_expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_binary_op(binary_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(PlannerResult::Planned(expr));
                }
                PlannerResult::Original(expr) => {
                    binary_expr = expr;
                }
            }
        }
        Ok(PlannerResult::Original(binary_expr))
    }

    pub fn sql_to_expr_with_alias(
        &self,
        sql: SQLExprWithAlias<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr =
            self.sql_expr_to_logical_expr_with_alias(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        let (expr, _) = expr.infer_placeholder_types(schema)?;
        Ok(expr)
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_expr(
        &self,
        sql: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // The location of the original SQL expression in the source code
        let mut expr = self.sql_expr_to_logical_expr(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, std::slice::from_ref(&expr))?;
        let (expr, _) = expr.infer_placeholder_types(schema)?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema.iter().find(|(qualifier, field)| match qualifier {
                        Some(field_q) => {
                            field.name() == &col.name
                                && field_q.to_string().ends_with(&format!(".{q}"))
                        }
                        _ => false,
                    }) {
                        Some((qualifier, df_field)) => Expr::from((qualifier, df_field)),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    /// Internal implementation. Use
    /// [`Self::sql_expr_to_logical_expr`] to plan exprs.
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn sql_expr_to_logical_expr_internal(
        &self,
        sql: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // NOTE: This function is called recursively, so each match arm body should be as
        //       small as possible to decrease stack requirement.
        //       Follow the common pattern of extracting into a separate function for
        //       non-trivial arms. See https://github.com/apache/datafusion/pull/12384 for
        //       more context.
        let span = sql.span;
        match sql.kind {
            ExprKind::Literal(value) => self.parse_value(value),
            ExprKind::Placeholder(param) => Self::create_placeholder_expr(
                param.into_owned(),
                planner_context.prepare_param_data_types(),
            ),
            ExprKind::Interval(interval) => self.sql_interval_to_expr(false, *interval),
            ExprKind::Ident(id) if is_keyword_function(&id) => self
                .function_args_to_logical_expr(
                    function::FunctionArgs::without_parentheses(ObjectName {
                        parts: vec![id],
                    }),
                    schema,
                    planner_context,
                ),
            ExprKind::Ident(id) => {
                self.sql_identifier_to_expr(id, schema, planner_context)
            }
            ExprKind::Compound(name) => {
                self.sql_compound_identifier_to_expr(name.parts, schema, planner_context)
            }

            ExprKind::Case(case) => {
                let case = *case;
                self.sql_case_identifier_to_expr(
                    case.operand,
                    case.branches,
                    case.else_result,
                    schema,
                    planner_context,
                )
            }

            ExprKind::Cast { expr, ty, style } => {
                self.sql_cast_style_to_expr(*expr, &ty, style, schema, planner_context)
            }

            ExprKind::TypedString(typed) => Ok(Expr::Cast(Cast::new_from_field(
                Box::new(lit(typed.value.value().into_owned())),
                self.convert_data_type_to_field(&typed.ty)?,
            ))),

            ExprKind::Is {
                expr,
                test,
                negated,
            } => self.sql_is_to_expr(*expr, test, negated, schema, planner_context),

            ExprKind::Unary { op, expr } => {
                self.parse_sql_unary_op(op, *expr, schema, planner_context)
            }

            ExprKind::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(self.sql_expr_to_logical_expr(
                    *expr,
                    schema,
                    planner_context,
                )?),
                negated,
                Box::new(self.sql_expr_to_logical_expr(*low, schema, planner_context)?),
                Box::new(self.sql_expr_to_logical_expr(
                    *high,
                    schema,
                    planner_context,
                )?),
            ))),

            ExprKind::InList {
                expr,
                list,
                negated,
            } => self.sql_in_list_to_expr(*expr, list, negated, schema, planner_context),

            ExprKind::Like {
                expr,
                pattern,
                negated,
                kind,
                escape,
            } => match kind {
                LikeKind::Like | LikeKind::ILike => self.sql_like_to_expr(
                    negated,
                    *expr,
                    *pattern,
                    escape,
                    schema,
                    planner_context,
                    kind == LikeKind::ILike,
                ),
                LikeKind::SimilarTo => self.sql_similarto_to_expr(
                    negated,
                    *expr,
                    *pattern,
                    escape,
                    schema,
                    planner_context,
                ),
                LikeKind::Named(name) => {
                    not_impl_err!("Unsupported ast node in sqltorel: {name}")
                }
            },

            ExprKind::Binary { .. } => {
                internal_err!("binary_op should be handled by sql_expr_to_logical_expr.")
            }

            ExprKind::Function {
                name,
                args,
                options,
            } => self.sql_function_to_expr(name, args, options, schema, planner_context),

            ExprKind::Nested(e) => {
                self.sql_expr_to_logical_expr(*e, schema, planner_context)
            }

            ExprKind::Exists(subquery) => {
                self.parse_exists_subquery(*subquery, false, schema, planner_context)
            }
            ExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context)
            }
            ExprKind::Subquery(subquery) => {
                self.parse_scalar_subquery(*subquery, schema, planner_context)
            }

            ExprKind::Special(special) => {
                self.sql_special_expr_to_expr(*special, span, schema, planner_context)
            }
            ExprKind::Ext(ext) => {
                self.sql_ext_expr_to_expr(ext, span, schema, planner_context)
            }
        }
    }

    /// Plans the less common expressions in [`SpecialExpr`].
    fn sql_special_expr_to_expr(
        &self,
        special: SpecialExpr<'_>,
        span: SQLSpan,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match special {
            SpecialExpr::Extract { field, expr } => {
                self.sql_extract_to_expr(&field, expr, schema, planner_context)
            }

            // <expr>["foo"], <expr>[4], <expr>[4:5] or <expr>.foo
            special @ (SpecialExpr::Subscript { .. }
            | SpecialExpr::Slice { .. }
            | SpecialExpr::FieldAccess { .. }) => self.sql_compound_field_access_to_expr(
                SQLExpr::new(ExprKind::Special(Box::new(special)), span),
                schema,
                planner_context,
            ),

            SpecialExpr::Quantified {
                left,
                op,
                quantifier,
                right,
            } => self.sql_quantified_to_expr(
                left,
                &op,
                quantifier,
                right,
                schema,
                planner_context,
            ),

            SpecialExpr::Array(elements) => self.sql_array_literal(elements, schema),
            SpecialExpr::Tuple(values) => {
                self.parse_tuple(schema, planner_context, values)
            }

            #[cfg(feature = "unicode_expressions")]
            SpecialExpr::Substring {
                expr,
                from,
                length,
                shorthand: _,
            } => self.sql_substring_to_expr(expr, from, length, schema, planner_context),

            #[cfg(not(feature = "unicode_expressions"))]
            SpecialExpr::Substring { .. } => {
                internal_err!(
                    "statement substring requires compilation with feature flag: unicode_expressions."
                )
            }

            SpecialExpr::Trim { side, what, expr } => {
                self.sql_trim_to_expr(expr, side, what, schema, planner_context)
            }

            SpecialExpr::Grouping { kind, sets } => match kind {
                GroupingKind::Rollup => {
                    self.sql_rollup_to_expr(sets, schema, planner_context)
                }
                GroupingKind::Cube => {
                    self.sql_cube_to_expr(sets, schema, planner_context)
                }
                GroupingKind::Sets => {
                    self.sql_grouping_sets_to_expr(sets, schema, planner_context)
                }
            },

            SpecialExpr::Round { ceil, .. } => {
                if ceil {
                    not_impl_err!("CEIL with datetime is not supported")
                } else {
                    not_impl_err!("FLOOR with datetime is not supported")
                }
            }
            SpecialExpr::Overlay {
                expr,
                what,
                from,
                length,
            } => self.sql_overlay_to_expr(
                expr,
                what,
                from,
                length,
                schema,
                planner_context,
            ),

            SpecialExpr::Position { needle, haystack } => {
                self.sql_position_to_expr(needle, haystack, schema, planner_context)
            }
            SpecialExpr::AtTimeZone { expr, zone } => {
                self.sql_at_time_zone_to_expr(expr, &zone, schema, planner_context)
            }

            other => not_impl_err!("Unsupported ast node in sqltorel: {other:?}"),
        }
    }

    /// Plans expressions that only some dialects have.
    fn sql_ext_expr_to_expr(
        &self,
        ext: BuiltinExpr<'_>,
        span: SQLSpan,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match ext {
            BuiltinExpr::DuckDb(DuckDbExpr::List(elements))
            | BuiltinExpr::BigQuery(BigQueryExpr::Array(elements)) => {
                self.sql_array_literal(elements, schema)
            }
            BuiltinExpr::DuckDb(DuckDbExpr::Struct(fields)) => {
                self.try_plan_dictionary_literal(fields, schema, planner_context)
            }
            BuiltinExpr::DuckDb(DuckDbExpr::Map(entries)) => {
                self.try_plan_map_literal(entries, schema, planner_context)
            }
            BuiltinExpr::DuckDb(DuckDbExpr::ColonPlaceholder(name)) => {
                Self::create_placeholder_expr(
                    format!(":{name}"),
                    planner_context.prepare_param_data_types(),
                )
            }
            // `receiver.f(args)` is `f(receiver, args)`
            BuiltinExpr::DuckDb(DuckDbExpr::MethodCall(call)) => {
                let call = *call;
                match call.call.kind {
                    ExprKind::Function {
                        name,
                        mut args,
                        options,
                    } => {
                        args.insert(0, crate::ast::FunctionArg::Expr(call.receiver));
                        self.sql_function_to_expr(
                            name,
                            args,
                            options,
                            schema,
                            planner_context,
                        )
                    }
                    other => {
                        internal_err!(
                            "Expected a function call in a method call: {other:?}"
                        )
                    }
                }
            }
            BuiltinExpr::BigQuery(BigQueryExpr::Struct(s)) => {
                if s.ty.is_some() {
                    return not_impl_err!("Struct fields are not supported yet");
                }
                let values = s
                    .values
                    .into_iter()
                    .map(|value| (value.expr, value.alias))
                    .collect();
                self.parse_struct(schema, planner_context, values)
            }
            BuiltinExpr::Snowflake(SnowflakeExpr::Path { value, path }) => {
                self.parse_json_access(schema, planner_context, *value, &path)
            }
            BuiltinExpr::Postgres(PostgresExpr::Binary(binary))
                if binary.qualified.is_none() =>
            {
                // Planned by `sql_expr_to_logical_expr`, which splits binary
                // operations, but kept here in case of a direct call.
                let binary = *binary;
                let expr = SQLExpr::new(
                    ExprKind::Binary {
                        left: Box::new(binary.left),
                        op: BinaryOp::Named(binary.op),
                        right: Box::new(binary.right),
                    },
                    span,
                );
                self.sql_expr_to_logical_expr(expr, schema, planner_context)
            }
            other => not_impl_err!("Unsupported ast node in sqltorel: {other:?}"),
        }
    }

    /// Plans `EXTRACT(field FROM expr)`.
    fn sql_extract_to_expr(
        &self,
        field: &str,
        expr: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // A unit written as a word is a keyword, which `date_part` takes in
        // upper case like the earlier parser rendered it. A quoted unit keeps
        // its quotes, which `date_part` removes.
        let field = if field.starts_with(['\'', '"']) {
            field.to_string()
        } else {
            field.to_ascii_uppercase()
        };
        let mut extract_args = vec![
            Expr::Literal(ScalarValue::from(field), None),
            self.sql_expr_to_logical_expr(expr, schema, planner_context)?,
        ];

        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_extract(extract_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => {
                    extract_args = args;
                }
            }
        }

        not_impl_err!("Extract not supported by ExprPlanner: {extract_args:?}")
    }

    fn sql_cast_style_to_expr(
        &self,
        expr: SQLExpr<'_>,
        data_type: &SQLDataType<'_>,
        style: CastStyle,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match style {
            CastStyle::Function | CastStyle::DoubleColon => {
                self.sql_cast_to_expr(expr, data_type, schema, planner_context)
            }
            CastStyle::TryFunction | CastStyle::SafeFunction => {
                Ok(Expr::TryCast(TryCast::new_from_field(
                    Box::new(self.sql_expr_to_logical_expr(
                        expr,
                        schema,
                        planner_context,
                    )?),
                    self.convert_data_type_to_field(data_type)?,
                )))
            }
        }
    }

    /// Plans `expr IS [NOT] test`.
    fn sql_is_to_expr(
        &self,
        expr: SQLExpr<'_>,
        test: IsTest<'_>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if let IsTest::DistinctFrom(right) = test {
            let op = if negated {
                Operator::IsNotDistinctFrom
            } else {
                Operator::IsDistinctFrom
            };
            return Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
                op,
                Box::new(self.sql_expr_to_logical_expr(
                    *right,
                    schema,
                    planner_context,
                )?),
            )));
        }
        if matches!(test, IsTest::Json { .. } | IsTest::Normalized(_)) {
            return not_impl_err!("Unsupported ast node in sqltorel: IS {test:?}");
        }
        let expr =
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?);
        Ok(match (test, negated) {
            (IsTest::Null, false) => Expr::IsNull(expr),
            (IsTest::Null, true) => Expr::IsNotNull(expr),
            (IsTest::True, false) => Expr::IsTrue(expr),
            (IsTest::True, true) => Expr::IsNotTrue(expr),
            (IsTest::False, false) => Expr::IsFalse(expr),
            (IsTest::False, true) => Expr::IsNotFalse(expr),
            (IsTest::Unknown, false) => Expr::IsUnknown(expr),
            (IsTest::Unknown, true) => Expr::IsNotUnknown(expr),
            (test, _) => {
                return internal_err!("Unexpected IS test: {test:?}");
            }
        })
    }

    /// Plans `left op ANY (right)` and `left op ALL (right)`.
    fn sql_quantified_to_expr(
        &self,
        left: SQLExpr<'_>,
        compare_op: &BinaryOp<'_>,
        quantifier: Quantifier,
        right: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // ANY/SOME are equivalent, the quantifier specifies which the user
        // specified but it doesn't affect the plan
        let quantifier = match quantifier {
            Quantifier::Any | Quantifier::Some => SetQuantifier::Any,
            Quantifier::All => SetQuantifier::All,
        };
        let right_span = right.span;
        match right.kind {
            ExprKind::Subquery(subquery) => self.parse_set_comparison_subquery(
                left,
                *subquery,
                compare_op,
                quantifier,
                schema,
                planner_context,
            ),
            kind => {
                let right = SQLExpr::new(kind, right_span);
                let left_expr = self.sql_to_expr(left, schema, planner_context)?;
                let right_expr = self.sql_to_expr(right, schema, planner_context)?;
                match quantifier {
                    SetQuantifier::Any => plan_any_op(left_expr, right_expr, compare_op),
                    SetQuantifier::All => {
                        plan_all_op(&left_expr, &right_expr, compare_op)
                    }
                }
            }
        }
    }

    fn parse_json_access(
        &self,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        value: SQLExpr<'_>,
        path: &[PathElement<'_>],
    ) -> Result<Expr> {
        let value = self.sql_to_expr(value, schema, planner_context)?;
        let json_path = json_path_to_string(path);
        let json_path = json_path.strip_prefix(":").unwrap_or(&json_path);
        let binary_expr = RawBinaryExpr {
            op: BinaryOp::Named(":".into()),
            left: value,
            right: Expr::Literal(ScalarValue::Utf8(Some(json_path.to_owned())), None),
        };
        let binary_expr = match self.try_plan_binary_op(binary_expr, schema)? {
            PlannerResult::Planned(expr) => return Ok(expr),
            PlannerResult::Original(expr) => expr,
        };

        if !path.is_empty()
            && matches!(&binary_expr.op, BinaryOp::Named(op) if op == ":")
            && is_struct_like(&binary_expr.left.get_type(schema)?)
            && path
                .iter()
                .all(|element| struct_field_name_from_json_path_elem(element).is_some())
        {
            let mut planned = binary_expr.left.clone();
            let mut all_fields_planned = true;

            for element in path {
                let field_name = struct_field_name_from_json_path_elem(element)
                    .expect("all path elements were checked above");
                let field_access = RawFieldAccessExpr {
                    expr: planned,
                    field_access: GetFieldAccess::NamedStructField {
                        name: ScalarValue::from(field_name),
                    },
                };
                match self.try_plan_field_access(field_access, schema)? {
                    PlannerResult::Planned(expr) => planned = expr,
                    PlannerResult::Original(field_access) => {
                        planned = field_access.expr;
                        all_fields_planned = false;
                        break;
                    }
                }
            }

            if all_fields_planned {
                return Ok(planned);
            }
        }

        let RawBinaryExpr { op, left, right } = binary_expr;
        self.build_binary_expr(&op, left, right)
    }

    fn try_plan_field_access(
        &self,
        mut field_access_expr: RawFieldAccessExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_field_access(field_access_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(PlannerResult::Planned(expr));
                }
                PlannerResult::Original(expr) => field_access_expr = expr,
            }
        }
        Ok(PlannerResult::Original(field_access_expr))
    }

    /// Parses a struct(..) expression and plans it creation. A value with a
    /// name, as in `STRUCT(v AS name)`, makes it a named struct.
    fn parse_struct<'s>(
        &self,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        values: Vec<(SQLExpr<'s>, Option<Ident<'s>>)>,
    ) -> Result<Expr> {
        let is_named_struct = values.iter().any(|(_, name)| name.is_some());

        let mut create_struct_args = if is_named_struct {
            self.create_named_struct_expr(values, schema, planner_context)?
        } else {
            self.create_struct_expr(
                values.into_iter().map(|(value, _)| value).collect(),
                schema,
                planner_context,
            )?
        };

        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_struct_literal(create_struct_args, is_named_struct)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => create_struct_args = args,
            }
        }
        not_impl_err!("Struct not supported by ExprPlanner: {create_struct_args:?}")
    }

    fn parse_tuple(
        &self,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        values: Vec<SQLExpr<'_>>,
    ) -> Result<Expr> {
        match values.first().map(|value| &value.kind) {
            Some(ExprKind::Ident(_))
            | Some(ExprKind::Literal(_))
            | Some(ExprKind::Placeholder(_))
            | Some(ExprKind::Compound(_)) => self.parse_struct(
                schema,
                planner_context,
                values.into_iter().map(|value| (value, None)).collect(),
            ),
            None => not_impl_err!("Empty tuple not supported yet"),
            _ => {
                not_impl_err!("Only identifiers and literals are supported in tuples")
            }
        }
    }

    /// Plan `<timestamp> AT TIME ZONE '<tz>'`.
    ///
    /// The result type depends on whether the input has a timezone *after
    /// type coercion*, and type coercion runs after this planner. So this
    /// method only builds `timezone('<tz>', <timestamp>)` through
    /// [`ExprPlanner::plan_at_time_zone`], and that function decides once the
    /// type is known. See `TimezoneFunc` in `datafusion-functions` for the
    /// semantics.
    ///
    /// [`ExprPlanner::plan_at_time_zone`]: datafusion_expr::planner::ExprPlanner::plan_at_time_zone
    fn sql_at_time_zone_to_expr(
        &self,
        timestamp: SQLExpr<'_>,
        time_zone: &SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let tz: Arc<str> = match &time_zone.kind {
            ExprKind::Literal(SQLLiteral::String(s)) if s.quote == '\'' => {
                s.value().as_ref().into()
            }
            _ => {
                return not_impl_err!("Unsupported ast node in sqltorel: {time_zone:?}");
            }
        };

        let expr =
            self.sql_expr_to_logical_expr_internal(timestamp, schema, planner_context)?;

        let mut args = vec![lit(tz.as_ref()), expr];
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_at_time_zone(args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(original) => {
                    args = original;
                }
            }
        }

        not_impl_err!(
            "AT TIME ZONE needs an ExprPlanner that implements `plan_at_time_zone`, \
             such as the `DatetimeFunctionPlanner` of `datafusion-functions`"
        )
    }

    fn sql_position_to_expr(
        &self,
        substr_expr: SQLExpr<'_>,
        str_expr: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let substr =
            self.sql_expr_to_logical_expr(substr_expr, schema, planner_context)?;
        let fullstr = self.sql_expr_to_logical_expr(str_expr, schema, planner_context)?;
        let mut position_args = vec![fullstr, substr];
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_position(position_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => {
                    position_args = args;
                }
            }
        }

        not_impl_err!("Position not supported by ExprPlanner: {position_args:?}")
    }

    /// Plans DuckDB's `{'key': value, ...}`.
    fn try_plan_dictionary_literal<'s>(
        &self,
        fields: Vec<(Ident<'s>, SQLExpr<'s>)>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut keys = Vec::with_capacity(fields.len());
        let mut values = Vec::with_capacity(fields.len());
        for (key, value) in fields {
            let key = lit(key.value().into_owned());
            let value = self.sql_expr_to_logical_expr(value, schema, planner_context)?;
            keys.push(key);
            values.push(value);
        }

        let mut raw_expr = RawDictionaryExpr { keys, values };

        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_dictionary_literal(raw_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => raw_expr = expr,
            }
        }
        not_impl_err!("Dictionary not supported by ExprPlanner: {raw_expr:?}")
    }

    /// Plans DuckDB's `MAP {key: value, ...}`.
    fn try_plan_map_literal<'s>(
        &self,
        entries: Vec<(SQLExpr<'s>, SQLExpr<'s>)>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut exprs: Vec<_> = entries
            .into_iter()
            .flat_map(|(key, value)| [key, value])
            .map(|expr| self.sql_expr_to_logical_expr(expr, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_make_map(exprs)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => exprs = expr,
            }
        }
        not_impl_err!("MAP not supported by ExprPlanner: {exprs:?}")
    }

    // Handles a call to struct(...) where the arguments are named. For example
    // `struct (v as foo, v2 as bar)` by creating a call to the `named_struct` function
    fn create_named_struct_expr<'s>(
        &self,
        values: Vec<(SQLExpr<'s>, Option<Ident<'s>>)>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        Ok(values
            .into_iter()
            .enumerate()
            .map(|(i, (value, name))| {
                let name = match name {
                    Some(name) => name.value().into_owned(),
                    None => format!("c{i}"),
                };
                Ok([
                    name.lit(),
                    self.sql_expr_to_logical_expr(value, input_schema, planner_context)?,
                ])
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    // Handles a call to struct(...) where the arguments are not named. For example
    // `struct (v, v2)` by creating a call to the `struct` function
    // which will create a struct with fields named `c0`, `c1`, etc.
    fn create_struct_expr(
        &self,
        values: Vec<SQLExpr<'_>>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        values
            .into_iter()
            .map(|value| {
                self.sql_expr_to_logical_expr(value, input_schema, planner_context)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn sql_in_list_to_expr(
        &self,
        expr: SQLExpr<'_>,
        list: Vec<SQLExpr<'_>>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let list_expr = list
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::InList(InList::new(
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            list_expr,
            negated,
        )))
    }

    #[expect(clippy::too_many_arguments)]
    fn sql_like_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr<'_>,
        pattern: SQLExpr<'_>,
        escape_char: Option<Box<SQLExpr<'_>>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        case_insensitive: bool,
    ) -> Result<Expr> {
        if matches!(
            &pattern.kind,
            ExprKind::Special(special) if matches!(**special, SpecialExpr::QuantifiedList { .. })
        ) {
            return not_impl_err!("ANY in LIKE expression");
        }
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let escape_char = match escape_char.map(|e| *e) {
            Some(expr) => match escape_character(&expr) {
                Some(c) => Some(c),
                None => {
                    return plan_err!(
                        "Invalid escape character in LIKE expression. Expected a single character wrapped with single quotes, got {expr}"
                    );
                }
            },
            None => None,
        };
        Ok(Expr::Like(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            case_insensitive,
        )))
    }

    fn sql_similarto_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr<'_>,
        pattern: SQLExpr<'_>,
        escape_char: Option<Box<SQLExpr<'_>>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let escape_char = match escape_char.map(|e| *e) {
            Some(expr) => match escape_character(&expr) {
                Some(c) => Some(c),
                None => {
                    return plan_err!(
                        "Invalid escape character in SIMILAR TO expression. Expected a single character wrapped with single quotes, got {expr}"
                    );
                }
            },
            None => None,
        };
        Ok(Expr::SimilarTo(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            false,
        )))
    }

    fn sql_trim_to_expr(
        &self,
        expr: SQLExpr<'_>,
        trim_where: Option<TrimSide>,
        trim_what: Option<SQLExpr<'_>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let args = match trim_what {
            Some(to_trim) => {
                let to_trim =
                    self.sql_expr_to_logical_expr(to_trim, schema, planner_context)?;
                vec![arg, to_trim]
            }
            None => vec![arg],
        };

        let fun_name = match trim_where {
            Some(TrimSide::Leading) => "ltrim",
            Some(TrimSide::Trailing) => "rtrim",
            Some(TrimSide::Both) => "btrim",
            None => "trim",
        };
        let fun = self
            .context_provider
            .get_function_meta(fun_name)
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected '{fun_name}' function")
            })?;

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }

    fn sql_overlay_to_expr(
        &self,
        expr: SQLExpr<'_>,
        overlay_what: SQLExpr<'_>,
        overlay_from: SQLExpr<'_>,
        overlay_for: Option<SQLExpr<'_>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let what_arg =
            self.sql_expr_to_logical_expr(overlay_what, schema, planner_context)?;
        let from_arg =
            self.sql_expr_to_logical_expr(overlay_from, schema, planner_context)?;
        let mut overlay_args = match overlay_for {
            Some(for_expr) => {
                let for_expr =
                    self.sql_expr_to_logical_expr(for_expr, schema, planner_context)?;
                vec![arg, what_arg, from_arg, for_expr]
            }
            None => vec![arg, what_arg, from_arg],
        };
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_overlay(overlay_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => overlay_args = args,
            }
        }
        not_impl_err!("Overlay not supported by ExprPlanner: {overlay_args:?}")
    }

    fn sql_cast_to_expr(
        &self,
        expr: SQLExpr<'_>,
        data_type: &SQLDataType<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let dt = self.convert_data_type_to_field(data_type)?;
        let expr = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;

        // numeric constants are treated as seconds (rather as nanoseconds)
        // to align with postgres / duckdb semantics
        let expr = match dt.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz)
                if expr.get_type(schema)? == DataType::Int64 =>
            {
                Expr::Cast(Cast::new(
                    Box::new(expr),
                    DataType::Timestamp(TimeUnit::Second, tz.clone()),
                ))
            }
            _ => expr,
        };

        Ok(Expr::Cast(Cast::new_from_field(Box::new(expr), dt)))
    }

    /// Extracts the root expression and access chain from a chain of
    /// subscripts, slices and field accesses such as `a.b[1].c`.
    ///
    /// This function attempts to identify if a compound expression (like `a.b.c`) should be treated
    /// as a column reference with a qualifier (like `table.column`) or as a field access expression.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * The resolved root expression
    /// * The remaining access chain that should be processed as field accesses
    fn extract_root_and_access_chain<'s>(
        &self,
        expr: SQLExpr<'s>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Vec<Access<'s>>)> {
        // Walk down to the root, collecting the steps from the last
        let mut access_chain = vec![];
        let mut current = expr;
        loop {
            let span = current.span;
            match current.kind {
                ExprKind::Special(special) => match *special {
                    SpecialExpr::Subscript { expr, index } => {
                        access_chain.push(Access::Index(index));
                        current = expr;
                    }
                    SpecialExpr::Slice {
                        expr,
                        lower,
                        upper,
                        step,
                    } => {
                        access_chain.push(Access::Slice { lower, upper, step });
                        current = expr;
                    }
                    SpecialExpr::FieldAccess { expr, field } => {
                        access_chain.push(Access::Field(field));
                        current = expr;
                    }
                    other => {
                        current = SQLExpr::new(ExprKind::Special(Box::new(other)), span);
                        break;
                    }
                },
                kind => {
                    current = SQLExpr::new(kind, span);
                    break;
                }
            }
        }
        access_chain.reverse();

        let mut compound_idents = match current.kind {
            ExprKind::Ident(ident) => vec![ident],
            ExprKind::Compound(name) => name.parts,
            kind => {
                let root = self.sql_expr_to_logical_expr(
                    SQLExpr::new(kind, current.span),
                    schema,
                    planner_context,
                )?;
                return Ok((root, access_chain));
            }
        };

        let first_non_ident = access_chain
            .iter()
            .position(|access| !matches!(access, Access::Field(_)))
            .unwrap_or(access_chain.len());
        for access in access_chain.drain(0..first_non_ident) {
            if let Access::Field(ident) = access {
                compound_idents.push(ident);
            } else {
                return internal_err!("Expected identifier in access chain");
            }
        }

        let root = if compound_idents.len() == 1 {
            self.sql_identifier_to_expr(
                compound_idents.pop().unwrap(),
                schema,
                planner_context,
            )?
        } else {
            self.sql_compound_identifier_to_expr(
                compound_idents,
                schema,
                planner_context,
            )?
        };
        Ok((root, access_chain))
    }

    fn sql_compound_field_access_to_expr(
        &self,
        expr: SQLExpr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let (root, access_chain) =
            self.extract_root_and_access_chain(expr, schema, planner_context)?;
        let fields = access_chain
            .into_iter()
            .map(|field| match field {
                Access::Index(index) => {
                    // index can be a name, in which case it is a named field access
                    match index.kind {
                        ExprKind::Literal(SQLLiteral::String(s)) => {
                            Ok(Some(GetFieldAccess::NamedStructField {
                                name: ScalarValue::from(s.value().into_owned()),
                            }))
                        }
                        // otherwise treat like a list index
                        kind => Ok(Some(GetFieldAccess::ListIndex {
                            key: Box::new(self.sql_expr_to_logical_expr(
                                SQLExpr::new(kind, index.span),
                                schema,
                                planner_context,
                            )?),
                        })),
                    }
                }
                Access::Slice { lower, upper, step } => {
                    // Means access like [:2]
                    let lower_bound = if let Some(lower_bound) = lower {
                        self.sql_expr_to_logical_expr(
                            lower_bound,
                            schema,
                            planner_context,
                        )
                    } else {
                        not_impl_err!("Slice subscript requires a lower bound")
                    }?;

                    // means access like [2:]
                    let upper_bound = if let Some(upper_bound) = upper {
                        self.sql_expr_to_logical_expr(
                            upper_bound,
                            schema,
                            planner_context,
                        )
                    } else {
                        not_impl_err!("Slice subscript requires an upper bound")
                    }?;

                    // stride, default to 1
                    let stride = if let Some(stride) = step {
                        self.sql_expr_to_logical_expr(stride, schema, planner_context)?
                    } else {
                        lit(1i64)
                    };

                    Ok(Some(GetFieldAccess::ListRange {
                        start: Box::new(lower_bound),
                        stop: Box::new(upper_bound),
                        stride: Box::new(stride),
                    }))
                }
                Access::Field(ident) => Ok(Some(GetFieldAccess::NamedStructField {
                    name: ScalarValue::from(self.ident_normalizer.normalize(ident)),
                })),
            })
            .collect::<Result<Vec<_>>>()?;

        fields
            .into_iter()
            .flatten()
            .try_fold(root, |expr, field_access| {
                let field_access_expr = RawFieldAccessExpr { expr, field_access };
                match self.try_plan_field_access(field_access_expr, schema)? {
                    PlannerResult::Planned(expr) => Ok(expr),
                    PlannerResult::Original(field_access_expr) => not_impl_err!(
                        "GetFieldAccess not supported by ExprPlanner: {field_access_expr:?}"
                    ),
                }
            })
    }
}

/// The character of a `LIKE ... ESCAPE 'c'` clause, if `expr` is a string of
/// one character.
fn escape_character(expr: &SQLExpr<'_>) -> Option<char> {
    match &expr.kind {
        ExprKind::Literal(SQLLiteral::String(s)) if s.quote == '\'' => {
            let value = s.value();
            if value.len() == 1 {
                value.chars().next()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Renders a Snowflake path such as `:a.b[0]` the way the earlier parser did.
fn json_path_to_string(path: &[PathElement<'_>]) -> String {
    let mut rendered = String::new();
    for element in path {
        match element {
            PathElement::Colon(key) => {
                rendered.push(':');
                rendered.push_str(&key.to_string());
            }
            PathElement::Dot(key) => {
                rendered.push('.');
                rendered.push_str(&key.to_string());
            }
            PathElement::Index(index) => {
                rendered.push('[');
                rendered.push_str(&index.to_string());
                rendered.push(']');
            }
        }
    }
    rendered
}

fn is_struct_like(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(_))
        || matches!(
            data_type,
            DataType::Dictionary(_, value_type)
                if matches!(value_type.as_ref(), DataType::Struct(_))
        )
}

fn struct_field_name_from_json_path_elem(element: &PathElement<'_>) -> Option<String> {
    match element {
        PathElement::Colon(key) | PathElement::Dot(key) => Some(key.value().into_owned()),
        PathElement::Index(SQLExpr {
            kind: ExprKind::Literal(SQLLiteral::String(key)),
            ..
        }) => Some(key.value().into_owned()),
        PathElement::Index(_) => None,
    }
}

/// Builds a CASE expression that handles NULL semantics for `x <op> ANY(arr)`:
///
/// ```text
/// CASE
///   WHEN <min_or_max>(arr) IS NOT NULL THEN <comparison>
///   WHEN arr IS NOT NULL THEN FALSE          -- empty or all-null array
///   ELSE NULL                                -- NULL array
/// END
/// ```
fn any_op_with_null_handling(bound: Expr, comparison: Expr, arr: Expr) -> Result<Expr> {
    when(bound.is_not_null(), comparison)
        .when(arr.is_not_null(), lit(false))
        .otherwise(lit(ScalarValue::Boolean(None)))
}

/// Plans a `<left> <op> ANY(<right>)` expression for non-subquery operands.
fn plan_any_op(
    left_expr: Expr,
    right_expr: Expr,
    compare_op: &BinaryOp<'_>,
) -> Result<Expr> {
    match compare_op {
        BinaryOp::Eq => Ok(array_has(right_expr, left_expr)),
        BinaryOp::NotEq => {
            let min = array_min(right_expr.clone());
            let max = array_max(right_expr.clone());
            // NOT EQ is true when either bound differs from left
            let comparison = min
                .not_eq(left_expr.clone())
                .or(max.clone().not_eq(left_expr));
            any_op_with_null_handling(max, comparison, right_expr)
        }
        BinaryOp::Gt => {
            let min = array_min(right_expr.clone());
            any_op_with_null_handling(min.clone(), min.lt(left_expr), right_expr)
        }
        BinaryOp::Lt => {
            let max = array_max(right_expr.clone());
            any_op_with_null_handling(max.clone(), max.gt(left_expr), right_expr)
        }
        BinaryOp::GtEq => {
            let min = array_min(right_expr.clone());
            any_op_with_null_handling(min.clone(), min.lt_eq(left_expr), right_expr)
        }
        BinaryOp::LtEq => {
            let max = array_max(right_expr.clone());
            any_op_with_null_handling(max.clone(), max.gt_eq(left_expr), right_expr)
        }
        _ => plan_err!(
            "Unsupported AnyOp: '{compare_op}', only '=', '<>', '>', '<', '>=', '<=' are supported"
        ),
    }
}

/// Plans `needle <compare_op> ALL(haystack)` with proper SQL NULL semantics.
///
/// CASE/WHEN structure:
///   WHEN arr IS NULL        → NULL
///   WHEN empty              → TRUE
///   WHEN lhs IS NULL        → NULL
///   WHEN decisive_condition → FALSE
///   WHEN has_nulls          → NULL
///   ELSE                    → TRUE
fn plan_all_op(
    needle: &Expr,
    haystack: &Expr,
    compare_op: &BinaryOp<'_>,
) -> Result<Expr> {
    let null_arr_check = haystack.clone().is_null();
    let empty_check = cardinality(haystack.clone()).eq(lit(0u64));
    let null_lhs_check = needle.clone().is_null();
    // DataFusion's array_position uses is_null() checks internally (not equality),
    // so it can locate NULL elements even though NULL = NULL is NULL in standard SQL.
    let has_nulls =
        array_position(haystack.clone(), lit(ScalarValue::Null), lit(1i64)).is_not_null();

    let decisive_condition = match compare_op {
        BinaryOp::NotEq => array_has(haystack.clone(), needle.clone()),
        BinaryOp::Eq => {
            let all_equal = array_min(haystack.clone())
                .eq(needle.clone())
                .and(array_max(haystack.clone()).eq(needle.clone()));
            Expr::Not(Box::new(all_equal))
        }
        BinaryOp::Gt => {
            Expr::Not(Box::new(needle.clone().gt(array_max(haystack.clone()))))
        }
        BinaryOp::Lt => {
            Expr::Not(Box::new(needle.clone().lt(array_min(haystack.clone()))))
        }
        BinaryOp::GtEq => {
            Expr::Not(Box::new(needle.clone().gt_eq(array_max(haystack.clone()))))
        }
        BinaryOp::LtEq => {
            Expr::Not(Box::new(needle.clone().lt_eq(array_min(haystack.clone()))))
        }
        _ => {
            return plan_err!(
                "Unsupported AllOp: '{compare_op}', only '=', '<>', '>', '<', '>=', '<=' are supported"
            );
        }
    };

    let null_bool = lit(ScalarValue::Boolean(None));
    when(null_arr_check, null_bool.clone())
        .when(empty_check, lit(true))
        .when(null_lhs_check, null_bool.clone())
        .when(decisive_condition, lit(false))
        .when(has_nulls, null_bool)
        .otherwise(lit(true))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use sqlparser::dialect::DuckDb;

    use datafusion_common::TableReference;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{
        AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF,
    };

    use super::*;

    struct TestContextProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl TestContextProvider {
        pub fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "table1".to_string(),
                create_table_source(vec![Field::new(
                    "column1".to_string(),
                    DataType::Utf8,
                    false,
                )]),
            );

            Self {
                options: Default::default(),
                tables,
            }
        }
    }

    impl ContextProvider for TestContextProvider {
        fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(Arc::clone(table)),
                _ => plan_err!("Table not found: {}", name.table()),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
            None
        }

        fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
            match name {
                "sum" => Some(datafusion_functions_aggregate::sum::sum_udaf()),
                _ => None,
            }
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }

        fn udf_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn higher_order_function_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udaf_names(&self) -> Vec<String> {
            vec!["sum".to_string()]
        }

        fn udwf_names(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }

    macro_rules! test_stack_overflow {
        ($name:ident, $num_expr:expr) => {
            #[test]
            fn $name() {
                let schema = DFSchema::empty();
                let mut planner_context = PlannerContext::default();

                let expr_str = (0..$num_expr)
                    .map(|i| format!("column1 = 'value{:?}'", i))
                    .collect::<Vec<String>>()
                    .join(" OR ");

                let sql_expr = sqlparser::parse_expr::<DuckDb>(&expr_str).unwrap();

                let context_provider = TestContextProvider::new();
                let sql_to_rel = SqlToRel::new(&context_provider);

                // Should not stack overflow
                sql_to_rel
                    .sql_expr_to_logical_expr(sql_expr, &schema, &mut planner_context)
                    .unwrap();
            }
        };
    }

    test_stack_overflow!(test_stack_overflow_64, 64);
    test_stack_overflow!(test_stack_overflow_128, 128);
    test_stack_overflow!(test_stack_overflow_256, 256);
    test_stack_overflow!(test_stack_overflow_512, 512);
    test_stack_overflow!(test_stack_overflow_1024, 1024);
    test_stack_overflow!(test_stack_overflow_2048, 2048);
    test_stack_overflow!(test_stack_overflow_4096, 4096);
    test_stack_overflow!(test_stack_overflow_8192, 8192);
    #[test]
    fn test_sql_to_expr_with_alias() {
        let schema = DFSchema::empty();
        let mut planner_context = PlannerContext::default();

        let sql_expr = SQLExprWithAlias {
            expr: sqlparser::parse_expr::<DuckDb>("SUM(int_col)").unwrap(),
            alias: Some(Ident {
                text: "sum_int_col".into(),
                quote: None,
                span: Default::default(),
            }),
        };

        let context_provider = TestContextProvider::new();
        let sql_to_rel = SqlToRel::new(&context_provider);

        let expr = sql_to_rel
            .sql_expr_to_logical_expr_with_alias(sql_expr, &schema, &mut planner_context)
            .unwrap();

        assert!(matches!(expr, Expr::Alias(_)));
    }

    #[test]
    fn test_parse_numbers_with_underscores() {
        use datafusion_common::ScalarValue::*;

        let context_provider = TestContextProvider::new();
        let sql_to_rel = SqlToRel::new(&context_provider);

        // (input, positive result, negative result)
        let test_cases = [
            ("1_000", Int64(Some(1000)), Int64(Some(-1000))),
            ("100_000", Int64(Some(100000)), Int64(Some(-100000))),
            ("1_2_3_4", Int64(Some(1234)), Int64(Some(-1234))),
            ("0_0", Int64(Some(0)), Int64(Some(-0))),
            ("1_23.4_56", Float64(Some(123.456)), Float64(Some(-123.456))),
        ];

        for (literal, out_positive, out_negative) in test_cases {
            assert_eq!(
                sql_to_rel.parse_sql_number(literal, false).unwrap(),
                Expr::Literal(out_positive, None)
            );
            assert_eq!(
                sql_to_rel.parse_sql_number(literal, true).unwrap(),
                Expr::Literal(out_negative, None)
            );
        }
    }
}
