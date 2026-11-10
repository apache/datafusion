// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use crate::ast::{
    BuiltinExpr, Expr as SQLExpr, ExprKind, FunctionArg, FunctionOptions, Ident,
    ObjectName, OrderByItem, SpecialExpr, WindowOver,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{
    DFSchema, Dependency, Diagnostic, HashSet, Result, Span, datatype::FieldExt,
    internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::{
    Expr, ExprSchemable, LambdaParametersProgress, SortExpr, ValueOrLambda, WindowFrame,
    WindowFunctionDefinition,
    arguments::ArgumentName,
    expr::{
        self, HigherOrderFunction, Lambda, NullTreatment, ScalarFunction, Unnest,
        WildcardOptions, WindowFunction,
    },
    planner::{PlannerResult, RawAggregateExpr, RawScalarExpr, RawWindowExpr},
    type_coercion::functions::value_fields_with_higher_order_udf,
};
use sqlparser::dialect::duckdb::{DuckDbExpr, DuckDbLambda};

/// Suggest a valid function based on an invalid input function name
///
/// Returns `None` if no valid matches are found. This happens when there are no
/// functions registered with the context.
pub fn suggest_valid_function(
    input_function_name: &str,
    is_window_func: bool,
    ctx: &dyn ContextProvider,
) -> Option<String> {
    let mut valid_funcs = Vec::new();
    if is_window_func {
        // All aggregate functions and builtin window functions
        valid_funcs.extend(ctx.udaf_names());
        valid_funcs.extend(ctx.udwf_names());
    } else {
        // All scalar functions and aggregate functions
        valid_funcs.extend(ctx.udf_names());
        valid_funcs.extend(ctx.higher_order_function_names());
        valid_funcs.extend(ctx.udaf_names());
    }
    find_closest_match(valid_funcs, input_function_name)
}

/// Find the closest matching string to the target string in the candidates list, using edit distance(case insensitive)
/// Input `candidates` must not be empty otherwise an error is returned.
fn find_closest_match(candidates: Vec<String>, target: &str) -> Option<String> {
    let target = target.to_lowercase();
    candidates.into_iter().min_by_key(|candidate| {
        datafusion_common::utils::datafusion_strsim::levenshtein(
            &candidate.to_lowercase(),
            &target,
        )
    })
}

/// Arguments for a function call extracted from the SQL AST
#[derive(Debug)]
pub(super) struct FunctionArgs<'s> {
    /// Function name
    name: ObjectName<'s>,
    /// Argument expressions
    args: Vec<FunctionArg<'s>>,
    /// ORDER BY clause, if any
    order_by: Vec<OrderByItem<'s>>,
    /// OVER clause, if any
    over: Option<WindowOver<'s>>,
    /// FILTER clause, if any
    filter: Option<SQLExpr<'s>>,
    /// NULL treatment clause, if any
    null_treatment: Option<NullTreatment>,
    /// DISTINCT
    distinct: bool,
    /// WITHIN GROUP clause, if any
    within_group: Vec<OrderByItem<'s>>,
    /// Was the function called without parenthesis, i.e. could this also be a column reference?
    function_without_parentheses: bool,
}

impl<'s> FunctionArgs<'s> {
    /// A call written without parentheses, such as `current_date`.
    pub(super) fn without_parentheses(name: ObjectName<'s>) -> Self {
        Self {
            name,
            args: vec![],
            order_by: vec![],
            over: None,
            filter: None,
            null_treatment: None,
            distinct: false,
            within_group: vec![],
            function_without_parentheses: true,
        }
    }

    fn try_new(
        name: ObjectName<'s>,
        args: Vec<FunctionArg<'s>>,
        options: Option<Box<FunctionOptions<'s>>>,
    ) -> Result<Self> {
        let Some(options) = options else {
            return Ok(Self {
                name,
                args,
                order_by: vec![],
                over: None,
                filter: None,
                null_treatment: None,
                distinct: false,
                within_group: vec![],
                function_without_parentheses: false,
            });
        };

        let FunctionOptions {
            parameters,
            quantifier,
            order_by,
            within_group,
            filter,
            null_treatment,
            inner_null_treatment,
            separator,
            on_overflow,
            over,
        } = *options;

        if !parameters.is_empty() {
            return not_impl_err!(
                "Calling {name}: parametric function calls are not supported"
            );
        }

        if let Some(sep) = separator {
            return not_impl_err!(
                "Calling {name}: SEPARATOR not supported in function arguments: {sep:?}"
            );
        }

        if on_overflow.is_some() {
            return not_impl_err!(
                "Calling {name}: ON OVERFLOW not supported in function arguments"
            );
        }

        let null_treatment = match (null_treatment, inner_null_treatment) {
            (Some(_), Some(_)) => {
                return not_impl_err!("Calling {name}: Duplicated null treatment clause");
            }
            (outer, inner) => outer.or(inner),
        };

        let distinct = quantifier == sqlparser::ast::SetQuantifier::Distinct;

        if within_group.len() > 1 {
            return not_impl_err!(
                "Only a single ordering expression is permitted in a WITHIN GROUP clause"
            );
        }

        Ok(Self {
            name,
            args,
            order_by,
            over,
            filter,
            null_treatment: null_treatment.map(|v| v.into()),
            distinct,
            within_group,
            function_without_parentheses: false,
        })
    }
}

/// A lambda function argument: `x -> body` or DuckDB's `lambda x: body`.
#[derive(Debug, Clone)]
struct SqlLambda<'s> {
    params: Vec<Ident<'s>>,
    body: SQLExpr<'s>,
}

impl<'s> SqlLambda<'s> {
    /// Returns the lambda if `arg` is an unnamed lambda argument.
    fn from_arg(arg: FunctionArg<'s>) -> std::result::Result<Self, FunctionArg<'s>> {
        let FunctionArg::Expr(expr) = arg else {
            return Err(arg);
        };
        match expr.kind {
            ExprKind::Special(special)
                if matches!(*special, SpecialExpr::Lambda { .. }) =>
            {
                let SpecialExpr::Lambda { params, body, .. } = *special else {
                    unreachable!()
                };
                Ok(Self { params, body })
            }
            ExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::Lambda(lambda))) => {
                let DuckDbLambda { params, body } = *lambda;
                Ok(Self { params, body })
            }
            kind => Err(FunctionArg::Expr(SQLExpr::new(kind, expr.span))),
        }
    }
}

/// The arguments and options of a call, handed back when it is not planned.
type CallParts<'s> = (Vec<FunctionArg<'s>>, Option<Box<FunctionOptions<'s>>>);

// Helper type for extracting WITHIN GROUP ordering and prepended args
type WithinGroupExtraction = (Vec<SortExpr>, Vec<Expr>, Vec<Option<ArgumentName>>);

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_function_to_expr<'s>(
        &self,
        name: ObjectName<'s>,
        args: Vec<FunctionArg<'s>>,
        options: Option<Box<FunctionOptions<'s>>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // `SUBSTRING(a, b, c)` and `TRIM(a, b)` are planned like their keyword
        // forms, `SUBSTRING(a FROM b FOR c)` and `TRIM(b FROM a)`.
        let (args, options) = match self.try_plan_special_function(
            &name,
            args,
            options,
            schema,
            planner_context,
        )? {
            Ok(expr) => return Ok(expr),
            Err(original) => original,
        };
        let function_args = FunctionArgs::try_new(name, args, options)?;
        self.function_args_to_logical_expr(function_args, schema, planner_context)
    }

    /// Plans `SUBSTRING(...)`, `SUBSTR(...)` and `TRIM(...)` written with
    /// commas, as the parser of an earlier version of `sqlparser` gave them
    /// their own expressions. Hands the arguments back for other functions.
    fn try_plan_special_function<'s>(
        &self,
        name: &ObjectName<'s>,
        args: Vec<FunctionArg<'s>>,
        options: Option<Box<FunctionOptions<'s>>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<std::result::Result<Expr, CallParts<'s>>> {
        let [part] = name.parts.as_slice() else {
            return Ok(Err((args, options)));
        };
        if options.is_some()
            || part.quote.is_some()
            || !args.iter().all(|arg| matches!(arg, FunctionArg::Expr(_)))
        {
            return Ok(Err((args, options)));
        }
        // `CEIL(x, scale)` and `FLOOR(x, scale)` were their own expressions
        // too, and the scale is not supported.
        for (function, name) in [("ceil", "CEIL"), ("floor", "FLOOR")] {
            if part.text.eq_ignore_ascii_case(function) && args.len() == 2 {
                return not_impl_err!("{name} with scale is not supported");
            }
        }
        let is_substring = part.text.eq_ignore_ascii_case("substring")
            || part.text.eq_ignore_ascii_case("substr");
        let is_trim = part.text.eq_ignore_ascii_case("trim");
        let arity_ok = (is_substring && (1..=3).contains(&args.len()))
            || (is_trim && (args.len() == 1 || args.len() == 2));
        if !arity_ok {
            return Ok(Err((args, options)));
        }
        let mut exprs = args.into_iter().map(|arg| match arg {
            FunctionArg::Expr(expr) => expr,
            _ => unreachable!("checked above"),
        });
        let first = exprs.next().unwrap();
        let second = exprs.next();
        let third = exprs.next();
        let expr = if is_substring {
            #[cfg(feature = "unicode_expressions")]
            {
                self.sql_substring_to_expr(first, second, third, schema, planner_context)?
            }
            #[cfg(not(feature = "unicode_expressions"))]
            {
                let _ = (first, second, third);
                return internal_err!(
                    "statement substring requires compilation with feature flag: unicode_expressions."
                );
            }
        } else {
            self.sql_trim_to_expr(first, None, second, schema, planner_context)?
        };
        Ok(Ok(expr))
    }

    pub(super) fn function_args_to_logical_expr(
        &self,
        function_args: FunctionArgs<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let distinct_arg = function_args.distinct;
        let FunctionArgs {
            name: object_name,
            args,
            order_by,
            over,
            filter,
            null_treatment,
            distinct,
            within_group,
            function_without_parentheses,
        } = function_args;

        if over.is_some() && !within_group.is_empty() {
            return plan_err!(
                "OVER and WITHIN GROUP clause cannot be used together. \
                OVER is for window functions, whereas WITHIN GROUP is for ordered set aggregate functions"
            );
        }

        if !order_by.is_empty() && !within_group.is_empty() {
            return plan_err!(
                "ORDER BY and WITHIN GROUP clauses cannot be used together in the same aggregate function"
            );
        }

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = over.is_some();
        let sql_parser_span = object_name.parts[0].span;
        let name = if object_name.parts.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            object_name.to_string()
        } else {
            crate::utils::normalize_ident(object_name.parts[0].clone())
        };

        // handle make_map and map functions
        // make_map always uses plan_make_map: make_map(k1, v1, k2, v2, ...)
        // map has 2 syntaxes:
        //     1. map([keys], [values]) - two arrays that get zipped
        //     2. map(k1, v1, k2, v2, ...) - variadic pairs (uses plan_make_map)
        let use_plan_make_map = match name.as_str() {
            "make_map" => true,
            "map" => {
                // for map, check if this is the first syntax variant (two-array)
                let args =
                    self.function_args_to_expr(args.clone(), schema, planner_context)?;

                let is_two_array_syntax = args.len() == 2
                    && args.iter().all(|arg| {
                        matches!(
                            arg.get_type(schema),
                            Ok(DataType::List(_))
                                | Ok(DataType::LargeList(_))
                                | Ok(DataType::FixedSizeList(_, _))
                        )
                    });

                // map function with variadic syntax requires non-empty list of arguments
                if !is_two_array_syntax && args.is_empty() {
                    return plan_err!(
                        "Function 'map' expected at least one argument but received 0"
                    );
                }

                !is_two_array_syntax
            }
            _ => false,
        };

        if use_plan_make_map {
            let mut fn_args =
                self.function_args_to_expr(args.clone(), schema, planner_context)?;

            for planner in self.context_provider.get_expr_planners().iter() {
                match planner.plan_make_map(fn_args)? {
                    PlannerResult::Planned(expr) => return Ok(expr),
                    PlannerResult::Original(args) => fn_args = args,
                }
            }
        }
        // User-defined function (UDF) should have precedence
        if let Some(fm) = self.context_provider.get_function_meta(&name) {
            let (args, arg_names) =
                self.function_args_to_expr_with_names(args, schema, planner_context)?;

            let resolved_args = if arg_names.iter().any(|name| name.is_some()) {
                if let Some(param_names) = &fm.signature().parameter_names {
                    datafusion_expr::arguments::resolve_function_arguments(
                        param_names,
                        args,
                        arg_names,
                    )?
                } else {
                    return plan_err!(
                        "Function '{}' does not support named arguments",
                        fm.name()
                    );
                }
            } else {
                args
            };

            // After resolution, all arguments are positional
            let mut scalar_expr = RawScalarExpr {
                func: fm,
                args: resolved_args,
            };

            for planner in self.context_provider.get_expr_planners().iter() {
                match planner.plan_scalar(scalar_expr, schema)? {
                    PlannerResult::Planned(expr) => return Ok(expr),
                    PlannerResult::Original(expr) => scalar_expr = expr,
                }
            }

            let RawScalarExpr { func, args } = scalar_expr;
            let inner = ScalarFunction::new_udf(func, args);

            if name.eq_ignore_ascii_case(inner.name()) {
                return Ok(Expr::ScalarFunction(inner));
            } else {
                // If the function is called by an alias, a verbose string representation is created
                // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                // to ensure the output column name matches the user's query.
                let arg_names = inner
                    .args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let verbose_alias = format!("{name}({arg_names})");

                return Ok(Expr::ScalarFunction(inner).alias(verbose_alias));
            }
        }

        if let Some(fm) = self.context_provider.get_higher_order_meta(&name) {
            // plan non-lambda arguments first so we can get theirs datatype and call
            // HigherOrderUDFImpl::lambda_parameters to then plan the lambda arguments with
            // resolved lambda variables
            enum ExprOrLambda<'s> {
                Expr(Expr),
                Lambda(SqlLambda<'s>),
            }

            let partially_planned = args
                .into_iter()
                .map(|a| match SqlLambda::from_arg(a) {
                    Ok(lambda) => {
                        if !all_unique(&lambda.params) {
                            let params = lambda
                                .params
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<_>>()
                                .join(", ");
                            return plan_err!(
                                "lambda parameters names must be unique, got ({params})"
                            );
                        }

                        Ok(ExprOrLambda::Lambda(lambda))
                    }
                    Err(a) => Ok(ExprOrLambda::Expr(self.sql_fn_arg_to_logical_expr(
                        a,
                        schema,
                        planner_context,
                    )?)),
                })
                .collect::<Result<Vec<_>>>()?;

            let current_fields = partially_planned
                .iter()
                .map(|e| match e {
                    ExprOrLambda::Expr(expr) => {
                        Ok(ValueOrLambda::Value(expr.to_field(schema)?.1))
                    }
                    ExprOrLambda::Lambda(_lambda_function) => {
                        Ok(ValueOrLambda::Lambda(()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            // coerce fields because coercion may alter the lambda parameters
            let mut fields =
                value_fields_with_higher_order_udf(&current_fields, fm.as_ref())?
                    .into_iter()
                    .map(|arg| match arg {
                        ValueOrLambda::Value(value) => ValueOrLambda::Value(value),
                        ValueOrLambda::Lambda(_lambda) => ValueOrLambda::Lambda(None),
                    })
                    .collect::<Vec<_>>();

            let lambda_count = partially_planned
                .iter()
                .filter(|a| matches!(a, ExprOrLambda::Lambda(_)))
                .count();

            let mut step = 0;

            let args = loop {
                match fm.lambda_parameters(step, &fields)? {
                    LambdaParametersProgress::Partial(params) => {
                        let mut params = params.into_iter();

                        if params.len() != lambda_count {
                            return plan_err!(
                                "{} lambda_parameters returned {} lambdas but {lambda_count} expected",
                                fm.name(),
                                params.len()
                            );
                        }

                        for (arg, field) in
                            std::iter::zip(&partially_planned, &mut fields)
                        {
                            match (arg, field) {
                                (
                                    ExprOrLambda::Lambda(lambda),
                                    ValueOrLambda::Lambda(field),
                                ) => {
                                    let params = params.next().ok_or_else(|| {
                                        internal_datafusion_err!(
                                            "params len should have been checked above"
                                        )
                                    })?;
                                    if let Some(params) = params {
                                        let lambda = self.sql_lambda_to_logical_lambda(
                                            planner_context,
                                            schema,
                                            lambda.clone(),
                                            params,
                                        )?;

                                        *field = Some(lambda.body.to_field(schema)?.1);
                                    }
                                }
                                (ExprOrLambda::Expr(_), ValueOrLambda::Value(_)) => {} // nothing to do
                                (ExprOrLambda::Expr(_), ValueOrLambda::Lambda(_)) => {
                                    return internal_err!(
                                        "value_fields_with_higher_order_udf returned a value for a lambda argument"
                                    );
                                }
                                (ExprOrLambda::Lambda(_), ValueOrLambda::Value(_)) => {
                                    return internal_err!(
                                        "value_fields_with_higher_order_udf returned a lambda for a value argument"
                                    );
                                }
                            }
                        }
                    }
                    LambdaParametersProgress::Complete(params) => {
                        let mut params = params.into_iter();

                        if params.len() != lambda_count {
                            return plan_err!(
                                "{} lambda_parameters returned {} lambdas but {lambda_count} expected",
                                fm.name(),
                                params.len()
                            );
                        }

                        break partially_planned
                            .into_iter()
                            .map(|arg| match arg {
                                ExprOrLambda::Expr(expr) => Ok(expr),
                                ExprOrLambda::Lambda(lambda) => {
                                    let params = params.next().ok_or_else(|| {
                                        internal_datafusion_err!(
                                            "params len should have been checked above"
                                        )
                                    })?;

                                    Ok(Expr::Lambda(self.sql_lambda_to_logical_lambda(
                                        planner_context,
                                        schema,
                                        lambda,
                                        params,
                                    )?))
                                }
                            })
                            .collect::<Result<Vec<_>>>()?;
                    }
                }

                let limit = fm.signature().lambda_parameters_max_iterations;

                step += 1;

                if step > limit {
                    return plan_err!(
                        "{} lambda_parameters called {limit} times without completion",
                        fm.name()
                    );
                }
            };

            let inner = HigherOrderFunction::new(fm, args);

            if name.eq_ignore_ascii_case(inner.name()) {
                return Ok(Expr::HigherOrderFunction(inner));
            } else {
                // If the function is called by an alias, a verbose string representation is created
                // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                // to ensure the output column name matches the user's query.
                let arg_names = inner
                    .args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let verbose_alias = format!("{name}({arg_names})");

                return Ok(Expr::HigherOrderFunction(inner).alias(verbose_alias));
            }
        }

        // Build Unnest expression.
        //
        // `unnest(col)` drops `NULL` and empty input lists (default SQL
        // semantics, matching DuckDB/PostgreSQL). `unnest_outer(col)` sets
        // `outer = true` so the downstream planner picks
        // `NullHandling::PreserveAndExpandEmpty`, which preserves `NULL`
        // and empty input lists as a single `NULL` output row.
        if name.eq("unnest") || name.eq("unnest_outer") {
            let outer = name.eq("unnest_outer");
            let mut exprs = self.function_args_to_expr(args, schema, planner_context)?;
            if exprs.len() != 1 {
                return plan_err!("{name}() requires exactly one argument");
            }
            let expr = exprs.swap_remove(0);
            Self::check_unnest_arg(&expr, schema)?;
            return Ok(Expr::Unnest(Unnest {
                expr: Box::new(expr),
                outer,
            }));
        }

        if !order_by.is_empty() && is_function_window {
            return plan_err!(
                "Aggregate ORDER BY is not implemented for window functions"
            );
        }
        // Then, window function
        if let Some(WindowOver::Spec(window)) = over {
            let partition_by = window
                .partition_by
                .into_iter()
                // Ignore window spec PARTITION BY for scalar values
                // as they do not change and thus do not generate new partitions
                .filter(|e| {
                    !matches!(e.kind, ExprKind::Literal(_) | ExprKind::Placeholder(_))
                })
                .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                .collect::<Result<Vec<_>>>()?;
            let mut order_by = self.order_by_to_sort_expr(
                window.order_by,
                schema,
                planner_context,
                // Numeric literals in window function ORDER BY are treated as constants
                false,
                None,
            )?;

            let func_deps = schema.functional_dependencies();
            // Find whether ties are possible in the given ordering. NULLs sort
            // together and are peers, so a nullable UNIQUE column, which
            // permits multiple NULL rows, does not rule out ties.
            let is_ordering_strict = order_by.iter().find_map(|orderby_expr| {
                if let Expr::Column(col) = &orderby_expr.expr {
                    let idx = schema.index_of_column(col).ok()?;
                    return if func_deps.iter().any(|dep| {
                        dep.source_indices == [idx]
                            && dep.mode == Dependency::Single
                            && dep.is_valid_across_nulls(schema)
                    }) {
                        Some(true)
                    } else {
                        Some(false)
                    };
                }
                Some(false)
            });

            let window_frame = window
                .frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame: WindowFrame = window_frame.clone().try_into()?;
                    window_frame
                        .regularize_order_bys(&mut order_by)
                        .map(|_| window_frame)
                })
                .transpose()?;

            let window_frame = if let Some(window_frame) = window_frame {
                window_frame
            } else if let Some(is_ordering_strict) = is_ordering_strict {
                WindowFrame::new(Some(is_ordering_strict))
            } else {
                WindowFrame::new((!order_by.is_empty()).then_some(false))
            };

            if let Ok(fun) = self.find_window_func(&name) {
                let (args, arg_names) =
                    self.function_args_to_expr_with_names(args, schema, planner_context)?;

                let resolved_args = if arg_names.iter().any(|name| name.is_some()) {
                    let signature = match &fun {
                        WindowFunctionDefinition::AggregateUDF(udaf) => udaf.signature(),
                        WindowFunctionDefinition::WindowUDF(udwf) => udwf.signature(),
                    };

                    if let Some(param_names) = &signature.parameter_names {
                        datafusion_expr::arguments::resolve_function_arguments(
                            param_names,
                            args,
                            arg_names,
                        )?
                    } else {
                        return plan_err!(
                            "Window function '{}' does not support named arguments",
                            name
                        );
                    }
                } else {
                    args
                };

                // Plan FILTER clause if present
                let filter = filter
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .transpose()?
                    .map(Box::new);

                let mut window_expr = RawWindowExpr {
                    func_def: fun,
                    args: resolved_args,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct: distinct_arg,
                };

                for planner in self.context_provider.get_expr_planners().iter() {
                    match planner.plan_window(window_expr)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => window_expr = expr,
                    }
                }

                let RawWindowExpr {
                    func_def,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct,
                } = window_expr;

                let inner = WindowFunction {
                    fun: func_def,
                    params: expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        filter,
                        null_treatment,
                        distinct,
                    },
                };

                if name.eq_ignore_ascii_case(inner.fun.name()) {
                    return Ok(Expr::WindowFunction(Box::new(inner)));
                } else {
                    // If the function is called by an alias, a verbose string representation is created
                    // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                    // to ensure the output column name matches the user's query.
                    let arg_names = inner
                        .params
                        .args
                        .iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let verbose_alias = format!("{name}({arg_names})");

                    return Ok(Expr::WindowFunction(Box::new(inner)).alias(verbose_alias));
                }
            }
        } else {
            // User defined aggregate functions (UDAF) have precedence in case it has the same name as a scalar built-in function
            if let Some(fm) = self.context_provider.get_aggregate_meta(&name) {
                if null_treatment.is_some() && !fm.supports_null_handling_clause() {
                    return plan_err!(
                        "[IGNORE | RESPECT] NULLS are not permitted for {}",
                        fm.name()
                    );
                }

                let (mut args, mut arg_names) =
                    self.function_args_to_expr_with_names(args, schema, planner_context)?;

                // UDAFs must opt-in via `supports_within_group_clause()` to
                // accept a WITHIN GROUP clause.
                let supports_within_group = fm.supports_within_group_clause();

                let mut within_group = within_group;
                let mut order_by = order_by;

                let is_inline_order_by = supports_within_group
                    && within_group.is_empty()
                    && !order_by.is_empty();

                if is_inline_order_by {
                    // Inline ORDER BY syntax:
                    // quantile_cont(value, percentile ORDER BY value)
                    within_group = order_by;
                    order_by = vec![];
                }

                if !supports_within_group && !within_group.is_empty() {
                    return plan_err!(
                        "WITHIN GROUP is only supported for ordered-set aggregate functions"
                    );
                }

                let order_by: Vec<SortExpr> = if supports_within_group {
                    if !within_group.is_empty() {
                        // WITHIN GROUP syntax
                        let sorts = self.order_by_to_sort_expr(
                            within_group,
                            schema,
                            planner_context,
                            false,
                            None,
                        )?;

                        if sorts.len() != 1 {
                            return plan_err!(
                                "Only a single ordering expression is permitted in WITHIN GROUP clause"
                            );
                        }

                        // Remove the ordered value only when it is explicitly repeated.
                        // This ensures these are equivalent:
                        //   approx_percentile_cont(c3, 0.95, 200 ORDER BY c3)
                        //   approx_percentile_cont(0.95, 200 ORDER BY c3)
                        if is_inline_order_by && args.first() == Some(&sorts[0].expr) {
                            args.remove(0);
                            arg_names.remove(0);
                        }

                        // Prepend ordered value expression to args
                        let value_expr = sorts[0].expr.clone();
                        arg_names = std::iter::once(None).chain(arg_names).collect();
                        args = std::iter::once(value_expr).chain(args).collect();

                        sorts
                    } else {
                        vec![]
                    }
                } else {
                    // Normal aggregate behavior
                    self.order_by_to_sort_expr(
                        order_by,
                        schema,
                        planner_context,
                        true,
                        None,
                    )?
                };

                let filter: Option<Box<Expr>> = filter
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .transpose()?
                    .map(Box::new);

                let resolved_args = if arg_names.iter().any(|name| name.is_some()) {
                    if let Some(param_names) = &fm.signature().parameter_names {
                        datafusion_expr::arguments::resolve_function_arguments(
                            param_names,
                            args,
                            arg_names,
                        )?
                    } else {
                        return plan_err!(
                            "Aggregate function '{}' does not support named arguments",
                            fm.name()
                        );
                    }
                } else {
                    args
                };

                let mut aggregate_expr = RawAggregateExpr {
                    func: fm,
                    args: resolved_args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                };
                for planner in self.context_provider.get_expr_planners().iter() {
                    match planner.plan_aggregate(aggregate_expr)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => aggregate_expr = expr,
                    }
                }

                let RawAggregateExpr {
                    func,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                } = aggregate_expr;

                let inner = expr::AggregateFunction::new_udf(
                    func,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                );

                if name.eq_ignore_ascii_case(inner.func.name()) {
                    return Ok(Expr::AggregateFunction(inner));
                } else {
                    // If the function is called by an alias, a verbose string representation is created
                    // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                    // to ensure the output column name matches the user's query.
                    let arg_names = inner
                        .params
                        .args
                        .iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let verbose_alias = format!("{name}({arg_names})");

                    return Ok(Expr::AggregateFunction(inner).alias(verbose_alias));
                }
            }
        }

        // workaround for https://github.com/apache/datafusion-sqlparser-rs/issues/1909
        if function_without_parentheses {
            let ids = object_name.parts;
            if ids.len() == 1 {
                return self.sql_identifier_to_expr(
                    ids.into_iter().next().unwrap(),
                    schema,
                    planner_context,
                );
            } else {
                return self.sql_compound_identifier_to_expr(
                    ids,
                    schema,
                    planner_context,
                );
            }
        }

        // Could not find the relevant function, so return an error
        if let Some(suggested_func_name) =
            suggest_valid_function(&name, is_function_window, self.context_provider)
        {
            let span = Span::try_from_sqlparser_span(sql_parser_span);
            let mut diagnostic =
                Diagnostic::new_error(format!("Invalid function '{name}'"), span);
            diagnostic
                .add_note(format!("Possible function '{suggested_func_name}'"), None);
            plan_err!("Invalid function '{name}'.\nDid you mean '{suggested_func_name}'?"; diagnostic=diagnostic)
        } else {
            internal_err!("No functions registered with this context.")
        }
    }

    fn sql_lambda_to_logical_lambda(
        &self,
        planner_context: &mut PlannerContext,
        schema: &DFSchema,
        lambda: SqlLambda<'_>,
        lambda_params: Vec<FieldRef>,
    ) -> Result<Lambda> {
        if lambda.params.len() > lambda_params.len() {
            return plan_err!(
                "lambda defined {} params but UDF support only {}",
                lambda.params.len(),
                lambda_params.len()
            );
        }

        let params = lambda
            .params
            .iter()
            .map(|p| crate::utils::normalize_ident(p.clone()))
            .collect();

        let lambda_parameters = std::iter::zip(lambda_params, &params)
            .map(|(f, n): (FieldRef, &String)| f.renamed(n.as_str()));

        let mut planner_context = planner_context
            .clone()
            .with_lambda_parameters(lambda_parameters);

        Ok(Lambda {
            params,
            body: Box::new(self.sql_expr_to_logical_expr(
                lambda.body,
                schema,
                &mut planner_context,
            )?),
        })
    }

    pub(super) fn sql_fn_name_to_expr(
        &self,
        expr: SQLExpr<'_>,
        fn_name: &str,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = self
            .context_provider
            .get_function_meta(fn_name)
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected '{fn_name}' function")
            })?;
        let args = vec![self.sql_expr_to_logical_expr(expr, schema, planner_context)?];
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }

    pub(super) fn find_window_func(
        &self,
        name: &str,
    ) -> Result<WindowFunctionDefinition> {
        // Check udaf first
        let udaf = self.context_provider.get_aggregate_meta(name);
        // Use the builtin window function instead of the user-defined aggregate function
        if udaf.as_ref().is_some_and(|udaf| {
            udaf.name() != "first_value"
                && udaf.name() != "last_value"
                && udaf.name() != "nth_value"
        }) {
            Ok(WindowFunctionDefinition::AggregateUDF(udaf.unwrap()))
        } else {
            self.context_provider
                .get_window_meta(name)
                .map(WindowFunctionDefinition::WindowUDF)
                .ok_or_else(|| {
                    plan_datafusion_err!("There is no window function named {name}")
                })
        }
    }

    fn sql_fn_arg_to_logical_expr(
        &self,
        sql: FunctionArg<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let (expr, _) =
            self.sql_fn_arg_to_logical_expr_with_name(sql, schema, planner_context)?;
        Ok(expr)
    }

    fn sql_fn_arg_to_logical_expr_with_name(
        &self,
        sql: FunctionArg<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Option<ArgumentName>)> {
        match sql {
            FunctionArg::Named { name, value }
            | FunctionArg::NamedWith { name, value, .. } => {
                let expr =
                    self.sql_expr_to_logical_expr(value, schema, planner_context)?;
                let arg_name = ArgumentName {
                    value: name.value().into_owned(),
                    is_quoted: name.quote.is_some(),
                };
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::Expr(arg) => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                Ok((expr, None))
            }
            FunctionArg::Wildcard => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                Ok((expr, None))
            }
            FunctionArg::QualifiedWildcard(object_name) => {
                let qualifier = self.object_name_to_table_reference(object_name)?;
                // Sanity check on qualifier with schema
                let qualified_indices = schema.fields_indices_with_qualified(&qualifier);
                if qualified_indices.is_empty() {
                    return plan_err!("Invalid qualifier {qualifier}");
                }

                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: qualifier.into(),
                    options: Box::new(WildcardOptions::default()),
                };
                Ok((expr, None))
            }
        }
    }

    pub(super) fn function_args_to_expr(
        &self,
        args: Vec<FunctionArg<'_>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| self.sql_fn_arg_to_logical_expr(a, schema, planner_context))
            .collect::<Result<Vec<Expr>>>()
    }

    pub(super) fn function_args_to_expr_with_names(
        &self,
        args: Vec<FunctionArg<'_>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Vec<Expr>, Vec<Option<ArgumentName>>)> {
        let results: Result<Vec<(Expr, Option<ArgumentName>)>> = args
            .into_iter()
            .map(|a| {
                self.sql_fn_arg_to_logical_expr_with_name(a, schema, planner_context)
            })
            .collect();

        let pairs = results?;
        let (exprs, names): (Vec<Expr>, Vec<Option<ArgumentName>>) =
            pairs.into_iter().unzip();
        Ok((exprs, names))
    }

    #[expect(dead_code)]
    fn extract_and_prepend_within_group_args(
        &self,
        within_group: Vec<OrderByItem<'_>>,
        mut args: Vec<Expr>,
        mut arg_names: Vec<Option<ArgumentName>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<WithinGroupExtraction> {
        let within_group = self.order_by_to_sort_expr(
            within_group,
            schema,
            planner_context,
            false,
            None,
        )?;

        if !within_group.is_empty() {
            let within_group_count = within_group.len();
            arg_names = std::iter::repeat_n(None, within_group_count)
                .chain(arg_names)
                .collect();

            args = within_group
                .iter()
                .map(|sort| sort.expr.clone())
                .chain(args)
                .collect::<Vec<_>>();
        }

        Ok((within_group, args, arg_names))
    }

    pub(crate) fn check_unnest_arg(arg: &Expr, schema: &DFSchema) -> Result<()> {
        // Check argument type, array types are supported
        match arg.get_type(schema)? {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_) => Ok(()),
            DataType::Null => {
                not_impl_err!("unnest() does not support null yet")
            }
            _ => {
                plan_err!("unnest() can only be applied to array, struct and null")
            }
        }
    }
}

/// After normalization with [normalize_ident], check whether all params are unique
///
/// [normalize_ident]: crate::utils::normalize_ident
fn all_unique(params: &[Ident<'_>]) -> bool {
    match params.len() {
        0 | 1 => true,
        2 => {
            crate::utils::normalize_ident(params[0].clone())
                != crate::utils::normalize_ident(params[1].clone())
        }
        _ => {
            let mut set = HashSet::with_capacity(params.len());

            params
                .iter()
                .map(|p| crate::utils::normalize_ident(p.clone()))
                .all(|p| set.insert(p))
        }
    }
}
