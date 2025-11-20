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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use arrow::datatypes::DataType;
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err, plan_err,
    DFSchema, Dependency, Diagnostic, Result, Span,
};
use datafusion_expr::{
    expr,
    expr::{NullTreatment, ScalarFunction, Unnest, WildcardOptions, WindowFunction},
    planner::{PlannerResult, RawAggregateExpr, RawWindowExpr},
    Expr, ExprSchemable, SortExpr, WindowFrame, WindowFunctionDefinition,
};
use sqlparser::ast::{
    DuplicateTreatment, Expr as SQLExpr, Function as SQLFunction, FunctionArg,
    FunctionArgExpr, FunctionArgumentClause, FunctionArgumentList, FunctionArguments,
    ObjectName, OrderByExpr, Spanned, WindowType,
};

/// Suggest a valid function based on an invalid input function name
///
/// Returns `None` if no valid matches are found. This happens when there are no
/// functions registered with the context.
pub fn suggest_valid_function(
    input_function_name: &str,
    is_window_func: bool,
    ctx: &dyn ContextProvider,
) -> Option<String> {
    let valid_funcs = if is_window_func {
        // All aggregate functions and builtin window functions
        let mut funcs = Vec::new();

        funcs.extend(ctx.udaf_names());
        funcs.extend(ctx.udwf_names());

        funcs
    } else {
        // All scalar functions and aggregate functions
        let mut funcs = Vec::new();

        funcs.extend(ctx.udf_names());
        funcs.extend(ctx.udaf_names());

        funcs
    };
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
struct FunctionArgs {
    /// Function name
    name: ObjectName,
    /// Argument expressions
    args: Vec<FunctionArg>,
    /// ORDER BY clause, if any
    order_by: Vec<OrderByExpr>,
    /// OVER clause, if any
    over: Option<WindowType>,
    /// FILTER clause, if any
    filter: Option<Box<SQLExpr>>,
    /// NULL treatment clause, if any
    null_treatment: Option<NullTreatment>,
    /// DISTINCT
    distinct: bool,
    /// WITHIN GROUP clause, if any
    within_group: Vec<OrderByExpr>,
    /// Was the function called without parenthesis, i.e. could this also be a column reference?
    function_without_parentheses: bool,
}

impl FunctionArgs {
    fn try_new(function: SQLFunction) -> Result<Self> {
        let SQLFunction {
            name,
            args,
            over,
            filter,
            mut null_treatment,
            within_group,
            ..
        } = function;

        // Handle no argument form (aka `current_time`  as opposed to `current_time()`)
        let FunctionArguments::List(args) = args else {
            return Ok(Self {
                name,
                args: vec![],
                order_by: vec![],
                over,
                filter,
                null_treatment: null_treatment.map(|v| v.into()),
                distinct: false,
                within_group,
                function_without_parentheses: matches!(args, FunctionArguments::None),
            });
        };

        let FunctionArgumentList {
            duplicate_treatment,
            args,
            clauses,
        } = args;

        let distinct = match duplicate_treatment {
            Some(DuplicateTreatment::Distinct) => true,
            Some(DuplicateTreatment::All) => false,
            None => false,
        };

        // Pull out argument handling
        let mut order_by = None;
        for clause in clauses {
            match clause {
                FunctionArgumentClause::IgnoreOrRespectNulls(nt) => {
                    if null_treatment.is_some() {
                        return not_impl_err!(
                            "Calling {name}: Duplicated null treatment clause"
                        );
                    }
                    null_treatment = Some(nt);
                }
                FunctionArgumentClause::OrderBy(oby) => {
                    if order_by.is_some() {
                        if !within_group.is_empty() {
                            return plan_err!("ORDER BY clause is only permitted in WITHIN GROUP clause when a WITHIN GROUP is used");
                        }
                        return not_impl_err!("Calling {name}: Duplicated ORDER BY clause in function arguments");
                    }
                    order_by = Some(oby);
                }
                FunctionArgumentClause::Limit(limit) => {
                    return not_impl_err!(
                        "Calling {name}: LIMIT not supported in function arguments: {limit}"
                    )
                }
                FunctionArgumentClause::OnOverflow(overflow) => {
                    return not_impl_err!(
                        "Calling {name}: ON OVERFLOW not supported in function arguments: {overflow}"
                    )
                }
                FunctionArgumentClause::Having(having) => {
                    return not_impl_err!(
                        "Calling {name}: HAVING not supported in function arguments: {having}"
                    )
                }
                FunctionArgumentClause::Separator(sep) => {
                    return not_impl_err!(
                        "Calling {name}: SEPARATOR not supported in function arguments: {sep}"
                    )
                }
                FunctionArgumentClause::JsonNullClause(jn) => {
                    return not_impl_err!(
                        "Calling {name}: JSON NULL clause not supported in function arguments: {jn}"
                    )
                }
                FunctionArgumentClause::JsonReturningClause(jr) => {
                    return not_impl_err!(
                        "Calling {name}: JSON RETURNING clause not supported in function arguments: {jr}"
                    )
                },
            }
        }

        if within_group.len() > 1 {
            return not_impl_err!(
                "Only a single ordering expression is permitted in a WITHIN GROUP clause"
            );
        }

        let order_by = order_by.unwrap_or_default();

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

// Helper type for extracting WITHIN GROUP ordering and prepended args
type WithinGroupExtraction = (Vec<SortExpr>, Vec<Expr>, Vec<Option<String>>);

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_function_to_expr(
        &self,
        function: SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let function_args = FunctionArgs::try_new(function)?;
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
            return plan_err!("OVER and WITHIN GROUP clause cannot be used together. \
                OVER is for window functions, whereas WITHIN GROUP is for ordered set aggregate functions");
        }

        if !order_by.is_empty() && !within_group.is_empty() {
            return plan_err!("ORDER BY and WITHIN GROUP clauses cannot be used together in the same aggregate function");
        }

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = over.is_some();
        let sql_parser_span = object_name.0[0].span();
        let name = if object_name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            object_name.to_string()
        } else {
            match object_name.0[0].as_ident() {
                Some(ident) => crate::utils::normalize_ident(ident.clone()),
                None => {
                    return plan_err!(
                        "Expected an identifier in function name, but found {:?}",
                        object_name.0[0]
                    )
                }
            }
        };

        if name.eq("make_map") {
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
            let inner = ScalarFunction::new_udf(fm, resolved_args);

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

        // Build Unnest expression
        if name.eq("unnest") {
            let mut exprs = self.function_args_to_expr(args, schema, planner_context)?;
            if exprs.len() != 1 {
                return plan_err!("unnest() requires exactly one argument");
            }
            let expr = exprs.swap_remove(0);
            Self::check_unnest_arg(&expr, schema)?;
            return Ok(Expr::Unnest(Unnest::new(expr)));
        }

        if !order_by.is_empty() && is_function_window {
            return plan_err!(
                "Aggregate ORDER BY is not implemented for window functions"
            );
        }
        // Then, window function
        if let Some(WindowType::WindowSpec(window)) = over {
            let partition_by = window
                .partition_by
                .into_iter()
                // Ignore window spec PARTITION BY for scalar values
                // as they do not change and thus do not generate new partitions
                .filter(|e| !matches!(e, sqlparser::ast::Expr::Value { .. },))
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
            // Find whether ties are possible in the given ordering
            let is_ordering_strict = order_by.iter().find_map(|orderby_expr| {
                if let Expr::Column(col) = &orderby_expr.expr {
                    let idx = schema.index_of_column(col).ok()?;
                    return if func_deps.iter().any(|dep| {
                        dep.source_indices == vec![idx] && dep.mode == Dependency::Single
                    }) {
                        Some(true)
                    } else {
                        Some(false)
                    };
                }
                Some(false)
            });

            let window_frame = window
                .window_frame
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
                    .map(|e| self.sql_expr_to_logical_expr(*e, schema, planner_context))
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
                    distinct: function_args.distinct,
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

                if !within_group.is_empty() && !supports_within_group {
                    return plan_err!(
                        "WITHIN GROUP is only supported for ordered-set aggregate functions"
                    );
                }

                // If the UDAF supports WITHIN GROUP, convert the ordering into
                // sort expressions and prepend them as unnamed function args.
                let order_by = if supports_within_group {
                    let (within_group_sorts, new_args, new_arg_names) = self
                        .extract_and_prepend_within_group_args(
                            within_group,
                            args,
                            arg_names,
                            schema,
                            planner_context,
                        )?;
                    args = new_args;
                    arg_names = new_arg_names;
                    within_group_sorts
                } else {
                    let order_by = if !order_by.is_empty() {
                        order_by
                    } else {
                        within_group
                    };
                    self.order_by_to_sort_expr(
                        order_by,
                        schema,
                        planner_context,
                        true,
                        None,
                    )?
                };

                let filter: Option<Box<Expr>> = filter
                    .map(|e| self.sql_expr_to_logical_expr(*e, schema, planner_context))
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
            let maybe_ids = object_name
                .0
                .iter()
                .map(|part| part.as_ident().cloned().ok_or(()))
                .collect::<Result<Vec<_>, ()>>();
            if let Ok(ids) = maybe_ids {
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

    pub(super) fn sql_fn_name_to_expr(
        &self,
        expr: SQLExpr,
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
        sql: FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let (expr, _) =
            self.sql_fn_arg_to_logical_expr_with_name(sql, schema, planner_context)?;
        Ok(expr)
    }

    fn sql_fn_arg_to_logical_expr_with_name(
        &self,
        sql: FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Option<String>)> {
        match sql {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(arg),
                operator: _,
            } => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                let arg_name = crate::utils::normalize_ident(name);
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Wildcard,
                operator: _,
            } => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                let arg_name = crate::utils::normalize_ident(name);
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                Ok((expr, None))
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                Ok((expr, None))
            }
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(object_name)) => {
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
            // PostgreSQL dialect uses ExprNamed variant with expression for name
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(name),
                arg: FunctionArgExpr::Expr(arg),
                operator: _,
            } => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                let arg_name = crate::utils::normalize_ident(name);
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(name),
                arg: FunctionArgExpr::Wildcard,
                operator: _,
            } => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                let arg_name = crate::utils::normalize_ident(name);
                Ok((expr, Some(arg_name)))
            }
            _ => not_impl_err!("Unsupported qualified wildcard argument: {sql:?}"),
        }
    }

    pub(super) fn function_args_to_expr(
        &self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| self.sql_fn_arg_to_logical_expr(a, schema, planner_context))
            .collect::<Result<Vec<Expr>>>()
    }

    pub(super) fn function_args_to_expr_with_names(
        &self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Vec<Expr>, Vec<Option<String>>)> {
        let results: Result<Vec<(Expr, Option<String>)>> = args
            .into_iter()
            .map(|a| {
                self.sql_fn_arg_to_logical_expr_with_name(a, schema, planner_context)
            })
            .collect();

        let pairs = results?;
        let (exprs, names): (Vec<Expr>, Vec<Option<String>>) = pairs.into_iter().unzip();
        Ok((exprs, names))
    }

    fn extract_and_prepend_within_group_args(
        &self,
        within_group: Vec<OrderByExpr>,
        mut args: Vec<Expr>,
        mut arg_names: Vec<Option<String>>,
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
