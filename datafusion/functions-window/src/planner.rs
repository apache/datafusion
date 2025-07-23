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

//! SQL planning extensions like [`WindowFunctionPlanner`]

use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{
    expr::{WindowFunction, WindowFunctionParams},
    expr_rewriter::NamePreserver,
    planner::{ExprPlanner, PlannerResult, RawWindowExpr},
    utils::COUNT_STAR_EXPANSION,
    Case, Expr, ExprFunctionExt,
};

use crate::match_recognize::{mr_first, mr_last, mr_next, mr_prev};

use datafusion_common::Column;

#[derive(Debug)]
pub struct WindowFunctionPlanner;

impl ExprPlanner for WindowFunctionPlanner {
    fn plan_window(
        &self,
        raw_expr: RawWindowExpr,
    ) -> Result<PlannerResult<RawWindowExpr>> {
        let RawWindowExpr {
            func_def,
            args,
            partition_by,
            order_by,
            window_frame,
            null_treatment,
        } = raw_expr;

        let origin_expr = Expr::from(WindowFunction {
            fun: func_def,
            params: WindowFunctionParams {
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            },
        });

        let saved_name = NamePreserver::new_for_projection().save(&origin_expr);

        let Expr::WindowFunction(window_fun) = origin_expr else {
            unreachable!("")
        };
        let WindowFunction {
            fun,
            params:
                WindowFunctionParams {
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
        } = *window_fun;

        if fun.name() == "count" {
            // Determine if we should rewrite and what argument to use
            #[expect(deprecated)]
            let rewrite_arg: Option<Expr> = match args.as_slice() {
                // COUNT(), COUNT(*), or COUNT(qualifier.*) (non symbol specific)
                [] | [Expr::Wildcard { symbol: None, .. }] => {
                    Some(Expr::Literal(COUNT_STAR_EXPANSION, None))
                }
                _ => None, // leave COUNT(symbol.*) untouched here
            };

            if let Some(arg_expr) = rewrite_arg {
                let new_expr =
                    Expr::from(WindowFunction::new(fun.clone(), vec![arg_expr]))
                        .partition_by(partition_by.clone())
                        .order_by(order_by.clone())
                        .window_frame(window_frame.clone())
                        .null_treatment(null_treatment)
                        .build()?;

                let new_expr = saved_name.restore(new_expr);

                return Ok(PlannerResult::Planned(new_expr));
            }
        }

        Ok(PlannerResult::Original(RawWindowExpr {
            func_def: fun,
            args,
            partition_by,
            order_by,
            window_frame,
            null_treatment,
        }))
    }

    fn plan_match_recognize(&self, expr: Expr) -> Result<PlannerResult<Expr>> {
        // Recursively rewrite expression, wrapping bare symbol columns in LAST()
        fn rewrite(e: Expr) -> Result<datafusion_common::tree_node::Transformed<Expr>> {
            use datafusion_common::tree_node::TreeNode;

            match &e {
                Expr::Column(col) if col.symbol.is_some() => {
                    let new_expr: Expr = mr_last(Expr::Column(col.clone()));
                    return Ok(datafusion_common::tree_node::Transformed::yes(new_expr));
                }

                Expr::WindowFunction(wf) => {
                    let fname = wf.fun.name().to_ascii_lowercase();

                    match fname.as_str() {
                        "lag" | "lead" => {
                            // Collect optional offset and default if they are simple literals
                            let offset_val =
                                wf.params.args.get(1).and_then(|ex| match ex {
                                    Expr::Literal(ScalarValue::Int64(Some(v)), _) => {
                                        Some(*v)
                                    }
                                    Expr::Literal(ScalarValue::UInt64(Some(v)), _) => {
                                        Some(*v as i64)
                                    }
                                    _ => None,
                                });

                            let default_val =
                                wf.params.args.get(2).and_then(|ex| match ex {
                                    Expr::Literal(sv, _) => Some(sv.clone()),
                                    _ => None,
                                });

                            if let Some(value_expr) = wf.params.args.get(0) {
                                let symbols =
                                    datafusion_expr::utils::find_symbol_predicates(
                                        value_expr,
                                    );
                                if symbols.len() >= 2 {
                                    return plan_err!(
                                        "Window function {fname} can reference at most one symbol predicate, found {}: {:?}",
                                        symbols.len(), symbols
                                    );
                                }
                                if !symbols.is_empty() {
                                    let new_expr = match fname.as_str() {
                                        "lag" => mr_prev(
                                            value_expr.clone(),
                                            offset_val,
                                            default_val,
                                        ),
                                        "lead" => mr_next(
                                            value_expr.clone(),
                                            offset_val,
                                            default_val,
                                        ),
                                        _ => unreachable!(),
                                    };

                                    return Ok(
                                        datafusion_common::tree_node::Transformed::yes(
                                            new_expr,
                                        ),
                                    );
                                }
                            }
                        }
                        "first_value" | "last_value" => {
                            // Check if first argument references symbol columns
                            if let Some(value_expr) = wf.params.args.get(0) {
                                let symbols =
                                    datafusion_expr::utils::find_symbol_predicates(
                                        value_expr,
                                    );
                                if symbols.len() >= 2 {
                                    return plan_err!(
                                        "Window function {fname} can reference at most one symbol predicate, found {}: {:?}",
                                        symbols.len(), symbols
                                    );
                                }
                                if !symbols.is_empty() {
                                    let new_expr = match fname.as_str() {
                                        "first_value" => mr_first(value_expr.clone()),
                                        "last_value" => mr_last(value_expr.clone()),
                                        _ => unreachable!(),
                                    };

                                    return Ok(
                                        datafusion_common::tree_node::Transformed::yes(
                                            new_expr,
                                        ),
                                    );
                                }
                            }
                        }
                        _ => {}
                    }

                    // Handle aggregate window functions (e.g., count/sum/avg) that operate on symbol predicates, including COUNT(symbol.*)
                    if let datafusion_expr::WindowFunctionDefinition::AggregateUDF(udaf) =
                        &wf.fun
                    {
                        // If the first argument is a wildcard and the function is not COUNT, return error
                        #[expect(deprecated)]
                        if let Some(Expr::Wildcard { .. }) = wf.params.args.get(0) {
                            if !udaf.name().eq_ignore_ascii_case("count") {
                                return plan_err!(
                                    "Wildcard argument is not supported for window function '{}' in MATCH_RECOGNIZE; only COUNT(symbol.*) or COUNT(*) is allowed",
                                    udaf.name()
                                );
                            }
                        }

                        // Special case: COUNT(symbol.*)
                        #[expect(deprecated)]
                        if udaf.name().eq_ignore_ascii_case("count") {
                            if let Some(Expr::Wildcard {
                                symbol: Some(symbol),
                                ..
                            }) = wf.params.args.get(0)
                            {
                                // Build CASE WHEN __mr_classifier = '<symbol>' THEN 1 ELSE NULL END
                                let classifier_col =
                                    Expr::Column(Column::from_name("__mr_classifier"));
                                let classifier_cond = classifier_col.eq(Expr::Literal(
                                    ScalarValue::Utf8(Some(symbol.to_uppercase())),
                                    None,
                                ));

                                let case_expr = Expr::Case(Case {
                                    expr: None,
                                    when_then_expr: vec![(
                                        Box::new(classifier_cond),
                                        Box::new(Expr::Literal(
                                            COUNT_STAR_EXPANSION,
                                            None,
                                        )),
                                    )],
                                    else_expr: Some(Box::new(Expr::Literal(
                                        ScalarValue::Null,
                                        None,
                                    ))),
                                });

                                let mut new_args = wf.params.args.clone();
                                new_args[0] = case_expr;

                                let new_wf = Expr::from(WindowFunction::new(
                                    wf.fun.clone(),
                                    new_args,
                                ))
                                .partition_by(wf.params.partition_by.clone())
                                .order_by(wf.params.order_by.clone())
                                .window_frame(wf.params.window_frame.clone())
                                .null_treatment(wf.params.null_treatment)
                                .build()?;

                                return Ok(
                                    datafusion_common::tree_node::Transformed::yes(
                                        new_wf,
                                    ),
                                );
                            }
                        }

                        // Only consider the first (value) argument of the aggregate function
                        if let Some(value_expr) = wf.params.args.get(0) {
                            // Identify any symbol predicates referenced by the argument
                            let symbols = datafusion_expr::utils::find_symbol_predicates(
                                value_expr,
                            );

                            if symbols.len() >= 2 {
                                return plan_err!(
                                    "Aggregate window function {} can reference at most one symbol predicate, found {}: {:?}",
                                    udaf.name(),
                                    symbols.len(),
                                    symbols
                                );
                            }

                            // Only rewrite if the expression references at least one symbol predicate.
                            if symbols.is_empty() {
                                // No symbol predicates → leave expression untouched
                                return Ok(
                                    datafusion_common::tree_node::Transformed::no(e),
                                );
                            }

                            // Build the classifier predicate:
                            //   • If a specific symbol is referenced:  __mr_classifier = '<symbol>'
                            //   • Otherwise (any symbol):            __mr_classifier != '(empty)'
                            let classifier_col =
                                Expr::Column(Column::from_name("__mr_classifier"));
                            let classifier_cond = if let Some(symbol) = symbols.first() {
                                classifier_col.eq(Expr::Literal(
                                    ScalarValue::Utf8(Some(symbol.to_uppercase())),
                                    None,
                                ))
                            } else {
                                classifier_col.not_eq(Expr::Literal(
                                    ScalarValue::Utf8(Some("(empty)".to_owned())),
                                    None,
                                ))
                            };

                            // Wrap the value in a CASE expression so the aggregate only sees
                            // rows that satisfy the classifier condition.
                            use datafusion_expr::expr::Case;
                            let case_expr = Expr::Case(Case {
                                expr: None,
                                when_then_expr: vec![(
                                    Box::new(classifier_cond),
                                    Box::new(value_expr.clone()),
                                )],
                                else_expr: Some(Box::new(Expr::Literal(
                                    ScalarValue::Null,
                                    None,
                                ))),
                            });

                            // Replace the first argument with our CASE expression
                            let mut new_args = wf.params.args.clone();
                            new_args[0] = case_expr;

                            // Re-assemble the WindowFunction with the updated arguments
                            let new_wf =
                                Expr::from(WindowFunction::new(wf.fun.clone(), new_args))
                                    .partition_by(wf.params.partition_by.clone())
                                    .order_by(wf.params.order_by.clone())
                                    .window_frame(wf.params.window_frame.clone())
                                    .null_treatment(wf.params.null_treatment)
                                    .build()?;

                            return Ok(datafusion_common::tree_node::Transformed::yes(
                                new_wf,
                            ));
                        }
                    }
                    // If we didn't match any of the above rewrite rules, leave the
                    // window function expression unchanged.

                    // Stop recursion inside window function: do not traverse inside
                    return Ok(datafusion_common::tree_node::Transformed::no(e));
                }

                _ => e.map_children(|child| rewrite(child)),
            }
        }

        let rewritten = rewrite(expr)?;
        if rewritten.transformed {
            Ok(PlannerResult::Planned(rewritten.data))
        } else {
            Ok(PlannerResult::Original(rewritten.data))
        }
    }
}
