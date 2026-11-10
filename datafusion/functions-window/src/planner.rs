// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! SQL planning extensions like [`WindowFunctionPlanner`]

use datafusion_common::Result;
use datafusion_expr::{
    Expr,
    expr::{WindowFunction, WindowFunctionParams},
    expr_rewriter::NamePreserver,
    planner::{ExprPlanner, PlannerResult, RawWindowExpr},
    utils::COUNT_STAR_EXPANSION,
};

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
            filter,
            null_treatment,
            distinct,
        } = raw_expr;

        let origin_expr = Expr::from(WindowFunction {
            fun: func_def,
            params: WindowFunctionParams {
                args,
                partition_by,
                order_by,
                window_frame,
                filter,
                null_treatment,
                distinct,
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
                    distinct,
                    filter,
                },
        } = *window_fun;
        let raw_expr = RawWindowExpr {
            func_def: fun,
            args,
            partition_by,
            order_by,
            window_frame,
            filter,
            null_treatment,
            distinct,
        };

        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        if raw_expr.func_def.name() == "count"
            && (raw_expr.args.len() == 1
                && matches!(raw_expr.args[0], Expr::Wildcard { .. })
                || raw_expr.args.is_empty())
        {
            let RawWindowExpr {
                func_def,
                args: _,
                partition_by,
                order_by,
                window_frame,
                filter,
                null_treatment,
                distinct,
            } = raw_expr;

            let new_expr = Expr::from(WindowFunction {
                fun: func_def,
                params: WindowFunctionParams {
                    args: vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct,
                },
            });
            let new_expr = saved_name.restore(new_expr);

            return Ok(PlannerResult::Planned(new_expr));
        }

        Ok(PlannerResult::Original(raw_expr))
    }
}
