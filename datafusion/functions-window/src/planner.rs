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

use datafusion_common::Result;
use datafusion_expr::{
    expr::{WindowFunction, WindowFunctionParams},
    expr_rewriter::NamePreserver,
    planner::{ExprPlanner, PlannerResult, RawWindowExpr},
    utils::COUNT_STAR_EXPANSION,
    Expr, ExprFunctionExt,
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
                },
        } = *window_fun;
        let raw_expr = RawWindowExpr {
            func_def: fun,
            args,
            partition_by,
            order_by,
            window_frame,
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
                null_treatment,
                distinct,
            } = raw_expr;

            let mut new_expr_before_build = Expr::from(WindowFunction::new(
                func_def,
                vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
            ))
            .partition_by(partition_by)
            .order_by(order_by)
            .window_frame(window_frame)
            .null_treatment(null_treatment);

            if distinct {
                new_expr_before_build = new_expr_before_build.distinct();
            }

            let new_expr = new_expr_before_build.build()?;
            let new_expr = saved_name.restore(new_expr);

            return Ok(PlannerResult::Planned(new_expr));
        }

        Ok(PlannerResult::Original(raw_expr))
    }
}
