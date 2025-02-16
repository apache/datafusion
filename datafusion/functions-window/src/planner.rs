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
    expr::WindowFunction,
    lit,
    planner::{ExprPlanner, PlannerResult, RawWindowExpr},
    utils::COUNT_STAR_EXPANSION,
    Expr, ExprFunctionExt,
};

#[derive(Debug)]
pub struct WindowFunctionPlanner;

impl ExprPlanner for WindowFunctionPlanner {
    fn plan_window(&self, expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
        if expr.func_def.name() == "count"
            && (expr.args.len() == 1 && matches!(expr.args[0], Expr::Wildcard { .. })
                || expr.args.is_empty())
        {
            let RawWindowExpr {
                func_def,
                args: _,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            } = expr;
            return Ok(PlannerResult::Planned(
                Expr::WindowFunction(WindowFunction::new(
                    func_def,
                    vec![lit(COUNT_STAR_EXPANSION)],
                ))
                .partition_by(partition_by)
                .order_by(order_by)
                .window_frame(window_frame)
                .null_treatment(null_treatment)
                .build()?,
            ));
        }

        Ok(PlannerResult::Original(expr))
    }
}
