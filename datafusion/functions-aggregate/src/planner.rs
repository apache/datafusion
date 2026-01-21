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

//! SQL planning extensions like [`AggregateFunctionPlanner`]

use datafusion_common::Result;
use datafusion_expr::{
    Expr,
    expr::{AggregateFunction, AggregateFunctionParams},
    expr_rewriter::NamePreserver,
    planner::{ExprPlanner, PlannerResult, RawAggregateExpr},
    utils::COUNT_STAR_EXPANSION,
};

#[derive(Debug)]
pub struct AggregateFunctionPlanner;

impl ExprPlanner for AggregateFunctionPlanner {
    fn plan_aggregate(
        &self,
        raw_expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        let RawAggregateExpr {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        } = raw_expr;

        let origin_expr = Expr::AggregateFunction(AggregateFunction {
            func,
            params: AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment,
            },
        });

        let saved_name = NamePreserver::new_for_projection().save(&origin_expr);

        let Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
        }) = origin_expr
        else {
            unreachable!("")
        };
        let raw_expr = RawAggregateExpr {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        };

        // handle count() and count(*) case
        // convert to count(1) as "count()"
        // or         count(1) as "count(*)"
        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        if raw_expr.func.name() == "count"
            && (raw_expr.args.len() == 1
                && matches!(raw_expr.args[0], Expr::Wildcard { .. })
                || raw_expr.args.is_empty())
        {
            let RawAggregateExpr {
                func,
                args: _,
                distinct,
                filter,
                order_by,
                null_treatment,
            } = raw_expr;

            let new_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                func,
                vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
                distinct,
                filter,
                order_by,
                null_treatment,
            ));

            let new_expr = saved_name.restore(new_expr);
            return Ok(PlannerResult::Planned(new_expr));
        }

        Ok(PlannerResult::Original(raw_expr))
    }
}
