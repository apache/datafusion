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
    expr::AggregateFunction,
    planner::{ExprPlanner, PlannerResult, RawAggregateExpr},
    utils::COUNT_STAR_EXPANSION,
    Expr,
};

#[derive(Debug)]
pub struct AggregateFunctionPlanner;

impl ExprPlanner for AggregateFunctionPlanner {
    fn plan_aggregate(
        &self,
        expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        // handle count() and count(*) case
        // convert to count(1) as "count()"
        // or         count(1) as "count(*)"
        if expr.func.name() == "count"
            && (expr.args.len() == 1 && matches!(expr.args[0], Expr::Wildcard { .. })
                || expr.args.is_empty())
        {
            let (orig_relation, orig_name) = match expr.args.len() {
                0 => (None, "".into()),
                _ => expr.args[0].qualified_name(),
            };

            let RawAggregateExpr {
                func,
                args: _,
                distinct,
                filter,
                order_by,
                null_treatment,
            } = expr;
            return Ok(PlannerResult::Planned(Expr::AggregateFunction(
                AggregateFunction::new_udf(
                    func,
                    vec![Expr::Literal(COUNT_STAR_EXPANSION)
                        .alias_qualified(orig_relation, orig_name)],
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                ),
            )));
        }

        Ok(PlannerResult::Original(expr))
    }
}
