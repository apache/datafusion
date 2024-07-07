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

use datafusion_expr::{
    expr, lit,
    planner::{PlannerResult, RawAggregateFunction, UserDefinedSQLPlanner},
    utils::COUNT_STAR_EXPANSION,
    Expr,
};

fn is_wildcard(expr: &Expr) -> bool {
    matches!(expr, Expr::Wildcard { qualifier: None })
}

pub struct AggregateFunctionPlanner;

impl AggregateFunctionPlanner {
    fn plan_count(
        &self,
        aggregate_function: RawAggregateFunction,
    ) -> datafusion_common::Result<PlannerResult<RawAggregateFunction>> {
        if aggregate_function.args.is_empty() {
            return Ok(PlannerResult::Planned(Expr::AggregateFunction(
                expr::AggregateFunction::new_udf(
                    aggregate_function.udf,
                    vec![lit(COUNT_STAR_EXPANSION).alias("count()")],
                    aggregate_function.distinct,
                    aggregate_function.filter,
                    aggregate_function.order_by,
                    aggregate_function.null_treatment,
                ),
            )));
        }

        if aggregate_function.udf.name() == "count"
            && aggregate_function.args.len() == 1
            && is_wildcard(&aggregate_function.args[0])
        {
            return Ok(PlannerResult::Planned(Expr::AggregateFunction(
                expr::AggregateFunction::new_udf(
                    aggregate_function.udf,
                    vec![lit(COUNT_STAR_EXPANSION).alias("*")],
                    aggregate_function.distinct,
                    aggregate_function.filter,
                    aggregate_function.order_by,
                    aggregate_function.null_treatment,
                ),
            )));
        }

        Ok(PlannerResult::Original(aggregate_function))
    }
}

impl UserDefinedSQLPlanner for AggregateFunctionPlanner {
    fn plan_aggregate_function(
        &self,
        aggregate_function: RawAggregateFunction,
    ) -> datafusion_common::Result<PlannerResult<RawAggregateFunction>> {
        if aggregate_function.udf.name() == "count" {
            return self.plan_count(aggregate_function);
        }

        Ok(PlannerResult::Original(aggregate_function))
    }
}
