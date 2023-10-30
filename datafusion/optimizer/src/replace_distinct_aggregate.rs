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

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::utils::expand_wildcard;
use datafusion_expr::{
    aggregate_function::AggregateFunction as AggregateFunctionFunc, col,
    expr::AggregateFunction, LogicalPlanBuilder,
};
use datafusion_expr::{Aggregate, Expr, LogicalPlan};
use datafusion_expr::{Distinct, DistinctOn};
use ApplyOrder::BottomUp;

/// Optimizer that replaces logical [[Distinct]] with a logical [[Aggregate]]
///
/// ```text
/// SELECT DISTINCT a, b FROM tab
/// ```
///
/// Into
/// ```text
/// SELECT a, b FROM tab GROUP BY a, b
/// ```

/// Optimizer that replaces logical [[Distinct]] with a logical [[Aggregate]]
#[derive(Default)]
pub struct ReplaceDistinctWithAggregate {}

impl ReplaceDistinctWithAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReplaceDistinctWithAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let group_expr = expand_wildcard(input.schema(), input, None)?;
                let aggregate = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    input.clone(),
                    group_expr,
                    vec![],
                    input.schema().clone(), // input schema and aggregate schema are the same in this case
                )?);
                Ok(Some(aggregate))
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                select_expr,
                on_expr,
                sort_expr,
                input,
                ..
            })) => {
                let aggr_expr = select_expr
                    .iter()
                    .map(|e| {
                        Ok(Expr::AggregateFunction(AggregateFunction::new(
                            AggregateFunctionFunc::FirstValue,
                            vec![e.clone()],
                            false,
                            None,
                            sort_expr.clone(),
                        ))
                        .alias(e.display_name()?))
                    })
                    .collect::<Result<Vec<Expr>>>()?;

                let on_expr = on_expr
                    .iter()
                    .enumerate()
                    .map(|(i, e)| e.clone().alias(format!("_on_expr_{i}")))
                    .collect::<Vec<Expr>>();

                let plan = LogicalPlanBuilder::from(input.as_ref().clone())
                    .aggregate(on_expr.clone(), aggr_expr.to_vec())?
                    .build()?;

                let project_exprs = plan
                    .schema()
                    .fields()
                    .iter()
                    .skip(on_expr.len())
                    .map(|f| col(f.unqualified_column().name))
                    .collect::<Vec<Expr>>();

                let plan = LogicalPlanBuilder::from(plan)
                    .project(project_exprs)?
                    .build()?;

                let plan = if let Some(sort_expr) = sort_expr {
                    LogicalPlanBuilder::from(plan)
                        .sort(sort_expr[..on_expr.len()].to_vec())?
                        .build()?
                } else {
                    plan
                };

                Ok(Some(plan))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "replace_distinct_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
    use crate::test::{assert_optimized_plan_eq, test_table_scan};
    use datafusion_expr::{col, LogicalPlanBuilder};
    use std::sync::Arc;

    #[test]
    fn replace_distinct() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]\
                            \n  Projection: test.a, test.b\
                            \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }
}
