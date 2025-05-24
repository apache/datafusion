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

use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Subquery};

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// (temporary) OPtimizer rule for rewriting current plan with
/// DependentJoin to jj
#[derive(Default, Debug)]
pub struct CreateDependentJoin {}

impl CreateDependentJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for CreateDependentJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "create_dependent_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(ref filter) = plan {
            match &filter.predicate {
                Expr::BinaryExpr(binary) => {
                    // Check if right hand side is a scalar subquery
                    if let Expr::ScalarSubquery(subquery) = binary.right.as_ref() {
                        let new_plan = build_dependent_join(
                            subquery,
                            filter.input.as_ref().clone(),
                            JoinType::Left,
                        )?;
                        return Ok(Transformed::yes(new_plan));
                    }
                    // Continue searching in children if no subquery found
                    return Ok(Transformed::no(plan));
                }
                _ => {
                    // TODO: add other type of subqueries.
                    return Ok(Transformed::no(plan));
                }
            }
        }

        // No Filter found, continue searching in children
        Ok(Transformed::no(plan))
    }
}

fn build_dependent_join(
    subquery: &Subquery,
    root: LogicalPlan,
    join_type: JoinType,
) -> Result<LogicalPlan> {
    let subquery_plan = (subquery.subquery).as_ref().clone();

    let new_plan = LogicalPlanBuilder::from(root)
        .dependent_join_on(
            subquery_plan,
            join_type,
            vec![Expr::Literal(ScalarValue::Boolean(Some(true)))],
            subquery.outer_ref_columns.clone(),
        )?
        .build()?;

    Ok(new_plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{Result, Spans};
    use datafusion_expr::{col, Expr, LogicalPlanBuilder, Subquery};
    use datafusion_functions_aggregate::expr_fn::avg;

    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::create_dependent_join::CreateDependentJoin;
    use crate::test::test_table_scan_with_name;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(CreateDependentJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn test_correlated_scalar_subquery() -> Result<()> {
        // outer table
        let employees = test_table_scan_with_name("employees")?;
        // inner table
        let salary = test_table_scan_with_name("salary")?;

        // SELECT AVG(d1) FROM salary WHERE a = c1
        let subquery = Arc::new(
            LogicalPlanBuilder::from(salary)
                .filter(col("salary.a").eq(col("employees.a")))?
                .aggregate(Vec::<Expr>::new(), vec![avg(col("salary.a"))])?
                .build()?,
        );

        // SELECT c1 FROM employees WHERE c1 > (subquery)
        let plan = LogicalPlanBuilder::from(employees)
            .filter(col("employees.a").gt(Expr::ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns: vec![col("employees.a")],
                spans: Spans::new(),
            })))?
            .project(vec![col("employees.a")])?
            .build()?;

        assert_optimized_plan_equal!(
        plan,
        @r"
        Projection: employees.a [a:UInt32]
          Left Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, avg(salary.a):Float64;N]
            TableScan: employees [a:UInt32, b:UInt32, c:UInt32]
            Aggregate: groupBy=[[]], aggr=[[avg(salary.a)]] [avg(salary.a):Float64;N]
              Filter: salary.a = employees.a [a:UInt32, b:UInt32, c:UInt32]
                TableScan: salary [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
