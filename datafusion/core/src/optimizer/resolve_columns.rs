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

///! Optimizer rule for resolving columns
use crate::execution::context::ExecutionProps;
use crate::optimizer::optimizer::OptimizerRule;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::expr::Subquery;
use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::{Expr, LogicalPlan};
use std::sync::Arc;

/// Optimizer rule for resolving columns
pub struct ResolveColumns {}

impl ResolveColumns {
    /// Create new instance of the ResolveColumns optimizer rule
    pub fn new() -> Self {
        Self {}
    }

    fn resolve_expr_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: Option<Vec<DFSchemaRef>>,
    ) -> datafusion_common::Result<LogicalPlan> {
        /*
        Projection: *
          Filter: Exists(SELECT id FROM bar WHERE id = foo.id)
            SubqueryAlias: foo
              Join
                TableScan
                TableScan
         */
        let plan_rewrite = match plan {
            LogicalPlan::Projection(projection) => {
                // TODO skip all of this if there are no Exists expressions
                let mut expr_rewrite = vec![];
                for e in &projection.expr {
                    let expr_new = match e {
                        Expr::Exists(subquery) => {
                            let outer_schema: Vec<DFSchemaRef> =
                                plan.all_schemas().iter().map(|s| (*s).clone()).collect();
                            let subquery_plan_rewrite = self.resolve_expr_in_plan(
                                &subquery.plan,
                                Some(outer_schema),
                            )?;
                            Expr::Exists(Subquery {
                                plan: Arc::new(subquery_plan_rewrite),
                            })
                        }
                        _ => e.clone(),
                    };
                    expr_rewrite.push(expr_new);
                }
                LogicalPlan::Projection(Projection {
                    expr: expr_rewrite,
                    input: projection.input.clone(),
                    schema: projection.schema.clone(),
                    alias: projection.alias.clone(),
                })
            }
            LogicalPlan::Filter(filter) => {
                let predicate_rewrite = match &filter.predicate {
                    Expr::UnresolvedColumn(col) => {
                        let mut resolved_column: Option<Expr> = None;
                        if let Some(x) = outer_schema {
                            for s in &x {
                                if s.index_of_column(col).is_ok() {
                                    // TODO do we need Expr::OuterColumn ?
                                    resolved_column = Some(Expr::Column(col.clone()));
                                    break;
                                }
                            }
                        }
                        resolved_column.unwrap_or(filter.predicate.clone())
                    }
                    _ => filter.predicate.clone(),
                };
                LogicalPlan::Filter(Filter {
                    predicate: predicate_rewrite,
                    input: filter.input.clone(),
                })
            }
            _ => plan.clone(),
        };

        Ok(plan_rewrite)
    }
}

impl OptimizerRule for ResolveColumns {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> datafusion_common::Result<LogicalPlan> {
        self.resolve_expr_in_plan(plan, None)
    }

    fn name(&self) -> &str {
        "ResolveColumns"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, LogicalPlanBuilder};
    use crate::test::*;
    use datafusion_expr::expr::exists;

    #[test]
    fn correlated_exists_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        // SELECT a FROM bar WHERE a = foo.a
        let subquery = LogicalPlanBuilder::from(bar)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("foo.a")))?
            .build()?;

        // SELECT * FROM foo WHERE EXISTS(SELECT a FROM bar WHERE a = foo.a)
        let plan = LogicalPlanBuilder::from(foo)
            .filter(exists(subquery))?
            .build()?;

        let expected = "Filter: EXISTS (Subquery { plan: Filter: #bar.a = #foo.a\
        \n  Projection: #bar.a\
        \n    TableScan: bar projection=None })\
        \n  TableScan: foo projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = ResolveColumns::new();
        rule.optimize(plan, &ExecutionProps::new())
    }
}
