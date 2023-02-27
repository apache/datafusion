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
use crate::utils::split_conjunction;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::utils::expand_wildcard;
use datafusion_expr::{Aggregate, Distinct, Expr, Filter, LogicalPlan, Subquery};
use ApplyOrder::BottomUp;

use std::sync::Arc;

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

    fn optimize_expr(
        &self,
        expr: &Expr,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<Expr>> {
        let optimized_expr = match expr {
            Expr::Exists { subquery, negated } => self
                .try_optimize(subquery.subquery.as_ref(), _config)?
                .map(|plan| Expr::Exists {
                    subquery: Subquery {
                        subquery: Arc::new(plan),
                    },
                    negated: *negated,
                }),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self
                .try_optimize(subquery.subquery.as_ref(), _config)?
                .map(|plan| Expr::InSubquery {
                    expr: expr.clone(),
                    subquery: Subquery {
                        subquery: Arc::new(plan),
                    },
                    negated: *negated,
                }),
            _ => None,
        };

        Ok(optimized_expr)
    }
}

impl OptimizerRule for ReplaceDistinctWithAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Distinct(Distinct { input }) => {
                let group_expr = expand_wildcard(input.schema(), input)?;
                let aggregate = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    input.clone(),
                    group_expr,
                    vec![],
                    input.schema().clone(), // input schema and aggregate schema are the same in this case
                )?);
                Ok(Some(aggregate))
            }
            LogicalPlan::Filter(filter) => {
                let expr_list = split_conjunction(&filter.predicate);
                if expr_list.is_empty() {
                    return Ok(None);
                }

                let mut optimized_expr_list = split_conjunction(&filter.predicate)
                    .iter()
                    .map(|expr| self.optimize_expr(expr, _config))
                    .collect::<Result<Vec<_>>>()?;

                assert!(!optimized_expr_list.is_empty());

                if optimized_expr_list.iter().any(|expr| expr.is_some()) {
                    // Conjunction the optimized predicates
                    let filter_expr = std::mem::replace(
                        &mut optimized_expr_list[0],
                        Option::<Expr>::None,
                    )
                    .unwrap_or_else(|| expr_list[0].clone());

                    let new_filter = optimized_expr_list
                        .into_iter()
                        .zip(expr_list)
                        .skip(1)
                        .fold(filter_expr, |mut filter_expr, (optimized_expr, expr)| {
                            let next_expr =
                                optimized_expr.unwrap_or_else(|| expr.clone());
                            filter_expr = filter_expr.and(next_expr);
                            filter_expr
                        });

                    let new_filter = Filter::try_new(new_filter, filter.input.clone())?;
                    Ok(Some(LogicalPlan::Filter(new_filter)))
                } else {
                    Ok(None)
                }
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
    use crate::test::*;
    use datafusion_expr::{col, exists, in_subquery, LogicalPlanBuilder};
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

    #[test]
    fn replace_single_distinct_where_in() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        // distinct in where-in subquery
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").eq(col("sq.a")))?
            .project(vec![col("sq.b")])?
            .distinct()?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("test.c"), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b\
                       \n  Filter: test.c IN (<subquery>)\
                       \n    Subquery:\
                       \n      Aggregate: groupBy=[[sq.b]], aggr=[[]]\
                       \n        Projection: sq.b\
                       \n          Filter: test.a = sq.a\
                       \n            TableScan: sq\
                       \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }

    #[test]
    fn replace_distinct_in_where_in() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        // distinct in where-in subquery
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").eq(col("sq.a")))?
            .project(vec![col("sq.b"), col("sq.c")])?
            .distinct()?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("test.c"), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b\
                       \n  Filter: test.c IN (<subquery>)\
                       \n    Subquery:\
                       \n      Aggregate: groupBy=[[sq.b, sq.c]], aggr=[[]]\
                       \n        Projection: sq.b, sq.c\
                       \n          Filter: test.a = sq.a\
                       \n            TableScan: sq\
                       \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }

    #[test]
    fn replace_distinct_in_where_exists() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        // distinct in where-exists subquery
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").eq(col("sq.a")))?
            .project(vec![col("sq.b"), col("sq.c")])?
            .distinct()?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b\
                       \n  Filter: EXISTS (<subquery>)\
                       \n    Subquery:\
                       \n      Aggregate: groupBy=[[sq.b, sq.c]], aggr=[[]]\
                       \n        Projection: sq.b, sq.c\
                       \n          Filter: test.a = sq.a\
                       \n            TableScan: sq\
                       \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }

    #[test]
    fn replace_distinct_multi_predicate() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;
        let subquery_scan3 = test_table_scan_with_name("sq3")?;
        let subquery_scan4 = test_table_scan_with_name("sq4")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .project(vec![col("sq1.a")])?
            .build()?;
        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .project(vec![col("sq2.a")])?
            .distinct()?
            .build()?;
        let subquery3 = LogicalPlanBuilder::from(subquery_scan3)
            .project(vec![col("sq3.a"), col("sq3.b")])?
            .build()?;

        let subquery4 = LogicalPlanBuilder::from(subquery_scan4)
            .project(vec![col("sq4.a"), col("sq4.b")])?
            .distinct()?
            .build()?;

        // filter: `test.a` IN subquery1 and `test.a` IN subquery2 and EXISTS subquery3 and EXISTS subquery4
        // subquery2 and subquery4 have distinct operator, and subquery1 and subquery3 do not have.
        let filter = in_subquery(col("test.a"), Arc::new(subquery1))
            .and(in_subquery(col("test.a"), Arc::new(subquery2)))
            .and(exists(Arc::new(subquery3)))
            .and(exists(Arc::new(subquery4)));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(filter)?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b\
                       \n  Filter: test.a IN (<subquery>) AND test.a IN (<subquery>) AND EXISTS (<subquery>) AND EXISTS (<subquery>)\
                       \n    Subquery:\
                       \n      Projection: sq1.a\
                       \n        TableScan: sq1\
                       \n    Subquery:\
                       \n      Aggregate: groupBy=[[sq2.a]], aggr=[[]]\
                       \n        Projection: sq2.a\
                       \n          TableScan: sq2\
                       \n    Subquery:\
                       \n      Projection: sq3.a, sq3.b\
                       \n        TableScan: sq3\
                       \n    Subquery:\
                       \n      Aggregate: groupBy=[[sq4.a, sq4.b]], aggr=[[]]\
                       \n        Projection: sq4.a, sq4.b\
                       \n          TableScan: sq4\
                       \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }
}
