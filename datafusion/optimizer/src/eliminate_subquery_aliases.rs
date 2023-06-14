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
use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion_common::{Column, OwnedTableReference, Result};
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};

/// Optimization rule that eliminate unnecessary [LogicalPlan::SubqueryAlias].
#[derive(Default)]
pub struct EliminateSubqueryAliases;

impl EliminateSubqueryAliases {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl OptimizerRule for EliminateSubqueryAliases {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias: _alias,
                schema,
                ..
            }) => {
                let exprs = input
                    .expressions()
                    .iter()
                    .zip(schema.fields().iter())
                    .map(|(expr, field)| match expr {
                        Expr::Alias(_, name) => Expr::Column(Column::new(
                            Option::<OwnedTableReference>::None,
                            name.clone(),
                        ))
                        .alias(field.qualified_name()),
                        _ => expr.clone().alias(field.qualified_name()),
                    })
                    .collect::<Vec<Expr>>();

                let new_plan = LogicalPlan::Projection(
                    Projection::try_new(exprs, input.clone()).unwrap(),
                );

                Ok(Some(new_plan))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_subquery_aliases"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::analyzer::count_wildcard_rule::COUNT_STAR;
    use crate::test::{assert_optimized_plan_eq, test_table_scan_with_name};
    use datafusion_expr::Expr::Wildcard;
    use datafusion_expr::{col, count, LogicalPlanBuilder};
    use std::sync::Arc;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(
            Arc::new(EliminateSubqueryAliases::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn eliminate_subquery_aliases() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            // .filter(col("a").eq(lit(1)))?
            .project(vec![col("a"), col("b"), col("c")])?
            .alias("t1")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq)
            .project(vec![col("a")])?
            .build()?;
        let expected = "Projection: t1.a\
          \n  SubqueryAlias: t1\
          \n    Projection: t.a, t.b, t.c\
          \n      TableScan: t";
        let origin_plan = format!("{plan:?}");
        assert_eq!(origin_plan, expected);

        let expected = "Projection: t1.a\
          \n  Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c\
          \n    Projection: t.a, t.b, t.c\
          \n      TableScan: t";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_subquery_aliases_wildcard() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .project(vec![Wildcard])?
            .alias("t1")?
            .build()?;

        let origin_plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;
        let expected = "Projection: t1.a, t1.b, t1.c\
          \n  SubqueryAlias: t1\
          \n    Projection: t.a, t.b, t.c\
          \n      TableScan: t";
        assert_eq!(format!("{origin_plan:?}"), expected);

        let expected = "Projection: t1.a, t1.b, t1.c\
          \n  Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c\
          \n    Projection: t.a, t.b, t.c\
          \n      TableScan: t";

        assert_optimized_plan_equal(&origin_plan, expected)
    }

    #[test]
    fn eliminate_multi_subquery_aliases() -> Result<()> {
        let sq_t1 = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .project(vec![Wildcard])?
            .alias("t1")?
            .build()?;

        let sq_t2 = LogicalPlanBuilder::from(sq_t1)
            .project(vec![Wildcard])?
            .alias("t2")?
            .build()?;

        let origin_plan = LogicalPlanBuilder::from(sq_t2)
            .project(vec![Wildcard])?
            .build()?;

        let expected = "Projection: t2.a, t2.b, t2.c\
          \n  SubqueryAlias: t2\
          \n    Projection: t1.a, t1.b, t1.c\
          \n      SubqueryAlias: t1\
          \n        Projection: t.a, t.b, t.c\
          \n          TableScan: t";
        assert_eq!(format!("{origin_plan:?}"), expected);

        let expected = "Projection: t2.a, t2.b, t2.c\
          \n  Projection: t1.a AS t2.a, t1.b AS t2.b, t1.c AS t2.c\
          \n    Projection: t1.a, t1.b, t1.c\
          \n      Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c\
          \n        Projection: t.a, t.b, t.c\
          \n          TableScan: t";

        assert_optimized_plan_equal(&origin_plan, expected)
    }

    #[test]
    fn eliminate_subquery_aliases_with_agg() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .aggregate(Vec::<Expr>::new(), vec![count(col("a"))])?
            .project(vec![count(col("a")).alias(COUNT_STAR)])?
            .alias("t1")?
            .build()?;

        let origin_plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;

        let expected = "Projection: t1.COUNT(*)\
          \n  SubqueryAlias: t1\
          \n    Projection: COUNT(t.a) AS COUNT(*)\
          \n      Aggregate: groupBy=[[]], aggr=[[COUNT(t.a)]]\
          \n        TableScan: t";

        assert_eq!(format!("{origin_plan:?}"), expected);

        let expected = "Projection: t1.COUNT(*)\
          \n  Projection: COUNT(*) AS t1.COUNT(*)\
          \n    Projection: COUNT(t.a) AS COUNT(*)\
          \n      Aggregate: groupBy=[[]], aggr=[[COUNT(t.a)]]\
          \n        TableScan: t";

        assert_optimized_plan_equal(&origin_plan, expected)
    }
    #[test]
    fn eliminate_subquery_aliases_with_gby_agg() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .aggregate(vec![col("b")], vec![count(col("a"))])?
            .project(vec![col("b"), count(col("a")).alias(COUNT_STAR)])?
            .alias("t1")?
            .build()?;

        let origin_plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;

        let expected = "Projection: t1.b, t1.COUNT(*)\
          \n  SubqueryAlias: t1\
          \n    Projection: t.b, COUNT(t.a) AS COUNT(*)\
          \n      Aggregate: groupBy=[[t.b]], aggr=[[COUNT(t.a)]]\
          \n        TableScan: t";

        assert_eq!(format!("{origin_plan:?}"), expected);

        let expected = "Projection: t1.b, t1.COUNT(*)\
          \n  Projection: t.b AS t1.b, COUNT(*) AS t1.COUNT(*)\
          \n    Projection: t.b, COUNT(t.a) AS COUNT(*)\
          \n      Aggregate: groupBy=[[t.b]], aggr=[[COUNT(t.a)]]\
          \n        TableScan: t";

        assert_optimized_plan_equal(&origin_plan, expected)
    }
}
