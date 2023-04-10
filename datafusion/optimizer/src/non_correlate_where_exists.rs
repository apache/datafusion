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
use crate::utils::{conjunction, extract_join_filters, split_conjunction};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::{
    col, lit, logical_plan::Filter, Distinct, Expr, JoinType, LogicalPlan,
    LogicalPlanBuilder,
};

use crate::alias::AliasGenerator;
use crate::decorrelate_where_exists::SubqueryInfo;
use datafusion_expr::expr::Exists;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct NonCorrelateWhereExists {
    alias: Arc<AliasGenerator>,
}

impl NonCorrelateWhereExists {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    fn optimize_exists(
        query_info: &SubqueryInfo,
        outer_input: &LogicalPlan,
        alias: &Arc<AliasGenerator>,
    ) -> Result<Option<LogicalPlan>> {
        let subquery = query_info.query.subquery.as_ref();
        // Rewrite non correlated exists subquery
        //   select count(1) from t1 WHERE EXISTS (select t2.c1 from t2 where t2.c2>0)
        // to left semi join
        //   select count(1) from t1 left semi join (select 1 as val from (select t2.c1 from t2 where t2.c2>0) limit 1) as sq on (sq.val > 0);

        // only optimize non-correlated subquery
        if !subquery.all_out_ref_exprs().is_empty() {
            return Ok(None);
        }

        if let Some(optimized_subquery) = optimize_subquery(subquery)? {
            let scalar_sq_id = alias.next("__scalar_sq");
            let scalar_val_id = alias.next("__scalar_val");
            let logical_plan = LogicalPlanBuilder::from(optimized_subquery)
                .project(vec![
                    lit(ScalarValue::Int64(Option::Some(1))).alias(scalar_val_id.clone())
                ])?
                .limit(0, Option::Some(1))?
                .alias(scalar_sq_id.clone())?
                .build()?;

            let filter = match query_info.negated {
                true => Expr::lt_eq(
                    col(format!("{}.{}", scalar_sq_id, scalar_val_id)),
                    lit(ScalarValue::Int64(Some(0))),
                ),
                false => Expr::gt(
                    col(format!("{}.{}", scalar_sq_id, scalar_val_id)),
                    lit(ScalarValue::Int64(Some(0))),
                ),
            };

            let new_plan = LogicalPlanBuilder::from(outer_input.clone())
                .join(
                    logical_plan,
                    JoinType::LeftSemi,
                    (Vec::<Column>::new(), Vec::<Column>::new()),
                    Some(filter),
                )?
                .build()?;

            Ok(Some(new_plan))
        } else {
            Ok(None)
        }
    }

    /// Finds expressions that have a where in subquery (and recurse when found)
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    /// * `optimizer_config` - For generating unique subquery aliases
    ///
    /// Returns a tuple (subqueries, non-subquery expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        config: &dyn OptimizerConfig,
    ) -> Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let filters = split_conjunction(predicate);

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::Exists(Exists { subquery, negated }) => {
                    let subquery_plan = self
                        .try_optimize(&subquery.subquery, config)?
                        .map(Arc::new)
                        .unwrap_or_else(|| subquery.subquery.clone());
                    let new_subquery = subquery.with_plan(subquery_plan);
                    subqueries.push(SubqueryInfo::new(new_subquery, *negated));
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

fn optimize_subquery(subquery: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    match subquery {
        LogicalPlan::Distinct(subqry_distinct) => {
            let distinct_input = &subqry_distinct.input;
            let optimized_plan = optimize_subquery(distinct_input)?.map(|right| {
                LogicalPlan::Distinct(Distinct {
                    input: Arc::new(right),
                })
            });
            Ok(optimized_plan)
        }
        LogicalPlan::Projection(projection) => {
            // extract subquery_input
            let (join_filters, subquery_input) = extract_join_filters(&projection.input)?;

            // only optimize non-correlated subquery
            if !join_filters.is_empty() {
                return Ok(None);
            }
            let logical_plan = LogicalPlanBuilder::from(subquery_input)
                .project(projection.clone().expr)?
                .build()?;

            Ok(Some(logical_plan))
        }
        _ => Ok(None),
    }
}
impl OptimizerRule for NonCorrelateWhereExists {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(&filter.predicate, config)?;
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(None);
                }

                // iterate through all exists clauses in predicate, turning each into a scalar subquery
                let mut cur_input = filter.input.as_ref().clone();
                for subquery in subqueries {
                    if let Some(x) = NonCorrelateWhereExists::optimize_exists(
                        &subquery,
                        &cur_input,
                        &self.alias,
                    )? {
                        cur_input = x;
                    } else {
                        return Ok(None);
                    }
                }

                let expr = conjunction(other_exprs);
                if let Some(expr) = expr {
                    let new_filter = Filter::try_new(expr, Arc::new(cur_input))?;
                    cur_input = LogicalPlan::Filter(new_filter);
                }

                Ok(Some(cur_input))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "non_correlate_where_exists"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::{col, exists, logical_plan::LogicalPlanBuilder, not_exists};
    fn assert_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(NonCorrelateWhereExists::new()),
            plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn multiple_subqueries() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                exists(test_subquery_with_name("sq")?)
                    .and(exists(test_subquery_with_name("sq")?)),
            )?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
            \n  LeftSemi Join:  Filter: __scalar_sq_3.__scalar_val_4 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
            \n    LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
            \n      SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
            \n        Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
            \n          Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
            \n            Projection: sq.c [c:UInt32]\
            \n              TableScan: sq [a:UInt32, b:UInt32, c:UInt32]\
            \n    SubqueryAlias: __scalar_sq_3 [__scalar_val_4:Int64]\
            \n      Limit: skip=0, fetch=1 [__scalar_val_4:Int64]\
            \n        Projection: Int64(1) AS __scalar_val_4 [__scalar_val_4:Int64]\
            \n          Projection: sq.c [c:UInt32]\
            \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(NonCorrelateWhereExists::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for exists subquery with both columns in schema
    #[test]
    fn exists_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [c_custkey:Int64, c_name:Utf8]\
          \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Projection: orders.o_custkey [o_custkey:Int64]\
          \n            Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
          \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(NonCorrelateWhereExists::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated exists without projection
    #[test]
    fn exists_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        assert_optimization_skipped(Arc::new(NonCorrelateWhereExists::new()), &plan)
    }

    /// Test for correlated exists subquery filter with additional filters
    #[test]
    fn should_support_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(lit(1)))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq).and(col("c_custkey").eq(lit(1))))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
          \n  Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]\
          \n    LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [c_custkey:Int64, c_name:Utf8]\
          \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
          \n      SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n        Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n          Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n            Projection: orders.o_custkey [o_custkey:Int64]\
          \n              Filter: orders.o_custkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
          \n                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_plan_eq(&plan, expected)
    }

    /// Test for single exists subquery filter
    #[test]
    fn exists_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Projection: sq.c [c:UInt32]\
          \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(NonCorrelateWhereExists::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for single NOT exists subquery filter
    #[test]
    fn not_exists_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_exists(test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 <= Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Projection: sq.c [c:UInt32]\
          \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(NonCorrelateWhereExists::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn two_exists_subquery_with_outer_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .filter(col("sq1.b").eq(col("sq1.a")))?
            .project(vec![col("c")])?
            .build()?;

        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .filter(col("sq2.b").eq(col("sq2.a")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                exists(Arc::new(subquery1))
                    .and(exists(Arc::new(subquery2)).and(col("test.c").gt(lit(1u32)))),
            )?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
              \n  Filter: test.c > UInt32(1) [a:UInt32, b:UInt32, c:UInt32]\
              \n    LeftSemi Join:  Filter: __scalar_sq_3.__scalar_val_4 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
              \n      LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
              \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
              \n        SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
              \n          Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
              \n            Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
              \n              Projection: sq1.c [c:UInt32]\
              \n                Filter: sq1.b = sq1.a [a:UInt32, b:UInt32, c:UInt32]\
              \n                  TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]\
              \n      SubqueryAlias: __scalar_sq_3 [__scalar_val_4:Int64]\
              \n        Limit: skip=0, fetch=1 [__scalar_val_4:Int64]\
              \n          Projection: Int64(1) AS __scalar_val_4 [__scalar_val_4:Int64]\
              \n            Projection: sq2.c [c:UInt32]\
              \n              Filter: sq2.b = sq2.a [a:UInt32, b:UInt32, c:UInt32]\
              \n                TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_subquery_expr_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter((lit(1u32) + col("sq.a")).gt(lit(2u32)))?
            .project(vec![lit(1u32)])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Projection: UInt32(1) [UInt32(1):UInt32]\
          \n            Filter: UInt32(1) + sq.a > UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
          \n              TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_subquery_with_same_table() -> Result<()> {
        let outer_scan = test_table_scan()?;
        let subquery_scan = test_table_scan()?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").gt(col("test.b")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        // Subquery and outer query refer to the same table.
        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Projection: test.c [c:UInt32]\
          \n            Filter: test.a > test.b [a:UInt32, b:UInt32, c:UInt32]\
          \n              TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_distinct_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter((lit(1u32) + col("sq.a")).gt(lit(2u32)))?
            .project(vec![col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Distinct: [c:UInt32]\
          \n            Projection: sq.c [c:UInt32]\
          \n              Filter: UInt32(1) + sq.a > UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
          \n                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_distinct_expr_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter((lit(1u32) + col("sq.a")).gt(lit(2u32)))?
            .project(vec![col("sq.b") + col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
          \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
          \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
          \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
          \n          Distinct: [sq.b + sq.c:UInt32]\
          \n            Projection: sq.b + sq.c [sq.b + sq.c:UInt32]\
          \n              Filter: UInt32(1) + sq.a > UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
          \n                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_distinct_subquery_with_literal() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter((lit(1u32) + col("sq.a")).gt(lit(2u32)))?
            .project(vec![lit(1u32), col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
            \n  LeftSemi Join:  Filter: __scalar_sq_1.__scalar_val_2 > Int64(0) [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
            \n    SubqueryAlias: __scalar_sq_1 [__scalar_val_2:Int64]\
            \n      Limit: skip=0, fetch=1 [__scalar_val_2:Int64]\
            \n        Projection: Int64(1) AS __scalar_val_2 [__scalar_val_2:Int64]\
            \n          Distinct: [UInt32(1):UInt32, c:UInt32]\
            \n            Projection: UInt32(1), sq.c [UInt32(1):UInt32, c:UInt32]\
            \n              Filter: UInt32(1) + sq.a > UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
            \n                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }
}
