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
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_expr::{
    logical_plan::{Filter, JoinType, Subquery},
    Expr, LogicalPlan, LogicalPlanBuilder,
};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateWhereExists {}

impl DecorrelateWhereExists {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have a where in subquery (and recurses when found)
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
                Expr::Exists { subquery, negated } => {
                    let subquery = self
                        .try_optimize(&subquery.subquery, config)?
                        .map(Arc::new)
                        .unwrap_or_else(|| subquery.subquery.clone());
                    let subquery = Subquery { subquery };
                    let subquery = SubqueryInfo::new(subquery.clone(), *negated);
                    subqueries.push(subquery);
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereExists {
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

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = filter.input.as_ref().clone();
                for subquery in subqueries {
                    if let Some(x) = optimize_exists(&subquery, &cur_input)? {
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
        "decorrelate_where_exists"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Takes a query like:
///
/// SELECT t1.id
/// FROM t1
/// WHERE exists
/// (
///    SELECT t2.id FROM t2 WHERE t1.id = t2.id
/// )
///
/// and optimizes it into:
///
/// SELECT t1.id
/// FROM t1 LEFT SEMI
/// JOIN t2
/// ON t1.id = t2.id
///
/// # Arguments
///
/// * query_info - The subquery and negated(exists/not exists) info.
/// * outer_input - The non-subquery portion (relation t1)
fn optimize_exists(
    query_info: &SubqueryInfo,
    outer_input: &LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let maybe_subqury_filter = match query_info.query.subquery.as_ref() {
        LogicalPlan::Distinct(subqry_distinct) => match subqry_distinct.input.as_ref() {
            LogicalPlan::Projection(subqry_proj) => &subqry_proj.input,
            _ => {
                return Ok(None);
            }
        },
        LogicalPlan::Projection(subqry_proj) => &subqry_proj.input,
        _ => {
            // Subquery currently only supports distinct or projection
            return Ok(None);
        }
    }
    .as_ref();

    // extract join filters
    let (join_filters, subquery_input) = extract_join_filters(maybe_subqury_filter)?;
    // cannot optimize non-correlated subquery
    if join_filters.is_empty() {
        return Ok(None);
    }

    let input_schema = subquery_input.schema();
    let subquery_cols: BTreeSet<Column> =
        join_filters
            .iter()
            .try_fold(BTreeSet::new(), |mut cols, expr| {
                let using_cols: Vec<Column> = expr
                    .to_columns()?
                    .into_iter()
                    .filter(|col| input_schema.field_from_column(col).is_ok())
                    .collect::<_>();

                cols.extend(using_cols);
                Result::<_, DataFusionError>::Ok(cols)
            })?;

    let projection_exprs: Vec<Expr> =
        subquery_cols.into_iter().map(Expr::Column).collect();

    let right = LogicalPlanBuilder::from(subquery_input)
        .project(projection_exprs)?
        .build()?;

    let join_filter = conjunction(join_filters);

    // join our sub query into the main plan
    let join_type = match query_info.negated {
        true => JoinType::LeftAnti,
        false => JoinType::LeftSemi,
    };

    // TODO: add Distinct if the original plan is a Distinct.
    let new_plan = LogicalPlanBuilder::from(outer_input.clone())
        .join(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            join_filter,
        )?
        .build()?;

    Ok(Some(new_plan))
}

struct SubqueryInfo {
    query: Subquery,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, negated: bool) -> Self {
        Self { query, negated }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::{
        col, exists, lit, logical_plan::LogicalPlanBuilder, not_exists,
    };
    use std::ops::Add;

    fn assert_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereExists::new()),
            plan,
            expected,
        );
        Ok(())
    }

    /// Test for multiple exists subqueries in the same filter expression
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("customer.c_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(orders.clone()).and(exists(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: orders.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    LeftSemi Join:  Filter: orders.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n      Projection: orders.o_custkey [o_custkey:Int64]\
                        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_plan_eq(&plan, expected)
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(col("lineitem.l_orderkey").eq(col("orders.o_orderkey")))?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    exists(lineitem)
                        .and(col("orders.o_custkey").eq(col("customer.c_custkey"))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: orders.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      LeftSemi Join:  Filter: lineitem.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
                        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
                        \n        Projection: lineitem.l_orderkey [l_orderkey:Int64]\
                        \n          TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";
        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated exists subquery filter with additional subquery filters
    #[test]
    fn exists_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey = orders.o_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
                        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(lit(1u32)))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Other rule will pushdown `customer.c_custkey = 1`,
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey = UInt32(1) [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection:  []\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
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

        assert_optimization_skipped(Arc::new(DecorrelateWhereExists::new()), &plan)
    }

    /// Test for correlated exists subquery not equal
    #[test]
    fn exists_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").not_eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey != orders.o_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated exists subquery less than
    #[test]
    fn exists_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").lt(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey < orders.o_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated exists subquery filter with subquery disjunction
    #[test]
    fn exists_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey = orders.o_custkey OR orders.o_orderkey = Int32(1) [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey, orders.o_orderkey [o_custkey:Int64, o_orderkey:Int64]\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
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

        assert_optimization_skipped(Arc::new(DecorrelateWhereExists::new()), &plan)
    }

    /// Test for correlated exists expressions
    #[test]
    fn exists_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  LeftSemi Join:  Filter: customer.c_custkey = orders.o_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n    Projection: orders.o_custkey [o_custkey:Int64]\
                        \n      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated exists subquery filter with additional filters
    #[test]
    fn should_support_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq).and(col("c_custkey").eq(lit(1))))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
                        \n  Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]\
                        \n    LeftSemi Join:  Filter: customer.c_custkey = orders.o_custkey [c_custkey:Int64, c_name:Utf8]\
                        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
                        \n      Projection: orders.o_custkey [o_custkey:Int64]\
                        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated exists subquery filter with disjustions
    #[test]
    fn exists_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq).or(col("customer.c_custkey").eq(lit(1))))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // not optimized
        let expected = r#"Projection: customer.c_custkey [c_custkey:Int64]
  Filter: EXISTS (<subquery>) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
    Subquery: [o_custkey:Int64]
      Projection: orders.o_custkey [o_custkey:Int64]
        Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]"#;

        assert_plan_eq(&plan, expected)
    }

    /// Test for correlated EXISTS subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(exists(sq))?
            .project(vec![col("test.c")])?
            .build()?;

        let expected  = "Projection: test.c [c:UInt32]\
                        \n  LeftSemi Join:  Filter: test.a = sq.a [a:UInt32, b:UInt32, c:UInt32]\
                        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
                        \n    Projection: sq.a [a:UInt32]\
                        \n      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

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

        assert_optimization_skipped(Arc::new(DecorrelateWhereExists::new()), &plan)
    }

    /// Test for single NOT exists subquery filter
    #[test]
    fn not_exists_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_exists(test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimization_skipped(Arc::new(DecorrelateWhereExists::new()), &plan)
    }

    #[test]
    fn two_exists_subquery_with_outer_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .filter(col("test.a").eq(col("sq1.a")))?
            .project(vec![col("c")])?
            .build()?;

        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .filter(col("test.a").eq(col("sq2.a")))?
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
                        \n    LeftSemi Join:  Filter: test.a = sq2.a [a:UInt32, b:UInt32, c:UInt32]\
                        \n      LeftSemi Join:  Filter: test.a = sq1.a [a:UInt32, b:UInt32, c:UInt32]\
                        \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
                        \n        Projection: sq1.a [a:UInt32]\
                        \n          TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]\
                        \n      Projection: sq2.a [a:UInt32]\
                        \n        TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn exists_subquery_expr_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter((lit(1u32) + col("sq.a")).gt(col("test.a") * lit(2u32)))?
            .project(vec![lit(1u32)])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
                        \n  LeftSemi Join:  Filter: UInt32(1) + sq.a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
                        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
                        \n    Projection: sq.a [a:UInt32]\
                        \n      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

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
                      \n  Filter: EXISTS (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
                      \n    Subquery: [c:UInt32]\
                      \n      Projection: test.c [c:UInt32]\
                      \n        Filter: test.a > test.b [a:UInt32, b:UInt32, c:UInt32]\
                      \n          TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
                      \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }
}
