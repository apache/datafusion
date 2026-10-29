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

//! [`InlineAutomaticCtes`] inlines the automatic materialized CTEs for which
//! materialization is not expected to help.

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_expr::logical_plan::FetchType;
use datafusion_expr::{LogicalPlan, MaterializedCte, MaterializedCtePlan};

use crate::{OptimizerConfig, OptimizerRule};

/// Decides, for each automatic [`MaterializedCte`], if the CTE stays
/// materialized or is inlined at each reference.
///
/// With `datafusion.optimizer.enable_automatic_cte_materialization`, the SQL
/// planner creates an automatic [`MaterializedCte`] for each CTE that has no
/// `MATERIALIZED` or `NOT MATERIALIZED` directive. Materialization computes
/// the body one time, but it buffers the full output of the body, and it stops
/// the pushdown of the filters and projections of each reference into the
/// body. This rule replaces the node with [`MaterializedCte::inline`] when one
/// of these conditions is true:
///
/// 1. The CTE has zero or one reference. A scan in a subquery expression or
///    in the body of another CTE is a reference.
/// 2. The body has a volatile expression, and
///    `datafusion.optimizer.automatic_cte_materialization_keeps_volatile` is
///    false. Each reference of an inlined CTE evaluates such an expression
///    again, so the result is the same as with automatic materialization off.
///    When the option is true, the CTE stays materialized and each reference
///    reads the same rows, as in PostgreSQL and DuckDB.
/// 3. The body has an outer reference. Such a body cannot be computed one
///    time.
/// 4. The body is cheap: it has only projections, filters and aliases on one
///    table scan, materialized CTE scan, `VALUES` list or empty relation, and
///    it has no subquery expression. To compute such a body again costs
///    about the same as to read it from the buffer, and the inlined copies
///    get filter and projection pushdown.
/// 5. A `LIMIT` with a fetch is above all references. The rule follows the
///    nodes with one input from the root of the continuation down, and it
///    stops at a node with a subquery expression. A materialized body is
///    computed completely before a reference reads a row, so the limit
///    cannot stop it early. An inlined copy stops when the limit has its
///    rows. A limit above only some references, for example above one side
///    of a join, does not inline the CTE, because the other references read
///    the full body. A limit in the body does not count. A sort with a fetch
///    (TopK) does not count, because it reads all of its input. DuckDB also
///    follows only the chain of nodes with one input, but it also counts
///    TopK.
///
/// In all other cases the CTE stays materialized. The rule does not use
/// statistics or a cost model.
///
/// A node from `AS MATERIALIZED` is not automatic and is never changed.
///
/// The rule decides the innermost CTE first. A later CTE of a `WITH` clause is
/// nested in the continuation of an earlier one, so when a later CTE that
/// reads an earlier CTE is inlined, the earlier CTE is decided with the new
/// number of references.
#[derive(Default, Debug)]
pub struct InlineAutomaticCtes;

impl InlineAutomaticCtes {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for InlineAutomaticCtes {
    fn name(&self) -> &str {
        "inline_automatic_ctes"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Most plans have no automatic CTE. Do not rebuild them.
        if !has_automatic_cte(&plan)? {
            return Ok(Transformed::no(plan));
        }
        let keep_volatile = config
            .options()
            .optimizer
            .automatic_cte_materialization_keeps_volatile;
        plan.transform_up_with_subqueries(|node| match node {
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte))
                if cte.is_automatic() && should_inline(&cte, keep_volatile)? =>
            {
                Ok(Transformed::yes(cte.inline()?))
            }
            _ => Ok(Transformed::no(node)),
        })
    }
}

fn has_automatic_cte(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte)) if cte.is_automatic()) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// See the conditions on [`InlineAutomaticCtes`].
fn should_inline(cte: &MaterializedCte, keep_volatile: bool) -> Result<bool> {
    let body = cte.body();
    Ok(cte.scans()?.len() <= 1
        || (!keep_volatile && is_volatile(body)?)
        || !body.all_out_ref_exprs().is_empty()
        || is_cheap(body)
        || has_limit_above_all_references(cte.continuation())?)
}

/// Returns `true` if a `LIMIT` with a fetch is on the chain of nodes with one
/// input that starts at `plan`, above all nodes with a subquery expression.
fn has_limit_above_all_references(mut plan: &LogicalPlan) -> Result<bool> {
    loop {
        if let LogicalPlan::Limit(limit) = plan
            && matches!(limit.get_fetch_type()?, FetchType::Literal(Some(_)))
        {
            return Ok(true);
        }
        match plan.inputs().as_slice() {
            [input] if !has_subquery_expr(plan) => plan = input,
            _ => return Ok(false),
        }
    }
}

/// Returns `true` if `plan` has a volatile expression, also in a subquery.
fn is_volatile(plan: &LogicalPlan) -> Result<bool> {
    let mut volatile = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            if expr.is_volatile() {
                volatile = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(volatile)
}

/// Returns `true` if `plan` is only projections, filters and aliases, without
/// subquery expressions, on one leaf that reads rows which already exist.
fn is_cheap(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_) => {
            !has_subquery_expr(plan) && is_cheap(plan.inputs()[0])
        }
        LogicalPlan::TableScan(_)
        | LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(_))
        | LogicalPlan::Values(_)
        | LogicalPlan::EmptyRelation(_) => !has_subquery_expr(plan),
        _ => false,
    }
}

fn has_subquery_expr(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply_subqueries(|_| {
        found = true;
        Ok(TreeNodeRecursion::Stop)
    })
    // The closure always returns Ok
    .unwrap();
    found
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan;
    use crate::test::udfs::PlacementTestUDF;
    use crate::{Optimizer, OptimizerContext, OptimizerRule};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::{
        JoinType, MaterializedCteId, MaterializedCteScan, ScalarUDF, Volatility, col,
        in_subquery, lit,
    };
    use datafusion_functions_aggregate::expr_fn::sum;

    macro_rules! assert_optimized_plan_equal {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(InlineAutomaticCtes::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan, @ $expected,)
        }};
    }

    /// A CTE named `name` with the body `body`. `continuation` builds the
    /// query from a function that returns a scan of the CTE with an alias.
    fn cte(
        name: &str,
        body: LogicalPlan,
        automatic: bool,
        continuation: impl FnOnce(&dyn Fn(&str) -> Result<LogicalPlan>) -> Result<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let body = LogicalPlanBuilder::from(body).alias(name)?.build()?;
        let id = MaterializedCteId::next();
        let schema = Arc::clone(body.schema());
        let scan = |alias: &str| {
            LogicalPlanBuilder::from(LogicalPlan::MaterializedCte(
                MaterializedCtePlan::Scan(MaterializedCteScan::new(
                    id,
                    name,
                    Arc::clone(&schema),
                )),
            ))
            .alias(alias)?
            .build()
        };
        let continuation = continuation(&scan)?;
        Ok(LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(
            MaterializedCte::try_new(id, name, Arc::new(body), Arc::new(continuation))?
                .with_automatic(automatic),
        )))
    }

    /// `SELECT a, sum(b) AS s FROM test GROUP BY a`
    fn aggregate_body() -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(test_table_scan()?)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("s")])?
            .build()
    }

    /// `SELECT a, b AS s FROM test WHERE c > 1`
    fn cheap_body() -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(test_table_scan()?)
            .filter(col("c").gt(lit(1u32)))?
            .project(vec![col("a"), col("b").alias("s")])?
            .build()
    }

    /// `SELECT x.a FROM <scan> x JOIN <scan> y ON x.s = y.s`
    fn self_join(scan: &dyn Fn(&str) -> Result<LogicalPlan>) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(scan("x")?)
            .join(
                scan("y")?,
                JoinType::Inner,
                (vec!["x.s"], vec!["y.s"]),
                None,
            )?
            .project(vec![col("x.a")])?
            .build()
    }

    #[test]
    fn keeps_two_references_to_an_aggregate() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, self_join)?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
              TableScan: test
          Projection: x.a
            Inner Join: x.s = y.s
              SubqueryAlias: x
                MaterializedCteScan: name=c
              SubqueryAlias: y
                MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn inlines_one_reference() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |scan| {
            LogicalPlanBuilder::from(scan("x")?)
                .filter(col("x.a").eq(lit(1u32)))?
                .build()
        })?;
        assert_optimized_plan_equal!(plan, @"
        Filter: x.a = UInt32(1)
          SubqueryAlias: x
            SubqueryAlias: c
              Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                TableScan: test
        ")
    }

    #[test]
    fn removes_a_cte_without_reference() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |_| test_table_scan())?;
        assert_optimized_plan_equal!(plan, @"TableScan: test")
    }

    #[test]
    fn inlines_a_cheap_body() -> Result<()> {
        let plan = cte("c", cheap_body()?, true, self_join)?;
        assert_optimized_plan_equal!(plan, @"
        Projection: x.a
          Inner Join: x.s = y.s
            SubqueryAlias: x
              SubqueryAlias: c
                Projection: test.a, test.b AS s
                  Filter: test.c > UInt32(1)
                    TableScan: test
            SubqueryAlias: y
              SubqueryAlias: c
                Projection: test.a, test.b AS s
                  Filter: test.c > UInt32(1)
                    TableScan: test
        ")
    }

    #[test]
    fn keeps_a_volatile_body_with_the_option() -> Result<()> {
        let volatile = ScalarUDF::new_from_impl(
            PlacementTestUDF::new().with_volatility(Volatility::Volatile),
        );
        let body = LogicalPlanBuilder::from(aggregate_body()?)
            .project(vec![col("a"), volatile.call(vec![col("s")]).alias("s")])?
            .build()?;
        let plan = cte("c", body, true, self_join)?;
        let mut options = ConfigOptions::default();
        options
            .optimizer
            .automatic_cte_materialization_keeps_volatile = true;
        let optimizer_ctx = OptimizerContext::new_with_config_options(Arc::new(options))
            .with_max_passes(1);
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
            vec![Arc::new(InlineAutomaticCtes::new())];
        let optimized = Optimizer::with_rules(rules).optimize(
            plan.clone(),
            &optimizer_ctx,
            |_, _| {},
        )?;
        assert_eq!(optimized, plan);
        Ok(())
    }

    #[test]
    fn inlines_a_volatile_body() -> Result<()> {
        let volatile = ScalarUDF::new_from_impl(
            PlacementTestUDF::new().with_volatility(Volatility::Volatile),
        );
        let body = LogicalPlanBuilder::from(aggregate_body()?)
            .project(vec![col("a"), volatile.call(vec![col("s")]).alias("s")])?
            .build()?;
        let plan = cte("c", body, true, self_join)?;
        assert_optimized_plan_equal!(plan, @"
        Projection: x.a
          Inner Join: x.s = y.s
            SubqueryAlias: x
              SubqueryAlias: c
                Projection: test.a, leaf_udf(s) AS s
                  Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                    TableScan: test
            SubqueryAlias: y
              SubqueryAlias: c
                Projection: test.a, leaf_udf(s) AS s
                  Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                    TableScan: test
        ")
    }

    #[test]
    fn never_inlines_an_explicit_cte() -> Result<()> {
        // One reference and a cheap body, but the user asked for the CTE.
        let plan = cte("c", cheap_body()?, false, |scan| scan("x"))?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c
          SubqueryAlias: c
            Projection: test.a, test.b AS s
              Filter: test.c > UInt32(1)
                TableScan: test
          SubqueryAlias: x
            MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn counts_a_scan_in_a_subquery_expression() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |scan| {
            let subquery = LogicalPlanBuilder::from(scan("y")?)
                .project(vec![col("y.s")])?
                .build()?;
            LogicalPlanBuilder::from(scan("x")?)
                .filter(in_subquery(col("x.s"), Arc::new(subquery)))?
                .build()
        })?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
              TableScan: test
          Filter: x.s IN (<subquery>)
            Subquery:
              Projection: y.s
                SubqueryAlias: y
                  MaterializedCteScan: name=c
            SubqueryAlias: x
              MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn counts_the_references_again_after_another_cte_is_inlined() -> Result<()> {
        // `c` has one reference, in the body of `d`. `d` is cheap and has
        // two references, so it is inlined, and then `c` has two references.
        let plan = cte("c", aggregate_body()?, true, |scan_c| {
            let body_d = LogicalPlanBuilder::from(scan_c("c")?)
                .filter(col("c.s").gt(lit(1u64)))?
                .build()?;
            cte("d", body_d, true, self_join)
        })?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
              TableScan: test
          Projection: x.a
            Inner Join: x.s = y.s
              SubqueryAlias: x
                SubqueryAlias: d
                  Filter: c.s > UInt64(1)
                    SubqueryAlias: c
                      MaterializedCteScan: name=c
              SubqueryAlias: y
                SubqueryAlias: d
                  Filter: c.s > UInt64(1)
                    SubqueryAlias: c
                      MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn inlines_a_cte_into_the_body_of_another_cte() -> Result<()> {
        // `c` has one reference, in the body of `d`, which stays materialized.
        let plan = cte("c", aggregate_body()?, true, |scan_c| {
            let body_d = LogicalPlanBuilder::from(scan_c("c")?)
                .aggregate(vec![col("c.s")], vec![sum(col("c.a")).alias("a")])?
                .build()?;
            cte("d", body_d, true, self_join)
        })?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=d, automatic=true
          SubqueryAlias: d
            Aggregate: groupBy=[[c.s]], aggr=[[sum(c.a) AS a]]
              SubqueryAlias: c
                SubqueryAlias: c
                  Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                    TableScan: test
          Projection: x.a
            Inner Join: x.s = y.s
              SubqueryAlias: x
                MaterializedCteScan: name=d
              SubqueryAlias: y
                MaterializedCteScan: name=d
        ")
    }
    #[test]
    fn inlines_under_a_limit_above_all_references() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |scan| {
            LogicalPlanBuilder::from(self_join(scan)?)
                .limit(0, Some(1))?
                .build()
        })?;
        assert_optimized_plan_equal!(plan, @"
        Limit: skip=0, fetch=1
          Projection: x.a
            Inner Join: x.s = y.s
              SubqueryAlias: x
                SubqueryAlias: c
                  Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                    TableScan: test
              SubqueryAlias: y
                SubqueryAlias: c
                  Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                    TableScan: test
        ")
    }

    #[test]
    fn keeps_a_cte_with_a_limit_above_one_reference() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |scan| {
            let limited = LogicalPlanBuilder::from(scan("x")?)
                .limit(0, Some(1))?
                .build()?;
            LogicalPlanBuilder::from(limited)
                .join(
                    scan("y")?,
                    JoinType::Inner,
                    (vec!["x.s"], vec!["y.s"]),
                    None,
                )?
                .project(vec![col("x.a")])?
                .build()
        })?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
              TableScan: test
          Projection: x.a
            Inner Join: x.s = y.s
              Limit: skip=0, fetch=1
                SubqueryAlias: x
                  MaterializedCteScan: name=c
              SubqueryAlias: y
                MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn keeps_a_cte_with_a_topk_above_all_references() -> Result<()> {
        let plan = cte("c", aggregate_body()?, true, |scan| {
            LogicalPlanBuilder::from(self_join(scan)?)
                .sort_with_limit(vec![col("x.a").sort(true, false)], Some(1))?
                .build()
        })?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
              TableScan: test
          Sort: x.a ASC NULLS LAST, fetch=1
            Projection: x.a
              Inner Join: x.s = y.s
                SubqueryAlias: x
                  MaterializedCteScan: name=c
                SubqueryAlias: y
                  MaterializedCteScan: name=c
        ")
    }

    #[test]
    fn keeps_a_cte_with_a_limit_in_the_body() -> Result<()> {
        let body = LogicalPlanBuilder::from(aggregate_body()?)
            .limit(0, Some(1))?
            .build()?;
        let plan = cte("c", body, true, self_join)?;
        assert_optimized_plan_equal!(plan, @"
        MaterializedCte: name=c, automatic=true
          SubqueryAlias: c
            Limit: skip=0, fetch=1
              Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS s]]
                TableScan: test
          Projection: x.a
            Inner Join: x.s = y.s
              SubqueryAlias: x
                MaterializedCteScan: name=c
              SubqueryAlias: y
                MaterializedCteScan: name=c
        ")
    }
}
