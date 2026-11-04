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

//! [`PushDownMaterializedCteFilter`] filters the body of a materialized CTE
//! with the filters that all its references have.

use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::logical_plan::{
    Filter, LogicalPlan, MaterializedCte, MaterializedCtePlan,
};
use datafusion_expr::utils::{conjunction, disjunction, split_conjunction};
use datafusion_expr::{Expr, MaterializedCteId};

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that filters the body of a [`MaterializedCte`] with the
/// disjunction of the filters of its scans.
///
/// The body of a materialized CTE is shared by all the scans of the CTE, so
/// [`PushDownFilter`] keeps the filter of one scan above that scan. If each
/// scan has a filter, a row of the body that passes none of them is not
/// visible in the result. This rule then puts the filter `f1 OR f2 OR ...` on
/// top of the body, so that the body computes and buffers fewer rows:
///
/// ```text
/// MaterializedCte: name=c                 MaterializedCte: name=c
///   TableScan: t                            Filter: t.a = 1 OR t.a = 2
///   Union                                     TableScan: t
///     Filter: c.a = 1               =>      Union
///       MaterializedCteScan: name=c           Filter: c.a = 1
///     Filter: c.a = 2                           MaterializedCteScan: name=c
///       MaterializedCteScan: name=c           Filter: c.a = 2
///                                               MaterializedCteScan: name=c
/// ```
///
/// Each scan keeps its filter, because the body returns the rows for all the
/// scans. If one scan has no filter, the plan does not change.
///
/// # Conditions
///
/// A filter is used only if it is directly above the scan, or above the
/// [`SubqueryAlias`] nodes of the scan. Scans in subquery expressions and in
/// the bodies of other materialized CTEs are scans as all others. From the
/// conjunction of one filter, the rule uses only the terms that are
/// deterministic, and have no subquery and no outer reference. Such a term is a
/// function of one row of the body. If the scan keeps a row, each of its terms
/// is true for that row, thus the disjunction is true too, and the body keeps
/// the row. If a filter has no such term, its scan counts as a scan without a
/// filter.
///
/// The new filter is put on top of the body and nowhere else. The body is
/// evaluated one time as before, and the rows that the filter removes are the
/// rows that no scan returns. A recursive body or a body with a limit is
/// therefore not a special case for this rule. To move the filter further down
/// is the function of [`PushDownFilter`], which does not move a filter through
/// a limit or a [`RecursiveQuery`].
///
/// A body with a volatile expression is not changed. A filter on top of such
/// a body is correct in the logical plan, and [`PushDownFilter`] does not move
/// a filter through a volatile expression. But the physical filter pushdown
/// can replace a column of the filter with the volatile expression that
/// computes it, and give that filter to the data source. The data source then
/// evaluates the volatile expression a second time, and removes rows that a
/// scan must return.
///
/// # Fixed point
///
/// After [`PushDownFilter`] moves the new filter into the body, this rule
/// cannot find it there again. The rule records the filter in
/// [`MaterializedCte::pushed_filter`] and does nothing when the disjunction is
/// equal to the record.
///
/// If another rule changes the filters of the scans between two passes, the
/// disjunction is not equal to the record. The rule then adds the new
/// disjunction to the body and replaces the record. The body then has the
/// two filters, which is correct because each of them is correct. The rule
/// adds a maximum of one filter to a CTE in each optimizer pass.
///
/// [`PushDownFilter`]: crate::push_down_filter::PushDownFilter
/// [`SubqueryAlias`]: datafusion_expr::logical_plan::SubqueryAlias
/// [`RecursiveQuery`]: datafusion_expr::logical_plan::RecursiveQuery
#[derive(Default, Debug)]
pub struct PushDownMaterializedCteFilter;

impl PushDownMaterializedCteFilter {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownMaterializedCteFilter {
    fn name(&self) -> &str {
        "push_down_materialized_cte_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte)) = plan else {
            return Ok(Transformed::no(plan));
        };
        let predicate = match shared_filter(&cte)? {
            Some(predicate) if cte.pushed_filter.as_ref() != Some(&predicate) => {
                predicate
            }
            _ => {
                return Ok(Transformed::no(LogicalPlan::MaterializedCte(
                    MaterializedCtePlan::Cte(cte),
                )));
            }
        };
        if has_volatile_expr(&cte.cte)? {
            return Ok(Transformed::no(LogicalPlan::MaterializedCte(
                MaterializedCtePlan::Cte(cte),
            )));
        }
        let body = Filter::try_new(predicate.clone(), cte.cte)?;
        Ok(Transformed::yes(LogicalPlan::MaterializedCte(
            MaterializedCtePlan::Cte(MaterializedCte {
                cte: Arc::new(LogicalPlan::Filter(body)),
                pushed_filter: Some(predicate),
                ..cte
            }),
        )))
    }
}

/// Returns true if a node of `plan`, also in a subquery expression, has a
/// volatile expression.
fn has_volatile_expr(plan: &LogicalPlan) -> Result<bool> {
    let mut volatile = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            if expr.is_volatile() {
                volatile = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
    })?;
    Ok(volatile)
}

/// Returns the disjunction of the filters of the scans of `cte`, with the
/// columns of the body of `cte`.
///
/// Returns `None` if there is no scan, if a scan has no filter that can be
/// used, or if the disjunction is the literal `true`.
fn shared_filter(cte: &MaterializedCte) -> Result<Option<Expr>> {
    let body_schema = cte.cte.schema();
    let mut scans = 0;
    let mut filters: Vec<Expr> = vec![];
    let mut filtered_scans = 0;
    // The traversal is the traversal of `MaterializedCteScan::find_in`. A
    // filter is visited before the scan below it, so `scans` is larger than
    // `filtered_scans` only after a scan without a filter.
    cte.continuation.apply_with_subqueries(|node| match node {
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(other))
            if other.id == cte.id =>
        {
            Ok(TreeNodeRecursion::Jump)
        }
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan))
            if scan.id == cte.id =>
        {
            scans += 1;
            if scans > filtered_scans {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        }
        LogicalPlan::Filter(filter) if is_scan_of(&filter.input, cte.id) => {
            if let Some(predicate) =
                body_predicate(&filter.predicate, filter.input.schema(), body_schema)?
            {
                filtered_scans += 1;
                if !filters.contains(&predicate) {
                    filters.push(predicate);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    if scans == 0 || scans != filtered_scans || filters.iter().any(is_true) {
        return Ok(None);
    }
    Ok(disjunction(filters))
}

/// Returns true if `plan` is a scan of the CTE `id`, or [`SubqueryAlias`]
/// nodes above such a scan.
///
/// [`SubqueryAlias`]: datafusion_expr::logical_plan::SubqueryAlias
fn is_scan_of(mut plan: &LogicalPlan, id: MaterializedCteId) -> bool {
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => plan = &alias.input,
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan)) => {
                return scan.id == id;
            }
            _ => return false,
        }
    }
}

/// Returns the part of `predicate` that can filter the body of the CTE, with
/// the columns of the body.
///
/// `predicate` is a filter above a scan of the CTE with the schema
/// `scan_schema`. The scan (and an alias above it) gives its columns other
/// qualifiers than the body, but the same positions, so a column is replaced
/// by the column of `body_schema` at the same position.
fn body_predicate(
    predicate: &Expr,
    scan_schema: &DFSchema,
    body_schema: &DFSchema,
) -> Result<Option<Expr>> {
    if scan_schema.fields().len() != body_schema.fields().len() {
        return Ok(None);
    }
    let mut terms = vec![];
    for term in split_conjunction(predicate) {
        if term.is_volatile() || term.contains_outer() || contains_subquery(term) {
            continue;
        }
        let mut resolved = true;
        let term = term
            .clone()
            .transform(|expr| match expr {
                Expr::Column(column) => {
                    match scan_schema.maybe_index_of_column(&column) {
                        Some(index) => Ok(Transformed::yes(Expr::Column(Column::from(
                            body_schema.qualified_field(index),
                        )))),
                        None => {
                            resolved = false;
                            Ok(Transformed::no(Expr::Column(column)))
                        }
                    }
                }
                expr => Ok(Transformed::no(expr)),
            })?
            .data;
        if resolved {
            terms.push(term);
        }
    }
    Ok(conjunction(terms))
}

fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
        ))
    })
    .expect("exists closure is infallible")
}

fn is_true(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(Some(true)), _))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::optimizer::Optimizer;
    use crate::push_down_filter::PushDownFilter;
    use crate::test::udfs::PlacementTestUDF;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::{MaterializedCteScan, ScalarUDF, Volatility, col, exists, lit};
    use insta::assert_snapshot;

    /// A volatile expression of the type `UInt32`.
    fn volatile() -> Expr {
        ScalarUDF::new_from_impl(
            PlacementTestUDF::new().with_volatility(Volatility::Volatile),
        )
        .call(vec![lit(1u32)])
    }

    /// A scan of the CTE `id`, whose body is `test_table_scan()`.
    fn scan(id: MaterializedCteId) -> Result<LogicalPlan> {
        Ok(LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
            MaterializedCteScan::new(
                id,
                "c",
                Arc::clone(test_table_scan_with_name("c")?.schema()),
            ),
        )))
    }

    fn filtered_scan(id: MaterializedCteId, predicate: Expr) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(scan(id)?)
            .filter(predicate)?
            .build()
    }

    fn materialized_cte(
        id: MaterializedCteId,
        body: LogicalPlan,
        continuation: LogicalPlan,
    ) -> Result<LogicalPlan> {
        Ok(LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(
            MaterializedCte::try_new(id, "c", Arc::new(body), Arc::new(continuation))?,
        )))
    }

    /// Runs the rule alone, to a fixed point.
    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        Optimizer::with_rules(vec![Arc::new(PushDownMaterializedCteFilter::new())])
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
    }

    #[test]
    fn pushes_the_disjunction_of_the_filters() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                .union(filtered_scan(id, col("c.a").eq(lit(2u32)))?)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @r"
        MaterializedCte: name=c
          Filter: test.a = UInt32(1) OR test.a = UInt32(2)
            TableScan: test
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(2)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    #[test]
    fn scan_without_filter_stops_the_rule() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                .union(scan(id)?)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @r"
        MaterializedCte: name=c
          TableScan: test
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// With `PushDownFilter`, the new filter moves into the body, and the
    /// rule does not add it again in the subsequent passes.
    #[test]
    fn filter_is_added_one_time() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                .union(filtered_scan(id, col("c.a").eq(lit(2u32)))?)?
                .build()?,
        )?;
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(PushDownFilter::new()),
            Arc::new(PushDownMaterializedCteFilter::new()),
        ]);
        let mut passes = 0;
        let optimized = optimizer.optimize(
            plan,
            &OptimizerContext::new().with_max_passes(8),
            |_, rule| {
                if rule.name() == "push_down_filter" {
                    passes += 1;
                }
            },
        )?;
        // Pass 1 adds the filter, pass 2 moves it, pass 3 changes nothing.
        assert_eq!(passes, 3);
        assert_snapshot!(optimized, @r"
        MaterializedCte: name=c
          TableScan: test, full_filters=[test.a = UInt32(1) OR test.a = UInt32(2)]
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(2)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// The filters of the scans can change between two passes. The rule then
    /// adds the new disjunction above the filter of the previous pass. Each
    /// of the two is correct, so the two together are correct.
    #[test]
    fn changed_filters_add_a_second_filter() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            filtered_scan(id, col("c.a").lt(lit(5u32)))?,
        )?;
        let LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte)) = optimize(plan)?
        else {
            unreachable!()
        };
        assert_eq!(cte.pushed_filter(), Some(&col("test.a").lt(lit(5u32))));
        let plan =
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte.with_new_inputs(
                Arc::clone(cte.body()),
                Arc::new(filtered_scan(id, col("c.a").lt(lit(3u32)))?),
            )));
        let optimized = optimize(plan)?;
        assert_snapshot!(optimized, @"
        MaterializedCte: name=c
          Filter: test.a < UInt32(3)
            Filter: test.a < UInt32(5)
              TableScan: test
          Filter: c.a < UInt32(3)
            MaterializedCteScan: name=c
        ");
        // The same filters do not add a third filter.
        assert_eq!(optimize(optimized.clone())?, optimized);
        Ok(())
    }

    #[test]
    fn filter_above_an_alias_of_the_scan() -> Result<()> {
        let id = MaterializedCteId::next();
        let aliased = LogicalPlanBuilder::from(scan(id)?)
            .alias("x")?
            .filter(col("x.b").gt(lit(1u32)))?
            .build()?;
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(aliased)
                .union(filtered_scan(id, col("c.a").eq(lit(2u32)))?)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.b > UInt32(1) OR test.a = UInt32(2)
            TableScan: test
          Union
            Filter: x.b > UInt32(1)
              SubqueryAlias: x
                MaterializedCteScan: name=c
            Filter: c.a = UInt32(2)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// The columns are mapped by position, so the body can have other column
    /// names and qualifiers than the scans.
    #[test]
    fn body_with_other_qualifiers() -> Result<()> {
        let id = MaterializedCteId::next();
        let body = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("test.b").alias("a"), col("test.a").alias("b")])?
            .build()?;
        let schema = Arc::clone(
            LogicalPlanBuilder::from(test_table_scan_with_name("c")?)
                .project(vec![col("c.a"), col("c.b")])?
                .build()?
                .schema(),
        );
        let scan = |predicate: Expr| {
            LogicalPlanBuilder::from(LogicalPlan::MaterializedCte(
                MaterializedCtePlan::Scan(MaterializedCteScan::new(
                    id,
                    "c",
                    Arc::clone(&schema),
                )),
            ))
            .filter(predicate)?
            .build()
        };
        let plan = materialized_cte(
            id,
            body,
            LogicalPlanBuilder::from(scan(col("c.a").eq(lit(1u32)))?)
                .union(scan(col("c.b").eq(lit(2u32)))?)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: a = UInt32(1) OR b = UInt32(2)
            Projection: test.b AS a, test.a AS b
              TableScan: test
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Filter: c.b = UInt32(2)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// Equal filters give one term, and each filter gives only its
    /// deterministic terms.
    #[test]
    fn volatile_terms_and_equal_filters() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(
                id,
                col("c.a").eq(lit(1u32)).and(volatile().gt(lit(0u32))),
            )?)
            .union(filtered_scan(id, col("c.a").eq(lit(1u32)))?)?
            .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.a = UInt32(1)
            TableScan: test
          Union
            Filter: c.a = UInt32(1) AND leaf_udf(UInt32(1)) > UInt32(0)
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// A scan with only a volatile filter is a scan without a filter.
    #[test]
    fn volatile_filter_stops_the_rule() -> Result<()> {
        let id = MaterializedCteId::next();
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(id, volatile().gt(col("c.a")))?)
                .union(filtered_scan(id, col("c.a").eq(lit(1u32)))?)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          TableScan: test
          Union
            Filter: leaf_udf(UInt32(1)) > c.a
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// A term with an outer reference or a subquery is not a function of one
    /// row of the body.
    #[test]
    fn outer_reference_and_subquery_terms_are_not_used() -> Result<()> {
        let id = MaterializedCteId::next();
        let outer = Expr::OuterReferenceColumn(
            Arc::new(Field::new("a", DataType::UInt32, false)),
            Column::from_qualified_name("o.a"),
        );
        let subquery = Arc::new(test_table_scan_with_name("s")?);
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(
                id,
                col("c.a").eq(outer).and(col("c.b").eq(lit(1u32))),
            )?)
            .union(filtered_scan(
                id,
                exists(subquery).and(col("c.b").eq(lit(2u32))),
            )?)?
            .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.b = UInt32(1) OR test.b = UInt32(2)
            TableScan: test
          Union
            Filter: c.a = outer_ref(o.a) AND c.b = UInt32(1)
              MaterializedCteScan: name=c
            Filter: EXISTS (<subquery>) AND c.b = UInt32(2)
              Subquery:
                TableScan: s
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// A scan in a subquery expression is a scan as all others: with a filter
    /// it gives one term, without a filter it stops the rule.
    #[test]
    fn scan_in_a_subquery_expression() -> Result<()> {
        let id = MaterializedCteId::next();
        let continuation = |subquery: LogicalPlan| {
            LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                .filter(exists(Arc::new(subquery)))?
                .build()
        };
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            continuation(filtered_scan(id, col("c.a").eq(lit(2u32)))?)?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.a = UInt32(2) OR test.a = UInt32(1)
            TableScan: test
          Filter: EXISTS (<subquery>)
            Subquery:
              Filter: c.a = UInt32(2)
                MaterializedCteScan: name=c
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
        ");

        let plan = materialized_cte(id, test_table_scan()?, continuation(scan(id)?)?)?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          TableScan: test
          Filter: EXISTS (<subquery>)
            Subquery:
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// The CTE `c` is read by the body of the CTE `d` and by the continuation
    /// of `d`. The body of `d` gets no row of `c` that its filter removes, so
    /// its filter is a term as all others. The CTE `d` has one scan without a
    /// filter and does not change.
    #[test]
    fn scan_in_the_body_of_another_cte() -> Result<()> {
        let c = MaterializedCteId::next();
        let d = MaterializedCteId::next();
        let inner = LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(
            MaterializedCte::try_new(
                d,
                "d",
                Arc::new(filtered_scan(c, col("c.a").eq(lit(1u32)))?),
                Arc::new(
                    LogicalPlanBuilder::from(LogicalPlan::MaterializedCte(
                        MaterializedCtePlan::Scan(MaterializedCteScan::new(
                            d,
                            "d",
                            Arc::clone(test_table_scan_with_name("c")?.schema()),
                        )),
                    ))
                    .union(filtered_scan(c, col("c.a").eq(lit(2u32)))?)?
                    .build()?,
                ),
            )?,
        ));
        let plan = materialized_cte(c, test_table_scan()?, inner)?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.a = UInt32(1) OR test.a = UInt32(2)
            TableScan: test
          MaterializedCte: name=d
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Union
              MaterializedCteScan: name=d
              Filter: c.a = UInt32(2)
                MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// A nested node with the same id is a separate occurrence of the CTE and
    /// owns its scans.
    #[test]
    fn nested_occurrence_of_the_same_cte() -> Result<()> {
        let id = MaterializedCteId::next();
        let nested = materialized_cte(id, test_table_scan()?, scan(id)?)?;
        let plan = materialized_cte(
            id,
            test_table_scan()?,
            LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                .union(nested)?
                .build()?,
        )?;
        assert_snapshot!(optimize(plan)?, @"
        MaterializedCte: name=c
          Filter: test.a = UInt32(1)
            TableScan: test
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            MaterializedCte: name=c
              TableScan: test
              MaterializedCteScan: name=c
        ");
        Ok(())
    }

    /// `PushDownFilter` does not move the new filter into a recursive query.
    /// A body with a volatile expression gets no filter.
    #[test]
    fn recursive_and_volatile_bodies() -> Result<()> {
        let optimize = |body: LogicalPlan| {
            let id = MaterializedCteId::next();
            let plan = materialized_cte(
                id,
                body,
                LogicalPlanBuilder::from(filtered_scan(id, col("c.a").eq(lit(1u32)))?)
                    .union(filtered_scan(id, col("c.a").eq(lit(2u32)))?)?
                    .build()?,
            )?;
            Optimizer::with_rules(vec![
                Arc::new(PushDownFilter::new()),
                Arc::new(PushDownMaterializedCteFilter::new()),
            ])
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
        };

        let recursive = LogicalPlanBuilder::from(test_table_scan()?)
            .to_recursive_query("r".to_string(), test_table_scan_with_name("r")?, false)?
            .build()?;
        assert_snapshot!(optimize(recursive)?, @"
        MaterializedCte: name=c
          Filter: test.a = UInt32(1) OR test.a = UInt32(2)
            RecursiveQuery: is_distinct=false
              TableScan: test
              TableScan: r
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(2)
              MaterializedCteScan: name=c
        ");

        let volatile_body = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile().alias("a"), col("test.b"), col("test.c")])?
            .build()?;
        assert_snapshot!(optimize(volatile_body)?, @"
        MaterializedCte: name=c
          Projection: leaf_udf(UInt32(1)) AS a, test.b, test.c
            TableScan: test
          Union
            Filter: c.a = UInt32(1)
              MaterializedCteScan: name=c
            Filter: c.a = UInt32(2)
              MaterializedCteScan: name=c
        ");
        Ok(())
    }
}
