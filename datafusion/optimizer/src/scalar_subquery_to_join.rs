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

//! [`ScalarSubqueryToJoin`] rewriting scalar subquery filters to `JOIN`s

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use crate::optimizer::ApplyOrder;
use crate::utils::{evaluates_to_null, replace_qualified_name};
use crate::{OptimizerConfig, OptimizerRule};

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result, ScalarValue, assert_or_internal_err, plan_err};
use datafusion_expr::expr_rewriter::create_col_from_scalar_expr;
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, lit, not, when};

/// Optimizer rule that rewrites scalar subquery filters to joins and places an
/// additional projection on top of the filter to preserve the original schema.
///
/// When [`datafusion_common::config::OptimizerOptions::enable_physical_uncorrelated_scalar_subquery`] is
/// true (the default), only *correlated* scalar subqueries are rewritten here;
/// uncorrelated ones are left for physical execution via `ScalarSubqueryExec`.
/// When the option is false, all scalar subqueries — correlated and
/// uncorrelated — are rewritten to left joins by this rule.
#[derive(Default, Debug)]
pub struct ScalarSubqueryToJoin {}

impl ScalarSubqueryToJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that contain correlated scalar subqueries (and
    /// recurses when found).
    ///
    /// # Arguments
    /// * `predicate` - A conjunction to split and search.
    /// * `alias_gen` - Generator used to produce unique aliases for each
    ///   extracted scalar subquery (e.g. `__scalar_sq_1`, `__scalar_sq_2`).
    ///   Each subquery is replaced by a column reference using the generated
    ///   alias, and the same alias is later used to construct the join.
    ///
    /// Returns a tuple (subqueries, alias)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        alias_gen: &Arc<AliasGenerator>,
        physical_uncorrelated: bool,
    ) -> Result<(Vec<(Subquery, String)>, Expr)> {
        let mut extract = ExtractScalarSubQuery {
            sub_query_info: vec![],
            alias_gen,
            physical_uncorrelated,
        };
        predicate
            .clone()
            .rewrite(&mut extract)
            .data()
            .map(|new_expr| (extract.sub_query_info, new_expr))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let physical_uncorrelated = config
                    .options()
                    .optimizer
                    .enable_physical_uncorrelated_scalar_subquery;
                // Optimization: skip the rest of the rule and its copies if
                // there are no scalar subqueries this rule should rewrite
                if !contains_scalar_subquery_to_rewrite(
                    &filter.predicate,
                    physical_uncorrelated,
                ) {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                let (subqueries, mut rewrite_expr) = self.extract_subquery_exprs(
                    &filter.predicate,
                    config.alias_generator(),
                    physical_uncorrelated,
                )?;

                assert_or_internal_err!(
                    !subqueries.is_empty(),
                    "Expected subqueries not found in filter"
                );

                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = filter.input.as_ref().clone();
                for (subquery, alias) in subqueries {
                    if let Some((optimized_subquery, compensation_exprs)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        if !compensation_exprs.is_empty() {
                            rewrite_expr = rewrite_expr
                                .transform_up(|expr| {
                                    if let Some(compensation_expr) = expr
                                        .try_as_col()
                                        .and_then(|col| compensation_exprs.get(col))
                                    {
                                        Ok(Transformed::yes(compensation_expr.clone()))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .data()?;
                        }
                        cur_input = optimized_subquery;
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                    }
                }

                // Preserve original schema as new Join might have more fields than what Filter & parents expect.
                let projection =
                    filter.input.schema().columns().into_iter().map(Expr::from);
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .filter(rewrite_expr)?
                    .project(projection)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::Projection(projection) => {
                let physical_uncorrelated = config
                    .options()
                    .optimizer
                    .enable_physical_uncorrelated_scalar_subquery;
                // Optimization: skip the rest of the rule and its copies if there
                // are no scalar subqueries this rule should rewrite
                if !projection.expr.iter().any(|expr| {
                    contains_scalar_subquery_to_rewrite(expr, physical_uncorrelated)
                }) {
                    return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                }

                let mut all_subqueries = vec![];
                let mut alias_to_index: HashMap<String, usize> = HashMap::new();
                let mut rewrite_exprs: Vec<Expr> =
                    Vec::with_capacity(projection.expr.len());
                for (idx, expr) in projection.expr.iter().enumerate() {
                    let (subqueries, rewrite_expr) = self.extract_subquery_exprs(
                        expr,
                        config.alias_generator(),
                        physical_uncorrelated,
                    )?;
                    for (_, alias) in &subqueries {
                        alias_to_index.insert(alias.clone(), idx);
                    }
                    all_subqueries.extend(subqueries);
                    rewrite_exprs.push(rewrite_expr);
                }
                assert_or_internal_err!(
                    !all_subqueries.is_empty(),
                    "Expected subqueries not found in projection"
                );
                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = projection.input.as_ref().clone();
                for (subquery, alias) in all_subqueries {
                    if let Some((optimized_subquery, compensation_exprs)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        cur_input = optimized_subquery;
                        if !compensation_exprs.is_empty()
                            && let Some(&idx) = alias_to_index.get(&alias)
                        {
                            let new_expr = rewrite_exprs[idx]
                                .clone()
                                .transform_up(|expr| {
                                    if let Some(compensation_expr) = expr
                                        .try_as_col()
                                        .and_then(|col| compensation_exprs.get(col))
                                    {
                                        Ok(Transformed::yes(compensation_expr.clone()))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .data()?;
                            rewrite_exprs[idx] = new_expr;
                        }
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                    }
                }

                let mut proj_exprs = vec![];
                for (expr, new_expr) in projection.expr.iter().zip(rewrite_exprs) {
                    let old_expr_name = expr.schema_name().to_string();
                    let new_expr_name = new_expr.schema_name().to_string();
                    if new_expr_name != old_expr_name {
                        proj_exprs.push(new_expr.alias(old_expr_name))
                    } else {
                        proj_exprs.push(new_expr);
                    }
                }
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .project(proj_exprs)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }

            plan => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Returns true if the expression contains a scalar subquery that this rule
/// should rewrite to a join.
///
/// When `enable_physical_uncorrelated_scalar_subquery` is true (the default) only
/// correlated scalar subqueries are rewritten — uncorrelated ones are handled
/// by the physical planner via `ScalarSubqueryExec`. When it is false, all
/// scalar subqueries (correlated and uncorrelated) are rewritten.
fn contains_scalar_subquery_to_rewrite(expr: &Expr, physical_uncorrelated: bool) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(sq)
                if !physical_uncorrelated || !sq.outer_ref_columns.is_empty()
        ))
    })
    .expect("Inner is always Ok")
}

struct ExtractScalarSubQuery<'a> {
    sub_query_info: Vec<(Subquery, String)>,
    alias_gen: &'a Arc<AliasGenerator>,
    physical_uncorrelated: bool,
}

impl TreeNodeRewriter for ExtractScalarSubQuery<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            // Match scalar subqueries this rule should rewrite to a join. When
            // `physical_uncorrelated` is true, only correlated subqueries are
            // rewritten — uncorrelated ones are handled later by the physical
            // planner. When false, both are rewritten.
            Expr::ScalarSubquery(ref subquery)
                if !self.physical_uncorrelated
                    || !subquery.outer_ref_columns.is_empty() =>
            {
                let subquery = subquery.clone();
                let scalar_expr = subquery
                    .subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), Ok)?;
                let subqry_alias = self.alias_gen.next("__scalar_sq");
                let col =
                    create_col_from_scalar_expr(&scalar_expr, subqry_alias.clone())?;
                self.sub_query_info.push((subquery, subqry_alias));
                Ok(Transformed::new(
                    Expr::Column(col),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

/// Takes a query like:
///
/// ```text
/// select id from customers where balance >
///     (select avg(total) from orders where orders.c_id = customers.id)
/// ```
///
/// and optimizes it into:
///
/// ```text
/// select c.id from customers c
/// left join (select c_id, avg(total) from orders group by c_id) o
/// on o.c_id = c.id
/// where c.balance > o."avg(total)"
/// ```
///
/// When [`datafusion_common::config::OptimizerOptions::enable_physical_uncorrelated_scalar_subquery`] is
/// false, this function also handles uncorrelated scalar subqueries, rewriting
/// them as a `Left Join: Filter: Boolean(true)` instead of leaving them for
/// `ScalarSubqueryExec`.
///
/// # Arguments
///
/// * `subquery` - The scalar subquery to rewrite (correlated, or uncorrelated
///   when `enable_physical_uncorrelated_scalar_subquery` is false).
/// * `outer_input` - The outer plan that the decorrelated subquery is
///   left-joined onto — the input of the `Filter` or `Projection` node
///   that contained the subquery.
/// * `subquery_alias` - The unique alias assigned to the decorrelated
///   subquery; used both to qualify the join condition and to produce
///   column references for the caller to substitute.
///
/// Returns `Ok(None)` if the subquery cannot be decorrelated. On success,
/// returns the rewritten outer plan and a map from each count-bug-affected
/// column to its `CASE WHEN __always_true IS NULL THEN ... END` compensation
/// expression, which the caller must substitute into any expression that
/// references those columns.
fn build_join(
    subquery: &Subquery,
    outer_input: &LogicalPlan,
    subquery_alias: &str,
) -> Result<Option<(LogicalPlan, HashMap<Column, Expr>)>> {
    // `build_join` also handles uncorrelated scalar subqueries (as a left
    // join with `Boolean(true)`) when the
    // `enable_physical_uncorrelated_scalar_subquery` option is disabled.
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let decorrelated_subquery = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let collected_count_expr_map = pull_up
        .collected_count_expr_map
        .get(&decorrelated_subquery)
        .cloned();
    let aliased_subquery = LogicalPlanBuilder::from(decorrelated_subquery)
        .alias(subquery_alias.to_string())?
        .build()?;

    let all_correlated_cols: BTreeSet<Column> = pull_up
        .correlated_subquery_cols_map
        .values()
        .flatten()
        .cloned()
        .collect();

    // Correlated columns now live in the decorrelated subquery's output,
    // so re-qualify them with the subquery alias.
    let join_filter_opt =
        conjunction(pull_up.join_filters).map_or(Ok(None), |filter| {
            replace_qualified_name(filter, &all_correlated_cols, subquery_alias).map(Some)
        })?;

    // When pull-up did not extract any usable join keys (a correlated subquery
    // whose predicate references only outer columns), fall back to `ON true`:
    // the decorrelated subquery still yields at most one row per outer row
    // because its aggregate is grouped by the (empty) set of correlated inner
    // columns.
    let join_filter = join_filter_opt.or_else(|| Some(lit(true)));

    let new_plan = LogicalPlanBuilder::from(outer_input.clone())
        .join_on(aliased_subquery, JoinType::Left, join_filter)?
        .build()?;

    // Add count-bug compensation for each of the subquery's projected
    // expressions that yield non-NULL values on empty input. We wrap each
    // such expression in a CASE that substitutes the empty-input value
    // when the LEFT JOIN produced synthetic right-side NULLs (no inner
    // row matched), and uses the actual right-side value (which may
    // itself be NULL) otherwise.
    let mut compensation_exprs = HashMap::new();
    if let Some(expr_map) = collected_count_expr_map {
        let mut expr_rewrite = TypeCoercionRewriter {
            schema: new_plan.schema(),
        };
        let having_arm = pull_up
            .pull_up_having_expr
            .as_ref()
            .map(|f| (not(f.clone()), lit(ScalarValue::Null)));
        for (name, result) in expr_map {
            if evaluates_to_null(result.clone(), result.column_refs())? {
                // Aggregates whose empty-input value is NULL (max/min/sum/…)
                // need no compensation: the LEFT JOIN already produces NULL
                // for unmatched outer rows.
                continue;
            }

            let indicator_col =
                Column::new(Some(subquery_alias), UN_MATCHED_ROW_INDICATOR);
            // Qualify with the subquery alias to avoid ambiguity when the
            // outer table has a column with the same name as the aggregate.
            let value_col = Column::new(Some(subquery_alias), name);

            let mut builder = when(Expr::Column(indicator_col).is_null(), result);
            if let Some((when_expr, then_expr)) = &having_arm {
                builder = builder.when(when_expr.clone(), then_expr.clone());
            }
            let compensation_expr = builder.otherwise(Expr::Column(value_col.clone()))?;
            compensation_exprs.insert(
                value_col,
                compensation_expr.rewrite(&mut expr_rewrite).data()?,
            );
        }
    }

    Ok(Some((new_plan, compensation_exprs)))
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use super::*;
    use crate::test::*;

    use arrow::datatypes::DataType;
    use datafusion_expr::test::function_stub::sum;

    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use datafusion_expr::{Between, col, expr, out_ref_col, scalar_subquery};
    use datafusion_functions_aggregate::min_max::{max, min};

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(ScalarSubqueryToJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    /// Test multiple correlated subqueries
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                lit(1)
                    .lt(scalar_subquery(Arc::clone(&orders)))
                    .and(lit(1).lt(scalar_subquery(orders))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: Int32(1) < __scalar_sq_1.max(orders.o_custkey) AND Int32(1) < __scalar_sq_2.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: __scalar_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                  TableScan: customer [c_custkey:Int64, c_name:Utf8]
                  SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                      Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                SubqueryAlias: __scalar_sq_2 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![sum(col("lineitem.l_extendedprice"))],
                )?
                .project(vec![sum(col("lineitem.l_extendedprice"))])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                        .and(col("orders.o_totalprice").lt(scalar_subquery(lineitem))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("orders.o_totalprice"))])?
                .project(vec![sum(col("orders.o_totalprice"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_acctbal").lt(scalar_subquery(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_acctbal < __scalar_sq_1.sum(orders.o_totalprice) [c_custkey:Int64, c_name:Utf8, sum(orders.o_totalprice):Float64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, sum(orders.o_totalprice):Float64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [sum(orders.o_totalprice):Float64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: sum(orders.o_totalprice), orders.o_custkey, __always_true [sum(orders.o_totalprice):Float64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[sum(orders.o_totalprice)]] [o_custkey:Int64, __always_true:Boolean, sum(orders.o_totalprice):Float64;N]
                      Projection: orders.o_orderkey, orders.o_custkey, orders.o_orderstatus, orders.o_totalprice [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        Filter: orders.o_totalprice < __scalar_sq_2.sum(lineitem.l_extendedprice) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N, __always_true:Boolean;N]
                          Left Join:  Filter: __scalar_sq_2.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N, __always_true:Boolean;N]
                            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                            SubqueryAlias: __scalar_sq_2 [sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64, __always_true:Boolean]
                              Projection: sum(lineitem.l_extendedprice), lineitem.l_orderkey, __always_true [sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64, __always_true:Boolean]
                                Aggregate: groupBy=[[lineitem.l_orderkey, Boolean(true) AS __always_true]], aggr=[[sum(lineitem.l_extendedprice)]] [l_orderkey:Int64, __always_true:Boolean, sum(lineitem.l_extendedprice):Float64;N]
                                  TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]
        "
        )
    }

    /// Test for correlated scalar subquery filter with additional subquery filters
    #[test]
    fn scalar_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery with no columns in schema
    #[test]
    fn scalar_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // it will optimize, but fail for the same reason the unoptimized query would
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
              Left Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N]
                  Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for scalar subquery with both columns in schema
    #[test]
    fn scalar_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery not equal
    #[test]
    fn scalar_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .not_eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) != orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery less than
    #[test]
    fn scalar_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .lt(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) < orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery filter with subquery disjunction
    #[test]
    fn scalar_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) = orders.o_custkey OR orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar without projection
    #[test]
    fn scalar_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Error during planning: Scalar subquery should only return one column, but found 4: orders.o_orderkey, orders.o_custkey, orders.o_orderstatus, orders.o_totalprice";
        assert_analyzer_check_err(vec![], plan, expected);
        Ok(())
    }

    /// Test for correlated scalar expressions
    #[test]
    fn scalar_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![col("max(orders.o_custkey)").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) + Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey) + Int32(1), orders.o_custkey, __always_true [max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery with non-strong project
    #[test]
    fn scalar_subquery_with_non_strong_project() -> Result<()> {
        let case = Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(col("max(orders.o_totalprice)")),
                Box::new(lit("a")),
            )],
            else_expr: Some(Box::new(lit("b"))),
        });

        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_totalprice"))])?
                .project(vec![case])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .project(vec![col("customer.c_custkey"), scalar_subquery(sq)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r#"
        Projection: customer.c_custkey, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN CASE WHEN CAST(Float64(NULL) AS Boolean) THEN Utf8("a") ELSE Utf8("b") END ELSE __scalar_sq_1.CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END END AS CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END [c_custkey:Int64, CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8;N]
          Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8;N, o_custkey:Int64;N, __always_true:Boolean;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __scalar_sq_1 [CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8, o_custkey:Int64, __always_true:Boolean]
              Projection: CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END, orders.o_custkey, __always_true [CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8, o_custkey:Int64, __always_true:Boolean]
                Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_totalprice)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_totalprice):Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "#
        )
    }

    /// Test for correlated scalar subquery multiple projected columns
    #[test]
    fn scalar_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Error during planning: Scalar subquery should only return one column, but found 2: orders.o_custkey, orders.o_orderkey";
        assert_analyzer_check_err(vec![], plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional filters
    #[test]
    fn scalar_subquery_additional_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .gt_eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey >= __scalar_sq_1.max(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    #[test]
    fn scalar_subquery_additional_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery filter with disjunctions
    #[test]
    fn scalar_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .or(col("customer.c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .aggregate(Vec::<Expr>::new(), vec![min(col("c"))])?
                .project(vec![min(col("c"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(col("test.c").lt(scalar_subquery(sq)))?
            .project(vec![col("test.c")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c [c:UInt32]
          Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]
            Filter: test.c < __scalar_sq_1.min(sq.c) [a:UInt32, b:UInt32, c:UInt32, min(sq.c):UInt32;N, a:UInt32;N, __always_true:Boolean;N]
              Left Join:  Filter: test.a = __scalar_sq_1.a [a:UInt32, b:UInt32, c:UInt32, min(sq.c):UInt32;N, a:UInt32;N, __always_true:Boolean;N]
                TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                SubqueryAlias: __scalar_sq_1 [min(sq.c):UInt32;N, a:UInt32, __always_true:Boolean]
                  Projection: min(sq.c), sq.a, __always_true [min(sq.c):UInt32;N, a:UInt32, __always_true:Boolean]
                    Aggregate: groupBy=[[sq.a, Boolean(true) AS __always_true]], aggr=[[min(sq.c)]] [a:UInt32, __always_true:Boolean, min(sq.c):UInt32;N]
                      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for non-correlated scalar subquery with no filters
    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").lt(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey < (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn correlated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey BETWEEN __scalar_sq_1.min(orders.o_custkey) AND __scalar_sq_2.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                  TableScan: customer [c_custkey:Int64, c_name:Utf8]
                  SubqueryAlias: __scalar_sq_1 [min(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Projection: min(orders.o_custkey), orders.o_custkey, __always_true [min(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                      Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[min(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, min(orders.o_custkey):Int64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                SubqueryAlias: __scalar_sq_2 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    #[test]
    fn uncorrelated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey BETWEEN (<subquery>) AND (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [min(orders.o_custkey):Int64;N]
              Projection: min(orders.o_custkey) [min(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[min(orders.o_custkey)]] [min(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn uncorrelated_scalar_subquery_rewritten_when_flag_off() -> Result<()> {
        use datafusion_common::config::ConfigOptions;

        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let mut options = ConfigOptions::default();
        options
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;
        let context = crate::OptimizerContext::new_with_config_options(Arc::new(options));

        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(ScalarSubqueryToJoin::new());
        let optimizer = crate::Optimizer::with_rules(vec![rule]);
        let optimized_plan = optimizer
            .optimize(plan, &context, |_, _| {})
            .expect("failed to optimize plan");
        let formatted_plan = optimized_plan.display_indent_schema();

        insta::assert_snapshot!(
            formatted_plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
              Left Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N]
                  Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        );

        Ok(())
    }
}
