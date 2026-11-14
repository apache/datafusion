// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! An optimizer rule that detects aggregate operations that could use a limited bucket count

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::materialized_cte::MaterializedCteExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use itertools::Itertools;

/// An optimizer rule that transforms Sort + Limit above a grouped Aggregate to use
/// a more efficient Grouped TopK aggregation implementation when possible.
///
/// For example, a query like
///
/// ```sql
/// SELECT trace_id, MAX(timestamp)
/// FROM traces
/// GROUP BY trace_id
/// ORDER BY MAX(timestamp) DESC
/// LIMIT 10
/// ```
///
/// is planned as a `SortExec` with a `fetch` above an `AggregateExec` (left).
/// After this rule, the `AggregateExec` uses a [`GroupedTopKAggregateStream`]
/// that retains only the best `N` groups (right).
///
/// [`GroupedTopKAggregateStream`]: mod@datafusion_physical_plan::aggregates#4-grouped-topk-aggregation
///
/// ```text
///          Before                                        After
///
/// ┌────────────────────────────┐             ┌────────────────────────────┐
/// │ SortExec: TopK(fetch=10)   │             │ SortExec: TopK(fetch=10)   │
/// │ expr=[max(timestamp) DESC] │             │ expr=[max(timestamp) DESC] │
/// └────────────────────────────┘             └────────────────────────────┘
///               ▲                                          ▲
///               │ every group                              │ at most 10 groups
///               │                                          │
/// ┌────────────────────────────┐             ┌────────────────────────────┐
/// │ AggregateExec              │             │ AggregateExec, lim=[10]    │
/// │ gby=[trace_id]             │             │ gby=[trace_id]             │
/// │ aggr=[max(timestamp)]      │  ────────▶  │ aggr=[max(timestamp)]      │
/// │                            │             │                            │
/// │ hash table holding         │             │ priority map holding only  │
/// │ every group                │             │ the 10 best groups         │
/// └────────────────────────────┘             └────────────────────────────┘
///               ▲                                          ▲
///               │                                          │
/// ┌────────────────────────────┐             ┌────────────────────────────┐
/// │ input                      │             │ input                      │
/// └────────────────────────────┘             └────────────────────────────┘
/// ```
///
/// Without this rule, the aggregate computes the aggregates for **all** groups
/// in the input, and the sort then discards all but `N` of them.
///
/// `GroupedTopKAggregateStream` keeps only the best `N` groups in a
/// priority map rather than a hash table and the `SortExec` simply
/// enforces the final ordering and row count.
///
/// See the "Grouped TopK aggregation" section of the
/// [`aggregates`](mod@datafusion_physical_plan::aggregates) module documentation
/// for more details.
///
/// This rule is enabled by default and is controlled by the
/// [`datafusion.optimizer.enable_topk_aggregation`](datafusion_common::config::OptimizerOptions::enable_topk_aggregation)
/// setting.
///
/// # Identifying the optimization in `EXPLAIN` output
///
/// When this rule applies, the `AggregateExec` lines in the physical plan show
/// a `lim=[N]` entry, for example:
///
/// ```text
///     AggregateExec: ... aggr=[max(...)], lim=[10] <-- LIM
/// ```
#[derive(Debug)]
pub struct TopKAggregation {}

impl TopKAggregation {
    /// Create a new `TopKAggregation`
    pub fn new() -> Self {
        Self {}
    }

    /// Push the bound into eligible aggregates to retain fewer groups; keep the
    /// sort to enforce the final ordering and row count.
    ///
    /// ```txt
    /// Before:
    /// Sort(fetch=K)
    ///   Aggregate
    ///     input
    ///
    /// After:
    /// Sort(fetch=K)
    ///   Aggregate(TopK(k))
    ///     input
    /// ```
    fn transform_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.into_iter().exactly_one().ok()?;
        let order = sort.properties().output_ordering()?;
        let order = order.iter().exactly_one().ok()?;
        let sort_options = order.options;
        let order = order.expr.downcast_ref::<Column>()?;
        let mut sort_col_name = order.name().to_string();
        let limit = sort.fetch()?;

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.downcast_ref::<AggregateExec>() {
                match aggr
                    .clone()
                    .try_optimize_topk(limit, &sort_col_name, sort_options)
                {
                    None => cardinality_preserved = false,
                    Some(aggr) => return Ok(Transformed::yes(Arc::new(aggr.data))),
                }
            } else if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
                // track renames due to successive projections
                for proj_expr in proj.expr() {
                    let Some(src_col) = proj_expr.expr.downcast_ref::<Column>() else {
                        continue;
                    };
                    if proj_expr.alias == sort_col_name {
                        sort_col_name = src_col.name().to_string();
                    }
                }
            } else if plan.downcast_ref::<MaterializedCteExec>().is_some() {
                // The output is the continuation, but the first child is the
                // CTE body, which every reference to the CTE reads in full.
                // A limit in the body would drop rows that they need.
                cardinality_preserved = false;
            } else {
                // or we continue down through types that don't reduce cardinality
                match plan.cardinality_effect() {
                    CardinalityEffect::Equal | CardinalityEffect::GreaterEqual => {}
                    CardinalityEffect::Unknown | CardinalityEffect::LowerEqual => {
                        cardinality_preserved = false;
                    }
                }
            }
            Ok(Transformed::no(plan))
        };
        let child = Arc::clone(child).transform_down(closure).data().ok()?;
        let sort = SortExec::new(sort.expr().clone(), child)
            .with_fetch(sort.fetch())
            .with_preserve_partitioning(sort.preserve_partitioning());
        Some(Arc::new(sort))
    }
}

impl Default for TopKAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for TopKAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.enable_topk_aggregation {
            plan.transform_down(|plan| {
                Ok(if let Some(plan) = TopKAggregation::transform_sort(&plan) {
                    Transformed::yes(plan)
                } else {
                    Transformed::no(plan)
                })
            })
            .data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "LimitAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// see `aggregates_topk.slt` for tests
