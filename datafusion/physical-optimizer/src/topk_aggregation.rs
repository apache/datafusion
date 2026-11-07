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
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use itertools::Itertools;

/// An optimizer rule that transforms Sort + Limit above a grouped Aggregate to use
/// a more efficient Grouped TopK aggregation implementation when possible.
///
/// For example a query like
///
/// ```sql
/// SELECT trace_id, MAX(timestamp)
/// FROM traces
/// GROUP BY trace_id
/// ORDER BY MAX(timestamp) DESC
/// LIMIT 10
/// ```
///
/// Is planned as a `SortExec` with a `fetch` above an `AggregateExec` (left). After this rule
/// the `AggregateExec` is transformed to a [`GroupedTopKAggregateStream`] that retains only
/// the best `N` groups (right).
///
/// [`GroupedTopKAggregateStream`]: datafusion_physical_plan::aggregates#4-grouped-topk-aggregation
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
/// Without this rule the aggregate computes the aggregates for all groups in the input
/// and the sort discards all but `N` of them afterwards.
///
/// This rule pushes the limit and sort direction into the `AggregateExec` via
/// [`AggregateExec::try_optimize_topk`]. The aggregate then runs the
/// `GroupedTopKAggregateStream`, which keeps only the best `N` groups in a
/// bounded priority map instead of a hash table. The `SortExec` is kept to
/// enforce the final ordering and row count.
///
/// The optimization only applies when the aggregate has a single group key and
/// either no aggregate expressions, or a single `MIN` / `MAX` aggregate that is
/// the sort key. See the "Grouped TopK aggregation" section of the
/// [`aggregates`](datafusion_physical_plan::aggregates) module documentation
/// for details on the eligible query shapes and the runtime implementation.
///
/// # Identifying the optimization in `EXPLAIN` output
///
/// When this rule applies, the `AggregateExec` lines in the physical plan show
/// a `lim=[N]` entry:
///
/// ```text
/// SortPreservingMergeExec: [max(traces.timestamp)@1 DESC], fetch=4
///   SortExec: TopK(fetch=4), expr=[max(traces.timestamp)@1 DESC], preserve_partitioning=[true]
///     AggregateExec: mode=FinalPartitioned, gby=[trace_id@0 as trace_id], aggr=[max(traces.timestamp)], lim=[4]
///       RepartitionExec: partitioning=Hash([trace_id@0], 4), input_partitions=1
///         AggregateExec: mode=Partial, gby=[trace_id@0 as trace_id], aggr=[max(traces.timestamp)], lim=[4]
///           DataSourceExec: partitions=1, partition_sizes=[1]
/// ```
///
/// `EXPLAIN ANALYZE` additionally reports the `topk_maintenance_time` metric
/// for aggregates that run the Grouped TopK implementation.
///
/// # Configuration
///
/// This rule is controlled by the
/// [`datafusion.optimizer.enable_topk_aggregation`](datafusion_common::config::OptimizerOptions::enable_topk_aggregation)
/// configuration setting, which defaults to `true`.
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
