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

//! Statistics computation for physical plans.
//!
//! [`StatisticsArgs`] provides external context to
//! [`ExecutionPlan::statistics_from_inputs`].

use crate::ExecutionPlan;
use crate::operator_statistics::{
    ExtendedStatistics, StatisticsRegistry, StatisticsResult,
};
use datafusion_common::{
    Result, Statistics, assert_eq_or_internal_err, assert_or_internal_err,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// Per-call memoization cache for statistics computation.
///
/// Keyed by `(plan node pointer address, partition)`. Shared across
/// a single statistics walk via [`StatisticsContext`].
///
/// The pointer-based key is safe within a single synchronous walk:
/// all `Arc<dyn ExecutionPlan>` nodes are held by the plan tree for
/// the duration of the walk, so addresses cannot be reused.
#[derive(Debug, Default)]
struct StatsCache(HashMap<(usize, Option<usize>), Arc<Statistics>>);

impl StatsCache {
    fn get(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<&Arc<Statistics>> {
        let key = (
            plan as *const dyn ExecutionPlan as *const () as usize,
            partition,
        );
        self.0.get(&key)
    }

    fn insert(
        &mut self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
        stats: Arc<Statistics>,
    ) {
        let key = (
            plan as *const dyn ExecutionPlan as *const () as usize,
            partition,
        );
        self.0.insert(key, stats);
    }
}

/// Arguments passed to [`ExecutionPlan::statistics_from_inputs`] carrying
/// external information that operators can use when computing their
/// statistics.
#[derive(Debug, Default, Clone)]
pub struct StatisticsArgs {
    partition: Option<usize>,
}

impl StatisticsArgs {
    /// Creates new statistics arguments.
    ///
    /// By default the partition is set to `None` (statistics should be computed
    /// for the entire plan).
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the partition to compute statistics
    ///
    /// * `None` means statistics should be computed for the entire plan.
    /// * `Some(idx)` means statistics should be computed for the specified
    ///   partition index.
    pub fn set_partition(&mut self, partition: Option<usize>) {
        self.partition = partition;
    }

    /// Builder Style API for [`Self::set_partition`]
    pub fn with_partition(mut self, partition: Option<usize>) -> Self {
        self.set_partition(partition);
        self
    }

    /// Return the partition to compute statistics
    pub fn partition(&self) -> Option<usize> {
        self.partition
    }
}

/// Directive returned by [`ExecutionPlan::child_stats_requests`] describing
/// how the [`StatisticsContext`] should obtain each child's statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildStats {
    /// Compute the child's statistics at this partition (`None` = overall).
    At(Option<usize>),
    /// Skip this child; the parent does not need its statistics. A placeholder
    /// [`Statistics::new_unknown`] is supplied in its slot.
    Skip,
}

/// Owns the bottom-up traversal and per-walk memoization cache for statistics
/// computation. Call [`StatisticsContext::compute`] to walk a plan tree.
///
/// An optional [`StatisticsRegistry`] plugs providers into the walk: at each node
/// they are consulted before the operator's built-in
/// [`ExecutionPlan::statistics_from_inputs`]. An empty registry is the built-in
/// computation.
pub struct StatisticsContext {
    cache: Rc<RefCell<StatsCache>>,
    registry: StatisticsRegistry,
}

impl Default for StatisticsContext {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsContext {
    /// Creates a context with an empty cache and no statistics providers.
    pub fn new() -> Self {
        Self::new_with_registry(StatisticsRegistry::new())
    }

    /// Creates a context whose walk consults `registry`'s provider chain before
    /// falling back to each operator's built-in statistics.
    pub fn new_with_registry(registry: StatisticsRegistry) -> Self {
        Self {
            cache: Rc::new(RefCell::new(StatsCache::default())),
            registry,
        }
    }

    /// Clears the memoization cache.
    ///
    /// The cache is keyed by raw plan-node pointers, which are only stable
    /// while the current plan tree is alive. Reset between optimizer passes
    /// (which rewrite the plan) when reusing one context across them, so stale
    /// pointer keys cannot collide.
    pub fn reset_cache(&self) {
        self.cache.borrow_mut().0.clear();
    }

    /// Computes statistics for `plan`, resolving children first and passing
    /// the results to [`ExecutionPlan::statistics_from_inputs`].
    ///
    /// When `args.partition()` is `Some(idx)`, `idx` is validated against the
    /// plan's partition count.
    pub fn compute(
        &self,
        plan: &dyn ExecutionPlan,
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let partition = args.partition();

        if let Some(idx) = partition {
            let partition_count = plan.properties().partitioning.partition_count();
            assert_or_internal_err!(
                idx < partition_count,
                "Invalid partition index: {}, the partition count is {}",
                idx,
                partition_count
            );
        }

        if let Some(cached) = self.cache.borrow().get(plan, partition) {
            return Ok(Arc::clone(cached));
        }

        let child_stats = self.resolve_children(plan, partition)?;

        let result = match self.try_provider_stats(plan, &child_stats, args)? {
            Some(stats) => stats,
            None => plan.statistics_from_inputs(&child_stats, args)?,
        };
        self.cache
            .borrow_mut()
            .insert(plan, partition, Arc::clone(&result));
        Ok(result)
    }

    /// Resolves each child's statistics following the operator's
    /// [`ExecutionPlan::child_stats_requests`] mapping: `At(p)` computes the child
    /// at partition `p`, `Skip` supplies a [`Statistics::new_unknown`] placeholder.
    ///
    /// A provider computes the same node's statistics, so it depends on the same
    /// children as the node itself: an operator whose statistics (built-in or via
    /// a provider) depend on a child must declare `At` for it.
    fn resolve_children(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Result<Vec<Arc<Statistics>>> {
        let children = plan.children();
        let requests = plan.child_stats_requests(partition);
        assert_eq_or_internal_err!(
            requests.len(),
            children.len(),
            "{} child_stats_requests returned {} entries for {} children",
            plan.name(),
            requests.len(),
            children.len()
        );
        children
            .iter()
            .zip(requests)
            .map(|(child, directive)| match directive {
                ChildStats::At(p) => {
                    self.compute(child.as_ref(), &StatisticsArgs::new().with_partition(p))
                }
                ChildStats::Skip => {
                    Ok(Arc::new(Statistics::new_unknown(child.schema().as_ref())))
                }
            })
            .collect()
    }

    /// Runs the provider chain, returning the first `Computed` result's base
    /// statistics, or `None` if all delegate or the chain is empty. A
    /// partition-blind provider applies only to overall stats (its default
    /// `compute_statistics_with_args` delegates per partition).
    ///
    /// Providers may attach custom extensions to their [`ExtendedStatistics`]
    /// result, but the walk uses only the base [`Statistics`].
    fn try_provider_stats(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Option<Arc<Statistics>>> {
        let providers = self.registry.providers();
        if providers.is_empty() {
            return Ok(None);
        }
        // Providers take `&[ExtendedStatistics]`; wrap the resolved base stats.
        let child_ext: Vec<ExtendedStatistics> = child_stats
            .iter()
            .map(|s| ExtendedStatistics::new_arc(Arc::clone(s)))
            .collect();
        for provider in providers {
            if let StatisticsResult::Computed(stats) =
                provider.compute_statistics_with_args(plan, &child_ext, args)?
            {
                return Ok(Some(Arc::clone(stats.base_arc())));
            }
        }
        Ok(None)
    }
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::operator_statistics::StatisticsProvider;
    use crate::test::exec::StatisticsExec;
    use crate::union::UnionExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, stats::Precision};

    /// Overall-only provider: sets a fixed row count for any node.
    #[derive(Debug)]
    struct FixedRowCountProvider(usize);
    impl StatisticsProvider for FixedRowCountProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            _child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(self.0);
            Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
        }
    }

    /// Partition-aware provider: encodes the requested partition in the row count.
    #[derive(Debug)]
    struct PartitionRowCountProvider;
    impl StatisticsProvider for PartitionRowCountProvider {
        fn compute_statistics_with_args(
            &self,
            plan: &dyn ExecutionPlan,
            _child_stats: &[ExtendedStatistics],
            args: &StatisticsArgs,
        ) -> Result<StatisticsResult> {
            let marker = 700 + args.partition().map_or(0, |p| p + 1);
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(marker);
            Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
        }
    }

    fn ctx_with(provider: Arc<dyn StatisticsProvider>) -> StatisticsContext {
        StatisticsContext::new_with_registry(StatisticsRegistry::with_providers(vec![
            provider,
        ]))
    }

    fn make_stats_leaf(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let col_stats = vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }];
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: col_stats,
            },
            schema,
        ))
    }

    #[test]
    fn coalesce_returns_overall_stats_for_any_partition() {
        let leaf = make_stats_leaf(100);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf));

        let ctx = StatisticsContext::new();
        let stats = ctx
            .compute(
                plan.as_ref(),
                &StatisticsArgs::new().with_partition(Some(0)),
            )
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(100));

        let stats_none = ctx.compute(plan.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(stats_none.num_rows, Precision::Exact(100));
    }

    #[test]
    fn context_caches_within_walk() {
        let leaf = make_stats_leaf(42);
        let ctx = StatisticsContext::new();
        let args = StatisticsArgs::new();

        let s1 = ctx.compute(leaf.as_ref(), &args).unwrap();
        assert!(!ctx.cache.borrow().0.is_empty());

        let s2 = ctx.compute(leaf.as_ref(), &args).unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn reset_cache_clears_entries() {
        let leaf = make_stats_leaf(10);
        let ctx = StatisticsContext::new();
        let _ = ctx.compute(leaf.as_ref(), &StatisticsArgs::new()).unwrap();
        assert!(!ctx.cache.borrow().0.is_empty());
        ctx.reset_cache();
        assert!(ctx.cache.borrow().0.is_empty());
    }

    #[test]
    fn partition_aware_provider_applies_per_partition() {
        let leaf = make_stats_leaf(10);
        let ctx = ctx_with(Arc::new(PartitionRowCountProvider));

        let per_part = ctx
            .compute(
                leaf.as_ref(),
                &StatisticsArgs::new().with_partition(Some(0)),
            )
            .unwrap();
        assert_eq!(per_part.num_rows, Precision::Exact(701));
    }

    #[test]
    fn per_partition_union_with_registry_no_out_of_bounds() {
        // Two 2-partition inputs -> 4 output partitions. Union owns output
        // partition 3 via its second input (owning_input(3) = (1, 1)); the first
        // input is Skipped, so the walk supplies a placeholder for it (never
        // resolving it at partition 3, which is out of that input's 0..2 range).
        // An overall-only provider delegates for a specific partition, so p3 keeps
        // the operator's honest per-partition answer while the overall request
        // picks up the provider's row count.
        let union =
            UnionExec::try_new(vec![make_stats_leaf(10), make_stats_leaf(20)]).unwrap();
        let ctx = ctx_with(Arc::new(FixedRowCountProvider(999)));

        let p3 = ctx
            .compute(
                union.as_ref(),
                &StatisticsArgs::new().with_partition(Some(3)),
            )
            .unwrap();
        assert_eq!(p3.num_rows, Precision::Absent);

        let overall = ctx.compute(union.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(overall.num_rows, Precision::Exact(999));
    }
}
