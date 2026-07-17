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
use datafusion_common::extensions::Extensions;
use datafusion_common::{
    Result, Statistics, assert_eq_or_internal_err, assert_or_internal_err,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

type CacheKey = (usize, Option<usize>);

fn cache_key(plan: &dyn ExecutionPlan, partition: Option<usize>) -> CacheKey {
    (
        plan as *const dyn ExecutionPlan as *const () as usize,
        partition,
    )
}

/// Per-call memoization cache for statistics computation.
///
/// Keyed by `(plan node pointer address, partition)`. Shared across
/// a single statistics walk via [`StatisticsContext`].
///
/// The pointer-based key is safe within a single synchronous walk:
/// all `Arc<dyn ExecutionPlan>` nodes are held by the plan tree for
/// the duration of the walk, so addresses cannot be reused.
///
/// Core statistics and provider extensions are cached separately: the
/// `statistics` map is the hot path (populated on every walk); the `extensions`
/// map is populated only when a provider returns non-empty extensions, so a walk
/// with no providers never touches it.
#[derive(Debug, Default)]
struct StatsCache {
    statistics: HashMap<CacheKey, Arc<Statistics>>,
    extensions: HashMap<CacheKey, Extensions>,
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
///
/// The walk carries [`ExtendedStatistics`]. A node has extensions only if a
/// provider `Computed` them for it; a node that falls back to the built-in
/// [`ExecutionPlan::statistics_from_inputs`] has none. So extensions propagate
/// upward only through an unbroken chain of provider-handled nodes: a single
/// built-in node yields no extensions and hides those of everything beneath it.
/// [`Self::compute_extended`] observes extensions; [`Self::compute`] returns core
/// [`Statistics`] only.
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

    /// Creates a context whose walk consults `registry`'s provider chain.
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
        let mut cache = self.cache.borrow_mut();
        cache.statistics.clear();
        cache.extensions.clear();
    }

    /// Computes the core [`Statistics`] for `plan`, discarding any
    /// provider-supplied extensions (see [`Self::compute_extended`]).
    ///
    /// With no providers registered this is the plain built-in walk: only the
    /// `statistics` cache is touched, so it carries no extension overhead.
    pub fn compute(
        &self,
        plan: &dyn ExecutionPlan,
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.compute_base(plan, args)
    }

    /// Computes the [`ExtendedStatistics`] for `plan`: the core statistics plus
    /// any extensions a provider attached to this node (see the type-level docs
    /// for how extensions propagate up the tree).
    pub fn compute_extended(
        &self,
        plan: &dyn ExecutionPlan,
        args: &StatisticsArgs,
    ) -> Result<Arc<ExtendedStatistics>> {
        let statistics = self.compute_base(plan, args)?;
        let extensions = self
            .cached_extensions(plan, args.partition())
            .unwrap_or_default();
        Ok(Arc::new(ExtendedStatistics::new_with_extensions(
            statistics, extensions,
        )))
    }

    /// Bottom-up walk producing the node's core statistics, resolving children
    /// first and consulting the provider chain before the operator's built-in
    /// [`ExecutionPlan::statistics_from_inputs`]. Any extensions a provider
    /// attaches are recorded in the extension cache for [`Self::compute_extended`].
    ///
    /// When `args.partition()` is `Some(idx)`, `idx` is validated against the
    /// plan's partition count.
    fn compute_base(
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

        if let Some(cached) = self.cached_statistics(plan, partition) {
            return Ok(cached);
        }

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
        let child_statistics: Vec<Arc<Statistics>> = children
            .iter()
            .zip(&requests)
            .map(|(child, directive)| match directive {
                ChildStats::At(p) => self.compute_base(
                    child.as_ref(),
                    &StatisticsArgs::new().with_partition(*p),
                ),
                ChildStats::Skip => {
                    Ok(Arc::new(Statistics::new_unknown(child.schema().as_ref())))
                }
            })
            .collect::<Result<_>>()?;

        let statistics = match self.try_provider_stats(
            plan,
            &children,
            &requests,
            &child_statistics,
            args,
        )? {
            Some(statistics) => statistics,
            None => plan.statistics_from_inputs(&child_statistics, args)?,
        };
        self.store_statistics(plan, partition, Arc::clone(&statistics));
        Ok(statistics)
    }

    /// Runs the provider chain, returning the first `Computed` result's core
    /// statistics (and recording its extensions), or `None` if the chain is empty
    /// or all delegate. A partition-blind provider applies only to overall stats
    /// (its default `compute_statistics_with_args` delegates per partition).
    ///
    /// Assembles each child's [`ExtendedStatistics`] (statistics plus cached
    /// extensions) only here, so a walk with no providers pays no extension cost.
    fn try_provider_stats(
        &self,
        plan: &dyn ExecutionPlan,
        children: &[&Arc<dyn ExecutionPlan>],
        requests: &[ChildStats],
        child_statistics: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Option<Arc<Statistics>>> {
        let providers = self.registry.providers();
        if providers.is_empty() {
            return Ok(None);
        }
        let child_extended =
            self.child_extended_stats(children, requests, child_statistics);
        for provider in providers {
            if let StatisticsResult::Computed(computed) =
                provider.compute_statistics_with_args(plan, &child_extended, args)?
            {
                if !computed.extensions().is_empty() {
                    self.store_extensions(
                        plan,
                        args.partition(),
                        computed.extensions().clone(),
                    );
                }
                return Ok(Some(Arc::clone(computed.base_arc())));
            }
        }
        Ok(None)
    }

    /// Pairs each child's core statistics with any extensions cached for it,
    /// producing the [`ExtendedStatistics`] the provider chain consumes. Called
    /// only when providers exist, so an empty registry pays no extension cost.
    fn child_extended_stats(
        &self,
        children: &[&Arc<dyn ExecutionPlan>],
        requests: &[ChildStats],
        child_statistics: &[Arc<Statistics>],
    ) -> Vec<ExtendedStatistics> {
        children
            .iter()
            .zip(requests)
            .zip(child_statistics)
            .map(|((child, directive), statistics)| {
                let extensions = match directive {
                    ChildStats::At(p) => self.cached_extensions(child.as_ref(), *p),
                    ChildStats::Skip => None,
                };
                match extensions {
                    Some(extensions) => ExtendedStatistics::new_with_extensions(
                        Arc::clone(statistics),
                        extensions,
                    ),
                    None => ExtendedStatistics::new_arc(Arc::clone(statistics)),
                }
            })
            .collect()
    }

    fn cached_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<Arc<Statistics>> {
        self.cache
            .borrow()
            .statistics
            .get(&cache_key(plan, partition))
            .cloned()
    }

    fn store_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
        statistics: Arc<Statistics>,
    ) {
        self.cache
            .borrow_mut()
            .statistics
            .insert(cache_key(plan, partition), statistics);
    }

    fn cached_extensions(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<Extensions> {
        self.cache
            .borrow()
            .extensions
            .get(&cache_key(plan, partition))
            .cloned()
    }

    fn store_extensions(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
        extensions: Extensions,
    ) {
        self.cache
            .borrow_mut()
            .extensions
            .insert(cache_key(plan, partition), extensions);
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

    #[derive(Debug, Clone, PartialEq)]
    struct Tag(u32);

    /// Leaf provider: sets a row count and attaches a `Tag` extension.
    #[derive(Debug)]
    struct TagLeafProvider {
        rows: usize,
        tag: u32,
    }
    impl StatisticsProvider for TagLeafProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            if !child_stats.is_empty() {
                return Ok(StatisticsResult::Delegate);
            }
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(self.rows);
            let mut extended = ExtendedStatistics::new(stats);
            extended.set_extension(Tag(self.tag));
            Ok(StatisticsResult::Computed(extended))
        }
    }

    /// Non-leaf provider: re-emits a `Tag` doubled from the first child's `Tag`,
    /// proving the child's extension reached this provider.
    #[derive(Debug)]
    struct TagDoublingProvider;
    impl StatisticsProvider for TagDoublingProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            let Some(Tag(v)) = child_stats.first().and_then(|c| c.get_extension::<Tag>())
            else {
                return Ok(StatisticsResult::Delegate);
            };
            let mut extended =
                ExtendedStatistics::new(Statistics::new_unknown(&plan.schema()));
            extended.set_extension(Tag(v * 2));
            Ok(StatisticsResult::Computed(extended))
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
        assert!(!ctx.cache.borrow().statistics.is_empty());

        let s2 = ctx.compute(leaf.as_ref(), &args).unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn reset_cache_clears_entries() {
        let leaf = make_stats_leaf(10);
        let ctx = StatisticsContext::new();
        let _ = ctx.compute(leaf.as_ref(), &StatisticsArgs::new()).unwrap();
        assert!(!ctx.cache.borrow().statistics.is_empty());
        ctx.reset_cache();
        assert!(ctx.cache.borrow().statistics.is_empty());
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
    fn extensions_reach_parent_provider() {
        let leaf = make_stats_leaf(100);
        let parent: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf));
        let ctx = StatisticsContext::new_with_registry(
            StatisticsRegistry::with_providers(vec![
                Arc::new(TagLeafProvider { rows: 100, tag: 7 }),
                Arc::new(TagDoublingProvider),
            ]),
        );
        let extended = ctx
            .compute_extended(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(extended.get_extension::<Tag>(), Some(&Tag(14)));
    }

    #[test]
    fn builtin_fallback_drops_extensions() {
        let leaf = make_stats_leaf(100);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let ctx = ctx_with(Arc::new(TagLeafProvider { rows: 100, tag: 7 }));

        let leaf_extended = ctx
            .compute_extended(leaf.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(leaf_extended.get_extension::<Tag>(), Some(&Tag(7)));

        let parent_extended = ctx
            .compute_extended(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(parent_extended.get_extension::<Tag>(), None);
        assert_eq!(parent_extended.base().num_rows, Precision::Exact(100));
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
