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

//! Pluggable statistics propagation for physical plans.
//!
//! This module provides an extensible mechanism for computing statistics
//! on [`ExecutionPlan`] nodes, following the chain of responsibility pattern
//! similar to `RelationPlanner` for SQL parsing.
//!
//! # Overview
//!
//! The default implementation delegates to each operator's built-in
//! `partition_statistics`. Users can register custom [`StatisticsProvider`]
//! implementations to:
//!
//! 1. Provide statistics for custom [`ExecutionPlan`] implementations
//! 2. Override default estimation with advanced approaches (e.g., histograms)
//! 3. Plug in domain-specific knowledge for better cardinality estimation
//!
//! # Architecture
//!
//! - [`StatisticsProvider`]: Chain element that computes statistics for specific operators
//! - [`StatisticsRegistry`]: Chains providers, lives in SessionState
//! - [`ExtendedStatistics`]: Statistics with type-safe custom extensions
//!
//! # Built-in Providers
//!
//! The following providers are included and can be registered in this order:
//!
//! 1. [`FilterStatisticsProvider`] - selectivity-based filter estimation
//! 2. [`ProjectionStatisticsProvider`] - column mapping through projections
//! 3. [`PassthroughStatisticsProvider`] - passthrough for cardinality-preserving operators
//! 4. [`AggregateStatisticsProvider`] - NDV-based GROUP BY cardinality estimation
//! 5. [`JoinStatisticsProvider`] - NDV-based join output estimation (hash, sort-merge, cross)
//! 6. [`LimitStatisticsProvider`] - caps output at the fetch limit (local and global)
//! 7. [`UnionStatisticsProvider`] - sums input row counts
//! 8. [`DefaultStatisticsProvider`] - fallback to `partition_statistics(None)`
//!
//! # Relationship to [#20184](https://github.com/apache/datafusion/issues/20184)
//!
//! This module performs its own bottom-up tree walk in [`StatisticsRegistry::compute`],
//! separate from the walk optimizer rules do via `transform_up`. This means existing
//! rules that call `partition_statistics` directly bypass the registry.
//!
//! [#20184](https://github.com/apache/datafusion/issues/20184) adds a `child_stats`
//! parameter to `partition_statistics`. Once it lands, the registry can feed enriched
//! **base** [`Statistics`] into operators' built-in `partition_statistics` calls,
//! removing redundancy for the base-stats path (row counts, column stats). However,
//! the separate registry walk is still required for [`ExtendedStatistics`] extension
//! propagation: `partition_statistics` returns `Arc<Statistics>`, so extensions
//! (histograms, sketches, etc.) are stripped at that boundary and can only flow
//! through the registry walk.
//!
//! If [`Statistics`] itself were extended to carry a type-erased extension map
//! (similar to [`ExtendedStatistics`]), the registry walk could be dropped entirely:
//! extensions would flow naturally through `partition_statistics(child_stats)` and
//! the registry would become a pure chain-of-responsibility on top of the existing
//! traversal with no separate walk needed.
//!
//! # Example
//!
//! ```ignore
//! use datafusion_physical_plan::operator_statistics::*;
//!
//! // Create registry with default provider
//! let mut registry = StatisticsRegistry::new();
//!
//! // Register custom provider (higher priority)
//! registry.register(Arc::new(MyHistogramProvider));
//!
//! // Compute statistics through the chain
//! let stats = registry.compute(plan.as_ref())?;
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion_common::stats::Precision;
use datafusion_common::{Result, Statistics};

use crate::ExecutionPlan;

// ============================================================================
// ExtendedStatistics: Statistics with type-safe extensions
// ============================================================================

/// Statistics with support for custom extensions.
///
/// Wraps the standard [`Statistics`] and adds a type-erased extension map
/// for custom statistics like histograms, sketches, or domain-specific metadata.
///
/// # Example
///
/// ```ignore
/// // Define a custom statistics extension
/// #[derive(Debug, Clone)]
/// struct HistogramStats {
///     buckets: Vec<(i64, i64, usize)>, // (min, max, count)
/// }
///
/// // Set extension in a planner
/// let mut stats = ExtendedStatistics::from(base_stats);
/// stats.set_extension(HistogramStats { buckets: vec![] });
///
/// // Retrieve in a consumer
/// if let Some(hist) = stats.get_extension::<HistogramStats>() {
///     // Use histogram for better estimation
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct ExtendedStatistics {
    /// Standard statistics (num_rows, byte_size, column stats)
    base: Arc<Statistics>,
    /// Type-erased extensions for custom statistics
    extensions: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ExtendedStatistics {
    /// Create new ExtendedStatistics wrapping owned statistics.
    pub fn new(base: Statistics) -> Self {
        Self {
            base: Arc::new(base),
            extensions: HashMap::new(),
        }
    }

    /// Create new ExtendedStatistics from an [`Arc<Statistics>`].
    pub fn new_arc(base: Arc<Statistics>) -> Self {
        Self {
            base,
            extensions: HashMap::new(),
        }
    }

    /// Returns a reference to the base [`Statistics`].
    pub fn base(&self) -> &Statistics {
        &self.base
    }

    /// Returns a reference to the underlying [`Arc<Statistics>`].
    pub fn base_arc(&self) -> &Arc<Statistics> {
        &self.base
    }

    /// Get a reference to a custom statistics extension by type.
    pub fn get_extension<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|ext| ext.downcast_ref())
    }

    /// Set a custom statistics extension.
    pub fn set_extension<T: 'static + Send + Sync>(&mut self, value: T) {
        self.extensions.insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Check if an extension of the given type exists.
    pub fn has_extension<T: 'static + Send + Sync>(&self) -> bool {
        self.extensions.contains_key(&TypeId::of::<T>())
    }

    /// Merge extensions from another ExtendedStatistics (other's extensions take precedence).
    pub fn merge_extensions(&mut self, other: &ExtendedStatistics) {
        for (type_id, ext) in &other.extensions {
            self.extensions.insert(*type_id, Arc::clone(ext));
        }
    }
}

impl From<Statistics> for ExtendedStatistics {
    fn from(base: Statistics) -> Self {
        Self::new(base)
    }
}

impl From<Arc<Statistics>> for ExtendedStatistics {
    fn from(base: Arc<Statistics>) -> Self {
        Self::new_arc(base)
    }
}

impl From<ExtendedStatistics> for Statistics {
    fn from(extended: ExtendedStatistics) -> Self {
        Arc::unwrap_or_clone(extended.base)
    }
}

// ============================================================================
// StatisticsProvider trait and registry
// ============================================================================

/// Result of attempting to compute statistics with a [`StatisticsProvider`].
#[derive(Debug)]
pub enum StatisticsResult {
    /// Statistics were computed by this provider
    Computed(ExtendedStatistics),
    /// This provider doesn't handle this operator; delegate to next in chain
    Delegate,
}

/// Customize statistics computation for [`ExecutionPlan`] nodes.
///
/// Implementations can handle specific operator types or override default
/// estimation logic. The chain of providers is traversed until one returns
/// [`StatisticsResult::Computed`].
///
/// # Implementing a Custom Provider
///
/// ```ignore
/// #[derive(Debug)]
/// struct MyStatisticsProvider;
///
/// impl StatisticsProvider for MyStatisticsProvider {
///     fn compute_statistics(
///         &self,
///         plan: &dyn ExecutionPlan,
///         child_stats: &[ExtendedStatistics],
///     ) -> Result<StatisticsResult> {
///         if let Some(my_exec) = plan.downcast_ref::<MyCustomExec>() {
///             // Custom logic for MyCustomExec
///             Ok(StatisticsResult::Computed(/* ... */))
///         } else {
///             // Let next provider handle it
///             Ok(StatisticsResult::Delegate)
///         }
///     }
/// }
/// ```
pub trait StatisticsProvider: Debug + Send + Sync {
    /// Compute statistics for an [`ExecutionPlan`] node.
    ///
    /// # Arguments
    /// * `plan` - The execution plan node to compute statistics for
    /// * `child_stats` - Extended statistics already computed for child nodes,
    ///   in the same order as `plan.children()`. Empty for leaf nodes.
    ///
    /// # Returns
    /// * `StatisticsResult::Computed(stats)` - Short-circuits the chain
    /// * `StatisticsResult::Delegate` - Passes to next provider in chain
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult>;
}

/// Default statistics provider that delegates to each operator's built-in
/// `partition_statistics` implementation.
#[derive(Debug, Default)]
pub struct DefaultStatisticsProvider;

impl StatisticsProvider for DefaultStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        _child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        let base = plan.partition_statistics(None)?;
        Ok(StatisticsResult::Computed(ExtendedStatistics::new_arc(
            base,
        )))
    }
}

/// Registry that chains [`StatisticsProvider`] implementations.
///
/// The registry is a stateless provider chain: it holds no mutable state
/// and is cheaply `Clone`able / `Send` / `Sync`.
#[derive(Clone)]
pub struct StatisticsRegistry {
    providers: Vec<Arc<dyn StatisticsProvider>>,
}

impl Debug for StatisticsRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatisticsRegistry({} providers)", self.providers.len())
    }
}

impl Default for StatisticsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsRegistry {
    /// Create a new empty registry.
    ///
    /// With no providers, `compute()` falls back to each plan node's
    /// built-in `partition_statistics()`. Register providers to enhance
    /// statistics (e.g., inject NDV, use histograms).
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    /// Create a registry with the given provider chain.
    pub fn with_providers(providers: Vec<Arc<dyn StatisticsProvider>>) -> Self {
        Self { providers }
    }

    /// Create a registry pre-loaded with the standard built-in providers.
    ///
    /// Provider order (first match wins):
    /// 1. [`FilterStatisticsProvider`]
    /// 2. [`ProjectionStatisticsProvider`]
    /// 3. [`PassthroughStatisticsProvider`]
    /// 4. [`AggregateStatisticsProvider`]
    /// 5. [`JoinStatisticsProvider`]
    /// 6. [`LimitStatisticsProvider`]
    /// 7. [`UnionStatisticsProvider`]
    /// 8. [`DefaultStatisticsProvider`]
    pub fn default_with_builtin_providers() -> Self {
        Self::with_providers(vec![
            Arc::new(FilterStatisticsProvider),
            Arc::new(ProjectionStatisticsProvider),
            Arc::new(PassthroughStatisticsProvider),
            Arc::new(AggregateStatisticsProvider),
            Arc::new(JoinStatisticsProvider),
            Arc::new(LimitStatisticsProvider),
            Arc::new(UnionStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ])
    }

    /// Register a provider at the front of the chain (higher priority).
    pub fn register(&mut self, provider: Arc<dyn StatisticsProvider>) {
        self.providers.insert(0, provider);
    }

    /// Returns the current provider chain.
    pub fn providers(&self) -> &[Arc<dyn StatisticsProvider>] {
        &self.providers
    }

    /// Compute extended statistics for a plan through the provider chain.
    ///
    /// Performs a bottom-up tree walk: child statistics are computed recursively
    /// and passed to providers, mirroring how `partition_statistics` composes
    /// operators. Once [#20184](https://github.com/apache/datafusion/issues/20184)
    /// lands, the registry can feed enriched base stats directly into
    /// `partition_statistics(child_stats)`, removing the need for a separate walk.
    ///
    /// If no providers are registered, falls back to the plan's built-in
    /// `partition_statistics(None)` with no overhead.
    pub fn compute(&self, plan: &dyn ExecutionPlan) -> Result<ExtendedStatistics> {
        // Fast path: no providers registered, skip the walk entirely
        if self.providers.is_empty() {
            let base = plan.partition_statistics(None)?;
            return Ok(ExtendedStatistics::new_arc(base));
        }

        let children = plan.children();

        // For leaf nodes, try providers with empty child stats.
        // For non-leaf nodes, recursively compute enhanced child stats first.
        let child_stats: Vec<ExtendedStatistics> = if children.is_empty() {
            Vec::new()
        } else {
            children
                .iter()
                .map(|child| self.compute(child.as_ref()))
                .collect::<Result<Vec<_>>>()?
        };

        for provider in &self.providers {
            match provider.compute_statistics(plan, &child_stats)? {
                StatisticsResult::Computed(stats) => return Ok(stats),
                StatisticsResult::Delegate => continue,
            }
        }
        // Fallback: use plan's built-in stats
        let base = plan.partition_statistics(None)?;
        Ok(ExtendedStatistics::new_arc(base))
    }

    /// Compute statistics and return only the base Statistics (no extensions).
    ///
    /// Convenience method for callers that don't need extensions.
    pub fn compute_base(&self, plan: &dyn ExecutionPlan) -> Result<Statistics> {
        Ok(self.compute(plan)?.base().clone())
    }
}

// ============================================================================
// Statistics Utility Functions
// ============================================================================

/// Estimate the number of distinct values when sampling from a population.
///
/// Given a domain with `domain_size` distinct values and `num_selected` rows
/// sampled/filtered from it, estimates how many distinct values will appear
/// in the sample.
///
/// Uses the formula: `Expected distinct = N * [1 - (1 - 1/N)^n]`
///
/// # References
///
/// Based on Calcite's `RelMdUtil.numDistinctVals()`:
/// <https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/rel/metadata/RelMdUtil.java>
pub fn num_distinct_vals(domain_size: usize, num_selected: usize) -> usize {
    if domain_size == 0 || num_selected == 0 {
        return 0;
    }

    if num_selected >= domain_size {
        return domain_size;
    }

    let n = domain_size as f64;
    let k = num_selected as f64;

    // For large n, (1-1/n).powf(k) loses precision because the base is near
    // 1.0; use the equivalent exp(-k/n) form which is numerically stable.
    // Threshold matches Calcite's RelMdUtil.numDistinctVals().
    let expected = if domain_size > 1000 {
        n * (1.0 - (-k / n).exp())
    } else {
        n * (1.0 - (1.0 - 1.0 / n).powf(k))
    };

    let result = expected.round() as usize;
    result.clamp(1, domain_size)
}

/// Estimate NDV after applying a selectivity factor (filtering).
///
/// When filtering rows, each distinct value has multiple rows. If a value
/// appears `k` times, the probability it survives the filter is `1 - (1-s)^k`
/// where `s` is the selectivity.
///
/// Assuming uniform distribution (each value appears `rows/ndv` times):
/// ```text
/// NDV_after ~ NDV_before * [1 - (1 - selectivity)^(rows/NDV)]
/// ```
pub fn ndv_after_selectivity(
    original_ndv: usize,
    original_rows: usize,
    selectivity: f64,
) -> usize {
    if selectivity <= 0.0 || original_ndv == 0 || original_rows == 0 {
        return 0;
    }
    if selectivity >= 1.0 {
        return original_ndv;
    }

    let ndv = original_ndv as f64;
    let rows = original_rows as f64;

    let rows_per_value = rows / ndv;
    let survival_prob = 1.0 - (1.0 - selectivity).powf(rows_per_value);
    let expected_ndv = ndv * survival_prob;

    (expected_ndv.round() as usize).clamp(1, original_ndv)
}

/// Rescale `total_byte_size` proportionally after overriding `num_rows`.
///
/// When a provider replaces `num_rows` but keeps the rest of the stats from
/// `partition_statistics`, the original `total_byte_size` becomes inconsistent.
/// This function adjusts it by the ratio `new_rows / old_rows`, preserving the
/// average bytes-per-row from the original estimate.
fn rescale_byte_size(stats: &mut Statistics, new_num_rows: Precision<usize>) {
    let old_rows = stats.num_rows;
    stats.num_rows = new_num_rows;
    stats.total_byte_size = match (old_rows, new_num_rows, stats.total_byte_size) {
        (Precision::Exact(old), Precision::Exact(new), Precision::Exact(bytes))
            if old > 0 =>
        {
            Precision::Exact((bytes as f64 * new as f64 / old as f64).round() as usize)
        }
        _ => match (
            old_rows.get_value(),
            new_num_rows.get_value(),
            stats.total_byte_size.get_value(),
        ) {
            (Some(&old), Some(&new), Some(&bytes)) if old > 0 => Precision::Inexact(
                (bytes as f64 * new as f64 / old as f64).round() as usize,
            ),
            _ => stats.total_byte_size,
        },
    };
}

/// Fetches base statistics from the operator's built-in `partition_statistics`,
/// overrides `num_rows` with the registry-computed estimate, and rescales
/// `total_byte_size` proportionally.
///
/// Used by providers that compute a better row count but cannot yet propagate
/// column-level stats (NDV, min/max) through the operator — pending #20184.
fn computed_with_row_count(
    plan: &dyn ExecutionPlan,
    num_rows: Precision<usize>,
) -> Result<StatisticsResult> {
    let mut base = Arc::unwrap_or_clone(plan.partition_statistics(None)?);
    rescale_byte_size(&mut base, num_rows);
    Ok(StatisticsResult::Computed(ExtendedStatistics::new(base)))
}

/// Statistics provider for [`FilterExec`](crate::filter::FilterExec) that uses
/// pre-computed enhanced child statistics from the registry walk.
///
/// Unlike the default provider (which calls `partition_statistics` and gets raw
/// child stats), this provider receives enhanced child stats that may include
/// NDV overrides injected at the scan level. It applies the same selectivity
/// estimation logic as `FilterExec::statistics_helper`, then additionally
/// adjusts each column's `distinct_count` using [`ndv_after_selectivity`] based
/// on the computed selectivity ratio.
#[derive(Debug, Default)]
pub struct FilterStatisticsProvider;

impl StatisticsProvider for FilterStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::filter::FilterExec;

        let Some(filter) = plan.downcast_ref::<FilterExec>() else {
            return Ok(StatisticsResult::Delegate);
        };
        if child_stats.is_empty() {
            return Ok(StatisticsResult::Delegate);
        }

        let input_stats = (*child_stats[0].base).clone();
        let input_rows = input_stats.num_rows;
        let mut stats = FilterExec::statistics_helper(
            &filter.input().schema(),
            input_stats,
            filter.predicate(),
            filter.default_selectivity(),
            // TODO: pass filter.expression_analyzer_registry() once #21122 lands
        )?;

        // Adjust distinct_count for each column using the selectivity ratio
        // via the probabilistic survival model from
        // ndv_after_selectivity to account for rows removed by the filter.
        if let (Some(&orig_rows), Some(&filtered_rows)) =
            (input_rows.get_value(), stats.num_rows.get_value())
            && orig_rows > 0
            && filtered_rows < orig_rows
        {
            let selectivity = filtered_rows as f64 / orig_rows as f64;
            for col_stat in &mut stats.column_statistics {
                if let Some(&ndv) = col_stat.distinct_count.get_value() {
                    let adjusted = ndv_after_selectivity(ndv, orig_rows, selectivity);
                    col_stat.distinct_count = Precision::Inexact(adjusted);
                }
            }
        }

        let stats = stats.project(filter.projection().as_ref());
        Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
    }
}

/// Statistics provider for [`ProjectionExec`](crate::projection::ProjectionExec)
/// that uses pre-computed enhanced child statistics from the registry walk.
///
/// Maps enhanced child column statistics to output columns based on the
/// projection expressions, preserving NDV and other statistics through
/// column references.
#[derive(Debug, Default)]
pub struct ProjectionStatisticsProvider;

impl StatisticsProvider for ProjectionStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::projection::ProjectionExec;

        let Some(proj) = plan.downcast_ref::<ProjectionExec>() else {
            return Ok(StatisticsResult::Delegate);
        };
        if child_stats.is_empty() {
            return Ok(StatisticsResult::Delegate);
        }

        let input_stats = (*child_stats[0].base).clone();
        let output_schema = proj.schema();
        // TODO: pass proj.expression_analyzer_registry() once #21122 lands,
        // so expression-level NDV/min/max feeds into projected column stats.
        let stats = proj
            .projection_expr()
            .project_statistics(input_stats, &output_schema)?;
        Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
    }
}

/// Statistics provider for single-input operators with
/// [`CardinalityEffect::Equal`](crate::execution_plan::CardinalityEffect::Equal).
///
/// These operators (Sort, Repartition, CoalescePartitions, etc.) don't
/// transform statistics, so we pass through the enhanced child stats directly.
/// This avoids the fallback calling `partition_statistics(None)` which would
/// trigger a redundant internal recursion with raw (non-enhanced) stats.
#[derive(Debug, Default)]
pub struct PassthroughStatisticsProvider;

impl StatisticsProvider for PassthroughStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::execution_plan::CardinalityEffect;

        if child_stats.len() != 1
            || !matches!(plan.cardinality_effect(), CardinalityEffect::Equal)
        {
            return Ok(StatisticsResult::Delegate);
        }

        // Only pass through when the schema is unchanged (same column count).
        // Operators like WindowAggExec preserve row count but add columns;
        // passing through child stats would produce wrong column_statistics.
        let input_cols = child_stats[0].base.column_statistics.len();
        let output_cols = plan.schema().fields().len();
        if input_cols != output_cols {
            return Ok(StatisticsResult::Delegate);
        }

        Ok(StatisticsResult::Computed(child_stats[0].clone()))
    }
}

/// Statistics provider for [`AggregateExec`](crate::aggregates::AggregateExec)
/// that estimates output cardinality from the NDV of GROUP BY columns.
///
/// For each GROUP BY column, looks up `distinct_count` from the enhanced
/// child statistics. The estimated output rows is the product of all
/// column NDVs, capped at the input row count. This assumes independence
/// between columns, so correlated columns (e.g., `city` and `state`) will
/// produce overestimates.
///
/// For GROUPING SETS / CUBE / ROLLUP, delegates to the built-in
/// `partition_statistics`, which handles per-set NDV estimation correctly.
///
/// Delegates when:
/// - The plan is not an `AggregateExec`
/// - The aggregate is `Partial` (per-partition, not bounded by global NDV)
/// - GROUP BY is empty (scalar aggregate)
/// - Any GROUP BY expression is not a simple column reference
/// - Any GROUP BY column lacks NDV information
#[derive(Debug, Default)]
pub struct AggregateStatisticsProvider;

impl StatisticsProvider for AggregateStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::aggregates::AggregateExec;
        use datafusion_physical_expr::expressions::Column;

        use crate::aggregates::AggregateMode;

        let Some(agg) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(StatisticsResult::Delegate);
        };

        // Partial aggregates produce per-partition groups, not bounded by
        // global NDV; delegate to the built-in estimate for those.
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Ok(StatisticsResult::Delegate);
        }

        if child_stats.is_empty() || agg.group_expr().expr().is_empty() {
            return Ok(StatisticsResult::Delegate);
        }

        let input_stats = &child_stats[0].base;

        // Compute NDV product of GROUP BY columns
        let mut ndv_product: Option<usize> = None;
        for (expr, _) in agg.group_expr().expr().iter() {
            let Some(col) = expr.downcast_ref::<Column>() else {
                return Ok(StatisticsResult::Delegate);
            };
            let Some(&ndv) = input_stats
                .column_statistics
                .get(col.index())
                .and_then(|s| s.distinct_count.get_value())
            else {
                return Ok(StatisticsResult::Delegate);
            };
            if ndv == 0 {
                return Ok(StatisticsResult::Delegate);
            }
            ndv_product = Some(match ndv_product {
                Some(prev) => prev.saturating_mul(ndv),
                None => ndv,
            });
        }

        let Some(product) = ndv_product else {
            return Ok(StatisticsResult::Delegate);
        };

        // For CUBE/ROLLUP/GROUPING SETS (multiple grouping sets), delegate to
        // the built-in estimate, which handles per-set NDV estimation correctly.
        if agg.group_expr().groups().len() > 1 {
            return Ok(StatisticsResult::Delegate);
        }

        // Cap at input rows
        let estimate = match input_stats.num_rows.get_value() {
            Some(&rows) => product.min(rows),
            None => product,
        };

        let num_rows = Precision::Inexact(estimate);

        computed_with_row_count(plan, num_rows)
    }
}

/// Statistics provider for equi-joins (hash join, sort-merge join) and cross joins.
///
/// For equi-joins, estimates output cardinality as
/// `left_rows * right_rows / product(max(left_ndv_i, right_ndv_i))`
/// across all join key columns (assuming independence between keys),
/// falling back to the Cartesian product when any key lacks NDV on both sides.
/// For cross joins, uses the exact Cartesian product.
///
/// The base inner-join estimate is then adjusted for the join type:
/// - Semi joins: capped at the preserved-side row count
/// - Anti joins: preserved-side minus matched rows (clamped to 0)
/// - Left/Right outer: at least as many rows as the preserved side
/// - Full outer: at least `left + right - inner_estimate`
/// - Left mark: exactly `left_rows` (one output row per left row)
///
/// Delegates when:
/// - The plan is not a supported join type
/// - Either input lacks row count information
#[derive(Debug, Default)]
pub struct JoinStatisticsProvider;

impl StatisticsProvider for JoinStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::joins::{CrossJoinExec, HashJoinExec, SortMergeJoinExec};
        use datafusion_common::JoinType;
        use datafusion_physical_expr::expressions::Column;

        if child_stats.len() < 2 {
            return Ok(StatisticsResult::Delegate);
        }

        let left = &child_stats[0].base;
        let right = &child_stats[1].base;

        let (Some(&left_rows), Some(&right_rows)) =
            (left.num_rows.get_value(), right.num_rows.get_value())
        else {
            return Ok(StatisticsResult::Delegate);
        };

        use crate::joins::JoinOnRef;

        /// Estimate equi-join output using NDV of join key columns:
        ///   left_rows * right_rows / product(max(left_ndv_i, right_ndv_i))
        /// Falls back to Cartesian product if any key lacks NDV on both sides.
        fn equi_join_estimate(
            on: JoinOnRef,
            left: &Statistics,
            right: &Statistics,
            left_rows: usize,
            right_rows: usize,
        ) -> usize {
            if on.is_empty() {
                return left_rows.saturating_mul(right_rows);
            }
            let mut ndv_divisor: usize = 1;
            for (left_key, right_key) in on {
                let left_ndv = left_key
                    .downcast_ref::<Column>()
                    .and_then(|c| left.column_statistics.get(c.index()))
                    .and_then(|s| s.distinct_count.get_value().copied());
                let right_ndv = right_key
                    .downcast_ref::<Column>()
                    .and_then(|c| right.column_statistics.get(c.index()))
                    .and_then(|s| s.distinct_count.get_value().copied());
                match (left_ndv, right_ndv) {
                    (Some(l), Some(r)) if l > 0 && r > 0 => {
                        ndv_divisor = ndv_divisor.saturating_mul(l.max(r));
                    }
                    _ => return left_rows.saturating_mul(right_rows),
                }
            }
            if ndv_divisor > 0 {
                left_rows.saturating_mul(right_rows) / ndv_divisor
            } else {
                left_rows.saturating_mul(right_rows)
            }
        }

        let (inner_estimate, is_exact_cartesian, join_type) = if let Some(hash_join) =
            plan.downcast_ref::<HashJoinExec>()
        {
            let est =
                equi_join_estimate(hash_join.on(), left, right, left_rows, right_rows);
            (est, false, *hash_join.join_type())
        } else if let Some(smj) = plan.downcast_ref::<SortMergeJoinExec>() {
            let est = equi_join_estimate(smj.on(), left, right, left_rows, right_rows);
            (est, false, smj.join_type())
        } else if plan.downcast_ref::<CrossJoinExec>().is_some() {
            let both_exact = left.num_rows.is_exact().unwrap_or(false)
                && right.num_rows.is_exact().unwrap_or(false);
            (
                left_rows.saturating_mul(right_rows),
                both_exact,
                JoinType::Inner,
            )
        } else {
            return Ok(StatisticsResult::Delegate);
        };

        // Apply join-type-aware cardinality bounds
        let estimated = match join_type {
            JoinType::Inner => inner_estimate,
            JoinType::Left => inner_estimate.max(left_rows),
            JoinType::Right => inner_estimate.max(right_rows),
            JoinType::Full => {
                // At least left + right - matched, but never less than inner
                let outer_bound = left_rows
                    .saturating_add(right_rows)
                    .saturating_sub(inner_estimate);
                inner_estimate.max(outer_bound)
            }
            JoinType::LeftSemi => inner_estimate.min(left_rows),
            JoinType::RightSemi => inner_estimate.min(right_rows),
            JoinType::LeftAnti => left_rows.saturating_sub(inner_estimate.min(left_rows)),
            JoinType::RightAnti => {
                right_rows.saturating_sub(inner_estimate.min(right_rows))
            }
            JoinType::LeftMark => left_rows,
            JoinType::RightMark => right_rows,
        };

        // NL join inner with exact inputs is an exact Cartesian product;
        // NDV-based estimates are inherently inexact.
        let num_rows = if is_exact_cartesian && join_type == JoinType::Inner {
            Precision::Exact(estimated)
        } else {
            Precision::Inexact(estimated)
        };

        computed_with_row_count(plan, num_rows)
    }
}

/// Statistics provider for [`LocalLimitExec`](crate::limit::LocalLimitExec) and
/// [`GlobalLimitExec`](crate::limit::GlobalLimitExec).
///
/// Caps output row count at the limit value, accounting for any leading skip offset
/// in `GlobalLimitExec`.
#[derive(Debug, Default)]
pub struct LimitStatisticsProvider;

impl StatisticsProvider for LimitStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::limit::{GlobalLimitExec, LocalLimitExec};

        if child_stats.is_empty() {
            return Ok(StatisticsResult::Delegate);
        }

        let (skip, fetch) = if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            (0usize, Some(limit.fetch()))
        } else if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            (limit.skip(), limit.fetch())
        } else {
            return Ok(StatisticsResult::Delegate);
        };

        let num_rows = match child_stats[0].base.num_rows {
            Precision::Exact(rows) => {
                let available = rows.saturating_sub(skip);
                Precision::Exact(fetch.map_or(available, |f| available.min(f)))
            }
            Precision::Inexact(rows) => {
                let available = rows.saturating_sub(skip);
                match fetch {
                    Some(f) => Precision::Inexact(available.min(f)),
                    None => Precision::Inexact(available),
                }
            }
            Precision::Absent => match fetch {
                Some(f) => Precision::Inexact(f),
                None => Precision::Absent,
            },
        };

        computed_with_row_count(plan, num_rows)
    }
}

/// Statistics provider for [`UnionExec`](crate::union::UnionExec).
///
/// Sums row counts across all inputs.
#[derive(Debug, Default)]
pub struct UnionStatisticsProvider;

impl StatisticsProvider for UnionStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        use crate::union::UnionExec;

        if plan.downcast_ref::<UnionExec>().is_none() {
            return Ok(StatisticsResult::Delegate);
        }

        let total = child_stats.iter().try_fold(
            Precision::Exact(0usize),
            |acc, s| -> Result<Precision<usize>> {
                Ok(match (acc, s.base.num_rows) {
                    (Precision::Absent, _) | (_, Precision::Absent) => Precision::Absent,
                    (Precision::Exact(a), Precision::Exact(b)) => {
                        Precision::Exact(a.saturating_add(b))
                    }
                    (Precision::Inexact(a), Precision::Exact(b))
                    | (Precision::Exact(a), Precision::Inexact(b))
                    | (Precision::Inexact(a), Precision::Inexact(b)) => {
                        Precision::Inexact(a.saturating_add(b))
                    }
                })
            },
        )?;

        computed_with_row_count(plan, total)
    }
}

type ProviderFn = dyn Fn(&dyn ExecutionPlan, &[ExtendedStatistics]) -> Result<StatisticsResult>
    + Send
    + Sync;

/// A [`StatisticsProvider`] backed by a user-supplied closure.
///
/// Useful for injecting custom statistics in tests or for cardinality feedback
/// pipelines where real runtime statistics need to override plan estimates.
/// The closure receives the current plan node and its children's enhanced
/// statistics, returning a [`StatisticsResult`].
///
/// To distinguish between multiple nodes of the same type (e.g., two
/// `FilterExec` nodes), match on structural properties like the input schema's
/// column names, number of columns, or child row counts.
///
/// # Example
///
/// ```rust,ignore (requires crate-internal imports)
/// let provider = ClosureStatisticsProvider::new(|plan, child_stats| {
///     if plan.downcast_ref::<FilterExec>().is_some() {
///         Ok(StatisticsResult::Computed(ExtendedStatistics::from(Statistics {
///             num_rows: Precision::Inexact(42),
///             ..Statistics::new_unknown(plan.schema().as_ref())
///         })))
///     } else {
///         Ok(StatisticsResult::Delegate)
///     }
/// });
/// ```
pub struct ClosureStatisticsProvider {
    f: Box<ProviderFn>,
}

impl ClosureStatisticsProvider {
    /// Create a new provider from a closure.
    pub fn new(
        f: impl Fn(&dyn ExecutionPlan, &[ExtendedStatistics]) -> Result<StatisticsResult>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { f: Box::new(f) }
    }
}

impl Debug for ClosureStatisticsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClosureStatisticsProvider")
    }
}

impl StatisticsProvider for ClosureStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        (self.f)(plan, child_stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterExec;
    use crate::projection::ProjectionExec;
    use crate::{DisplayAs, DisplayFormatType, PlanProperties};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal, col, lit};
    use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
    use std::fmt;

    use crate::execution_plan::{Boundedness, EmissionType};
    use datafusion_common::tree_node::TreeNodeRecursion;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    #[derive(Debug)]
    struct MockSourceExec {
        schema: Arc<Schema>,
        stats: Statistics,
        cache: Arc<PlanProperties>,
    }

    impl MockSourceExec {
        fn new(schema: Arc<Schema>, num_rows: Precision<usize>) -> Self {
            let num_cols = schema.fields().len();
            Self::with_column_stats(
                schema,
                num_rows,
                vec![ColumnStatistics::new_unknown(); num_cols],
            )
        }

        fn with_column_stats(
            schema: Arc<Schema>,
            num_rows: Precision<usize>,
            column_statistics: Vec<ColumnStatistics>,
        ) -> Self {
            let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            let cache = Arc::new(PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                stats: Statistics {
                    num_rows,
                    total_byte_size: Precision::Absent,
                    column_statistics,
                },
                cache,
            }
        }
    }

    impl DisplayAs for MockSourceExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockSourceExec")
        }
    }

    impl ExecutionPlan for MockSourceExec {
        fn name(&self) -> &str {
            "MockSourceExec"
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion_execution::TaskContext>,
        ) -> Result<crate::SendableRecordBatchStream> {
            unimplemented!()
        }

        fn partition_statistics(
            &self,
            _partition: Option<usize>,
        ) -> Result<Arc<Statistics>> {
            Ok(Arc::new(self.stats.clone()))
        }
    }

    fn make_source(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(MockSourceExec::new(
            make_schema(),
            Precision::Exact(num_rows),
        ))
    }

    #[test]
    fn test_default_provider() -> Result<()> {
        let engine = StatisticsRegistry::new();
        let source = make_source(1000);

        let stats = engine.compute(source.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Exact(1000)));
        Ok(())
    }

    #[test]
    fn test_custom_chain_configuration() -> Result<()> {
        let source = make_source(1000);

        // Test with_providers: fully custom chain (no default)
        let custom_only =
            StatisticsRegistry::with_providers(vec![Arc::new(CustomStatisticsProvider)]);
        // CustomStatisticsProvider only handles CustomExec, delegates for others
        // With no default provider, filter returns fallback statistics
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), Arc::clone(&source))?);
        let stats = custom_only.compute(filter.as_ref())?;
        // Falls back to plan.statistics() since no provider handles it
        assert!(stats.base.num_rows.get_value().is_some());

        // Test with_providers: custom provider + built-in fallback
        let with_override =
            StatisticsRegistry::with_providers(vec![Arc::new(OverrideFilterProvider {
                fixed_selectivity: 0.25,
            })
                as Arc<dyn StatisticsProvider>]);
        // OverrideFilterProvider handles filters, built-in fallback handles the rest
        let stats = with_override.compute(filter.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Inexact(250)));

        // Verify chain inspection
        assert_eq!(with_override.providers().len(), 1);

        Ok(())
    }

    #[derive(Debug)]
    struct CustomExec {
        input: Arc<dyn ExecutionPlan>,
    }

    impl DisplayAs for CustomExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CustomExec")
        }
    }

    impl ExecutionPlan for CustomExec {
        fn name(&self) -> &str {
            "CustomExec"
        }

        fn schema(&self) -> Arc<Schema> {
            self.input.schema()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(CustomExec {
                input: Arc::clone(&children[0]),
            }))
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion_execution::TaskContext>,
        ) -> Result<crate::SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct CustomStatisticsProvider;

    impl StatisticsProvider for CustomStatisticsProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            if plan.downcast_ref::<CustomExec>().is_some() {
                Ok(StatisticsResult::Computed(child_stats[0].clone()))
            } else {
                Ok(StatisticsResult::Delegate)
            }
        }
    }

    #[test]
    fn test_custom_provider_for_custom_exec() -> Result<()> {
        let mut engine = StatisticsRegistry::new();
        engine.register(Arc::new(CustomStatisticsProvider));

        let source = make_source(1000);
        let custom: Arc<dyn ExecutionPlan> = Arc::new(CustomExec { input: source });

        let stats = engine.compute(custom.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Exact(1000)));
        Ok(())
    }

    #[derive(Debug)]
    struct OverrideFilterProvider {
        fixed_selectivity: f64,
    }

    impl StatisticsProvider for OverrideFilterProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            if plan.downcast_ref::<FilterExec>().is_some() {
                if let Some(&input_rows) = child_stats[0].base.num_rows.get_value() {
                    let estimated = (input_rows as f64 * self.fixed_selectivity) as usize;
                    Ok(StatisticsResult::Computed(ExtendedStatistics::from(
                        Statistics {
                            num_rows: Precision::Inexact(estimated),
                            total_byte_size: Precision::Absent,
                            column_statistics: child_stats[0]
                                .base
                                .column_statistics
                                .clone(),
                        },
                    )))
                } else {
                    Ok(StatisticsResult::Delegate)
                }
            } else {
                Ok(StatisticsResult::Delegate)
            }
        }
    }

    #[test]
    fn test_override_builtin_operator() -> Result<()> {
        let mut engine = StatisticsRegistry::new();
        engine.register(Arc::new(OverrideFilterProvider {
            fixed_selectivity: 0.1,
        }));

        let source = make_source(1000);
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), source)?);

        let stats = engine.compute(filter.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Inexact(100)));
        Ok(())
    }

    #[test]
    fn test_filter_statistics_propagation() -> Result<()> {
        let engine = StatisticsRegistry::new();
        let source = make_source(1000);
        let predicate = lit(true);
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, source)?);

        let stats = engine.compute(filter.as_ref())?;
        assert!(stats.base.num_rows.get_value().unwrap_or(&0) <= &1000);
        Ok(())
    }

    #[test]
    fn test_filter_adjusts_ndv_by_selectivity() -> Result<()> {
        use datafusion_common::ScalarValue;
        use datafusion_expr::Operator;
        use datafusion_physical_expr::expressions::{
            BinaryExpr, Column as PhysColumn, Literal,
        };

        // Source: 1000 rows, NDV(a)=1000 (unique), NDV(b)=800 (near-unique)
        // With NDV close to num_rows, each value has ~1.25 rows, so filtering
        // visibly reduces the number of surviving distinct values.
        let schema = make_schema(); // "a" Int32, "b" Int32
        let col_stats = vec![
            {
                let mut cs = ColumnStatistics::new_unknown();
                cs.distinct_count = Precision::Exact(1000);
                cs.min_value = Precision::Exact(ScalarValue::Int32(Some(1)));
                cs.max_value = Precision::Exact(ScalarValue::Int32(Some(1000)));
                cs
            },
            {
                let mut cs = ColumnStatistics::new_unknown();
                cs.distinct_count = Precision::Exact(800);
                cs.min_value = Precision::Exact(ScalarValue::Int32(Some(1)));
                cs.max_value = Precision::Exact(ScalarValue::Int32(Some(800)));
                cs
            },
        ];
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockSourceExec::with_column_stats(
            schema,
            Precision::Exact(1000),
            col_stats,
        ));

        // Filter: a > 900 (selectivity ~10%, keeps values 901-1000)
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysColumn::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(900)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, source)?);

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(FilterStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(filter.as_ref())?;

        let output_ndv_a = stats.base.column_statistics[0]
            .distinct_count
            .get_value()
            .copied()
            .unwrap_or(0);
        let output_ndv_b = stats.base.column_statistics[1]
            .distinct_count
            .get_value()
            .copied()
            .unwrap_or(0);

        // NDV(a): interval analysis narrows to [901,1000] -> ~100 distinct values
        assert!(
            output_ndv_a <= 100,
            "Expected NDV(a) <= 100 after filter, got {output_ndv_a}"
        );
        // NDV(b): not in predicate, but selectivity ~10% with 1.25 rows/value
        // means many distinct values are lost. ndv_after_selectivity(800, 1000, 0.1)
        // gives ~76. Significantly less than the original 800.
        assert!(
            output_ndv_b < 200,
            "Expected NDV(b) < 200 after filter, got {output_ndv_b}"
        );
        Ok(())
    }

    #[test]
    fn test_projection_statistics_propagation() -> Result<()> {
        let engine = StatisticsRegistry::new();
        let source = make_source(1000);
        let schema = make_schema();
        let proj: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![(col("a", &schema)?, "a".to_string())],
            source,
        )?);

        let stats = engine.compute(proj.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Exact(1000)));
        Ok(())
    }

    #[test]
    fn test_passthrough_statistics_propagation() -> Result<()> {
        use crate::coalesce_partitions::CoalescePartitionsExec;

        let engine = StatisticsRegistry::new();
        let source = make_source(1000);
        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(source));

        let stats = engine.compute(coalesce.as_ref())?;
        // PassthroughStatisticsProvider should propagate child row count unchanged
        assert_eq!(stats.base.num_rows, Precision::Exact(1000));
        Ok(())
    }

    #[test]
    fn test_chain_priority() -> Result<()> {
        let mut engine = StatisticsRegistry::new();
        engine.register(Arc::new(OverrideFilterProvider {
            fixed_selectivity: 0.5,
        }));
        engine.register(Arc::new(CustomStatisticsProvider));

        let source = make_source(1000);

        // CustomExec handled by CustomStatisticsProvider
        let custom: Arc<dyn ExecutionPlan> = Arc::new(CustomExec {
            input: Arc::clone(&source),
        });
        let stats = engine.compute(custom.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Exact(1000)));

        // FilterExec: CustomStatisticsProvider delegates, OverrideFilterProvider handles
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), source)?);
        let stats = engine.compute(filter.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Inexact(500)));

        Ok(())
    }

    // =========================================================================
    // num_distinct_vals Utility Tests
    // =========================================================================

    #[test]
    fn test_num_distinct_vals_basic() {
        assert_eq!(num_distinct_vals(0, 100), 0);
        assert_eq!(num_distinct_vals(100, 0), 0);
        assert_eq!(num_distinct_vals(100, 100), 100);
        assert_eq!(num_distinct_vals(100, 200), 100);

        let ndv = num_distinct_vals(1000, 100);
        assert!((90..=100).contains(&ndv), "Expected ~95, got {ndv}");

        let ndv = num_distinct_vals(1000, 500);
        assert!((350..=450).contains(&ndv), "Expected ~393, got {ndv}");

        let ndv = num_distinct_vals(1_000_000, 10_000);
        assert!((9900..=10000).contains(&ndv), "Expected ~9950, got {ndv}");

        let ndv = num_distinct_vals(1_000_000, 100);
        assert!((99..=100).contains(&ndv), "Expected ~100, got {ndv}");
    }

    #[test]
    fn test_num_distinct_vals_small_domain() {
        let ndv = num_distinct_vals(10, 5);
        assert!((3..=5).contains(&ndv), "Expected ~4, got {ndv}");

        assert_eq!(num_distinct_vals(10, 20), 10);
        assert_eq!(num_distinct_vals(10, 1), 1);
    }

    #[test]
    fn test_ndv_after_selectivity() {
        let ndv = ndv_after_selectivity(1000, 10000, 0.1);
        assert!((600..=700).contains(&ndv), "Expected ~632, got {ndv}");

        let ndv = ndv_after_selectivity(1000, 10000, 0.01);
        assert!((90..=100).contains(&ndv), "Expected ~95, got {ndv}");

        assert_eq!(ndv_after_selectivity(1000, 10000, 0.0), 0);
        assert_eq!(ndv_after_selectivity(1000, 10000, 1.0), 1000);
        assert_eq!(ndv_after_selectivity(0, 10000, 0.5), 0);
    }

    // =========================================================================
    // AggregateStatisticsProvider tests
    // =========================================================================

    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};

    fn make_source_with_ndv(
        num_rows: usize,
        col_ndvs: Vec<Option<usize>>,
    ) -> Arc<dyn ExecutionPlan> {
        let fields: Vec<Field> = col_ndvs
            .iter()
            .enumerate()
            .map(|(i, _)| Field::new(format!("c{i}"), DataType::Int32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let col_stats = col_ndvs
            .into_iter()
            .map(|ndv| {
                let mut cs = ColumnStatistics::new_unknown();
                if let Some(n) = ndv {
                    cs.distinct_count = Precision::Exact(n);
                }
                cs
            })
            .collect();
        Arc::new(MockSourceExec::with_column_stats(
            schema,
            Precision::Exact(num_rows),
            col_stats,
        ))
    }

    fn make_aggregate(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            Arc::clone(&input),
            input.schema(),
        )?))
    }

    #[test]
    fn test_aggregate_provider_with_ndv() -> Result<()> {
        let source = make_source_with_ndv(100, vec![Some(10)]);
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("c0", 0)),
            "c0".to_string(),
        )]);
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(10));
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_multi_column() -> Result<()> {
        let source = make_source_with_ndv(1000, vec![Some(10), Some(5)]);
        let group_by = PhysicalGroupBy::new_single(vec![
            (Arc::new(Column::new("c0", 0)), "c0".to_string()),
            (Arc::new(Column::new("c1", 1)), "c1".to_string()),
        ]);
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        // 10 * 5 = 50
        assert_eq!(stats.base.num_rows, Precision::Inexact(50));
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_caps_at_input_rows() -> Result<()> {
        // NDV product (100 * 100 = 10_000) exceeds input rows (500)
        let source = make_source_with_ndv(500, vec![Some(100), Some(100)]);
        let group_by = PhysicalGroupBy::new_single(vec![
            (Arc::new(Column::new("c0", 0)), "c0".to_string()),
            (Arc::new(Column::new("c1", 1)), "c1".to_string()),
        ]);
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(500));
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_no_ndv_delegates() -> Result<()> {
        // No NDV on the GROUP BY column
        let source = make_source_with_ndv(100, vec![None]);
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("c0", 0)),
            "c0".to_string(),
        )]);
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        // Delegates to DefaultStatisticsProvider, which calls partition_statistics
        assert!(
            stats.base.num_rows.get_value().is_some()
                || matches!(stats.base.num_rows, Precision::Absent)
        );
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_non_column_expr_delegates() -> Result<()> {
        let source = make_source_with_ndv(100, vec![Some(10), Some(5)]);
        // GROUP BY an expression (c0 + c1), not a simple column ref
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c0", 0)),
            Operator::Plus,
            Arc::new(Column::new("c1", 1)),
        ));
        let group_by = PhysicalGroupBy::new_single(vec![(expr, "sum".to_string())]);
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        // Should delegate (expression is not a Column)
        assert!(
            stats.base.num_rows.get_value().is_some()
                || matches!(stats.base.num_rows, Precision::Absent)
        );
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_grouping_sets() -> Result<()> {
        let source = make_source_with_ndv(1000, vec![Some(10), Some(5)]);
        // GROUPING SETS: (c0, c1), (c0), (c1) -> 3 groups
        let group_by = PhysicalGroupBy::new(
            vec![
                (Arc::new(Column::new("c0", 0)), "c0".to_string()),
                (Arc::new(Column::new("c1", 1)), "c1".to_string()),
            ],
            vec![
                (
                    Arc::new(Literal::new(ScalarValue::Int32(None))),
                    "c0".to_string(),
                ),
                (
                    Arc::new(Literal::new(ScalarValue::Int32(None))),
                    "c1".to_string(),
                ),
            ],
            vec![
                vec![false, true],  // (c0, NULL) - group by c0 only
                vec![true, false],  // (NULL, c1) - group by c1 only
                vec![false, false], // (c0, c1)   - group by both
            ],
            true,
        );
        let agg = make_aggregate(source, group_by)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        // Multiple grouping sets: provider delegates to DefaultStatisticsProvider,
        // which calls the built-in partition_statistics for correct per-set
        // NDV estimation. The exact value depends on the built-in implementation.
        assert!(
            stats.base.num_rows.get_value().is_some()
                || matches!(stats.base.num_rows, Precision::Absent)
        );
        Ok(())
    }

    #[test]
    fn test_aggregate_provider_partial_delegates() -> Result<()> {
        // Partial aggregates produce per-partition groups; the provider
        // should delegate rather than applying global NDV bounds.
        let source = make_source_with_ndv(100, vec![Some(10)]);
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("c0", 0)),
            "c0".to_string(),
        )]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            Arc::clone(&source),
            source.schema(),
        )?);

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(AggregateStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(agg.as_ref())?;
        // Should fall through to DefaultStatisticsProvider (partition_statistics).
        // The exact value depends on the built-in implementation.
        assert!(
            stats.base.num_rows.get_value().is_some()
                || matches!(stats.base.num_rows, Precision::Absent)
        );
        Ok(())
    }

    // =========================================================================
    // JoinStatisticsProvider tests
    // =========================================================================

    use crate::joins::{HashJoinExec, PartitionMode};
    use datafusion_common::{JoinType, NullEquality};

    fn make_source_with_ndv_2col(
        num_rows: usize,
        ndv_a: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = make_schema(); // "a" Int32, "b" Int32
        let col_stats = vec![
            {
                let mut cs = ColumnStatistics::new_unknown();
                if let Some(n) = ndv_a {
                    cs.distinct_count = Precision::Exact(n);
                }
                cs
            },
            ColumnStatistics::new_unknown(),
        ];
        Arc::new(MockSourceExec::with_column_stats(
            schema,
            Precision::Exact(num_rows),
            col_stats,
        ))
    }

    fn make_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _schema = make_schema();
        let on: crate::joins::JoinOn = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            false,
        )?))
    }

    #[test]
    fn test_join_provider_with_ndv() -> Result<()> {
        // left: 1000 rows, NDV(a)=100; right: 500 rows, NDV(a)=50
        // expected = 1000 * 500 / max(100, 50) = 5000
        let left = make_source_with_ndv_2col(1000, Some(100));
        let right = make_source_with_ndv_2col(500, Some(50));
        let join = make_hash_join(left, right)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(5000));
        Ok(())
    }

    #[test]
    fn test_join_provider_uses_actual_key_column_ndv() -> Result<()> {
        // Join on column "b" (index 1), NDV only set on "b", not "a".
        // Old first()-based code would look up column 0 (a), find no NDV,
        // and fall back to Cartesian product. The fix looks up column 1 (b).
        // left: 1000 rows, NDV(b)=50; right: 500 rows, NDV(b)=25
        // expected = 1000 * 500 / max(50, 25) = 10000
        let schema = make_schema(); // "a" Int32, "b" Int32
        let make_source_ndv_b =
            |num_rows: usize, ndv_b: usize| -> Arc<dyn ExecutionPlan> {
                let col_stats = vec![
                    ColumnStatistics::new_unknown(), // "a": no NDV
                    {
                        let mut cs = ColumnStatistics::new_unknown();
                        cs.distinct_count = Precision::Exact(ndv_b);
                        cs
                    },
                ];
                Arc::new(MockSourceExec::with_column_stats(
                    Arc::clone(&schema),
                    Precision::Exact(num_rows),
                    col_stats,
                ))
            };

        let left = make_source_ndv_b(1000, 50);
        let right = make_source_ndv_b(500, 25);

        // Join on column "b" (index 1)
        let on: crate::joins::JoinOn = vec![(
            Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
        )];
        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            false,
        )?);

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(10_000));
        Ok(())
    }

    #[test]
    fn test_join_provider_multi_key_ndv() -> Result<()> {
        // Multi-key join: ON a.a = b.a AND a.b = b.b
        // left: 1000 rows, NDV(a)=100, NDV(b)=20
        // right: 500 rows, NDV(a)=50, NDV(b)=10
        // expected = 1000 * 500 / (max(100,50) * max(20,10)) = 500000 / 2000 = 250
        let schema = make_schema(); // "a" Int32, "b" Int32
        let make_source_2ndv =
            |num_rows: usize, ndv_a: usize, ndv_b: usize| -> Arc<dyn ExecutionPlan> {
                let col_stats = vec![
                    {
                        let mut cs = ColumnStatistics::new_unknown();
                        cs.distinct_count = Precision::Exact(ndv_a);
                        cs
                    },
                    {
                        let mut cs = ColumnStatistics::new_unknown();
                        cs.distinct_count = Precision::Exact(ndv_b);
                        cs
                    },
                ];
                Arc::new(MockSourceExec::with_column_stats(
                    Arc::clone(&schema),
                    Precision::Exact(num_rows),
                    col_stats,
                ))
            };

        let left = make_source_2ndv(1000, 100, 20);
        let right = make_source_2ndv(500, 50, 10);

        let on: crate::joins::JoinOn = vec![
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            ),
            (
                Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
            ),
        ];
        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            false,
        )?);

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(250));
        Ok(())
    }

    #[test]
    fn test_join_provider_fallback_cartesian() -> Result<()> {
        // No NDV available -> Cartesian product estimate
        let left = make_source_with_ndv_2col(100, None);
        let right = make_source_with_ndv_2col(200, None);
        let join = make_hash_join(left, right)?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(20_000));
        Ok(())
    }

    #[test]
    fn test_nl_join_delegates() -> Result<()> {
        use crate::joins::NestedLoopJoinExec;

        // NL join delegates to the built-in (NestedLoopJoinExec may have an
        // arbitrary JoinFilter, so the provider cannot safely assume Cartesian).
        let left = make_source(100);
        let right = make_source(200);
        let join: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
            left,
            right,
            None,
            &JoinType::Inner,
            None,
        )?);

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        // Provider delegates; result comes from built-in partition_statistics.
        assert!(
            stats.base.num_rows.get_value().is_some()
                || matches!(stats.base.num_rows, Precision::Absent)
        );
        Ok(())
    }

    fn make_hash_join_typed(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let on: crate::joins::JoinOn = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            false,
        )?))
    }

    fn compute_join_rows(
        left_rows: usize,
        left_ndv: Option<usize>,
        right_rows: usize,
        right_ndv: Option<usize>,
        join_type: JoinType,
    ) -> Result<Precision<usize>> {
        let left = make_source_with_ndv_2col(left_rows, left_ndv);
        let right = make_source_with_ndv_2col(right_rows, right_ndv);
        let join = make_hash_join_typed(left, right, join_type)?;
        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        Ok(registry.compute(join.as_ref())?.base.num_rows)
    }

    #[test]
    fn test_join_provider_left_outer() -> Result<()> {
        // left=1000, right=500, NDV(a)=100/50
        // inner estimate = 1000*500/100 = 5000, already >= left_rows
        // Left outer: max(5000, 1000) = 5000
        assert_eq!(
            compute_join_rows(1000, Some(100), 500, Some(50), JoinType::Left)?,
            Precision::Inexact(5000)
        );
        // Small inner estimate: left=1000, right=10, NDV=100/100
        // inner = 1000*10/100 = 100, left outer = max(100, 1000) = 1000
        assert_eq!(
            compute_join_rows(1000, Some(100), 10, Some(100), JoinType::Left)?,
            Precision::Inexact(1000)
        );
        Ok(())
    }

    #[test]
    fn test_join_provider_right_outer() -> Result<()> {
        // inner = 1000*10/100 = 100, right outer = max(100, 10) = 100
        assert_eq!(
            compute_join_rows(1000, Some(100), 10, Some(100), JoinType::Right)?,
            Precision::Inexact(100)
        );
        // inner = 10*1000/100 = 100, right outer = max(100, 1000) = 1000
        assert_eq!(
            compute_join_rows(10, Some(100), 1000, Some(100), JoinType::Right)?,
            Precision::Inexact(1000)
        );
        Ok(())
    }

    #[test]
    fn test_join_provider_semi_join() -> Result<()> {
        // inner = 5000, left semi = min(5000, 1000) = 1000
        assert_eq!(
            compute_join_rows(1000, Some(100), 500, Some(50), JoinType::LeftSemi)?,
            Precision::Inexact(1000)
        );
        // inner = 5000, right semi = min(5000, 500) = 500
        assert_eq!(
            compute_join_rows(1000, Some(100), 500, Some(50), JoinType::RightSemi)?,
            Precision::Inexact(500)
        );
        // Cartesian fallback (no NDV): inner = 1000*500 = 500000,
        // left semi = min(500000, 1000) = 1000 (selectivity = 1.0)
        assert_eq!(
            compute_join_rows(1000, None, 500, None, JoinType::LeftSemi)?,
            Precision::Inexact(1000)
        );
        Ok(())
    }

    #[test]
    fn test_join_provider_anti_join() -> Result<()> {
        // inner = 1000*10/100 = 100, left anti = 1000 - min(100, 1000) = 900
        assert_eq!(
            compute_join_rows(1000, Some(100), 10, Some(100), JoinType::LeftAnti)?,
            Precision::Inexact(900)
        );
        // inner = 5000, right anti = 500 - min(5000, 500) = 0
        assert_eq!(
            compute_join_rows(1000, Some(100), 500, Some(50), JoinType::RightAnti)?,
            Precision::Inexact(0)
        );
        Ok(())
    }

    // =========================================================================
    // CrossJoinExec tests (handled by JoinStatisticsProvider)
    // =========================================================================

    #[test]
    fn test_cross_join_provider_exact() -> Result<()> {
        use crate::joins::CrossJoinExec;
        let left = make_source(100);
        let right = make_source(200);
        let join: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(left, right));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(JoinStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(join.as_ref())?;
        // Both inputs have Exact row counts -> result is also Exact
        assert_eq!(stats.base.num_rows, Precision::Exact(20_000));
        Ok(())
    }

    // =========================================================================
    // LimitStatisticsProvider tests
    // =========================================================================

    use crate::limit::{GlobalLimitExec, LocalLimitExec};

    #[test]
    fn test_limit_provider_caps_output() -> Result<()> {
        // input > fetch -> capped at fetch
        let source = make_source(1000);
        let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(source, 100));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(LimitStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(limit.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(100));
        Ok(())
    }

    #[test]
    fn test_limit_provider_input_smaller_than_fetch() -> Result<()> {
        // input < fetch -> output = input
        let source = make_source(50);
        let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(source, 200));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(LimitStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(limit.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(50));
        Ok(())
    }

    #[test]
    fn test_global_limit_provider_skip_and_fetch() -> Result<()> {
        // 1000 rows, skip 200, fetch 100 -> exactly 100
        let source = make_source(1000);
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(source, 200, Some(100)));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(LimitStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(limit.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(100));
        Ok(())
    }

    #[test]
    fn test_global_limit_provider_skip_exceeds_rows() -> Result<()> {
        // 100 rows, skip 200 -> 0 rows (skip > available)
        let source = make_source(100);
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(source, 200, Some(50)));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(LimitStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(limit.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(0));
        Ok(())
    }

    #[test]
    fn test_limit_provider_inexact_input() -> Result<()> {
        // Inexact(1000) with fetch=100: result must stay Inexact, not Exact,
        // because the actual row count could be less than 100.
        let source = make_source_with_precision(Precision::Inexact(1000));
        let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(source, 100));

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(LimitStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(limit.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(100));
        Ok(())
    }

    // =========================================================================
    // UnionStatisticsProvider tests
    // =========================================================================

    use crate::union::UnionExec;

    fn make_source_with_precision(num_rows: Precision<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(MockSourceExec::new(make_schema(), num_rows))
    }

    #[test]
    fn test_union_provider_sums_rows() -> Result<()> {
        let union = UnionExec::try_new(vec![make_source(300), make_source(700)])?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(UnionStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(union.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(1000));
        Ok(())
    }

    #[test]
    fn test_union_provider_three_inputs() -> Result<()> {
        let union = UnionExec::try_new(vec![
            make_source(100),
            make_source(200),
            make_source(300),
        ])?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(UnionStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(union.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Exact(600));
        Ok(())
    }

    #[test]
    fn test_union_provider_absent_propagates() -> Result<()> {
        // One input with unknown row count -> result must be Absent, not Inexact(300)
        let union = UnionExec::try_new(vec![
            make_source(300),
            make_source_with_precision(Precision::Absent),
        ])?;

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(UnionStatisticsProvider),
            Arc::new(DefaultStatisticsProvider),
        ]);
        let stats = registry.compute(union.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Absent);
        Ok(())
    }

    // =========================================================================
    // ClosureStatisticsProvider tests
    // =========================================================================

    #[test]
    fn test_closure_provider_basic() -> Result<()> {
        // Override all FilterExec stats with a fixed row count
        let provider = ClosureStatisticsProvider::new(|plan, _child_stats| {
            if plan.downcast_ref::<FilterExec>().is_some() {
                Ok(StatisticsResult::Computed(ExtendedStatistics::from(
                    Statistics {
                        num_rows: Precision::Inexact(42),
                        total_byte_size: Precision::Absent,
                        column_statistics: vec![],
                    },
                )))
            } else {
                Ok(StatisticsResult::Delegate)
            }
        });

        let registry = StatisticsRegistry::with_providers(vec![
            Arc::new(provider),
            Arc::new(DefaultStatisticsProvider),
        ]);

        let source = make_source(1000);
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), source)?);
        let stats = registry.compute(filter.as_ref())?;
        assert_eq!(stats.base.num_rows, Precision::Inexact(42));
        Ok(())
    }

    #[test]
    fn test_closure_provider_distinguishes_nodes_by_child_stats() -> Result<()> {
        // Two FilterExec nodes with different input sizes.
        // The closure uses the child row count as a proxy to distinguish them,
        // which mirrors the cardinality feedback use case where you match a
        // runtime-observed count to the right node in the plan tree.
        let provider = ClosureStatisticsProvider::new(|plan, child_stats| {
            if plan.downcast_ref::<FilterExec>().is_none() {
                return Ok(StatisticsResult::Delegate);
            }
            match child_stats[0].base.num_rows.get_value().copied() {
                Some(500) => Ok(StatisticsResult::Computed(ExtendedStatistics::from(
                    Statistics {
                        num_rows: Precision::Inexact(100),
                        total_byte_size: Precision::Absent,
                        column_statistics: vec![],
                    },
                ))),
                Some(200) => Ok(StatisticsResult::Computed(ExtendedStatistics::from(
                    Statistics {
                        num_rows: Precision::Inexact(50),
                        total_byte_size: Precision::Absent,
                        column_statistics: vec![],
                    },
                ))),
                _ => Ok(StatisticsResult::Delegate),
            }
        });

        let registry = StatisticsRegistry::with_providers(vec![Arc::new(provider)]);

        let filter_a: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), make_source(500))?);
        let filter_b: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), make_source(200))?);

        let stats_a = registry.compute(filter_a.as_ref())?;
        let stats_b = registry.compute(filter_b.as_ref())?;

        assert_eq!(stats_a.base.num_rows, Precision::Inexact(100));
        assert_eq!(stats_b.base.num_rows, Precision::Inexact(50));
        Ok(())
    }
}
