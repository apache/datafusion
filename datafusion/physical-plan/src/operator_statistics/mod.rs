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
//! similar to [`RelationPlanner`] for SQL parsing.
//!
//! # Overview
//!
//! The default implementation uses classic Selinger-style estimation
//! (selectivity factors, independence assumptions). Users can register
//! custom [`StatisticsProvider`] implementations to:
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
        Ok(StatisticsResult::Computed(ExtendedStatistics::new_arc(base)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterExec;
    use crate::{DisplayAs, DisplayFormatType, PlanProperties};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ColumnStatistics;
    use datafusion_common::stats::Precision;
    use datafusion_physical_expr::expressions::lit;
    use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion_physical_expr::PhysicalExpr;
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
        fn new(schema: Arc<Schema>, num_rows: usize) -> Self {
            let num_cols = schema.fields().len();
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
                    num_rows: Precision::Exact(num_rows),
                    total_byte_size: Precision::Absent,
                    column_statistics: vec![
                        ColumnStatistics::new_unknown();
                        num_cols
                    ],
                },
                cache,
            }
        }

        fn with_column_stats(
            schema: Arc<Schema>,
            num_rows: usize,
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
                    num_rows: Precision::Exact(num_rows),
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

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
            Ok(Arc::new(self.stats.clone()))
        }
    }

    fn make_source(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(MockSourceExec::new(make_schema(), num_rows))
    }

    #[test]
    fn test_default_planner() -> Result<()> {
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
            StatisticsRegistry::with_providers(vec![Arc::new(
                OverrideFilterPlanner {
                    fixed_selectivity: 0.25,
                },
            )
                as Arc<dyn StatisticsProvider>]);
        // OverrideFilterPlanner handles filters, built-in fallback handles the rest
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
    fn test_custom_planner_for_custom_exec() -> Result<()> {
        let mut engine = StatisticsRegistry::new();
        engine.register(Arc::new(CustomStatisticsProvider));

        let source = make_source(1000);
        let custom: Arc<dyn ExecutionPlan> = Arc::new(CustomExec { input: source });

        let stats = engine.compute(custom.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Exact(1000)));
        Ok(())
    }

    #[derive(Debug)]
    struct OverrideFilterPlanner {
        fixed_selectivity: f64,
    }

    impl StatisticsProvider for OverrideFilterPlanner {
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
        engine.register(Arc::new(OverrideFilterPlanner {
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
    fn test_chain_priority() -> Result<()> {
        let mut engine = StatisticsRegistry::new();
        engine.register(Arc::new(OverrideFilterPlanner {
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

        // FilterExec: CustomStatisticsProvider delegates, OverrideFilterPlanner handles
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), source)?);
        let stats = engine.compute(filter.as_ref())?;
        assert!(matches!(stats.base.num_rows, Precision::Inexact(500)));

        Ok(())
    }

}
