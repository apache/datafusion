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

//! Adaptive filter selectivity tracking for Parquet row filters.
//!
//! This module provides infrastructure to track filter effectiveness across files
//! and adaptively decide which filters should be pushed down as row filters vs.
//! applied post-scan.
//!
//! The key insight is that filters with low effectiveness (those that don't filter
//! out many rows) may not be worth the I/O cost of late materialization. By tracking
//! effectiveness across files, we can learn which filters are worth pushing down.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Result of partitioning filters based on their effectiveness.
///
/// Filters are split into two groups:
/// - `row_filters`: Filters that should be pushed down as row filters
/// - `post_scan`: Filters that should be applied after scanning
#[derive(Debug, Clone, Default)]
pub struct PartitionedFilters {
    /// Filters to push down as row filters (effective or unknown effectiveness)
    pub row_filters: Vec<Arc<dyn PhysicalExpr>>,
    /// Filters to apply post-scan (known to be ineffective)
    pub post_scan: Vec<Arc<dyn PhysicalExpr>>,
}

/// Wrapper for `Arc<dyn PhysicalExpr>` that uses structural Hash/Eq.
///
/// This is needed because `Arc<dyn PhysicalExpr>` uses pointer equality by default,
/// but we want to use the structural equality provided by `DynEq` and `DynHash`.
///
/// For dynamic expressions (like `DynamicFilterPhysicalExpr`), we use the snapshot
/// of the expression to ensure stable hash/eq values even as the dynamic expression
/// updates. This is critical for HashMap correctness.
#[derive(Clone, Debug)]
pub struct ExprKey(Arc<dyn PhysicalExpr>);

impl ExprKey {
    /// Create a new ExprKey from an expression.
    ///
    /// For dynamic expressions, this takes a snapshot to ensure stable hash/eq.
    pub fn new(expr: &Arc<dyn PhysicalExpr>) -> Self {
        // Try to get a snapshot; if available, use it for stable hash/eq
        let stable_expr = expr
            .snapshot()
            .ok()
            .flatten()
            .unwrap_or_else(|| Arc::clone(expr));
        Self(stable_expr)
    }
}

impl Hash for ExprKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // dyn PhysicalExpr implements Hash, which delegates to dyn_hash
        self.0.as_ref().hash(state);
    }
}

impl PartialEq for ExprKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for ExprKey {}

/// Tracks selectivity statistics for a single filter expression.
#[derive(Debug, Clone, Default)]
pub struct SelectivityStats {
    /// Number of rows that matched (passed) the filter
    pub rows_matched: u64,
    /// Total number of rows evaluated
    pub rows_total: u64,
}

impl SelectivityStats {
    /// Create new stats with given values.
    pub fn new(rows_matched: u64, rows_total: u64) -> Self {
        Self {
            rows_matched,
            rows_total,
        }
    }

    /// Returns the filter effectiveness (fraction of rows filtered out).
    ///
    /// - 1.0 = perfect filter (all rows filtered out)
    /// - 0.0 = useless filter (no rows filtered out)
    ///
    /// Returns 0.0 if no rows have been evaluated (unknown effectiveness).
    pub fn effectiveness(&self) -> f64 {
        if self.rows_total == 0 {
            0.0 // Unknown, assume ineffective
        } else {
            1.0 - (self.rows_matched as f64 / self.rows_total as f64)
        }
    }

    /// Update stats with new observations.
    pub fn update(&mut self, matched: u64, total: u64) {
        self.rows_matched += matched;
        self.rows_total += total;
    }
}

/// Cross-file selectivity tracker for adaptive filter ordering.
///
/// This tracker maintains effectiveness statistics for filter expressions
/// across multiple files, allowing the system to learn which filters are
/// worth pushing down as row filters.
#[derive(Debug)]
pub struct SelectivityTracker {
    /// Per-expression effectiveness statistics
    stats: HashMap<ExprKey, SelectivityStats>,
    /// Minimum effectiveness threshold to keep a filter as a row filter.
    /// Filters with effectiveness < threshold are demoted to post-scan.
    /// Default: 0.8 (must filter out at least 80% of rows)
    threshold: f64,
}

impl Default for SelectivityTracker {
    fn default() -> Self {
        Self::new(0.8)
    }
}

impl SelectivityTracker {
    /// Create a new tracker with the given effectiveness threshold.
    ///
    /// # Arguments
    /// * `threshold` - Minimum effectiveness (0.0-1.0) to keep as row filter.
    ///   Filters with effectiveness < threshold are demoted to post-scan.
    pub fn new(threshold: f64) -> Self {
        Self {
            stats: HashMap::new(),
            threshold,
        }
    }

    /// Get the effectiveness threshold.
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Get the effectiveness for a filter expression, if known.
    pub fn get_effectiveness(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<f64> {
        let key = ExprKey::new(expr);
        self.stats.get(&key).map(|s| s.effectiveness())
    }

    /// Partition filters into row_filters and post_scan based on effectiveness.
    ///
    /// Filters start as row filters (pushed down) to take advantage of late
    /// materialization. As we learn their effectiveness, ineffective filters
    /// (those that pass most rows) get demoted to post-scan for future files.
    ///
    /// - Filters with effectiveness >= threshold → row_filters (push down)
    /// - Filters with effectiveness < threshold → post_scan (not worth pushing)
    /// - Filters with unknown effectiveness → row_filters (try pushing first)
    pub fn partition_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> PartitionedFilters {
        // If the selectivity is set to 0.0, all filters are promoted to row filters
        // even if we have no stats on them.
        if self.threshold == 0.0 {
            return PartitionedFilters {
                row_filters: filters,
                post_scan: Vec::new(),
            };
        }

        let mut row_filters = Vec::new();
        let mut post_scan = Vec::new();

        for filter in filters {
            let key = ExprKey::new(&filter);
            match self.stats.get(&key) {
                Some(stats) if stats.effectiveness() < self.threshold => {
                    // Known to be ineffective - demote to post-scan
                    post_scan.push(filter);
                }
                _ => {
                    // Unknown or effective - push down as row filter
                    row_filters.push(filter);
                }
            }
        }

        PartitionedFilters {
            row_filters,
            post_scan,
        }
    }

    /// Update stats for a filter expression after processing a file.
    pub fn update(&mut self, expr: &Arc<dyn PhysicalExpr>, matched: u64, total: u64) {
        let key = ExprKey::new(expr);
        self.stats.entry(key).or_default().update(matched, total);
    }

    /// Get the current stats for a filter expression, if any.
    pub fn get_stats(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<&SelectivityStats> {
        let key = ExprKey::new(expr);
        self.stats.get(&key)
    }

    /// Iterate all known selectivities.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&Arc<dyn PhysicalExpr>, &SelectivityStats)> {
        self.stats.iter().map(|(key, stats)| (&key.0, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    fn make_filter(col_name: &str, value: i32) -> Arc<dyn PhysicalExpr> {
        let schema = test_schema();
        Arc::new(BinaryExpr::new(
            col(col_name, &schema).unwrap(),
            Operator::Eq,
            lit(value),
        ))
    }

    #[test]
    fn test_expr_key_equality() {
        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 5);
        let filter3 = make_filter("a", 10);

        let key1 = ExprKey::new(&filter1);
        let key2 = ExprKey::new(&filter2);
        let key3 = ExprKey::new(&filter3);

        // Same expression structure should be equal
        assert_eq!(key1, key2);
        // Different value should not be equal
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_expr_key_hash() {
        use std::collections::hash_map::DefaultHasher;

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 5);

        let key1 = ExprKey::new(&filter1);
        let key2 = ExprKey::new(&filter2);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_selectivity_stats_effectiveness() {
        // No data - unknown
        let stats = SelectivityStats::new(0, 0);
        assert_eq!(stats.effectiveness(), 0.0);

        // All rows pass - useless filter
        let stats = SelectivityStats::new(100, 100);
        assert_eq!(stats.effectiveness(), 0.0);

        // No rows pass - perfect filter
        let stats = SelectivityStats::new(0, 100);
        assert_eq!(stats.effectiveness(), 1.0);

        // 20% pass = 80% filtered = 0.8 effectiveness
        let stats = SelectivityStats::new(20, 100);
        assert_eq!(stats.effectiveness(), 0.8);

        // 50% pass = 50% filtered = 0.5 effectiveness
        let stats = SelectivityStats::new(50, 100);
        assert_eq!(stats.effectiveness(), 0.5);
    }

    #[test]
    fn test_selectivity_stats_update() {
        let mut stats = SelectivityStats::default();
        assert_eq!(stats.rows_matched, 0);
        assert_eq!(stats.rows_total, 0);

        stats.update(20, 100);
        assert_eq!(stats.rows_matched, 20);
        assert_eq!(stats.rows_total, 100);

        stats.update(30, 100);
        assert_eq!(stats.rows_matched, 50);
        assert_eq!(stats.rows_total, 200);
        assert_eq!(stats.effectiveness(), 0.75); // 150/200 filtered = 0.75
    }

    #[test]
    fn test_tracker_partition_unknown_filters() {
        let tracker = SelectivityTracker::new(0.8);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        // Unknown filters should go to row_filters to be tried first
        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()]);

        assert_eq!(row_filters.len(), 2);
        assert_eq!(post_scan.len(), 0);
    }

    #[test]
    fn test_tracker_partition_effective_filters() {
        let mut tracker = SelectivityTracker::new(0.8);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        // Update filter1 with high effectiveness (90% filtered)
        tracker.update(&filter1, 10, 100);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()]);

        // filter1 is effective (0.9 >= 0.8) → row_filters, filter2 is unknown → row_filters
        assert_eq!(row_filters.len(), 2);
        assert_eq!(post_scan.len(), 0);

        // Both filters should be in row_filters
        assert!(
            row_filters
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter1))
        );
        assert!(
            row_filters
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter2))
        );
    }

    #[test]
    fn test_tracker_partition_ineffective_filters() {
        let mut tracker = SelectivityTracker::new(0.8);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        // Update filter1 with low effectiveness (50% filtered)
        tracker.update(&filter1, 50, 100);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()]);

        // filter1 is ineffective (0.5 < 0.8) → post_scan, filter2 is unknown → row_filters
        assert_eq!(row_filters.len(), 1);
        assert_eq!(post_scan.len(), 1);

        // The unknown filter should be in row_filters
        assert!(
            row_filters
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter2))
        );
        // The ineffective filter should be in post_scan
        assert!(
            post_scan
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter1))
        );
    }

    #[test]
    fn test_tracker_threshold_boundary() {
        let mut tracker = SelectivityTracker::new(0.8);

        let filter = make_filter("a", 5);

        // Exactly at threshold (80% filtered = 0.8 effectiveness)
        tracker.update(&filter, 20, 100);
        assert_eq!(tracker.get_effectiveness(&filter), Some(0.8));

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter.clone()]);

        // At threshold boundary, should stay as row filter (>= threshold)
        assert_eq!(row_filters.len(), 1);
        assert_eq!(post_scan.len(), 0);
    }

    #[test]
    fn test_tracker_just_below_threshold() {
        let mut tracker = SelectivityTracker::new(0.8);

        let filter = make_filter("a", 5);

        // Just below threshold (79% filtered = 0.79 effectiveness)
        tracker.update(&filter, 21, 100);
        assert!((tracker.get_effectiveness(&filter).unwrap() - 0.79).abs() < 0.001);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter.clone()]);

        // Below threshold, should be demoted to post_scan
        assert_eq!(row_filters.len(), 0);
        assert_eq!(post_scan.len(), 1);
    }
}
