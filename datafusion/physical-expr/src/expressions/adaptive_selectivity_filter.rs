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

//! A wrapper [`PhysicalExpr`] that tracks filter selectivity at runtime and
//! automatically disables filters that aren't pruning enough rows.

use std::any::Any;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::DynHash;

use crate::PhysicalExpr;

/// Configuration for selectivity-based filter disabling.
#[derive(Debug, Clone)]
pub struct SelectivityConfig {
    /// Threshold above which the filter is disabled (e.g., 0.95 = 95% selectivity).
    /// If the filter passes this fraction or more of rows, it will be disabled.
    pub threshold: f64,
    /// Minimum rows to process before making a selectivity decision.
    pub min_rows: usize,
}

impl Default for SelectivityConfig {
    fn default() -> Self {
        Self {
            threshold: 0.95,
            min_rows: 10_000,
        }
    }
}

// State values for the atomic state machine
const STATE_TRACKING: u8 = 0;
const STATE_ACTIVE: u8 = 1;
const STATE_DISABLED: u8 = 2;

/// A wrapper [`PhysicalExpr`] that tracks selectivity and can disable filters
/// that pass too many rows.
///
/// This wrapper is designed to be used with dynamic filters from joins.
/// It monitors how many rows pass through the filter, and if the filter
/// is found to be ineffective (passes most rows), it automatically disables
/// itself to avoid evaluation overhead.
///
/// The wrapper resets its statistics when the inner filter's generation changes,
/// which happens when the dynamic filter is updated (e.g., when the hash table
/// is built in a hash join).
#[derive(Debug)]
pub struct AdaptiveSelectivityFilterExpr {
    /// The inner filter expression (typically DynamicFilterPhysicalExpr).
    inner: Arc<dyn PhysicalExpr>,
    /// Simple atomic state: 0 = Tracking, 1 = Active, 2 = Disabled
    /// This allows the hot path to be a single atomic load with no locks.
    state: AtomicUsize,
    /// Rows that passed the filter (only used in Tracking state).
    rows_passed: AtomicUsize,
    /// Total rows processed (only used in Tracking state).
    rows_total: AtomicUsize,
    /// The generation of the inner filter when we started tracking.
    /// If this changes, we need to reset our state.
    tracked_generation: AtomicU64,
    /// Configuration for selectivity tracking.
    config: SelectivityConfig,
}

impl AdaptiveSelectivityFilterExpr {
    /// Create a new `AdaptiveSelectivityFilterExpr` wrapping the given inner expression.
    pub fn new(inner: Arc<dyn PhysicalExpr>, config: SelectivityConfig) -> Self {
        let current_generation = inner.snapshot_generation();
        Self {
            inner,
            state: AtomicUsize::new(STATE_TRACKING as usize),
            rows_passed: AtomicUsize::new(0),
            rows_total: AtomicUsize::new(0),
            tracked_generation: AtomicU64::new(current_generation),
            config,
        }
    }

    /// Get the current selectivity information for observability.
    ///
    /// Returns `(rows_passed, rows_total, is_disabled)`.
    pub fn selectivity_info(&self) -> (usize, usize, bool) {
        let state = self.state.load(Ordering::Relaxed) as u8;
        match state {
            STATE_TRACKING => {
                let passed = self.rows_passed.load(Ordering::Relaxed);
                let total = self.rows_total.load(Ordering::Relaxed);
                (passed, total, false)
            }
            STATE_ACTIVE => (0, 0, false),
            STATE_DISABLED => (0, 0, true),
            _ => (0, 0, false),
        }
    }

    /// Check if the filter is disabled.
    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.state.load(Ordering::Relaxed) as u8 == STATE_DISABLED
    }

    /// Get the inner expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    /// Create an all-true boolean array of the given length.
    #[inline]
    fn all_true_array(len: usize) -> ArrayRef {
        Arc::new(BooleanArray::from(vec![true; len]))
    }

    /// Fast path evaluation - just check the atomic state.
    /// Returns Some(result) if we can short-circuit, None if we need to do full evaluation.
    #[inline]
    fn try_fast_path(&self, batch: &RecordBatch) -> Option<ColumnarValue> {
        let state = self.state.load(Ordering::Relaxed) as u8;
        match state {
            STATE_DISABLED => {
                // Fast path: filter is disabled, return all-true
                Some(ColumnarValue::Array(Self::all_true_array(batch.num_rows())))
            }
            STATE_ACTIVE => {
                // Fast path: filter is active and we've finished tracking
                // Just evaluate the inner expression, no tracking overhead
                None
            }
            STATE_TRACKING => {
                // Need to do tracking - check generation first
                let current_gen = self.inner.snapshot_generation();
                let tracked_gen = self.tracked_generation.load(Ordering::Relaxed);
                if current_gen != tracked_gen {
                    // Generation changed - reset tracking
                    self.rows_passed.store(0, Ordering::Relaxed);
                    self.rows_total.store(0, Ordering::Relaxed);
                    self.tracked_generation
                        .store(current_gen, Ordering::Relaxed);
                    self.state.store(STATE_TRACKING as usize, Ordering::Relaxed);
                }
                None
            }
            _ => None,
        }
    }

    /// Update tracking statistics after evaluating a batch.
    /// Only called when in TRACKING state.
    #[inline]
    fn update_tracking(&self, result: &ColumnarValue) {
        // Only update if still in tracking state
        if self.state.load(Ordering::Relaxed) as u8 != STATE_TRACKING {
            return;
        }

        let (true_count, total_count) = match result {
            ColumnarValue::Array(array) => {
                let bool_array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Filter expression should return BooleanArray");
                (bool_array.true_count(), array.len())
            }
            ColumnarValue::Scalar(scalar) => {
                if let datafusion_common::ScalarValue::Boolean(Some(v)) = scalar {
                    if *v { (1, 1) } else { (0, 1) }
                } else {
                    return;
                }
            }
        };

        // Update counters
        self.rows_passed.fetch_add(true_count, Ordering::Relaxed);
        let new_total =
            self.rows_total.fetch_add(total_count, Ordering::Relaxed) + total_count;

        // Check if we've seen enough rows to make a decision
        if new_total >= self.config.min_rows {
            let passed = self.rows_passed.load(Ordering::Relaxed);
            let selectivity = passed as f64 / new_total as f64;

            // Use compare_exchange to ensure only one thread makes the transition
            let new_state = if selectivity >= self.config.threshold {
                STATE_DISABLED
            } else {
                STATE_ACTIVE
            };

            // Try to transition from TRACKING to the new state
            // If this fails, another thread already did the transition, which is fine
            let _ = self.state.compare_exchange(
                STATE_TRACKING as usize,
                new_state as usize,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }
}

impl Display for AdaptiveSelectivityFilterExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (passed, total, disabled) = self.selectivity_info();
        if disabled {
            write!(f, "AdaptiveSelectivity(DISABLED) [ {} ]", self.inner)
        } else if total > 0 {
            let selectivity = passed as f64 / total as f64;
            write!(
                f,
                "AdaptiveSelectivity({:.1}%, {}/{}) [ {} ]",
                selectivity * 100.0,
                passed,
                total,
                self.inner
            )
        } else {
            write!(f, "AdaptiveSelectivity [ {} ]", self.inner)
        }
    }
}

impl Hash for AdaptiveSelectivityFilterExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based on the inner expression
        self.inner.dyn_hash(state);
    }
}

impl PartialEq for AdaptiveSelectivityFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for AdaptiveSelectivityFilterExpr {}

impl PhysicalExpr for AdaptiveSelectivityFilterExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "AdaptiveSelectivityFilterExpr expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.config.clone(),
        )))
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    #[inline]
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Fast path: single atomic load to check state
        if let Some(result) = self.try_fast_path(batch) {
            return Ok(result);
        }

        // Evaluate the inner expression
        let result = self.inner.evaluate(batch)?;

        // Update tracking if in tracking state (cheap check + possible update)
        if self.state.load(Ordering::Relaxed) as u8 == STATE_TRACKING {
            self.update_tracking(&result);
        }

        Ok(result)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Return the inner's snapshot
        self.inner.snapshot()
    }

    fn snapshot_generation(&self) -> u64 {
        // Return the inner's generation
        self.inner.snapshot_generation()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, col, lit};
    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion_expr::Operator;

    fn create_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values)) as ArrayRef])
            .unwrap()
    }

    fn create_filter_expr(threshold: i32) -> Arc<dyn PhysicalExpr> {
        // Create a filter: a < threshold
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Arc::new(BinaryExpr::new(
            col("a", &schema).unwrap(),
            Operator::Lt,
            lit(threshold),
        ))
    }

    #[test]
    fn test_high_selectivity_filter_gets_disabled() {
        // Create a filter that passes 95%+ of rows: a < 100 (all values pass)
        let filter = create_filter_expr(100);
        let config = SelectivityConfig {
            threshold: 0.95,
            min_rows: 100,
        };
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        // Create batches where all rows pass the filter
        let batch = create_batch((0..100).collect());

        // Evaluate - should process and track
        let result = wrapper.evaluate(&batch).unwrap();
        let ColumnarValue::Array(arr) = result else {
            panic!("Expected array result");
        };
        assert_eq!(arr.len(), 100);

        // After enough rows, the filter should be disabled
        assert!(
            wrapper.is_disabled(),
            "Filter should be disabled after high selectivity"
        );
    }

    #[test]
    fn test_low_selectivity_filter_stays_active() {
        // Create a filter that passes ~50% of rows: a < 50
        let filter = create_filter_expr(50);
        let config = SelectivityConfig {
            threshold: 0.95,
            min_rows: 100,
        };
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        // Create batch where ~50% pass
        let batch = create_batch((0..100).collect());

        // Evaluate
        let _result = wrapper.evaluate(&batch).unwrap();

        // Filter should stay active (not disabled)
        assert!(
            !wrapper.is_disabled(),
            "Low selectivity filter should stay active"
        );
    }

    #[test]
    fn test_disabled_filter_returns_all_true() {
        // Create a filter that will be disabled
        let filter = create_filter_expr(100); // All pass
        let config = SelectivityConfig {
            threshold: 0.95,
            min_rows: 10,
        };
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        // First batch - get it disabled
        let batch = create_batch((0..100).collect());
        let _ = wrapper.evaluate(&batch).unwrap();

        assert!(wrapper.is_disabled(), "Filter should be disabled");

        // Now create a batch where the original filter would return some false
        // But since we're disabled, we should get all true
        let batch2 = create_batch(vec![200, 201, 202]); // These would fail a < 100
        let result = wrapper.evaluate(&batch2).unwrap();

        let ColumnarValue::Array(arr) = result else {
            panic!("Expected array result");
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();

        // All values should be true because the filter is disabled
        assert_eq!(bool_arr.true_count(), 3);
    }

    #[test]
    fn test_min_rows_threshold_respected() {
        let filter = create_filter_expr(100); // All pass
        let config = SelectivityConfig {
            threshold: 0.95,
            min_rows: 1000, // High threshold
        };
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        // Process less than min_rows
        let batch = create_batch((0..100).collect());
        let _ = wrapper.evaluate(&batch).unwrap();

        // Should still be tracking, not yet disabled
        let (passed, total, disabled) = wrapper.selectivity_info();
        assert_eq!(passed, 100);
        assert_eq!(total, 100);
        assert!(
            !disabled,
            "Should still be tracking under min_rows threshold"
        );
    }

    #[test]
    fn test_display() {
        let filter = create_filter_expr(50);
        let config = SelectivityConfig::default();
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        let display = format!("{wrapper}");
        assert!(
            display.contains("AdaptiveSelectivity"),
            "Display should show wrapper name"
        );
    }

    #[test]
    fn test_with_new_children() {
        let filter = create_filter_expr(50);
        let config = SelectivityConfig {
            threshold: 0.80,
            min_rows: 5000,
        };
        let wrapper = Arc::new(AdaptiveSelectivityFilterExpr::new(filter, config));

        let new_filter = create_filter_expr(75);
        let new_wrapper = wrapper.with_new_children(vec![new_filter]).unwrap();

        // Should create a new wrapper with the new child
        let new_wrapper = new_wrapper
            .as_any()
            .downcast_ref::<AdaptiveSelectivityFilterExpr>()
            .unwrap();
        assert!(!new_wrapper.is_disabled());
    }

    #[test]
    fn test_active_state_no_tracking_overhead() {
        // Test that once in Active state, there's minimal overhead
        let filter = create_filter_expr(50); // ~50% pass rate
        let config = SelectivityConfig {
            threshold: 0.95,
            min_rows: 100,
        };
        let wrapper = AdaptiveSelectivityFilterExpr::new(filter, config);

        // Process enough rows to transition to Active
        let batch = create_batch((0..100).collect());
        let _ = wrapper.evaluate(&batch).unwrap();

        // Should be in Active state now
        assert!(!wrapper.is_disabled());
        let state = wrapper.state.load(Ordering::Relaxed) as u8;
        assert_eq!(state, STATE_ACTIVE, "Should be in Active state");

        // Further evaluations should not update tracking counters
        let initial_passed = wrapper.rows_passed.load(Ordering::Relaxed);
        let initial_total = wrapper.rows_total.load(Ordering::Relaxed);

        // Evaluate more batches
        for _ in 0..10 {
            let _ = wrapper.evaluate(&batch).unwrap();
        }

        // Counters should NOT have changed (no tracking in Active state)
        assert_eq!(
            wrapper.rows_passed.load(Ordering::Relaxed),
            initial_passed,
            "Counters should not change in Active state"
        );
        assert_eq!(
            wrapper.rows_total.load(Ordering::Relaxed),
            initial_total,
            "Counters should not change in Active state"
        );
    }
}
