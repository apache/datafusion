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
use parking_lot::RwLock;

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

/// State machine for selectivity tracking.
#[derive(Debug)]
enum SelectivityState {
    /// Collecting statistics, not yet enough data.
    Tracking {
        rows_passed: AtomicUsize,
        rows_total: AtomicUsize,
    },
    /// Filter is sufficiently selective, keep active.
    Active,
    /// Filter has been disabled due to poor selectivity.
    Disabled,
}

impl SelectivityState {
    fn new_tracking() -> Self {
        Self::Tracking {
            rows_passed: AtomicUsize::new(0),
            rows_total: AtomicUsize::new(0),
        }
    }
}

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
    /// Selectivity tracking state.
    state: RwLock<SelectivityState>,
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
            state: RwLock::new(SelectivityState::new_tracking()),
            tracked_generation: AtomicU64::new(current_generation),
            config,
        }
    }

    /// Get the current selectivity information for observability.
    ///
    /// Returns `(rows_passed, rows_total, is_disabled)`.
    pub fn selectivity_info(&self) -> (usize, usize, bool) {
        let state = self.state.read();
        match &*state {
            SelectivityState::Tracking {
                rows_passed,
                rows_total,
            } => {
                let passed = rows_passed.load(Ordering::Relaxed);
                let total = rows_total.load(Ordering::Relaxed);
                (passed, total, false)
            }
            SelectivityState::Active => (0, 0, false),
            SelectivityState::Disabled => (0, 0, true),
        }
    }

    /// Check if the filter is disabled.
    pub fn is_disabled(&self) -> bool {
        matches!(*self.state.read(), SelectivityState::Disabled)
    }

    /// Get the inner expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    /// Check if the inner generation has changed and reset state if needed.
    fn check_and_reset_if_needed(&self) {
        let current_generation = self.inner.snapshot_generation();
        let tracked = self.tracked_generation.load(Ordering::Relaxed);

        if current_generation != tracked {
            // Generation changed - reset to tracking state
            let mut state = self.state.write();
            *state = SelectivityState::new_tracking();
            self.tracked_generation
                .store(current_generation, Ordering::Relaxed);
        }
    }

    /// Count the number of true values in a boolean array.
    fn count_true_values(array: &BooleanArray) -> usize {
        array.true_count()
    }

    /// Process the result and update selectivity statistics.
    fn process_result(&self, result: &ColumnarValue) -> Result<()> {
        // Only track selectivity for array results
        let (true_count, total_count) = match result {
            ColumnarValue::Array(array) => {
                let bool_array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Filter expression should return BooleanArray");
                (Self::count_true_values(bool_array), array.len())
            }
            ColumnarValue::Scalar(scalar) => {
                // Scalar result - we can't track selectivity meaningfully
                // Just skip tracking for this batch
                if let datafusion_common::ScalarValue::Boolean(Some(v)) = scalar {
                    if *v { (1, 1) } else { (0, 1) }
                } else {
                    return Ok(());
                }
            }
        };

        let state = self.state.read();
        if let SelectivityState::Tracking {
            rows_passed,
            rows_total,
        } = &*state
        {
            rows_passed.fetch_add(true_count, Ordering::Relaxed);
            let new_total =
                rows_total.fetch_add(total_count, Ordering::Relaxed) + total_count;
            let passed = rows_passed.load(Ordering::Relaxed);

            // Check if we've seen enough rows to make a decision
            if new_total >= self.config.min_rows {
                // Calculate selectivity
                let selectivity = passed as f64 / new_total as f64;
                drop(state);

                // Decide whether to disable or keep active
                let mut state = self.state.write();
                // Re-check in case another thread already updated
                if matches!(*state, SelectivityState::Tracking { .. }) {
                    if selectivity >= self.config.threshold {
                        *state = SelectivityState::Disabled;
                    } else {
                        *state = SelectivityState::Active;
                    }
                }
            }
        }

        Ok(())
    }

    /// Create an all-true boolean array of the given length.
    fn all_true_array(len: usize) -> ArrayRef {
        Arc::new(BooleanArray::from(vec![true; len]))
    }
}

impl Display for AdaptiveSelectivityFilterExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (passed, total, disabled) = self.selectivity_info();
        if disabled {
            write!(f, "SelectivityAware(DISABLED) [ {} ]", self.inner)
        } else if total > 0 {
            let selectivity = passed as f64 / total as f64;
            write!(
                f,
                "SelectivityAware({:.1}%, {}/{}) [ {} ]",
                selectivity * 100.0,
                passed,
                total,
                self.inner
            )
        } else {
            write!(f, "SelectivityAware [ {} ]", self.inner)
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

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Check if the inner generation has changed
        self.check_and_reset_if_needed();

        // Check if we're disabled
        {
            let state = self.state.read();
            if matches!(*state, SelectivityState::Disabled) {
                // Return all-true to bypass the filter
                return Ok(ColumnarValue::Array(Self::all_true_array(batch.num_rows())));
            }
        }

        // Evaluate the inner expression
        let result = self.inner.evaluate(batch)?;

        // Update selectivity statistics
        self.process_result(&result)?;

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
        let (passed, total, disabled) = wrapper.selectivity_info();
        assert_eq!(passed, 0); // Moved to Disabled state, counters reset conceptually
        assert_eq!(total, 0);
        assert!(disabled, "Filter should be disabled after high selectivity");
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
        let (_, _, disabled) = wrapper.selectivity_info();
        assert!(!disabled, "Low selectivity filter should stay active");
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
            display.contains("SelectivityAware"),
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
}
