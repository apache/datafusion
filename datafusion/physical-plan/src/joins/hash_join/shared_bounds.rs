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

//! Utilities for shared bounds. Used in dynamic filter pushdown in Hash Joins.
// TODO: include the link to the Dynamic Filter blog post.

use std::fmt;
use std::sync::Arc;

use crate::joins::PartitionMode;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use arrow::array::{new_null_array, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, and_not, is_null, not, nullif};
use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    cast::as_uint64_array, internal_datafusion_err, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use datafusion_expr::ScalarUDF;
use datafusion_functions::hash::Hash;
use datafusion_physical_expr::expressions::{lit, BinaryExpr, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

use ahash::RandomState;
use datafusion_physical_expr_common::physical_expr::DynEq;
use itertools::Itertools;
use parking_lot::Mutex;
use std::collections::HashSet;

/// RandomState used by RepartitionExec for consistent hash partitioning
/// This must match the seeds used in RepartitionExec to ensure our hash-based
/// filter expressions compute the same partition assignments as the actual partitioning
const REPARTITION_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Represents the minimum and maximum values for a specific column.
/// Used in dynamic filter pushdown to establish value boundaries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnBounds {
    /// The minimum value observed for this column
    min: ScalarValue,
    /// The maximum value observed for this column  
    max: ScalarValue,
}

impl ColumnBounds {
    pub(crate) fn new(min: ScalarValue, max: ScalarValue) -> Self {
        Self { min, max }
    }
}

/// Represents the bounds for all join key columns from a single partition.
/// This contains the min/max values computed from one partition's build-side data.
#[derive(Debug, Clone)]
pub(crate) struct PartitionBounds {
    /// Partition identifier for debugging and determinism (not strictly necessary)
    partition: usize,
    /// Min/max bounds for each join key column in this partition.
    /// Index corresponds to the join key expression index.
    column_bounds: Vec<ColumnBounds>,
}

impl PartitionBounds {
    pub(crate) fn new(partition: usize, column_bounds: Vec<ColumnBounds>) -> Self {
        Self {
            partition,
            column_bounds,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.column_bounds.len()
    }

    pub(crate) fn get_column_bounds(&self, index: usize) -> Option<&ColumnBounds> {
        self.column_bounds.get(index)
    }
}

/// Coordinates dynamic filter bounds collection across multiple partitions with progressive filtering
///
/// This structure applies dynamic filters progressively as each partition completes, rather than
/// waiting for all partitions. This reduces probe-side scan overhead and improves performance.
///
/// ## Progressive Filtering Strategy
///
/// 1. Each partition computes bounds from its build-side data and immediately injects a filter
/// 2. Filters use hash-based expressions to avoid false negatives:
///    `(hash(cols) % num_partitions != partition_id OR col >= min AND col <= max)`
/// 3. As partitions complete, their specific bounds are added to the combined filter
/// 4. When all partitions complete, hash checks are removed for optimization
///
/// ## Filter Expression Evolution
///
/// **Progressive Phase**: `(hash(cols) % n != 0 OR bounds_0) AND (hash(cols) % n != 1 OR bounds_1) AND ...`
/// **Final Phase**: `bounds_0 OR bounds_1 OR bounds_2 OR ...`
///
/// ## Thread Safety
///
/// All fields use a single mutex to ensure correct coordination between concurrent
/// partition executions.
pub(crate) struct SharedBoundsAccumulator {
    /// Shared state protected by a single mutex to avoid ordering concerns
    inner: Mutex<SharedBoundsState>,
    /// Total number of partitions expected to report
    total_partitions: usize,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter bounds
    on_right: Vec<PhysicalExprRef>,
    /// Cached ConfigOptions to avoid repeated allocations
    config_options: Arc<ConfigOptions>,
}

/// State protected by SharedBoundsAccumulator's mutex
struct SharedBoundsState {
    /// Bounds from completed partitions.
    /// Each element represents the column bounds computed by one partition.
    bounds: Vec<PartitionBounds>,
    /// Set of partition IDs that have reported their bounds
    completed_partitions: HashSet<usize>,
    /// Whether we've optimized the filter to remove hash checks
    filter_optimized: bool,
}

impl SharedBoundsAccumulator {
    /// Creates a new SharedBoundsAccumulator configured for the given partition mode
    ///
    /// This method calculates how many times `collect_build_side` will be called based on the
    /// partition mode's execution pattern. This count is critical for determining when we have
    /// complete information from all partitions to build the dynamic filter.
    ///
    /// ## Partition Mode Execution Patterns
    ///
    /// - **CollectLeft**: Build side is collected ONCE from partition 0 and shared via `OnceFut`
    ///   across all output partitions. Each output partition calls `collect_build_side` to access the shared build data.
    ///   Although this results in multiple invocations, the  `report_partition_bounds` function contains deduplication logic to handle them safely.
    ///   Expected calls = number of output partitions.
    ///
    ///
    /// - **Partitioned**: Each partition independently builds its own hash table by calling
    ///   `collect_build_side` once. Expected calls = number of build partitions.
    ///
    /// - **Auto**: Placeholder mode resolved during optimization. Uses 1 as safe default since
    ///   the actual mode will be determined and a new bounds_accumulator created before execution.
    ///
    /// ## Why This Matters
    ///
    /// We cannot build a partial filter from some partitions - it would incorrectly eliminate
    /// valid join results. We must wait until we have complete bounds information from ALL
    /// relevant partitions before updating the dynamic filter.
    pub(crate) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        on_right: Vec<PhysicalExprRef>,
    ) -> Self {
        // Troubleshooting: If partition counts are incorrect, verify this logic matches
        // the actual execution pattern in collect_build_side()
        let total_partitions = match partition_mode {
            // Each output partition accesses shared build data
            PartitionMode::CollectLeft => {
                right_child.output_partitioning().partition_count()
            }
            // Each partition builds its own data
            PartitionMode::Partitioned => {
                left_child.output_partitioning().partition_count()
            }
            // Default value, will be resolved during optimization (does not exist once `execute()` is called; will be replaced by one of the other two)
            PartitionMode::Auto => unreachable!("PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"),
        };
        Self {
            inner: Mutex::new(SharedBoundsState {
                bounds: Vec::with_capacity(total_partitions),
                completed_partitions: HashSet::new(),
                filter_optimized: false,
            }),
            total_partitions,
            dynamic_filter,
            on_right,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    /// Create hash expression for the join keys: hash(col1, col2, ...)
    fn create_hash_expression(&self) -> Result<Arc<dyn PhysicalExpr>> {
        // Use the same random state as RepartitionExec for consistent partitioning
        // This ensures hash(row) % num_partitions produces the same partition assignment
        // as the original repartitioning operation
        let hash_udf = Arc::new(ScalarUDF::from(Hash::new_with_random_state(
            REPARTITION_RANDOM_STATE,
        )));

        // Create the hash expression using ScalarFunctionExpr
        Ok(Arc::new(ScalarFunctionExpr::new(
            "hash",
            hash_udf,
            self.on_right.clone(),
            Field::new("hash_result", DataType::UInt64, false).into(),
            Arc::clone(&self.config_options),
        )))
    }

    /// Create a bounds predicate for a single partition: (col >= min AND col <= max) for all columns
    fn create_partition_bounds_predicate(
        &self,
        partition_bounds: &PartitionBounds,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let mut column_predicates = Vec::with_capacity(partition_bounds.len());

        for (col_idx, right_expr) in self.on_right.iter().enumerate() {
            if let Some(column_bounds) = partition_bounds.get_column_bounds(col_idx) {
                // Create predicate: col >= min AND col <= max
                let min_expr = Arc::new(BinaryExpr::new(
                    Arc::clone(right_expr),
                    Operator::GtEq,
                    lit(column_bounds.min.clone()),
                )) as Arc<dyn PhysicalExpr>;
                let max_expr = Arc::new(BinaryExpr::new(
                    Arc::clone(right_expr),
                    Operator::LtEq,
                    lit(column_bounds.max.clone()),
                )) as Arc<dyn PhysicalExpr>;
                let range_expr =
                    Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr))
                        as Arc<dyn PhysicalExpr>;
                column_predicates.push(range_expr);
            }
        }

        // Combine all column predicates for this partition with AND
        if column_predicates.is_empty() {
            Ok(None)
        } else {
            let predicate = column_predicates
                .into_iter()
                .reduce(|acc, pred| {
                    Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                        as Arc<dyn PhysicalExpr>
                })
                .unwrap();
            Ok(Some(predicate))
        }
    }

    /// Create progressive filter: (hash(...) % n != partition_id OR bounds_predicate)
    fn create_progressive_partition_filter(
        &self,
        partition_bounds: &PartitionBounds,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let hash_expr = self.create_hash_expression()?;

        // hash(...) % total_partitions
        let modulo_expr = Arc::new(BinaryExpr::new(
            hash_expr,
            Operator::Modulo,
            lit(ScalarValue::UInt64(Some(self.total_partitions as u64))),
        )) as Arc<dyn PhysicalExpr>;

        // hash(...) % total_partitions != partition_id
        let hash_check = Arc::new(BinaryExpr::new(
            modulo_expr,
            Operator::NotEq,
            lit(ScalarValue::UInt64(Some(partition_bounds.partition as u64))),
        )) as Arc<dyn PhysicalExpr>;

        // Create bounds predicate for this partition
        let bounds_predicate =
            self.create_partition_bounds_predicate(partition_bounds)?;

        if let Some(bounds_predicate) = bounds_predicate {
            // Combine: (hash_check OR bounds_predicate)
            return Ok(Arc::new(BinaryExpr::new(
                hash_check,
                Operator::Or,
                bounds_predicate,
            )) as Arc<dyn PhysicalExpr>);
        } else {
            // No bounds, just return true
            return Ok(lit(true));
        }
    }

    /// Create final optimized filter from all partition bounds using OR logic
    pub(crate) fn create_optimized_filter_from_partition_bounds(
        &self,
        bounds: &[PartitionBounds],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if bounds.is_empty() {
            return Ok(lit(true));
        }

        // Create a predicate for each partition
        let mut partition_predicates = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.iter().sorted_by_key(|b| b.partition) {
            partition_predicates.push(
                self.create_partition_bounds_predicate(partition_bounds)?
                    .unwrap_or_else(|| lit(true)),
            );
        }
        // Combine all partition predicates with OR using HashedPhysicalExpr to avoid re-evaluation of the hash expression
        // This ensures that the hash expression is only computed once per row.
        let combined_predicate = Arc::new(HashedPhysicalExpr::new(
            self.create_hash_expression()?,
            partition_predicates,
        )) as Arc<dyn PhysicalExpr>;

        Ok(combined_predicate)
    }

    /// Create progressive filter from completed partitions using AND logic
    pub(crate) fn create_progressive_filter_from_partition_bounds(
        &self,
        bounds: &[PartitionBounds],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if bounds.is_empty() {
            return Ok(lit(true));
        }

        // Create a progressive filter for each completed partition
        let mut partition_filters = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.iter().sorted_by_key(|b| b.partition) {
            let progressive_filter =
                self.create_progressive_partition_filter(partition_bounds)?;
            partition_filters.push(progressive_filter);
        }

        // Combine all partition filters with AND
        let combined_filter = partition_filters
            .into_iter()
            .reduce(|acc, filter| {
                Arc::new(BinaryExpr::new(acc, Operator::And, filter))
                    as Arc<dyn PhysicalExpr>
            })
            .unwrap_or_else(|| lit(true));

        Ok(combined_filter)
    }

    /// Report bounds from a completed partition and immediately update the dynamic filter
    ///
    /// This method applies progressive filtering by immediately injecting a filter for each
    /// completed partition. The filter uses hash-based expressions to ensure correctness:
    /// `(hash(cols) % num_partitions != partition_id OR col >= min AND col <= max)`
    ///
    /// When all partitions have completed, the filter is optimized to remove hash checks.
    ///
    /// # Arguments
    /// * `left_side_partition_id` - The identifier for the **left-side** partition reporting its bounds
    /// * `partition_bounds` - The bounds computed by this partition (if any)
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed
    pub(crate) fn report_partition_bounds(
        &self,
        left_side_partition_id: usize,
        partition_bounds: Option<Vec<ColumnBounds>>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();

        // Skip processing if this partition has already reported its bounds to prevent duplicate updates
        if inner.completed_partitions.contains(&left_side_partition_id) {
            return Ok(());
        }

        // Store bounds and mark partition as completed
        if let Some(bounds) = partition_bounds {
            let should_push = if let Some(last_bound) = inner.bounds.last() {
                // In `PartitionMode::CollectLeft`, all streams on the left side share the same partition id (0).
                // Since this function can be called multiple times for that same partition, we must deduplicate
                // by checking against the last recorded bound.
                last_bound.partition != left_side_partition_id
            } else {
                true
            };

            if should_push {
                inner
                    .bounds
                    .push(PartitionBounds::new(left_side_partition_id, bounds));
            }
        }
        inner.completed_partitions.insert(left_side_partition_id);

        let all_partitions_complete =
            inner.completed_partitions.len() == self.total_partitions;

        // Create the appropriate filter based on completion status
        let filter_expr = if all_partitions_complete && !inner.filter_optimized {
            // All partitions complete - use optimized filter without hash checks
            inner.filter_optimized = true;
            self.create_optimized_filter_from_partition_bounds(&inner.bounds)?
        } else {
            // Progressive phase - use hash-based filter
            self.create_progressive_filter_from_partition_bounds(&inner.bounds)?
        };

        // Release lock before updating filter to avoid holding it during the update
        drop(inner);

        // Update the dynamic filter
        self.dynamic_filter.update(filter_expr)?;

        Ok(())
    }
}

impl fmt::Debug for SharedBoundsAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBoundsAccumulator")
    }
}

/// Physical expression that uses a hash value to select one of several expressions to evaluate.
/// The point is to avoid re-evaluating the hash expression multiple times.
/// This is very similar to a CASE expression, but the selection is based on the modulo of a hash value, i.e. something like (not valid SQL):
///
/// ```sql
/// CASE (hash(col) % n)
///    WHEN 0 THEN col >= 1 AND col <= 10
///    WHEN 1 THEN col >= 9 AND col <= 30
/// END
/// ```
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HashedPhysicalExpr {
    /// The expression used to get the hash value.
    /// This must return a UInt64 value.
    /// It will be moduloed by the number of expressions in `exprs` to determine which expression to evaluate.
    hash: Arc<dyn PhysicalExpr>,
    /// The expressions to evaluate based on the hash value.
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl HashedPhysicalExpr {
    #[allow(dead_code)]
    fn new(hash: Arc<dyn PhysicalExpr>, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        assert!(!exprs.is_empty(), "exprs must not be empty");
        Self { hash, exprs }
    }
}

impl std::hash::Hash for HashedPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.dyn_hash(state);
        for expr in &self.exprs {
            expr.dyn_hash(state);
        }
    }
}

impl PartialEq for HashedPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.hash.dyn_eq(&*other.hash) && self.exprs == other.exprs
    }
}

impl Eq for HashedPhysicalExpr {}

impl fmt::Display for HashedPhysicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r"HashedPhysicalExpr(hash: {}, exprs: [", self.hash)?;
        for (i, expr) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expr}")?;
        }
        write!(f, r"])")
    }
}

impl PhysicalExpr for HashedPhysicalExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = Vec::with_capacity(1 + self.exprs.len());
        children.push(&self.hash);
        children.extend(self.exprs.iter());
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 + self.exprs.len() {
            return Err(datafusion_common::DataFusionError::Internal(
                "HashedPhysicalExpr::with_new_children: incorrect number of children"
                    .to_string(),
            ));
        }
        let hash = Arc::clone(&children[0]);
        let exprs = children[1..].to_vec();
        Ok(Arc::new(HashedPhysicalExpr::new(hash, exprs)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HashedPhysicalExpr(hash: ")?;
        self.hash.fmt_sql(f)?;
        write!(f, r", exprs: [")?;
        for (i, expr) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, r"])")
    }

    fn data_type(&self, input_schema: &arrow::datatypes::Schema) -> Result<DataType> {
        // All expressions must have the same data type.
        let first_type = self.exprs[0].data_type(input_schema)?;
        for expr in &self.exprs[1..] {
            let expr_type = expr.data_type(input_schema)?;
            if expr_type != first_type {
                return Err(datafusion_common::DataFusionError::Internal(
                    "All expressions in HashedPhysicalExpr must have the same data type"
                        .to_string(),
                ));
            }
        }
        Ok(first_type)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        // If any expression is nullable, the result is nullable.
        for expr in &self.exprs {
            if expr.nullable(input_schema)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        let expr = &self.hash;
        let base_value = expr.evaluate(batch)?;
        let base_value = base_value.into_array(batch.num_rows())?;
        let base_nulls = is_null(base_value.as_ref())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        // We only consider non-null values while computing hash indices
        let mut remainder = not(&base_nulls)?;

        // If all values are null, return the null array
        if remainder.true_count() == 0 {
            return Ok(ColumnarValue::Array(current_value));
        }

        // Cast hash values to UInt64Array for modulo operations
        let hash_array = as_uint64_array(&base_value).map_err(|_| {
            internal_datafusion_err!("Hash expression must return UInt64 values")
        })?;

        // Create a mapping from hash index to row mask for efficient batch processing
        let num_expressions = self.exprs.len() as u64;
        let mut index_masks = vec![Vec::new(); self.exprs.len()];

        // Build masks for each expression index
        for row_idx in 0..batch.num_rows() {
            if remainder.value(row_idx) {
                if let Some(hash_value) =
                    hash_array.value(row_idx).checked_rem(num_expressions)
                {
                    let expr_index = hash_value as usize;
                    index_masks[expr_index].push(row_idx);
                }
            }
        }

        // Process each expression that has rows to evaluate
        for (expr_index, row_indices) in index_masks.iter().enumerate() {
            if row_indices.is_empty() {
                continue;
            }

            // Create boolean mask for this expression's rows
            let mut mask_values = vec![false; batch.num_rows()];
            for &row_idx in row_indices {
                mask_values[row_idx] = true;
            }
            let expr_mask = BooleanArray::from(mask_values);

            // Combine with remainder mask to ensure we only process non-null hash values
            let expr_mask = and(&expr_mask, &remainder)?;

            if expr_mask.true_count() == 0 {
                continue;
            }

            // Evaluate the selected expression for these rows
            let then_value =
                self.exprs[expr_index].evaluate_selection(batch, &expr_mask)?;

            // Merge the results into current_value
            current_value = match then_value {
                ColumnarValue::Scalar(ScalarValue::Null) => {
                    nullif(current_value.as_ref(), &expr_mask)?
                }
                ColumnarValue::Scalar(then_value) => {
                    zip(&expr_mask, &then_value.to_scalar()?, &current_value)?
                }
                ColumnarValue::Array(then_value) => {
                    zip(&expr_mask, &then_value, &current_value)?
                }
            };

            // Update remainder to exclude processed rows
            remainder = and_not(&remainder, &expr_mask)?;
        }

        Ok(ColumnarValue::Array(current_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::cast::as_int32_array;
    use datafusion_physical_expr::expressions::{col, lit};
    use std::sync::Arc;

    /// Simple test hash expression that doubles the input value
    #[derive(Debug, Clone)]
    struct DoublingHashExpr {
        input: Arc<dyn PhysicalExpr>,
    }

    impl DoublingHashExpr {
        fn new(input: Arc<dyn PhysicalExpr>) -> Self {
            Self { input }
        }
    }

    impl std::hash::Hash for DoublingHashExpr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.input.dyn_hash(state);
        }
    }

    impl PartialEq for DoublingHashExpr {
        fn eq(&self, other: &Self) -> bool {
            self.input.dyn_eq(&*other.input)
        }
    }

    impl Eq for DoublingHashExpr {}

    impl fmt::Display for DoublingHashExpr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "DoublingHash({})", self.input)
        }
    }

    impl PhysicalExpr for DoublingHashExpr {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::UInt64)
        }

        fn nullable(&self, input_schema: &Schema) -> Result<bool> {
            self.input.nullable(input_schema)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            let input_value = self.input.evaluate(batch)?;
            match input_value {
                ColumnarValue::Array(array) => {
                    // Convert to UInt64 and double each value
                    let uint64_array = array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "DoublingHashExpr expects UInt64 input".to_string(),
                            )
                        })?;

                    let doubled: Vec<Option<u64>> = uint64_array
                        .iter()
                        .map(|opt_val| opt_val.map(|val| val * 2))
                        .collect();

                    Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(doubled))))
                }
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::UInt64(Some(val)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(val * 2))))
                    }
                    ScalarValue::UInt64(None) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::UInt64(None)))
                    }
                    _ => Err(datafusion_common::DataFusionError::Internal(
                        "DoublingHashExpr expects UInt64 input".to_string(),
                    )),
                },
            }
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.input]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if children.len() != 1 {
                return Err(datafusion_common::DataFusionError::Internal(
                    "DoublingHashExpr expects exactly one child".to_string(),
                ));
            }
            Ok(Arc::new(DoublingHashExpr::new(children[0].clone())))
        }

        fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "DoublingHash(")?;
            self.input.fmt_sql(f)?;
            write!(f, ")")
        }
    }

    #[test]
    fn test_hashed_physical_expr_predictable_selection() -> Result<()> {
        // Create schema and test batch
        let schema = Schema::new(vec![Field::new("input", DataType::UInt64, false)]);

        // Test with input value 5
        // Hash: 5 * 2 = 10
        // Selection: 10 % 3 = 1 (should select index 1, which is lit(2))
        let input_array = Arc::new(UInt64Array::from(vec![5u64]));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input_array])?;

        // Create doubling hash expression
        let hash_expr = Arc::new(DoublingHashExpr::new(col("input", &schema)?));

        // Create three literal expressions: [lit(1), lit(2), lit(3)]
        let expr1 = lit(1i32); // index 0
        let expr2 = lit(2i32); // index 1 <- this should be selected
        let expr3 = lit(3i32); // index 2

        // Create HashedPhysicalExpr
        let hashed_expr = HashedPhysicalExpr::new(hash_expr, vec![expr1, expr2, expr3]);

        // Evaluate the expression
        let result = hashed_expr.evaluate(&batch)?;
        let result_array = result.into_array(batch.num_rows())?;
        let result_array = as_int32_array(&result_array)?;

        // Verify the result
        assert_eq!(result_array.len(), 1);
        assert_eq!(
            result_array.value(0),
            2,
            "Input 5 -> hash 10 -> 10 % 3 = 1 -> should select lit(2) at index 1"
        );

        Ok(())
    }

    #[test]
    fn test_hashed_physical_expr_multiple_predictable_values() -> Result<()> {
        // Create schema and test batch with multiple values
        let schema = Schema::new(vec![Field::new("input", DataType::UInt64, false)]);

        // Test with multiple input values to verify the pattern
        // Input 0: hash 0 -> 0 % 3 = 0 -> lit(10)
        // Input 1: hash 2 -> 2 % 3 = 2 -> lit(30)
        // Input 2: hash 4 -> 4 % 3 = 1 -> lit(20)
        // Input 3: hash 6 -> 6 % 3 = 0 -> lit(10)
        let input_array = Arc::new(UInt64Array::from(vec![0u64, 1u64, 2u64, 3u64]));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input_array])?;

        // Create doubling hash expression
        let hash_expr = Arc::new(DoublingHashExpr::new(col("input", &schema)?));

        // Create three literal expressions with distinct values for easy verification
        let expr1 = lit(10i32); // index 0
        let expr2 = lit(20i32); // index 1
        let expr3 = lit(30i32); // index 2

        // Create HashedPhysicalExpr
        let hashed_expr = HashedPhysicalExpr::new(hash_expr, vec![expr1, expr2, expr3]);

        // Evaluate the expression
        let result = hashed_expr.evaluate(&batch)?;
        let result_array = result.into_array(batch.num_rows())?;
        let result_array = as_int32_array(&result_array)?;

        // Verify the results
        assert_eq!(result_array.len(), 4);

        // Input 0: 0 * 2 = 0 -> 0 % 3 = 0 -> lit(10)
        assert_eq!(result_array.value(0), 10);

        // Input 1: 1 * 2 = 2 -> 2 % 3 = 2 -> lit(30)
        assert_eq!(result_array.value(1), 30);

        // Input 2: 2 * 2 = 4 -> 4 % 3 = 1 -> lit(20)
        assert_eq!(result_array.value(2), 20);

        // Input 3: 3 * 2 = 6 -> 6 % 3 = 0 -> lit(10)
        assert_eq!(result_array.value(3), 10);

        Ok(())
    }

    #[test]
    fn test_hashed_physical_expr_with_null_hash() -> Result<()> {
        // Create schema and test batch with null value
        let schema = Schema::new(vec![Field::new("input", DataType::UInt64, true)]);

        // Test with null input - should produce null output
        let input_array = Arc::new(UInt64Array::from(vec![Some(5u64), None]));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input_array])?;

        // Create doubling hash expression
        let hash_expr = Arc::new(DoublingHashExpr::new(col("input", &schema)?));

        // Create literal expressions
        let expr1 = lit(100i32);
        let expr2 = lit(200i32);

        // Create HashedPhysicalExpr
        let hashed_expr = HashedPhysicalExpr::new(hash_expr, vec![expr1, expr2]);

        // Evaluate the expression
        let result = hashed_expr.evaluate(&batch)?;
        let result_array = result.into_array(batch.num_rows())?;

        // Verify the results
        assert_eq!(result_array.len(), 2);

        // Check null status before casting
        assert!(result_array.is_null(1), "Second value should be null");

        let result_array = as_int32_array(&result_array)?;

        // First value: 5 * 2 = 10 -> 10 % 2 = 0 -> lit(100)
        assert_eq!(result_array.value(0), 100);

        Ok(())
    }
}
