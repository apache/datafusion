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

//! This file contains common subroutines for regular and symmetric hash join
//! related functionality, used both in join calculations and optimization rules.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::ops::IndexMut;
use std::sync::Arc;
use std::{fmt, usize};

use crate::joins::utils::JoinFilter;

use arrow::compute::concat_batches;
use arrow::datatypes::{ArrowNativeType, SchemaRef};
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{ArrowPrimitiveType, NativeAdapter, PrimitiveArray, RecordBatch};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, JoinSide, Result, ScalarValue};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::intervals::{Interval, IntervalBound};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

use hashbrown::raw::RawTable;
use hashbrown::HashSet;

/// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
///
/// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
/// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
///
/// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
/// As the key is a hash value, we need to check possible hash collisions in the probe stage
/// During this stage it might be the case that a row is contained the same hashmap value,
/// but the values don't match. Those are checked in the [`equal_rows_arr`](crate::joins::hash_join::equal_rows_arr) method.
///
/// The indices (values) are stored in a separate chained list stored in the `Vec<u64>`.
///
/// The first value (+1) is stored in the hashmap, whereas the next value is stored in array at the position value.
///
/// The chain can be followed until the value "0" has been reached, meaning the end of the list.
/// Also see chapter 5.3 of [Balancing vectorized query execution with bandwidth-optimized storage](https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487)
///
/// # Example
///
/// ``` text
/// See the example below:
/// Insert (1,1)
/// map:
/// ---------
/// | 1 | 2 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (2,2)
/// map:
/// ---------
/// | 1 | 2 |
/// | 2 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (1,3)
/// map:
/// ---------
/// | 1 | 4 |
/// | 2 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 0 |  <--- hash value 1 maps to 4,2 (which means indices values 3,1)
/// ---------------------
/// Insert (1,4)
/// map:
/// ---------
/// | 1 | 5 |
/// | 2 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 4 | <--- hash value 1 maps to 5,4,2 (which means indices values 4,3,1)
/// ---------------------
/// ```
pub struct JoinHashMap {
    // Stores hash value to last row index
    pub map: RawTable<(u64, u64)>,
    // Stores indices in chained list data structure
    pub next: Vec<u64>,
}

impl JoinHashMap {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        JoinHashMap {
            map: RawTable::with_capacity(capacity),
            next: vec![0; capacity],
        }
    }
}

/// Trait defining methods that must be implemented by a hash map type to be used for joins.
pub trait JoinHashMapType {
    /// The type of list used to store the hash values.
    type NextType: IndexMut<usize, Output = u64>;
    /// Extend with zero
    fn extend_zero(&mut self, len: usize);
    /// Returns mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut RawTable<(u64, u64)>, &mut Self::NextType);
    /// Returns a reference to the hash map.
    fn get_map(&self) -> &RawTable<(u64, u64)>;
    /// Returns a reference to the next.
    fn get_list(&self) -> &Self::NextType;
}

/// Implementation of `JoinHashMapType` for `JoinHashMap`.
impl JoinHashMapType for JoinHashMap {
    type NextType = Vec<u64>;

    // Void implementation
    fn extend_zero(&mut self, _: usize) {}

    /// Get mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut RawTable<(u64, u64)>, &mut Self::NextType) {
        (&mut self.map, &mut self.next)
    }

    /// Get a reference to the hash map.
    fn get_map(&self) -> &RawTable<(u64, u64)> {
        &self.map
    }

    /// Get a reference to the next.
    fn get_list(&self) -> &Self::NextType {
        &self.next
    }
}

/// Implementation of `JoinHashMapType` for `PruningJoinHashMap`.
impl JoinHashMapType for PruningJoinHashMap {
    type NextType = VecDeque<u64>;

    // Extend with zero
    fn extend_zero(&mut self, len: usize) {
        self.next.resize(self.next.len() + len, 0)
    }

    /// Get mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut RawTable<(u64, u64)>, &mut Self::NextType) {
        (&mut self.map, &mut self.next)
    }

    /// Get a reference to the hash map.
    fn get_map(&self) -> &RawTable<(u64, u64)> {
        &self.map
    }

    /// Get a reference to the next.
    fn get_list(&self) -> &Self::NextType {
        &self.next
    }
}

impl fmt::Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

/// The `PruningJoinHashMap` is similar to a regular `JoinHashMap`, but with
/// the capability of pruning elements in an efficient manner. This structure
/// is particularly useful for cases where it's necessary to remove elements
/// from the map based on their buffer order.
///
/// # Example
///
/// ``` text
/// Let's continue the example of `JoinHashMap` and then show how `PruningJoinHashMap` would
/// handle the pruning scenario.
///
/// Insert the pair (1,4) into the `PruningJoinHashMap`:
/// map:
/// ---------
/// | 1 | 5 |
/// | 2 | 3 |
/// ---------
/// list:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 4 | <--- hash value 1 maps to 5,4,2 (which means indices values 4,3,1)
/// ---------------------
///
/// Now, let's prune 3 rows from `PruningJoinHashMap`:
/// map:
/// ---------
/// | 1 | 5 |
/// ---------
/// list:
/// ---------
/// | 2 | 4 | <--- hash value 1 maps to 2 (5 - 3), 1 (4 - 3), NA (2 - 3) (which means indices values 1,0)
/// ---------
///
/// After pruning, the | 2 | 3 | entry is deleted from `PruningJoinHashMap` since
/// there are no values left for this key.
/// ```
pub struct PruningJoinHashMap {
    /// Stores hash value to last row index
    pub map: RawTable<(u64, u64)>,
    /// Stores indices in chained list data structure
    pub next: VecDeque<u64>,
}

impl PruningJoinHashMap {
    /// Constructs a new `PruningJoinHashMap` with the given capacity.
    /// Both the map and the list are pre-allocated with the provided capacity.
    ///
    /// # Arguments
    /// * `capacity`: The initial capacity of the hash map.
    ///
    /// # Returns
    /// A new instance of `PruningJoinHashMap`.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        PruningJoinHashMap {
            map: RawTable::with_capacity(capacity),
            next: VecDeque::with_capacity(capacity),
        }
    }

    /// Shrinks the capacity of the hash map, if necessary, based on the
    /// provided scale factor.
    ///
    /// # Arguments
    /// * `scale_factor`: The scale factor that determines how conservative the
    ///   shrinking strategy is. The capacity will be reduced by 1/`scale_factor`
    ///   when necessary.
    ///
    /// # Note
    /// Increasing the scale factor results in less aggressive capacity shrinking,
    /// leading to potentially higher memory usage but fewer resizes. Conversely,
    /// decreasing the scale factor results in more aggressive capacity shrinking,
    /// potentially leading to lower memory usage but more frequent resizing.
    pub(crate) fn shrink_if_necessary(&mut self, scale_factor: usize) {
        let capacity = self.map.capacity();

        if capacity > scale_factor * self.map.len() {
            let new_capacity = (capacity * (scale_factor - 1)) / scale_factor;
            // Resize the map with the new capacity.
            self.map.shrink_to(new_capacity, |(hash, _)| *hash)
        }
    }

    /// Calculates the size of the `PruningJoinHashMap` in bytes.
    ///
    /// # Returns
    /// The size of the hash map in bytes.
    pub(crate) fn size(&self) -> usize {
        self.map.allocation_info().1.size()
            + self.next.capacity() * std::mem::size_of::<u64>()
    }

    /// Removes hash values from the map and the list based on the given pruning
    /// length and deleting offset.
    ///
    /// # Arguments
    /// * `prune_length`: The number of elements to remove from the list.
    /// * `deleting_offset`: The offset used to determine which hash values to remove from the map.
    ///
    /// # Returns
    /// A `Result` indicating whether the operation was successful.
    pub(crate) fn prune_hash_values(
        &mut self,
        prune_length: usize,
        deleting_offset: u64,
        shrink_factor: usize,
    ) -> Result<()> {
        // Remove elements from the list based on the pruning length.
        self.next.drain(0..prune_length);

        // Calculate the keys that should be removed from the map.
        let removable_keys = unsafe {
            self.map
                .iter()
                .map(|bucket| bucket.as_ref())
                .filter_map(|(hash, tail_index)| {
                    (*tail_index < prune_length as u64 + deleting_offset).then_some(*hash)
                })
                .collect::<Vec<_>>()
        };

        // Remove the keys from the map.
        removable_keys.into_iter().for_each(|hash_value| {
            self.map
                .remove_entry(hash_value, |(hash, _)| hash_value == *hash);
        });

        // Shrink the map if necessary.
        self.shrink_if_necessary(shrink_factor);
        Ok(())
    }
}

fn check_filter_expr_contains_sort_information(
    expr: &Arc<dyn PhysicalExpr>,
    reference: &Arc<dyn PhysicalExpr>,
) -> bool {
    expr.eq(reference)
        || expr
            .children()
            .iter()
            .any(|e| check_filter_expr_contains_sort_information(e, reference))
}

/// Create a one to one mapping from main columns to filter columns using
/// filter column indices. A column index looks like:
/// ```text
/// ColumnIndex {
///     index: 0, // field index in main schema
///     side: JoinSide::Left, // child side
/// }
/// ```
pub fn map_origin_col_to_filter_col(
    filter: &JoinFilter,
    schema: &SchemaRef,
    side: &JoinSide,
) -> Result<HashMap<Column, Column>> {
    let filter_schema = filter.schema();
    let mut col_to_col_map: HashMap<Column, Column> = HashMap::new();
    for (filter_schema_index, index) in filter.column_indices().iter().enumerate() {
        if index.side.eq(side) {
            // Get the main field from column index:
            let main_field = schema.field(index.index);
            // Create a column expression:
            let main_col = Column::new_with_schema(main_field.name(), schema.as_ref())?;
            // Since the order of by filter.column_indices() is the same with
            // that of intermediate schema fields, we can get the column directly.
            let filter_field = filter_schema.field(filter_schema_index);
            let filter_col = Column::new(filter_field.name(), filter_schema_index);
            // Insert mapping:
            col_to_col_map.insert(main_col, filter_col);
        }
    }
    Ok(col_to_col_map)
}

/// This function analyzes [`PhysicalSortExpr`] graphs with respect to monotonicity
/// (sorting) properties. This is necessary since monotonically increasing and/or
/// decreasing expressions are required when using join filter expressions for
/// data pruning purposes.
///
/// The method works as follows:
/// 1. Maps the original columns to the filter columns using the [`map_origin_col_to_filter_col`] function.
/// 2. Collects all columns in the sort expression using the [`collect_columns`] function.
/// 3. Checks if all columns are included in the map we obtain in the first step.
/// 4. If all columns are included, the sort expression is converted into a filter expression using
///    the [`convert_filter_columns`] function.
/// 5. Searches for the converted filter expression in the filter expression using the
///    [`check_filter_expr_contains_sort_information`] function.
/// 6. If an exact match is found, returns the converted filter expression as [`Some(Arc<dyn PhysicalExpr>)`].
/// 7. If all columns are not included or an exact match is not found, returns [`None`].
///
/// Examples:
/// Consider the filter expression "a + b > c + 10 AND a + b < c + 100".
/// 1. If the expression "a@ + d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 2. If the expression "d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 3. If the expression "a@ + b@ + c@" is sorted, all columns are represented in the filter expression. However,
///    there is no exact match, so this expression does not indicate pruning.
pub fn convert_sort_expr_with_filter_schema(
    side: &JoinSide,
    filter: &JoinFilter,
    schema: &SchemaRef,
    sort_expr: &PhysicalSortExpr,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let column_map = map_origin_col_to_filter_col(filter, schema, side)?;
    let expr = sort_expr.expr.clone();
    // Get main schema columns:
    let expr_columns = collect_columns(&expr);
    // Calculation is possible with `column_map` since sort exprs belong to a child.
    let all_columns_are_included =
        expr_columns.iter().all(|col| column_map.contains_key(col));
    if all_columns_are_included {
        // Since we are sure that one to one column mapping includes all columns, we convert
        // the sort expression into a filter expression.
        let converted_filter_expr = expr.transform_up(&|p| {
            convert_filter_columns(p.as_ref(), &column_map).map(|transformed| {
                match transformed {
                    Some(transformed) => Transformed::Yes(transformed),
                    None => Transformed::No(p),
                }
            })
        })?;
        // Search the converted `PhysicalExpr` in filter expression; if an exact
        // match is found, use this sorted expression in graph traversals.
        if check_filter_expr_contains_sort_information(
            filter.expression(),
            &converted_filter_expr,
        ) {
            return Ok(Some(converted_filter_expr));
        }
    }
    Ok(None)
}

/// This function is used to build the filter expression based on the sort order of input columns.
///
/// It first calls the [`convert_sort_expr_with_filter_schema`] method to determine if the sort
/// order of columns can be used in the filter expression. If it returns a [`Some`] value, the
/// method wraps the result in a [`SortedFilterExpr`] instance with the original sort expression and
/// the converted filter expression. Otherwise, this function returns an error.
///
/// The `SortedFilterExpr` instance contains information about the sort order of columns that can
/// be used in the filter expression, which can be used to optimize the query execution process.
pub fn build_filter_input_order(
    side: JoinSide,
    filter: &JoinFilter,
    schema: &SchemaRef,
    order: &PhysicalSortExpr,
) -> Result<Option<SortedFilterExpr>> {
    let opt_expr = convert_sort_expr_with_filter_schema(&side, filter, schema, order)?;
    Ok(opt_expr.map(|filter_expr| SortedFilterExpr::new(order.clone(), filter_expr)))
}

/// Convert a physical expression into a filter expression using the given
/// column mapping information.
fn convert_filter_columns(
    input: &dyn PhysicalExpr,
    column_map: &HashMap<Column, Column>,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Attempt to downcast the input expression to a Column type.
    Ok(if let Some(col) = input.as_any().downcast_ref::<Column>() {
        // If the downcast is successful, retrieve the corresponding filter column.
        column_map.get(col).map(|c| Arc::new(c.clone()) as _)
    } else {
        // If the downcast fails, return the input expression as is.
        None
    })
}

/// The [SortedFilterExpr] object represents a sorted filter expression. It
/// contains the following information: The origin expression, the filter
/// expression, an interval encapsulating expression bounds, and a stable
/// index identifying the expression in the expression DAG.
///
/// Physical schema of a [JoinFilter]'s intermediate batch combines two sides
/// and uses new column names. In this process, a column exchange is done so
/// we can utilize sorting information while traversing the filter expression
/// DAG for interval calculations. When evaluating the inner buffer, we use
/// `origin_sorted_expr`.
#[derive(Debug, Clone)]
pub struct SortedFilterExpr {
    /// Sorted expression from a join side (i.e. a child of the join)
    origin_sorted_expr: PhysicalSortExpr,
    /// Expression adjusted for filter schema.
    filter_expr: Arc<dyn PhysicalExpr>,
    /// Interval containing expression bounds
    interval: Interval,
    /// Node index in the expression DAG
    node_index: usize,
}

impl SortedFilterExpr {
    /// Constructor
    pub fn new(
        origin_sorted_expr: PhysicalSortExpr,
        filter_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            origin_sorted_expr,
            filter_expr,
            interval: Interval::default(),
            node_index: 0,
        }
    }
    /// Get origin expr information
    pub fn origin_sorted_expr(&self) -> &PhysicalSortExpr {
        &self.origin_sorted_expr
    }
    /// Get filter expr information
    pub fn filter_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter_expr
    }
    /// Get interval information
    pub fn interval(&self) -> &Interval {
        &self.interval
    }
    /// Sets interval
    pub fn set_interval(&mut self, interval: Interval) {
        self.interval = interval;
    }
    /// Node index in ExprIntervalGraph
    pub fn node_index(&self) -> usize {
        self.node_index
    }
    /// Node index setter in ExprIntervalGraph
    pub fn set_node_index(&mut self, node_index: usize) {
        self.node_index = node_index;
    }
}

/// Calculate the filter expression intervals.
///
/// This function updates the `interval` field of each `SortedFilterExpr` based
/// on the first or the last value of the expression in `build_input_buffer`
/// and `probe_batch`.
///
/// # Arguments
///
/// * `build_input_buffer` - The [RecordBatch] on the build side of the join.
/// * `build_sorted_filter_expr` - Build side [SortedFilterExpr] to update.
/// * `probe_batch` - The `RecordBatch` on the probe side of the join.
/// * `probe_sorted_filter_expr` - Probe side `SortedFilterExpr` to update.
///
/// ### Note
/// ```text
///
/// Interval arithmetic is used to calculate viable join ranges for build-side
/// pruning. This is done by first creating an interval for join filter values in
/// the build side of the join, which spans [-∞, FV] or [FV, ∞] depending on the
/// ordering (descending/ascending) of the filter expression. Here, FV denotes the
/// first value on the build side. This range is then compared with the probe side
/// interval, which either spans [-∞, LV] or [LV, ∞] depending on the ordering
/// (ascending/descending) of the probe side. Here, LV denotes the last value on
/// the probe side.
///
/// As a concrete example, consider the following query:
///
///   SELECT * FROM left_table, right_table
///   WHERE
///     left_key = right_key AND
///     a > b - 3 AND
///     a < b + 10
///
/// where columns "a" and "b" come from tables "left_table" and "right_table",
/// respectively. When a new `RecordBatch` arrives at the right side, the
/// condition a > b - 3 will possibly indicate a prunable range for the left
/// side. Conversely, when a new `RecordBatch` arrives at the left side, the
/// condition a < b + 10 will possibly indicate prunability for the right side.
/// Let’s inspect what happens when a new RecordBatch` arrives at the right
/// side (i.e. when the left side is the build side):
///
///         Build      Probe
///       +-------+  +-------+
///       | a | z |  | b | y |
///       |+--|--+|  |+--|--+|
///       | 1 | 2 |  | 4 | 3 |
///       |+--|--+|  |+--|--+|
///       | 3 | 1 |  | 4 | 3 |
///       |+--|--+|  |+--|--+|
///       | 5 | 7 |  | 6 | 1 |
///       |+--|--+|  |+--|--+|
///       | 7 | 1 |  | 6 | 3 |
///       +-------+  +-------+
///
/// In this case, the interval representing viable (i.e. joinable) values for
/// column "a" is [1, ∞], and the interval representing possible future values
/// for column "b" is [6, ∞]. With these intervals at hand, we next calculate
/// intervals for the whole filter expression and propagate join constraint by
/// traversing the expression graph.
/// ```
pub fn calculate_filter_expr_intervals(
    build_input_buffer: &RecordBatch,
    build_sorted_filter_expr: &mut SortedFilterExpr,
    probe_batch: &RecordBatch,
    probe_sorted_filter_expr: &mut SortedFilterExpr,
) -> Result<()> {
    // If either build or probe side has no data, return early:
    if build_input_buffer.num_rows() == 0 || probe_batch.num_rows() == 0 {
        return Ok(());
    }
    // Calculate the interval for the build side filter expression (if present):
    update_filter_expr_interval(
        &build_input_buffer.slice(0, 1),
        build_sorted_filter_expr,
    )?;
    // Calculate the interval for the probe side filter expression (if present):
    update_filter_expr_interval(
        &probe_batch.slice(probe_batch.num_rows() - 1, 1),
        probe_sorted_filter_expr,
    )
}

/// This is a subroutine of the function [`calculate_filter_expr_intervals`].
/// It constructs the current interval using the given `batch` and updates
/// the filter expression (i.e. `sorted_expr`) with this interval.
pub fn update_filter_expr_interval(
    batch: &RecordBatch,
    sorted_expr: &mut SortedFilterExpr,
) -> Result<()> {
    // Evaluate the filter expression and convert the result to an array:
    let array = sorted_expr
        .origin_sorted_expr()
        .expr
        .evaluate(batch)?
        .into_array(1)?;
    // Convert the array to a ScalarValue:
    let value = ScalarValue::try_from_array(&array, 0)?;
    // Create a ScalarValue representing positive or negative infinity for the same data type:
    let unbounded = IntervalBound::make_unbounded(value.data_type())?;
    // Update the interval with lower and upper bounds based on the sort option:
    let interval = if sorted_expr.origin_sorted_expr().options.descending {
        Interval::new(unbounded, IntervalBound::new(value, false))
    } else {
        Interval::new(IntervalBound::new(value, false), unbounded)
    };
    // Set the calculated interval for the sorted filter expression:
    sorted_expr.set_interval(interval);
    Ok(())
}

/// Get the anti join indices from the visited hash set.
///
/// This method returns the indices from the original input that were not present in the visited hash set.
///
/// # Arguments
///
/// * `prune_length` - The length of the pruned record batch.
/// * `deleted_offset` - The offset to the indices.
/// * `visited_rows` - The hash set of visited indices.
///
/// # Returns
///
/// A `PrimitiveArray` of the anti join indices.
pub fn get_pruning_anti_indices<T: ArrowPrimitiveType>(
    prune_length: usize,
    deleted_offset: usize,
    visited_rows: &HashSet<usize>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(prune_length);
    bitmap.append_n(prune_length, false);
    // mark the indices as true if they are present in the visited hash set
    for v in 0..prune_length {
        let row = v + deleted_offset;
        bitmap.set_bit(v, visited_rows.contains(&row));
    }
    // get the anti index
    (0..prune_length)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect()
}

/// This method creates a boolean buffer from the visited rows hash set
/// and the indices of the pruned record batch slice.
///
/// It gets the indices from the original input that were present in the visited hash set.
///
/// # Arguments
///
/// * `prune_length` - The length of the pruned record batch.
/// * `deleted_offset` - The offset to the indices.
/// * `visited_rows` - The hash set of visited indices.
///
/// # Returns
///
/// A [PrimitiveArray] of the specified type T, containing the semi indices.
pub fn get_pruning_semi_indices<T: ArrowPrimitiveType>(
    prune_length: usize,
    deleted_offset: usize,
    visited_rows: &HashSet<usize>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(prune_length);
    bitmap.append_n(prune_length, false);
    // mark the indices as true if they are present in the visited hash set
    (0..prune_length).for_each(|v| {
        let row = &(v + deleted_offset);
        bitmap.set_bit(v, visited_rows.contains(row));
    });
    // get the semi index
    (0..prune_length)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect::<PrimitiveArray<T>>()
}

pub fn combine_two_batches(
    output_schema: &SchemaRef,
    left_batch: Option<RecordBatch>,
    right_batch: Option<RecordBatch>,
) -> Result<Option<RecordBatch>> {
    match (left_batch, right_batch) {
        (Some(batch), None) | (None, Some(batch)) => {
            // If only one of the batches are present, return it:
            Ok(Some(batch))
        }
        (Some(left_batch), Some(right_batch)) => {
            // If both batches are present, concatenate them:
            concat_batches(output_schema, &[left_batch, right_batch])
                .map_err(DataFusionError::ArrowError)
                .map(Some)
        }
        (None, None) => {
            // If neither is present, return an empty batch:
            Ok(None)
        }
    }
}

/// Records the visited indices from the input `PrimitiveArray` of type `T` into the given hash set `visited`.
/// This function will insert the indices (offset by `offset`) into the `visited` hash set.
///
/// # Arguments
///
/// * `visited` - A hash set to store the visited indices.
/// * `offset` - An offset to the indices in the `PrimitiveArray`.
/// * `indices` - The input `PrimitiveArray` of type `T` which stores the indices to be recorded.
///
pub fn record_visited_indices<T: ArrowPrimitiveType>(
    visited: &mut HashSet<usize>,
    offset: usize,
    indices: &PrimitiveArray<T>,
) {
    for i in indices.values() {
        visited.insert(i.as_usize() + offset);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        expressions::Column,
        expressions::PhysicalSortExpr,
        joins::utils::{ColumnIndex, JoinFilter},
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, cast, col, lit};
    use std::sync::Arc;

    /// Filter expr for a + b > c + 10 AND a + b < c + 100
    pub(crate) fn complicated_filter(
        filter_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            cast(
                binary(
                    col("0", filter_schema)?,
                    Operator::Plus,
                    col("1", filter_schema)?,
                    filter_schema,
                )?,
                filter_schema,
                DataType::Int64,
            )?,
            Operator::Gt,
            binary(
                cast(col("2", filter_schema)?, filter_schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(10))),
                filter_schema,
            )?,
            filter_schema,
        )?;

        let right_expr = binary(
            cast(
                binary(
                    col("0", filter_schema)?,
                    Operator::Plus,
                    col("1", filter_schema)?,
                    filter_schema,
                )?,
                filter_schema,
                DataType::Int64,
            )?,
            Operator::Lt,
            binary(
                cast(col("2", filter_schema)?, filter_schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(100))),
                filter_schema,
            )?,
            filter_schema,
        )?;
        binary(left_expr, Operator::And, right_expr, filter_schema)
    }

    #[test]
    fn test_column_exchange() -> Result<()> {
        let left_child_schema =
            Schema::new(vec![Field::new("left_1", DataType::Int32, true)]);
        // Sorting information for the left side:
        let left_child_sort_expr = PhysicalSortExpr {
            expr: col("left_1", &left_child_schema)?,
            options: SortOptions::default(),
        };

        let right_child_schema = Schema::new(vec![
            Field::new("right_1", DataType::Int32, true),
            Field::new("right_2", DataType::Int32, true),
        ]);
        // Sorting information for the right side:
        let right_child_sort_expr = PhysicalSortExpr {
            expr: binary(
                col("right_1", &right_child_schema)?,
                Operator::Plus,
                col("right_2", &right_child_schema)?,
                &right_child_schema,
            )?,
            options: SortOptions::default(),
        };

        let intermediate_schema = Schema::new(vec![
            Field::new("filter_1", DataType::Int32, true),
            Field::new("filter_2", DataType::Int32, true),
            Field::new("filter_3", DataType::Int32, true),
        ]);
        // Our filter expression is: left_1 > right_1 + right_2.
        let filter_left = col("filter_1", &intermediate_schema)?;
        let filter_right = binary(
            col("filter_2", &intermediate_schema)?,
            Operator::Plus,
            col("filter_3", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let filter_expr = binary(
            filter_left.clone(),
            Operator::Gt,
            filter_right.clone(),
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let left_sort_filter_expr = build_filter_input_order(
            JoinSide::Left,
            &filter,
            &Arc::new(left_child_schema),
            &left_child_sort_expr,
        )?
        .unwrap();
        assert!(left_child_sort_expr.eq(left_sort_filter_expr.origin_sorted_expr()));

        let right_sort_filter_expr = build_filter_input_order(
            JoinSide::Right,
            &filter,
            &Arc::new(right_child_schema),
            &right_child_sort_expr,
        )?
        .unwrap();
        assert!(right_child_sort_expr.eq(right_sort_filter_expr.origin_sorted_expr()));

        // Assert that adjusted (left) filter expression matches with `left_child_sort_expr`:
        assert!(filter_left.eq(left_sort_filter_expr.filter_expr()));
        // Assert that adjusted (right) filter expression matches with `right_child_sort_expr`:
        assert!(filter_right.eq(right_sort_filter_expr.filter_expr()));
        Ok(())
    }

    #[test]
    fn test_column_collector() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&schema)?;
        let columns = collect_columns(&filter_expr);
        assert_eq!(columns.len(), 3);
        Ok(())
    }

    #[test]
    fn find_expr_inside_expr() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&schema)?;

        let expr_1 = Arc::new(Column::new("gnz", 0)) as _;
        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_1
        ));

        let expr_2 = col("1", &schema)? as _;

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_2
        ));

        let expr_3 = cast(
            binary(
                col("0", &schema)?,
                Operator::Plus,
                col("1", &schema)?,
                &schema,
            )?,
            &schema,
            DataType::Int64,
        )?;

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_3
        ));

        let expr_4 = Arc::new(Column::new("1", 42)) as _;

        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_4,
        ));
        Ok(())
    }

    #[test]
    fn build_sorted_expr() -> Result<()> {
        let left_schema = Schema::new(vec![
            Field::new("la1", DataType::Int32, false),
            Field::new("lb1", DataType::Int32, false),
            Field::new("lc1", DataType::Int32, false),
            Field::new("lt1", DataType::Int32, false),
            Field::new("la2", DataType::Int32, false),
            Field::new("la1_des", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![
            Field::new("ra1", DataType::Int32, false),
            Field::new("rb1", DataType::Int32, false),
            Field::new("rc1", DataType::Int32, false),
            Field::new("rt1", DataType::Int32, false),
            Field::new("ra2", DataType::Int32, false),
            Field::new("ra1_des", DataType::Int32, false),
        ]);

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let left_schema = Arc::new(left_schema);
        let right_schema = Arc::new(right_schema);

        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: col("la1", left_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: col("lt1", left_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_none());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: col("ra1", right_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: col("rb1", right_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_none());

        Ok(())
    }

    // Test the case when we have an "ORDER BY a + b", and join filter condition includes "a - b".
    #[test]
    fn sorted_filter_expr_build() -> Result<()> {
        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
        ]);
        let filter_expr = binary(
            col("0", &intermediate_schema)?,
            Operator::Minus,
            col("1", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let sorted = PhysicalSortExpr {
            expr: binary(
                col("a", &schema)?,
                Operator::Plus,
                col("b", &schema)?,
                &schema,
            )?,
            options: SortOptions::default(),
        };

        let res = convert_sort_expr_with_filter_schema(
            &JoinSide::Left,
            &filter,
            &Arc::new(schema),
            &sorted,
        )?;
        assert!(res.is_none());
        Ok(())
    }

    #[test]
    fn test_shrink_if_necessary() {
        let scale_factor = 4;
        let mut join_hash_map = PruningJoinHashMap::with_capacity(100);
        let data_size = 2000;
        let deleted_part = 3 * data_size / 4;
        // Add elements to the JoinHashMap
        for hash_value in 0..data_size {
            join_hash_map.map.insert(
                hash_value,
                (hash_value, hash_value),
                |(hash, _)| *hash,
            );
        }

        assert_eq!(join_hash_map.map.len(), data_size as usize);
        assert!(join_hash_map.map.capacity() >= data_size as usize);

        // Remove some elements from the JoinHashMap
        for hash_value in 0..deleted_part {
            join_hash_map
                .map
                .remove_entry(hash_value, |(hash, _)| hash_value == *hash);
        }

        assert_eq!(join_hash_map.map.len(), (data_size - deleted_part) as usize);

        // Old capacity
        let old_capacity = join_hash_map.map.capacity();

        // Test shrink_if_necessary
        join_hash_map.shrink_if_necessary(scale_factor);

        // The capacity should be reduced by the scale factor
        let new_expected_capacity =
            join_hash_map.map.capacity() * (scale_factor - 1) / scale_factor;
        assert!(join_hash_map.map.capacity() >= new_expected_capacity);
        assert!(join_hash_map.map.capacity() <= old_capacity);
    }
}
