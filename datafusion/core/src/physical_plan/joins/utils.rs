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

//! Join related functionality used both on logical and physical plans

use arrow::array::{
    downcast_array, new_null_array, Array, BooleanBufferBuilder, UInt32Array,
    UInt32Builder, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{Field, Schema, SchemaBuilder};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_physical_expr::expressions::Column;
use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt};
use parking_lot::Mutex;
use std::cmp::max;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::usize;

use datafusion_common::cast::as_boolean_array;
use datafusion_common::{ScalarValue, SharedResult};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::{EquivalentClass, PhysicalExpr};

use datafusion_common::JoinType;
use datafusion_common::{DataFusionError, Result};

use crate::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use crate::physical_plan::SchemaRef;
use crate::physical_plan::{
    ColumnStatistics, EquivalenceProperties, ExecutionPlan, Partitioning, Statistics,
};

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(Column, Column)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(Column, Column)];

/// Checks whether the schemas "left" and "right" and columns "on" represent a valid join.
/// They are valid whenever their columns' intersection equals the set `on`
pub fn check_join_is_valid(left: &Schema, right: &Schema, on: JoinOnRef) -> Result<()> {
    let left: HashSet<Column> = left
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();
    let right: HashSet<Column> = right
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();

    check_join_set_is_valid(&left, &right, on)
}

/// Checks whether the sets left, right and on compose a valid join.
/// They are valid whenever their intersection equals the set `on`
fn check_join_set_is_valid(
    left: &HashSet<Column>,
    right: &HashSet<Column>,
    on: &[(Column, Column)],
) -> Result<()> {
    let on_left = &on.iter().map(|on| on.0.clone()).collect::<HashSet<_>>();
    let left_missing = on_left.difference(left).collect::<HashSet<_>>();

    let on_right = &on.iter().map(|on| on.1.clone()).collect::<HashSet<_>>();
    let right_missing = on_right.difference(right).collect::<HashSet<_>>();

    if !left_missing.is_empty() | !right_missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
                "The left or right side of the join does not have all columns on \"on\": \nMissing on the left: {left_missing:?}\nMissing on the right: {right_missing:?}",
            )));
    };

    Ok(())
}

/// Calculate the OutputPartitioning for Partitioned Join
pub fn partitioned_join_output_partitioning(
    join_type: JoinType,
    left_partitioning: Partitioning,
    right_partitioning: Partitioning,
    left_columns_len: usize,
) -> Partitioning {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
            left_partitioning
        }
        JoinType::RightSemi | JoinType::RightAnti => right_partitioning,
        JoinType::Right => {
            adjust_right_output_partitioning(right_partitioning, left_columns_len)
        }
        JoinType::Full => {
            Partitioning::UnknownPartitioning(right_partitioning.partition_count())
        }
    }
}

/// Adjust the right out partitioning to new Column Index
pub fn adjust_right_output_partitioning(
    right_partitioning: Partitioning,
    left_columns_len: usize,
) -> Partitioning {
    match right_partitioning {
        Partitioning::RoundRobinBatch(size) => Partitioning::RoundRobinBatch(size),
        Partitioning::UnknownPartitioning(size) => {
            Partitioning::UnknownPartitioning(size)
        }
        Partitioning::Hash(exprs, size) => {
            let new_exprs = exprs
                .into_iter()
                .map(|expr| {
                    expr.transform_down(&|e| match e.as_any().downcast_ref::<Column>() {
                        Some(col) => Ok(Transformed::Yes(Arc::new(Column::new(
                            col.name(),
                            left_columns_len + col.index(),
                        )))),
                        None => Ok(Transformed::No(e)),
                    })
                    .unwrap()
                })
                .collect::<Vec<_>>();
            Partitioning::Hash(new_exprs, size)
        }
    }
}

/// Combine the Equivalence Properties for Join Node
pub fn combine_join_equivalence_properties(
    join_type: JoinType,
    left_properties: EquivalenceProperties,
    right_properties: EquivalenceProperties,
    left_columns_len: usize,
    on: &[(Column, Column)],
    schema: SchemaRef,
) -> EquivalenceProperties {
    let mut new_properties = EquivalenceProperties::new(schema);
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            new_properties.extend(left_properties.classes().to_vec());
            let new_right_properties = right_properties
                .classes()
                .iter()
                .map(|prop| {
                    let new_head = Column::new(
                        prop.head().name(),
                        left_columns_len + prop.head().index(),
                    );
                    let new_others = prop
                        .others()
                        .iter()
                        .map(|col| {
                            Column::new(col.name(), left_columns_len + col.index())
                        })
                        .collect::<Vec<_>>();
                    EquivalentClass::new(new_head, new_others)
                })
                .collect::<Vec<_>>();

            new_properties.extend(new_right_properties);
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            new_properties.extend(left_properties.classes().to_vec())
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            new_properties.extend(right_properties.classes().to_vec())
        }
    }

    if join_type == JoinType::Inner {
        on.iter().for_each(|(column1, column2)| {
            let new_column2 =
                Column::new(column2.name(), left_columns_len + column2.index());
            new_properties.add_equal_conditions((column1, &new_column2))
        })
    }
    new_properties
}

/// Calculate the Equivalence Properties for CrossJoin Node
pub fn cross_join_equivalence_properties(
    left_properties: EquivalenceProperties,
    right_properties: EquivalenceProperties,
    left_columns_len: usize,
    schema: SchemaRef,
) -> EquivalenceProperties {
    let mut new_properties = EquivalenceProperties::new(schema);
    new_properties.extend(left_properties.classes().to_vec());
    let new_right_properties = right_properties
        .classes()
        .iter()
        .map(|prop| {
            let new_head =
                Column::new(prop.head().name(), left_columns_len + prop.head().index());
            let new_others = prop
                .others()
                .iter()
                .map(|col| Column::new(col.name(), left_columns_len + col.index()))
                .collect::<Vec<_>>();
            EquivalentClass::new(new_head, new_others)
        })
        .collect::<Vec<_>>();
    new_properties.extend(new_right_properties);
    new_properties
}

impl Display for JoinSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinSide::Left => write!(f, "left"),
            JoinSide::Right => write!(f, "right"),
        }
    }
}

/// Used in ColumnIndex to distinguish which side the index is for
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinSide {
    /// Left side of the join
    Left,
    /// Right side of the join
    Right,
}

impl JoinSide {
    /// Inverse the join side
    pub fn negate(&self) -> Self {
        match self {
            JoinSide::Left => JoinSide::Right,
            JoinSide::Right => JoinSide::Left,
        }
    }
}

/// Information about the index and placement (left or right) of the columns
#[derive(Debug, Clone)]
pub struct ColumnIndex {
    /// Index of the column
    pub index: usize,
    /// Whether the column is at the left or right side
    pub side: JoinSide,
}

/// Filter applied before join output
#[derive(Debug, Clone)]
pub struct JoinFilter {
    /// Filter expression
    expression: Arc<dyn PhysicalExpr>,
    /// Column indices required to construct intermediate batch for filtering
    column_indices: Vec<ColumnIndex>,
    /// Physical schema of intermediate batch
    schema: Schema,
}

impl JoinFilter {
    /// Creates new JoinFilter
    pub fn new(
        expression: Arc<dyn PhysicalExpr>,
        column_indices: Vec<ColumnIndex>,
        schema: Schema,
    ) -> JoinFilter {
        JoinFilter {
            expression,
            column_indices,
            schema,
        }
    }

    /// Helper for building ColumnIndex vector from left and right indices
    pub fn build_column_indices(
        left_indices: Vec<usize>,
        right_indices: Vec<usize>,
    ) -> Vec<ColumnIndex> {
        left_indices
            .into_iter()
            .map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Left,
            })
            .chain(right_indices.into_iter().map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Right,
            }))
            .collect()
    }

    /// Filter expression
    pub fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expression
    }

    /// Column indices for intermediate batch creation
    pub fn column_indices(&self) -> &[ColumnIndex] {
        &self.column_indices
    }

    /// Intermediate batch schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Returns the output field given the input field. Outer joins may
/// insert nulls even if the input was not null
///
fn output_join_field(old_field: &Field, join_type: &JoinType, is_left: bool) -> Field {
    let force_nullable = match join_type {
        JoinType::Inner => false,
        JoinType::Left => !is_left, // right input is padded with nulls
        JoinType::Right => is_left, // left input is padded with nulls
        JoinType::Full => true,     // both inputs can be padded with nulls
        JoinType::LeftSemi => false, // doesn't introduce nulls
        JoinType::RightSemi => false, // doesn't introduce nulls
        JoinType::LeftAnti => false, // doesn't introduce nulls (or can it??)
        JoinType::RightAnti => false, // doesn't introduce nulls (or can it??)
    };

    if force_nullable {
        old_field.clone().with_nullable(true)
    } else {
        old_field.clone()
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: &JoinType,
) -> (Schema, Vec<ColumnIndex>) {
    let (fields, column_indices): (SchemaBuilder, Vec<ColumnIndex>) = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let left_fields = left
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, true))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Left,
                        },
                    )
                });
            let right_fields = right
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, false))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Right,
                        },
                    )
                });

            // left then right
            left_fields.chain(right_fields).unzip()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => left
            .fields()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Left,
                    },
                )
            })
            .unzip(),
        JoinType::RightSemi | JoinType::RightAnti => right
            .fields()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Right,
                    },
                )
            })
            .unzip(),
    };

    (fields.finish(), column_indices)
}

/// A [`OnceAsync`] can be used to run an async closure once, with subsequent calls
/// to [`OnceAsync::once`] returning a [`OnceFut`] to the same asynchronous computation
///
/// This is useful for joins where the results of one child are buffered in memory
/// and shared across potentially multiple output partitions
pub(crate) struct OnceAsync<T> {
    fut: Mutex<Option<OnceFut<T>>>,
}

impl<T> Default for OnceAsync<T> {
    fn default() -> Self {
        Self {
            fut: Mutex::new(None),
        }
    }
}

impl<T> std::fmt::Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnceAsync")
    }
}

impl<T: 'static> OnceAsync<T> {
    /// If this is the first call to this function on this object, will invoke
    /// `f` to obtain a future and return a [`OnceFut`] referring to this
    ///
    /// If this is not the first call, will return a [`OnceFut`] referring
    /// to the same future as was returned by the first call
    pub(crate) fn once<F, Fut>(&self, f: F) -> OnceFut<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fut
            .lock()
            .get_or_insert_with(|| OnceFut::new(f()))
            .clone()
    }
}

/// The shared future type used internally within [`OnceAsync`]
type OnceFutPending<T> = Shared<BoxFuture<'static, SharedResult<Arc<T>>>>;

/// A [`OnceFut`] represents a shared asynchronous computation, that will be evaluated
/// once for all [`Clone`]'s, with [`OnceFut::get`] providing a non-consuming interface
/// to drive the underlying [`Future`] to completion
pub(crate) struct OnceFut<T> {
    state: OnceFutState<T>,
}

impl<T> Clone for OnceFut<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// A shared state between statistic aggregators for a join
/// operation.
#[derive(Clone, Debug, Default)]
struct PartialJoinStatistics {
    pub num_rows: usize,
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Estimate the statistics for the given join's output.
pub(crate) fn estimate_join_statistics(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: &JoinType,
) -> Statistics {
    let left_stats = left.statistics();
    let right_stats = right.statistics();

    let join_stats = estimate_join_cardinality(join_type, left_stats, right_stats, &on);
    let (num_rows, column_statistics) = match join_stats {
        Some(stats) => (Some(stats.num_rows), Some(stats.column_statistics)),
        None => (None, None),
    };
    Statistics {
        num_rows,
        total_byte_size: None,
        column_statistics,
        is_exact: false,
    }
}

// Estimate the cardinality for the given join with input statistics.
fn estimate_join_cardinality(
    join_type: &JoinType,
    left_stats: Statistics,
    right_stats: Statistics,
    on: &JoinOn,
) -> Option<PartialJoinStatistics> {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let left_num_rows = left_stats.num_rows?;
            let right_num_rows = right_stats.num_rows?;

            // Take the left_col_stats and right_col_stats using the index
            // obtained from index() method of the each element of 'on'.
            let all_left_col_stats = left_stats.column_statistics?;
            let all_right_col_stats = right_stats.column_statistics?;
            let (left_col_stats, right_col_stats) = on
                .iter()
                .map(|(left, right)| {
                    (
                        all_left_col_stats[left.index()].clone(),
                        all_right_col_stats[right.index()].clone(),
                    )
                })
                .unzip::<_, _, Vec<_>, Vec<_>>();

            let ij_cardinality = estimate_inner_join_cardinality(
                left_num_rows,
                right_num_rows,
                left_col_stats,
                right_col_stats,
                left_stats.is_exact && right_stats.is_exact,
            )?;

            // The cardinality for inner join can also be used to estimate
            // the cardinality of left/right/full outer joins as long as it
            // it is greater than the minimum cardinality constraints of these
            // joins (so that we don't underestimate the cardinality).
            let cardinality = match join_type {
                JoinType::Inner => ij_cardinality,
                JoinType::Left => max(ij_cardinality, left_num_rows),
                JoinType::Right => max(ij_cardinality, right_num_rows),
                JoinType::Full => {
                    max(ij_cardinality, left_num_rows)
                        + max(ij_cardinality, right_num_rows)
                        - ij_cardinality
                }
                _ => unreachable!(),
            };

            Some(PartialJoinStatistics {
                num_rows: cardinality,
                // We don't do anything specific here, just combine the existing
                // statistics which might yield subpar results (although it is
                // true, esp regarding min/max). For a better estimation, we need
                // filter selectivity analysis first.
                column_statistics: all_left_col_stats
                    .into_iter()
                    .chain(all_right_col_stats.into_iter())
                    .collect(),
            })
        }

        JoinType::LeftSemi
        | JoinType::RightSemi
        | JoinType::LeftAnti
        | JoinType::RightAnti => None,
    }
}

/// Estimate the inner join cardinality by using the basic building blocks of
/// column-level statistics and the total row count. This is a very naive and
/// a very conservative implementation that can quickly give up if there is not
/// enough input statistics.
fn estimate_inner_join_cardinality(
    left_num_rows: usize,
    right_num_rows: usize,
    left_col_stats: Vec<ColumnStatistics>,
    right_col_stats: Vec<ColumnStatistics>,
    is_exact: bool,
) -> Option<usize> {
    // The algorithm here is partly based on the non-histogram selectivity estimation
    // from Spark's Catalyst optimizer.

    let mut join_selectivity = None;
    for (left_stat, right_stat) in left_col_stats.iter().zip(right_col_stats.iter()) {
        if (left_stat.min_value.clone()? > right_stat.max_value.clone()?)
            || (left_stat.max_value.clone()? < right_stat.min_value.clone()?)
        {
            // If there is no overlap in any of the join columns, that means the join
            // itself is disjoint and the cardinality is 0. Though we can only assume
            // this when the statistics are exact (since it is a very strong assumption).
            return if is_exact { Some(0) } else { None };
        }

        let left_max_distinct = max_distinct_count(left_num_rows, left_stat.clone());
        let right_max_distinct = max_distinct_count(right_num_rows, right_stat.clone());
        let max_distinct = max(left_max_distinct, right_max_distinct);
        if max_distinct > join_selectivity {
            // Seems like there are a few implementations of this algorithm that implement
            // exponential decay for the selectivity (like Hive's Optiq Optimizer). Needs
            // further exploration.
            join_selectivity = max_distinct;
        }
    }

    // With the assumption that the smaller input's domain is generally represented in the bigger
    // input's domain, we can estimate the inner join's cardinality by taking the cartesian product
    // of the two inputs and normalizing it by the selectivity factor.
    match join_selectivity {
        Some(selectivity) if selectivity > 0 => {
            Some((left_num_rows * right_num_rows) / selectivity)
        }
        // Since we don't have any information about the selectivity (which is derived
        // from the number of distinct rows information) we can give up here for now.
        // And let other passes handle this (otherwise we would need to produce an
        // overestimation using just the cartesian product).
        _ => None,
    }
}

/// Estimate the number of maximum distinct values that can be present in the
/// given column from its statistics.
///
/// If distinct_count is available, uses it directly. If the column numeric, and
/// has min/max values, then they might be used as a fallback option. Otherwise,
/// returns None.
fn max_distinct_count(num_rows: usize, stats: ColumnStatistics) -> Option<usize> {
    match (stats.distinct_count, stats.max_value, stats.min_value) {
        (Some(_), _, _) => stats.distinct_count,
        (_, Some(max), Some(min)) => {
            // Note that float support is intentionally omitted here, since the computation
            // of a range between two float values is not trivial and the result would be
            // highly inaccurate.
            let numeric_range = get_int_range(min, max)?;

            // The number can never be greater than the number of rows we have (minus
            // the nulls, since they don't count as distinct values).
            let ceiling = num_rows - stats.null_count.unwrap_or(0);
            Some(numeric_range.min(ceiling))
        }
        _ => None,
    }
}

/// Return the numeric range between the given min and max values.
fn get_int_range(min: ScalarValue, max: ScalarValue) -> Option<usize> {
    let delta = &max.sub(&min).ok()?;
    match delta {
        ScalarValue::Int8(Some(delta)) if *delta >= 0 => Some(*delta as usize),
        ScalarValue::Int16(Some(delta)) if *delta >= 0 => Some(*delta as usize),
        ScalarValue::Int32(Some(delta)) if *delta >= 0 => Some(*delta as usize),
        ScalarValue::Int64(Some(delta)) if *delta >= 0 => Some(*delta as usize),
        ScalarValue::UInt8(Some(delta)) => Some(*delta as usize),
        ScalarValue::UInt16(Some(delta)) => Some(*delta as usize),
        ScalarValue::UInt32(Some(delta)) => Some(*delta as usize),
        ScalarValue::UInt64(Some(delta)) => Some(*delta as usize),
        _ => None,
    }
    // The delta (directly) is not the real range, since it does not include the
    // first term.
    // E.g. (min=2, max=4) -> (4 - 2) -> 2, but the actual result should be 3 (1, 2, 3).
    .map(|open_ended_range| open_ended_range + 1)
}

enum OnceFutState<T> {
    Pending(OnceFutPending<T>),
    Ready(SharedResult<Arc<T>>),
}

impl<T> Clone for OnceFutState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending(p) => Self::Pending(p.clone()),
            Self::Ready(r) => Self::Ready(r.clone()),
        }
    }
}

impl<T: 'static> OnceFut<T> {
    /// Create a new [`OnceFut`] from a [`Future`]
    pub(crate) fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        Self {
            state: OnceFutState::Pending(
                fut.map(|res| res.map(Arc::new).map_err(Arc::new))
                    .boxed()
                    .shared(),
            ),
        }
    }

    /// Get the result of the computation if it is ready, without consuming it
    pub(crate) fn get(&mut self, cx: &mut Context<'_>) -> Poll<Result<&T>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        // Cannot use loop as this would trip up the borrow checker
        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.as_ref()
                    .map(|r| r.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e.clone()))),
            ),
        }
    }
}

/// Some type `join_type` of join need to maintain the matched indices bit map for the left side, and
/// use the bit map to generate the part of result of the join.
///
/// For example of the `Left` join, in each iteration of right side, can get the matched result, but need
/// to maintain the matched indices bit map to get the unmatched row for the left side.
pub(crate) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftSemi | JoinType::Full
    )
}

/// In the end of join execution, need to use bit map of the matched
/// indices to generate the final left and right indices.
///
/// For example:
///
/// 1. left_bit_map: `[true, false, true, true, false]`
/// 2. join_type: `Left`
///
/// The result is: `([1,4], [null, null])`
pub(crate) fn get_final_indices_from_bit_map(
    left_bit_map: &BooleanBufferBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let left_size = left_bit_map.len();
    let left_indices = if join_type == JoinType::LeftSemi {
        (0..left_size)
            .filter_map(|idx| (left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    } else {
        // just for `Left`, `LeftAnti` and `Full` join
        // `LeftAnti`, `Left` and `Full` will produce the unmatched left row finally
        (0..left_size)
            .filter_map(|idx| (!left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    };
    // right_indices
    // all the element in the right side is None
    let mut builder = UInt32Builder::with_capacity(left_indices.len());
    builder.append_nulls(left_indices.len());
    let right_indices = builder.finish();
    (left_indices, right_indices)
}

pub(crate) fn apply_join_filter_to_indices(
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
    filter: &JoinFilter,
    build_side: JoinSide,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_indices.is_empty() && probe_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    };

    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        filter.column_indices(),
        build_side,
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows());
    let mask = as_boolean_array(&filter_result)?;

    let left_filtered = compute::filter(&build_indices, mask)?;
    let right_filtered = compute::filter(&probe_indices, mask)?;
    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
pub(crate) fn build_batch_from_indices(
    schema: &Schema,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: &UInt64Array,
    probe_indices: &UInt32Array,
    column_indices: &[ColumnIndex],
    build_side: JoinSide,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(build_indices.len()));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == build_side {
            let array = build_input_buffer.column(column_index.index);
            if array.is_empty() || build_indices.null_count() == build_indices.len() {
                // Outer join would generate a null index when finding no match at our side.
                // Therefore, it's possible we are empty but need to populate an n-length null array,
                // where n is the length of the index array.
                assert_eq!(build_indices.null_count(), build_indices.len());
                new_null_array(array.data_type(), build_indices.len())
            } else {
                compute::take(array.as_ref(), build_indices, None)?
            }
        } else {
            let array = probe_batch.column(column_index.index);
            if array.is_empty() || probe_indices.null_count() == probe_indices.len() {
                assert_eq!(probe_indices.null_count(), probe_indices.len());
                new_null_array(array.data_type(), probe_indices.len())
            } else {
                compute::take(array.as_ref(), probe_indices, None)?
            }
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub(crate) fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    count_right_batch: usize,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    match join_type {
        JoinType::Inner => {
            // matched
            (left_indices, right_indices)
        }
        JoinType::Left => {
            // matched
            (left_indices, right_indices)
            // unmatched left row will be produced in the end of loop, and it has been set in the left visited bitmap
        }
        JoinType::Right | JoinType::Full => {
            // matched
            // unmatched right row will be produced in this batch
            let right_unmatched_indices =
                get_anti_indices(count_right_batch, &right_indices);
            // combine the matched and unmatched right result together
            append_right_indices(left_indices, right_indices, right_unmatched_indices)
        }
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            (left_indices, right_indices)
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            (left_indices, right_indices)
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            (
                UInt64Array::from_iter_values(vec![]),
                UInt32Array::from_iter_values(vec![]),
            )
        }
    }
}

/// Appends the `right_unmatched_indices` to the `right_indices`,
/// and fills Null to tail of `left_indices` to
/// keep the length of `right_indices` and `left_indices` consistent.
pub(crate) fn append_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    right_unmatched_indices: UInt32Array,
) -> (UInt64Array, UInt32Array) {
    // left_indices, right_indices and right_unmatched_indices must not contain the null value
    if right_unmatched_indices.is_empty() {
        (left_indices, right_indices)
    } else {
        let unmatched_size = right_unmatched_indices.len();
        // the new left indices: left_indices + null array
        // the new right indices: right_indices + right_unmatched_indices
        let new_left_indices = left_indices
            .iter()
            .chain(std::iter::repeat(None).take(unmatched_size))
            .collect::<UInt64Array>();
        let new_right_indices = right_indices
            .iter()
            .chain(right_unmatched_indices.iter())
            .collect::<UInt32Array>();
        (new_left_indices, new_right_indices)
    }
}

/// Get unmatched and deduplicated indices
pub(crate) fn get_anti_indices(
    row_count: usize,
    input_indices: &UInt32Array,
) -> UInt32Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the anti index
    (0..row_count)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(idx as u32))
        .collect::<UInt32Array>()
}

/// Get unmatched and deduplicated indices
pub(crate) fn get_anti_u64_indices(
    row_count: usize,
    input_indices: &UInt64Array,
) -> UInt64Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the anti index
    (0..row_count)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(idx as u64))
        .collect::<UInt64Array>()
}

/// Get matched and deduplicated indices
pub(crate) fn get_semi_indices(
    row_count: usize,
    input_indices: &UInt32Array,
) -> UInt32Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the semi index
    (0..row_count)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(idx as u32))
        .collect::<UInt32Array>()
}

/// Get matched and deduplicated indices
pub(crate) fn get_semi_u64_indices(
    row_count: usize,
    input_indices: &UInt64Array,
) -> UInt64Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the semi index
    (0..row_count)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(idx as u64))
        .collect::<UInt64Array>()
}

/// Metrics for build & probe joins
#[derive(Clone, Debug)]
pub(crate) struct BuildProbeJoinMetrics {
    /// Total time for collecting build-side of join
    pub(crate) build_time: metrics::Time,
    /// Number of batches consumed by build-side
    pub(crate) build_input_batches: metrics::Count,
    /// Number of rows consumed by build-side
    pub(crate) build_input_rows: metrics::Count,
    /// Memory used by build-side in bytes
    pub(crate) build_mem_used: metrics::Gauge,
    /// Total time for joining probe-side batches to the build-side batches
    pub(crate) join_time: metrics::Time,
    /// Number of batches consumed by probe-side of this operator
    pub(crate) input_batches: metrics::Count,
    /// Number of rows consumed by probe-side this operator
    pub(crate) input_rows: metrics::Count,
    /// Number of batches produced by this operator
    pub(crate) output_batches: metrics::Count,
    /// Number of rows produced by this operator
    pub(crate) output_rows: metrics::Count,
}

impl BuildProbeJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);

        let build_time = MetricBuilder::new(metrics).subset_time("build_time", partition);

        let build_input_batches =
            MetricBuilder::new(metrics).counter("build_input_batches", partition);

        let build_input_rows =
            MetricBuilder::new(metrics).counter("build_input_rows", partition);

        let build_mem_used =
            MetricBuilder::new(metrics).gauge("build_mem_used", partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            build_time,
            build_input_batches,
            build_input_rows,
            build_mem_used,
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Fields;
    use arrow::error::Result as ArrowResult;
    use arrow::{datatypes::DataType, error::ArrowError};
    use datafusion_common::ScalarValue;
    use std::pin::Pin;

    fn check(left: &[Column], right: &[Column], on: &[(Column, Column)]) -> Result<()> {
        let left = left
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        let right = right
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        check_join_set_is_valid(&left, &right, on)
    }

    #[test]
    fn check_valid() -> Result<()> {
        let left = vec![Column::new("a", 0), Column::new("b1", 1)];
        let right = vec![Column::new("a", 0), Column::new("b2", 1)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        check(&left, &right, on)?;
        Ok(())
    }

    #[test]
    fn check_not_in_right() {
        let left = vec![Column::new("a", 0), Column::new("b", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        assert!(check(&left, &right, on).is_err());
    }

    #[tokio::test]
    async fn check_error_nesting() {
        let once_fut = OnceFut::<()>::new(async {
            Err(DataFusionError::ArrowError(ArrowError::CsvError(
                "some error".to_string(),
            )))
        });

        struct TestFut(OnceFut<()>);
        impl Future for TestFut {
            type Output = ArrowResult<()>;

            fn poll(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                match ready!(self.0.get(cx)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(e.into())),
                }
            }
        }

        let res = TestFut(once_fut).await;
        let arrow_err_from_fut = res.expect_err("once_fut always return error");

        let wrapped_err = DataFusionError::from(arrow_err_from_fut);
        let root_err = wrapped_err.find_root();

        assert!(matches!(
            root_err,
            DataFusionError::ArrowError(ArrowError::CsvError(_))
        ))
    }

    #[test]
    fn check_not_in_left() {
        let left = vec![Column::new("b", 0)];
        let right = vec![Column::new("a", 0)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_collision() {
        // column "a" would appear both in left and right
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("a", 0), Column::new("b", 1)];
        let on = &[(Column::new("a", 0), Column::new("b", 1))];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn check_in_right() {
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(Column::new("a", 0), Column::new("b", 0))];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn test_join_schema() -> Result<()> {
        let a = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a_nulls = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let b = Schema::new(vec![Field::new("b", DataType::Int32, false)]);
        let b_nulls = Schema::new(vec![Field::new("b", DataType::Int32, true)]);

        let cases = vec![
            (&a, &b, JoinType::Inner, &a, &b),
            (&a, &b_nulls, JoinType::Inner, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Inner, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Inner, &a_nulls, &b_nulls),
            // right input of a `LEFT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Left, &a, &b_nulls),
            (&a, &b_nulls, JoinType::Left, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Left, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Left, &a_nulls, &b_nulls),
            // left input of a `RIGHT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Right, &a_nulls, &b),
            (&a, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Right, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            // Either input of a `FULL` join can be null
            (&a, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
        ];

        for (left_in, right_in, join_type, left_out, right_out) in cases {
            let (schema, _) = build_join_schema(left_in, right_in, &join_type);

            let expected_fields = left_out
                .fields()
                .iter()
                .cloned()
                .chain(right_out.fields().iter().cloned())
                .collect::<Fields>();

            let expected_schema = Schema::new(expected_fields);
            assert_eq!(
                schema,
                expected_schema,
                "Mismatch with left_in={}:{}, right_in={}:{}, join_type={:?}",
                left_in.fields()[0].name(),
                left_in.fields()[0].is_nullable(),
                right_in.fields()[0].name(),
                right_in.fields()[0].is_nullable(),
                join_type
            );
        }

        Ok(())
    }

    fn create_stats(
        num_rows: Option<usize>,
        column_stats: Option<Vec<ColumnStatistics>>,
        is_exact: bool,
    ) -> Statistics {
        Statistics {
            num_rows,
            column_statistics: column_stats,
            is_exact,
            ..Default::default()
        }
    }

    fn create_column_stats(
        min: Option<i64>,
        max: Option<i64>,
        distinct_count: Option<usize>,
    ) -> ColumnStatistics {
        ColumnStatistics {
            distinct_count,
            min_value: min.map(|size| ScalarValue::Int64(Some(size))),
            max_value: max.map(|size| ScalarValue::Int64(Some(size))),
            ..Default::default()
        }
    }

    type PartialStats = (usize, Option<i64>, Option<i64>, Option<usize>);

    // This is mainly for validating the all edge cases of the estimation, but
    // more advanced (and real world test cases) are below where we need some control
    // over the expected output (since it depends on join type to join type).
    #[test]
    fn test_inner_join_cardinality_single_column() -> Result<()> {
        let cases: Vec<(PartialStats, PartialStats, Option<usize>)> = vec![
            // -----------------------------------------------------------------------------
            // | left(rows, min, max, distinct), right(rows, min, max, distinct), expected |
            // -----------------------------------------------------------------------------

            // Cardinality computation
            // =======================
            //
            // distinct(left) == NaN, distinct(right) == NaN
            (
                (10, Some(1), Some(10), None),
                (10, Some(1), Some(10), None),
                Some(10),
            ),
            // range(left) > range(right)
            (
                (10, Some(6), Some(10), None),
                (10, Some(8), Some(10), None),
                Some(20),
            ),
            // range(right) > range(left)
            (
                (10, Some(8), Some(10), None),
                (10, Some(6), Some(10), None),
                Some(20),
            ),
            // range(left) > len(left), range(right) > len(right)
            (
                (10, Some(1), Some(15), None),
                (20, Some(1), Some(40), None),
                Some(10),
            ),
            // When we have distinct count.
            (
                (10, Some(1), Some(10), Some(10)),
                (10, Some(1), Some(10), Some(10)),
                Some(10),
            ),
            // distinct(left) > distinct(right)
            (
                (10, Some(1), Some(10), Some(5)),
                (10, Some(1), Some(10), Some(2)),
                Some(20),
            ),
            // distinct(right) > distinct(left)
            (
                (10, Some(1), Some(10), Some(2)),
                (10, Some(1), Some(10), Some(5)),
                Some(20),
            ),
            // min(left) < 0 (range(left) > range(right))
            (
                (10, Some(-5), Some(5), None),
                (10, Some(1), Some(5), None),
                Some(10),
            ),
            // min(right) < 0, max(right) < 0 (range(right) > range(left))
            (
                (10, Some(-25), Some(-20), None),
                (10, Some(-25), Some(-15), None),
                Some(10),
            ),
            // range(left) < 0, range(right) >= 0
            // (there isn't a case where both left and right ranges are negative
            //  so one of them is always going to work, this just proves negative
            //  ranges with bigger absolute values are not are not accidentally used).
            (
                (10, Some(10), Some(0), None),
                (10, Some(0), Some(10), Some(5)),
                Some(20), // It would have been ten if we have used abs(range(left))
            ),
            // range(left) = 1, range(right) = 1
            (
                (10, Some(1), Some(1), None),
                (10, Some(1), Some(1), None),
                Some(100),
            ),
            //
            // Edge cases
            // ==========
            //
            // No column level stats.
            ((10, None, None, None), (10, None, None, None), None),
            // No min or max (or both).
            ((10, None, None, Some(3)), (10, None, None, Some(3)), None),
            (
                (10, Some(2), None, Some(3)),
                (10, None, Some(5), Some(3)),
                None,
            ),
            (
                (10, None, Some(3), Some(3)),
                (10, Some(1), None, Some(3)),
                None,
            ),
            ((10, None, Some(3), None), (10, Some(1), None, None), None),
            // Non overlapping min/max (when exact=False).
            (
                (10, Some(0), Some(10), None),
                (10, Some(11), Some(20), None),
                None,
            ),
            (
                (10, Some(11), Some(20), None),
                (10, Some(0), Some(10), None),
                None,
            ),
            (
                (10, Some(5), Some(10), Some(10)),
                (10, Some(11), Some(3), Some(10)),
                None,
            ),
            (
                (10, Some(10), Some(5), Some(10)),
                (10, Some(3), Some(7), Some(10)),
                None,
            ),
            // distinct(left) = 0, distinct(right) = 0
            (
                (10, Some(1), Some(10), Some(0)),
                (10, Some(1), Some(10), Some(0)),
                None,
            ),
        ];

        for (left_info, right_info, expected_cardinality) in cases {
            let left_num_rows = left_info.0;
            let left_col_stats =
                vec![create_column_stats(left_info.1, left_info.2, left_info.3)];

            let right_num_rows = right_info.0;
            let right_col_stats = vec![create_column_stats(
                right_info.1,
                right_info.2,
                right_info.3,
            )];

            assert_eq!(
                estimate_inner_join_cardinality(
                    left_num_rows,
                    right_num_rows,
                    left_col_stats.clone(),
                    right_col_stats.clone(),
                    false,
                ),
                expected_cardinality
            );

            // We should also be able to use join_cardinality to get the same results
            let join_type = JoinType::Inner;
            let join_on = vec![(Column::new("a", 0), Column::new("b", 0))];
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(left_num_rows), Some(left_col_stats.clone()), false),
                create_stats(Some(right_num_rows), Some(right_col_stats.clone()), false),
                &join_on,
            );

            assert_eq!(
                partial_join_stats.clone().map(|s| s.num_rows),
                expected_cardinality
            );
            assert_eq!(
                partial_join_stats.map(|s| s.column_statistics),
                expected_cardinality.map(|_| [left_col_stats, right_col_stats].concat())
            );
        }
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_multiple_column() -> Result<()> {
        let left_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(100)),
            create_column_stats(Some(100), Some(500), Some(150)),
        ];

        let right_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(50)),
            create_column_stats(Some(100), Some(500), Some(200)),
        ];

        // We have statistics about 4 columns, where the highest distinct
        // count is 200, so we are going to pick it.
        assert_eq!(
            estimate_inner_join_cardinality(
                400,
                400,
                left_col_stats,
                right_col_stats,
                false
            ),
            Some((400 * 400) / 200)
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_decimal_range() -> Result<()> {
        let left_col_stats = vec![ColumnStatistics {
            distinct_count: None,
            min_value: Some(ScalarValue::Decimal128(Some(32500), 14, 4)),
            max_value: Some(ScalarValue::Decimal128(Some(35000), 14, 4)),
            ..Default::default()
        }];

        let right_col_stats = vec![ColumnStatistics {
            distinct_count: None,
            min_value: Some(ScalarValue::Decimal128(Some(33500), 14, 4)),
            max_value: Some(ScalarValue::Decimal128(Some(34000), 14, 4)),
            ..Default::default()
        }];

        assert_eq!(
            estimate_inner_join_cardinality(
                100,
                100,
                left_col_stats,
                right_col_stats,
                false
            ),
            None
        );
        Ok(())
    }

    #[test]
    fn test_join_cardinality() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, b=d (ignore x/y)
        let cases = vec![
            (JoinType::Inner, 800),
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            (JoinType::Full, 2200),
        ];

        let left_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(100)),
            create_column_stats(Some(0), Some(500), Some(500)),
            create_column_stats(Some(1000), Some(10000), None),
        ];

        let right_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(50)),
            create_column_stats(Some(0), Some(2000), Some(2500)),
            create_column_stats(Some(0), Some(100), None),
        ];

        for (join_type, expected_num_rows) in cases {
            let join_on = vec![
                (Column::new("a", 0), Column::new("c", 0)),
                (Column::new("b", 1), Column::new("d", 1)),
            ];

            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), Some(left_col_stats.clone()), false),
                create_stats(Some(2000), Some(right_col_stats.clone()), false),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }

    #[test]
    fn test_join_cardinality_when_one_column_is_disjoint() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, x=y (ignores b/d) where x and y does not intersect

        let left_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(100)),
            create_column_stats(Some(0), Some(500), Some(500)),
            create_column_stats(Some(1000), Some(10000), None),
        ];

        let right_col_stats = vec![
            create_column_stats(Some(0), Some(100), Some(50)),
            create_column_stats(Some(0), Some(2000), Some(2500)),
            create_column_stats(Some(0), Some(100), None),
        ];

        let join_on = vec![
            (Column::new("a", 0), Column::new("c", 0)),
            (Column::new("x", 2), Column::new("y", 2)),
        ];

        let cases = vec![
            // Join type, expected cardinality
            //
            // When an inner join is disjoint, that means it won't
            // produce any rows.
            (JoinType::Inner, 0),
            // But left/right outer joins will produce at least
            // the amount of rows from the left/right side.
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            // And a full outer join will produce at least the combination
            // of the rows above (minus the cardinality of the inner join, which
            // is 0).
            (JoinType::Full, 3000),
        ];

        for (join_type, expected_num_rows) in cases {
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), Some(left_col_stats.clone()), true),
                create_stats(Some(2000), Some(right_col_stats.clone()), true),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }
}
