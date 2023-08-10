// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation copyrighted code.

use crate::physical_plan::joins::hash_join_utils::{
    get_pruning_anti_indices, get_pruning_semi_indices, SortedFilterExpr,
};
use crate::physical_plan::joins::utils::{
    append_right_indices, get_anti_indices, get_semi_indices,
};

use arrow_array::builder::{PrimitiveBuilder, UInt32Builder, UInt64Builder};
use arrow_array::types::{UInt32Type, UInt64Type};
use arrow_array::{
    ArrowPrimitiveType, NativeAdapter, PrimitiveArray, RecordBatch, UInt32Array,
    UInt64Array,
};
use arrow_schema::SortOptions;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, JoinType, ScalarValue};
use datafusion_physical_expr::intervals::{ExprIntervalGraph, Interval, IntervalBound};

use hashbrown::HashSet;

/// This function checks if the batch offers a reference value that enables us
/// to tell whether is falls in the viable sliding window via interval analysis.
pub fn is_batch_suitable_interval_calculation(
    probe_sorted_filter_expr: &SortedFilterExpr,
    batch: &RecordBatch,
) -> Result<bool> {
    // Return false if the batch is empty:
    if batch.num_rows() == 0 {
        return Ok(false);
    }

    // Calculate the latest value of the sorted filter expression:
    let probe_order = probe_sorted_filter_expr.origin_sorted_expr();
    let array_ref = probe_order
        .expr
        .evaluate(batch)?
        .into_array(batch.num_rows());
    let latest_value = ScalarValue::try_from_array(&array_ref, batch.num_rows() - 1)?;

    // Return true if the latest value is not null:
    Ok(!latest_value.is_null())
}

/// This function takes a batch of data from the probe side, calculates the
/// interval for the build side filter expression, and updates the probe
/// buffer and the stream state accordingly.
pub fn calculate_the_necessary_build_side_range(
    input_buffer: &RecordBatch,
    graph: &mut ExprIntervalGraph,
    left_sorted_filter_expr: &mut SortedFilterExpr,
    right_sorted_filter_expr: &mut SortedFilterExpr,
    probe_batch: &RecordBatch,
) -> datafusion_common::Result<Interval> {
    // Calculate the interval for the build side filter expression (if present):
    update_filter_expr_bounds(
        input_buffer,
        left_sorted_filter_expr,
        probe_batch,
        right_sorted_filter_expr,
    )?;
    let mut filter_intervals = vec![];
    for expr in [left_sorted_filter_expr, right_sorted_filter_expr] {
        filter_intervals.push((expr.node_index(), expr.interval().clone()))
    }
    // Update the physical expression graph using the join filter intervals:
    graph.update_ranges(&mut filter_intervals)?;
    Ok(filter_intervals.remove(0).1)
}

/// Checks whether the given reference value (i.e. `latest_value`) falls within
/// the viable sliding window specified by `interval` according to the sort
/// options of the join in question (i.e. `sort_options`).
pub fn check_if_sliding_window_condition_is_met(
    latest_value: &ScalarValue, // latest value pulled from the left stream
    interval: &Interval, // interval in the build side against which we are checking
    sort_options: &SortOptions, // sort options used in the join
) -> bool {
    match sort_options {
        SortOptions {
            descending: false, ..
        } =>
        // Data is sorted in ascending order, so check if latest value is greater
        // than the upper bound of the interval. If it is, we must have processed
        // all rows that are needed from the build side for this window.
        {
            latest_value > &interval.upper.value
        }

        SortOptions {
            descending: true, ..
        } =>
        // Data is sorted in descending order, so check if latest value is less
        // than the lower bound of the interval. If it is, we must have processed
        // all rows that are needed from the build side for this window.
        {
            latest_value < &interval.lower.value
        }
    }
}

/// This function combines the given batches into a probe batch.
pub fn get_probe_batch(
    mut batches: Vec<RecordBatch>,
) -> datafusion_common::Result<RecordBatch> {
    let probe_batch = if batches.len() == 1 {
        batches.remove(0)
    } else {
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches)?
    };
    Ok(probe_batch)
}

/// Appends probe indices in order by considering the given build indices.
///
/// This function constructs new build and probe indices by iterating through
/// the provided indices, and appends any missing values between previous and
/// current probe index with a corresponding null build index. It handles various
/// edge cases and returns an error if either index is `None`.
///
/// # Parameters
/// - `build_indices`: `PrimitiveArray` of `UInt64Type` containing build indices.
/// - `probe_indices`: `PrimitiveArray` of `UInt32Type` containing probe indices.
/// - `count_probe_batch`: The number of elements in the probe batch, used for
///   filling in any remaining indices.
///
/// # Returns
/// A `Result` containing a tuple of two arrays:
/// - A `PrimitiveArray` of `UInt64Type` with the newly constructed build indices.
/// - A `PrimitiveArray` of `UInt32Type` with the newly constructed probe indices.
///
/// # Errors
/// Returns an error if there is a failure in calculating probe indices.
fn append_probe_indices_in_order(
    build_indices: PrimitiveArray<UInt64Type>,
    probe_indices: PrimitiveArray<UInt32Type>,
    count_probe_batch: u32,
) -> datafusion_common::Result<(PrimitiveArray<UInt64Type>, PrimitiveArray<UInt32Type>)> {
    // Builders for new indices:
    let mut new_build_indices = UInt64Builder::new();
    let mut new_probe_indices = UInt32Builder::new();

    // Set previous index as zero for the initial loop.
    let mut prev_index = 0;

    // Zip the two iterators.
    for (maybe_build_index, maybe_probe_index) in
        build_indices.iter().zip(probe_indices.iter())
    {
        // Unwrap index options.
        let (build_index, probe_index) = match (maybe_build_index, maybe_probe_index) {
            (Some(bi), Some(pi)) => (bi, pi),
            // If either index is None, return an error.
            _ => {
                return Err(DataFusionError::Internal(
                    "Error on probe indices calculation".to_owned(),
                ))
            }
        };

        // Append values between previous and current left index with null right index.
        for val in prev_index..probe_index {
            new_probe_indices.append_value(val);
            new_build_indices.append_null();
        }

        // Append current indices.
        new_probe_indices.append_value(probe_index);
        new_build_indices.append_value(build_index);

        // Set current left index as previous for the next loop.
        prev_index = probe_index + 1;
    }

    // Append remaining left indices after the last valid left index with null right index.
    for val in prev_index..count_probe_batch {
        new_probe_indices.append_value(val);
        new_build_indices.append_null();
    }

    // Build arrays and return.
    Ok((new_build_indices.finish(), new_probe_indices.finish()))
}

/// Adjusts indices of the probe side according to the specified join type.
///
/// The main purpose of this function is to align the indices for different types
/// of joins, including `Inner`, `Left`, `Right`, `Full`, `RightSemi`, `RightAnti`,
/// `LeftAnti` and `LeftSemi`.
///
/// # Parameters
/// - `build_indices`: The `UInt64Array` containing build indices.
/// - `probe_indices`: The `UInt32Array` containing probe indices.
/// - `count_probe_batch`: The number of elements in the probe batch.
/// - `join_type`: The type of join in question.
///
/// # Returns
/// A `Result` containing a tuple of two arrays:
/// - A `UInt64Array` with the adjusted build indices.
/// - A `UInt32Array` with the adjusted probe indices.
///
/// # Errors
/// Returns an error if there is a failure in processing the indices according
/// to the given join type.
pub(crate) fn adjust_probe_side_indices_by_join_type(
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
    count_probe_batch: usize,
    join_type: JoinType,
) -> Result<(UInt64Array, UInt32Array)> {
    match join_type {
        JoinType::Inner | JoinType::Left => {
            // Unmatched rows for the left join will be produced in the pruning phase.
            Ok((build_indices, probe_indices))
        }
        JoinType::Right => {
            // We use an order preserving index calculation algorithm, since it is possible in theory.
            append_probe_indices_in_order(
                build_indices,
                probe_indices,
                count_probe_batch as u32,
            )
        }
        JoinType::Full => {
            // Unmatched probe rows will be produced in this batch. Since we do
            // not preserve the order, we do not need to iterate through the left
            // indices. This is why we split the full join.

            let right_unmatched_indices =
                get_anti_indices(count_probe_batch, &probe_indices);
            // Combine the matched and unmatched right result together:
            Ok(append_right_indices(
                build_indices,
                probe_indices,
                right_unmatched_indices,
            ))
        }
        JoinType::RightSemi => {
            // We need to remove duplicated records in the probe side:
            let probe_indices = get_semi_indices(count_probe_batch, &probe_indices);
            Ok((build_indices, probe_indices))
        }
        JoinType::RightAnti => {
            // We need to remove duplicated records in the probe side.
            // For this purpose, get anti indices for the probe side:
            let probe_indices = get_anti_indices(count_probe_batch, &probe_indices);
            Ok((build_indices, probe_indices))
        }
        JoinType::LeftAnti | JoinType::LeftSemi => {
            // Matched or unmatched build side rows will be produced in the
            // pruning phase of the build side.
            // When we visit the right batch, we can output the matched left
            // row and don't need to wait for the pruning phase.
            Ok((
                UInt64Array::from_iter_values(vec![]),
                UInt32Array::from_iter_values(vec![]),
            ))
        }
    }
}

/// Calculates the build side outer indices based on the specified join type.
///
/// This function calculates the build side outer indices for specific join types,
/// including `Left`, `LeftAnti`, `LeftSemi` and `Full`. It computes unmatched indices
/// for pruning and constructs corresponding probe indices with null values.
///
/// # Parameters
/// - `prune_length`: Length for pruning calculations.
/// - `visited_rows`: A `HashSet` containing visited row indices.
/// - `deleted_offset`: Offset for deleted indices.
/// - `join_type`: The type of join in question.
///
/// # Returns
/// A `Result` containing a tuple of two arrays:
/// - A `PrimitiveArray` of generic type `L` with build indices.
/// - A `PrimitiveArray` of generic type `R` with probe indices containing null values.
///
/// # Errors
/// No explicit error handling in the function, but it may return errors coming from
/// underlying calls. The case of other join types is not considered, and the function
/// will return an `DatafusionError::Internal` if called with such a join type.
///
/// # Type Parameters
/// - `L`: The Arrow primitive type for build indices.
/// - `R`: The Arrow primitive type for probe indices.
pub fn calculate_build_outer_indices_by_join_type<
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
>(
    prune_length: usize,
    visited_rows: &HashSet<usize>,
    deleted_offset: usize,
    join_type: JoinType,
) -> Result<(PrimitiveArray<L>, PrimitiveArray<R>)>
where
    NativeAdapter<L>: From<<L as ArrowPrimitiveType>::Native>,
{
    let result = match join_type {
        JoinType::Left | JoinType::LeftAnti | JoinType::Full => {
            // Calculate anti indices for pruning:
            let build_unmatched_indices =
                get_pruning_anti_indices(prune_length, deleted_offset, visited_rows);
            // Prepare probe indices with null values corresponding to build side
            // unmatched indices:
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        JoinType::LeftSemi => {
            // Calculate semi indices for pruning:
            let build_unmatched_indices =
                get_pruning_semi_indices(prune_length, deleted_offset, visited_rows);
            // Prepare probe indices with null values corresponding to build side
            // unmatched indices:
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        // Return an internal error if an unsupported join type is given.
        _ => {
            return Err(DataFusionError::Internal(
                "Given join type is not supported".to_owned(),
            ))
        }
    };
    Ok(result)
}

/// Represents the various states of a sliding window join stream.
///
/// This `enum` encapsulates the different states that a join stream might be
/// in throughout its execution. Depending on its current state, the join
/// operation will perform different actions such as pulling data from the build
/// side or the probe side, or performing the join itself.
pub enum JoinStreamState {
    /// The action is to pull data from the probe side (right stream).
    /// This state continues to pull data until the probe batches are suitable
    /// for interval calculations, or the probe stream is exhausted.
    PullProbe,
    /// The action is to pull data from the build side (left stream) within a
    /// given interval.
    /// This state continues to pull data until a suitable range of batches is
    /// found, or the build stream is exhausted.
    PullBuild { interval: Interval },
    /// The probe side is completely processed. In this state, the build side
    /// will be ready and its results will be processed until the build stream
    /// is also exhausted.
    ProbeExhausted,
    /// The build side is completely processed. In this state, the join operation
    /// will switch to the "Join" state to perform the final join operation.
    BuildExhausted,
    /// Both the build and probe sides have been completely processed.
    /// If `final_result` is `false`, a final result may still be produced from
    /// the build side. Otherwise, the join operation is complete.
    BothExhausted { final_result: bool },
    /// The join operation is actively processing data from both sides to produce
    /// the result. In this state, equal and anti join results are calculated and
    /// combined into a single batch, and the state is updated to `PullProbe` for
    /// the next iteration.
    Join,
}

/// Updates the filter expression bounds for both build and probe sides.
///
/// This function evaluates the build/probe-side sorted filter expressions to
/// determine feasible interval bounds. It then sets these intervals within
/// the expressions. The function sets a null interval for the build side and
/// calculates the actual interval for the probe side based on the sort options.
pub(crate) fn update_filter_expr_bounds(
    build_inner_buffer: &RecordBatch,
    build_sorted_filter_expr: &mut SortedFilterExpr,
    probe_batch: &RecordBatch,
    probe_sorted_filter_expr: &mut SortedFilterExpr,
) -> Result<()> {
    // Evaluate the build side order expression to get datatype:
    let build_order_datatype = build_sorted_filter_expr
        .origin_sorted_expr()
        .expr
        .evaluate(&build_inner_buffer.slice(0, 0))?
        .data_type();

    // Create a null scalar value with the obtained datatype:
    let null_scalar = ScalarValue::try_from(build_order_datatype)?;
    // Create a null interval using the null scalar value:
    let null_interval = Interval::new(
        IntervalBound::new(null_scalar.clone(), true),
        IntervalBound::new(null_scalar, true),
    );
    // Set the null interval for the build side filter expression:
    build_sorted_filter_expr.set_interval(null_interval);

    // Evaluate the probe side filter expression and convert the result to an array:
    let array = probe_sorted_filter_expr
        .origin_sorted_expr()
        .expr
        .evaluate(probe_batch)?
        .into_array(probe_batch.num_rows() - 1);

    // Extract the left and right values from the array:
    let left_value = ScalarValue::try_from_array(&array, 0)?;
    let right_value = ScalarValue::try_from_array(&array, probe_batch.num_rows() - 1)?;

    // Determine the interval bounds based on sort options:
    let interval = if probe_sorted_filter_expr
        .origin_sorted_expr()
        .options
        .descending
    {
        Interval::new(
            IntervalBound::new(right_value, false),
            IntervalBound::new(left_value, false),
        )
    } else {
        Interval::new(
            IntervalBound::new(left_value, false),
            IntervalBound::new(right_value, false),
        )
    };
    // Set the calculated interval for the sorted filter expression:
    probe_sorted_filter_expr.set_interval(interval);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::joins::sliding_window_join_utils::append_probe_indices_in_order;
    use arrow_array::{UInt32Array, UInt64Array};

    #[test]
    fn test_append_left_indices_in_order() {
        let left_indices = UInt32Array::from(vec![Some(1), Some(1), Some(2), Some(4)]);
        let right_indices =
            UInt64Array::from(vec![Some(10), Some(20), Some(30), Some(40)]);
        let left_len = 7;

        let (new_right_indices, new_left_indices) =
            append_probe_indices_in_order(right_indices, left_indices, left_len).unwrap();

        // Expected results
        let expected_left_indices = UInt32Array::from(vec![
            Some(0),
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ]);
        let expected_right_indices = UInt64Array::from(vec![
            None,
            Some(10),
            Some(20),
            Some(30),
            None,
            Some(40),
            None,
            None,
        ]);

        assert_eq!(new_left_indices, expected_left_indices);
        assert_eq!(new_right_indices, expected_right_indices);
    }
}
