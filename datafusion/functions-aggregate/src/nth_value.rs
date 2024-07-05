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

//! Defines NTH_VALUE aggregate expression which may specify ordering requirement
//! that can evaluated at runtime during query execution

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, AsArray, StructArray};
use arrow_schema::{DataType, Field, Fields, SortOptions};

use datafusion_common::utils::{
    array_into_list_array_nullable, compare_rows, get_row_at_idx,
};
use datafusion_common::{exec_err, internal_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, ReversedUDAF, Signature,
    Volatility,
};
use datafusion_physical_expr_common::aggregate::utils::ordering_fields;
use datafusion_physical_expr_common::sort_expr::{
    limited_convert_logical_sort_exprs_to_physical, LexOrdering, PhysicalSortExpr,
};

make_udaf_expr_and_func!(
    NthValueAgg,
    nth_value,
    "Returns the nth value in a group of values.",
    nth_value_udaf
);

/// Expression for a `NTH_VALUE(... ORDER BY ..., ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
#[derive(Debug)]
pub struct NthValueAgg {
    signature: Signature,
    /// Determines whether `N` is relative to the beginning or the end
    /// of the aggregation. When set to `true`, then `N` is from the end.
    reversed: bool,
}

impl NthValueAgg {
    /// Create a new `NthValueAgg` aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            reversed: false,
        }
    }

    pub fn with_reversed(mut self, reversed: bool) -> Self {
        self.reversed = reversed;
        self
    }
}

impl Default for NthValueAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for NthValueAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nth_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let n = match acc_args.input_exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(value))) => {
                if self.reversed {
                    Ok(-value)
                } else {
                    Ok(value)
                }
            }
            _ => not_impl_err!(
                "{} not supported for n: {}",
                self.name(),
                &acc_args.input_exprs[1]
            ),
        }?;

        let ordering_req = limited_convert_logical_sort_exprs_to_physical(
            acc_args.sort_exprs,
            acc_args.schema,
        )?;

        let ordering_dtypes = ordering_req
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        NthValueAccumulator::try_new(
            n,
            acc_args.input_type,
            &ordering_dtypes,
            ordering_req,
        )
        .map(|acc| Box::new(acc) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(self.name(), "nth_value"),
            Field::new("item", args.input_type.clone(), true),
            false,
        )];
        let orderings = args.ordering_fields.to_vec();
        if !orderings.is_empty() {
            fields.push(Field::new_list(
                format_state_name(self.name(), "nth_value_orderings"),
                Field::new("item", DataType::Struct(Fields::from(orderings)), true),
                false,
            ));
        }
        Ok(fields)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        let nth_value = AggregateUDF::from(Self::new().with_reversed(!self.reversed));

        ReversedUDAF::Reversed(Arc::from(nth_value))
    }
}

#[derive(Debug)]
pub struct NthValueAccumulator {
    /// The `N` value.
    n: i64,
    /// Stores entries in the `NTH_VALUE` result.
    values: VecDeque<ScalarValue>,
    /// Stores values of ordering requirement expressions corresponding to each
    /// entry in `values`. This information is used when merging results from
    /// different partitions. For detailed information how merging is done, see
    /// [`merge_ordered_arrays`].
    ordering_values: VecDeque<Vec<ScalarValue>>,
    /// Stores datatypes of expressions inside values and ordering requirement
    /// expressions.
    datatypes: Vec<DataType>,
    /// Stores the ordering requirement of the `Accumulator`.
    ordering_req: LexOrdering,
}

impl NthValueAccumulator {
    /// Create a new order-sensitive NTH_VALUE accumulator based on the given
    /// item data type.
    pub fn try_new(
        n: i64,
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> datafusion_common::Result<Self> {
        if n == 0 {
            // n cannot be 0
            return internal_err!("Nth value indices are 1 based. 0 is invalid index");
        }
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            n,
            values: VecDeque::new(),
            ordering_values: VecDeque::new(),
            datatypes,
            ordering_req,
        })
    }
}

impl Accumulator for NthValueAccumulator {
    /// Updates its state with the `values`. Assumes data in the `values` satisfies the required
    /// ordering for the accumulator (across consecutive batches, not just batch-wise).
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        if from_start {
            // direction is from start
            let n_remaining = n_required.saturating_sub(self.values.len());
            self.append_new_data(values, Some(n_remaining))?;
        } else {
            // direction is from end
            self.append_new_data(values, None)?;
            let start_offset = self.values.len().saturating_sub(n_required);
            if start_offset > 0 {
                self.values.drain(0..start_offset);
                self.ordering_values.drain(0..start_offset);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is the aggregation result.
        let array_agg_values = &states[0];
        let n_required = self.n.unsigned_abs() as usize;
        if self.ordering_req.is_empty() {
            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;
            for v in array_agg_res.into_iter() {
                self.values.extend(v);
                if self.values.len() > n_required {
                    // There is enough data collected can stop merging
                    break;
                }
            }
        } else if let Some(agg_orderings) = states[1].as_list_opt::<i32>() {
            // 2nd entry stores values received for ordering requirement columns, for each aggregation value inside NTH_VALUE list.
            // For each `StructArray` inside NTH_VALUE list, we will receive an `Array` that stores
            // values received from its ordering requirement expression. (This information is necessary for during merging).

            // Stores NTH_VALUE results coming from each partition
            let mut partition_values: Vec<VecDeque<ScalarValue>> = vec![];
            // Stores ordering requirement expression results coming from each partition
            let mut partition_ordering_values: Vec<VecDeque<Vec<ScalarValue>>> = vec![];

            // Existing values should be merged also.
            partition_values.push(self.values.clone());

            partition_ordering_values.push(self.ordering_values.clone());

            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;

            for v in array_agg_res.into_iter() {
                partition_values.push(v.into());
            }

            let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;

            let ordering_values = orderings.into_iter().map(|partition_ordering_rows| {
                // Extract value from struct to ordering_rows for each group/partition
                partition_ordering_rows.into_iter().map(|ordering_row| {
                    if let ScalarValue::Struct(s) = ordering_row {
                        let mut ordering_columns_per_row = vec![];

                        for column in s.columns() {
                            let sv = ScalarValue::try_from_array(column, 0)?;
                            ordering_columns_per_row.push(sv);
                        }

                        Ok(ordering_columns_per_row)
                    } else {
                        exec_err!(
                            "Expects to receive ScalarValue::Struct(Some(..), _) but got: {:?}",
                            ordering_row.data_type()
                        )
                    }
                }).collect::<datafusion_common::Result<Vec<_>>>()
            }).collect::<datafusion_common::Result<Vec<_>>>()?;
            for ordering_values in ordering_values.into_iter() {
                partition_ordering_values.push(ordering_values.into());
            }

            let sort_options = self
                .ordering_req
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>();
            let (new_values, new_orderings) = merge_ordered_arrays(
                &mut partition_values,
                &mut partition_ordering_values,
                &sort_options,
            )?;
            self.values = new_values.into();
            self.ordering_values = new_orderings.into();
        } else {
            return exec_err!("Expects to receive a list array");
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate_values()];
        if !self.ordering_req.is_empty() {
            result.push(self.evaluate_orderings()?);
        }
        Ok(result)
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        let nth_value_idx = if from_start {
            // index is from start
            let forward_idx = n_required - 1;
            (forward_idx < self.values.len()).then_some(forward_idx)
        } else {
            // index is from end
            self.values.len().checked_sub(n_required)
        };
        if let Some(idx) = nth_value_idx {
            Ok(self.values[idx].clone())
        } else {
            ScalarValue::try_from(self.datatypes[0].clone())
        }
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + ScalarValue::size_of_vec_deque(&self.values)
            - std::mem::size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total +=
            std::mem::size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - std::mem::size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += std::mem::size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total += dtype.size() - std::mem::size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += std::mem::size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}

impl NthValueAccumulator {
    fn evaluate_orderings(&self) -> datafusion_common::Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());

        let mut column_wise_ordering_values = vec![];
        let num_columns = fields.len();
        for i in 0..num_columns {
            let column_values = self
                .ordering_values
                .iter()
                .map(|x| x[i].clone())
                .collect::<Vec<_>>();
            let array = if column_values.is_empty() {
                new_empty_array(fields[i].data_type())
            } else {
                ScalarValue::iter_to_array(column_values.into_iter())?
            };
            column_wise_ordering_values.push(array);
        }

        let ordering_array = StructArray::try_new(
            struct_field.clone(),
            column_wise_ordering_values,
            None,
        )?;

        Ok(ScalarValue::List(Arc::new(array_into_list_array_nullable(
            Arc::new(ordering_array),
        ))))
    }

    fn evaluate_values(&self) -> ScalarValue {
        let mut values_cloned = self.values.clone();
        let values_slice = values_cloned.make_contiguous();
        ScalarValue::List(ScalarValue::new_list_nullable(
            values_slice,
            &self.datatypes[0],
        ))
    }

    /// Updates state, with the `values`. Fetch contains missing number of entries for state to be complete
    /// None represents all of the new `values` need to be added to the state.
    fn append_new_data(
        &mut self,
        values: &[ArrayRef],
        fetch: Option<usize>,
    ) -> datafusion_common::Result<()> {
        let n_row = values[0].len();
        let n_to_add = if let Some(fetch) = fetch {
            std::cmp::min(fetch, n_row)
        } else {
            n_row
        };
        for index in 0..n_to_add {
            let row = get_row_at_idx(values, index)?;
            self.values.push_back(row[0].clone());
            // At index 1, we have n index argument.
            // Ordering values cover starting from 2nd index to end
            self.ordering_values.push_back(row[2..].to_vec());
        }
        Ok(())
    }
}

/// This is a wrapper struct to be able to correctly merge `ARRAY_AGG` data from
/// multiple partitions using `BinaryHeap`. When used inside `BinaryHeap`, this
/// struct returns smallest `CustomElement`, where smallest is determined by
/// `ordering` values (`Vec<ScalarValue>`) according to `sort_options`.
#[derive(Debug, PartialEq, Eq)]
struct CustomElement<'a> {
    /// Stores the partition this entry came from
    branch_idx: usize,
    /// Values to merge
    value: ScalarValue,
    // Comparison "key"
    ordering: Vec<ScalarValue>,
    /// Options defining the ordering semantics
    sort_options: &'a [SortOptions],
}

impl<'a> CustomElement<'a> {
    fn new(
        branch_idx: usize,
        value: ScalarValue,
        ordering: Vec<ScalarValue>,
        sort_options: &'a [SortOptions],
    ) -> Self {
        Self {
            branch_idx,
            value,
            ordering,
            sort_options,
        }
    }

    fn ordering(
        &self,
        current: &[ScalarValue],
        target: &[ScalarValue],
    ) -> datafusion_common::Result<Ordering> {
        // Calculate ordering according to `sort_options`
        compare_rows(current, target, self.sort_options)
    }
}

// Overwrite ordering implementation such that
// - `self.ordering` values are used for comparison,
// - When used inside `BinaryHeap` it is a min-heap.
impl<'a> Ord for CustomElement<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compares according to custom ordering
        self.ordering(&self.ordering, &other.ordering)
            // Convert max heap to min heap
            .map(|ordering| ordering.reverse())
            // This function return error, when `self.ordering` and `other.ordering`
            // have different types (such as one is `ScalarValue::Int64`, other is `ScalarValue::Float32`)
            // Here this case won't happen, because data from each partition will have same type
            .unwrap()
    }
}

impl<'a> PartialOrd for CustomElement<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// This functions merges `values` array (`&[Vec<ScalarValue>]`) into single array `Vec<ScalarValue>`
/// Merging done according to ordering values stored inside `ordering_values` (`&[Vec<Vec<ScalarValue>>]`)
/// Inner `Vec<ScalarValue>` in the `ordering_values` can be thought as ordering information for the
/// each `ScalarValue` in the `values` array.
/// Desired ordering specified by `sort_options` argument (Should have same size with inner `Vec<ScalarValue>`
/// of the `ordering_values` array).
///
/// As an example
/// values can be \[
///      \[1, 2, 3, 4, 5\],
///      \[1, 2, 3, 4\],
///      \[1, 2, 3, 4, 5, 6\],
/// \]
/// In this case we will be merging three arrays (doesn't have to be same size)
/// and produce a merged array with size 15 (sum of 5+4+6)
/// Merging will be done according to ordering at `ordering_values` vector.
/// As an example `ordering_values` can be [
///      \[(1, a), (2, b), (3, b), (4, a), (5, b) \],
///      \[(1, a), (2, b), (3, b), (4, a) \],
///      \[(1, b), (2, c), (3, d), (4, e), (5, a), (6, b) \],
/// ]
/// For each ScalarValue in the `values` we have a corresponding `Vec<ScalarValue>` (like timestamp of it)
/// for the example above `sort_options` will have size two, that defines ordering requirement of the merge.
/// Inner `Vec<ScalarValue>`s of the `ordering_values` will be compared according `sort_options` (Their sizes should match)
fn merge_ordered_arrays(
    // We will merge values into single `Vec<ScalarValue>`.
    values: &mut [VecDeque<ScalarValue>],
    // `values` will be merged according to `ordering_values`.
    // Inner `Vec<ScalarValue>` can be thought as ordering information for the
    // each `ScalarValue` in the values`.
    ordering_values: &mut [VecDeque<Vec<ScalarValue>>],
    // Defines according to which ordering comparisons should be done.
    sort_options: &[SortOptions],
) -> datafusion_common::Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    // Keep track the most recent data of each branch, in binary heap data structure.
    let mut heap = BinaryHeap::<CustomElement>::new();

    if values.len() != ordering_values.len()
        || values
            .iter()
            .zip(ordering_values.iter())
            .any(|(vals, ordering_vals)| vals.len() != ordering_vals.len())
    {
        return exec_err!(
            "Expects values arguments and/or ordering_values arguments to have same size"
        );
    }
    let n_branch = values.len();
    let mut merged_values = vec![];
    let mut merged_orderings = vec![];
    // Continue iterating the loop until consuming data of all branches.
    loop {
        let minimum = if let Some(minimum) = heap.pop() {
            minimum
        } else {
            // Heap is empty, fill it with the next entries from each branch.
            for branch_idx in 0..n_branch {
                if let Some(orderings) = ordering_values[branch_idx].pop_front() {
                    // Their size should be same, we can safely .unwrap here.
                    let value = values[branch_idx].pop_front().unwrap();
                    // Push the next element to the heap:
                    heap.push(CustomElement::new(
                        branch_idx,
                        value,
                        orderings,
                        sort_options,
                    ));
                }
                // If None, we consumed this branch, skip it.
            }

            // Now we have filled the heap, get the largest entry (this will be
            // the next element in merge).
            if let Some(minimum) = heap.pop() {
                minimum
            } else {
                // Heap is empty, this means that all indices are same with
                // `end_indices`. We have consumed all of the branches, merge
                // is completed, exit from the loop:
                break;
            }
        };
        let CustomElement {
            branch_idx,
            value,
            ordering,
            ..
        } = minimum;
        // Add minimum value in the heap to the result
        merged_values.push(value);
        merged_orderings.push(ordering);

        // If there is an available entry, push next entry in the most
        // recently consumed branch to the heap.
        if let Some(orderings) = ordering_values[branch_idx].pop_front() {
            // Their size should be same, we can safely .unwrap here.
            let value = values[branch_idx].pop_front().unwrap();
            // Push the next element to the heap:
            heap.push(CustomElement::new(
                branch_idx,
                value,
                orderings,
                sort_options,
            ));
        }
    }

    Ok((merged_values, merged_orderings))
}
