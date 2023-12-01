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

//! Defines physical expressions which specify ordering requirement
//! that can evaluated at runtime during query execution

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr};

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{Fields, SortOptions};
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::{compare_rows, get_arrayref_at_indices, get_row_at_idx};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;

use itertools::izip;

/// Expression for a ARRAY_AGG(ORDER BY) aggregation.
/// When aggregation works in multiple partitions
/// aggregations are split into multiple partitions,
/// then their results are merged. This aggregator
/// is a version of ARRAY_AGG that can support producing
/// intermediate aggregation (with necessary side information)
/// and that can merge aggregations from multiple partitions.
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    /// Column name
    name: String,
    /// The DataType for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
    /// If the input expression can have NULLs
    nullable: bool,
    /// Ordering data types
    order_by_data_types: Vec<DataType>,
    /// Ordering requirement
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAgg {
    /// Create a new `OrderSensitiveArrayAgg` aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        nullable: bool,
        order_by_data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            expr,
            nullable,
            order_by_data_types,
            ordering_req,
        }
    }
}

impl AggregateExpr for OrderSensitiveArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            // This should be the same as return type of AggregateFunction::ArrayAgg
            Field::new("item", self.input_data_type.clone(), true),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(OrderSensitiveArrayAggAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.input_data_type.clone(), true),
            self.nullable, // This should be the same as field()
        )];
        let orderings = ordering_fields(&self.ordering_req, &self.order_by_data_types);
        fields.push(Field::new_list(
            format_state_name(&self.name, "array_agg_orderings"),
            Field::new("item", DataType::Struct(Fields::from(orderings)), true),
            self.nullable,
        ));
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        if self.ordering_req.is_empty() {
            None
        } else {
            Some(&self.ordering_req)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for OrderSensitiveArrayAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.order_by_data_types == x.order_by_data_types
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct OrderSensitiveArrayAggAccumulator {
    // `values` stores entries in the ARRAY_AGG result.
    values: Vec<ScalarValue>,
    // `ordering_values` stores values of ordering requirement expression
    // corresponding to each value in the ARRAY_AGG.
    // For each `ScalarValue` inside `values`, there will be a corresponding
    // `Vec<ScalarValue>` inside `ordering_values` which stores it ordering.
    // This information is used during merging results of the different partitions.
    // For detailed information how merging is done see [`merge_ordered_arrays`]
    ordering_values: Vec<Vec<ScalarValue>>,
    // `datatypes` stores, datatype of expression inside ARRAY_AGG and ordering requirement expressions.
    datatypes: Vec<DataType>,
    // Stores ordering requirement of the Accumulator
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAggAccumulator {
    /// Create a new order-sensitive ARRAY_AGG accumulator based on the given
    /// item data type.
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> Result<Self> {
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            values: vec![],
            ordering_values: vec![],
            datatypes,
            ordering_req,
        })
    }
}

impl Accumulator for OrderSensitiveArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let (new_values, new_ordering_values) = self.reorder_according_to_reqs(values)?;
        let sort_options = get_sort_options(&self.ordering_req);

        // Merge new values and new orderings
        let (merged_values, merged_ordering_values) = merge_ordered_arrays(
            &[self.values.clone(), new_values],
            &[self.ordering_values.clone(), new_ordering_values],
            &sort_options,
        )?;
        self.values = merged_values;
        self.ordering_values = merged_ordering_values;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is the aggregation result.
        let array_agg_values = &states[0];
        // 2nd entry stores values received for ordering requirement columns, for each aggregation value inside ARRAY_AGG list.
        // For each `StructArray` inside ARRAY_AGG list, we will receive an `Array` that stores
        // values received from its ordering requirement expression. (This information is necessary for during merging).
        let agg_orderings = &states[1];

        if as_list_array(agg_orderings).is_ok() {
            // Stores ARRAY_AGG results coming from each partition
            let mut partition_values = vec![];
            // Stores ordering requirement expression results coming from each partition
            let mut partition_ordering_values = vec![];

            // Existing values should be merged also.
            partition_values.push(self.values.clone());
            partition_ordering_values.push(self.ordering_values.clone());

            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;

            for v in array_agg_res.into_iter() {
                partition_values.push(v);
            }

            let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;
            // Ordering requirement expression values for each entry in the ARRAY_AGG list
            let other_ordering_values = self.convert_array_agg_to_orderings(orderings)?;
            for v in other_ordering_values.into_iter() {
                partition_ordering_values.push(v);
            }

            let sort_options = get_sort_options(&self.ordering_req);
            let (new_values, new_orderings) = merge_ordered_arrays(
                &partition_values,
                &partition_ordering_values,
                &sort_options,
            )?;
            self.values = new_values;
            self.ordering_values = new_orderings;
        } else {
            return exec_err!("Expects to receive a list array");
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate()?];
        result.push(self.evaluate_orderings()?);
        Ok(result)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let arr = ScalarValue::new_list(&self.values, &self.datatypes[0]);
        Ok(ScalarValue::List(arr))
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + ScalarValue::size_of_vec(&self.values)
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

impl OrderSensitiveArrayAggAccumulator {
    /// Inner Vec\<ScalarValue> in the ordering_values can be thought as ordering information for the each ScalarValue in the values array.
    /// See [`merge_ordered_arrays`] for more information.
    fn convert_array_agg_to_orderings(
        &self,
        array_agg: Vec<Vec<ScalarValue>>,
    ) -> Result<Vec<Vec<Vec<ScalarValue>>>> {
        let mut orderings = vec![];
        // in_data is Vec<ScalarValue> where ScalarValue does not include ScalarValue::List
        for in_data in array_agg.into_iter() {
            let ordering = in_data.into_iter().map(|struct_vals| {
                    if let ScalarValue::Struct(Some(orderings), _) = struct_vals {
                        Ok(orderings)
                    } else {
                        exec_err!(
                            "Expects to receive ScalarValue::Struct(Some(..), _) but got:{:?}",
                            struct_vals.data_type()
                        )
                    }
                }).collect::<Result<Vec<_>>>()?;
            orderings.push(ordering);
        }
        Ok(orderings)
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());
        let orderings: Vec<ScalarValue> = self
            .ordering_values
            .iter()
            .map(|ordering| {
                ScalarValue::Struct(Some(ordering.clone()), struct_field.clone())
            })
            .collect();
        let struct_type = DataType::Struct(Fields::from(fields));

        let arr = ScalarValue::new_list(&orderings, &struct_type);
        Ok(ScalarValue::List(arr))
    }

    fn reorder_according_to_reqs(
        &self,
        values: &[ArrayRef],
    ) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
        let value = &values[0];
        let orderings = &values[1..];

        if self.ordering_req.is_empty() {
            // No requirement
            Self::convert_arr_to_vec(value, orderings)
        } else {
            let sort_options = get_sort_options(&self.ordering_req);
            // Sort data according to requirements
            let sort_columns = orderings
                .iter()
                .zip(sort_options)
                .map(|(ordering, options)| SortColumn {
                    values: ordering.clone(),
                    options: Some(options),
                })
                .collect::<Vec<_>>();
            let indices = lexsort_to_indices(&sort_columns, None)?;
            let sorted_value = compute::take(
                value.as_ref(),
                &indices,
                None, // None: no index check
            )?;
            let orderings = get_arrayref_at_indices(orderings, &indices)?;

            Self::convert_arr_to_vec(&sorted_value, &orderings)
        }
    }

    fn convert_arr_to_vec(
        value: &ArrayRef,
        orderings: &[ArrayRef],
    ) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
        let n_row = value.len();
        // Convert &[ArrayRef] to Vec<Vec<ScalarValue>>
        let orderings = (0..n_row)
            .map(|idx| get_row_at_idx(orderings, idx))
            .collect::<Result<Vec<_>>>()?;

        // Convert ArrayRef to Vec<ScalarValue>
        let value = (0..n_row)
            .map(|idx| ScalarValue::try_from_array(value, idx))
            .collect::<Result<Vec<_>>>()?;

        Ok((value, orderings))
    }
}

/// This is a wrapper struct to be able to correctly merge ARRAY_AGG
/// data from multiple partitions using `BinaryHeap`.
/// When used inside `BinaryHeap` this struct returns smallest `CustomElement`,
/// where smallest is determined by `ordering` values (`Vec<ScalarValue>`)
/// according to `sort_options`
#[derive(Debug, PartialEq, Eq)]
struct CustomElement<'a> {
    // Stores from which partition entry is received
    branch_idx: usize,
    // values to be merged
    value: ScalarValue,
    // according to `ordering` values, comparisons will be done.
    ordering: Vec<ScalarValue>,
    // `sort_options` defines, desired ordering by the user
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
    ) -> Result<Ordering> {
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
    values: &[Vec<ScalarValue>],
    // `values` will be merged according to `ordering_values`.
    // Inner `Vec<ScalarValue>` can be thought as ordering information for the
    // each `ScalarValue` in the values`.
    ordering_values: &[Vec<Vec<ScalarValue>>],
    // Defines according to which ordering comparisons should be done.
    sort_options: &[SortOptions],
) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    // Keep track the most recent data of each branch, in binary heap data structure.
    let mut heap: BinaryHeap<CustomElement> = BinaryHeap::new();

    if !(values.len() == ordering_values.len()
        && values
            .iter()
            .zip(ordering_values.iter())
            .all(|(vals, ordering_vals)| vals.len() == ordering_vals.len()))
    {
        return exec_err!(
            "Expects values arguments and/or ordering_values arguments to have same size"
        );
    }
    let n_branch = values.len();
    // For each branch we keep track of indices of next will be merged entry
    let mut indices = vec![0_usize; n_branch];
    // Keep track of sizes of each branch.
    let end_indices = (0..n_branch)
        .map(|idx| values[idx].len())
        .collect::<Vec<_>>();
    let mut merged_values = vec![];
    let mut merged_orderings = vec![];
    // Continue iterating the loop until consuming data of all branches.
    loop {
        let min_elem = if let Some(min_elem) = heap.pop() {
            min_elem
        } else {
            // Heap is empty, fill it with the next entries from each branch.
            for (idx, end_idx, ordering, branch_index) in izip!(
                indices.iter(),
                end_indices.iter(),
                ordering_values.iter(),
                0..n_branch
            ) {
                // We consumed this branch, skip it
                if idx == end_idx {
                    continue;
                }

                // Push the next element to the heap.
                let elem = CustomElement::new(
                    branch_index,
                    values[branch_index][*idx].clone(),
                    ordering[*idx].to_vec(),
                    sort_options,
                );
                heap.push(elem);
            }
            // Now we have filled the heap, get the largest entry (this will be the next element in merge)
            if let Some(min_elem) = heap.pop() {
                min_elem
            } else {
                // Heap is empty, this means that all indices are same with end_indices. e.g
                // We have consumed all of the branches. Merging is completed
                // Exit from the loop
                break;
            }
        };
        let branch_idx = min_elem.branch_idx;
        // Increment the index of merged branch,
        indices[branch_idx] += 1;
        let row_idx = indices[branch_idx];
        merged_values.push(min_elem.value.clone());
        merged_orderings.push(min_elem.ordering.clone());
        if row_idx < end_indices[branch_idx] {
            // Push next entry in the most recently consumed branch to the heap
            // If there is an available entry
            let value = values[branch_idx][row_idx].clone();
            let ordering_row = ordering_values[branch_idx][row_idx].to_vec();
            let elem = CustomElement::new(branch_idx, value, ordering_row, sort_options);
            heap.push(elem);
        }
    }

    Ok((merged_values, merged_orderings))
}

/// Selects the sort option attribute from all the given `PhysicalSortExpr`s.
fn get_sort_options(ordering_req: &[PhysicalSortExpr]) -> Vec<SortOptions> {
    ordering_req
        .iter()
        .map(|item| item.options)
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use crate::aggregate::array_agg_ordered::merge_ordered_arrays;
    use arrow_array::{Array, ArrayRef, Int64Array};
    use arrow_schema::SortOptions;
    use datafusion_common::utils::get_row_at_idx;
    use datafusion_common::{Result, ScalarValue};
    use std::sync::Arc;

    #[test]
    fn test_merge_asc() -> Result<()> {
        let lhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
        ];
        let n_row = lhs_arrays[0].len();
        let lhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&lhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;
        let sort_options = vec![
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ];

        let lhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef;
        let lhs_vals = (0..lhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&lhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;
        let expected =
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4])) as ArrayRef;
        let expected_ts = vec![
            Arc::new(Int64Array::from(vec![0, 0, 0, 0, 1, 1, 1, 1, 2, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4])) as ArrayRef,
        ];

        let (merged_vals, merged_ts) = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;
        let merged_ts = (0..merged_ts[0].len())
            .map(|col_idx| {
                ScalarValue::iter_to_array(
                    (0..merged_ts.len())
                        .map(|row_idx| merged_ts[row_idx][col_idx].clone()),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(&merged_vals, &expected);
        assert_eq!(&merged_ts, &expected_ts);

        Ok(())
    }

    #[test]
    fn test_merge_desc() -> Result<()> {
        let lhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2, 1, 1, 0, 0])),
            Arc::new(Int64Array::from(vec![4, 3, 2, 1, 0])),
        ];
        let n_row = lhs_arrays[0].len();
        let lhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&lhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2, 1, 1, 0, 0])),
            Arc::new(Int64Array::from(vec![4, 3, 2, 1, 0])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;
        let sort_options = vec![
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];

        // Values (which will be merged) doesn't have to be ordered.
        let lhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 1, 2])) as ArrayRef;
        let lhs_vals = (0..lhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&lhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 1, 2])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;
        let expected =
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2, 2, 1, 1, 2, 2])) as ArrayRef;
        let expected_ts = vec![
            Arc::new(Int64Array::from(vec![2, 2, 1, 1, 1, 1, 0, 0, 0, 0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![4, 4, 3, 3, 2, 2, 1, 1, 0, 0])) as ArrayRef,
        ];
        let (merged_vals, merged_ts) = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;
        let merged_ts = (0..merged_ts[0].len())
            .map(|col_idx| {
                ScalarValue::iter_to_array(
                    (0..merged_ts.len())
                        .map(|row_idx| merged_ts[row_idx][col_idx].clone()),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(&merged_vals, &expected);
        assert_eq!(&merged_ts, &expected_ts);
        Ok(())
    }
}
