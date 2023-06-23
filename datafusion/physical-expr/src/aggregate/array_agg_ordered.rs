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

use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Array, ListArray};
use arrow_schema::{Fields, SortOptions};
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use itertools::izip;
use std::any::Any;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::sync::Arc;

/// Expression for a ARRAY_AGG(ORDER BY) aggregation.
/// When aggregation works in multiple partitions
/// aggregations are split into multiple partitions,
/// then their results are merged. This aggregator
/// is a version of ARRAY_AGG that can support producing
/// intermediate aggregation (with necessary side information)
/// and that can merge aggregations from multiple partitions.
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    name: String,
    input_data_type: DataType,
    order_by_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAgg {
    /// Create a new `OrderSensitiveArrayAgg` aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        order_by_data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            input_data_type,
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
            Field::new("item", self.input_data_type.clone(), true),
            false,
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
            false,
        )];
        let orderings = ordering_fields(&self.ordering_req, &self.order_by_data_types);
        fields.push(Field::new_list(
            format_state_name(&self.name, "array_agg_orderings"),
            Field::new(
                "item",
                DataType::Struct(Fields::from(orderings.clone())),
                true,
            ),
            false,
        ));
        fields.extend(orderings);
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
        let n_row = values[0].len();
        for index in 0..n_row {
            let row = get_row_at_idx(values, index)?;
            self.values.push(row[0].clone());
            self.ordering_values.push(row[1..].to_vec());
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is the aggregation result.
        let array_agg_values = &states[0];
        // 2nd entry stores values received for ordering requirement columns, for each aggregation value inside ARRAY_AGG list.
        // For each `ScalarValue` inside ARRAY_AGG list, we will receive a `Vec<ScalarValue>` that stores
        // values received from its ordering requirement expression. (This information is necessary for during merging).
        let agg_orderings = &states[1];
        if agg_orderings.as_any().is::<ListArray>() {
            // Stores ARRAY_AGG results coming from each partition
            let mut partition_values = vec![];
            // Stores ordering requirement expression results coming from each partition
            let mut partition_ordering_values = vec![];
            for index in 0..agg_orderings.len() {
                let ordering = ScalarValue::try_from_array(agg_orderings, index)?;
                // Ordering requirement expression values for each entry in the ARRAY_AGG list
                let other_ordering_values =
                    self.convert_array_agg_to_orderings(ordering)?;
                // ARRAY_AGG result. (It is a `ScalarValue::List` under the hood, it stores `Vec<ScalarValue>`)
                let array_agg_res = ScalarValue::try_from_array(array_agg_values, index)?;
                if let ScalarValue::List(Some(other_values), _) = array_agg_res {
                    partition_values.push(other_values);
                    partition_ordering_values.push(other_ordering_values);
                } else {
                    return Err(DataFusionError::Internal(
                        "ARRAY_AGG state must be list!".into(),
                    ));
                }
            }
            let sort_options = self
                .ordering_req
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>();
            self.values = merge_ordered_arrays(
                &partition_values,
                &partition_ordering_values,
                &sort_options,
            )?;
        } else {
            return Err(DataFusionError::Execution(
                "Expects to receive a list array".to_string(),
            ));
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate()?];
        result.push(self.evaluate_orderings()?);
        let last_ordering = if let Some(ordering) = self.ordering_values.last() {
            ordering.clone()
        } else {
            // In case ordering is empty, construct ordering as NULL:
            self.datatypes
                .iter()
                .skip(1)
                .map(ScalarValue::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        result.extend(last_ordering);
        Ok(result)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::new_list(
            Some(self.values.clone()),
            self.datatypes[0].clone(),
        ))
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
    fn convert_array_agg_to_orderings(
        &self,
        in_data: ScalarValue,
    ) -> Result<Vec<Vec<ScalarValue>>> {
        if let ScalarValue::List(Some(list_vals), _field_ref) = in_data {
            list_vals.into_iter().map(|struct_vals| {
                if let ScalarValue::Struct(Some(orderings), _fields) = struct_vals {
                    Ok(orderings)
                } else {
                    Err(DataFusionError::Execution(format!(
                        "Expects to receive ScalarValue::Struct(Some(..), _) but got:{:?}",
                        struct_vals.get_datatype()
                    )))
                }
            }).collect::<Result<Vec<_>>>()
        } else {
            Err(DataFusionError::Execution(format!(
                "Expects to receive ScalarValue::List(Some(..), _) but got:{:?}",
                in_data.get_datatype()
            )))
        }
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());
        let orderings = self
            .ordering_values
            .iter()
            .map(|ordering| {
                ScalarValue::Struct(Some(ordering.clone()), struct_field.clone())
            })
            .collect();
        let struct_type = DataType::Struct(Fields::from(fields));
        Ok(ScalarValue::new_list(Some(orderings), struct_type))
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
) -> Result<Vec<ScalarValue>> {
    // Keep track the most recent data of each branch, in binary heap data structure.
    let mut heap: BinaryHeap<CustomElement> = BinaryHeap::new();

    if !(values.len() == ordering_values.len()
        && values
            .iter()
            .zip(ordering_values.iter())
            .all(|(vals, ordering_vals)| vals.len() == ordering_vals.len()))
    {
        return Err(DataFusionError::Execution(
            "Expects values arguments and/or ordering_values arguments to have same size"
                .to_string(),
        ));
    }
    let n_branch = values.len();
    // For each branch we keep track of indices of next will be merged entry
    let mut indices = vec![0_usize; n_branch];
    // Keep track of sizes of each branch.
    let end_indices = (0..n_branch)
        .map(|idx| values[idx].len())
        .collect::<Vec<_>>();
    let mut merged_values = vec![];
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
        if row_idx < end_indices[branch_idx] {
            // Push next entry in the most recently consumed branch to the heap
            // If there is an available entry
            let value = values[branch_idx][row_idx].clone();
            let ordering_row = ordering_values[branch_idx][row_idx].to_vec();
            let elem = CustomElement::new(branch_idx, value, ordering_row, sort_options);
            heap.push(elem);
        }
    }

    Ok(merged_values)
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

        let merged_vals = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;
        assert_eq!(&merged_vals, &expected);

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

        let merged_vals = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;

        assert_eq!(&merged_vals, &expected);
        Ok(())
    }
}
