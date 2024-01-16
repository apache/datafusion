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
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{
    reverse_order_bys, AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use arrow_array::cast::AsArray;
use arrow_schema::{Fields, SortOptions};
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;

use itertools::izip;

/// Expression for a `ARRAY_AGG(... ORDER BY ..., ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    /// Column name
    name: String,
    /// The `DataType` for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
    /// If the input expression can have `NULL`s
    nullable: bool,
    /// Ordering data types
    order_by_data_types: Vec<DataType>,
    /// Ordering requirement
    ordering_req: LexOrdering,
    /// Whether the aggregation is running in reverse
    reverse: bool,
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
            reverse: false,
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
        OrderSensitiveArrayAggAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
            self.reverse,
        )
        .map(|acc| Box::new(acc) as _)
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
        (!self.ordering_req.is_empty()).then_some(&self.ordering_req)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(Self {
            name: self.name.to_string(),
            input_data_type: self.input_data_type.clone(),
            expr: self.expr.clone(),
            nullable: self.nullable,
            order_by_data_types: self.order_by_data_types.clone(),
            // Reverse requirement:
            ordering_req: reverse_order_bys(&self.ordering_req),
            reverse: !self.reverse,
        }))
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
    /// Stores entries in the `ARRAY_AGG` result.
    values: Vec<ScalarValue>,
    /// Stores values of ordering requirement expressions corresponding to each
    /// entry in `values`. This information is used when merging results from
    /// different partitions. For detailed information how merging is done, see
    /// [`merge_ordered_arrays`].
    ordering_values: Vec<Vec<ScalarValue>>,
    /// Stores datatypes of expressions inside values and ordering requirement
    /// expressions.
    datatypes: Vec<DataType>,
    /// Stores the ordering requirement of the `Accumulator`.
    ordering_req: LexOrdering,
    /// Whether the aggregation is running in reverse.
    reverse: bool,
}

impl OrderSensitiveArrayAggAccumulator {
    /// Create a new order-sensitive ARRAY_AGG accumulator based on the given
    /// item data type.
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        reverse: bool,
    ) -> Result<Self> {
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            values: vec![],
            ordering_values: vec![],
            datatypes,
            ordering_req,
            reverse,
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
        // First entry in the state is the aggregation result. Second entry
        // stores values received for ordering requirement columns for each
        // aggregation value inside `ARRAY_AGG` list. For each `StructArray`
        // inside `ARRAY_AGG` list, we will receive an `Array` that stores values
        // received from its ordering requirement expression. (This information
        // is necessary for during merging).
        let [array_agg_values, agg_orderings, ..] = &states else {
            return exec_err!("State should have two elements");
        };
        let Some(agg_orderings) = agg_orderings.as_list_opt::<i32>() else {
            return exec_err!("Expects to receive a list array");
        };

        // Stores ARRAY_AGG results coming from each partition
        let mut partition_values = vec![];
        // Stores ordering requirement expression results coming from each partition
        let mut partition_ordering_values = vec![];

        // Existing values should be merged also.
        partition_values.push(self.values.clone().into());
        partition_ordering_values.push(self.ordering_values.clone().into());

        let array_agg_res = ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;

        for v in array_agg_res.into_iter() {
            partition_values.push(v.into());
        }

        let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;

        let ordering_values = orderings.into_iter().map(|partition_ordering_rows| {
            // Extract value from struct to ordering_rows for each group/partition
            partition_ordering_rows.into_iter().map(|ordering_row| {
                if let ScalarValue::Struct(Some(ordering_columns_per_row), _) = ordering_row {
                    Ok(ordering_columns_per_row)
                } else {
                    exec_err!(
                        "Expects to receive ScalarValue::Struct(Some(..), _) but got: {:?}",
                        ordering_row.data_type()
                    )
                }
            }).collect::<Result<VecDeque<_>>>()
        }).collect::<Result<Vec<_>>>()?;
        for ordering_values in ordering_values.into_iter() {
            partition_ordering_values.push(ordering_values);
        }

        let sort_options = self
            .ordering_req
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();
        (self.values, self.ordering_values) = merge_ordered_arrays(
            &partition_values,
            &partition_ordering_values,
            &sort_options,
        )?;
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate()?];
        result.push(self.evaluate_orderings()?);
        Ok(result)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let values = self.values.clone();
        let array = if self.reverse {
            ScalarValue::new_list_from_iter(values.into_iter().rev(), &self.datatypes[0])
        } else {
            ScalarValue::new_list_from_iter(values.into_iter(), &self.datatypes[0])
        };
        Ok(ScalarValue::List(array))
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
    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields);

        let orderings: Vec<ScalarValue> = self
            .ordering_values
            .iter()
            .map(|ordering| {
                ScalarValue::Struct(Some(ordering.clone()), struct_field.clone())
            })
            .collect();
        let struct_type = DataType::Struct(struct_field);

        // Wrap in List, so we have the same data structure ListArray(StructArray..) for group by cases
        let arr = ScalarValue::new_list(&orderings, &struct_type);
        Ok(ScalarValue::List(arr))
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
pub(crate) fn merge_ordered_arrays(
    // We will merge values into single `Vec<ScalarValue>`.
    values: &[VecDeque<ScalarValue>],
    // `values` will be merged according to `ordering_values`.
    // Inner `Vec<ScalarValue>` can be thought as ordering information for the
    // each `ScalarValue` in the values`.
    ordering_values: &[VecDeque<Vec<ScalarValue>>],
    // Defines according to which ordering comparisons should be done.
    sort_options: &[SortOptions],
) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    // Keep track the most recent data of each branch, in binary heap data structure.
    let mut heap = BinaryHeap::<CustomElement>::new();

    if values.len() != ordering_values.len()
        || values
            .iter()
            .zip(ordering_values)
            .any(|(vals, ordering_vals)| vals.len() != ordering_vals.len())
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
        let minimum = if let Some(minimum) = heap.pop() {
            minimum
        } else {
            // Heap is empty, fill it with the next entries from each branch.
            for (idx, end_idx, ordering, branch_index) in izip!(
                indices.iter(),
                end_indices.iter(),
                ordering_values.iter(),
                0..n_branch
            ) {
                // If we consumed this branch, skip it.
                if idx != end_idx {
                    // Push the next element to the heap:
                    heap.push(CustomElement::new(
                        branch_index,
                        values[branch_index][*idx].clone(),
                        ordering[*idx].to_vec(),
                        sort_options,
                    ));
                }
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
        let branch_idx = minimum.branch_idx;
        // Increment the index of merged branch,
        indices[branch_idx] += 1;
        let row_idx = indices[branch_idx];
        merged_values.push(minimum.value);
        merged_orderings.push(minimum.ordering);
        if row_idx < end_indices[branch_idx] {
            // If there is an available entry, push next entry in the most
            // recently consumed branch to the heap.
            let value = values[branch_idx][row_idx].clone();
            let ordering_row = ordering_values[branch_idx][row_idx].to_vec();
            heap.push(CustomElement::new(
                branch_idx,
                value,
                ordering_row,
                sort_options,
            ));
        }
    }

    Ok((merged_values, merged_orderings))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use crate::aggregate::array_agg_ordered::merge_ordered_arrays;

    use arrow_array::{Array, ArrayRef, Int64Array};
    use arrow_schema::SortOptions;
    use datafusion_common::utils::get_row_at_idx;
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn test_merge_asc() -> Result<()> {
        let lhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
        ];
        let n_row = lhs_arrays[0].len();
        let lhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&lhs_arrays, idx))
            .collect::<Result<VecDeque<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<VecDeque<_>>>()?;
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
            .collect::<Result<VecDeque<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<VecDeque<_>>>()?;
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
            .collect::<Result<VecDeque<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2, 1, 1, 0, 0])),
            Arc::new(Int64Array::from(vec![4, 3, 2, 1, 0])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<VecDeque<_>>>()?;
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
            .collect::<Result<VecDeque<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from(vec![0, 1, 2, 1, 2])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<VecDeque<_>>>()?;
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
