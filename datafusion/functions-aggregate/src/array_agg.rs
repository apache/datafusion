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

//! `ARRAY_AGG` aggregate implementation: [`ArrayAgg`]

use arrow::array::{new_empty_array, Array, ArrayRef, AsArray, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};

use datafusion_common::cast::as_list_array;
use datafusion_common::utils::{get_row_at_idx, SingleRowListArrayBuilder};
use datafusion_common::{exec_err, ScalarValue};
use datafusion_common::{internal_err, Result};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, Signature, Volatility};
use datafusion_expr::{AggregateUDFImpl, Documentation};
use datafusion_functions_aggregate_common::merge_arrays::merge_ordered_arrays;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use std::collections::{HashSet, VecDeque};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

make_udaf_expr_and_func!(
    ArrayAgg,
    array_agg,
    expression,
    "input values, including nulls, concatenated into an array",
    array_agg_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns an array created from the expression elements. If ordering is required, elements are inserted in the specified order.",
    syntax_example = "array_agg(expression [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT array_agg(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| array_agg(column_name ORDER BY other_column)  |
+-----------------------------------------------+
| [element1, element2, element3]                |
+-----------------------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug)]
/// ARRAY_AGG aggregate expression
pub struct ArrayAgg {
    signature: Signature,
}

impl Default for ArrayAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArrayAgg {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_agg"
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if args.is_distinct {
            return Ok(vec![Field::new_list(
                format_state_name(args.name, "distinct_array_agg"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.input_types[0].clone(), true),
                true,
            )]);
        }

        let mut fields = vec![Field::new_list(
            format_state_name(args.name, "array_agg"),
            // See COMMENTS.md to understand why nullable is set to true
            Field::new_list_field(args.input_types[0].clone(), true),
            true,
        )];

        if args.ordering_fields.is_empty() {
            return Ok(fields);
        }

        let orderings = args.ordering_fields.to_vec();
        fields.push(Field::new_list(
            format_state_name(args.name, "array_agg_orderings"),
            Field::new_list_field(DataType::Struct(Fields::from(orderings)), true),
            false,
        ));

        Ok(fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;

        if acc_args.is_distinct {
            return Ok(Box::new(DistinctArrayAggAccumulator::try_new(&data_type)?));
        }

        if acc_args.ordering_req.is_empty() {
            return Ok(Box::new(ArrayAggAccumulator::try_new(&data_type)?));
        }

        let ordering_dtypes = acc_args
            .ordering_req
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        OrderSensitiveArrayAggAccumulator::try_new(
            &data_type,
            &ordering_dtypes,
            acc_args.ordering_req.clone(),
            acc_args.is_reversed,
        )
        .map(|acc| Box::new(acc) as _)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(array_agg_udaf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct ArrayAggAccumulator {
    values: Vec<ArrayRef>,
    datatype: DataType,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
        })
    }

    /// This function will return the underlying list array values if all valid values are consecutive without gaps (i.e. no null value point to a non empty list)
    /// If there are gaps but only in the end of the list array, the function will return the values without the null values in the end
    fn get_optional_values_to_merge_as_is(list_array: &ListArray) -> Option<ArrayRef> {
        let offsets = list_array.value_offsets();
        // Offsets always have at least 1 value
        let initial_offset = offsets[0];
        let null_count = list_array.null_count();

        // If no nulls than just use the fast path
        // This is ok as the state is a ListArray rather than a ListViewArray so all the values are consecutive
        if null_count == 0 {
            // According to Arrow specification, the first offset can be non-zero
            let list_values = list_array.values().slice(
                initial_offset as usize,
                (offsets[offsets.len() - 1] - initial_offset) as usize,
            );
            return Some(list_values);
        }

        // If all the values are null than just return an empty values array
        if list_array.null_count() == list_array.len() {
            return Some(list_array.values().slice(0, 0));
        }

        // According to the Arrow spec, null values can point to non empty lists
        // So this will check if all null values starting from the first valid value to the last one point to a 0 length list so we can just slice the underlying value

        // Unwrapping is safe as we just checked if there is a null value
        let nulls = list_array.nulls().unwrap();

        let mut valid_slices_iter = nulls.valid_slices();

        // This is safe as we validated that that are at least 1 valid value in the array
        let (start, end) = valid_slices_iter.next().unwrap();

        let start_offset = offsets[start];

        // End is exclusive, so it already point to the last offset value
        // This is valid as the length of the array is always 1 less than the length of the offsets
        let mut end_offset_of_last_valid_value = offsets[end];

        for (start, end) in valid_slices_iter {
            // If there is a null value that point to a non empty list than the start offset of the valid value
            // will be different that the end offset of the last valid value
            if offsets[start] != end_offset_of_last_valid_value {
                return None;
            }

            // End is exclusive, so it already point to the last offset value
            // This is valid as the length of the array is always 1 less than the length of the offsets
            end_offset_of_last_valid_value = offsets[end];
        }

        let consecutive_valid_values = list_array.values().slice(
            start_offset as usize,
            (end_offset_of_last_valid_value - start_offset) as usize,
        );

        Some(consecutive_valid_values)
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Append value like Int64Array(1,2,3)
        if values.is_empty() {
            return Ok(());
        }

        if values.len() != 1 {
            return internal_err!("expects single batch");
        }

        let val = Arc::clone(&values[0]);
        if val.len() > 0 {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Append value like ListArray(Int64Array(1,2,3), Int64Array(4,5,6))
        if states.is_empty() {
            return Ok(());
        }

        if states.len() != 1 {
            return internal_err!("expects single state");
        }

        let list_arr = as_list_array(&states[0])?;

        match Self::get_optional_values_to_merge_as_is(list_arr) {
            Some(values) => {
                // Make sure we don't insert empty lists
                if values.len() > 0 {
                    self.values.push(values);
                }
            }
            None => {
                for arr in list_arr.iter().flatten() {
                    self.values.push(arr);
                }
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Transform Vec<ListArr> to ListArr
        let element_arrays: Vec<&dyn Array> =
            self.values.iter().map(|a| a.as_ref()).collect();

        if element_arrays.is_empty() {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }

        let concated_array = arrow::compute::concat(&element_arrays)?;

        Ok(SingleRowListArrayBuilder::new(concated_array).build_list_scalar())
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + (size_of::<ArrayRef>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + self.datatype.size()
            - size_of_val(&self.datatype)
    }
}

#[derive(Debug)]
struct DistinctArrayAggAccumulator {
    values: HashSet<ScalarValue>,
    datatype: DataType,
}

impl DistinctArrayAggAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: HashSet::new(),
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for DistinctArrayAggAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return internal_err!("expects single batch");
        }

        let array = &values[0];

        for i in 0..array.len() {
            let scalar = ScalarValue::try_from_array(&array, i)?;
            self.values.insert(scalar);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        if states.len() != 1 {
            return internal_err!("expects single state");
        }

        states[0]
            .as_list::<i32>()
            .iter()
            .flatten()
            .try_for_each(|val| self.update_batch(&[val]))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<ScalarValue> = self.values.iter().cloned().collect();
        if values.is_empty() {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }
        let arr = ScalarValue::new_list(&values, &self.datatype, true);
        Ok(ScalarValue::List(arr))
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_hashset(&self.values)
            - size_of_val(&self.values)
            + self.datatype.size()
            - size_of_val(&self.datatype)
    }
}

/// Accumulator for a `ARRAY_AGG(... ORDER BY ..., ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
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

        // Convert array to Scalars to sort them easily. Convert back to array at evaluation.
        let array_agg_res = ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;
        for v in array_agg_res.into_iter() {
            partition_values.push(v.into());
        }

        let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;

        for partition_ordering_rows in orderings.into_iter() {
            // Extract value from struct to ordering_rows for each group/partition
            let ordering_value = partition_ordering_rows.into_iter().map(|ordering_row| {
                    if let ScalarValue::Struct(s) = ordering_row {
                        let mut ordering_columns_per_row = vec![];

                        for column in s.columns() {
                            let sv = ScalarValue::try_from_array(column, 0)?;
                            ordering_columns_per_row.push(sv);
                        }

                        Ok(ordering_columns_per_row)
                    } else {
                        exec_err!(
                            "Expects to receive ScalarValue::Struct(Arc<StructArray>) but got:{:?}",
                            ordering_row.data_type()
                        )
                    }
                }).collect::<Result<VecDeque<_>>>()?;

            partition_ordering_values.push(ordering_value);
        }

        let sort_options = self
            .ordering_req
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();

        (self.values, self.ordering_values) = merge_ordered_arrays(
            &mut partition_values,
            &mut partition_ordering_values,
            &sort_options,
        )?;

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate()?];
        result.push(self.evaluate_orderings()?);

        Ok(result)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::new_null_list(
                self.datatypes[0].clone(),
                true,
                1,
            ));
        }

        let values = self.values.clone();
        let array = if self.reverse {
            ScalarValue::new_list_from_iter(
                values.into_iter().rev(),
                &self.datatypes[0],
                true,
            )
        } else {
            ScalarValue::new_list_from_iter(values.into_iter(), &self.datatypes[0], true)
        };
        Ok(ScalarValue::List(array))
    }

    fn size(&self) -> usize {
        let mut total = size_of_val(self) + ScalarValue::size_of_vec(&self.values)
            - size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total += size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total += dtype.size() - size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}

impl OrderSensitiveArrayAggAccumulator {
    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(self.ordering_req.as_ref(), &self.datatypes[1..]);
        let num_columns = fields.len();
        let struct_field = Fields::from(fields.clone());

        let mut column_wise_ordering_values = vec![];
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

        let ordering_array =
            StructArray::try_new(struct_field, column_wise_ordering_values, None)?;
        Ok(SingleRowListArrayBuilder::new(Arc::new(ordering_array)).build_list_scalar())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::compute::SortOptions;

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
            &mut [lhs_vals, rhs_vals],
            &mut [lhs_orderings, rhs_orderings],
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
            &mut [lhs_vals, rhs_vals],
            &mut [lhs_orderings, rhs_orderings],
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
