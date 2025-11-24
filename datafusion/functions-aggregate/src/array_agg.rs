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

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::mem::{size_of, size_of_val, take};
use std::sync::Arc;

use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BooleanArray, ListArray, StructArray,
};
use arrow::compute::{filter, SortOptions};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use datafusion_common::cast::as_list_array;
use datafusion_common::utils::{
    compare_rows, get_row_at_idx, take_function_args, SingleRowListArrayBuilder,
};
use datafusion_common::{assert_eq_or_internal_err, exec_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_functions_aggregate_common::merge_arrays::merge_ordered_arrays;
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

make_udaf_expr_and_func!(
    ArrayAgg,
    array_agg,
    expression,
    "input values, including nulls, concatenated into an array",
    array_agg_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = r#"Returns an array created from the expression elements. If ordering is required, elements are inserted in the specified order.
This aggregation function can only mix DISTINCT and ORDER BY if the ordering expression is exactly the same as the argument expression."#,
    syntax_example = "array_agg(expression [ORDER BY expression])",
    sql_example = r#"
```sql
> SELECT array_agg(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| array_agg(column_name ORDER BY other_column)  |
+-----------------------------------------------+
| [element1, element2, element3]                |
+-----------------------------------------------+
> SELECT array_agg(DISTINCT column_name ORDER BY column_name) FROM table_name;
+--------------------------------------------------------+
| array_agg(DISTINCT column_name ORDER BY column_name)  |
+--------------------------------------------------------+
| [element1, element2, element3]                         |
+--------------------------------------------------------+
```
"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
/// ARRAY_AGG aggregate expression
pub struct ArrayAgg {
    signature: Signature,
    is_input_pre_ordered: bool,
}

impl Default for ArrayAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_input_pre_ordered: false,
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

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            return Ok(vec![Field::new_list(
                format_state_name(args.name, "distinct_array_agg"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.input_fields[0].data_type().clone(), true),
                true,
            )
            .into()]);
        }

        let mut fields = vec![Field::new_list(
            format_state_name(args.name, "array_agg"),
            // See COMMENTS.md to understand why nullable is set to true
            Field::new_list_field(args.input_fields[0].data_type().clone(), true),
            true,
        )
        .into()];

        if args.ordering_fields.is_empty() {
            return Ok(fields);
        }

        let orderings = args.ordering_fields.to_vec();
        fields.push(
            Field::new_list(
                format_state_name(args.name, "array_agg_orderings"),
                Field::new_list_field(DataType::Struct(Fields::from(orderings)), true),
                false,
            )
            .into(),
        );

        Ok(fields)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::SoftRequirement
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(Self {
            signature: self.signature.clone(),
            is_input_pre_ordered: beneficial_ordering,
        })))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let field = &acc_args.expr_fields[0];
        let data_type = field.data_type();
        let ignore_nulls = acc_args.ignore_nulls && field.is_nullable();

        if acc_args.is_distinct {
            // Limitation similar to Postgres. The aggregation function can only mix
            // DISTINCT and ORDER BY if all the expressions in the ORDER BY appear
            // also in the arguments of the function. This implies that if the
            // aggregation function only accepts one argument, only one argument
            // can be used in the ORDER BY, For example:
            //
            // ARRAY_AGG(DISTINCT col)
            //
            // can only be mixed with an ORDER BY if the order expression is "col".
            //
            // ARRAY_AGG(DISTINCT col ORDER BY col)                         <- Valid
            // ARRAY_AGG(DISTINCT concat(col, '') ORDER BY concat(col, '')) <- Valid
            // ARRAY_AGG(DISTINCT col ORDER BY other_col)                   <- Invalid
            // ARRAY_AGG(DISTINCT col ORDER BY concat(col, ''))             <- Invalid
            let sort_option = match acc_args.order_bys {
                [single] if single.expr.eq(&acc_args.exprs[0]) => Some(single.options),
                [] => None,
                _ => {
                    return exec_err!(
                        "In an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    );
                }
            };
            return Ok(Box::new(DistinctArrayAggAccumulator::try_new(
                data_type,
                sort_option,
                ignore_nulls,
            )?));
        }

        let Some(ordering) = LexOrdering::new(acc_args.order_bys.to_vec()) else {
            return Ok(Box::new(ArrayAggAccumulator::try_new(
                data_type,
                ignore_nulls,
            )?));
        };

        let ordering_dtypes = ordering
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        OrderSensitiveArrayAggAccumulator::try_new(
            data_type,
            &ordering_dtypes,
            ordering,
            self.is_input_pre_ordered,
            acc_args.is_reversed,
            ignore_nulls,
        )
        .map(|acc| Box::new(acc) as _)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(array_agg_udaf())
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct ArrayAggAccumulator {
    values: Vec<ArrayRef>,
    datatype: DataType,
    ignore_nulls: bool,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType, ignore_nulls: bool) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
            ignore_nulls,
        })
    }

    /// This function will return the underlying list array values if all valid values are consecutive without gaps (i.e. no null value point to a non-empty list)
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

        // According to the Arrow spec, null values can point to non-empty lists
        // So this will check if all null values starting from the first valid value to the last one point to a 0 length list so we can just slice the underlying value

        // Unwrapping is safe as we just checked if there is a null value
        let nulls = list_array.nulls().unwrap();

        let mut valid_slices_iter = nulls.valid_slices();

        // This is safe as we validated that there is at least 1 valid value in the array
        let (start, end) = valid_slices_iter.next().unwrap();

        let start_offset = offsets[start];

        // End is exclusive, so it already point to the last offset value
        // This is valid as the length of the array is always 1 less than the length of the offsets
        let mut end_offset_of_last_valid_value = offsets[end];

        for (start, end) in valid_slices_iter {
            // If there is a null value that point to a non-empty list than the start offset of the valid value
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

        assert_eq_or_internal_err!(values.len(), 1, "expects single batch");

        let val = &values[0];
        let nulls = if self.ignore_nulls {
            val.logical_nulls()
        } else {
            None
        };

        let val = match nulls {
            Some(nulls) if nulls.null_count() >= val.len() => return Ok(()),
            Some(nulls) => filter(val, &BooleanArray::new(nulls.inner().clone(), None))?,
            None => Arc::clone(val),
        };

        if !val.is_empty() {
            self.values.push(val)
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Append value like ListArray(Int64Array(1,2,3), Int64Array(4,5,6))
        if states.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(states.len(), 1, "expects single state");

        let list_arr = as_list_array(&states[0])?;

        match Self::get_optional_values_to_merge_as_is(list_arr) {
            Some(values) => {
                // Make sure we don't insert empty lists
                if !values.is_empty() {
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
                // Each ArrayRef might be just a reference to a bigger array, and many
                // ArrayRefs here might be referencing exactly the same array, so if we
                // were to call `arr.get_array_memory_size()`, we would be double-counting
                // the same underlying data many times.
                //
                // Instead, we do an approximation by estimating how much memory each
                // ArrayRef would occupy if its underlying data was fully owned by this
                // accumulator.
                //
                // Note that this is just an estimation, but the reality is that this
                // accumulator might not own any data.
                .map(|arr| arr.to_data().get_slice_memory_size().unwrap_or_default())
                .sum::<usize>()
            + self.datatype.size()
            - size_of_val(&self.datatype)
    }
}

#[derive(Debug)]
struct DistinctArrayAggAccumulator {
    values: HashSet<ScalarValue>,
    datatype: DataType,
    sort_options: Option<SortOptions>,
    ignore_nulls: bool,
}

impl DistinctArrayAggAccumulator {
    pub fn try_new(
        datatype: &DataType,
        sort_options: Option<SortOptions>,
        ignore_nulls: bool,
    ) -> Result<Self> {
        Ok(Self {
            values: HashSet::new(),
            datatype: datatype.clone(),
            sort_options,
            ignore_nulls,
        })
    }
}

impl Accumulator for DistinctArrayAggAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let val = &values[0];
        let nulls = if self.ignore_nulls {
            val.logical_nulls()
        } else {
            None
        };

        let nulls = nulls.as_ref();
        if nulls.is_none_or(|nulls| nulls.null_count() < val.len()) {
            for i in 0..val.len() {
                if nulls.is_none_or(|nulls| nulls.is_valid(i)) {
                    self.values
                        .insert(ScalarValue::try_from_array(val, i)?.compacted());
                }
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(states.len(), 1, "expects single state");

        states[0]
            .as_list::<i32>()
            .iter()
            .flatten()
            .try_for_each(|val| self.update_batch(&[val]))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut values: Vec<ScalarValue> = self.values.iter().cloned().collect();
        if values.is_empty() {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }

        if let Some(opts) = self.sort_options {
            let mut delayed_cmp_err = Ok(());
            values.sort_by(|a, b| {
                if a.is_null() {
                    return match opts.nulls_first {
                        true => Ordering::Less,
                        false => Ordering::Greater,
                    };
                }
                if b.is_null() {
                    return match opts.nulls_first {
                        true => Ordering::Greater,
                        false => Ordering::Less,
                    };
                }
                match opts.descending {
                    true => b.try_cmp(a),
                    false => a.try_cmp(b),
                }
                .unwrap_or_else(|err| {
                    delayed_cmp_err = Err(err);
                    Ordering::Equal
                })
            });
            delayed_cmp_err?;
        };

        let arr = ScalarValue::new_list(&values, &self.datatype, true);
        Ok(ScalarValue::List(arr))
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_hashset(&self.values)
            - size_of_val(&self.values)
            + self.datatype.size()
            - size_of_val(&self.datatype)
            - size_of_val(&self.sort_options)
            + size_of::<Option<SortOptions>>()
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
    /// Whether the input is known to be pre-ordered
    is_input_pre_ordered: bool,
    /// Whether the aggregation is running in reverse.
    reverse: bool,
    /// Whether the aggregation should ignore null values.
    ignore_nulls: bool,
}

impl OrderSensitiveArrayAggAccumulator {
    /// Create a new order-sensitive ARRAY_AGG accumulator based on the given
    /// item data type.
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        is_input_pre_ordered: bool,
        reverse: bool,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            values: vec![],
            ordering_values: vec![],
            datatypes,
            ordering_req,
            is_input_pre_ordered,
            reverse,
            ignore_nulls,
        })
    }

    fn sort(&mut self) {
        let sort_options = self
            .ordering_req
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();
        let mut values = take(&mut self.values)
            .into_iter()
            .zip(take(&mut self.ordering_values))
            .collect::<Vec<_>>();
        let mut delayed_cmp_err = Ok(());
        values.sort_by(|(_, left_ordering), (_, right_ordering)| {
            compare_rows(left_ordering, right_ordering, &sort_options).unwrap_or_else(
                |err| {
                    delayed_cmp_err = Err(err);
                    Ordering::Equal
                },
            )
        });
        (self.values, self.ordering_values) = values.into_iter().unzip();
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);

        let column_wise_ordering_values = if self.ordering_values.is_empty() {
            fields
                .iter()
                .map(|f| new_empty_array(f.data_type()))
                .collect::<Vec<_>>()
        } else {
            (0..fields.len())
                .map(|i| {
                    let column_values = self.ordering_values.iter().map(|x| x[i].clone());
                    ScalarValue::iter_to_array(column_values)
                })
                .collect::<Result<_>>()?
        };

        let ordering_array = StructArray::try_new(
            Fields::from(fields),
            column_wise_ordering_values,
            None,
        )?;
        Ok(SingleRowListArrayBuilder::new(Arc::new(ordering_array)).build_list_scalar())
    }
}

impl Accumulator for OrderSensitiveArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let val = &values[0];
        let ord = &values[1..];
        let nulls = if self.ignore_nulls {
            val.logical_nulls()
        } else {
            None
        };

        let nulls = nulls.as_ref();
        if nulls.is_none_or(|nulls| nulls.null_count() < val.len()) {
            for i in 0..val.len() {
                if nulls.is_none_or(|nulls| nulls.is_valid(i)) {
                    self.values
                        .push(ScalarValue::try_from_array(val, i)?.compacted());
                    self.ordering_values.push(
                        get_row_at_idx(ord, i)?
                            .into_iter()
                            .map(|v| v.compacted())
                            .collect(),
                    )
                }
            }
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
        let [array_agg_values, agg_orderings] =
            take_function_args("OrderSensitiveArrayAggAccumulator::merge_batch", states)?;
        let Some(agg_orderings) = agg_orderings.as_list_opt::<i32>() else {
            return exec_err!("Expects to receive a list array");
        };

        // Stores ARRAY_AGG results coming from each partition
        let mut partition_values = vec![];
        // Stores ordering requirement expression results coming from each partition
        let mut partition_ordering_values = vec![];

        // Existing values should be merged also.
        if !self.is_input_pre_ordered {
            self.sort();
        }
        partition_values.push(take(&mut self.values).into());
        partition_ordering_values.push(take(&mut self.ordering_values).into());

        // Convert array to Scalars to sort them easily. Convert back to array at evaluation.
        let array_agg_res = ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;
        for maybe_v in array_agg_res.into_iter() {
            if let Some(v) = maybe_v {
                partition_values.push(v.into());
            } else {
                partition_values.push(vec![].into());
            }
        }

        let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;
        for partition_ordering_rows in orderings.into_iter().flatten() {
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
        if !self.is_input_pre_ordered {
            self.sort();
        }

        let mut result = vec![self.evaluate()?];
        result.push(self.evaluate_orderings()?);

        Ok(result)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if !self.is_input_pre_ordered {
            self.sort();
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListBuilder, StringBuilder};
    use arrow::datatypes::{FieldRef, Schema};
    use datafusion_common::cast::as_generic_string_array;
    use datafusion_common::internal_err;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use std::sync::Arc;

    #[test]
    fn no_duplicates_no_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"])])?;
        acc2.update_batch(&[data(["d", "e", "f"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["a", "b", "c", "d", "e", "f"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"])])?;
        acc2.update_batch(&[data(["d", "e", "f"])])?;
        acc1 = merge(acc1, acc2)?;

        let mut result = print_nulls(str_arr(acc1.evaluate()?)?);
        result.sort();

        assert_eq!(result, vec!["a", "b", "c", "d", "e", "f"]);

        Ok(())
    }

    #[test]
    fn duplicates_no_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"])])?;
        acc2.update_batch(&[data(["a", "b", "c"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["a", "b", "c", "a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn duplicates_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"])])?;
        acc2.update_batch(&[data(["a", "b", "c"])])?;
        acc1 = merge(acc1, acc2)?;

        let mut result = print_nulls(str_arr(acc1.evaluate()?)?);
        result.sort();

        assert_eq!(result, vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn duplicates_on_second_batch_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data(["a", "c"])])?;
        acc2.update_batch(&[data(["d", "a", "b", "c"])])?;
        acc1 = merge(acc1, acc2)?;

        let mut result = print_nulls(str_arr(acc1.evaluate()?)?);
        result.sort();

        assert_eq!(result, vec!["a", "b", "c", "d"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_asc() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(false, false))
            .build_two()?;

        acc1.update_batch(&[data(["e", "b", "d"])])?;
        acc2.update_batch(&[data(["f", "a", "c"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["a", "b", "c", "d", "e", "f"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_desc() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(true, false))
            .build_two()?;

        acc1.update_batch(&[data(["e", "b", "d"])])?;
        acc2.update_batch(&[data(["f", "a", "c"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["f", "e", "d", "c", "b", "a"]);

        Ok(())
    }

    #[test]
    fn duplicates_distinct_sort_asc() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(false, false))
            .build_two()?;

        acc1.update_batch(&[data(["a", "c", "b"])])?;
        acc2.update_batch(&[data(["b", "c", "a"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn duplicates_distinct_sort_desc() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(true, false))
            .build_two()?;

        acc1.update_batch(&[data(["a", "c", "b"])])?;
        acc2.update_batch(&[data(["b", "c", "a"])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["c", "b", "a"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_asc_nulls_first() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(false, true))
            .build_two()?;

        acc1.update_batch(&[data([Some("e"), Some("b"), None])])?;
        acc2.update_batch(&[data([Some("f"), Some("a"), None])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["NULL", "a", "b", "e", "f"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_asc_nulls_last() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(false, false))
            .build_two()?;

        acc1.update_batch(&[data([Some("e"), Some("b"), None])])?;
        acc2.update_batch(&[data([Some("f"), Some("a"), None])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["a", "b", "e", "f", "NULL"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_desc_nulls_first() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(true, true))
            .build_two()?;

        acc1.update_batch(&[data([Some("e"), Some("b"), None])])?;
        acc2.update_batch(&[data([Some("f"), Some("a"), None])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["NULL", "f", "e", "b", "a"]);

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_desc_nulls_last() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .order_by_col("col", SortOptions::new(true, false))
            .build_two()?;

        acc1.update_batch(&[data([Some("e"), Some("b"), None])])?;
        acc2.update_batch(&[data([Some("f"), Some("a"), None])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);

        assert_eq!(result, vec!["f", "e", "b", "a", "NULL"]);

        Ok(())
    }

    #[test]
    fn all_nulls_on_first_batch_with_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data::<Option<&str>, 3>([None, None, None])])?;
        acc2.update_batch(&[data([Some("a"), None, None, None])])?;
        acc1 = merge(acc1, acc2)?;

        let mut result = print_nulls(str_arr(acc1.evaluate()?)?);
        result.sort();
        assert_eq!(result, vec!["NULL", "a"]);
        Ok(())
    }

    #[test]
    fn all_nulls_on_both_batches_with_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data::<Option<&str>, 3>([None, None, None])])?;
        acc2.update_batch(&[data::<Option<&str>, 4>([None, None, None, None])])?;
        acc1 = merge(acc1, acc2)?;

        let result = print_nulls(str_arr(acc1.evaluate()?)?);
        assert_eq!(result, vec!["NULL"]);
        Ok(())
    }

    #[test]
    fn does_not_over_account_memory() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data(["a", "c", "b"])])?;
        acc2.update_batch(&[data(["b", "c", "a"])])?;
        acc1 = merge(acc1, acc2)?;

        assert_eq!(acc1.size(), 266);

        Ok(())
    }
    #[test]
    fn does_not_over_account_memory_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::string()
            .distinct()
            .build_two()?;

        acc1.update_batch(&[string_list_data([
            vec!["a", "b", "c"],
            vec!["d", "e", "f"],
        ])])?;
        acc2.update_batch(&[string_list_data([vec!["e", "f", "g"]])])?;
        acc1 = merge(acc1, acc2)?;

        // without compaction, the size is 16660
        assert_eq!(acc1.size(), 1660);

        Ok(())
    }

    #[test]
    fn does_not_over_account_memory_ordered() -> Result<()> {
        let mut acc = ArrayAggAccumulatorBuilder::string()
            .order_by_col("col", SortOptions::new(false, false))
            .build()?;

        acc.update_batch(&[string_list_data([
            vec!["a", "b", "c"],
            vec!["c", "d", "e"],
            vec!["b", "c", "d"],
        ])])?;

        // without compaction, the size is 17112
        assert_eq!(acc.size(), 2184);

        Ok(())
    }

    struct ArrayAggAccumulatorBuilder {
        return_field: FieldRef,
        distinct: bool,
        order_bys: Vec<PhysicalSortExpr>,
        schema: Schema,
    }

    impl ArrayAggAccumulatorBuilder {
        fn string() -> Self {
            Self::new(DataType::Utf8)
        }

        fn new(data_type: DataType) -> Self {
            Self {
                return_field: Field::new("f", data_type.clone(), true).into(),
                distinct: false,
                order_bys: vec![],
                schema: Schema {
                    fields: Fields::from(vec![Field::new(
                        "col",
                        DataType::new_list(data_type, true),
                        true,
                    )]),
                    metadata: Default::default(),
                },
            }
        }

        fn distinct(mut self) -> Self {
            self.distinct = true;
            self
        }

        fn order_by_col(mut self, col: &str, sort_options: SortOptions) -> Self {
            let new_order = PhysicalSortExpr::new(
                Arc::new(
                    Column::new_with_schema(col, &self.schema)
                        .expect("column not available in schema"),
                ),
                sort_options,
            );
            self.order_bys.push(new_order);
            self
        }

        fn build(&self) -> Result<Box<dyn Accumulator>> {
            let expr = Arc::new(Column::new("col", 0));
            let expr_field = expr.return_field(&self.schema)?;
            ArrayAgg::default().accumulator(AccumulatorArgs {
                return_field: Arc::clone(&self.return_field),
                schema: &self.schema,
                expr_fields: &[expr_field],
                ignore_nulls: false,
                order_bys: &self.order_bys,
                is_reversed: false,
                name: "",
                is_distinct: self.distinct,
                exprs: &[expr],
            })
        }

        fn build_two(&self) -> Result<(Box<dyn Accumulator>, Box<dyn Accumulator>)> {
            Ok((self.build()?, self.build()?))
        }
    }

    fn str_arr(value: ScalarValue) -> Result<Vec<Option<String>>> {
        let ScalarValue::List(list) = value else {
            return internal_err!("ScalarValue was not a List");
        };
        Ok(as_generic_string_array::<i32>(list.values())?
            .iter()
            .map(|v| v.map(|v| v.to_string()))
            .collect())
    }

    fn print_nulls(sort: Vec<Option<String>>) -> Vec<String> {
        sort.into_iter()
            .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
            .collect()
    }

    fn string_list_data<'a>(data: impl IntoIterator<Item = Vec<&'a str>>) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for string_list in data.into_iter() {
            builder.append_value(string_list.iter().map(Some).collect::<Vec<_>>());
        }

        Arc::new(builder.finish())
    }

    fn data<T, const N: usize>(list: [T; N]) -> ArrayRef
    where
        ScalarValue: From<T>,
    {
        let values: Vec<_> = list.into_iter().map(ScalarValue::from).collect();
        ScalarValue::iter_to_array(values).expect("Cannot convert to array")
    }

    fn merge(
        mut acc1: Box<dyn Accumulator>,
        mut acc2: Box<dyn Accumulator>,
    ) -> Result<Box<dyn Accumulator>> {
        let intermediate_state = acc2.state().and_then(|e| {
            e.iter()
                .map(|v| v.to_array())
                .collect::<Result<Vec<ArrayRef>>>()
        })?;
        acc1.merge_batch(&intermediate_state)?;
        Ok(acc1)
    }
}
