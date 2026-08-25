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

use std::collections::VecDeque;
use std::mem::{size_of, size_of_val, take};
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, ListArray, NullBufferBuilder, StructArray,
    UInt32Array, make_array, new_empty_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, cast, filter};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::row::{OwnedRow, Row, RowConverter, Rows, SortField};

use datafusion_common::cast::as_list_array;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::scalar::copy_array_data;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::utils::proxy::HashTableAllocExt;
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_err, internal_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filter_to_nulls;
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use hashbrown::hash_table::HashTable;
use itertools::{Either, Itertools};

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
| array_agg(DISTINCT column_name ORDER BY column_name)   |
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
            return Ok(vec![
                Field::new_list(
                    format_state_name(args.name, "distinct_array_agg"),
                    // See COMMENTS.md to understand why nullable is set to true
                    Field::new_list_field(args.input_fields[0].data_type().clone(), true),
                    true,
                )
                .into(),
            ]);
        }

        let mut fields = vec![
            Field::new_list(
                format_state_name(args.name, "array_agg"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.input_fields[0].data_type().clone(), true),
                true,
            )
            .into(),
        ];

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
            &ordering,
            self.is_input_pre_ordered,
            acc_args.is_reversed,
            ignore_nulls,
        )
        .map(|acc| Box::new(acc) as _)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(array_agg_udaf())
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let field = &args.expr_fields[0];
        let data_type = field.data_type().clone();
        let ignore_nulls = args.ignore_nulls && field.is_nullable();
        Ok(Box::new(ArrayAggGroupsAccumulator::new(
            data_type,
            ignore_nulls,
        )))
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
    values: VecDeque<ArrayRef>,
    datatype: DataType,
    ignore_nulls: bool,
    /// Number of elements already consumed (retracted) from the front array.
    /// Used by sliding window frames to avoid copying on partial retract.
    front_offset: usize,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType, ignore_nulls: bool) -> Result<Self> {
        Ok(Self {
            values: VecDeque::new(),
            datatype: datatype.clone(),
            ignore_nulls,
            front_offset: 0,
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
            self.values.push_back(val)
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
                    self.values.push_back(values);
                }
            }
            None => {
                for arr in list_arr.iter().flatten() {
                    self.values.push_back(arr);
                }
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }

        let element_arrays: Vec<ArrayRef> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, a)| {
                if i == 0 && self.front_offset > 0 {
                    a.slice(self.front_offset, a.len() - self.front_offset)
                } else {
                    Arc::clone(a)
                }
            })
            .collect();

        let element_refs: Vec<&dyn Array> =
            element_arrays.iter().map(|a| a.as_ref()).collect();

        if element_refs.iter().all(|a| a.is_empty()) {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }

        let concated_array = arrow::compute::concat(&element_refs)?;

        Ok(SingleRowListArrayBuilder::new(concated_array).build_list_scalar())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(values.len(), 1, "expects single batch");

        let val = &values[0];
        let mut to_retract = if self.ignore_nulls {
            val.len() - val.logical_null_count()
        } else {
            val.len()
        };

        while to_retract > 0 {
            let Some(front) = self.values.front() else {
                break;
            };
            let available = front.len() - self.front_offset;
            if to_retract >= available {
                self.values.pop_front();
                to_retract -= available;
                self.front_offset = 0;
            } else {
                self.front_offset += to_retract;
                to_retract = 0;
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
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
struct ArrayAggGroupsAccumulator {
    datatype: DataType,
    ignore_nulls: bool,
    /// Source arrays — input arrays (from update_batch) or list backing
    /// arrays (from merge_batch).
    batches: Vec<ArrayRef>,
    /// Per-batch list of (group_idx, row_idx) pairs.
    batch_entries: Vec<Vec<(u32, u32)>>,
    /// Total number of groups tracked.
    num_groups: usize,
}

impl ArrayAggGroupsAccumulator {
    fn new(datatype: DataType, ignore_nulls: bool) -> Self {
        Self {
            datatype,
            ignore_nulls,
            batches: Vec::new(),
            batch_entries: Vec::new(),
            num_groups: 0,
        }
    }

    fn clear_state(&mut self) {
        // `size()` measures Vec capacity rather than len, so allocate new
        // buffers instead of using `clear()`.
        self.batches = Vec::new();
        self.batch_entries = Vec::new();
        self.num_groups = 0;
    }

    fn compact_retained_state(&mut self, emit_groups: usize) -> Result<()> {
        // EmitTo::First is used to recover from memory pressure. Simply
        // removing emitted entries in place is not enough because mixed batches
        // would continue to pin their original Array arrays, even if only a few
        // retained rows remain.
        //
        // Rebuild the retained state from scratch so fully emitted batches are
        // dropped, mixed batches are compacted to arrays containing only the
        // surviving rows, and retained metadata is right-sized.
        let emit_groups = emit_groups as u32;
        let old_batches = take(&mut self.batches);
        let old_batch_entries = take(&mut self.batch_entries);

        let mut batches = Vec::new();
        let mut batch_entries = Vec::new();

        for (batch, entries) in old_batches.into_iter().zip(old_batch_entries) {
            let retained_len = entries.iter().filter(|(g, _)| *g >= emit_groups).count();

            if retained_len == 0 {
                continue;
            }

            if retained_len == entries.len() {
                // Nothing was emitted from this batch, so we keep the existing
                // array and only renumber the remaining group IDs so that they
                // start from 0.
                let mut retained_entries = entries;
                for (g, _) in &mut retained_entries {
                    *g -= emit_groups;
                }
                retained_entries.shrink_to_fit();
                batches.push(batch);
                batch_entries.push(retained_entries);
                continue;
            }

            let mut retained_entries = Vec::with_capacity(retained_len);
            let mut retained_rows = Vec::with_capacity(retained_len);

            for (g, r) in entries {
                if g >= emit_groups {
                    // Compute the new `(group_idx, row_idx)` pair for a
                    // retained row. `group_idx` is renumbered to start from
                    // 0, and `row_idx` points into the new dense batch we are
                    // building.
                    retained_entries.push((g - emit_groups, retained_rows.len() as u32));
                    retained_rows.push(r);
                }
            }

            debug_assert_eq!(retained_entries.len(), retained_len);
            debug_assert_eq!(retained_rows.len(), retained_len);

            let batch = if retained_len == batch.len() {
                batch
            } else {
                // Compact mixed batches so retained rows no longer pin the
                // original array.
                let retained_rows = UInt32Array::from(retained_rows);
                arrow::compute::take(batch.as_ref(), &retained_rows, None)?
            };

            batches.push(batch);
            batch_entries.push(retained_entries);
        }

        self.batches = batches;
        self.batch_entries = batch_entries;
        self.num_groups -= emit_groups as usize;

        Ok(())
    }
}

impl GroupsAccumulator for ArrayAggGroupsAccumulator {
    /// Store a reference to the input batch, plus a `(group_idx, row_idx)` pair
    /// for every row.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let input = &values[0];

        self.num_groups = self.num_groups.max(total_num_groups);

        let nulls = if self.ignore_nulls {
            input.logical_nulls()
        } else {
            None
        };

        let mut entries = Vec::new();

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            // Skip filtered rows
            if let Some(filter) = opt_filter
                && (filter.is_null(row_idx) || !filter.value(row_idx))
            {
                continue;
            }

            // Skip null values when ignore_nulls is set
            if let Some(ref nulls) = nulls
                && nulls.is_null(row_idx)
            {
                continue;
            }

            entries.push((group_idx as u32, row_idx as u32));
        }

        // We only need to record the batch if it was non-empty.
        if !entries.is_empty() {
            self.batches.push(Arc::clone(input));
            self.batch_entries.push(entries);
        }

        Ok(())
    }

    /// Produce a `ListArray` ordered by group index: the list at
    /// position N contains the aggregated values for group N.
    ///
    /// Uses a counting sort to rearrange the stored `(group, row)`
    /// entries into group order, then calls `interleave` to gather
    /// the values into a flat array that backs the output `ListArray`.
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emit_groups = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };

        // Step 1: Count entries per group. For EmitTo::First(n), only groups
        // 0..n are counted; the rest are retained to be emitted in the future.
        let mut counts = vec![0u32; emit_groups];
        for entries in &self.batch_entries {
            for &(g, _) in entries {
                let g = g as usize;
                if g < emit_groups {
                    counts[g] += 1;
                }
            }
        }

        // Step 2: Do a prefix sum over the counts and use it to build ListArray
        // offsets, null buffer, and write positions for the counting sort.
        let mut offsets = Vec::<i32>::with_capacity(emit_groups + 1);
        offsets.push(0);
        let mut nulls_builder = NullBufferBuilder::new(emit_groups);
        let mut write_positions = Vec::with_capacity(emit_groups);
        let mut cur_offset = 0u32;
        for &count in &counts {
            if count == 0 {
                nulls_builder.append_null();
            } else {
                nulls_builder.append_non_null();
            }
            write_positions.push(cur_offset);
            cur_offset += count;
            offsets.push(cur_offset as i32);
        }
        let total_rows = cur_offset as usize;

        // Step 3: Scatter entries into group order using the counting sort. The
        // batch index is implicit from the outer loop position.
        let flat_values = if total_rows == 0 {
            new_empty_array(&self.datatype)
        } else {
            let mut interleave_indices = vec![(0usize, 0usize); total_rows];
            for (batch_idx, entries) in self.batch_entries.iter().enumerate() {
                for &(g, r) in entries {
                    let g = g as usize;
                    if g < emit_groups {
                        let wp = write_positions[g] as usize;
                        interleave_indices[wp] = (batch_idx, r as usize);
                        write_positions[g] += 1;
                    }
                }
            }

            let sources: Vec<&dyn Array> =
                self.batches.iter().map(|b| b.as_ref()).collect();
            arrow::compute::interleave(&sources, &interleave_indices)?
        };

        // Step 4: Release state for emitted groups.
        match emit_to {
            EmitTo::All => self.clear_state(),
            EmitTo::First(_) => self.compact_retained_state(emit_groups)?,
        }

        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let field = Arc::new(Field::new_list_field(self.datatype.clone(), true));
        let result = ListArray::new(field, offsets, flat_values, nulls_builder.finish());

        Ok(Arc::new(result))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        let input_list = values[0].as_list::<i32>();

        self.num_groups = self.num_groups.max(total_num_groups);

        // Push the ListArray's backing values array as a single batch.
        let list_values = input_list.values();
        let list_offsets = input_list.offsets();

        let mut entries = Vec::new();

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if input_list.is_null(row_idx) {
                continue;
            }
            let start = list_offsets[row_idx] as u32;
            let end = list_offsets[row_idx + 1] as u32;
            for pos in start..end {
                entries.push((group_idx as u32, pos));
            }
        }

        if !entries.is_empty() {
            self.batches.push(Arc::clone(list_values));
            self.batch_entries.push(entries);
        }

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "one argument to convert_to_state");

        let input = &values[0];

        // Each row becomes a 1-element list: offsets are [0, 1, 2, ..., n].
        let offsets = OffsetBuffer::from_repeated_length(1, input.len());

        // Filtered rows become null list entries, which merge_batch will skip.
        let filter_nulls = opt_filter.map(filter_to_nulls);

        // With ignore_nulls, null values also become null list entries. Without
        // ignore_nulls, null values stay as [NULL] so merge_batch retains them.
        let nulls = if self.ignore_nulls {
            let logical = input.logical_nulls();
            NullBuffer::union(filter_nulls.as_ref(), logical.as_ref())
        } else {
            filter_nulls
        };

        let field = Arc::new(Field::new_list_field(self.datatype.clone(), true));
        let list_array = ListArray::new(field, offsets, Arc::clone(input), nulls);

        Ok(vec![Arc::new(list_array)])
    }
    fn size(&self) -> usize {
        self.batches
            .iter()
            .map(|arr| arr.to_data().get_slice_memory_size().unwrap_or_default())
            .sum::<usize>()
            + self.batches.capacity() * size_of::<ArrayRef>()
            + self
                .batch_entries
                .iter()
                .map(|e| e.capacity() * size_of::<(u32, u32)>())
                .sum::<usize>()
            + self.batch_entries.capacity() * size_of::<Vec<(u32, u32)>>()
    }
}

/// Resources that are allocated lazily on the first `update_batch` call,
/// once the concrete runtime Arrow type is known.
///
/// Grouping all three fields together makes the "either all present or all
/// absent" invariant explicit in the type system, replacing the scattered
/// `.expect()` calls that would otherwise be needed.
#[derive(Debug)]
struct DistinctState {
    /// Converts Arrow arrays to/from the comparable row format.
    converter: RowConverter,
    /// One owned encoded row per live distinct value, indexed by group index.
    /// Compacted via swap-remove on eviction so there are never dead slots.
    group_rows: Vec<OwnedRow>,
    /// Live refcount per group index. `counts[i]` is how many times the value
    /// at `group_rows[i]` is currently present in the window frame.
    counts: Vec<u64>,
    /// Hash of the encoded row at group index `i`, kept in sync with
    /// `group_rows` and `counts`. Needed to patch the map on swap-remove
    /// eviction without re-encoding the moved row.
    row_hashes: Vec<u64>,
    /// Temporary buffer for encoding an incoming batch; reused across calls.
    rows_buffer: Rows,
}

#[derive(Debug)]
pub struct DistinctArrayAggAccumulator {
    /// Lazily allocated on the first `update_batch`; `None` until then.
    state: Option<DistinctState>,
    /// Hash table storing `(hash, group_index)`. Only contains live entries
    /// (those whose count is > 0). Evicted on `retract_batch` when count
    /// drops to zero.
    map: HashTable<(u64, usize)>,
    /// Heap size of `map` in bytes, tracked for `size()` reporting.
    map_size: usize,
    /// Reused buffer for batch hashes.
    hashes_buffer: Vec<u64>,
    /// Random state used by `create_hashes`.
    random_state: RandomState,
    datatype: DataType,
    sort_options: Option<SortOptions>,
    ignore_nulls: bool,
}

/// Returns `true` if `dt` is, or recursively contains, a `Dictionary` type.
///
/// `RowConverter` always decodes to the physical (non-dictionary) type, so a
/// cast back to the declared logical type is required when this is true.
fn datatype_contains_dictionary(dt: &DataType) -> bool {
    match dt {
        DataType::Dictionary(_, _) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => datatype_contains_dictionary(f.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|f| datatype_contains_dictionary(f.data_type())),
        _ => false,
    }
}

impl DistinctArrayAggAccumulator {
    pub fn try_new(
        datatype: &DataType,
        sort_options: Option<SortOptions>,
        ignore_nulls: bool,
    ) -> Result<Self> {
        Ok(Self {
            state: None,
            map: HashTable::new(),
            map_size: 0,
            hashes_buffer: Vec::new(),
            random_state: RandomState::default(),
            datatype: datatype.clone(),
            sort_options,
            ignore_nulls,
        })
    }

    /// Lazily initialises the `DistinctState` on the first call, using the
    /// actual runtime column type.
    fn ensure_state(&mut self, data_type: &DataType) -> Result<()> {
        if self.state.is_none() {
            let sort_field = match self.sort_options {
                Some(opts) => SortField::new_with_options(data_type.clone(), opts),
                None => SortField::new(data_type.clone()),
            };
            let converter = RowConverter::new(vec![sort_field])?;
            let rows_buffer = converter.empty_rows(0, 0);
            self.state = Some(DistinctState {
                converter,
                group_rows: Vec::new(),
                counts: Vec::new(),
                row_hashes: Vec::new(),
                rows_buffer,
            });
        }
        Ok(())
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

        // Filter nulls out upfront when ignore_nulls is set so they are
        // never inserted into the dedup state.
        let filtered;
        let col: &ArrayRef = if self.ignore_nulls {
            if let Some(nulls) = val.logical_nulls() {
                if nulls.null_count() > 0 {
                    let mask: BooleanArray = nulls.iter().map(Some).collect();
                    filtered = filter(val.as_ref(), &mask)?;
                    &filtered
                } else {
                    val
                }
            } else {
                val
            }
        } else {
            val
        };

        if col.is_empty() {
            return Ok(());
        }

        self.ensure_state(col.data_type())?;

        // Encode the entire incoming batch into rows_buffer in one pass.
        let DistinctState {
            converter,
            group_rows,
            counts,
            row_hashes,
            rows_buffer,
        } = self.state.as_mut().unwrap();
        rows_buffer.clear();
        converter.append(rows_buffer, std::slice::from_ref(col))?;

        // Pre-compute all hashes for the batch in one SIMD-friendly pass.
        self.hashes_buffer.clear();
        self.hashes_buffer.resize(col.len(), 0);
        create_hashes(
            std::slice::from_ref(col),
            &self.random_state,
            &mut self.hashes_buffer,
        )?;

        for (row_idx, &hash) in self.hashes_buffer.iter().enumerate() {
            let row = rows_buffer.row(row_idx);
            let entry = self.map.find_mut(hash, |&(h, group_idx)| {
                h == hash && group_rows[group_idx].row() == row
            });
            match entry {
                Some((_, group_idx)) => {
                    // Already known: just increment the live refcount.
                    counts[*group_idx] += 1;
                }
                None => {
                    // New distinct value: own the encoded row, record it.
                    let new_group_idx = group_rows.len();
                    group_rows.push(row.owned());
                    counts.push(1);
                    row_hashes.push(hash);
                    self.map.insert_accounted(
                        (hash, new_group_idx),
                        |&(h, _)| h,
                        &mut self.map_size,
                    );
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

        // The DISTINCT state is `List<value>`.
        states[0]
            .as_list::<i32>()
            .iter()
            .flatten()
            .try_for_each(|val| self.update_batch(&[val]))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.map.is_empty() {
            return Ok(ScalarValue::new_null_list(self.datatype.clone(), true, 1));
        }

        let DistinctState {
            converter,
            group_rows,
            ..
        } = self
            .state
            .as_ref()
            .expect("state must be set when map is non-empty");

        // Collect the group indices of all live entries.
        let mut live_indices: Vec<usize> =
            self.map.iter().map(|&(_, group_idx)| group_idx).collect();

        // If ORDER BY was specified, the RowConverter bakes the sort direction
        // into the row bytes, so lexicographic sort gives the correct order.
        if self.sort_options.is_some() {
            live_indices
                .sort_unstable_by(|&a, &b| group_rows[a].row().cmp(&group_rows[b].row()));
        }

        // Decode the selected rows back into an Arrow array.
        let rows: Vec<Row<'_>> =
            live_indices.iter().map(|&i| group_rows[i].row()).collect();
        let arrays = converter.convert_rows(rows)?;

        // `convert_rows` always returns the physical (non-dictionary) type.
        // Cast back to the declared logical type when they differ AND the
        // declared type contains a Dictionary somewhere (directly or nested
        // inside a Struct, List, etc.) — that is the only case where
        // RowConverter strips the logical type.
        let decoded = if arrays[0].data_type() != &self.datatype
            && datatype_contains_dictionary(&self.datatype)
        {
            cast(arrays[0].as_ref(), &self.datatype)?
        } else {
            Arc::clone(&arrays[0])
        };

        let values: Vec<ScalarValue> = (0..decoded.len())
            .map(|i| ScalarValue::try_from_array(decoded.as_ref(), i))
            .collect::<Result<_>>()?;

        let arr = ScalarValue::new_list(&values, &self.datatype, true);
        Ok(ScalarValue::List(arr))
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(values.len(), 1, "expects single batch");

        let val = &values[0];

        // Mirror the null-filtering logic from update_batch so we only
        // retract values that were actually inserted.
        let filtered;
        let col: &ArrayRef = if self.ignore_nulls {
            if let Some(nulls) = val.logical_nulls() {
                if nulls.null_count() > 0 {
                    let mask: BooleanArray = nulls.iter().map(Some).collect();
                    filtered = filter(val.as_ref(), &mask)?;
                    &filtered
                } else {
                    val
                }
            } else {
                val
            }
        } else {
            val
        };

        if col.is_empty() {
            return Ok(());
        }

        let DistinctState {
            converter,
            group_rows,
            counts,
            row_hashes,
            rows_buffer,
        } = self
            .state
            .as_mut()
            .expect("retract_batch called before update_batch");

        rows_buffer.clear();
        converter.append(rows_buffer, std::slice::from_ref(col))?;

        self.hashes_buffer.clear();
        self.hashes_buffer.resize(col.len(), 0);
        create_hashes(
            std::slice::from_ref(col),
            &self.random_state,
            &mut self.hashes_buffer,
        )?;

        for (row_idx, &hash) in self.hashes_buffer.iter().enumerate() {
            let row = rows_buffer.row(row_idx);
            match self.map.find_entry(hash, |&(h, group_idx)| {
                h == hash && group_rows[group_idx].row() == row
            }) {
                Err(_) => {
                    return internal_err!(
                        "DistinctArrayAggAccumulator::retract_batch: \
                         value not present in state"
                    );
                }
                Ok(occupied) => {
                    let (_, dead_idx) = *occupied.get();
                    counts[dead_idx] -= 1;
                    if counts[dead_idx] == 0 {
                        occupied.remove();
                        // Compact via swap-remove: move the last slot into the
                        // dead slot so group_rows / counts / row_hashes stay
                        // dense with no dead entries.
                        let last_idx = group_rows.len() - 1;
                        if dead_idx != last_idx {
                            // Patch the map entry that points to last_idx so
                            // it points to dead_idx instead.
                            let last_hash = row_hashes[last_idx];
                            self.map
                                .find_mut(last_hash, |&(_, idx)| idx == last_idx)
                                .ok_or_else(|| {
                                    datafusion_common::internal_datafusion_err!(
                                        "DistinctArrayAggAccumulator: map is missing \
                                         group index {last_idx} during swap-remove \
                                         compaction"
                                    )
                                })?
                                .1 = dead_idx;
                        }
                        group_rows.swap_remove(dead_idx);
                        counts.swap_remove(dead_idx);
                        row_hashes.swap_remove(dead_idx);
                    }
                }
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .state
                .as_ref()
                .map(|s| {
                    s.group_rows
                        .iter()
                        .map(|r| r.row().data().len())
                        .sum::<usize>()
                        + s.group_rows.capacity() * size_of::<OwnedRow>()
                        + s.counts.capacity() * size_of::<u64>()
                        + s.row_hashes.capacity() * size_of::<u64>()
                        + s.rows_buffer.size()
                        + s.converter.size()
                })
                .unwrap_or(0)
            + self.map_size
            + self.hashes_buffer.capacity() * size_of::<u64>()
            + self.datatype.size()
            - size_of_val(&self.datatype)
    }
}

/// Accumulator for a `ARRAY_AGG(... ORDER BY ..., ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
#[derive(Debug, Clone, Copy)]
struct OrderedArrayAggEntry {
    batch_idx: usize,
    row_idx: usize,
}

#[derive(Debug)]
pub(crate) struct OrderSensitiveArrayAggAccumulator {
    /// Arrow payload arrays. Entries refer to rows in these batches.
    batches: Vec<ArrayRef>,
    /// One compact payload location per retained result element.
    entries: Vec<OrderedArrayAggEntry>,
    /// Ordering keys encoded with the complete sort options. Row `i` always
    /// corresponds to entry `i`.
    ordering_rows: Rows,
    /// Input ranges already known to be sorted, such as preordered raw input
    /// and individual partial-state rows.
    sorted_runs: Vec<Range<usize>>,
    /// Lazily computed sorted permutation shared by state/evaluate.
    sorted_entry_indices: Option<Vec<usize>>,
    ordering_converter: RowConverter,
    value_type: DataType,
    ordering_fields: Fields,
    /// Whether the input is known to be pre-ordered
    is_input_pre_ordered: bool,
    /// Whether the aggregation is running in reverse.
    reverse: bool,
    /// Whether the aggregation should ignore null values.
    ignore_nulls: bool,
    /// A partial state breaks continuity even when subsequent raw input is
    /// itself known to be preordered.
    can_extend_preordered_run: bool,
}

impl OrderSensitiveArrayAggAccumulator {
    /// Create a new order-sensitive ARRAY_AGG accumulator based on the given
    /// item data type.
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: &LexOrdering,
        is_input_pre_ordered: bool,
        reverse: bool,
        ignore_nulls: bool,
    ) -> Result<Self> {
        assert_eq_or_internal_err!(
            ordering_dtypes.len(),
            ordering_req.len(),
            "ordered array_agg requires one datatype per ordering expression"
        );
        let ordering_fields =
            Fields::from(ordering_fields(ordering_req, ordering_dtypes));
        let sort_fields = ordering_dtypes
            .iter()
            .zip(ordering_req.iter())
            .map(|(data_type, sort_expr)| {
                SortField::new_with_options(data_type.clone(), sort_expr.options)
            })
            .collect();
        let ordering_converter = RowConverter::new(sort_fields)?;
        let ordering_rows = ordering_converter.empty_rows(0, 0);
        Ok(Self {
            batches: vec![],
            entries: vec![],
            ordering_rows,
            sorted_runs: vec![],
            sorted_entry_indices: None,
            ordering_converter,
            value_type: datatype.clone(),
            ordering_fields,
            is_input_pre_ordered,
            reverse,
            ignore_nulls,
            can_extend_preordered_run: false,
        })
    }

    fn append_input_batch(
        &mut self,
        values: &ArrayRef,
        ordering_values: &[ArrayRef],
    ) -> Result<()> {
        let Some(entry_range) =
            self.store_batch(values, ordering_values, self.ignore_nulls)?
        else {
            return Ok(());
        };
        if self.is_input_pre_ordered {
            if self.can_extend_preordered_run {
                self.sorted_runs
                    .last_mut()
                    .expect("an extendable preordered run must exist")
                    .end = entry_range.end;
            } else {
                self.sorted_runs.push(entry_range);
            }
        }
        self.can_extend_preordered_run = self.is_input_pre_ordered;
        Ok(())
    }

    fn append_sorted_run(
        &mut self,
        values: &ArrayRef,
        ordering_values: &[ArrayRef],
    ) -> Result<()> {
        if let Some(entry_range) = self.store_batch(values, ordering_values, false)?
            && entry_range
                .clone()
                .zip(entry_range.start + 1..entry_range.end)
                .all(|(left, right)| self.ordering_row(left) <= self.ordering_row(right))
        {
            self.sorted_runs.push(entry_range);
        }
        self.can_extend_preordered_run = false;
        Ok(())
    }

    fn ordering_row(&self, entry_idx: usize) -> Row<'_> {
        self.ordering_rows.row(entry_idx)
    }

    fn merge_sorted_runs(&self, unsorted_indices: Vec<usize>) -> Vec<usize> {
        let unsorted_run = (!unsorted_indices.is_empty())
            .then(|| Either::Right(unsorted_indices.into_iter()));
        self.sorted_runs
            .iter()
            .cloned()
            .map(Either::Left)
            .chain(unsorted_run)
            .kmerge_by(|left, right| {
                self.ordering_row(*left)
                    .cmp(&self.ordering_row(*right))
                    .then_with(|| left.cmp(right))
                    .is_lt()
            })
            .collect()
    }

    fn ensure_sorted_indices(&mut self) {
        if self.sorted_entry_indices.is_some() {
            return;
        }

        let sorted_len = self.sorted_runs.iter().map(|run| run.len()).sum::<usize>();
        let mut unsorted_indices = Vec::with_capacity(self.entries.len() - sorted_len);
        let mut next_unsorted = 0;
        for run in &self.sorted_runs {
            debug_assert!(run.start >= next_unsorted);
            debug_assert!(run.end <= self.entries.len());
            unsorted_indices.extend(next_unsorted..run.start);
            next_unsorted = run.end;
        }
        unsorted_indices.extend(next_unsorted..self.entries.len());
        unsorted_indices.sort_by(|left, right| {
            self.ordering_row(*left)
                .cmp(&self.ordering_row(*right))
                .then_with(|| left.cmp(right))
        });
        self.sorted_entry_indices = Some(self.merge_sorted_runs(unsorted_indices));
    }

    fn select_values(&self, sorted_indices: &[usize], reverse: bool) -> Result<ArrayRef> {
        if sorted_indices.is_empty() {
            return Ok(new_empty_array(&self.value_type));
        }

        // A common preordered case is a consecutive range in one input batch.
        // Return a zero-copy slice instead of invoking interleave.
        if !reverse {
            let first = self.entries[sorted_indices[0]];
            let is_contiguous = sorted_indices.iter().enumerate().all(|(offset, idx)| {
                let entry = self.entries[*idx];
                entry.batch_idx == first.batch_idx
                    && entry.row_idx == first.row_idx + offset
            });
            if is_contiguous {
                return Ok(self.batches[first.batch_idx]
                    .slice(first.row_idx, sorted_indices.len()));
            }
        }

        let sources = self
            .batches
            .iter()
            .map(|batch| batch.as_ref())
            .collect::<Vec<_>>();
        let indices = if reverse {
            sorted_indices
                .iter()
                .rev()
                .map(|idx| {
                    let entry = self.entries[*idx];
                    (entry.batch_idx, entry.row_idx)
                })
                .collect::<Vec<_>>()
        } else {
            sorted_indices
                .iter()
                .map(|idx| {
                    let entry = self.entries[*idx];
                    (entry.batch_idx, entry.row_idx)
                })
                .collect::<Vec<_>>()
        };
        Ok(arrow::compute::interleave(&sources, &indices)?)
    }

    fn evaluate_orderings(
        &self,
        sorted_indices: &[usize],
        reverse: bool,
    ) -> Result<ScalarValue> {
        let indices = if reverse {
            Either::Left(sorted_indices.iter().rev())
        } else {
            Either::Right(sorted_indices.iter())
        };
        let mut columns = self
            .ordering_converter
            .convert_rows(indices.map(|idx| self.ordering_row(*idx)))?;

        // RowConverter decodes dictionary values to their physical type. State
        // fields, however, are required to retain their declared logical type.
        for (column, field) in columns.iter_mut().zip(&self.ordering_fields) {
            if column.data_type() != field.data_type() {
                *column = cast(column.as_ref(), field.data_type())?;
            }
        }

        let ordering_array =
            StructArray::try_new(self.ordering_fields.clone(), columns, None)?;
        Ok(SingleRowListArrayBuilder::new(Arc::new(ordering_array)).build_list_scalar())
    }

    fn store_batch(
        &mut self,
        values: &ArrayRef,
        ordering_values: &[ArrayRef],
        ignore_nulls: bool,
    ) -> Result<Option<Range<usize>>> {
        let values = if values.data_type() == &self.value_type {
            Arc::clone(values)
        } else if self.value_type.contains(values.data_type()) {
            cast(values.as_ref(), &self.value_type)?
        } else {
            return exec_err!(
                "ordered array_agg payload has type {}, expected {}",
                values.data_type(),
                self.value_type
            );
        };
        if let Some(column) = ordering_values.first() {
            assert_eq_or_internal_err!(
                column.len(),
                values.len(),
                "ordered array_agg payload and ordering columns must have equal lengths"
            );
        }

        let nulls = ignore_nulls
            .then(|| values.logical_nulls())
            .flatten()
            .filter(|nulls| nulls.null_count() > 0);
        let (values, filtered_ordering_values) = if let Some(nulls) = nulls {
            let mask: BooleanArray = nulls.iter().map(Some).collect();
            let values = filter(values.as_ref(), &mask)?;
            let ordering_values = ordering_values
                .iter()
                .map(|column| filter(column.as_ref(), &mask))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            (values, Some(ordering_values))
        } else {
            (values, None)
        };
        let ordering_values = filtered_ordering_values
            .as_deref()
            .unwrap_or(ordering_values);
        // Detach the stored payload from potentially oversized backing buffers.
        let values = make_array(copy_array_data(&values.to_data()));

        let row_count = values.len();
        // RowConverter validates the number, lengths, and types of ordering columns.
        self.ordering_converter
            .append(&mut self.ordering_rows, ordering_values)?;
        if row_count == 0 {
            return Ok(None);
        }

        let start = self.entries.len();
        let batch_idx = self.batches.len();
        self.batches.push(values);
        self.entries.extend(
            (0..row_count).map(|row_idx| OrderedArrayAggEntry { batch_idx, row_idx }),
        );
        self.sorted_entry_indices = None;

        debug_assert_eq!(self.entries.len(), self.ordering_rows.num_rows());
        Ok(Some(start..self.entries.len()))
    }
}

impl Accumulator for OrderSensitiveArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        self.append_input_batch(&values[0], &values[1..])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let [array_agg_values, agg_orderings] = states else {
            return exec_err!("ordered array_agg expects two state arrays");
        };
        let array_agg_values = as_list_array(array_agg_values.as_ref())?;
        let agg_orderings = as_list_array(agg_orderings.as_ref())?;
        assert_eq_or_internal_err!(
            array_agg_values.len(),
            agg_orderings.len(),
            "ordered array_agg payload and ordering states must have equal outer lengths"
        );
        let ordering_struct = agg_orderings
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "ordered array_agg ordering state must contain Struct values"
                )
            })?;
        for row_idx in 0..array_agg_values.len() {
            let payload_len = if array_agg_values.is_null(row_idx) {
                0
            } else {
                array_agg_values.value_length(row_idx) as usize
            };
            let ordering_len = if agg_orderings.is_null(row_idx) {
                0
            } else {
                agg_orderings.value_length(row_idx) as usize
            };
            if payload_len != ordering_len {
                return exec_err!(
                    "ordered array_agg payload and ordering state lengths differ at row {row_idx}: {payload_len} vs {ordering_len}"
                );
            }
            let payload = array_agg_values.value(row_idx);
            let ordering_start = agg_orderings.value_offsets()[row_idx] as usize;
            let len = payload.len();
            let ordering_columns = ordering_struct
                .columns()
                .iter()
                .map(|column| column.slice(ordering_start, len))
                .collect::<Vec<_>>();
            self.append_sorted_run(&payload, &ordering_columns)?;
        }
        self.can_extend_preordered_run = false;

        debug_assert_eq!(self.entries.len(), self.ordering_rows.num_rows());
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.ensure_sorted_indices();
        let sorted_indices = self.sorted_entry_indices.as_deref().unwrap();
        let payload = if sorted_indices.is_empty() {
            ScalarValue::new_null_list(self.value_type.clone(), true, 1)
        } else {
            SingleRowListArrayBuilder::new(
                self.select_values(sorted_indices, self.reverse)?,
            )
            .build_list_scalar()
        };
        let orderings = self.evaluate_orderings(sorted_indices, self.reverse)?;
        Ok(vec![payload, orderings])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.ensure_sorted_indices();
        let sorted_indices = self.sorted_entry_indices.as_deref().unwrap();
        if sorted_indices.is_empty() {
            return Ok(ScalarValue::new_null_list(self.value_type.clone(), true, 1));
        }

        Ok(SingleRowListArrayBuilder::new(
            self.select_values(sorted_indices, self.reverse)?,
        )
        .build_list_scalar())
    }

    fn size(&self) -> usize {
        let mut total = size_of_val(self)
            + self
                .batches
                .iter()
                .map(|batch| batch.get_array_memory_size())
                .sum::<usize>()
            + self.batches.capacity() * size_of::<ArrayRef>()
            + self.entries.capacity() * size_of::<OrderedArrayAggEntry>()
            + self.ordering_rows.size()
            - size_of_val(&self.ordering_rows)
            + self.ordering_converter.size()
            - size_of_val(&self.ordering_converter)
            + self.sorted_runs.capacity() * size_of::<Range<usize>>()
            + self
                .sorted_entry_indices
                .as_ref()
                .map(|indices| indices.capacity() * size_of::<usize>())
                .unwrap_or_default();

        total += self.value_type.size() - size_of_val(&self.value_type);
        total + self.ordering_fields.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListBuilder, StringBuilder};
    use arrow::datatypes::Schema;
    use datafusion_common::cast::as_generic_string_array;
    use datafusion_common::internal_err;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

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

        assert_eq!(acc1.size(), 174);

        Ok(())
    }
    #[test]
    fn does_not_over_account_memory_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = ArrayAggAccumulatorBuilder::new(DataType::List(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
        ))
        .distinct()
        .build_two()?;

        acc1.update_batch(&[string_list_data([
            vec!["a", "b", "c"],
            vec!["d", "e", "f"],
        ])])?;
        acc2.update_batch(&[string_list_data([vec!["e", "f", "g"]])])?;
        acc1 = merge(acc1, acc2)?;

        assert_eq!(acc1.size(), 2274);

        Ok(())
    }

    #[test]
    fn does_not_over_account_memory_ordered() -> Result<()> {
        let mut acc = ArrayAggAccumulatorBuilder::new(DataType::List(Arc::new(
            Field::new_list_field(DataType::Utf8, true),
        )))
        .order_by_col("col", SortOptions::new(false, false))
        .build()?;

        let input = string_list_data([
            vec!["a", "b", "c"],
            vec!["c", "d", "e"],
            vec!["b", "c", "d"],
        ]);
        acc.update_batch(&[Arc::clone(&input), input])?;

        assert_eq!(acc.size(), 2295);

        Ok(())
    }

    #[test]
    fn ordered_aggregate_nested_nullability_mismatch_issue_24022() -> Result<()> {
        use arrow::array::{Int32Array, Int64Array, StructArray};
        use datafusion_physical_expr::expressions::Column;

        let requested_element_type =
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int32, true)]));
        let inferred_field = Field::new("n", DataType::Int32, false);

        let ordering_dtype = DataType::Int64;
        let schema = Schema::new(vec![
            Field::new("val", requested_element_type.clone(), true),
            Field::new("ord", DataType::Int64, true),
        ]);
        let ord_expr = Arc::new(
            Column::new_with_schema("ord", &schema).expect("column not in schema"),
        ) as Arc<dyn PhysicalExpr>;

        let asc_opts = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let asc_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::clone(&ord_expr),
            asc_opts,
        )])
        .unwrap();

        let mut acc = OrderSensitiveArrayAggAccumulator::try_new(
            &requested_element_type,
            std::slice::from_ref(&ordering_dtype),
            &asc_ordering,
            /*is_input_pre_ordered=*/ true,
            /*reverse=*/ false,
            /*ignore_nulls=*/ false,
        )?;

        let value_arr = Arc::new(StructArray::from(vec![(
            Arc::new(inferred_field),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])) as ArrayRef;

        let ord_arr = Arc::new(Int64Array::from(vec![0i64])) as ArrayRef;

        acc.update_batch(&[value_arr, ord_arr])?;

        let evaluated = acc.evaluate()?;

        if let ScalarValue::List(arr) = evaluated {
            assert_eq!(
                arr.data_type(),
                &DataType::List(Arc::new(Field::new_list_field(
                    requested_element_type.clone(),
                    true
                )))
            );

            let expected_struct_array = StructArray::from(vec![(
                Arc::new(Field::new("n", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            )]);
            let expected_array = Arc::new(expected_struct_array) as ArrayRef;
            assert_eq!(&arr.value(0), &expected_array);
        } else {
            panic!("Expected ScalarValue::List");
        }

        Ok(())
    }

    #[test]
    fn distinct_aggregate_nested_nullability_mismatch_issue_24022() -> Result<()> {
        use arrow::array::{Int32Array, StructArray};
        use datafusion_common::ScalarValue;

        let requested_element_type =
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int32, true)]));
        let inferred_field = Field::new("n", DataType::Int32, false);

        let mut acc = DistinctArrayAggAccumulator::try_new(
            &requested_element_type,
            None,
            /*ignore_nulls=*/ false,
        )?;

        let value_arr = Arc::new(StructArray::from(vec![(
            Arc::new(inferred_field),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])) as ArrayRef;

        acc.update_batch(&[value_arr])?;

        let evaluated = acc.evaluate()?;

        if let ScalarValue::List(arr) = evaluated {
            assert_eq!(
                arr.data_type(),
                &DataType::List(Arc::new(Field::new_list_field(
                    requested_element_type.clone(),
                    true
                )))
            );

            let expected_struct_array = StructArray::from(vec![(
                Arc::new(Field::new("n", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            )]);
            let expected_array = Arc::new(expected_struct_array) as ArrayRef;
            assert_eq!(&arr.value(0), &expected_array);
        } else {
            panic!("Expected ScalarValue::List");
        }

        Ok(())
    }

    // Reproduces the bug where `state()` emits reversed values but non-reversed
    // orderings when the optimizer sets is_input_pre_ordered=true + reverse=true
    // (DESC aggregate with ASC pre-sorted input). The partial states are fed into
    // a final accumulator via merge_batch; without the fix the ordering keys and
    // values are mismatched so the final sort produces wrong order.
    #[test]
    fn desc_order_partial_final_merge_correct() -> Result<()> {
        use arrow::array::Int64Array;

        // The optimizer can feed ASC preordered input into reversed partial
        // accumulators even though the final accumulator requires DESC.
        let mut partial_states = vec![];
        for values in [vec![0, 1, 2], vec![3, 4, 5]] {
            let mut partial = ordered_accumulator(
                DataType::Int64,
                DataType::Int64,
                SortOptions::new(false, false),
                true,
                true,
            )?;
            let values = Arc::new(Int64Array::from(values)) as ArrayRef;
            partial.update_batch(&[Arc::clone(&values), values])?;
            partial_states.push(accumulator_state(&mut partial)?);
        }

        let mut final_acc = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(true, false),
            false,
            false,
        )?;
        for state in partial_states {
            final_acc.merge_batch(&state)?;
        }

        let ScalarValue::List(result) = final_acc.evaluate()? else {
            return internal_err!("expected List");
        };
        assert_eq!(
            result
                .values()
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[5, 4, 3, 2, 1, 0]
        );
        Ok(())
    }

    fn ordered_accumulator(
        payload_type: DataType,
        ordering_type: DataType,
        sort_options: SortOptions,
        is_input_pre_ordered: bool,
        reverse: bool,
    ) -> Result<OrderSensitiveArrayAggAccumulator> {
        let schema = Schema::new(vec![
            Field::new("val", payload_type.clone(), true),
            Field::new("ord", ordering_type.clone(), true),
        ]);
        let ord_expr = Arc::new(
            Column::new_with_schema("ord", &schema).expect("column not in schema"),
        ) as Arc<dyn PhysicalExpr>;
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new(ord_expr, sort_options)])
                .unwrap();

        OrderSensitiveArrayAggAccumulator::try_new(
            &payload_type,
            &[ordering_type],
            &ordering,
            is_input_pre_ordered,
            reverse,
            false,
        )
    }

    fn accumulator_state(
        acc: &mut OrderSensitiveArrayAggAccumulator,
    ) -> Result<Vec<ArrayRef>> {
        acc.state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()
    }

    #[test]
    fn input_batches_register_expected_runs() -> Result<()> {
        use arrow::array::Int64Array;

        for (is_input_pre_ordered, batches, expected_run) in [
            (false, [vec![4, 2], vec![3, 1]], None),
            (true, [vec![1, 2], vec![3, 4]], Some(0..4)),
        ] {
            let mut acc = ordered_accumulator(
                DataType::Int64,
                DataType::Int64,
                SortOptions::new(false, false),
                is_input_pre_ordered,
                false,
            )?;
            for values in batches {
                acc.update_batch(&[
                    Arc::new(Int64Array::from(values.clone())),
                    Arc::new(Int64Array::from(values)),
                ])?;
            }

            assert_eq!(
                acc.sorted_runs,
                expected_run.into_iter().collect::<Vec<_>>()
            );
            let ScalarValue::List(result) = acc.evaluate()? else {
                return internal_err!("expected List");
            };
            assert_eq!(
                result
                    .values()
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values(),
                &[1, 2, 3, 4]
            );
        }
        Ok(())
    }

    #[test]
    fn partial_state_breaks_preordered_run_continuity() -> Result<()> {
        use arrow::array::Int64Array;

        let mut partial = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(true, false),
            true,
            true,
        )?;
        partial.update_batch(&[
            Arc::new(Int64Array::from(vec![3, 2])),
            Arc::new(Int64Array::from(vec![3, 2])),
        ])?;
        let state = accumulator_state(&mut partial)?;

        let mut final_acc = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(false, false),
            true,
            false,
        )?;
        final_acc.update_batch(&[
            Arc::new(Int64Array::from(vec![1, 4])),
            Arc::new(Int64Array::from(vec![1, 4])),
        ])?;
        final_acc.merge_batch(&state)?;
        final_acc.update_batch(&[
            Arc::new(Int64Array::from(vec![0, 5])),
            Arc::new(Int64Array::from(vec![0, 5])),
        ])?;

        assert_eq!(final_acc.sorted_runs, vec![0..2, 2..4, 4..6]);
        let ScalarValue::List(result) = final_acc.evaluate()? else {
            return internal_err!("expected List");
        };
        assert_eq!(
            result
                .values()
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[0, 1, 2, 3, 4, 5]
        );
        Ok(())
    }

    #[test]
    fn equal_ordering_keys_preserve_partial_append_order() -> Result<()> {
        use arrow::array::{Int64Array, StringArray};

        let options = SortOptions::new(false, false);
        let mut partial_a =
            ordered_accumulator(DataType::Utf8, DataType::Int64, options, false, false)?;
        partial_a.update_batch(&[
            Arc::new(StringArray::from(vec!["a1", "a2"])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])?;
        let state_a = accumulator_state(&mut partial_a)?;

        let mut partial_b =
            ordered_accumulator(DataType::Utf8, DataType::Int64, options, false, false)?;
        partial_b.update_batch(&[
            Arc::new(StringArray::from(vec!["b1", "b2"])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])?;
        let state_b = accumulator_state(&mut partial_b)?;

        let mut final_acc =
            ordered_accumulator(DataType::Utf8, DataType::Int64, options, false, false)?;
        final_acc.merge_batch(&state_a)?;
        final_acc.merge_batch(&state_b)?;

        let result = print_nulls(str_arr(final_acc.evaluate()?)?);
        assert_eq!(result, vec!["a1", "a2", "b1", "b2"]);
        Ok(())
    }

    #[test]
    fn ordered_state_lifecycle_applies_reverse_and_is_repeatable() -> Result<()> {
        use arrow::array::Int64Array;

        let mut acc = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(false, false),
            true,
            true,
        )?;

        let empty_state = accumulator_state(&mut acc)?;
        let empty_payload = empty_state[0].as_list::<i32>();
        let empty_orderings = empty_state[1].as_list::<i32>();
        assert!(empty_payload.is_null(0));
        assert_eq!(empty_payload.values().len(), 0);
        assert_eq!(empty_orderings.value_length(0), 0);
        assert_eq!(empty_orderings.values().len(), 0);

        let mut empty_final = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        empty_final.merge_batch(&empty_state)?;
        let ScalarValue::List(empty_result) = empty_final.evaluate()? else {
            return internal_err!("expected List");
        };
        assert!(empty_result.is_null(0));
        assert_eq!(empty_result.values().len(), 0);

        acc.update_batch(&[
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(Int64Array::from(vec![0, 1, 2])),
        ])?;

        let state = accumulator_state(&mut acc)?;
        let payload_state = state[0].as_list::<i32>().value(0);
        assert_eq!(
            payload_state
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[2, 1, 0]
        );
        let ordering_state = state[1].as_list::<i32>().value(0);
        let ordering_state = ordering_state.as_struct();
        assert_eq!(
            ordering_state
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[2, 1, 0]
        );

        let first = acc.evaluate()?;
        let second = acc.evaluate()?;
        assert_eq!(first, second);
        let ScalarValue::List(result) = first else {
            return internal_err!("expected List");
        };
        assert_eq!(
            result
                .values()
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[2, 1, 0]
        );
        Ok(())
    }

    #[test]
    fn merge_batch_treats_each_partial_row_as_a_sorted_run() -> Result<()> {
        use arrow::array::Int64Array;

        let options = SortOptions::new(false, false);
        let mut partial_a =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, true, false)?;
        partial_a.update_batch(&[
            Arc::new(Int64Array::from(vec![1, 3, 5])),
            Arc::new(Int64Array::from(vec![1, 3, 5])),
        ])?;
        let state_a = accumulator_state(&mut partial_a)?;

        let mut partial_b =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, true, false)?;
        partial_b.update_batch(&[
            Arc::new(Int64Array::from(vec![2, 4, 6])),
            Arc::new(Int64Array::from(vec![2, 4, 6])),
        ])?;
        let state_b = accumulator_state(&mut partial_b)?;

        let payload_states =
            arrow::compute::concat(&[state_a[0].as_ref(), state_b[0].as_ref()])?;
        let ordering_states =
            arrow::compute::concat(&[state_a[1].as_ref(), state_b[1].as_ref()])?;

        let mut final_acc =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, false, false)?;
        final_acc.merge_batch(&[payload_states, ordering_states])?;

        let ScalarValue::List(result) = final_acc.evaluate()? else {
            return internal_err!("expected List");
        };
        assert_eq!(
            result
                .values()
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[1, 2, 3, 4, 5, 6]
        );
        Ok(())
    }

    #[test]
    fn ordering_state_preserves_declared_types() -> Result<()> {
        use arrow::array::{
            DictionaryArray, Int32Array, Int64Array, PrimitiveArray, RunArray,
            StringArray,
        };
        use arrow::datatypes::Int32Type;

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::new(
            Int32Array::from(vec![1, 0]),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ));

        let nested: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Arc::new(Field::new(
                "dict",
                dictionary_type.clone(),
                true,
            ))]),
            vec![Arc::new(DictionaryArray::new(
                Int32Array::from(vec![1, 0]),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ))],
            None,
        ));

        let run_ends = PrimitiveArray::<Int32Type>::from(vec![1, 3]);
        let run_values = Int64Array::from(vec![2, 1]);
        let run_end_encoded: ArrayRef =
            Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &run_values)?);

        for (name, ordering, payload, expected) in [
            ("dictionary", dictionary, vec![20, 10], vec![10, 20]),
            ("nested dictionary", nested, vec![20, 10], vec![10, 20]),
            (
                "run-end encoded",
                run_end_encoded,
                vec![20, 10, 11],
                vec![10, 11, 20],
            ),
        ] {
            let ordering_type = ordering.data_type().clone();
            let mut partial = ordered_accumulator(
                DataType::Int64,
                ordering_type.clone(),
                SortOptions::new(false, false),
                false,
                false,
            )?;
            partial.update_batch(&[Arc::new(Int64Array::from(payload)), ordering])?;

            let state = accumulator_state(&mut partial)?;
            let ordering_state = state[1].as_list::<i32>().value(0);
            assert_eq!(
                ordering_state.as_struct().column(0).data_type(),
                &ordering_type,
                "{name}"
            );

            let mut final_acc = ordered_accumulator(
                DataType::Int64,
                ordering_type,
                SortOptions::new(false, false),
                false,
                false,
            )?;
            final_acc.merge_batch(&state)?;
            let ScalarValue::List(result) = final_acc.evaluate()? else {
                return internal_err!("expected List");
            };
            assert_eq!(
                result
                    .values()
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values(),
                expected.as_slice(),
                "{name}"
            );
        }
        Ok(())
    }

    #[test]
    fn payload_types_roundtrip() -> Result<()> {
        use arrow::array::{DictionaryArray, Int32Array, Int64Array, StringArray};

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut partial_a = ordered_accumulator(
            dictionary_type.clone(),
            DataType::Int64,
            SortOptions::new(false, false),
            true,
            false,
        )?;
        partial_a.update_batch(&[
            Arc::new(DictionaryArray::new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec!["a", "c"])),
            )),
            Arc::new(Int64Array::from(vec![1, 3])),
        ])?;
        let state_a = accumulator_state(&mut partial_a)?;

        let mut partial_b = ordered_accumulator(
            dictionary_type.clone(),
            DataType::Int64,
            SortOptions::new(false, false),
            true,
            false,
        )?;
        partial_b.update_batch(&[
            Arc::new(DictionaryArray::new(
                Int32Array::from(vec![1, 0]),
                Arc::new(StringArray::from(vec!["d", "b"])),
            )),
            Arc::new(Int64Array::from(vec![2, 4])),
        ])?;
        let state_b = accumulator_state(&mut partial_b)?;

        let mut final_acc = ordered_accumulator(
            dictionary_type.clone(),
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        final_acc.merge_batch(&state_a)?;
        final_acc.merge_batch(&state_b)?;
        let ScalarValue::List(result) = final_acc.evaluate()? else {
            return internal_err!("expected List");
        };
        assert_eq!(result.values().data_type(), &dictionary_type);
        let strings = cast(result.values().as_ref(), &DataType::Utf8)?;
        assert_eq!(
            strings.as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), Some("c"), Some("d")]
        );

        let payload = string_list_data([vec!["b"], vec!["a"]]);
        let payload_type = payload.data_type().clone();
        let mut acc = ordered_accumulator(
            payload_type.clone(),
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        acc.update_batch(&[payload, Arc::new(Int64Array::from(vec![2, 1]))])?;

        let state = accumulator_state(&mut acc)?;
        let mut final_acc = ordered_accumulator(
            payload_type.clone(),
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        final_acc.merge_batch(&state)?;

        let ScalarValue::List(result) = final_acc.evaluate()? else {
            return internal_err!("expected List");
        };
        assert_eq!(result.values().data_type(), &payload_type);
        let nested = result.values().as_list::<i32>();
        assert_eq!(nested.value(0).as_string::<i32>().value(0), "a");
        assert_eq!(nested.value(1).as_string::<i32>().value(0), "b");
        Ok(())
    }

    #[test]
    fn ordered_ignore_nulls_keeps_payload_and_ordering_state_aligned() -> Result<()> {
        use arrow::array::{Int64Array, StringArray};

        let mut acc = ordered_accumulator(
            DataType::Utf8,
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        acc.ignore_nulls = true;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
            Arc::new(Int64Array::from(vec![2, 0, 1])),
        ])?;

        assert_eq!(acc.batches.len(), 1);
        assert_eq!(acc.batches[0].len(), 2);
        assert_eq!(
            acc.entries
                .iter()
                .map(|entry| entry.row_idx)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let state = accumulator_state(&mut acc)?;
        let payload_state = state[0].as_list::<i32>().value(0);
        assert_eq!(
            payload_state.as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        let ordering_state = state[1].as_list::<i32>().value(0);
        assert_eq!(
            ordering_state
                .as_struct()
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[1, 2]
        );
        Ok(())
    }

    #[test]
    fn merge_rejects_malformed_state() -> Result<()> {
        use arrow::array::{Int64Array, StringArray};

        let mut source = ordered_accumulator(
            DataType::Utf8,
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        source.update_batch(&[
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![1])),
        ])?;
        let state = accumulator_state(&mut source)?;

        let mut acc = ordered_accumulator(
            DataType::Int64,
            DataType::Int64,
            SortOptions::new(false, false),
            false,
            false,
        )?;
        let err = acc.merge_batch(&state).unwrap_err();
        assert!(err.to_string().contains("payload has type"));

        let options = SortOptions::new(false, false);
        let mut nonempty =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, false, false)?;
        nonempty.update_batch(&[
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
        ])?;
        let nonempty_state = accumulator_state(&mut nonempty)?;

        let mut empty =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, false, false)?;
        let empty_state = accumulator_state(&mut empty)?;

        let mut final_acc =
            ordered_accumulator(DataType::Int64, DataType::Int64, options, false, false)?;
        let payload_states = arrow::compute::concat(&[
            nonempty_state[0].as_ref(),
            nonempty_state[0].as_ref(),
        ])?;
        let ordering_states = arrow::compute::concat(&[
            nonempty_state[1].as_ref(),
            empty_state[1].as_ref(),
        ])?;
        let err = final_acc
            .merge_batch(&[payload_states, ordering_states])
            .unwrap_err();
        assert!(err.to_string().contains("state lengths differ"));
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
                return_field: Field::new(
                    "f",
                    DataType::List(Arc::new(Field::new_list_field(
                        data_type.clone(),
                        true,
                    ))),
                    true,
                )
                .into(),
                distinct: false,
                order_bys: vec![],
                schema: Schema {
                    fields: Fields::from(vec![Field::new("col", data_type, true)]),
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

    // ---- GroupsAccumulator tests ----

    use arrow::array::Int32Array;

    fn list_array_to_i32_vecs(list: &ListArray) -> Vec<Option<Vec<Option<i32>>>> {
        (0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    let arr = list.value(i);
                    let vals: Vec<Option<i32>> = arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect();
                    Some(vals)
                }
            })
            .collect()
    }

    fn eval_i32_lists(
        acc: &mut ArrayAggGroupsAccumulator,
        emit_to: EmitTo,
    ) -> Result<Vec<Option<Vec<Option<i32>>>>> {
        let result = acc.evaluate(emit_to)?;
        Ok(list_array_to_i32_vecs(result.as_list::<i32>()))
    }

    #[test]
    fn groups_accumulator_multiple_batches() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        // First batch
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        acc.update_batch(&[values], &[0, 1, 0], None, 2)?;

        // Second batch
        let values: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
        acc.update_batch(&[values], &[1, 0], None, 2)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(1), Some(3), Some(5)]));
        assert_eq!(vals[1], Some(vec![Some(2), Some(4)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_emit_first() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        acc.update_batch(&[values], &[0, 1, 2], None, 3)?;

        // Emit first 2 groups
        let vals = eval_i32_lists(&mut acc, EmitTo::First(2))?;
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], Some(vec![Some(10)]));
        assert_eq!(vals[1], Some(vec![Some(20)]));

        // Remaining group (was index 2, now shifted to 0)
        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0], Some(vec![Some(30)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_emit_first_frees_batches() -> Result<()> {
        // Batch 0 has rows only for group 0; batch 1 has rows for
        // both groups. After emitting group 0, batch 0 should be
        // dropped entirely and batch 1 should be compacted to the
        // retained row(s).
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let batch0: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        acc.update_batch(&[batch0], &[0, 0], None, 2)?;

        let batch1: ArrayRef = Arc::new(Int32Array::from(vec![30, 40]));
        acc.update_batch(&[batch1], &[0, 1], None, 2)?;

        assert_eq!(acc.batches.len(), 2);
        assert!(!acc.batches[0].is_empty());
        assert!(!acc.batches[1].is_empty());

        // Emit group 0. Batch 0 is only referenced by group 0, so it
        // should be removed. Batch 1 is mixed, so it should be compacted
        // to contain only the retained row for group 1.
        let vals = eval_i32_lists(&mut acc, EmitTo::First(1))?;
        assert_eq!(vals[0], Some(vec![Some(10), Some(20), Some(30)]));

        assert_eq!(acc.batches.len(), 1);
        let retained = acc.batches[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(retained.values(), &[40]);
        assert_eq!(acc.batch_entries, vec![vec![(0, 0)]]);

        // Emit remaining group 1
        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(40)]));

        assert!(acc.batches.is_empty());
        assert_eq!(acc.size(), 0);

        Ok(())
    }

    #[test]
    fn groups_accumulator_emit_first_compacts_mixed_batches() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let batch: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
        acc.update_batch(&[batch], &[0, 1, 0, 1], None, 2)?;

        let size_before = acc.size();
        let vals = eval_i32_lists(&mut acc, EmitTo::First(1))?;
        assert_eq!(vals[0], Some(vec![Some(10), Some(30)]));

        assert_eq!(acc.num_groups, 1);
        assert_eq!(acc.batches.len(), 1);
        let retained = acc.batches[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(retained.values(), &[20, 40]);
        assert_eq!(acc.batch_entries, vec![vec![(0, 0), (0, 1)]]);
        assert!(acc.size() < size_before);

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(20), Some(40)]));
        assert_eq!(acc.size(), 0);

        Ok(())
    }

    #[test]
    fn groups_accumulator_emit_all_releases_capacity() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let batch: ArrayRef = Arc::new(Int32Array::from_iter_values(0..64));
        acc.update_batch(
            &[batch],
            &(0..64).map(|i| i % 4).collect::<Vec<_>>(),
            None,
            4,
        )?;

        assert!(acc.size() > 0);
        let _ = eval_i32_lists(&mut acc, EmitTo::All)?;

        assert_eq!(acc.size(), 0);
        assert_eq!(acc.batches.capacity(), 0);
        assert_eq!(acc.batch_entries.capacity(), 0);

        Ok(())
    }

    #[test]
    fn groups_accumulator_null_groups() -> Result<()> {
        // Groups that never receive values should produce null
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        // Only group 0 gets a value, groups 1 and 2 are empty
        acc.update_batch(&[values], &[0], None, 3)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals, vec![Some(vec![Some(1)]), None, None]);

        Ok(())
    }

    #[test]
    fn groups_accumulator_ignore_nulls() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, true);

        let values: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));
        acc.update_batch(&[values], &[0, 0, 1, 1], None, 2)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        // Group 0: only non-null value is 1
        assert_eq!(vals[0], Some(vec![Some(1)]));
        // Group 1: only non-null value is 3
        assert_eq!(vals[1], Some(vec![Some(3)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_opt_filter() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        // Use a mix of false and null to filter out rows — both should
        // be skipped.
        let filter = BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]);
        acc.update_batch(&[values], &[0, 0, 1, 1], Some(&filter), 2)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(1)])); // row 1 filtered (null)
        assert_eq!(vals[1], Some(vec![Some(3)])); // row 3 filtered (false)

        Ok(())
    }

    #[test]
    fn groups_accumulator_state_merge_roundtrip() -> Result<()> {
        // Accumulator 1: update_batch, then merge, then update_batch again.
        // Verifies that values appear in chronological insertion order.
        let mut acc1 = ArrayAggGroupsAccumulator::new(DataType::Int32, false);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        acc1.update_batch(&[values], &[0, 1], None, 2)?;

        // Accumulator 2
        let mut acc2 = ArrayAggGroupsAccumulator::new(DataType::Int32, false);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![3, 4]));
        acc2.update_batch(&[values], &[0, 1], None, 2)?;

        // Merge acc2's state into acc1
        let state = acc2.state(EmitTo::All)?;
        acc1.merge_batch(&state, &[0, 1], 2)?;

        // Another update_batch on acc1 after the merge
        let values: ArrayRef = Arc::new(Int32Array::from(vec![5, 6]));
        acc1.update_batch(&[values], &[0, 1], None, 2)?;

        // Each group's values in insertion order:
        // group 0: update(1), merge(3), update(5) → [1, 3, 5]
        // group 1: update(2), merge(4), update(6) → [2, 4, 6]
        let vals = eval_i32_lists(&mut acc1, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(1), Some(3), Some(5)]));
        assert_eq!(vals[1], Some(vec![Some(2), Some(4), Some(6)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_convert_to_state() -> Result<()> {
        let acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let state = acc.convert_to_state(&[values], None)?;

        assert_eq!(state.len(), 1);
        let vals = list_array_to_i32_vecs(state[0].as_list::<i32>());
        assert_eq!(
            vals,
            vec![
                Some(vec![Some(10)]),
                Some(vec![None]), // null preserved inside list, not promoted
                Some(vec![Some(30)]),
            ]
        );

        Ok(())
    }

    #[test]
    fn groups_accumulator_convert_to_state_with_filter() -> Result<()> {
        let acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let filter = BooleanArray::from(vec![true, false, true]);
        let state = acc.convert_to_state(&[values], Some(&filter))?;

        let vals = list_array_to_i32_vecs(state[0].as_list::<i32>());
        assert_eq!(
            vals,
            vec![
                Some(vec![Some(10)]),
                None, // filtered
                Some(vec![Some(30)]),
            ]
        );

        Ok(())
    }

    #[test]
    fn groups_accumulator_convert_to_state_merge_preserves_nulls() -> Result<()> {
        // Verifies that null values survive the convert_to_state -> merge_batch
        // round-trip when ignore_nulls is false (default null handling).
        let acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let state = acc.convert_to_state(&[values], None)?;

        // Feed state into a new accumulator via merge_batch
        let mut acc2 = ArrayAggGroupsAccumulator::new(DataType::Int32, false);
        acc2.merge_batch(&state, &[0, 0, 1], 2)?;

        // Group 0 received rows 0 ([1]) and 1 ([NULL]) → [1, NULL]
        let vals = eval_i32_lists(&mut acc2, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(1), None]));
        // Group 1 received row 2 ([3]) → [3]
        assert_eq!(vals[1], Some(vec![Some(3)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_convert_to_state_merge_ignore_nulls() -> Result<()> {
        // Verifies that null values are dropped in the convert_to_state ->
        // merge_batch round-trip when ignore_nulls is true.
        let acc = ArrayAggGroupsAccumulator::new(DataType::Int32, true);

        let values: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));
        let state = acc.convert_to_state(&[values], None)?;

        let list = state[0].as_list::<i32>();
        // Rows 0 and 2 are valid lists; rows 1 and 3 are null list entries
        assert!(!list.is_null(0));
        assert!(list.is_null(1));
        assert!(!list.is_null(2));
        assert!(list.is_null(3));

        // Feed state into a new accumulator via merge_batch
        let mut acc2 = ArrayAggGroupsAccumulator::new(DataType::Int32, true);
        acc2.merge_batch(&state, &[0, 0, 1, 1], 2)?;

        // Group 0: received [1] and null (skipped) → [1]
        let vals = eval_i32_lists(&mut acc2, EmitTo::All)?;
        assert_eq!(vals[0], Some(vec![Some(1)]));
        // Group 1: received [3] and null (skipped) → [3]
        assert_eq!(vals[1], Some(vec![Some(3)]));

        Ok(())
    }

    #[test]
    fn groups_accumulator_all_groups_empty() -> Result<()> {
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, false);

        // Create groups but don't add any values (all filtered out)
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let filter = BooleanArray::from(vec![false, false]);
        acc.update_batch(&[values], &[0, 1], Some(&filter), 2)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals, vec![None, None]);

        Ok(())
    }

    #[test]
    fn groups_accumulator_ignore_nulls_all_null_group() -> Result<()> {
        // When ignore_nulls is true and a group receives only nulls,
        // it should produce a null output
        let mut acc = ArrayAggGroupsAccumulator::new(DataType::Int32, true);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1), None]));
        acc.update_batch(&[values], &[0, 1, 0], None, 2)?;

        let vals = eval_i32_lists(&mut acc, EmitTo::All)?;
        assert_eq!(vals[0], None); // group 0 got only nulls, all filtered
        assert_eq!(vals[1], Some(vec![Some(1)])); // group 1 got value 1

        Ok(())
    }

    // ---- retract_batch tests ----

    #[test]
    fn retract_basic_sliding_window() -> Result<()> {
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        // Simulate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW over [A, B, C, D]
        // Row 1: frame = [A]
        acc.update_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A"]);

        // Row 2: frame = [A, B]
        acc.update_batch(&[data(["B"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        // Row 3: frame = [B, C] — A leaves
        acc.update_batch(&[data(["C"])])?;
        acc.retract_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["B", "C"]);

        // Row 4: frame = [C, D] — B leaves
        acc.update_batch(&[data(["D"])])?;
        acc.retract_batch(&[data(["B"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["C", "D"]);

        Ok(())
    }

    #[test]
    fn retract_multi_element_across_arrays() -> Result<()> {
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        // First batch: 3 elements
        acc.update_batch(&[data(["A", "B", "C"])])?;
        // Second batch: 1 element
        acc.update_batch(&[data(["D"])])?;

        assert_eq!(
            print_nulls(str_arr(acc.evaluate()?)?),
            vec!["A", "B", "C", "D"]
        );

        // Partial retract from front array: A leaves
        acc.retract_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["B", "C", "D"]);

        // Retract spanning two arrays: B, C (rest of first array) + D (second array)
        acc.retract_batch(&[data(["B", "C", "D"])])?;
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list after full retract, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn retract_with_nulls_preserved() -> Result<()> {
        // ignore_nulls = false: NULLs are stored and counted for retract
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        acc.update_batch(&[data([Some("A"), None, Some("C")])])?;
        assert_eq!(
            print_nulls(str_arr(acc.evaluate()?)?),
            vec!["A", "NULL", "C"]
        );

        // Retract 2 elements: A and NULL both leave
        acc.retract_batch(&[data([Some("A"), None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["C"]);

        Ok(())
    }

    #[test]
    fn retract_with_ignore_nulls() -> Result<()> {
        // ignore_nulls = true: NULLs are NOT stored by update_batch,
        // so retract must only count non-null values
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, true)?;

        // update_batch with [A, NULL, C] → stores only [A, C] (NULL filtered)
        acc.update_batch(&[data([Some("A"), None, Some("C")])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "C"]);

        // retract_batch receives the original values including NULL: [A, NULL]
        // But only 1 non-null value (A) should be retracted
        acc.retract_batch(&[data([Some("A"), None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["C"]);

        // retract_batch with [NULL, C] — only C (1 non-null) retracted
        acc.retract_batch(&[data([None, Some("C")])])?;
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list after full retract, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn retract_ignore_nulls_all_nulls_batch() -> Result<()> {
        // When ignore_nulls = true and retract batch is all NULLs, nothing is retracted
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, true)?;

        acc.update_batch(&[data([Some("A"), Some("B")])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        // Retract batch of all NULLs: to_retract = 0, nothing changes
        acc.retract_batch(&[data::<Option<&str>, 3>([None, None, None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        Ok(())
    }

    #[test]
    fn retract_empty_accumulator() -> Result<()> {
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        // Retract on empty accumulator should be a no-op
        acc.retract_batch(&[data(["A"])])?;
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list for empty accumulator, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn retract_front_offset_partial_consume() -> Result<()> {
        // Reproduces the RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING scenario:
        //   ts: 1, 2, 3, 4, 100
        //
        // Row 1 (ts=1): update [A,B,C] (3 elements, ts in [-1,3])
        // Row 2 (ts=2): update [D]     (ts=4 enters)
        // Row 3 (ts=3): no change      (same frame [0..4))
        // Row 4 (ts=4): retract [A]    (ts=1 leaves, partial consume)
        // Row 5 (ts=100): retract [B,C,D] (3-element retract spanning arrays)
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        // Row 1: update_batch(["A","B","C"])
        acc.update_batch(&[data(["A", "B", "C"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B", "C"]);

        // Row 2: update_batch(["D"])
        acc.update_batch(&[data(["D"])])?;
        assert_eq!(
            print_nulls(str_arr(acc.evaluate()?)?),
            vec!["A", "B", "C", "D"]
        );

        // Row 4: retract_batch(["A"]) — partial consume, front_offset = 1
        acc.retract_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["B", "C", "D"]);

        // Row 5: update_batch(["E"]), then retract_batch(["B","C","D"])
        // retract spans: ["A","B","C"] (offset=1, 2 remaining) + ["D"] (1 element)
        acc.update_batch(&[data(["E"])])?;
        acc.retract_batch(&[data(["B", "C", "D"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["E"]);

        Ok(())
    }

    #[test]
    fn retract_update_after_full_drain() -> Result<()> {
        // Verify accumulator works correctly after being fully drained
        let mut acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;

        acc.update_batch(&[data(["A", "B"])])?;
        acc.retract_batch(&[data(["A", "B"])])?;

        // Accumulator is empty now
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list, got {result:?}"
        );

        // New values should work normally after drain
        acc.update_batch(&[data(["X", "Y"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["X", "Y"]);

        acc.retract_batch(&[data(["X"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["Y"]);

        Ok(())
    }

    #[test]
    fn retract_supports_retract_batch() -> Result<()> {
        let acc = ArrayAggAccumulator::try_new(&DataType::Utf8, false)?;
        assert!(acc.supports_retract_batch());

        let acc_ignore = ArrayAggAccumulator::try_new(&DataType::Utf8, true)?;
        assert!(acc_ignore.supports_retract_batch());

        Ok(())
    }

    #[test]
    fn retract_ignore_nulls_logical_vs_physical() -> Result<()> {
        // Regression test: DictionaryArray where logical nulls differ from physical nulls.
        // Manually construct a DictionaryArray where all indices are valid
        // (physical null_count = 0) but some point to null dictionary values
        // (logical_null_count > 0).
        use arrow::array::{DictionaryArray, Int32Array, StringArray};

        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut acc = ArrayAggAccumulator::try_new(&dict_type, true)?;

        // Dictionary values: ["hello", NULL, "world"]
        // Keys: [0, 1, 2, 1] — all valid, but keys 1 and 3 point to null value
        let values = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let keys = Int32Array::from(vec![0, 1, 2, 1]);
        let dict_array: ArrayRef = Arc::new(DictionaryArray::new(keys, Arc::new(values)));

        // Confirm the divergence this test exists to exercise
        assert_eq!(
            dict_array.null_count(),
            0,
            "physical nulls: none in keys bitmap"
        );
        assert_eq!(
            dict_array.logical_null_count(),
            2,
            "logical nulls: keys pointing to null values"
        );

        // update_batch uses logical_nulls() → stores only ["hello", "world"]
        acc.update_batch(std::slice::from_ref(&dict_array))?;

        // Verify 2 elements stored
        let result = acc.evaluate()?;
        match &result {
            ScalarValue::List(arr) => {
                let values = arr.value(0);
                assert_eq!(values.len(), 2);
            }
            other => panic!("expected List, got {other:?}"),
        }

        // retract_batch with same array: should retract 2 (logical non-nulls), not 4 (len) or 0 (physical non-nulls would be len-0=4)
        acc.retract_batch(&[dict_array])?;
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list after full retract, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn retract_ignore_nulls_dict_partial() -> Result<()> {
        // Partial retraction with DictionaryArray where logical != physical nulls.
        // Manually construct so keys are all valid but some point to null values.
        use arrow::array::{DictionaryArray, Int32Array, StringArray};

        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut acc = ArrayAggAccumulator::try_new(&dict_type, true)?;

        // update with ["A", "B", "C"] (no nulls)
        let values = StringArray::from(vec!["A", "B", "C"]);
        let keys = Int32Array::from(vec![0, 1, 2]);
        let update_array: ArrayRef =
            Arc::new(DictionaryArray::new(keys, Arc::new(values)));
        acc.update_batch(&[update_array])?;

        // retract with dict ["A", NULL, NULL]:
        //   keys [0, 1, 1] all valid → physical null_count = 0
        //   keys 1,2 point to null value → logical_null_count = 2
        //   non-null count = 3 - 2 = 1 → retract 1 element
        let values = StringArray::from(vec![Some("A"), None]);
        let keys = Int32Array::from(vec![0, 1, 1]);
        let retract_array: ArrayRef =
            Arc::new(DictionaryArray::new(keys, Arc::new(values)));

        assert_eq!(
            retract_array.null_count(),
            0,
            "physical nulls: none in keys bitmap"
        );
        assert_eq!(
            retract_array.logical_null_count(),
            2,
            "logical nulls: keys pointing to null values"
        );

        acc.retract_batch(&[retract_array])?;

        // Should have retracted only 1 element, leaving ["B", "C"]
        let result = acc.evaluate()?;
        match &result {
            ScalarValue::List(arr) => {
                let values = arr.value(0);
                assert_eq!(values.len(), 2);
            }
            other => panic!("expected List with 2 elements, got {other:?}"),
        }

        Ok(())
    }

    // ---- DistinctArrayAggAccumulator retract_batch tests ----

    // Build a DISTINCT accumulator with ascending sort so evaluate output is
    // deterministic regardless of HashMap iteration order.
    fn distinct_acc(ignore_nulls: bool) -> Result<DistinctArrayAggAccumulator> {
        DistinctArrayAggAccumulator::try_new(
            &DataType::Utf8,
            Some(SortOptions::default()),
            ignore_nulls,
        )
    }

    #[test]
    fn distinct_retract_duplicate_remains() -> Result<()> {
        // Canonical regression for the HashSet-can't-retract bug: a value
        // that appears multiple times in-frame must survive retraction of
        // a single occurrence.
        let mut acc = distinct_acc(false)?;

        // Feed [A, A, B] across two batches to exercise multi-batch state.
        acc.update_batch(&[data(["A", "A"])])?;
        acc.update_batch(&[data(["B"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        // Retract a single A — the other A is still in the frame.
        acc.retract_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        // Retract the remaining A — only B left.
        acc.retract_batch(&[data(["A"])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["B"]);

        Ok(())
    }

    #[test]
    fn distinct_retract_full_removal() -> Result<()> {
        let mut acc = distinct_acc(false)?;

        acc.update_batch(&[data(["A", "B"])])?;
        acc.retract_batch(&[data(["A", "B"])])?;

        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list after full retract, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn distinct_retract_ignore_nulls_skips() -> Result<()> {
        // ignore_nulls=true: NULL never enters state on update, so retract
        // must also skip NULL — otherwise we'd error on the missing key.
        let mut acc = distinct_acc(true)?;

        acc.update_batch(&[data([Some("A"), None, Some("B")])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A", "B"]);

        // Retract [A, NULL] — the NULL is skipped, only A is removed.
        acc.retract_batch(&[data([Some("A"), None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["B"]);

        Ok(())
    }

    #[test]
    fn distinct_retract_null_tracked() -> Result<()> {
        // ignore_nulls=false: NULL enters state with a refcount and must
        // retract symmetrically; the NULL key must be removed at zero
        // (else evaluate still emits a NULL element).
        let mut acc = distinct_acc(false)?;

        acc.update_batch(&[data([Some("A"), None, None])])?;
        // With nulls_first=true (SortOptions default), NULL sorts before A.
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["NULL", "A"]);

        // Retract one NULL — count drops to 1, key still present.
        acc.retract_batch(&[data::<Option<&str>, 1>([None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["NULL", "A"]);

        // Retract the remaining NULL — key is removed.
        acc.retract_batch(&[data::<Option<&str>, 1>([None])])?;
        assert_eq!(print_nulls(str_arr(acc.evaluate()?)?), vec!["A"]);

        Ok(())
    }

    #[test]
    fn distinct_supports_retract_batch() -> Result<()> {
        let acc = distinct_acc(false)?;
        assert!(acc.supports_retract_batch());

        let acc_ignore = distinct_acc(true)?;
        assert!(acc_ignore.supports_retract_batch());

        Ok(())
    }

    #[test]
    fn distinct_merge_then_evaluate_regression() -> Result<()> {
        // Non-window path: state -> merge_batch -> evaluate must still
        // produce the union of distinct values across partitions.
        let mut acc1 = distinct_acc(false)?;
        let mut acc2 = distinct_acc(false)?;

        acc1.update_batch(&[data(["A", "A", "B"])])?;
        acc2.update_batch(&[data(["A", "C"])])?;

        let state = acc2.state()?;
        let state_arrs: Vec<ArrayRef> = state
            .into_iter()
            .map(|sv| sv.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        acc1.merge_batch(&state_arrs)?;

        assert_eq!(print_nulls(str_arr(acc1.evaluate()?)?), vec!["A", "B", "C"]);

        Ok(())
    }

    #[test]
    fn distinct_array_agg_utf8_deduplicates() -> Result<()> {
        use arrow::array::StringArray;

        // 7 rows with 4 distinct values, each duplicate appearing twice.
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "postgres", "mysql", "postgres", "redis", "mysql", "duckdb", "redis",
        ]));

        let mut acc = DistinctArrayAggAccumulator::try_new(&DataType::Utf8, None, false)?;
        acc.update_batch(&[input])?;

        let result = acc.evaluate()?;
        let ScalarValue::List(arr) = &result else {
            panic!("expected ScalarValue::List, got {result:?}");
        };

        let inner = arr.value(0);
        let strings = inner
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("inner array should be StringArray");

        // HashSet ordering is nondeterministic — sort before asserting.
        let mut values: Vec<&str> =
            (0..strings.len()).map(|i| strings.value(i)).collect();
        values.sort_unstable();

        assert_eq!(values, vec!["duckdb", "mysql", "postgres", "redis"]);
        Ok(())
    }

    #[test]
    fn distinct_array_agg_int64_deduplicates() -> Result<()> {
        use arrow::array::Int64Array;

        // 7 rows with 4 distinct values, each duplicate appearing twice.
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1, 3, 2, 4, 3]));

        let mut acc =
            DistinctArrayAggAccumulator::try_new(&DataType::Int64, None, false)?;
        acc.update_batch(&[input])?;

        let result = acc.evaluate()?;
        let ScalarValue::List(arr) = &result else {
            panic!("expected ScalarValue::List, got {result:?}");
        };

        let inner = arr.value(0);
        let ints = inner
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("inner array should be Int64Array");

        let mut values: Vec<i64> = (0..ints.len()).map(|i| ints.value(i)).collect();
        values.sort_unstable();

        assert_eq!(values, vec![1i64, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn distinct_array_agg_float64_deduplicates() -> Result<()> {
        use arrow::array::Float64Array;

        // 7 rows with 4 distinct values, each duplicate appearing twice.
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            1.0f64, 2.5, 1.0, 3.75, 2.5, 4.0, 3.75,
        ]));

        let mut acc =
            DistinctArrayAggAccumulator::try_new(&DataType::Float64, None, false)?;
        acc.update_batch(&[input])?;

        let result = acc.evaluate()?;
        let ScalarValue::List(arr) = &result else {
            panic!("expected ScalarValue::List, got {result:?}");
        };

        let inner = arr.value(0);
        let floats = inner
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("inner array should be Float64Array");

        // f64 has no Ord — use total_cmp for a stable sort.
        let mut values: Vec<f64> = (0..floats.len()).map(|i| floats.value(i)).collect();
        values.sort_unstable_by(|a, b| a.total_cmp(b));

        assert_eq!(values, vec![1.0f64, 2.5, 3.75, 4.0]);
        Ok(())
    }

    #[test]
    fn distinct_array_agg_dictionary_preserves_type() -> Result<()> {
        use arrow::array::{DictionaryArray, Int32Array, StringArray};

        // Dictionary(Int32, Utf8) input with duplicates.
        let keys = Int32Array::from(vec![0, 1, 0, 2, 1]); // "a", "b", "a", "c", "b"
        let values = StringArray::from(vec!["a", "b", "c"]);
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, Arc::new(values)));

        let datatype =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut acc = DistinctArrayAggAccumulator::try_new(&datatype, None, false)?;
        acc.update_batch(&[dict])?;

        let result = acc.evaluate()?;
        let ScalarValue::List(arr) = &result else {
            panic!("expected ScalarValue::List, got {result:?}");
        };

        // The element type of the returned list must stay Dictionary(Int32, Utf8),
        // not be silently widened to Utf8.
        assert_eq!(
            arr.values().data_type(),
            &datatype,
            "element type must be Dictionary(Int32, Utf8), got {}",
            arr.values().data_type()
        );

        // There should be exactly 3 distinct values.
        assert_eq!(arr.value(0).len(), 3);
        Ok(())
    }

    #[test]
    fn distinct_array_agg_date32_deduplicates() -> Result<()> {
        use arrow::array::Date32Array;

        // 7 rows with 4 distinct dates (days since epoch), each duplicate appearing twice.
        let input: ArrayRef = Arc::new(Date32Array::from(vec![
            100i32, 200, 100, 300, 200, 400, 300,
        ]));

        let mut acc =
            DistinctArrayAggAccumulator::try_new(&DataType::Date32, None, false)?;
        acc.update_batch(&[input])?;

        let result = acc.evaluate()?;
        let ScalarValue::List(arr) = &result else {
            panic!("expected ScalarValue::List, got {result:?}");
        };

        let inner = arr.value(0);
        let dates = inner
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("inner array should be Date32Array");

        let mut values: Vec<i32> = (0..dates.len()).map(|i| dates.value(i)).collect();
        values.sort_unstable();

        assert_eq!(values, vec![100i32, 200, 300, 400]);
        Ok(())
    }

    #[test]
    fn distinct_retract_memory_is_bounded() -> Result<()> {
        use arrow::array::Int64Array;

        // Emulates a sliding window where each value enters and immediately
        // leaves. Only CARDINALITY distinct values are ever live at once;
        // memory must not grow with the number of rows processed.
        const CARDINALITY: i64 = 10;
        const WARMUP_ROWS: i64 = 1_000;
        const EXTRA_ROWS: i64 = 20_000;

        let mut acc =
            DistinctArrayAggAccumulator::try_new(&DataType::Int64, None, false)?;

        let slide = |acc: &mut DistinctArrayAggAccumulator, rows: i64| -> Result<()> {
            for i in 0..rows {
                let value: ArrayRef = Arc::new(Int64Array::from(vec![i % CARDINALITY]));
                acc.update_batch(std::slice::from_ref(&value))?;
                acc.retract_batch(std::slice::from_ref(&value))?;
            }
            Ok(())
        };

        // Let every buffer reach its steady state before taking a baseline.
        slide(&mut acc, WARMUP_ROWS)?;
        let baseline = acc.size();

        slide(&mut acc, EXTRA_ROWS)?;
        let grown = acc.size();

        assert!(
            grown <= 2 * baseline,
            "size() must not grow with the number of retracted rows: \
             {baseline} bytes after {WARMUP_ROWS} rows, \
             {grown} bytes after {} rows",
            WARMUP_ROWS + EXTRA_ROWS
        );

        // Everything was retracted so evaluate must return null.
        let result = acc.evaluate()?;
        assert!(
            matches!(&result, ScalarValue::List(arr) if arr.is_null(0)),
            "expected null list after retracting every row, got {result:?}"
        );

        Ok(())
    }
}
