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

//! Defines the FIRST_VALUE/LAST_VALUE aggregations.

use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, BooleanBufferBuilder,
    PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{self, LexicographicalComparator, SortColumn, SortOptions};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field, Float16Type,
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::utils::{compare_rows, extract_row_at_idx_to_buf, get_row_at_idx};
use datafusion_common::{
    arrow_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, Expr, ExprFunctionExt,
    GroupsAccumulator, Signature, SortExpr, Volatility,
};
use datafusion_functions_aggregate_common::utils::get_sort_options;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

create_func!(FirstValue, first_value_udaf);

/// Returns the first value in a group of values.
pub fn first_value(expression: Expr, order_by: Option<Vec<SortExpr>>) -> Expr {
    if let Some(order_by) = order_by {
        first_value_udaf()
            .call(vec![expression])
            .order_by(order_by)
            .build()
            // guaranteed to be `Expr::AggregateFunction`
            .unwrap()
    } else {
        first_value_udaf().call(vec![expression])
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the first element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.",
    syntax_example = "first_value(expression [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT first_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| first_value(column_name ORDER BY other_column)|
+-----------------------------------------------+
| first_element                                 |
+-----------------------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
pub struct FirstValue {
    signature: Signature,
    requirement_satisfied: bool,
}

impl Debug for FirstValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FirstValue")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for FirstValue {
    fn default() -> Self {
        Self::new()
    }
}

impl FirstValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            requirement_satisfied: false,
        }
    }

    fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }
}

impl AggregateUDFImpl for FirstValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "first_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordering_dtypes = acc_args
            .ordering_req
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        // When requirement is empty, or it is signalled by outside caller that
        // the ordering requirement is/will be satisfied.
        let requirement_satisfied =
            acc_args.ordering_req.is_empty() || self.requirement_satisfied;

        FirstValueAccumulator::try_new(
            acc_args.return_type,
            &ordering_dtypes,
            acc_args.ordering_req.clone(),
            acc_args.ignore_nulls,
        )
        .map(|acc| Box::new(acc.with_requirement_satisfied(requirement_satisfied)) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(args.name, "first_value"),
            args.return_type.clone(),
            true,
        )];
        fields.extend(args.ordering_fields.to_vec());
        fields.push(Field::new("is_set", DataType::Boolean, true));
        Ok(fields)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        fn create_accumulator<T>(
            args: AccumulatorArgs,
        ) -> Result<Box<dyn GroupsAccumulator>>
        where
            T: ArrowPrimitiveType + Send,
        {
            let ordering_dtypes = args
                .ordering_req
                .iter()
                .map(|e| e.expr.data_type(args.schema))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(FirstPrimitiveGroupsAccumulator::<T>::try_new(
                args.ordering_req.clone(),
                args.ignore_nulls,
                args.return_type,
                &ordering_dtypes,
            )?))
        }

        match args.return_type {
            DataType::Int8 => create_accumulator::<Int8Type>(args),
            DataType::Int16 => create_accumulator::<Int16Type>(args),
            DataType::Int32 => create_accumulator::<Int32Type>(args),
            DataType::Int64 => create_accumulator::<Int64Type>(args),
            DataType::UInt8 => create_accumulator::<UInt8Type>(args),
            DataType::UInt16 => create_accumulator::<UInt16Type>(args),
            DataType::UInt32 => create_accumulator::<UInt32Type>(args),
            DataType::UInt64 => create_accumulator::<UInt64Type>(args),
            DataType::Float16 => create_accumulator::<Float16Type>(args),
            DataType::Float32 => create_accumulator::<Float32Type>(args),
            DataType::Float64 => create_accumulator::<Float64Type>(args),

            DataType::Decimal128(_, _) => create_accumulator::<Decimal128Type>(args),
            DataType::Decimal256(_, _) => create_accumulator::<Decimal256Type>(args),

            DataType::Timestamp(TimeUnit::Second, _) => {
                create_accumulator::<TimestampSecondType>(args)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                create_accumulator::<TimestampMillisecondType>(args)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                create_accumulator::<TimestampMicrosecondType>(args)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                create_accumulator::<TimestampNanosecondType>(args)
            }

            DataType::Date32 => create_accumulator::<Date32Type>(args),
            DataType::Date64 => create_accumulator::<Date64Type>(args),
            DataType::Time32(TimeUnit::Second) => {
                create_accumulator::<Time32SecondType>(args)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                create_accumulator::<Time32MillisecondType>(args)
            }

            DataType::Time64(TimeUnit::Microsecond) => {
                create_accumulator::<Time64MicrosecondType>(args)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                create_accumulator::<Time64NanosecondType>(args)
            }

            _ => internal_err!(
                "GroupsAccumulator not supported for first({})",
                args.return_type
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(
            FirstValue::new().with_requirement_satisfied(beneficial_ordering),
        )))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(last_value_udaf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

struct FirstPrimitiveGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    // ================ state ===========
    vals: Vec<T::Native>,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator.
    // The `orderings` are stored row-wise, meaning that `orderings[group_idx]`
    // represents the ordering values corresponding to the `group_idx`-th group.
    orderings: Vec<Vec<ScalarValue>>,
    // At the beginning, `is_sets[group_idx]` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_sets[group_idx]` flag
    is_sets: BooleanBufferBuilder,
    // null_builder[group_idx] == false => vals[group_idx] is null
    null_builder: BooleanBufferBuilder,
    // size of `self.orderings`
    // Calculating the memory usage of `self.orderings` using `ScalarValue::size_of_vec` is quite costly.
    // Therefore, we cache it and compute `size_of` only after each update
    // to avoid calling `ScalarValue::size_of_vec` by Self.size.
    size_of_orderings: usize,

    // buffer for `get_filtered_min_of_each_group`
    // filter_min_of_each_group_buf.0[group_idx] -> idx_in_val
    // only valid if filter_min_of_each_group_buf.1[group_idx] == true
    min_of_each_group_buf: (Vec<usize>, BooleanBufferBuilder),

    // =========== option ============

    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // derived from `ordering_req`.
    sort_options: Vec<SortOptions>,
    // Stores whether incoming data already satisfies the ordering requirement.
    input_requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
    /// The output type
    data_type: DataType,
    default_orderings: Vec<ScalarValue>,
}

impl<T> FirstPrimitiveGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    fn try_new(
        ordering_req: LexOrdering,
        ignore_nulls: bool,
        data_type: &DataType,
        ordering_dtypes: &[DataType],
    ) -> Result<Self> {
        let requirement_satisfied = ordering_req.is_empty();

        let default_orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;

        let sort_options = get_sort_options(ordering_req.as_ref());

        Ok(Self {
            null_builder: BooleanBufferBuilder::new(0),
            ordering_req,
            sort_options,
            input_requirement_satisfied: requirement_satisfied,
            ignore_nulls,
            default_orderings,
            data_type: data_type.clone(),
            vals: Vec::new(),
            orderings: Vec::new(),
            is_sets: BooleanBufferBuilder::new(0),
            size_of_orderings: 0,
            min_of_each_group_buf: (Vec::new(), BooleanBufferBuilder::new(0)),
        })
    }

    fn need_update(&self, group_idx: usize) -> bool {
        if !self.is_sets.get_bit(group_idx) {
            return true;
        }

        if self.ignore_nulls && !self.null_builder.get_bit(group_idx) {
            return true;
        }

        !self.input_requirement_satisfied
    }

    fn should_update_state(
        &self,
        group_idx: usize,
        new_ordering_values: &[ScalarValue],
    ) -> Result<bool> {
        if !self.is_sets.get_bit(group_idx) {
            return Ok(true);
        }

        assert!(new_ordering_values.len() == self.ordering_req.len());
        let current_ordering = &self.orderings[group_idx];
        compare_rows(current_ordering, new_ordering_values, &self.sort_options)
            .map(|x| x.is_gt())
    }

    fn take_orderings(&mut self, emit_to: EmitTo) -> Vec<Vec<ScalarValue>> {
        let result = emit_to.take_needed(&mut self.orderings);

        match emit_to {
            EmitTo::All => self.size_of_orderings = 0,
            EmitTo::First(_) => {
                self.size_of_orderings -=
                    result.iter().map(ScalarValue::size_of_vec).sum::<usize>()
            }
        }

        result
    }

    fn take_need(
        bool_buf_builder: &mut BooleanBufferBuilder,
        emit_to: EmitTo,
    ) -> BooleanBuffer {
        let bool_buf = bool_buf_builder.finish();
        match emit_to {
            EmitTo::All => bool_buf,
            EmitTo::First(n) => {
                // split off the first N values in seen_values
                //
                // TODO make this more efficient rather than two
                // copies and bitwise manipulation
                let first_n: BooleanBuffer = bool_buf.iter().take(n).collect();
                // reset the existing buffer
                for b in bool_buf.iter().skip(n) {
                    bool_buf_builder.append(b);
                }
                first_n
            }
        }
    }

    fn resize_states(&mut self, new_size: usize) {
        self.vals.resize(new_size, T::default_value());

        self.null_builder.resize(new_size);

        if self.orderings.len() < new_size {
            let current_len = self.orderings.len();

            self.orderings
                .resize(new_size, self.default_orderings.clone());

            self.size_of_orderings += (new_size - current_len)
                * ScalarValue::size_of_vec(
                    // Note: In some cases (such as in the unit test below)
                    // ScalarValue::size_of_vec(&self.default_orderings) != ScalarValue::size_of_vec(&self.default_orderings.clone())
                    // This may be caused by the different vec.capacity() values?
                    self.orderings.last().unwrap(),
                );
        }

        self.is_sets.resize(new_size);

        self.min_of_each_group_buf.0.resize(new_size, 0);
        self.min_of_each_group_buf.1.resize(new_size);
    }

    fn update_state(
        &mut self,
        group_idx: usize,
        orderings: &[ScalarValue],
        new_val: T::Native,
        is_null: bool,
    ) {
        self.vals[group_idx] = new_val;
        self.is_sets.set_bit(group_idx, true);

        self.null_builder.set_bit(group_idx, !is_null);

        assert!(orderings.len() == self.ordering_req.len());
        let old_size = ScalarValue::size_of_vec(&self.orderings[group_idx]);
        self.orderings[group_idx].clear();
        self.orderings[group_idx].extend_from_slice(orderings);
        let new_size = ScalarValue::size_of_vec(&self.orderings[group_idx]);
        self.size_of_orderings = self.size_of_orderings - old_size + new_size;
    }

    fn take_state(
        &mut self,
        emit_to: EmitTo,
    ) -> (ArrayRef, Vec<Vec<ScalarValue>>, BooleanBuffer) {
        emit_to.take_needed(&mut self.min_of_each_group_buf.0);
        self.min_of_each_group_buf
            .1
            .truncate(self.min_of_each_group_buf.0.len());

        (
            self.take_vals_and_null_buf(emit_to),
            self.take_orderings(emit_to),
            Self::take_need(&mut self.is_sets, emit_to),
        )
    }

    // should be used in test only
    #[cfg(test)]
    fn compute_size_of_orderings(&self) -> usize {
        self.orderings
            .iter()
            .map(ScalarValue::size_of_vec)
            .sum::<usize>()
    }

    /// Returns a vector of tuples `(group_idx, idx_in_val)` representing the index of the
    /// minimum value in `orderings` for each group, using lexicographical comparison.
    /// Values are filtered using `opt_filter` and `is_set_arr` if provided.
    fn get_filtered_min_of_each_group(
        &mut self,
        orderings: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        vals: &PrimitiveArray<T>,
        is_set_arr: Option<&BooleanArray>,
    ) -> Result<Vec<(usize, usize)>> {
        // Set all values in min_of_each_group_buf.1 to false.
        self.min_of_each_group_buf.1.truncate(0);
        self.min_of_each_group_buf
            .1
            .append_n(self.vals.len(), false);

        // No need to call `clear` since `self.min_of_each_group_buf.0[group_idx]`
        // is only valid when `self.min_of_each_group_buf.1[group_idx] == true`.

        let comparator = {
            assert_eq!(orderings.len(), self.ordering_req.len());
            let sort_columns = orderings
                .iter()
                .zip(self.ordering_req.iter())
                .map(|(array, req)| SortColumn {
                    values: Arc::clone(array),
                    options: Some(req.options),
                })
                .collect::<Vec<_>>();

            LexicographicalComparator::try_new(&sort_columns)?
        };

        for (idx_in_val, group_idx) in group_indices.iter().enumerate() {
            let group_idx = *group_idx;

            let passed_filter = opt_filter.is_none_or(|x| x.value(idx_in_val));

            let is_set = is_set_arr.is_none_or(|x| x.value(idx_in_val));

            if !passed_filter || !is_set {
                continue;
            }

            if !self.need_update(group_idx) {
                continue;
            }

            if self.ignore_nulls && vals.is_null(idx_in_val) {
                continue;
            }

            let is_valid = self.min_of_each_group_buf.1.get_bit(group_idx);
            if is_valid
                && comparator
                    .compare(self.min_of_each_group_buf.0[group_idx], idx_in_val)
                    .is_gt()
            {
                self.min_of_each_group_buf.0[group_idx] = idx_in_val;
            } else if !is_valid {
                self.min_of_each_group_buf.1.set_bit(group_idx, true);
                self.min_of_each_group_buf.0[group_idx] = idx_in_val;
            }
        }

        Ok(self
            .min_of_each_group_buf
            .0
            .iter()
            .enumerate()
            .filter(|(group_idx, _)| self.min_of_each_group_buf.1.get_bit(*group_idx))
            .map(|(group_idx, idx_in_val)| (group_idx, *idx_in_val))
            .collect::<Vec<_>>())
    }

    fn take_vals_and_null_buf(&mut self, emit_to: EmitTo) -> ArrayRef {
        let r = emit_to.take_needed(&mut self.vals);

        let null_buf = NullBuffer::new(Self::take_need(&mut self.null_builder, emit_to));

        let values = PrimitiveArray::<T>::new(r.into(), Some(null_buf)) // no copy
            .with_data_type(self.data_type.clone());
        Arc::new(values)
    }
}

impl<T> GroupsAccumulator for FirstPrimitiveGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    fn update_batch(
        &mut self,
        // e.g. first_value(a order by b): values_and_order_cols will be [a, b]
        values_and_order_cols: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize_states(total_num_groups);

        let vals = values_and_order_cols[0].as_primitive::<T>();

        let mut ordering_buf = Vec::with_capacity(self.ordering_req.len());

        // The overhead of calling `extract_row_at_idx_to_buf` is somewhat high, so we need to minimize its calls as much as possible.
        for (group_idx, idx) in self
            .get_filtered_min_of_each_group(
                &values_and_order_cols[1..],
                group_indices,
                opt_filter,
                vals,
                None,
            )?
            .into_iter()
        {
            extract_row_at_idx_to_buf(
                &values_and_order_cols[1..],
                idx,
                &mut ordering_buf,
            )?;

            if self.should_update_state(group_idx, &ordering_buf)? {
                self.update_state(
                    group_idx,
                    &ordering_buf,
                    vals.value(idx),
                    vals.is_null(idx),
                );
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(self.take_state(emit_to).0)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (val_arr, orderings, is_sets) = self.take_state(emit_to);
        let mut result = Vec::with_capacity(self.orderings.len() + 2);

        result.push(val_arr);

        let ordering_cols = {
            let mut ordering_cols = Vec::with_capacity(self.ordering_req.len());
            for _ in 0..self.ordering_req.len() {
                ordering_cols.push(Vec::with_capacity(self.orderings.len()));
            }
            for row in orderings.into_iter() {
                assert_eq!(row.len(), self.ordering_req.len());
                for (col_idx, ordering) in row.into_iter().enumerate() {
                    ordering_cols[col_idx].push(ordering);
                }
            }

            ordering_cols
        };
        for ordering_col in ordering_cols {
            result.push(ScalarValue::iter_to_array(ordering_col)?);
        }

        result.push(Arc::new(BooleanArray::new(is_sets, None)));

        Ok(result)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize_states(total_num_groups);

        let mut ordering_buf = Vec::with_capacity(self.ordering_req.len());

        let (is_set_arr, val_and_order_cols) = match values.split_last() {
            Some(result) => result,
            None => return internal_err!("Empty row in FISRT_VALUE"),
        };

        let is_set_arr = as_boolean_array(is_set_arr)?;

        let vals = values[0].as_primitive::<T>();
        // The overhead of calling `extract_row_at_idx_to_buf` is somewhat high, so we need to minimize its calls as much as possible.
        let groups = self.get_filtered_min_of_each_group(
            &val_and_order_cols[1..],
            group_indices,
            opt_filter,
            vals,
            Some(is_set_arr),
        )?;

        for (group_idx, idx) in groups.into_iter() {
            extract_row_at_idx_to_buf(&val_and_order_cols[1..], idx, &mut ordering_buf)?;

            if self.should_update_state(group_idx, &ordering_buf)? {
                self.update_state(
                    group_idx,
                    &ordering_buf,
                    vals.value(idx),
                    vals.is_null(idx),
                );
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.vals.capacity() * size_of::<T::Native>()
            + self.null_builder.capacity() / 8 // capacity is in bits, so convert to bytes 
            + self.is_sets.capacity() / 8
            + self.size_of_orderings
            + self.min_of_each_group_buf.0.capacity() * size_of::<usize>()
            + self.min_of_each_group_buf.1.capacity() / 8
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut result = values.to_vec();
        match opt_filter {
            Some(f) => {
                result.push(Arc::new(f.clone()));
                Ok(result)
            }
            None => {
                result.push(Arc::new(BooleanArray::from(vec![true; values[0].len()])));
                Ok(result)
            }
        }
    }
}
#[derive(Debug)]
pub struct FirstValueAccumulator {
    first: ScalarValue,
    // At the beginning, `is_set` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_set` flag and do not update `first` anymore.
    is_set: bool,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator. These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // Stores whether incoming data already satisfies the ordering requirement.
    requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let requirement_satisfied = ordering_req.is_empty();
        ScalarValue::try_from(data_type).map(|first| Self {
            first,
            is_set: false,
            orderings,
            ordering_req,
            requirement_satisfied,
            ignore_nulls,
        })
    }

    pub fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.first = row[0].clone();
        self.orderings = row[1..].to_vec();
        self.is_set = true;
    }

    fn get_first_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in FIRST_VALUE");
        };
        if self.requirement_satisfied {
            // Get first entry according to the pre-existing ordering (0th index):
            if self.ignore_nulls {
                // If ignoring nulls, find the first non-null value.
                for i in 0..value.len() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                // If not ignoring nulls, return the first value if it exists.
                return Ok((!value.is_empty()).then_some(0));
            }
        }

        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: Arc::clone(values),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;

        let min_index = if self.ignore_nulls {
            (0..value.len())
                .filter(|&index| !value.is_null(index))
                .min_by(|&a, &b| comparator.compare(a, b))
        } else {
            (0..value.len()).min_by(|&a, &b| comparator.compare(a, b))
        };

        Ok(min_index)
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.first.clone()];
        result.extend(self.orderings.iter().cloned());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                self.update_with_new_row(&row);
            }
        } else if !self.requirement_satisfied {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                let orderings = &row[1..];
                if compare_rows(
                    &self.orderings,
                    orderings,
                    &get_sort_options(self.ordering_req.as_ref()),
                )?
                .is_gt()
                {
                    self.update_with_new_row(&row);
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        let filtered_states =
            filter_states_according_to_is_set(&states[0..is_set_idx], flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_columns = convert_to_sort_cols(
            &filtered_states[1..is_set_idx],
            self.ordering_req.as_ref(),
        );

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let min = (0..filtered_states[0].len()).min_by(|&a, &b| comparator.compare(a, b));

        if let Some(first_idx) = min {
            let first_row = get_row_at_idx(&filtered_states, first_idx)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let first_ordering = &first_row[1..is_set_idx];
            let sort_options = get_sort_options(self.ordering_req.as_ref());
            // Either there is no existing value, or there is an earlier version in new data.
            if !self.is_set
                || compare_rows(&self.orderings, first_ordering, &sort_options)?.is_gt()
            {
                // Update with first value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                self.update_with_new_row(&first_row[0..is_set_idx]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.first)
            + self.first.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - size_of_val(&self.orderings)
    }
}

make_udaf_expr_and_func!(
    LastValue,
    last_value,
    "Returns the last value in a group of values.",
    last_value_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the last element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.",
    syntax_example = "last_value(expression [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT last_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| last_value(column_name ORDER BY other_column) |
+-----------------------------------------------+
| last_element                                  |
+-----------------------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
pub struct LastValue {
    signature: Signature,
    requirement_satisfied: bool,
}

impl Debug for LastValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("LastValue")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for LastValue {
    fn default() -> Self {
        Self::new()
    }
}

impl LastValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            requirement_satisfied: false,
        }
    }

    fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }
}

impl AggregateUDFImpl for LastValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordering_dtypes = acc_args
            .ordering_req
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        let requirement_satisfied =
            acc_args.ordering_req.is_empty() || self.requirement_satisfied;

        LastValueAccumulator::try_new(
            acc_args.return_type,
            &ordering_dtypes,
            acc_args.ordering_req.clone(),
            acc_args.ignore_nulls,
        )
        .map(|acc| Box::new(acc.with_requirement_satisfied(requirement_satisfied)) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let StateFieldsArgs {
            name,
            input_types,
            return_type: _,
            ordering_fields,
            is_distinct: _,
        } = args;
        let mut fields = vec![Field::new(
            format_state_name(name, "last_value"),
            input_types[0].clone(),
            true,
        )];
        fields.extend(ordering_fields.to_vec());
        fields.push(Field::new("is_set", DataType::Boolean, true));
        Ok(fields)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(
            LastValue::new().with_requirement_satisfied(beneficial_ordering),
        )))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(first_value_udaf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
struct LastValueAccumulator {
    last: ScalarValue,
    // The `is_set` flag keeps track of whether the last value is finalized.
    // This information is used to discriminate genuine NULLs and NULLS that
    // occur due to empty partitions.
    is_set: bool,
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // Stores whether incoming data already satisfies the ordering requirement.
    requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let requirement_satisfied = ordering_req.is_empty();
        ScalarValue::try_from(data_type).map(|last| Self {
            last,
            is_set: false,
            orderings,
            ordering_req,
            requirement_satisfied,
            ignore_nulls,
        })
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.last = row[0].clone();
        self.orderings = row[1..].to_vec();
        self.is_set = true;
    }

    fn get_last_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in LAST_VALUE");
        };
        if self.requirement_satisfied {
            // Get last entry according to the order of data:
            if self.ignore_nulls {
                // If ignoring nulls, find the last non-null value.
                for i in (0..value.len()).rev() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                return Ok((!value.is_empty()).then_some(value.len() - 1));
            }
        }
        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: Arc::clone(values),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let max_ind = if self.ignore_nulls {
            (0..value.len())
                .filter(|&index| !(value.is_null(index)))
                .max_by(|&a, &b| comparator.compare(a, b))
        } else {
            (0..value.len()).max_by(|&a, &b| comparator.compare(a, b))
        };

        Ok(max_ind)
    }

    fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.last.clone()];
        result.extend(self.orderings.clone());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set || self.requirement_satisfied {
            if let Some(last_idx) = self.get_last_idx(values)? {
                let row = get_row_at_idx(values, last_idx)?;
                self.update_with_new_row(&row);
            }
        } else if let Some(last_idx) = self.get_last_idx(values)? {
            let row = get_row_at_idx(values, last_idx)?;
            let orderings = &row[1..];
            // Update when there is a more recent entry
            if compare_rows(
                &self.orderings,
                orderings,
                &get_sort_options(self.ordering_req.as_ref()),
            )?
            .is_lt()
            {
                self.update_with_new_row(&row);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        let filtered_states =
            filter_states_according_to_is_set(&states[0..is_set_idx], flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_columns = convert_to_sort_cols(
            &filtered_states[1..is_set_idx],
            self.ordering_req.as_ref(),
        );

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let max = (0..filtered_states[0].len()).max_by(|&a, &b| comparator.compare(a, b));

        if let Some(last_idx) = max {
            let last_row = get_row_at_idx(&filtered_states, last_idx)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let last_ordering = &last_row[1..is_set_idx];
            let sort_options = get_sort_options(self.ordering_req.as_ref());
            // Either there is no existing value, or there is a newer (latest)
            // version in the new data:
            if !self.is_set
                || self.requirement_satisfied
                || compare_rows(&self.orderings, last_ordering, &sort_options)?.is_lt()
            {
                // Update with last value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                self.update_with_new_row(&last_row[0..is_set_idx]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.last)
            + self.last.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - size_of_val(&self.orderings)
    }
}

/// Filters states according to the `is_set` flag at the last column and returns
/// the resulting states.
fn filter_states_according_to_is_set(
    states: &[ArrayRef],
    flags: &BooleanArray,
) -> Result<Vec<ArrayRef>> {
    states
        .iter()
        .map(|state| compute::filter(state, flags).map_err(|e| arrow_datafusion_err!(e)))
        .collect::<Result<Vec<_>>>()
}

/// Combines array refs and their corresponding orderings to construct `SortColumn`s.
fn convert_to_sort_cols(arrs: &[ArrayRef], sort_exprs: &LexOrdering) -> Vec<SortColumn> {
    arrs.iter()
        .zip(sort_exprs.iter())
        .map(|(item, sort_expr)| SortColumn {
            values: Arc::clone(item),
            options: Some(sort_expr.options),
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use arrow::{array::Int64Array, compute::SortOptions, datatypes::Schema};
    use datafusion_physical_expr::{expressions::col, PhysicalSortExpr};

    use super::*;

    #[test]
    fn test_first_last_value_value() -> Result<()> {
        let mut first_accumulator = FirstValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        let mut last_accumulator = LastValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        // first value in the tuple is start of the range (inclusive),
        // second value in the tuple is end of the range (exclusive)
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())) as ArrayRef
            })
            .collect::<Vec<_>>();
        for arr in arrs {
            // Once first_value is set, accumulator should remember it.
            // It shouldn't update first_value for each new batch
            first_accumulator.update_batch(&[Arc::clone(&arr)])?;
            // last_value should be updated for each new batch.
            last_accumulator.update_batch(&[arr])?;
        }
        // First Value comes from the first value of the first batch which is 0
        assert_eq!(first_accumulator.evaluate()?, ScalarValue::Int64(Some(0)));
        // Last value comes from the last value of the last batch which is 12
        assert_eq!(last_accumulator.evaluate()?, ScalarValue::Int64(Some(12)));
        Ok(())
    }

    #[test]
    fn test_first_last_state_after_merge() -> Result<()> {
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new((start..end).collect::<Int64Array>()) as ArrayRef
            })
            .collect::<Vec<_>>();

        // FirstValueAccumulator
        let mut first_accumulator = FirstValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;

        first_accumulator.update_batch(&[Arc::clone(&arrs[0])])?;
        let state1 = first_accumulator.state()?;

        let mut first_accumulator = FirstValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        first_accumulator.update_batch(&[Arc::clone(&arrs[1])])?;
        let state2 = first_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(compute::concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut first_accumulator = FirstValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        first_accumulator.merge_batch(&states)?;

        let merged_state = first_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());

        // LastValueAccumulator
        let mut last_accumulator = LastValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;

        last_accumulator.update_batch(&[Arc::clone(&arrs[0])])?;
        let state1 = last_accumulator.state()?;

        let mut last_accumulator = LastValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        last_accumulator.update_batch(&[Arc::clone(&arrs[1])])?;
        let state2 = last_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(compute::concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut last_accumulator = LastValueAccumulator::try_new(
            &DataType::Int64,
            &[],
            LexOrdering::default(),
            false,
        )?;
        last_accumulator.merge_batch(&states)?;

        let merged_state = last_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());

        Ok(())
    }

    #[test]
    fn test_frist_group_acc() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]));

        let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }]);

        let mut group_acc = FirstPrimitiveGroupsAccumulator::<Int64Type>::try_new(
            sort_key,
            true,
            &DataType::Int64,
            &[DataType::Int64],
        )?;

        let mut val_with_orderings = {
            let mut val_with_orderings = Vec::<ArrayRef>::new();

            let vals = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-6)]));
            let orderings = Arc::new(Int64Array::from(vec![1, -9, 3, -6]));

            val_with_orderings.push(vals);
            val_with_orderings.push(orderings);

            val_with_orderings
        };

        group_acc.update_batch(
            &val_with_orderings,
            &[0, 1, 2, 1],
            Some(&BooleanArray::from(vec![true, true, false, true])),
            3,
        )?;
        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        let state = group_acc.state(EmitTo::All)?;

        let expected_state: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
        ];
        assert_eq!(state, expected_state);

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        group_acc.merge_batch(
            &state,
            &[0, 1, 2],
            Some(&BooleanArray::from(vec![true, false, false])),
            3,
        )?;

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        val_with_orderings.clear();
        val_with_orderings.push(Arc::new(Int64Array::from(vec![6, 6])));
        val_with_orderings.push(Arc::new(Int64Array::from(vec![6, 6])));

        group_acc.update_batch(&val_with_orderings, &[1, 2], None, 4)?;

        let binding = group_acc.evaluate(EmitTo::All)?;
        let eval_result = binding.as_any().downcast_ref::<Int64Array>().unwrap();

        let expect: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(1), Some(6), Some(6), None]);

        assert_eq!(eval_result, &expect);

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        Ok(())
    }

    #[test]
    fn test_frist_group_acc_size_of_ordering() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]));

        let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }]);

        let mut group_acc = FirstPrimitiveGroupsAccumulator::<Int64Type>::try_new(
            sort_key,
            true,
            &DataType::Int64,
            &[DataType::Int64],
        )?;

        let val_with_orderings = {
            let mut val_with_orderings = Vec::<ArrayRef>::new();

            let vals = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-6)]));
            let orderings = Arc::new(Int64Array::from(vec![1, -9, 3, -6]));

            val_with_orderings.push(vals);
            val_with_orderings.push(orderings);

            val_with_orderings
        };

        for _ in 0..10 {
            group_acc.update_batch(
                &val_with_orderings,
                &[0, 1, 2, 1],
                Some(&BooleanArray::from(vec![true, true, false, true])),
                100,
            )?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.state(EmitTo::First(2))?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            let s = group_acc.state(EmitTo::All)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.merge_batch(&s, &Vec::from_iter(0..s[0].len()), None, 100)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.evaluate(EmitTo::First(2))?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.evaluate(EmitTo::All)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );
        }

        Ok(())
    }
}
