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

//! Defines the ANY_VALUE aggregation.

use std::fmt::Debug;
use std::hash::Hash;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

use crate::first_last::TrivialFirstValueAccumulator;

make_udaf_expr_and_func!(
    AnyValue,
    any_value,
    expression,
    "Returns an arbitrary non-null value",
    any_value_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns an arbitrary non-null value from a group, or NULL if the group contains only NULL values.",
    syntax_example = "any_value(expression)",
    sql_example = r#"```sql
> SELECT any_value(column_name) FROM table_name;
+------------------------+
| any_value(column_name) |
+------------------------+
| arbitrary_value        |
+------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct AnyValue {
    signature: Signature,
}

impl Default for AnyValue {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AnyValue {
    fn name(&self) -> &str {
        "any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        not_impl_err!("Not called because return_field is implemented")
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(
            Field::new(self.name(), arg_fields[0].data_type().clone(), true)
                .with_metadata(arg_fields[0].metadata().clone()),
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        TrivialFirstValueAccumulator::try_new(acc_args.return_field.data_type(), true)
            .map(|acc| Box::new(acc) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "any_value"),
                args.return_type().clone(),
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "any_value_is_set"),
                DataType::Boolean,
                true,
            )
            .into(),
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        create_groups_accumulator(args.return_field.data_type())
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn create_groups_accumulator(data_type: &DataType) -> Result<Box<dyn GroupsAccumulator>> {
    macro_rules! instantiate_primitive {
        ($t:ty) => {
            Ok(Box::new(PrimitiveAnyValueGroupsAccumulator::<$t>::new(
                data_type.clone(),
            )) as _)
        };
    }

    match data_type {
        DataType::Int8 => instantiate_primitive!(Int8Type),
        DataType::Int16 => instantiate_primitive!(Int16Type),
        DataType::Int32 => instantiate_primitive!(Int32Type),
        DataType::Int64 => instantiate_primitive!(Int64Type),
        DataType::UInt8 => instantiate_primitive!(UInt8Type),
        DataType::UInt16 => instantiate_primitive!(UInt16Type),
        DataType::UInt32 => instantiate_primitive!(UInt32Type),
        DataType::UInt64 => instantiate_primitive!(UInt64Type),
        DataType::Float16 => instantiate_primitive!(Float16Type),
        DataType::Float32 => instantiate_primitive!(Float32Type),
        DataType::Float64 => instantiate_primitive!(Float64Type),

        DataType::Decimal32(_, _) => instantiate_primitive!(Decimal32Type),
        DataType::Decimal64(_, _) => instantiate_primitive!(Decimal64Type),
        DataType::Decimal128(_, _) => instantiate_primitive!(Decimal128Type),
        DataType::Decimal256(_, _) => instantiate_primitive!(Decimal256Type),

        DataType::Timestamp(TimeUnit::Second, _) => {
            instantiate_primitive!(TimestampSecondType)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            instantiate_primitive!(TimestampMillisecondType)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            instantiate_primitive!(TimestampMicrosecondType)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            instantiate_primitive!(TimestampNanosecondType)
        }

        DataType::Date32 => instantiate_primitive!(Date32Type),
        DataType::Date64 => instantiate_primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => instantiate_primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => {
            instantiate_primitive!(Time32MillisecondType)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            instantiate_primitive!(Time64MicrosecondType)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            instantiate_primitive!(Time64NanosecondType)
        }

        _ => Ok(Box::new(AnyValueGroupsAccumulator::try_new(data_type)?) as _),
    }
}

#[derive(Debug)]
struct PrimitiveAnyValueGroupsAccumulator<T: ArrowPrimitiveType + Send> {
    values: Vec<T::Native>,
    is_set: BooleanBufferBuilder,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType + Send> PrimitiveAnyValueGroupsAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            values: vec![],
            is_set: BooleanBufferBuilder::new(0),
            data_type,
        }
    }

    fn ensure_groups(&mut self, total_num_groups: usize) {
        if self.values.len() < total_num_groups {
            self.values.resize(total_num_groups, T::default_value());
            self.is_set.resize(total_num_groups);
        }
    }

    fn take_state(&mut self, emit_to: EmitTo) -> (Vec<T::Native>, BooleanBuffer) {
        let values = emit_to.take_needed(&mut self.values);
        let is_set = self.is_set.finish();
        match emit_to {
            EmitTo::All => (values, is_set),
            EmitTo::First(n) => {
                let emitted = is_set.slice(0, n);
                self.is_set
                    .append_buffer(&is_set.slice(n, is_set.len() - n));
                (values, emitted)
            }
        }
    }

    fn values_array(
        &self,
        values: Vec<T::Native>,
        is_set: BooleanBuffer,
    ) -> PrimitiveArray<T> {
        PrimitiveArray::<T>::new(values.into(), Some(NullBuffer::new(is_set)))
            .with_data_type(self.data_type.clone())
    }
}

impl<T: ArrowPrimitiveType + Send> GroupsAccumulator
    for PrimitiveAnyValueGroupsAccumulator<T>
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "any_value expects one argument");
        let values = values[0].as_primitive::<T>();
        self.ensure_groups(total_num_groups);

        for (row, &group_index) in group_indices.iter().enumerate() {
            if opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row))
                && !self.is_set.get_bit(group_index)
                && values.is_valid(row)
            {
                self.values[group_index] = values.value(row);
                self.is_set.set_bit(group_index, true);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, is_set) = self.take_state(emit_to);
        Ok(Arc::new(self.values_array(values, is_set)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (values, is_set) = self.take_state(emit_to);
        Ok(vec![
            Arc::new(self.values_array(values, is_set.clone())),
            Arc::new(BooleanArray::new(is_set, None)),
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "any_value expects value and is_set state");
        let state_values = values[0].as_primitive::<T>();
        let is_set = as_boolean_array(&values[1])?;
        self.ensure_groups(total_num_groups);

        for (row, &group_index) in group_indices.iter().enumerate() {
            if is_set.is_valid(row)
                && is_set.value(row)
                && !self.is_set.get_bit(group_index)
                && state_values.is_valid(row)
            {
                self.values[group_index] = state_values.value(row);
                self.is_set.set_bit(group_index, true);
            }
        }
        Ok(())
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "any_value expects one argument");
        let values = &values[0];
        let is_set = BooleanArray::from_iter((0..values.len()).map(|row| {
            values.is_valid(row)
                && opt_filter
                    .is_none_or(|filter| filter.is_valid(row) && filter.value(row))
        }));
        Ok(vec![Arc::clone(values), Arc::new(is_set)])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<T::Native>()
            + self.is_set.capacity() / 8
    }
}

#[derive(Debug)]
struct AnyValueGroupsAccumulator {
    values: Vec<ScalarValue>,
    is_set: BooleanBufferBuilder,
    null_value: ScalarValue,
}

impl AnyValueGroupsAccumulator {
    fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            values: vec![],
            is_set: BooleanBufferBuilder::new(0),
            null_value: ScalarValue::try_from(data_type)?,
        })
    }

    fn ensure_groups(&mut self, total_num_groups: usize) {
        if self.values.len() < total_num_groups {
            self.values
                .resize(total_num_groups, self.null_value.clone());
            self.is_set.resize(total_num_groups);
        }
    }

    fn take_state(&mut self, emit_to: EmitTo) -> (Vec<ScalarValue>, BooleanBuffer) {
        let values = emit_to.take_needed(&mut self.values);
        let is_set = self.is_set.finish();
        match emit_to {
            EmitTo::All => (values, is_set),
            EmitTo::First(n) => {
                let emitted = is_set.slice(0, n);
                self.is_set
                    .append_buffer(&is_set.slice(n, is_set.len() - n));
                (values, emitted)
            }
        }
    }

    fn update_row(
        &mut self,
        values: &ArrayRef,
        group_index: usize,
        row: usize,
    ) -> Result<()> {
        if !self.is_set.get_bit(group_index) && values.is_valid(row) {
            self.values[group_index] = ScalarValue::try_from_array(values, row)?;
            self.is_set.set_bit(group_index, true);
        }
        Ok(())
    }
}

impl GroupsAccumulator for AnyValueGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "any_value expects one argument");
        let values = &values[0];
        self.ensure_groups(total_num_groups);

        for (row, &group_index) in group_indices.iter().enumerate() {
            if opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row)) {
                self.update_row(values, group_index, row)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, _) = self.take_state(emit_to);
        ScalarValue::iter_to_array(values)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (values, is_set) = self.take_state(emit_to);
        Ok(vec![
            ScalarValue::iter_to_array(values)?,
            Arc::new(BooleanArray::new(is_set, None)),
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "any_value expects value and is_set state");
        let is_set = as_boolean_array(&values[1])?;
        self.ensure_groups(total_num_groups);

        for (row, &group_index) in group_indices.iter().enumerate() {
            if is_set.is_valid(row)
                && is_set.value(row)
                && !self.is_set.get_bit(group_index)
            {
                self.values[group_index] = ScalarValue::try_from_array(&values[0], row)?;
                self.is_set.set_bit(group_index, true);
            }
        }
        Ok(())
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "any_value expects one argument");
        let values = &values[0];
        let is_set = BooleanArray::from_iter((0..values.len()).map(|row| {
            values.is_valid(row)
                && opt_filter
                    .is_none_or(|filter| filter.is_valid(row) && filter.value(row))
        }));
        Ok(vec![Arc::clone(values), Arc::new(is_set)])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + ScalarValue::size_of_vec(&self.values)
            + self.is_set.capacity() / 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    #[test]
    fn groups_accumulator_uses_first_non_null_value() -> Result<()> {
        let mut acc = create_groups_accumulator(&DataType::Int64)?;
        let values = Arc::new(Int64Array::from(vec![
            None,
            Some(10),
            Some(11),
            Some(20),
            None,
            Some(30),
        ])) as ArrayRef;
        let filter = BooleanArray::from(vec![true, true, true, false, true, true]);

        acc.update_batch(&[values], &[0, 0, 0, 1, 1, 2], Some(&filter), 4)?;
        let result = acc.evaluate(EmitTo::All)?;
        let expected =
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30), None])) as ArrayRef;
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn groups_accumulator_merges_partial_state() -> Result<()> {
        let mut acc = AnyValueGroupsAccumulator::try_new(&DataType::Utf8)?;
        let values = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]))
            as ArrayRef;
        let is_set = Arc::new(BooleanArray::from(vec![false, true, true])) as ArrayRef;

        acc.merge_batch(&[values, is_set], &[0, 0, 1], 3)?;
        let state = acc.state(EmitTo::All)?;
        let expected_values =
            Arc::new(StringArray::from(vec![Some("b"), Some("c"), None])) as ArrayRef;
        let expected_is_set =
            Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef;
        assert_eq!(&state[0], &expected_values);
        assert_eq!(&state[1], &expected_is_set);
        Ok(())
    }

    #[test]
    fn groups_accumulator_convert_to_state_applies_filter_and_nulls() -> Result<()> {
        let acc = create_groups_accumulator(&DataType::Int64)?;
        let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let filter = BooleanArray::from(vec![true, true, false]);

        let state = acc.convert_to_state(&[Arc::clone(&values)], Some(&filter))?;
        let expected_is_set =
            Arc::new(BooleanArray::from(vec![true, false, false])) as ArrayRef;
        assert_eq!(&state[0], &values);
        assert_eq!(&state[1], &expected_is_set);
        Ok(())
    }

    #[test]
    fn groups_accumulator_emit_first_retains_remaining_groups() -> Result<()> {
        let mut acc = create_groups_accumulator(&DataType::Int64)?;
        let values =
            Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)])) as ArrayRef;
        acc.update_batch(&[values], &[0, 1, 2], None, 3)?;

        let first = acc.evaluate(EmitTo::First(2))?;
        let expected_first =
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        assert_eq!(&first, &expected_first);

        let values = Arc::new(Int64Array::from(vec![Some(31), Some(40)])) as ArrayRef;
        acc.update_batch(&[values], &[0, 1], None, 2)?;
        let remaining = acc.evaluate(EmitTo::All)?;
        let expected_remaining =
            Arc::new(Int64Array::from(vec![Some(30), Some(40)])) as ArrayRef;
        assert_eq!(&remaining, &expected_remaining);
        Ok(())
    }

    #[test]
    fn primitive_groups_accumulator_merges_partial_state() -> Result<()> {
        let mut partial = create_groups_accumulator(&DataType::Int64)?;
        let values = Arc::new(Int64Array::from(vec![Some(10), None, Some(30), Some(40)]))
            as ArrayRef;
        partial.update_batch(&[values], &[0, 1, 2, 2], None, 3)?;
        let state = partial.state(EmitTo::All)?;

        let mut merged = create_groups_accumulator(&DataType::Int64)?;
        merged.merge_batch(&state, &[0, 1, 2], 4)?;
        let result = merged.evaluate(EmitTo::All)?;
        let expected =
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30), None])) as ArrayRef;
        assert_eq!(&result, &expected);
        Ok(())
    }
}
