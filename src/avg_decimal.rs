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

use arrow::{array::BooleanBufferBuilder, buffer::NullBuffer, compute::sum};
use arrow_array::{
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{Decimal128Type, Int64Type},
    Array, ArrayRef, Decimal128Array, Int64Array, PrimitiveArray,
};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator, Signature};
use datafusion_common::{not_impl_err, Result, ScalarValue};
use datafusion_physical_expr::expressions::format_state_name;
use std::{any::Any, sync::Arc};

use crate::utils::is_valid_decimal_precision;
use arrow_array::ArrowNativeTypeOp;
use arrow_data::decimal::{MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::avg_return_type;
use datafusion_expr::{AggregateUDFImpl, ReversedUDAF};
use num::{integer::div_ceil, Integer};
use DataType::*;

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct AvgDecimal {
    signature: Signature,
    sum_data_type: DataType,
    result_data_type: DataType,
}

impl AvgDecimal {
    /// Create a new AVG aggregate function
    pub fn new(result_type: DataType, sum_type: DataType) -> Self {
        Self {
            signature: Signature::user_defined(Immutable),
            result_data_type: result_type,
            sum_data_type: sum_type,
        }
    }
}

impl AggregateUDFImpl for AvgDecimal {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match (&self.sum_data_type, &self.result_data_type) {
            (Decimal128(sum_precision, sum_scale), Decimal128(target_precision, target_scale)) => {
                Ok(Box::new(AvgDecimalAccumulator::new(
                    *sum_scale,
                    *sum_precision,
                    *target_precision,
                    *target_scale,
                )))
            }
            _ => not_impl_err!(
                "AvgDecimalAccumulator for ({} --> {})",
                self.sum_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(self.name(), "sum"),
                self.sum_data_type.clone(),
                true,
            ),
            Field::new(
                format_state_name(self.name(), "count"),
                DataType::Int64,
                true,
            ),
        ])
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.sum_data_type, &self.result_data_type) {
            (Decimal128(sum_precision, sum_scale), Decimal128(target_precision, target_scale)) => {
                Ok(Box::new(AvgDecimalGroupsAccumulator::new(
                    &self.result_data_type,
                    &self.sum_data_type,
                    *target_precision,
                    *target_scale,
                    *sum_precision,
                    *sum_scale,
                )))
            }
            _ => not_impl_err!(
                "AvgDecimalGroupsAccumulator for ({} --> {})",
                self.sum_data_type,
                self.result_data_type
            ),
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        match &self.result_data_type {
            Decimal128(target_precision, target_scale) => {
                Ok(make_decimal128(None, *target_precision, *target_scale))
            }
            _ => not_impl_err!(
                "The result_data_type of AvgDecimal should be Decimal128 but got{}",
                self.result_data_type
            ),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }
}

/// An accumulator to compute the average for decimals
#[derive(Debug)]
struct AvgDecimalAccumulator {
    sum: Option<i128>,
    count: i64,
    is_empty: bool,
    is_not_null: bool,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
}

impl AvgDecimalAccumulator {
    pub fn new(sum_scale: i8, sum_precision: u8, target_precision: u8, target_scale: i8) -> Self {
        Self {
            sum: None,
            count: 0,
            is_empty: true,
            is_not_null: true,
            sum_scale,
            sum_precision,
            target_precision,
            target_scale,
        }
    }

    fn update_single(&mut self, values: &Decimal128Array, idx: usize) {
        let v = unsafe { values.value_unchecked(idx) };
        let (new_sum, is_overflow) = match self.sum {
            Some(sum) => sum.overflowing_add(v),
            None => (v, false),
        };

        if is_overflow || !is_valid_decimal_precision(new_sum, self.sum_precision) {
            // Overflow: set buffer accumulator to null
            self.is_not_null = false;
            return;
        }

        self.sum = Some(new_sum);

        if let Some(new_count) = self.count.checked_add(1) {
            self.count = new_count;
        } else {
            self.is_not_null = false;
            return;
        }

        self.is_not_null = true;
    }
}

fn make_decimal128(value: Option<i128>, precision: u8, scale: i8) -> ScalarValue {
    ScalarValue::Decimal128(value, precision, scale)
}

impl Accumulator for AvgDecimalAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Decimal128(self.sum, self.sum_precision, self.sum_scale),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_empty && !self.is_not_null {
            // This means there's a overflow in decimal, so we will just skip the rest
            // of the computation
            return Ok(());
        }

        let values = &values[0];
        let data = values.as_primitive::<Decimal128Type>();

        self.is_empty = self.is_empty && values.len() == values.null_count();

        if values.null_count() == 0 {
            for i in 0..data.len() {
                self.update_single(data, i);
            }
        } else {
            for i in 0..data.len() {
                if data.is_null(i) {
                    continue;
                }
                self.update_single(data, i);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[1].as_primitive::<Int64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[0].as_primitive::<Decimal128Type>()) {
            let v = self.sum.get_or_insert(0);
            let (result, overflowed) = v.overflowing_add(x);
            if overflowed {
                // Set to None if overflow happens
                self.sum = None;
            } else {
                *v = result;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let scaler = 10_i128.pow(self.target_scale.saturating_sub(self.sum_scale) as u32);
        let target_min = MIN_DECIMAL_FOR_EACH_PRECISION[self.target_precision as usize - 1];
        let target_max = MAX_DECIMAL_FOR_EACH_PRECISION[self.target_precision as usize - 1];

        let result = self
            .sum
            .map(|v| avg(v, self.count as i128, target_min, target_max, scaler));

        match result {
            Some(value) => Ok(make_decimal128(
                value,
                self.target_precision,
                self.target_scale,
            )),
            _ => Ok(make_decimal128(
                None,
                self.target_precision,
                self.target_scale,
            )),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct AvgDecimalGroupsAccumulator {
    /// Tracks if the value is null
    is_not_null: BooleanBufferBuilder,

    // Tracks if the value is empty
    is_empty: BooleanBufferBuilder,

    /// The type of the avg return type
    return_data_type: DataType,
    target_precision: u8,
    target_scale: i8,

    /// Count per group (use i64 to make Int64Array)
    counts: Vec<i64>,

    /// Sums per group, stored as i128
    sums: Vec<i128>,

    /// The type of the sum
    sum_data_type: DataType,
    /// This is input_precision + 10 to be consistent with Spark
    sum_precision: u8,
    sum_scale: i8,
}

impl AvgDecimalGroupsAccumulator {
    pub fn new(
        return_data_type: &DataType,
        sum_data_type: &DataType,
        target_precision: u8,
        target_scale: i8,
        sum_precision: u8,
        sum_scale: i8,
    ) -> Self {
        Self {
            is_not_null: BooleanBufferBuilder::new(0),
            is_empty: BooleanBufferBuilder::new(0),
            return_data_type: return_data_type.clone(),
            target_precision,
            target_scale,
            sum_data_type: sum_data_type.clone(),
            sum_precision,
            sum_scale,
            counts: vec![],
            sums: vec![],
        }
    }

    fn is_overflow(&self, index: usize) -> bool {
        !self.is_empty.get_bit(index) && !self.is_not_null.get_bit(index)
    }

    fn update_single(&mut self, group_index: usize, value: i128) {
        if self.is_overflow(group_index) {
            // This means there's a overflow in decimal, so we will just skip the rest
            // of the computation
            return;
        }

        self.is_empty.set_bit(group_index, false);
        let (new_sum, is_overflow) = self.sums[group_index].overflowing_add(value);
        self.counts[group_index] += 1;

        if is_overflow || !is_valid_decimal_precision(new_sum, self.sum_precision) {
            // Overflow: set buffer accumulator to null
            self.is_not_null.set_bit(group_index, false);
            return;
        }

        self.sums[group_index] = new_sum;
        self.is_not_null.set_bit(group_index, true)
    }
}

fn ensure_bit_capacity(builder: &mut BooleanBufferBuilder, capacity: usize) {
    if builder.len() < capacity {
        let additional = capacity - builder.len();
        builder.append_n(additional, true);
    }
}

impl GroupsAccumulator for AvgDecimalGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Decimal128Type>();
        let data = values.values();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, 0);
        ensure_bit_capacity(&mut self.is_empty, total_num_groups);
        ensure_bit_capacity(&mut self.is_not_null, total_num_groups);

        let iter = group_indices.iter().zip(data.iter());
        if values.null_count() == 0 {
            for (&group_index, &value) in iter {
                self.update_single(group_index, value);
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if values.is_null(idx) {
                    continue;
                }
                self.update_single(group_index, value);
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<Decimal128Type>();
        let partial_counts = values[1].as_primitive::<Int64Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        let iter1 = group_indices.iter().zip(partial_counts.values().iter());
        for (&group_index, &partial_count) in iter1 {
            self.counts[group_index] += partial_count;
        }

        // update sums
        self.sums.resize(total_num_groups, 0);
        let iter2 = group_indices.iter().zip(partial_sums.values().iter());
        for (&group_index, &new_value) in iter2 {
            let sum = &mut self.sums[group_index];
            *sum = sum.add_wrapping(new_value);
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);

        let mut builder = PrimitiveBuilder::<Decimal128Type>::with_capacity(sums.len())
            .with_data_type(self.return_data_type.clone());
        let iter = sums.into_iter().zip(counts);

        let scaler = 10_i128.pow(self.target_scale.saturating_sub(self.sum_scale) as u32);
        let target_min = MIN_DECIMAL_FOR_EACH_PRECISION[self.target_precision as usize - 1];
        let target_max = MAX_DECIMAL_FOR_EACH_PRECISION[self.target_precision as usize - 1];

        for (sum, count) in iter {
            if count != 0 {
                match avg(sum, count as i128, target_min, target_max, scaler) {
                    Some(value) => {
                        builder.append_value(value);
                    }
                    _ => {
                        builder.append_null();
                    }
                }
            } else {
                builder.append_null();
            }
        }
        let array: PrimitiveArray<Decimal128Type> = builder.finish();

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.is_not_null.finish();
        let nulls = Some(NullBuffer::new(nulls));

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = Int64Array::new(counts.into(), nulls.clone());

        let sums = emit_to.take_needed(&mut self.sums);
        let sums =
            Decimal128Array::new(sums.into(), nulls).with_data_type(self.sum_data_type.clone());

        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<i64>()
            + self.sums.capacity() * std::mem::size_of::<i128>()
    }
}

/// Returns the `sum`/`count` as a i128 Decimal128 with
/// target_scale and target_precision and return None if overflows.
///
/// * sum: The total sum value stored as Decimal128 with sum_scale
/// * count: total count, stored as a i128 (*NOT* a Decimal128 value)
/// * target_min: The minimum output value possible to represent with the target precision
/// * target_max: The maximum output value possible to represent with the target precision
/// * scaler: scale factor for avg
#[inline(always)]
fn avg(sum: i128, count: i128, target_min: i128, target_max: i128, scaler: i128) -> Option<i128> {
    if let Some(value) = sum.checked_mul(scaler) {
        // `sum / count` with ROUND_HALF_UP
        let (div, rem) = value.div_rem(&count);
        let half = div_ceil(count, 2);
        let half_neg = half.neg_wrapping();
        let new_value = match value >= 0 {
            true if rem >= half => div.add_wrapping(1),
            false if rem <= half_neg => div.sub_wrapping(1),
            _ => div,
        };
        if new_value >= target_min && new_value <= target_max {
            Some(new_value)
        } else {
            None
        }
    } else {
        None
    }
}
