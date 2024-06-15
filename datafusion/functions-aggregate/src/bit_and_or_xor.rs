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

//! Defines BitAnd, BitOr, and BitXor Aggregate accumulators

use std::any::Any;
use std::collections::HashSet;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    ArrowNativeType, ArrowNumericType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_schema::Field;

use datafusion_common::{exec_err, not_impl_err, Result, ScalarValue};
use datafusion_common::cast::as_list_array;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::INTEGERS;
use datafusion_expr::utils::format_state_name;

macro_rules! downcast_logical {
    ($args:ident, $helper:ident) => {
        match $args.data_type {
            DataType::Int8 => $helper!(Int8Type, $args.data_type),
            DataType::Int16 => $helper!(Int16Type, $args.data_type),
            DataType::Int32 => $helper!(Int32Type, $args.data_type),
            DataType::Int64 => $helper!(Int64Type, $args.data_type),
            DataType::UInt8 => $helper!(UInt8Type, $args.data_type),
            DataType::UInt16 => $helper!(UInt16Type, $args.data_type),
            DataType::UInt32  => $helper!(UInt32Type, $args.data_type),
            DataType::UInt64 => $helper!(UInt64Type, $args.data_type),
            _ => {
                not_impl_err!("not supported for {}: {}", $args.name, $args.data_type)
            }
        }
    };
}

make_udaf_expr_and_func!(
    BitAnd,
    bit_and,
    expression,
    "Returns the bit wise AND of a group of values.",
    bit_and_udaf
);

make_udaf_expr_and_func!(
    BitOr,
    bit_or,
    expression,
    "Returns the bit wise OR of a group of values.",
    bit_or_udaf
);

make_udaf_expr_and_func!(
    BitXor,
    bit_xor,
    expression,
    "Returns the bit wise XOR of a group of values.",
    bit_xor_udaf
);

#[derive(Debug)]
pub struct BitAnd {
    signature: Signature,
}

impl BitAnd {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, INTEGERS.to_vec(), Volatility::Immutable)
        }
    }
}

impl Default for BitAnd {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for BitAnd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];
        if !is_bit_and_or_xor_support_arg_type(arg_type) {
            return exec_err!("[return_type] AND not supported for {}", arg_type)
        }
        return Ok(arg_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(BitAndAccumulator::<$t>::default()))
                };
        }
        downcast_logical!(acc_args, helper)
    }
}

struct BitAndAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitAndAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitAndAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitAndAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitAndAccumulator<T>
    where
        T::Native: std::ops::BitAnd<Output=T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_and(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(x);
            *v = *v & x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

#[derive(Debug)]
pub struct BitOr {
    signature: Signature,
}

impl BitOr {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, INTEGERS.to_vec(), Volatility::Immutable)
        }
    }
}

impl Default for BitOr {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for BitOr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_or"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];
        if !is_bit_and_or_xor_support_arg_type(arg_type) {
            return exec_err!("[return_type] OR not supported for {}", arg_type)
        }
        return Ok(arg_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(BitOrAccumulator::<$t>::default()))
                };
        }
        downcast_logical!(acc_args, helper)
    }
}

struct BitOrAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitOrAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitOrAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitOrAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitOrAccumulator<T>
    where
        T::Native: std::ops::BitOr<Output=T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_or(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v | x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

#[derive(Debug)]
pub struct BitXor {
    signature: Signature,
}

impl BitXor {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, INTEGERS.to_vec(), Volatility::Immutable)
        }
    }
}

impl Default for BitXor {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for BitXor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_xor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];
        if !is_bit_and_or_xor_support_arg_type(arg_type) {
            return exec_err!("[return_type] XOR not supported for {}", arg_type)
        }
        return Ok(arg_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {

        if acc_args.is_distinct {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(DistinctBitXorAccumulator::<$t>::default()))
                };
            }
            downcast_logical!(acc_args, helper)
        } else {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(BitXorAccumulator::<$t>::default()))
                };
            }
            downcast_logical!(acc_args, helper)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "xor distinct"),
                Field::new("item", args.return_type.clone(), true),
                false,
            )])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, "xor"),
                args.return_type.clone(),
                true,
            )])
        }
    }

}

struct BitXorAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitXorAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitXorAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitXorAccumulator<T>
    where
        T::Native: std::ops::BitXor<Output=T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_xor(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v ^ x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

struct DistinctBitXorAccumulator<T: ArrowNumericType> {
    values: HashSet<T::Native, RandomState>,
}

impl<T: ArrowNumericType> std::fmt::Debug for DistinctBitXorAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctBitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for DistinctBitXorAccumulator<T> {
    fn default() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T: ArrowNumericType> Accumulator for DistinctBitXorAccumulator<T>
    where
        T::Native: std::ops::BitXor<Output = T::Native> + std::hash::Hash + Eq,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(array.value(idx));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(*x);
            }),
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc ^ *distinct_value;
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<T::Native>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let values = self
                .values
                .iter()
                .map(|x| ScalarValue::new_primitive::<T>(Some(*x), &T::DATA_TYPE))
                .collect::<Result<Vec<_>>>()?;

            let arr = ScalarValue::new_list(&values, &T::DATA_TYPE);
            vec![ScalarValue::List(arr)]
        };
        Ok(state_out)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(state) = states.first() {
            let list_arr = as_list_array(state)?;
            for arr in list_arr.iter().flatten() {
                self.update_batch(&[arr])?;
            }
        }
        Ok(())
    }
}

fn is_bit_and_or_xor_support_arg_type(arg_type: &DataType) -> bool {
    INTEGERS.contains(arg_type)
}