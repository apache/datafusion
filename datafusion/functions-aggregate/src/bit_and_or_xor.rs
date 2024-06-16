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

//! Defines `BitAnd`, `BitOr`, `BitXor` and `BitXor DISTINCT` aggregate accumulators

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    ArrowNativeType, ArrowNumericType, ArrowPrimitiveType, DataType, Int16Type,
    Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_schema::Field;

use datafusion_common::cast::as_list_array;
use datafusion_common::{exec_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::INTEGERS;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

/// `accumulator_helper` is a macro accepting ([ArrowPrimitiveType], [BitwiseOperationType])
macro_rules! accumulator_helper {
    ($t:ty, $opr:expr) => {
        match $opr {
            BitwiseOperationType::And => Ok(Box::<BitAndAccumulator<$t>>::default()),
            BitwiseOperationType::Or => Ok(Box::<BitOrAccumulator<$t>>::default()),
            BitwiseOperationType::Xor => Ok(Box::<BitXorAccumulator<$t>>::default()),
            BitwiseOperationType::XorDistinct => {
                Ok(Box::<DistinctBitXorAccumulator<$t>>::default())
            }
        }
    };
}

/// AND, OR and XOR only supports a subset of numeric types
///
/// `args` is [AccumulatorArgs]
/// `opr` is [BitwiseOperationType]
macro_rules! downcast_bitwise_accumulator {
    ($args:ident, $opr:expr) => {
        match $args.data_type {
            DataType::Int8 => accumulator_helper!(Int8Type, $opr),
            DataType::Int16 => accumulator_helper!(Int16Type, $opr),
            DataType::Int32 => accumulator_helper!(Int32Type, $opr),
            DataType::Int64 => accumulator_helper!(Int64Type, $opr),
            DataType::UInt8 => accumulator_helper!(UInt8Type, $opr),
            DataType::UInt16 => accumulator_helper!(UInt16Type, $opr),
            DataType::UInt32 => accumulator_helper!(UInt32Type, $opr),
            DataType::UInt64 => accumulator_helper!(UInt64Type, $opr),
            _ => {
                not_impl_err!(
                    "{} not supported for {}: {}",
                    stringify!($opr),
                    $args.name,
                    $args.data_type
                )
            }
        }
    };
}

/// Simplifies the creation of User-Defined Aggregate Functions (UDAFs) for performing bitwise operations in a declarative manner.
///
/// `EXPR_FN` identifier used to name the generated expression function.
/// `AGGREGATE_UDF_FN` is an identifier used to name the underlying UDAF function.
/// `OPR_TYPE` is an expression that evaluates to the type of bitwise operation to be performed.
macro_rules! make_bitwise_udaf_expr_and_func {
    ($EXPR_FN:ident, $AGGREGATE_UDF_FN:ident, $OPR_TYPE:expr) => {
        make_udaf_expr!($EXPR_FN, expr_y expr_x, concat!("Returns the bitwise", stringify!($OPR_TYPE), "of a group of values"), $AGGREGATE_UDF_FN);
        create_func!($EXPR_FN, $AGGREGATE_UDF_FN, BitwiseOperation::new($OPR_TYPE, stringify!($EXPR_FN)));
    }
}

make_bitwise_udaf_expr_and_func!(bit_and, bit_and_udaf, BitwiseOperationType::And);
make_bitwise_udaf_expr_and_func!(bit_or, bit_or_udaf, BitwiseOperationType::Or);
make_bitwise_udaf_expr_and_func!(bit_xor, bit_xor_udaf, BitwiseOperationType::Xor);

/// The different types of bitwise operations that can be performed.
#[derive(Debug, Clone, Eq, PartialEq)]
enum BitwiseOperationType {
    And,
    Or,
    Xor,
    /// `XorDistinct` is a variation of the bitwise XOR operation specifically for the scenario of BitXor DISTINCT
    XorDistinct,
}

impl Display for BitwiseOperationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// [BitwiseOperation] struct encapsulates information about a bitwise operation.
#[derive(Debug)]
struct BitwiseOperation {
    signature: Signature,
    /// `operation` indicates the type of bitwise operation to be performed.
    operation: BitwiseOperationType,
    func_name: &'static str,
}

impl BitwiseOperation {
    pub fn new(operator: BitwiseOperationType, func_name: &'static str) -> Self {
        Self {
            operation: operator,
            signature: Signature::uniform(1, INTEGERS.to_vec(), Volatility::Immutable),
            func_name,
        }
    }
}

impl AggregateUDFImpl for BitwiseOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.func_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];
        if !is_bit_and_or_xor_support_arg_type(arg_type) {
            return exec_err!(
                "[return_type] {} not supported for {}",
                self.operation.to_string(),
                arg_type
            );
        }
        Ok(arg_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct && self.operation == BitwiseOperationType::Xor {
            downcast_bitwise_accumulator!(acc_args, BitwiseOperationType::XorDistinct)
        } else {
            downcast_bitwise_accumulator!(acc_args, self.operation)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if self.operation == BitwiseOperationType::Xor && args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "xor distinct"),
                Field::new("item", args.return_type.clone(), true),
                false,
            )])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, self.operation.to_string().as_str()),
                args.return_type.clone(),
                true,
            )])
        }
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
    T::Native: std::ops::BitAnd<Output = T::Native>,
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
    T::Native: std::ops::BitOr<Output = T::Native>,
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
    T::Native: std::ops::BitXor<Output = T::Native>,
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
