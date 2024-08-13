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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::hyperloglog::HyperLogLog;
use arrow::array::BinaryArray;
use arrow::array::{
    GenericBinaryArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};
use datafusion_common::ScalarValue;
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, DataFusionError, Result,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
make_udaf_expr_and_func!(
    ApproxDistinct,
    approx_distinct,
    expression,
    "approximate number of distinct input values",
    approx_distinct_udaf
);

impl<T: Hash> From<&HyperLogLog<T>> for ScalarValue {
    fn from(v: &HyperLogLog<T>) -> ScalarValue {
        let values = v.as_ref().to_vec();
        ScalarValue::Binary(Some(values))
    }
}

impl<T: Hash> TryFrom<&[u8]> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &[u8]) -> Result<HyperLogLog<T>> {
        let arr: [u8; 16384] = v.try_into().map_err(|_| {
            DataFusionError::Internal(
                "Impossibly got invalid binary array from states".into(),
            )
        })?;
        Ok(HyperLogLog::<T>::new_with_registers(arr))
    }
}

impl<T: Hash> TryFrom<&ScalarValue> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &ScalarValue) -> Result<HyperLogLog<T>> {
        if let ScalarValue::Binary(Some(slice)) = v {
            slice.as_slice().try_into()
        } else {
            internal_err!(
                "Impossibly got invalid scalar value while converting to HyperLogLog"
            )
        }
    }
}

#[derive(Debug)]
struct NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    hll: HyperLogLog<T::Native>,
}

impl<T> NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
        }
    }
}

#[derive(Debug)]
struct StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    hll: HyperLogLog<String>,
    phantom_data: PhantomData<T>,
}

impl<T> StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
            phantom_data: PhantomData,
        }
    }
}

#[derive(Debug)]
struct BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    hll: HyperLogLog<Vec<u8>>,
    phantom_data: PhantomData<T>,
}

impl<T> BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
            phantom_data: PhantomData,
        }
    }
}

macro_rules! default_accumulator_impl {
    () => {
        fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
            assert_eq!(1, states.len(), "expect only 1 element in the states");
            let binary_array = downcast_value!(states[0], BinaryArray);
            for v in binary_array.iter() {
                let v = v.ok_or_else(|| {
                    DataFusionError::Internal(
                        "Impossibly got empty binary array from states".into(),
                    )
                })?;
                let other = v.try_into()?;
                self.hll.merge(&other);
            }
            Ok(())
        }

        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            let value = ScalarValue::from(&self.hll);
            Ok(vec![value])
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::UInt64(Some(self.hll.count() as u64)))
        }

        fn size(&self) -> usize {
            // HLL has static size
            std::mem::size_of_val(self)
        }
    };
}

impl<T> Accumulator for BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &GenericBinaryArray<T> =
            downcast_value!(values[0], GenericBinaryArray, T);
        // flatten because we would skip nulls
        self.hll
            .extend(array.into_iter().flatten().map(|v| v.to_vec()));
        Ok(())
    }

    default_accumulator_impl!();
}

impl<T> Accumulator for StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &GenericStringArray<T> =
            downcast_value!(values[0], GenericStringArray, T);
        // flatten because we would skip nulls
        self.hll
            .extend(array.into_iter().flatten().map(|i| i.to_string()));
        Ok(())
    }

    default_accumulator_impl!();
}

impl<T> Accumulator for NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType + Debug,
    T::Native: Hash,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &PrimitiveArray<T> = downcast_value!(values[0], PrimitiveArray, T);
        // flatten because we would skip nulls
        self.hll.extend(array.into_iter().flatten());
        Ok(())
    }

    default_accumulator_impl!();
}

impl Debug for ApproxDistinct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinct")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ApproxDistinct {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ApproxDistinct {
    signature: Signature,
}

impl ApproxDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ApproxDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "hll_registers"),
            DataType::Binary,
            false,
        )])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;

        let accumulator: Box<dyn Accumulator> = match data_type {
            // TODO u8, i8, u16, i16 shall really be done using bitmap, not HLL
            // TODO support for boolean (trivial case)
            // https://github.com/apache/datafusion/issues/1109
            DataType::UInt8 => Box::new(NumericHLLAccumulator::<UInt8Type>::new()),
            DataType::UInt16 => Box::new(NumericHLLAccumulator::<UInt16Type>::new()),
            DataType::UInt32 => Box::new(NumericHLLAccumulator::<UInt32Type>::new()),
            DataType::UInt64 => Box::new(NumericHLLAccumulator::<UInt64Type>::new()),
            DataType::Int8 => Box::new(NumericHLLAccumulator::<Int8Type>::new()),
            DataType::Int16 => Box::new(NumericHLLAccumulator::<Int16Type>::new()),
            DataType::Int32 => Box::new(NumericHLLAccumulator::<Int32Type>::new()),
            DataType::Int64 => Box::new(NumericHLLAccumulator::<Int64Type>::new()),
            DataType::Utf8 => Box::new(StringHLLAccumulator::<i32>::new()),
            DataType::LargeUtf8 => Box::new(StringHLLAccumulator::<i64>::new()),
            DataType::Binary => Box::new(BinaryHLLAccumulator::<i32>::new()),
            DataType::LargeBinary => Box::new(BinaryHLLAccumulator::<i64>::new()),
            other => {
                return not_impl_err!(
                "Support for 'approx_distinct' for data type {other} is not implemented"
            )
            }
        };
        Ok(accumulator)
    }
}
