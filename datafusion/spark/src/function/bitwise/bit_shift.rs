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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::compute;
use arrow::datatypes::{
    ArrowNativeType, DataType, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};

/// Performs a bitwise left shift on each element of the `value` array by the corresponding amount in the `shift` array.
/// The shift amount is normalized to the bit width of the type, matching Spark/Java semantics for negative and large shifts.
///
/// # Arguments
/// * `value` - The array of values to shift.
/// * `shift` - The array of shift amounts (must be Int32).
///
/// # Returns
/// A new array with the shifted values.
fn shift_left<T: ArrowPrimitiveType>(
    value: &PrimitiveArray<T>,
    shift: &PrimitiveArray<Int32Type>,
) -> Result<PrimitiveArray<T>>
where
    T::Native: ArrowNativeType + std::ops::Shl<i32, Output = T::Native>,
{
    let bit_num = (T::Native::get_byte_width() * 8) as i32;
    let result = compute::binary::<_, Int32Type, _, _>(
        value,
        shift,
        |value: T::Native, shift: i32| {
            let shift = ((shift % bit_num) + bit_num) % bit_num;
            value << shift
        },
    )?;
    Ok(result)
}

/// Performs a bitwise right shift on each element of the `value` array by the corresponding amount in the `shift` array.
/// The shift amount is normalized to the bit width of the type, matching Spark/Java semantics for negative and large shifts.
///
/// # Arguments
/// * `value` - The array of values to shift.
/// * `shift` - The array of shift amounts (must be Int32).
///
/// # Returns
/// A new array with the shifted values.
fn shift_right<T: ArrowPrimitiveType>(
    value: &PrimitiveArray<T>,
    shift: &PrimitiveArray<Int32Type>,
) -> Result<PrimitiveArray<T>>
where
    T::Native: ArrowNativeType + std::ops::Shr<i32, Output = T::Native>,
{
    let bit_num = (T::Native::get_byte_width() * 8) as i32;
    let result = compute::binary::<_, Int32Type, _, _>(
        value,
        shift,
        |value: T::Native, shift: i32| {
            let shift = ((shift % bit_num) + bit_num) % bit_num;
            value >> shift
        },
    )?;
    Ok(result)
}

/// Trait for performing an unsigned right shift (logical shift right).
/// This is used to mimic Java's `>>>` operator, which does not exist in Rust.
/// For unsigned types, this is just the normal right shift.
/// For signed types, this casts to the unsigned type, shifts, then casts back.
trait UShr<Rhs> {
    fn ushr(self, rhs: Rhs) -> Self;
}

impl UShr<i32> for u32 {
    fn ushr(self, rhs: i32) -> Self {
        self >> rhs
    }
}

impl UShr<i32> for u64 {
    fn ushr(self, rhs: i32) -> Self {
        self >> rhs
    }
}

impl UShr<i32> for i32 {
    fn ushr(self, rhs: i32) -> Self {
        ((self as u32) >> rhs) as i32
    }
}

impl UShr<i32> for i64 {
    fn ushr(self, rhs: i32) -> Self {
        ((self as u64) >> rhs) as i64
    }
}

/// Performs a bitwise unsigned right shift on each element of the `value` array by the corresponding amount in the `shift` array.
/// The shift amount is normalized to the bit width of the type, matching Spark/Java semantics for negative and large shifts.
///
/// # Arguments
/// * `value` - The array of values to shift.
/// * `shift` - The array of shift amounts (must be Int32).
///
/// # Returns
/// A new array with the shifted values.
fn shift_right_unsigned<T: ArrowPrimitiveType>(
    value: &PrimitiveArray<T>,
    shift: &PrimitiveArray<Int32Type>,
) -> Result<PrimitiveArray<T>>
where
    T::Native: ArrowNativeType + UShr<i32>,
{
    let bit_num = (T::Native::get_byte_width() * 8) as i32;
    let result = compute::binary::<_, Int32Type, _, _>(
        value,
        shift,
        |value: T::Native, shift: i32| {
            let shift = ((shift % bit_num) + bit_num) % bit_num;
            value.ushr(shift)
        },
    )?;
    Ok(result)
}

trait BitShiftUDF: ScalarUDFImpl {
    fn shift<T: ArrowPrimitiveType>(
        &self,
        value: &PrimitiveArray<T>,
        shift: &PrimitiveArray<Int32Type>,
    ) -> Result<PrimitiveArray<T>>
    where
        T::Native: ArrowNativeType
            + std::ops::Shl<i32, Output = T::Native>
            + std::ops::Shr<i32, Output = T::Native>
            + UShr<i32>;

    fn spark_shift(&self, arrays: &[ArrayRef]) -> Result<ArrayRef> {
        let value_array = arrays[0].as_ref();
        let shift_array = arrays[1].as_ref();

        // Ensure shift array is Int32
        let shift_array = if shift_array.data_type() != &DataType::Int32 {
            return plan_err!("{} shift amount must be Int32", self.name());
        } else {
            shift_array.as_primitive::<Int32Type>()
        };

        match value_array.data_type() {
            DataType::Int32 => {
                let value_array = value_array.as_primitive::<Int32Type>();
                Ok(Arc::new(self.shift(value_array, shift_array)?))
            }
            DataType::Int64 => {
                let value_array = value_array.as_primitive::<Int64Type>();
                Ok(Arc::new(self.shift(value_array, shift_array)?))
            }
            DataType::UInt32 => {
                let value_array = value_array.as_primitive::<UInt32Type>();
                Ok(Arc::new(self.shift(value_array, shift_array)?))
            }
            DataType::UInt64 => {
                let value_array = value_array.as_primitive::<UInt64Type>();
                Ok(Arc::new(self.shift(value_array, shift_array)?))
            }
            _ => {
                plan_err!(
                    "{} function does not support data type: {}",
                    self.name(),
                    value_array.data_type()
                )
            }
        }
    }
}

fn bit_shift_coerce_types(arg_types: &[DataType], func: &str) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return Err(invalid_arg_count_exec_err(func, (2, 2), arg_types.len()));
    }
    if !arg_types[0].is_integer() && !arg_types[0].is_null() {
        return Err(unsupported_data_type_exec_err(
            func,
            "Integer Type",
            &arg_types[0],
        ));
    }
    if !arg_types[1].is_integer() && !arg_types[1].is_null() {
        return Err(unsupported_data_type_exec_err(
            func,
            "Integer Type",
            &arg_types[1],
        ));
    }

    // Coerce smaller integer types to Int32
    let coerced_first = match &arg_types[0] {
        DataType::Int8 | DataType::Int16 | DataType::Null => DataType::Int32,
        DataType::UInt8 | DataType::UInt16 => DataType::UInt32,
        _ => arg_types[0].clone(),
    };

    Ok(vec![coerced_first, DataType::Int32])
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkShiftLeft {
    signature: Signature,
}

impl Default for SparkShiftLeft {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShiftLeft {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl BitShiftUDF for SparkShiftLeft {
    fn shift<T: ArrowPrimitiveType>(
        &self,
        value: &PrimitiveArray<T>,
        shift: &PrimitiveArray<Int32Type>,
    ) -> Result<PrimitiveArray<T>>
    where
        T::Native: ArrowNativeType
            + std::ops::Shl<i32, Output = T::Native>
            + std::ops::Shr<i32, Output = T::Native>
            + UShr<i32>,
    {
        shift_left(value, shift)
    }
}

impl ScalarUDFImpl for SparkShiftLeft {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shiftleft"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        bit_shift_coerce_types(arg_types, "shiftleft")
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("shiftleft expects exactly 2 arguments");
        }
        // Return type is the same as the first argument (the value to shift)
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("shiftleft expects exactly 2 arguments");
        }
        let inner = |arr: &[ArrayRef]| -> Result<ArrayRef> { self.spark_shift(arr) };
        make_scalar_function(inner, vec![])(&args.args)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkShiftRightUnsigned {
    signature: Signature,
}

impl Default for SparkShiftRightUnsigned {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShiftRightUnsigned {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl BitShiftUDF for SparkShiftRightUnsigned {
    fn shift<T: ArrowPrimitiveType>(
        &self,
        value: &PrimitiveArray<T>,
        shift: &PrimitiveArray<Int32Type>,
    ) -> Result<PrimitiveArray<T>>
    where
        T::Native: ArrowNativeType
            + std::ops::Shl<i32, Output = T::Native>
            + std::ops::Shr<i32, Output = T::Native>
            + UShr<i32>,
    {
        shift_right_unsigned(value, shift)
    }
}

impl ScalarUDFImpl for SparkShiftRightUnsigned {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shiftrightunsigned"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        bit_shift_coerce_types(arg_types, "shiftrightunsigned")
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("shiftrightunsigned expects exactly 2 arguments");
        }
        // Return type is the same as the first argument (the value to shift)
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("shiftrightunsigned expects exactly 2 arguments");
        }
        let inner = |arr: &[ArrayRef]| -> Result<ArrayRef> { self.spark_shift(arr) };
        make_scalar_function(inner, vec![])(&args.args)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkShiftRight {
    signature: Signature,
}

impl Default for SparkShiftRight {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShiftRight {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl BitShiftUDF for SparkShiftRight {
    fn shift<T: ArrowPrimitiveType>(
        &self,
        value: &PrimitiveArray<T>,
        shift: &PrimitiveArray<Int32Type>,
    ) -> Result<PrimitiveArray<T>>
    where
        T::Native: ArrowNativeType
            + std::ops::Shl<i32, Output = T::Native>
            + std::ops::Shr<i32, Output = T::Native>
            + UShr<i32>,
    {
        shift_right(value, shift)
    }
}

impl ScalarUDFImpl for SparkShiftRight {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shiftright"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        bit_shift_coerce_types(arg_types, "shiftright")
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("shiftright expects exactly 2 arguments");
        }
        // Return type is the same as the first argument (the value to shift)
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("shiftright expects exactly 2 arguments");
        }
        let inner = |arr: &[ArrayRef]| -> Result<ArrayRef> { self.spark_shift(arr) };
        make_scalar_function(inner, vec![])(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, UInt32Array, UInt64Array};

    #[test]
    fn test_shift_right_unsigned_int32() {
        let value_array = Arc::new(Int32Array::from(vec![4, 8, 16, 32]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 4 >>> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >>> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >>> 3 = 2
        assert_eq!(arr.value(3), 2); // 32 >>> 4 = 2
    }

    #[test]
    fn test_shift_right_unsigned_int64() {
        let value_array = Arc::new(Int64Array::from(vec![4i64, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 2); // 4 >>> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >>> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >>> 3 = 2
    }

    #[test]
    fn test_shift_right_unsigned_uint32() {
        let value_array = Arc::new(UInt32Array::from(vec![4u32, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt32Type>();
        assert_eq!(arr.value(0), 2); // 4 >>> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >>> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >>> 3 = 2
    }

    #[test]
    fn test_shift_right_unsigned_uint64() {
        let value_array = Arc::new(UInt64Array::from(vec![4u64, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt64Type>();
        assert_eq!(arr.value(0), 2); // 4 >>> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >>> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >>> 3 = 2
    }

    #[test]
    fn test_shift_right_unsigned_nulls() {
        let value_array = Arc::new(Int32Array::from(vec![Some(4), None, Some(8)]));
        let shift_array = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 4 >>> 1 = 2
        assert!(arr.is_null(1)); // null >>> 2 = null
        assert!(arr.is_null(2)); // 8 >>> null = null
    }

    #[test]
    fn test_shift_right_unsigned_negative_shift() {
        let value_array = Arc::new(Int32Array::from(vec![4, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![-1, -2, -3]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 4 >>> -1 = 0
        assert_eq!(arr.value(1), 0); // 8 >>> -2 = 0
        assert_eq!(arr.value(2), 0); // 16 >>> -3 = 0
    }

    #[test]
    fn test_shift_right_unsigned_negative_values() {
        let value_array = Arc::new(Int32Array::from(vec![-4, -8, -16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRightUnsigned::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        // For unsigned right shift, negative values are treated as large positive values
        // -4 as u32 = 4294967292, -4 >>> 1 = 2147483646
        assert_eq!(arr.value(0), 2147483646);
        // -8 as u32 = 4294967288, -8 >>> 2 = 1073741822
        assert_eq!(arr.value(1), 1073741822);
        // -16 as u32 = 4294967280, -16 >>> 3 = 536870910
        assert_eq!(arr.value(2), 536870910);
    }

    #[test]
    fn test_shift_right_int32() {
        let value_array = Arc::new(Int32Array::from(vec![4, 8, 16, 32]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 4 >> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >> 3 = 2
        assert_eq!(arr.value(3), 2); // 32 >> 4 = 2
    }

    #[test]
    fn test_shift_right_int64() {
        let value_array = Arc::new(Int64Array::from(vec![4i64, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 2); // 4 >> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >> 3 = 2
    }

    #[test]
    fn test_shift_right_uint32() {
        let value_array = Arc::new(UInt32Array::from(vec![4u32, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt32Type>();
        assert_eq!(arr.value(0), 2); // 4 >> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >> 3 = 2
    }

    #[test]
    fn test_shift_right_uint64() {
        let value_array = Arc::new(UInt64Array::from(vec![4u64, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt64Type>();
        assert_eq!(arr.value(0), 2); // 4 >> 1 = 2
        assert_eq!(arr.value(1), 2); // 8 >> 2 = 2
        assert_eq!(arr.value(2), 2); // 16 >> 3 = 2
    }

    #[test]
    fn test_shift_right_nulls() {
        let value_array = Arc::new(Int32Array::from(vec![Some(4), None, Some(8)]));
        let shift_array = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 4 >> 1 = 2
        assert!(arr.is_null(1)); // null >> 2 = null
        assert!(arr.is_null(2)); // 8 >> null = null
    }

    #[test]
    fn test_shift_right_large_shift() {
        let value_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let shift_array = Arc::new(Int32Array::from(vec![32, 33, 64]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 1); // 1 >> 32 = 1
        assert_eq!(arr.value(1), 1); // 2 >> 33 = 1
        assert_eq!(arr.value(2), 3); // 3 >> 64 = 3
    }

    #[test]
    fn test_shift_right_negative_shift() {
        let value_array = Arc::new(Int32Array::from(vec![4, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![-1, -2, -3]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 4 >> -1 = 0
        assert_eq!(arr.value(1), 0); // 8 >> -2 = 0
        assert_eq!(arr.value(2), 0); // 16 >> -3 = 0
    }

    #[test]
    fn test_shift_right_negative_values() {
        let value_array = Arc::new(Int32Array::from(vec![-4, -8, -16]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftRight::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        // For signed integers, right shift preserves the sign bit
        assert_eq!(arr.value(0), -2); // -4 >> 1 = -2
        assert_eq!(arr.value(1), -2); // -8 >> 2 = -2
        assert_eq!(arr.value(2), -2); // -16 >> 3 = -2
    }

    #[test]
    fn test_shift_left_int32() {
        let value_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 1 << 1 = 2
        assert_eq!(arr.value(1), 8); // 2 << 2 = 8
        assert_eq!(arr.value(2), 24); // 3 << 3 = 24
        assert_eq!(arr.value(3), 64); // 4 << 4 = 64
    }

    #[test]
    fn test_shift_left_int64() {
        let value_array = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 2); // 1 << 1 = 2
        assert_eq!(arr.value(1), 8); // 2 << 2 = 8
        assert_eq!(arr.value(2), 24); // 3 << 3 = 24
    }

    #[test]
    fn test_shift_left_uint32() {
        let value_array = Arc::new(UInt32Array::from(vec![1u32, 2, 3]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt32Type>();
        assert_eq!(arr.value(0), 2); // 1 << 1 = 2
        assert_eq!(arr.value(1), 8); // 2 << 2 = 8
        assert_eq!(arr.value(2), 24); // 3 << 3 = 24
    }

    #[test]
    fn test_shift_left_uint64() {
        let value_array = Arc::new(UInt64Array::from(vec![1u64, 2, 3]));
        let shift_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<UInt64Type>();
        assert_eq!(arr.value(0), 2); // 1 << 1 = 2
        assert_eq!(arr.value(1), 8); // 2 << 2 = 8
        assert_eq!(arr.value(2), 24); // 3 << 3 = 24
    }

    #[test]
    fn test_shift_left_nulls() {
        let value_array = Arc::new(Int32Array::from(vec![Some(2), None, Some(3)]));
        let shift_array = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 4); // 2 << 1 = 4
        assert!(arr.is_null(1)); // null << 2 = null
        assert!(arr.is_null(2)); // 3 << null = null
    }

    #[test]
    fn test_shift_left_large_shift() {
        let value_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let shift_array = Arc::new(Int32Array::from(vec![32, 33, 64]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 1); // 1 << 32 = 0 (overflow)
        assert_eq!(arr.value(1), 4); // 2 << 33 = 0 (overflow)
        assert_eq!(arr.value(2), 3); // 3 << 64 = 0 (overflow)
    }

    #[test]
    fn test_shift_left_negative_shift() {
        let value_array = Arc::new(Int32Array::from(vec![4, 8, 16]));
        let shift_array = Arc::new(Int32Array::from(vec![-1, -2, -3]));
        let result = SparkShiftLeft::new()
            .spark_shift(&[value_array, shift_array])
            .unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 4 << -1 = 0
        assert_eq!(arr.value(1), 0); // 8 << -2 = 0
        assert_eq!(arr.value(2), 0); // 16 << -3 = 0
    }
}
