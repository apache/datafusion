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

use arrow::array::{ArrayRef, ArrowPrimitiveType, AsArray, Int32Array, PrimitiveArray};
use arrow::compute;
use arrow::datatypes::{
    ArrowNativeType, DataType, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use datafusion_common::types::{
    logical_int16, logical_int32, logical_int64, logical_int8, logical_uint16,
    logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Bitwise left shift on elements in `value` by corresponding `shift` amount.
/// The shift amount is normalized to the bit width of the type, matching Spark/Java
/// semantics for negative and large shifts.
fn shift_left<T>(
    value: &PrimitiveArray<T>,
    shift: &Int32Array,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: std::ops::Shl<i32, Output = T::Native>,
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

/// Bitwise right shift on elements in `value` by corresponding `shift` amount.
/// The shift amount is normalized to the bit width of the type, matching Spark/Java
/// semantics for negative and large shifts.
fn shift_right<T>(
    value: &PrimitiveArray<T>,
    shift: &Int32Array,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: std::ops::Shr<i32, Output = T::Native>,
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
trait UShr {
    fn ushr(self, rhs: i32) -> Self;
}

impl UShr for u32 {
    fn ushr(self, rhs: i32) -> Self {
        self >> rhs
    }
}

impl UShr for u64 {
    fn ushr(self, rhs: i32) -> Self {
        self >> rhs
    }
}

impl UShr for i32 {
    fn ushr(self, rhs: i32) -> Self {
        ((self as u32) >> rhs) as i32
    }
}

impl UShr for i64 {
    fn ushr(self, rhs: i32) -> Self {
        ((self as u64) >> rhs) as i64
    }
}

/// Bitwise unsigned right shift on elements in `value` by corresponding `shift`
/// amount. The shift amount is normalized to the bit width of the type, matching
/// Spark/Java semantics for negative and large shifts.
fn shift_right_unsigned<T>(
    value: &PrimitiveArray<T>,
    shift: &Int32Array,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: UShr,
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

fn shift_inner(
    arrays: &[ArrayRef],
    name: &str,
    bit_shift_type: BitShiftType,
) -> Result<ArrayRef> {
    let [value_array, shift_array] = take_function_args(name, arrays)?;
    let shift_array = shift_array.as_primitive::<Int32Type>();

    fn shift<T>(
        value: &PrimitiveArray<T>,
        shift: &Int32Array,
        bit_shift_type: BitShiftType,
    ) -> Result<PrimitiveArray<T>>
    where
        T: ArrowPrimitiveType,
        T::Native: std::ops::Shl<i32, Output = T::Native>
            + std::ops::Shr<i32, Output = T::Native>
            + UShr,
    {
        match bit_shift_type {
            BitShiftType::Left => shift_left(value, shift),
            BitShiftType::Right => shift_right(value, shift),
            BitShiftType::RightUnsigned => shift_right_unsigned(value, shift),
        }
    }

    match value_array.data_type() {
        DataType::Int32 => {
            let value_array = value_array.as_primitive::<Int32Type>();
            Ok(Arc::new(shift(value_array, shift_array, bit_shift_type)?))
        }
        DataType::Int64 => {
            let value_array = value_array.as_primitive::<Int64Type>();
            Ok(Arc::new(shift(value_array, shift_array, bit_shift_type)?))
        }
        DataType::UInt32 => {
            let value_array = value_array.as_primitive::<UInt32Type>();
            Ok(Arc::new(shift(value_array, shift_array, bit_shift_type)?))
        }
        DataType::UInt64 => {
            let value_array = value_array.as_primitive::<UInt64Type>();
            Ok(Arc::new(shift(value_array, shift_array, bit_shift_type)?))
        }
        dt => {
            internal_err!("{name} function does not support data type: {dt}")
        }
    }
}

#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq)]
enum BitShiftType {
    Left,
    Right,
    RightUnsigned,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkBitShift {
    signature: Signature,
    name: &'static str,
    bit_shift_type: BitShiftType,
}

impl SparkBitShift {
    fn new(name: &'static str, bit_shift_type: BitShiftType) -> Self {
        let shift_amount = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // Upcast small ints to 32bit
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int32()),
                            vec![
                                TypeSignatureClass::Native(logical_int8()),
                                TypeSignatureClass::Native(logical_int16()),
                            ],
                            NativeType::Int32,
                        ),
                        shift_amount.clone(),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_uint32()),
                            vec![
                                TypeSignatureClass::Native(logical_uint8()),
                                TypeSignatureClass::Native(logical_uint16()),
                            ],
                            NativeType::UInt32,
                        ),
                        shift_amount.clone(),
                    ]),
                    // Otherwise accept direct 64 bit integers
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                        shift_amount.clone(),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_uint64())),
                        shift_amount.clone(),
                    ]),
                ],
                Volatility::Immutable,
            ),
            name,
            bit_shift_type,
        }
    }

    pub fn left() -> Self {
        Self::new("shiftleft", BitShiftType::Left)
    }

    pub fn right() -> Self {
        Self::new("shiftright", BitShiftType::Right)
    }

    pub fn right_unsigned() -> Self {
        Self::new("shiftrightunsigned", BitShiftType::RightUnsigned)
    }
}

impl ScalarUDFImpl for SparkBitShift {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let inner = |arr: &[ArrayRef]| -> Result<ArrayRef> {
            shift_inner(arr, self.name(), self.bit_shift_type)
        };
        make_scalar_function(inner, vec![])(&args.args)
    }
}
