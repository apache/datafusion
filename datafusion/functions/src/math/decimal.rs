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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{
    ArrowNativeTypeOp, DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION,
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, DecimalType,
};
use arrow::error::ArrowError;
use arrow_buffer::ArrowNativeType;
use datafusion_common::{DataFusionError, Result};

pub(super) fn apply_decimal_op<T, F>(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    output_scale: i8,
    fn_name: &str,
    op: F,
) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    if scale <= 0 {
        return Ok(Arc::clone(array));
    }

    let factor = decimal_scale_factor::<T>(scale, fn_name)?;
    let decimal = array.as_primitive::<T>();
    let data_type = T::TYPE_CONSTRUCTOR(precision, output_scale);

    let result: PrimitiveArray<T> = decimal.try_unary(|value| {
        let new_value = op(value, factor);
        T::validate_decimal_precision(new_value, precision, output_scale).map_err(
            |_| {
                ArrowError::ComputeError(format!(
                    "Decimal overflow while applying {fn_name}"
                ))
            },
        )?;
        Ok::<_, ArrowError>(new_value)
    })?;

    let result = result.with_data_type(data_type);

    Ok(Arc::new(result))
}

fn decimal_scale_factor<T>(scale: i8, fn_name: &str) -> Result<T::Native>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
{
    let base = <T::Native as ArrowNativeType>::from_usize(10).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Cannot get 10_{} from usize: {:?}",
            std::any::type_name::<T::Native>(),
            10_usize
        ))
    })?;

    base.pow_checked(scale as u32).map_err(|_| {
        DataFusionError::Execution(format!("Decimal overflow while applying {fn_name}"))
    })
}

/// Compute the return precision for floor/ceil result to accommodate the result
pub(super) fn decimal_floor_ceil_precision(
    precision: u8,
    scale: i8,
    max_precision: u8,
) -> u8 {
    ((precision as i64) - (scale as i64) + 1).clamp(1, max_precision as i64) as u8
}

pub(super) fn decimal_floor_ceil_return_type(arg_type: &DataType) -> DataType {
    match arg_type {
        DataType::Null => DataType::Float64,
        DataType::Decimal32(precision, scale) if *scale > 0 => DataType::Decimal32(
            decimal_floor_ceil_precision(*precision, *scale, DECIMAL32_MAX_PRECISION),
            0,
        ),
        DataType::Decimal64(precision, scale) if *scale > 0 => DataType::Decimal64(
            decimal_floor_ceil_precision(*precision, *scale, DECIMAL64_MAX_PRECISION),
            0,
        ),
        DataType::Decimal128(precision, scale) if *scale > 0 => DataType::Decimal128(
            decimal_floor_ceil_precision(*precision, *scale, DECIMAL128_MAX_PRECISION),
            0,
        ),
        DataType::Decimal256(precision, scale) if *scale > 0 => DataType::Decimal256(
            decimal_floor_ceil_precision(*precision, *scale, DECIMAL256_MAX_PRECISION),
            0,
        ),
        other => other.clone(),
    }
}

/// Computes `ceil(value / factor)` as an integer at scale 0,
/// avoiding overflow with carrying to next decimal points (999 -> 1000)
pub(super) fn ceil_decimal_value<T>(value: T, factor: T) -> T
where
    T: ArrowNativeTypeOp + std::ops::Rem<Output = T>,
{
    let quotient = value.div_wrapping(factor);
    let remainder = value % factor;

    if remainder != T::ZERO && value > T::ZERO {
        quotient.add_wrapping(T::ONE)
    } else {
        quotient
    }
}

/// Computes `floor(value / factor)` as an integer at scale 0,
/// avoiding overflow with carrying to next decimal points (-999 -> -1000)
pub(super) fn floor_decimal_value<T>(value: T, factor: T) -> T
where
    T: ArrowNativeTypeOp + std::ops::Rem<Output = T>,
{
    let quotient = value.div_wrapping(factor);
    let remainder = value % factor;

    if remainder != T::ZERO && value < T::ZERO {
        quotient.sub_wrapping(T::ONE)
    } else {
        quotient
    }
}
