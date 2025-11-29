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

use std::ops::Rem;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{ArrowNativeTypeOp, DecimalType};
use arrow::error::ArrowError;
use arrow_buffer::ArrowNativeType;
use datafusion_common::{DataFusionError, Result};

pub(super) fn apply_decimal_op<T, F>(
    array: &ArrayRef,
    scale: i8,
    fn_name: &str,
    op: F,
) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
    F: Fn(T::Native, T::Native) -> std::result::Result<T::Native, ArrowError>,
{
    if scale <= 0 {
        return Ok(Arc::clone(array));
    }

    let factor = decimal_scale_factor::<T>(scale, fn_name)?;
    let decimal = array.as_primitive::<T>();
    let data_type = array.data_type().clone();

    let result: PrimitiveArray<T> = decimal
        .try_unary(|value| op(value, factor))?
        .with_data_type(data_type);

    Ok(Arc::new(result))
}

fn decimal_scale_factor<T>(scale: i8, fn_name: &str) -> Result<T::Native>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
{
    let base = <T::Native as ArrowNativeType>::from_usize(10).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Decimal scale {scale} is too large for {fn_name}"
        ))
    })?;

    base.pow_checked(scale as u32).map_err(|_| {
        DataFusionError::Execution(format!(
            "Decimal scale {scale} is too large for {fn_name}"
        ))
    })
}

pub(super) fn ceil_decimal_value<T>(
    value: T,
    factor: T,
) -> std::result::Result<T, ArrowError>
where
    T: ArrowNativeTypeOp + Rem<Output = T>,
{
    let remainder = value % factor;

    if remainder == T::ZERO {
        return Ok(value);
    }

    if value >= T::ZERO {
        let increment = factor
            .sub_checked(remainder)
            .map_err(|_| overflow_err("ceil"))?;
        value
            .add_checked(increment)
            .map_err(|_| overflow_err("ceil"))
    } else {
        value
            .sub_checked(remainder)
            .map_err(|_| overflow_err("ceil"))
    }
}

pub(super) fn floor_decimal_value<T>(
    value: T,
    factor: T,
) -> std::result::Result<T, ArrowError>
where
    T: ArrowNativeTypeOp + Rem<Output = T>,
{
    let remainder = value % factor;

    if remainder == T::ZERO {
        return Ok(value);
    }

    if value >= T::ZERO {
        value
            .sub_checked(remainder)
            .map_err(|_| overflow_err("floor"))
    } else {
        let adjustment = factor
            .add_checked(remainder)
            .map_err(|_| overflow_err("floor"))?;
        value
            .sub_checked(adjustment)
            .map_err(|_| overflow_err("floor"))
    }
}

fn overflow_err(name: &str) -> ArrowError {
    ArrowError::ComputeError(format!("Decimal overflow while applying {name}"))
}
