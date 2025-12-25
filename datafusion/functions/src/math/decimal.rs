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
use arrow::datatypes::{ArrowNativeTypeOp, DecimalType};
use arrow::error::ArrowError;
use arrow_buffer::ArrowNativeType;
use datafusion_common::{DataFusionError, Result};

pub(super) fn apply_decimal_op<T, F>(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
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
    let data_type = array.data_type().clone();

    let result: PrimitiveArray<T> = decimal.try_unary(|value| {
        let new_value = op(value, factor);
        T::validate_decimal_precision(new_value, precision, scale).map_err(|_| {
            ArrowError::ComputeError(format!("Decimal overflow while applying {fn_name}"))
        })?;
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

pub(super) fn ceil_decimal_value<T>(value: T, factor: T) -> T
where
    T: ArrowNativeTypeOp + std::ops::Rem<Output = T>,
{
    let remainder = value % factor;

    if remainder == T::ZERO {
        return value;
    }

    if value >= T::ZERO {
        let increment = factor.sub_wrapping(remainder);
        value.add_wrapping(increment)
    } else {
        value.sub_wrapping(remainder)
    }
}

pub(super) fn floor_decimal_value<T>(value: T, factor: T) -> T
where
    T: ArrowNativeTypeOp + std::ops::Rem<Output = T>,
{
    let remainder = value % factor;

    if remainder == T::ZERO {
        return value;
    }

    if value >= T::ZERO {
        value.sub_wrapping(remainder)
    } else {
        let adjustment = factor.add_wrapping(remainder);
        value.sub_wrapping(adjustment)
    }
}
