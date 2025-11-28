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
use arrow::datatypes::DecimalType;
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::{exec_err, Result};

/// Operations required to manipulate the native representation of Arrow decimal arrays.
pub(super) trait DecimalNative:
    Copy + Rem<Output = Self> + PartialEq + PartialOrd
{
    fn zero() -> Self;
    fn checked_add(self, other: Self) -> Option<Self>;
    fn checked_sub(self, other: Self) -> Option<Self>;
    fn checked_pow10(exp: u32) -> Option<Self>;
}

impl DecimalNative for i32 {
    fn zero() -> Self {
        0
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_sub(other)
    }

    fn checked_pow10(exp: u32) -> Option<Self> {
        10_i32.checked_pow(exp)
    }
}

impl DecimalNative for i64 {
    fn zero() -> Self {
        0
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_sub(other)
    }

    fn checked_pow10(exp: u32) -> Option<Self> {
        10_i64.checked_pow(exp)
    }
}

impl DecimalNative for i128 {
    fn zero() -> Self {
        0
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_sub(other)
    }

    fn checked_pow10(exp: u32) -> Option<Self> {
        10_i128.checked_pow(exp)
    }
}

impl DecimalNative for i256 {
    fn zero() -> Self {
        i256::ZERO
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_sub(other)
    }

    fn checked_pow10(exp: u32) -> Option<Self> {
        i256::from_i128(10).checked_pow(exp)
    }
}

pub(super) fn apply_decimal_op<T, F>(
    array: &ArrayRef,
    scale: i8,
    fn_name: &str,
    op: F,
) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: DecimalNative,
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
    T::Native: DecimalNative,
{
    if scale < 0 {
        return exec_err!("Decimal scale {scale} must be non-negative");
    }

    if let Some(value) = T::Native::checked_pow10(scale as u32) {
        Ok(value)
    } else {
        exec_err!("Decimal scale {scale} is too large for {fn_name}")
    }
}

pub(super) fn ceil_decimal_value<T>(
    value: T,
    factor: T,
) -> std::result::Result<T, ArrowError>
where
    T: DecimalNative,
{
    let remainder = value % factor;

    if remainder == T::zero() {
        return Ok(value);
    }

    if value >= T::zero() {
        let increment = factor
            .checked_sub(remainder)
            .ok_or_else(|| overflow_err("ceil"))?;
        value
            .checked_add(increment)
            .ok_or_else(|| overflow_err("ceil"))
    } else {
        value
            .checked_sub(remainder)
            .ok_or_else(|| overflow_err("ceil"))
    }
}

pub(super) fn floor_decimal_value<T>(
    value: T,
    factor: T,
) -> std::result::Result<T, ArrowError>
where
    T: DecimalNative,
{
    let remainder = value % factor;

    if remainder == T::zero() {
        return Ok(value);
    }

    if value >= T::zero() {
        value
            .checked_sub(remainder)
            .ok_or_else(|| overflow_err("floor"))
    } else {
        let adjustment = factor
            .checked_add(remainder)
            .ok_or_else(|| overflow_err("floor"))?;
        value
            .checked_sub(adjustment)
            .ok_or_else(|| overflow_err("floor"))
    }
}

fn overflow_err(name: &str) -> ArrowError {
    ArrowError::ComputeError(format!("Decimal overflow while applying {name}"))
}
