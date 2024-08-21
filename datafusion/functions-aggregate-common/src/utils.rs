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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::ArrowNativeType;
use arrow::{
    array::ArrowNativeTypeOp,
    compute::SortOptions,
    datatypes::{
        DataType, Decimal128Type, DecimalType, Field, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        ToByteSlice,
    },
};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

/// Convert scalar values from an accumulator into arrays.
pub fn get_accum_scalar_values_as_arrays(
    accum: &mut dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    accum
        .state()?
        .iter()
        .map(|s| s.to_array_of_size(1))
        .collect()
}

/// Adjust array type metadata if needed
///
/// Since `Decimal128Arrays` created from `Vec<NativeType>` have
/// default precision and scale, this function adjusts the output to
/// match `data_type`, if necessary
pub fn adjust_output_array(data_type: &DataType, array: ArrayRef) -> Result<ArrayRef> {
    let array = match data_type {
        DataType::Decimal128(p, s) => Arc::new(
            array
                .as_primitive::<Decimal128Type>()
                .clone()
                .with_precision_and_scale(*p, *s)?,
        ) as ArrayRef,
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
            array
                .as_primitive::<TimestampNanosecondType>()
                .clone()
                .with_timezone_opt(tz.clone()),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone_opt(tz.clone()),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Arc::new(
            array
                .as_primitive::<TimestampMillisecondType>()
                .clone()
                .with_timezone_opt(tz.clone()),
        ),
        DataType::Timestamp(TimeUnit::Second, tz) => Arc::new(
            array
                .as_primitive::<TimestampSecondType>()
                .clone()
                .with_timezone_opt(tz.clone()),
        ),
        // no adjustment needed for other arrays
        _ => array,
    };
    Ok(array)
}

/// Construct corresponding fields for lexicographical ordering requirement expression
pub fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<Field> {
    ordering_req
        .iter()
        .zip(data_types.iter())
        .map(|(sort_expr, dtype)| {
            Field::new(
                sort_expr.expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .collect()
}

/// Selects the sort option attribute from all the given `PhysicalSortExpr`s.
pub fn get_sort_options(ordering_req: &[PhysicalSortExpr]) -> Vec<SortOptions> {
    ordering_req.iter().map(|item| item.options).collect()
}

/// A wrapper around a type to provide hash for floats
#[derive(Copy, Clone, Debug)]
pub struct Hashable<T>(pub T);

impl<T: ToByteSlice> std::hash::Hash for Hashable<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_byte_slice().hash(state)
    }
}

impl<T: ArrowNativeTypeOp> PartialEq for Hashable<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_eq(other.0)
    }
}

impl<T: ArrowNativeTypeOp> Eq for Hashable<T> {}

/// Computes averages for `Decimal128`/`Decimal256` values, checking for overflow
///
/// This is needed because different precisions for Decimal128/Decimal256 can
/// store different ranges of values and thus sum/count may not fit in
/// the target type.
///
/// For example, the precision is 3, the max of value is `999` and the min
/// value is `-999`
pub struct DecimalAverager<T: DecimalType> {
    /// scale factor for sum values (10^sum_scale)
    sum_mul: T::Native,
    /// scale factor for target (10^target_scale)
    target_mul: T::Native,
    /// the output precision
    target_precision: u8,
}

impl<T: DecimalType> DecimalAverager<T> {
    /// Create a new `DecimalAverager`:
    ///
    /// * sum_scale: the scale of `sum` values passed to [`Self::avg`]
    /// * target_precision: the output precision
    /// * target_scale: the output scale
    ///
    /// Errors if the resulting data can not be stored
    pub fn try_new(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Result<Self> {
        let sum_mul = T::Native::from_usize(10_usize)
            .map(|b| b.pow_wrapping(sum_scale as u32))
            .ok_or(DataFusionError::Internal(
                "Failed to compute sum_mul in DecimalAverager".to_string(),
            ))?;

        let target_mul = T::Native::from_usize(10_usize)
            .map(|b| b.pow_wrapping(target_scale as u32))
            .ok_or(DataFusionError::Internal(
                "Failed to compute target_mul in DecimalAverager".to_string(),
            ))?;

        if target_mul >= sum_mul {
            Ok(Self {
                sum_mul,
                target_mul,
                target_precision,
            })
        } else {
            // can't convert the lit decimal to the returned data type
            exec_err!("Arithmetic Overflow in AvgAccumulator")
        }
    }

    /// Returns the `sum`/`count` as a i128/i256 Decimal128/Decimal256 with
    /// target_scale and target_precision and reporting overflow.
    ///
    /// * sum: The total sum value stored as Decimal128 with sum_scale
    ///   (passed to `Self::try_new`)
    /// * count: total count, stored as a i128/i256 (*NOT* a Decimal128/Decimal256 value)
    #[inline(always)]
    pub fn avg(&self, sum: T::Native, count: T::Native) -> Result<T::Native> {
        if let Ok(value) = sum.mul_checked(self.target_mul.div_wrapping(self.sum_mul)) {
            let new_value = value.div_wrapping(count);

            let validate =
                T::validate_decimal_precision(new_value, self.target_precision);

            if validate.is_ok() {
                Ok(new_value)
            } else {
                exec_err!("Arithmetic Overflow in AvgAccumulator")
            }
        } else {
            // can't convert the lit decimal to the returned data type
            exec_err!("Arithmetic Overflow in AvgAccumulator")
        }
    }
}
