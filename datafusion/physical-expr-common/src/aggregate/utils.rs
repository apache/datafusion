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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, ArrowNativeTypeOp, AsArray},
    compute::SortOptions,
    datatypes::{
        DataType, Decimal128Type, Field, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        ToByteSlice,
    },
};
use datafusion_common::Result;
use datafusion_expr::Accumulator;

use crate::sort_expr::PhysicalSortExpr;

use super::AggregateExpr;

/// Downcast a `Box<dyn AggregateExpr>` or `Arc<dyn AggregateExpr>`
/// and return the inner trait object as [`Any`] so
/// that it can be downcast to a specific implementation.
///
/// This method is used when implementing the `PartialEq<dyn Any>`
/// for [`AggregateExpr`] aggregation expressions and allows comparing the equality
/// between the trait objects.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if let Some(obj) = any.downcast_ref::<Arc<dyn AggregateExpr>>() {
        obj.as_any()
    } else if let Some(obj) = any.downcast_ref::<Box<dyn AggregateExpr>>() {
        obj.as_any()
    } else {
        any
    }
}

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
