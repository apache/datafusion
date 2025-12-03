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

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, PrimitiveArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    ArrowNativeType, DataType, DecimalType, Field, FieldRef, ToByteSlice,
};
use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{
    HashSet, Result, ScalarValue, exec_err, internal_datafusion_err,
};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use std::sync::Arc;

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

/// Construct corresponding fields for the expressions in an ORDER BY clause.
pub fn ordering_fields(
    order_bys: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<FieldRef> {
    order_bys
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
        .map(Arc::new)
        .collect()
}

/// Selects the sort option attribute from all the given `PhysicalSortExpr`s.
pub fn get_sort_options(ordering_req: &LexOrdering) -> Vec<SortOptions> {
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
    /// the output scale
    target_scale: i8,
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
            .ok_or_else(|| {
                internal_datafusion_err!("Failed to compute sum_mul in DecimalAverager")
            })?;

        let target_mul = T::Native::from_usize(10_usize)
            .map(|b| b.pow_wrapping(target_scale as u32))
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "Failed to compute target_mul in DecimalAverager"
                )
            })?;

        if target_mul >= sum_mul {
            Ok(Self {
                sum_mul,
                target_mul,
                target_precision,
                target_scale,
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

            let validate = T::validate_decimal_precision(
                new_value,
                self.target_precision,
                self.target_scale,
            );

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

/// Generic way to collect distinct values for accumulators.
///
/// The intermediate state is represented as a List of scalar values updated by
/// `merge_batch` and a `Vec` of `ArrayRef` that are converted to scalar values
/// in the final evaluation step so that we avoid expensive conversions and
/// allocations during `update_batch`.
pub struct GenericDistinctBuffer<T: ArrowPrimitiveType> {
    pub values: HashSet<Hashable<T::Native>, RandomState>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for GenericDistinctBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GenericDistinctBuffer({}, values={})",
            self.data_type,
            self.values.len()
        )
    }
}

impl<T: ArrowPrimitiveType> GenericDistinctBuffer<T> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            values: HashSet::default(),
            data_type,
        }
    }

    /// Mirrors [`Accumulator::state`].
    pub fn state(&self) -> Result<Vec<ScalarValue>> {
        let arr = Arc::new(
            PrimitiveArray::<T>::from_iter_values(self.values.iter().map(|v| v.0))
                // Ideally we'd just use T::DATA_TYPE but this misses things like
                // decimal scale/precision and timestamp timezones, which need to
                // match up with Accumulator::state_fields
                .with_data_type(self.data_type.clone()),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    /// Mirrors [`Accumulator::update_batch`].
    pub fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        debug_assert_eq!(
            values.len(),
            1,
            "DistinctValuesBuffer::update_batch expects only a single input array"
        );

        let arr = as_primitive_array::<T>(&values[0])?;
        if arr.null_count() > 0 {
            self.values.extend(arr.iter().flatten().map(Hashable));
        } else {
            self.values
                .extend(arr.values().iter().cloned().map(Hashable));
        }

        Ok(())
    }

    /// Mirrors [`Accumulator::merge_batch`].
    pub fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let array = as_list_array(&states[0])?;
        for list in array.iter().flatten() {
            self.update_batch(&[list])?;
        }

        Ok(())
    }

    /// Mirrors [`Accumulator::size`].
    pub fn size(&self) -> usize {
        let num_elements = self.values.len();
        let fixed_size = size_of_val(self) + size_of_val(&self.values);
        estimate_memory_size::<T::Native>(num_elements, fixed_size).unwrap()
    }
}
