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

use crate::aggregates::group_values::{GroupValues, HashValue};
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder,
    PrimitiveArray, cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupSelection};
use std::sync::Arc;

/// A [`GroupValues`] storing a single column of primitive values over ordered
///
/// This specialization is significantly faster than using the more general
/// purpose [`GroupValuesPrimitive`].
///
/// [`GroupValuesPrimitive`]: [`crate::aggregates::group_values::single_group_by::primitive::GroupValuesPrimitive`]
pub(crate) struct FullyOrderedGroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index
    values: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> FullyOrderedGroupValuesPrimitive<T> {
    pub(crate) fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            values: Vec::with_capacity(128),
            null_group: None,
        }
    }

    fn handle_valid(
        &mut self,
        groups: &mut Vec<usize>,
        mut current_value_valid: T::Native,
        mut current_group: usize,
        values_slice: &[T::Native],
    ) -> usize
    where
        T::Native: HashValue,
    {
        for &v in values_slice {
            let v = v.canonicalize();
            // If new group, save the current group and start a new one
            if v.is_ne(current_value_valid) {
                current_group += 1;
                current_value_valid = v;
                self.values.push(current_value_valid);
            }
            groups.push(current_group);
        }

        current_group
    }

    /// Return the current value and the current group index, if any
    ///
    /// If the current value is null, the current group index will be the index of the null group
    /// If the current value is not null, the current group index will be the index of the last value in the values vector
    /// If there are no values, return None
    fn current_value(&self) -> Option<(Option<T::Native>, usize)> {
        let current_group_index = self.values.len() - 1;
        let last_value = self.values.last()?;

        let current_value = if self
            .null_group
            .is_some_and(|group_index| group_index == current_group_index)
        {
            None
        } else {
            Some(*last_value)
        };

        Some((current_value, current_group_index))
    }
}

impl<T: ArrowPrimitiveType> GroupValues for FullyOrderedGroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        let col = cols[0].as_primitive::<T>();

        if col.is_empty() {
            return Ok(());
        }

        // On first batch

        if self.is_empty() {
            let value = if col.is_null(0) {
                // Set the null group to 0, since the first value is null
                self.null_group = Some(0);
                None
            } else {
                Some(col.value(0).canonicalize())
            };
            self.values.push(value.unwrap_or_default());
        }

        let (current_value, mut current_group) = self.current_value().unwrap();

        match (current_value, col.null_count()) {
            // If current group is null and the entire column is null
            (None, null_count) if null_count == col.len() => {
                // All groups with the same current group
                groups.resize(col.len(), current_group);
            }

            // If current group is null and there may be nulls or not, but not all are nulls
            (None, null_count) => {
                // Add the nulls to the current group
                groups.resize(null_count, current_group);

                assert!(
                    !col.is_null(null_count),
                    "input is ordered, so once null was seen all nulls should be at the beginning of the column"
                );
                debug_assert_eq!(
                    col.slice(0, null_count).null_count(),
                    null_count,
                    "input is ordered, so once null was seen all nulls should be at the beginning of the column"
                );

                let values_slice = &col.values()[null_count..];

                let current_value_valid = values_slice[0];
                current_group += 1;
                self.values.push(current_value_valid);

                self.handle_valid(
                    groups,
                    current_value_valid,
                    current_group,
                    values_slice,
                );
            }

            // If current value is valid
            (Some(current_value_valid), null_count) => {
                let values_without_nulls = col.len() - null_count;

                debug_assert_eq!(
                    col.slice(values_without_nulls, null_count).null_count(),
                    null_count,
                    "input is ordered, so once null was seen after non nulls, all nulls should be at the end of the column"
                );

                current_group = self.handle_valid(
                    groups,
                    current_value_valid,
                    current_group,
                    &col.values()[0..values_without_nulls],
                );

                // If there are nulls
                if null_count > 0 {
                    current_group += 1;
                    self.values.push(T::default_value());
                    self.null_group = Some(current_group);

                    groups.resize(col.len(), current_group);
                }
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        fn build_primitive<T: ArrowPrimitiveType>(
            values: Vec<T::Native>,
            null_idx: Option<usize>,
        ) -> PrimitiveArray<T> {
            let nulls = null_idx.map(|null_idx| {
                let mut buffer = NullBufferBuilder::new(values.len());
                buffer.append_n_non_nulls(null_idx);
                buffer.append_null();
                buffer.append_n_non_nulls(values.len() - null_idx - 1);
                // NOTE: The inner builder must be constructed as there is at least one null
                buffer.finish().unwrap()
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };

                build_primitive(split_vec_min_alloc(&mut self.values, n), null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.values.len())?;
        let values: Vec<T::Native> =
            selection.iter().map(|index| self.values[index]).collect();
        let nulls = if let Some(null_group) = self.null_group {
            let mut nulls = NullBufferBuilder::new(values.len());
            for index in selection.iter() {
                if index == null_group {
                    nulls.append_null();
                } else {
                    nulls.append_non_null();
                }
            }
            nulls.finish()
        } else {
            None
        };
        let array = PrimitiveArray::<T>::new(values.into(), nulls)
            .with_data_type(self.data_type.clone());
        Ok(vec![Arc::new(array)])
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.null_group = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::Int32Type;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;
    use datafusion_expr::EmitTo;
    use std::sync::Arc;

    /// Mirror of the `EmitTo::take_needed` regression test, applied to the
    /// concrete `FullyOrderedGroupValuesPrimitive` accumulator.
    ///
    /// When `n` is small, the old `split_off(n) + swap` pattern used inside
    /// `emit(EmitTo::First(n))` left `self.values` with a small fresh allocation
    /// and returned the emitted prefix carrying the original large backing.
    ///
    /// With `split_vec_min_alloc` and `n * 2 <= len`, the drain branch is taken:
    /// the emitted prefix gets a compact allocation and `self.values` retains the
    /// original large one.
    #[test]
    fn emit_first_small_n_allocates_minimally() -> Result<()> {
        let mut gv = FullyOrderedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32);

        // Intern 20 distinct values; `new()` pre-allocates capacity 128 for `values`.
        let arr: ArrayRef = Arc::new(Int32Array::from_iter_values(0..20i32));
        let mut groups = vec![];
        gv.intern(&[arr], &mut groups)?;
        let capacity_before = gv.values.capacity(); // 128

        // n=4, n*2=8 <= len=20 -> drain branch
        let emitted = gv.emit(EmitTo::First(4))?;

        assert_eq!(emitted[0].len(), 4);

        // `self.values` must retain its original large allocation.
        // Old split_off+swap left it with a fresh small allocation (~16).
        assert_eq!(
            gv.values.capacity(),
            capacity_before,
            "self.values capacity {} should equal original {} after small First(n) emit",
            gv.values.capacity(),
            capacity_before,
        );

        Ok(())
    }
}
