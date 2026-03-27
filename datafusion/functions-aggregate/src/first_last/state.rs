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

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryBuilder, BinaryViewBuilder,
    BooleanBufferBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveArray,
    StringBuilder, StringViewBuilder,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion_common::{Result, internal_err};
use datafusion_expr::EmitTo;

pub(crate) trait ValueState: Send + Sync {
    /// Resizes the state to accommodate `new_size` groups.
    fn resize(&mut self, new_size: usize);
    /// Updates the state for the specified `group_idx` using the value at `idx` from the provided `array`.
    ///
    /// Note: While this is not a batch interface, it is not a performance bottleneck.
    /// In heavy aggregation benchmarks, the overhead of this method is typically less than 1%.
    ///
    /// ```sql
    /// -- TPC-H SF10
    /// select l_shipmode, last_value(l_partkey order by l_orderkey, l_linenumber, l_comment, l_suppkey, l_tax)
    /// from 'benchmarks/data/tpch_sf10/lineitem'
    /// group by l_shipmode;
    ///
    /// -- H2O G1_1e8
    /// select t.id1, first_value(t.id3 order by t.id2, t.id4) as r2
    /// from 'benchmarks/data/h2o/G1_1e8_1e8_100_0.parquet' as t
    /// group by t.id1, t.v1;
    /// ```
    fn update(&mut self, group_idx: usize, array: &ArrayRef, idx: usize) -> Result<()>;
    /// Takes the accumulated state and returns it as an [`ArrayRef`], respecting the `emit_to` strategy.
    fn take(&mut self, emit_to: EmitTo) -> Result<ArrayRef>;
    /// Returns the estimated memory size of the state in bytes.
    fn size(&self) -> usize;
}

pub(crate) struct PrimitiveValueState<T: ArrowPrimitiveType + Send> {
    /// Values data
    vals: Vec<T::Native>,
    nulls: BooleanBufferBuilder,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType + Send> PrimitiveValueState<T> {
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            vals: vec![],
            nulls: BooleanBufferBuilder::new(0),
            data_type,
        }
    }
}

impl<T: ArrowPrimitiveType + Send> ValueState for PrimitiveValueState<T> {
    fn resize(&mut self, new_size: usize) {
        self.vals.resize(new_size, T::default_value());
        self.nulls.resize(new_size);
    }

    fn update(&mut self, group_idx: usize, array: &ArrayRef, idx: usize) -> Result<()> {
        let array = array.as_primitive::<T>();
        self.vals[group_idx] = array.value(idx);
        self.nulls.set_bit(group_idx, !array.is_null(idx));
        Ok(())
    }

    fn take(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.vals);
        let null_buf = NullBuffer::new(take_need(&mut self.nulls, emit_to));
        let array: PrimitiveArray<T> =
            PrimitiveArray::<T>::new(values.into(), Some(null_buf))
                .with_data_type(self.data_type.clone());
        Ok(Arc::new(array))
    }

    fn size(&self) -> usize {
        self.vals.capacity() * size_of::<T::Native>() + self.nulls.capacity() / 8
    }
}

/// Stores internal state for "bytes" types (Utf8, Binary, etc.).
///
/// This implementation is similar to `MinMaxBytesState` in `min_max_bytes.rs`, but
/// it does not reuse it for two main reasons:
///
/// 1. **Direct Overwrite**: `MinMaxBytesState::update_batch` is tightly coupled
///    with min/max comparison logic, whereas `FirstLast` performs its own comparisons
///    externally (using ordering columns) and only needs a simple interface to
///    unconditionally set/overwrite values for specific groups.
/// 2. **Different NULL Handling**: `MinMaxBytesState` always ignores `NULL` values
///    in the input, while `BytesValueState` needs to support setting `NULL` values
///    to correctly implement `RESPECT NULLS` behavior.
///
pub(crate) struct BytesValueState {
    vals: Vec<Option<Vec<u8>>>,
    data_type: DataType,
    /// The sum of the capacities of all vectors in `vals`.
    total_capacity: usize,
}

impl BytesValueState {
    pub(crate) fn try_new(data_type: DataType) -> Result<Self> {
        if !matches!(
            data_type,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        ) {
            return internal_err!("BytesValueState does not support {}", data_type);
        }
        Ok(Self {
            vals: vec![],
            data_type,
            total_capacity: 0,
        })
    }
}

impl ValueState for BytesValueState {
    fn resize(&mut self, new_size: usize) {
        if new_size < self.vals.len() {
            for v in self.vals[new_size..].iter().flatten() {
                self.total_capacity -= v.capacity();
            }
        }
        self.vals.resize(new_size, None);
    }

    fn update(&mut self, group_idx: usize, array: &ArrayRef, idx: usize) -> Result<()> {
        if let Some(v) = &self.vals[group_idx] {
            self.total_capacity -= v.capacity();
        }

        if array.is_null(idx) {
            self.vals[group_idx] = None;
        } else {
            let val = match self.data_type {
                DataType::Utf8 => array.as_string::<i32>().value(idx).as_bytes(),
                DataType::LargeUtf8 => array.as_string::<i64>().value(idx).as_bytes(),
                DataType::Utf8View => array.as_string_view().value(idx).as_bytes(),
                DataType::Binary => array.as_binary::<i32>().value(idx),
                DataType::LargeBinary => array.as_binary::<i64>().value(idx),
                DataType::BinaryView => array.as_binary_view().value(idx),
                _ => {
                    return internal_err!(
                        "Unsupported data type for BytesValueState: {}",
                        self.data_type
                    );
                }
            };

            if let Some(v) = &mut self.vals[group_idx] {
                v.clear();
                v.extend_from_slice(val);
            } else {
                let v = val.to_vec();
                self.vals[group_idx] = Some(v);
            }

            self.vals[group_idx]
                .as_ref()
                .inspect(|x| self.total_capacity += x.capacity());
        }
        Ok(())
    }

    fn take(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.vals);

        let (total_len, taken_capacity) = values
            .iter()
            .flatten()
            .fold((0, 0), |(len_acc, cap_acc), v| {
                (len_acc + v.len(), cap_acc + v.capacity())
            });
        self.total_capacity -= taken_capacity;

        match self.data_type {
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(values.len(), total_len);
                for val in values {
                    match val {
                        Some(v) => builder.append_value(
                            // SAFETY: The bytes were originally from a valid UTF-8 array in `update`
                            unsafe { std::str::from_utf8_unchecked(&v) },
                        ),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::LargeUtf8 => {
                let mut builder =
                    LargeStringBuilder::with_capacity(values.len(), total_len);
                for val in values {
                    match val {
                        Some(v) => builder.append_value(
                            // SAFETY: The bytes were originally from a valid UTF-8 array in `update`
                            unsafe { std::str::from_utf8_unchecked(&v) },
                        ),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8View => {
                let mut builder = StringViewBuilder::with_capacity(values.len());
                for val in values {
                    match val {
                        Some(v) => builder.append_value(
                            // SAFETY: The bytes were originally from a valid UTF-8 array in `update`
                            unsafe { std::str::from_utf8_unchecked(&v) },
                        ),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(values.len(), total_len);
                for val in values {
                    match val {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::LargeBinary => {
                let mut builder =
                    LargeBinaryBuilder::with_capacity(values.len(), total_len);
                for val in values {
                    match val {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::BinaryView => {
                let mut builder = BinaryViewBuilder::with_capacity(values.len());
                for val in values {
                    match val {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => internal_err!(
                "Unsupported data type for BytesValueState: {}",
                self.data_type
            ),
        }
    }

    fn size(&self) -> usize {
        self.vals.capacity() * size_of::<Option<Vec<u8>>>() + self.total_capacity
    }
}

impl BytesValueState {
    #[cfg(test)]
    /// For testing only: strictly calculate the sum of capacities of all vectors in `vals`.
    fn total_capacity_calculated(&self) -> usize {
        self.vals.iter().flatten().map(|v| v.capacity()).sum()
    }
}

pub(crate) fn take_need(
    bool_buf_builder: &mut BooleanBufferBuilder,
    emit_to: EmitTo,
) -> BooleanBuffer {
    let bool_buf = bool_buf_builder.finish();
    match emit_to {
        EmitTo::All => bool_buf,
        EmitTo::First(n) => {
            // split off the first N values in seen_values
            //
            // TODO make this more efficient rather than two
            // copies and bitwise manipulation
            let first_n: BooleanBuffer = bool_buf.iter().take(n).collect();
            // reset the existing buffer
            for b in bool_buf.iter().skip(n) {
                bool_buf_builder.append(b);
            }
            first_n
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
        StringViewArray,
    };

    #[test]
    fn test_bytes_value_state_utf8() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::Utf8)?;
        state.resize(2);

        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("longer_string_than_hello"),
        ]));

        state.update(0, &array, 0)?; // group 0 = "hello"
        state.update(1, &array, 1)?; // group 1 = "world"

        assert_eq!(state.total_capacity, state.total_capacity_calculated());

        // Overwrite group 0 with a longer string (checks capacity update logic)
        state.update(0, &array, 2)?;
        assert_eq!(state.total_capacity, state.total_capacity_calculated());

        let result = state.take(EmitTo::All)?;
        let result = result.as_string::<i32>();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "longer_string_than_hello");
        assert_eq!(result.value(1), "world");

        // After take all, size should be 0 (excluding vals vector capacity)
        assert_eq!(state.total_capacity, 0);
        assert_eq!(state.total_capacity, state.total_capacity_calculated());

        Ok(())
    }

    #[test]
    fn test_bytes_value_state_large_utf8() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::LargeUtf8)?;
        state.resize(1);
        let array: ArrayRef = Arc::new(LargeStringArray::from(vec!["large_utf8"]));
        state.update(0, &array, 0)?;
        let result = state.take(EmitTo::All)?;
        assert_eq!(result.as_string::<i64>().value(0), "large_utf8");
        Ok(())
    }

    #[test]
    fn test_bytes_value_state_utf8_view() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::Utf8View)?;
        state.resize(1);
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["Utf8View"]));
        state.update(0, &array, 0)?;
        let result = state.take(EmitTo::All)?;
        assert_eq!(result.as_string_view().value(0), "Utf8View");
        Ok(())
    }

    #[test]
    fn test_bytes_value_state_binary() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::Binary)?;
        state.resize(1);
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![b"binary" as &[u8]]));
        state.update(0, &array, 0)?;
        let result = state.take(EmitTo::All)?;
        assert_eq!(result.as_binary::<i32>().value(0), b"binary");
        Ok(())
    }

    #[test]
    fn test_bytes_value_state_large_binary() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::LargeBinary)?;
        state.resize(1);
        let array: ArrayRef =
            Arc::new(LargeBinaryArray::from(vec![b"large_binary" as &[u8]]));
        state.update(0, &array, 0)?;
        let result = state.take(EmitTo::All)?;
        assert_eq!(result.as_binary::<i64>().value(0), b"large_binary");
        Ok(())
    }

    #[test]
    fn test_bytes_value_state_binary_view() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::BinaryView)?;
        state.resize(1);

        let data: Vec<Option<&[u8]>> = vec![Some(b"long_binary_value_to_test_view")];
        let array: ArrayRef = Arc::new(BinaryViewArray::from(data));

        state.update(0, &array, 0)?;

        let result = state.take(EmitTo::All)?;
        let result = result.as_binary_view();
        assert_eq!(result.value(0), b"long_binary_value_to_test_view");

        Ok(())
    }

    #[test]
    fn test_bytes_value_state_emit_first() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::Utf8)?;
        state.resize(3);

        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        state.update(0, &array, 0)?;
        state.update(1, &array, 1)?;
        state.update(2, &array, 2)?;

        let result = state.take(EmitTo::First(2))?;
        let result = result.as_string::<i32>();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "a");
        assert_eq!(result.value(1), "b");

        // Remaining should be "c"
        let result = state.take(EmitTo::All)?;
        let result = result.as_string::<i32>();
        assert_eq!(result.len(), 1);
        assert_eq!(result.value(0), "c");

        Ok(())
    }

    #[test]
    fn test_bytes_value_state_update_null() -> Result<()> {
        let mut state = BytesValueState::try_new(DataType::Utf8)?;
        state.resize(1);

        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), None]));

        // group 0 = "hello"
        state.update(0, &array, 0)?;
        assert_eq!(state.total_capacity, state.total_capacity_calculated());
        assert!(state.total_capacity > 0);

        // group 0 = NULL
        state.update(0, &array, 1)?;
        assert_eq!(
            state.total_capacity,
            state.total_capacity_calculated(),
            "total_capacity should match calculated capacity after update(NULL)"
        );
        assert_eq!(state.total_capacity, 0);

        Ok(())
    }
}
