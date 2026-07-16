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
use datafusion_common::{Result, ScalarValue, internal_err};
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

/// Fallback state for arbitrary Arrow types (List, LargeList, Struct, Map, ...)
/// that are not covered by [`PrimitiveValueState`] or [`BytesValueState`].
///
/// Stores one [`ScalarValue`] per group. Winners are identified by the
/// vectorized comparator in the enclosing accumulator, so the per-row
/// allocation cost of the fallback `Accumulator` path is avoided:
/// `ScalarValue::try_from_array` is called once per group per batch (at
/// winner-update time), not once per candidate row.
pub(crate) struct GenericValueState {
    vals: Vec<Option<ScalarValue>>,
    data_type: DataType,
    /// Cached total heap size of `vals`, updated on each mutation to avoid
    /// walking the vector on every `size()` call.
    total_size: usize,
}

impl GenericValueState {
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            vals: vec![],
            data_type,
            total_size: 0,
        }
    }
}

impl ValueState for GenericValueState {
    fn resize(&mut self, new_size: usize) {
        if new_size < self.vals.len() {
            for v in self.vals[new_size..].iter().flatten() {
                self.total_size -= v.size();
            }
        }
        self.vals.resize(new_size, None);
    }

    fn update(&mut self, group_idx: usize, array: &ArrayRef, idx: usize) -> Result<()> {
        if let Some(v) = &self.vals[group_idx] {
            self.total_size -= v.size();
        }
        let mut scalar = ScalarValue::try_from_array(array, idx)?;
        // `try_from_array` for nested types returns Arc slices into the source
        // batch buffers, so a single stored winner would pin the entire batch
        // in memory. Compact copies the referenced bytes into an owned buffer
        // so old batches can be dropped as new ones arrive.
        scalar.compact();
        self.total_size += scalar.size();
        self.vals[group_idx] = Some(scalar);
        Ok(())
    }

    fn take(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let taken = emit_to.take_needed(&mut self.vals);
        let taken_size: usize = taken.iter().flatten().map(|v| v.size()).sum();
        self.total_size -= taken_size;

        let default = ScalarValue::try_from(&self.data_type)?;
        let scalars = taken
            .into_iter()
            .map(|opt| opt.unwrap_or_else(|| default.clone()))
            .collect::<Vec<_>>();
        if scalars.is_empty() {
            return Ok(arrow::array::new_empty_array(&self.data_type));
        }
        ScalarValue::iter_to_array(scalars)
    }

    fn size(&self) -> usize {
        self.vals.capacity() * size_of::<Option<ScalarValue>>() + self.total_size
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
            let first_n: BooleanBuffer = bool_buf.slice(0, n);
            // reset the existing buffer
            bool_buf_builder.append_buffer(&bool_buf.slice(n, bool_buf.len() - n));
            first_n
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, BinaryViewArray, FixedSizeListArray, Int32Array,
        Int32Builder, LargeBinaryArray, LargeListArray, LargeStringArray, ListBuilder,
        MapArray, StringArray, StringBuilder, StringViewArray, StructArray,
    };
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Fields};

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

    // ---------- GenericValueState (nested types) ----------

    /// Build a `List<Utf8>` array with three rows: `["a"]`, `["b", "c"]`,
    /// `["d", "e", "f"]`. Used by several tests.
    fn make_list_utf8_array() -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.append(true);
        builder.values().append_value("b");
        builder.values().append_value("c");
        builder.append(true);
        builder.values().append_value("d");
        builder.values().append_value("e");
        builder.values().append_value("f");
        builder.append(true);
        Arc::new(builder.finish())
    }

    #[test]
    fn test_generic_value_state_list_utf8() -> Result<()> {
        let list_utf8 =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let mut state = GenericValueState::new(list_utf8.clone());
        state.resize(2);

        let array = make_list_utf8_array();

        // group 0 <- ["a"] ; group 1 <- ["b", "c"]
        state.update(0, &array, 0)?;
        state.update(1, &array, 1)?;

        // Overwrite group 0 with the wider ["d", "e", "f"] (size-accounting
        // must decrement the old value before adding the new one).
        let size_after_first = state.total_size;
        state.update(0, &array, 2)?;
        assert!(
            state.total_size > 0,
            "total_size must remain positive after overwrite"
        );
        // The overwrite replaced group 0's payload; the delta relative to the
        // previous state should equal `new.size() - old.size()`. If the caller
        // forgot to subtract the old size, `total_size` would drift upward.
        let expected_delta = {
            let new_scalar = {
                let mut s = ScalarValue::try_from_array(&array, 2)?;
                s.compact();
                s
            };
            let old_scalar = {
                let mut s = ScalarValue::try_from_array(&array, 0)?;
                s.compact();
                s
            };
            new_scalar.size() as isize - old_scalar.size() as isize
        };
        assert_eq!(
            state.total_size as isize - size_after_first as isize,
            expected_delta,
            "size accounting drifted after overwrite"
        );

        let result = state.take(EmitTo::All)?;
        let result = result.as_list::<i32>();
        assert_eq!(result.len(), 2);
        let g0 = result.value(0);
        let g0 = g0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(g0.value(0), "d");
        assert_eq!(g0.value(1), "e");
        assert_eq!(g0.value(2), "f");
        let g1 = result.value(1);
        let g1 = g1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(g1.value(0), "b");
        assert_eq!(g1.value(1), "c");

        assert_eq!(state.total_size, 0, "state must be fully drained");
        Ok(())
    }

    #[test]
    fn test_generic_value_state_struct() -> Result<()> {
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let struct_type = DataType::Struct(fields.clone());

        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let name = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let struct_array =
            Arc::new(StructArray::new(fields, vec![id, name], None)) as ArrayRef;

        let mut state = GenericValueState::new(struct_type);
        state.resize(2);
        state.update(0, &struct_array, 0)?;
        state.update(1, &struct_array, 2)?;

        let out = state.take(EmitTo::All)?;
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out.len(), 2);

        let out_id = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let out_name = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out_id.value(0), 1);
        assert_eq!(out_id.value(1), 3);
        assert_eq!(out_name.value(0), "a");
        assert_eq!(out_name.value(1), "c");
        Ok(())
    }

    #[test]
    fn test_generic_value_state_large_list() -> Result<()> {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let large_list_type = DataType::LargeList(Arc::clone(&field));

        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets: OffsetBuffer<i64> =
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 5, 6]));
        let array = Arc::new(LargeListArray::new(field, offsets, Arc::new(values), None))
            as ArrayRef;

        let mut state = GenericValueState::new(large_list_type);
        state.resize(2);
        state.update(0, &array, 0)?; // [1, 2]
        state.update(1, &array, 2)?; // [6]

        let out = state.take(EmitTo::All)?;
        let out = out.as_list::<i64>();
        assert_eq!(out.len(), 2);
        let g0 = out.value(0);
        let g0 = g0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(g0.len(), 2);
        assert_eq!(g0.value(0), 1);
        assert_eq!(g0.value(1), 2);
        let g1 = out.value(1);
        let g1 = g1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(g1.len(), 1);
        assert_eq!(g1.value(0), 6);
        Ok(())
    }

    #[test]
    fn test_generic_value_state_fixed_size_list() -> Result<()> {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let fsl_type = DataType::FixedSizeList(Arc::clone(&field), 2);

        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let array = Arc::new(FixedSizeListArray::new(field, 2, Arc::new(values), None))
            as ArrayRef;

        let mut state = GenericValueState::new(fsl_type);
        state.resize(2);
        state.update(0, &array, 0)?; // [1, 2]
        state.update(1, &array, 2)?; // [5, 6]

        let out = state.take(EmitTo::All)?;
        let out = out
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("emitted FixedSizeListArray");
        assert_eq!(out.len(), 2);
        let g0 = out.value(0);
        let g0 = g0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(g0.value(0), 1);
        assert_eq!(g0.value(1), 2);
        let g1 = out.value(1);
        let g1 = g1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(g1.value(0), 5);
        assert_eq!(g1.value(1), 6);
        Ok(())
    }

    #[test]
    fn test_generic_value_state_map() -> Result<()> {
        // Map<Utf8, Int32> with two entries: {"a": 1, "b": 2}, {"c": 3}
        let keys = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let entry_fields = Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 3]));
        let map_field =
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
        let map_array = Arc::new(MapArray::new(
            Arc::clone(&map_field),
            offsets,
            entries,
            None,
            false,
        )) as ArrayRef;
        let map_type = DataType::Map(map_field, false);

        let mut state = GenericValueState::new(map_type);
        state.resize(2);
        state.update(0, &map_array, 0)?; // {"a": 1, "b": 2}
        state.update(1, &map_array, 1)?; // {"c": 3}

        let out = state.take(EmitTo::All)?;
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out.value_length(0), 2);
        assert_eq!(out.value_length(1), 1);
        Ok(())
    }

    #[test]
    fn test_generic_value_state_emit_first() -> Result<()> {
        let list_utf8 =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let mut state = GenericValueState::new(list_utf8);
        state.resize(3);

        let array = make_list_utf8_array();
        state.update(0, &array, 0)?;
        state.update(1, &array, 1)?;
        state.update(2, &array, 2)?;

        let after_all_updates = state.total_size;
        assert!(after_all_updates > 0);

        // Emit the first 2 groups; remaining group 2 stays.
        let head = state.take(EmitTo::First(2))?;
        let head = head.as_list::<i32>();
        assert_eq!(head.len(), 2);
        let h0 = head.value(0);
        let h0 = h0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(h0.value(0), "a");
        let h1 = head.value(1);
        let h1 = h1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(h1.value(0), "b");
        assert_eq!(h1.value(1), "c");

        // After partial emit, `total_size` shrank but is still positive.
        assert!(state.total_size > 0);
        assert!(state.total_size < after_all_updates);

        let tail = state.take(EmitTo::All)?;
        let tail = tail.as_list::<i32>();
        assert_eq!(tail.len(), 1);
        let t0 = tail.value(0);
        let t0 = t0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(t0.value(0), "d");
        assert_eq!(t0.value(1), "e");
        assert_eq!(t0.value(2), "f");

        assert_eq!(state.total_size, 0);
        Ok(())
    }

    #[test]
    fn test_generic_value_state_update_null() -> Result<()> {
        // List<Int32> with rows: [1, 2], NULL
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.append(false); // null entry
        let array: ArrayRef = Arc::new(builder.finish());

        let list_type = array.data_type().clone();
        let mut state = GenericValueState::new(list_type);
        state.resize(1);

        // group 0 = [1, 2]
        state.update(0, &array, 0)?;
        let size_after_value = state.total_size;
        assert!(size_after_value > 0);

        // Overwrite group 0 with NULL. The size accounting must subtract the
        // previous value's size and then add the null-scalar's size; the point
        // of this test is that `total_size` stays consistent (no drift) and
        // the null is emitted correctly.
        state.update(0, &array, 1)?;
        // Recomputing from scratch must match the cached total_size.
        let recomputed: usize = state.vals.iter().flatten().map(|v| v.size()).sum();
        assert_eq!(
            state.total_size, recomputed,
            "total_size drifted after null update"
        );

        let out = state.take(EmitTo::All)?;
        let out = out.as_list::<i32>();
        assert_eq!(out.len(), 1);
        assert!(out.is_null(0));
        assert_eq!(state.total_size, 0);
        Ok(())
    }

    #[test]
    fn test_generic_value_state_compact_releases_parent_batch() -> Result<()> {
        // Regression test for the memory-pinning bug: without compact(),
        // `ScalarValue::try_from_array` on a List column returns an Arc
        // slice that keeps the whole parent batch alive. compact() must
        // copy the referenced bytes into an owned buffer so the parent
        // array can be dropped.
        let list_utf8 =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let mut state = GenericValueState::new(list_utf8);
        state.resize(1);

        let array: ArrayRef = make_list_utf8_array();
        let weak_before = Arc::downgrade(&array);
        assert_eq!(weak_before.strong_count(), 1);

        state.update(0, &array, 0)?;
        drop(array);

        // If compact() did its job, dropping the source array must release
        // the only strong reference. If the stored ScalarValue kept a slice
        // of the parent, strong_count would stay >= 1 and the batch would
        // remain pinned.
        assert_eq!(
            weak_before.strong_count(),
            0,
            "compact() failed to break the Arc reference to the source batch"
        );

        // Data must still be readable from the stored copy.
        let out = state.take(EmitTo::All)?;
        let out = out.as_list::<i32>();
        assert_eq!(out.len(), 1);
        let g0 = out.value(0);
        let g0 = g0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(g0.value(0), "a");
        Ok(())
    }

    #[test]
    fn test_generic_value_state_resize_shrink_recovers_size() -> Result<()> {
        let list_utf8 =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let mut state = GenericValueState::new(list_utf8);
        state.resize(3);

        let array = make_list_utf8_array();
        state.update(0, &array, 0)?;
        state.update(1, &array, 1)?;
        state.update(2, &array, 2)?;
        let full_size = state.total_size;
        assert!(full_size > 0);

        // Shrinking must subtract the dropped groups' sizes from total_size.
        state.resize(1);
        assert!(state.total_size > 0);
        assert!(state.total_size < full_size);
        Ok(())
    }
}
