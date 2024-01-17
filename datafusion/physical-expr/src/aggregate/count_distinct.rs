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

use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{PrimitiveArray, StringArray};
use arrow_buffer::{BufferBuilder, MutableBuffer, OffsetBuffer};

use std::any::Any;
use std::cmp::Eq;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::aggregate::utils::{down_cast_any_ref, Hashable};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::cast::{as_list_array, as_primitive_array, as_string_array};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::memory_pool::proxy::RawTableAllocExt;
use datafusion_expr::Accumulator;

type DistinctScalarValues = ScalarValue;

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCount {
    /// Column name
    name: String,
    /// The DataType used to hold the state for each input
    state_data_type: DataType,
    /// The input arguments
    expr: Arc<dyn PhysicalExpr>,
}

impl DistinctCount {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_type: DataType,
        expr: Arc<dyn PhysicalExpr>,
        name: String,
    ) -> Self {
        Self {
            name,
            state_data_type: input_data_type,
            expr,
        }
    }
}

macro_rules! native_distinct_count_accumulator {
    ($TYPE:ident) => {{
        Ok(Box::new(NativeDistinctCountAccumulator::<$TYPE>::new()))
    }};
}

macro_rules! float_distinct_count_accumulator {
    ($TYPE:ident) => {{
        Ok(Box::new(FloatDistinctCountAccumulator::<$TYPE>::new()))
    }};
}

impl AggregateExpr for DistinctCount {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Int64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "count distinct"),
            Field::new("item", self.state_data_type.clone(), true),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        use DataType::*;
        use TimeUnit::*;

        match &self.state_data_type {
            Int8 => native_distinct_count_accumulator!(Int8Type),
            Int16 => native_distinct_count_accumulator!(Int16Type),
            Int32 => native_distinct_count_accumulator!(Int32Type),
            Int64 => native_distinct_count_accumulator!(Int64Type),
            UInt8 => native_distinct_count_accumulator!(UInt8Type),
            UInt16 => native_distinct_count_accumulator!(UInt16Type),
            UInt32 => native_distinct_count_accumulator!(UInt32Type),
            UInt64 => native_distinct_count_accumulator!(UInt64Type),
            Decimal128(_, _) => native_distinct_count_accumulator!(Decimal128Type),
            Decimal256(_, _) => native_distinct_count_accumulator!(Decimal256Type),

            Date32 => native_distinct_count_accumulator!(Date32Type),
            Date64 => native_distinct_count_accumulator!(Date64Type),
            Time32(Millisecond) => {
                native_distinct_count_accumulator!(Time32MillisecondType)
            }
            Time32(Second) => {
                native_distinct_count_accumulator!(Time32SecondType)
            }
            Time64(Microsecond) => {
                native_distinct_count_accumulator!(Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                native_distinct_count_accumulator!(Time64NanosecondType)
            }
            Timestamp(Microsecond, _) => {
                native_distinct_count_accumulator!(TimestampMicrosecondType)
            }
            Timestamp(Millisecond, _) => {
                native_distinct_count_accumulator!(TimestampMillisecondType)
            }
            Timestamp(Nanosecond, _) => {
                native_distinct_count_accumulator!(TimestampNanosecondType)
            }
            Timestamp(Second, _) => {
                native_distinct_count_accumulator!(TimestampSecondType)
            }

            Float16 => float_distinct_count_accumulator!(Float16Type),
            Float32 => float_distinct_count_accumulator!(Float32Type),
            Float64 => float_distinct_count_accumulator!(Float64Type),

            Utf8 => Ok(Box::new(StringDistinctCountAccumulator::new())),

            _ => Ok(Box::new(DistinctCountAccumulator {
                values: HashSet::default(),
                state_data_type: self.state_data_type.clone(),
            })),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for DistinctCount {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.state_data_type == x.state_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<DistinctScalarValues, RandomState>,
    state_data_type: DataType,
}

impl DistinctCountAccumulator {
    // calculating the size for fixed length values, taking first batch size * number of batches
    // This method is faster than .full_size(), however it is not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .next()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .unwrap_or(0)
            + std::mem::size_of::<DataType>()
    }

    // calculates the size as accurate as possible, call to this method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .sum::<usize>()
            + std::mem::size_of::<DataType>()
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let scalars = self.values.iter().cloned().collect::<Vec<_>>();
        let arr = ScalarValue::new_list(scalars.as_slice(), &self.state_data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        if arr.data_type() == &DataType::Null {
            return Ok(());
        }

        (0..arr.len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                self.values.insert(scalar);
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "array_agg states must be singleton!");
        let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&states[0])?;
        for scalars in scalar_vec.into_iter() {
            self.values.extend(scalars);
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        match &self.state_data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }
}

#[derive(Debug)]
struct NativeDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    values: HashSet<T::Native, RandomState>,
}

impl<T> NativeDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    fn new() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T> Accumulator for NativeDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash,
{
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let arr = Arc::new(PrimitiveArray::<T>::from_iter_values(
            self.values.iter().cloned(),
        )) as ArrayRef;
        let list = Arc::new(array_into_list_array(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;
        arr.iter().for_each(|value| {
            if let Some(value) = value {
                self.values.insert(value);
            }
        });

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<T>(&list)?;
                self.values.extend(list.values())
            };
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        let estimated_buckets = (self.values.len().checked_mul(8).unwrap_or(usize::MAX)
            / 7)
        .next_power_of_two();

        // Size of accumulator
        // + size of entry * number of buckets
        // + 1 byte for each bucket
        // + fixed size of HashSet
        std::mem::size_of_val(self)
            + std::mem::size_of::<T::Native>() * estimated_buckets
            + estimated_buckets
            + std::mem::size_of_val(&self.values)
    }
}

#[derive(Debug)]
struct FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    values: HashSet<Hashable<T::Native>, RandomState>,
}

impl<T> FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    fn new() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T> Accumulator for FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
{
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let arr = Arc::new(PrimitiveArray::<T>::from_iter_values(
            self.values.iter().map(|v| v.0),
        )) as ArrayRef;
        let list = Arc::new(array_into_list_array(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;
        arr.iter().for_each(|value| {
            if let Some(value) = value {
                self.values.insert(Hashable(value));
            }
        });

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<T>(&list)?;
                self.values
                    .extend(list.values().iter().map(|v| Hashable(*v)));
            };
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        let estimated_buckets = (self.values.len().checked_mul(8).unwrap_or(usize::MAX)
            / 7)
        .next_power_of_two();

        // Size of accumulator
        // + size of entry * number of buckets
        // + 1 byte for each bucket
        // + fixed size of HashSet
        std::mem::size_of_val(self)
            + std::mem::size_of::<T::Native>() * estimated_buckets
            + estimated_buckets
            + std::mem::size_of_val(&self.values)
    }
}

#[derive(Debug)]
struct StringDistinctCountAccumulator(SSOStringHashSet);
impl StringDistinctCountAccumulator {
    fn new() -> Self {
        Self(SSOStringHashSet::new())
    }
}

impl Accumulator for StringDistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let arr = self.0.state();
        let list = Arc::new(array_into_list_array(Arc::new(arr)));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_string_array(&values[0])?;
        arr.iter().for_each(|value| {
            if let Some(value) = value {
                self.0.insert(value);
            }
        });

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_string_array(&list)?;

                list.iter().for_each(|value| {
                    if let Some(value) = value {
                        self.0.insert(value);
                    }
                })
            };
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.len() as i64)))
    }

    fn size(&self) -> usize {
        // Size of accumulator
        // + SSOStringHashSet size
        std::mem::size_of_val(self) + self.0.size()
    }
}

const SHORT_STRING_LEN: usize = mem::size_of::<usize>();

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct SSOStringHeader {
    /// hash of the string value (used when resizing table)
    hash: u64,
    /// length of the string
    len: usize,
    /// short strings are stored inline, long strings are stored in the buffer
    offset_or_inline: usize,
}

// Short String Optimizated HashSet for String
// Equivalent to HashSet<String> but with better memory usage
#[derive(Default)]
struct SSOStringHashSet {
    /// Core of the HashSet, it stores both the short and long string headers
    header_set: HashSet<SSOStringHeader>,
    /// Used to check if the long string already exists
    long_string_map: hashbrown::raw::RawTable<SSOStringHeader>,
    /// Total size of the map in bytes
    map_size: usize,
    /// Buffer containing all long strings
    buffer: BufferBuilder<u8>,
    /// The random state used to generate hashes
    state: RandomState,
    /// Used for capacity calculation, equivalent to the sum of all string lengths
    size_hint: usize,
}

impl SSOStringHashSet {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, value: &str) {
        let value_len = value.len();
        self.size_hint += value_len;
        let value_bytes = value.as_bytes();

        if value_len <= SHORT_STRING_LEN {
            let inline = value_bytes
                .iter()
                .fold(0usize, |acc, &x| acc << 8 | x as usize);
            let short_string_header = SSOStringHeader {
                hash: 0, // no need for short string cases
                len: value_len,
                offset_or_inline: inline,
            };
            self.header_set.insert(short_string_header);
        } else {
            let hash = self.state.hash_one(value_bytes);

            let entry = self.long_string_map.get_mut(hash, |header| {
                // if hash matches, check if the bytes match
                let offset = header.offset_or_inline;
                let len = header.len;

                // SAFETY: buffer is only appended to, and we correctly inserted values
                let existing_value =
                    unsafe { self.buffer.as_slice().get_unchecked(offset..offset + len) };

                value_bytes == existing_value
            });

            if entry.is_none() {
                let offset = self.buffer.len();
                self.buffer.append_slice(value_bytes);
                let header = SSOStringHeader {
                    hash,
                    len: value_len,
                    offset_or_inline: offset,
                };
                self.long_string_map.insert_accounted(
                    header,
                    |header| header.hash,
                    &mut self.map_size,
                );
                self.header_set.insert(header);
            }
        }
    }

    // Returns a StringArray with the current state of the set
    fn state(&self) -> StringArray {
        let mut offsets = Vec::with_capacity(self.size_hint + 1);
        offsets.push(0);

        let mut values = MutableBuffer::new(0);
        let buffer = self.buffer.as_slice();

        for header in self.header_set.iter() {
            let s = if header.len <= SHORT_STRING_LEN {
                let inline = header.offset_or_inline;
                // convert usize to &[u8]
                let ptr = &inline as *const usize as *const u8;
                let len = std::mem::size_of::<usize>();
                unsafe { std::slice::from_raw_parts(ptr, len) }
            } else {
                let offset = header.offset_or_inline;
                // SAFETY: buffer is only appended to, and we correctly inserted values
                unsafe { buffer.get_unchecked(offset..offset + header.len) }
            };

            values.extend_from_slice(s);
            offsets.push(values.len() as i32);
        }

        let value_offsets = OffsetBuffer::<i32>::new(offsets.into());
        StringArray::new(value_offsets, values.into(), None)
    }

    fn len(&self) -> usize {
        self.header_set.len()
    }

    fn size(&self) -> usize {
        self.header_set.len() * mem::size_of::<SSOStringHeader>()
            + self.map_size
            + self.buffer.len()
    }
}

impl Debug for SSOStringHashSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSOStringHashSet")
            .field("header_set", &self.header_set)
            // TODO: Print long_string_map
            .field("map_size", &self.map_size)
            .field("buffer", &self.buffer)
            .field("state", &self.state)
            .finish()
    }
}
#[cfg(test)]
mod tests {
    use crate::expressions::NoOp;

    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::DataType;
    use arrow::datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    };
    use arrow_array::Decimal256Array;
    use arrow_buffer::i256;
    use datafusion_common::cast::{as_boolean_array, as_list_array, as_primitive_array};
    use datafusion_common::internal_err;
    use datafusion_common::DataFusionError;

    macro_rules! state_to_vec_primitive {
        ($LIST:expr, $DATA_TYPE:ident) => {{
            let arr = ScalarValue::raw_data($LIST).unwrap();
            let list_arr = as_list_array(&arr).unwrap();
            let arr = list_arr.values();
            let arr = as_primitive_array::<$DATA_TYPE>(arr)?;
            arr.values().iter().cloned().collect::<Vec<_>>()
        }};
    }

    macro_rules! test_count_distinct_update_batch_numeric {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(1),
                Some(1),
                None,
                Some(3),
                Some(2),
                None,
                Some(2),
                Some(3),
                Some(1),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec = state_to_vec_primitive!(&states[0], $DATA_TYPE);
            state_vec.sort();

            assert_eq!(states.len(), 1);
            assert_eq!(state_vec, vec![1, 2, 3]);
            assert_eq!(result, ScalarValue::Int64(Some(3)));

            Ok(())
        }};
    }

    fn state_to_vec_bool(sv: &ScalarValue) -> Result<Vec<bool>> {
        let arr = ScalarValue::raw_data(sv)?;
        let list_arr = as_list_array(&arr)?;
        let arr = list_arr.values();
        let bool_arr = as_boolean_array(arr)?;
        Ok(bool_arr.iter().flatten().collect())
    }

    fn run_update_batch(arrays: &[ArrayRef]) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            arrays[0].data_type().clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    fn run_update(
        data_types: &[DataType],
        rows: &[Vec<ScalarValue>],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            data_types[0].clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );

        let mut accum = agg.create_accumulator()?;

        let cols = (0..rows[0].len())
            .map(|i| {
                rows.iter()
                    .map(|inner| inner[i].clone())
                    .collect::<Vec<ScalarValue>>()
            })
            .collect::<Vec<_>>();

        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|c| ScalarValue::iter_to_array(c.clone()))
            .collect::<Result<Vec<ArrayRef>>>()?;

        accum.update_batch(&arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    // Used trait to create associated constant for f32 and f64
    trait SubNormal: 'static {
        const SUBNORMAL: Self;
    }

    impl SubNormal for f64 {
        const SUBNORMAL: Self = 1.0e-308_f64;
    }

    impl SubNormal for f32 {
        const SUBNORMAL: Self = 1.0e-38_f32;
    }

    macro_rules! test_count_distinct_update_batch_floating_point {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(<$PRIM_TYPE>::INFINITY),
                Some(<$PRIM_TYPE>::NAN),
                Some(1.0),
                Some(<$PRIM_TYPE as SubNormal>::SUBNORMAL),
                Some(1.0),
                Some(<$PRIM_TYPE>::INFINITY),
                None,
                Some(3.0),
                Some(-4.5),
                Some(2.0),
                None,
                Some(2.0),
                Some(3.0),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
                Some(1.0),
                Some(<$PRIM_TYPE>::NAN),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec = state_to_vec_primitive!(&states[0], $DATA_TYPE);

            dbg!(&state_vec);
            state_vec.sort_by(|a, b| match (a, b) {
                (lhs, rhs) => lhs.total_cmp(rhs),
            });

            let nan_idx = state_vec.len() - 1;
            assert_eq!(states.len(), 1);
            assert_eq!(
                &state_vec[..nan_idx],
                vec![
                    <$PRIM_TYPE>::NEG_INFINITY,
                    -4.5,
                    <$PRIM_TYPE as SubNormal>::SUBNORMAL,
                    1.0,
                    2.0,
                    3.0,
                    <$PRIM_TYPE>::INFINITY
                ]
            );
            assert!(state_vec[nan_idx].is_nan());
            assert_eq!(result, ScalarValue::Int64(Some(8)));

            Ok(())
        }};
    }

    macro_rules! test_count_distinct_update_batch_bigint {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(i256::from(1)),
                Some(i256::from(1)),
                None,
                Some(i256::from(3)),
                Some(i256::from(2)),
                None,
                Some(i256::from(2)),
                Some(i256::from(3)),
                Some(i256::from(1)),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec = state_to_vec_primitive!(&states[0], $DATA_TYPE);
            state_vec.sort();

            assert_eq!(states.len(), 1);
            assert_eq!(state_vec, vec![i256::from(1), i256::from(2), i256::from(3)]);
            assert_eq!(result, ScalarValue::Int64(Some(3)));

            Ok(())
        }};
    }

    #[test]
    fn count_distinct_update_batch_i8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int8Array, Int8Type, i8)
    }

    #[test]
    fn count_distinct_update_batch_i16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int16Array, Int16Type, i16)
    }

    #[test]
    fn count_distinct_update_batch_i32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int32Array, Int32Type, i32)
    }

    #[test]
    fn count_distinct_update_batch_i64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int64Array, Int64Type, i64)
    }

    #[test]
    fn count_distinct_update_batch_u8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt8Array, UInt8Type, u8)
    }

    #[test]
    fn count_distinct_update_batch_u16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt16Array, UInt16Type, u16)
    }

    #[test]
    fn count_distinct_update_batch_u32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt32Array, UInt32Type, u32)
    }

    #[test]
    fn count_distinct_update_batch_u64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt64Array, UInt64Type, u64)
    }

    #[test]
    fn count_distinct_update_batch_f32() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float32Array, Float32Type, f32)
    }

    #[test]
    fn count_distinct_update_batch_f64() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float64Array, Float64Type, f64)
    }

    #[test]
    fn count_distinct_update_batch_i256() -> Result<()> {
        test_count_distinct_update_batch_bigint!(Decimal256Array, Decimal256Type, i256)
    }

    #[test]
    fn count_distinct_update_batch_boolean() -> Result<()> {
        let get_count = |data: BooleanArray| -> Result<(Vec<bool>, i64)> {
            let arrays = vec![Arc::new(data) as ArrayRef];
            let (states, result) = run_update_batch(&arrays)?;
            let mut state_vec = state_to_vec_bool(&states[0])?;
            state_vec.sort();

            let count = match result {
                ScalarValue::Int64(c) => c.ok_or_else(|| {
                    DataFusionError::Internal("Found None count".to_string())
                }),
                scalar => {
                    internal_err!("Found non int64 scalar value from count: {scalar}")
                }
            }?;
            Ok((state_vec, count))
        };

        let zero_count_values = BooleanArray::from(Vec::<bool>::new());

        let one_count_values = BooleanArray::from(vec![false, false]);
        let one_count_values_with_null =
            BooleanArray::from(vec![Some(true), Some(true), None, None]);

        let two_count_values = BooleanArray::from(vec![true, false, true, false, true]);
        let two_count_values_with_null = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None,
            Some(true),
            Some(false),
        ]);

        assert_eq!(get_count(zero_count_values)?, (Vec::<bool>::new(), 0));
        assert_eq!(get_count(one_count_values)?, (vec![false], 1));
        assert_eq!(get_count(one_count_values_with_null)?, (vec![true], 1));
        assert_eq!(get_count(two_count_values)?, (vec![false, true], 2));
        assert_eq!(
            get_count(two_count_values_with_null)?,
            (vec![false, true], 2)
        );
        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_all_nulls() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(
            vec![None, None, None, None] as Vec<Option<i32>>
        )) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;
        let state_vec = state_to_vec_primitive!(&states[0], Int32Type);
        assert_eq!(states.len(), 1);
        assert!(state_vec.is_empty());
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_empty() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(vec![0_i32; 0])) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;
        let state_vec = state_to_vec_primitive!(&states[0], Int32Type);
        assert_eq!(states.len(), 1);
        assert!(state_vec.is_empty());
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32],
            &[
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(5))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(5))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(2))],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(3)));

        let (states, result) = run_update(
            &[DataType::UInt64],
            &[
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(2))],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(3)));
        Ok(())
    }

    #[test]
    fn count_distinct_update_with_nulls() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32],
            &[
                // None of these updates contains a None, so these are accumulated.
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-2))],
                // Each of these updates contains at least one None, so these
                // won't be accumulated.
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(None)],
                vec![ScalarValue::Int32(None)],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(2)));

        let (states, result) = run_update(
            &[DataType::UInt64],
            &[
                // None of these updates contains a None, so these are accumulated.
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(2))],
                // Each of these updates contains at least one None, so these
                // won't be accumulated.
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(None)],
                vec![ScalarValue::UInt64(None)],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(2)));
        Ok(())
    }
}
