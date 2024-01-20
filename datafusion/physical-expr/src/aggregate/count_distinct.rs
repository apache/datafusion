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

use std::any::Any;
use std::cmp::Eq;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{PrimitiveArray, StringArray};
use arrow_buffer::{BufferBuilder, OffsetBuffer, ScalarBuffer};

use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::memory_pool::proxy::RawTableAllocExt;
use datafusion_expr::Accumulator;

use crate::aggregate::utils::{down_cast_any_ref, Hashable};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};

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
struct StringDistinctCountAccumulator(Mutex<SSOStringHashSet>);
impl StringDistinctCountAccumulator {
    fn new() -> Self {
        Self(Mutex::new(SSOStringHashSet::new()))
    }
}

impl Accumulator for StringDistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        // TODO this should not need a lock/clone (should make
        // `Accumulator::state` take a mutable reference)
        let mut lk = self.0.lock().unwrap();
        let set: &mut SSOStringHashSet = &mut lk;
        // take the state out of the string set and replace with default
        let set = std::mem::take(set);
        let arr = set.into_state();
        let list = Arc::new(array_into_list_array(Arc::new(arr)));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.lock().unwrap().insert(values[0].clone());

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
                self.0.lock().unwrap().insert(list);
            };
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(
            Some(self.0.lock().unwrap().len() as i64),
        ))
    }

    fn size(&self) -> usize {
        // Size of accumulator
        // + SSOStringHashSet size
        std::mem::size_of_val(self) + self.0.lock().unwrap().size()
    }
}

/// Maximum size of a string that can be inlined in the hash table
const SHORT_STRING_LEN: usize = mem::size_of::<usize>();

/// Entry that is stored in the actual hash table
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct SSOStringHeader {
    /// hash of the string value (stored to avoid recomputing it when checking)
    /// TODO can we simply recreate when needed
    hash: u64,
    /// length of the string, in bytes
    len: usize,
    /// if len =< SHORT_STRING_LEN: the string data inlined
    /// if len > SHORT_STRING_LEN, the offset of where the data starts
    offset_or_inline: usize,
}

impl SSOStringHeader {}

impl SSOStringHeader {
    /// returns self.offset..self.offset + self.len
    fn range(&self) -> Range<usize> {
        self.offset_or_inline..self.offset_or_inline + self.len
    }
}

// Short String Optimized HashSet for String
// Equivalent to HashSet<String> but with better memory usage
struct SSOStringHashSet {
    /// Store entries for each distinct string
    map: hashbrown::raw::RawTable<SSOStringHeader>,
    /// Total size of the map in bytes (TODO)
    map_size: usize,
    /// Buffer containing all string values
    buffer: BufferBuilder<u8>,
    /// offsets into buffer of the distinct values. These are the same offsets
    /// as are used for a GenericStringArray
    offsets: Vec<i32>,
    /// The random state used to generate hashes
    random_state: RandomState,
    // buffer to be reused to store hashes
    hashes_buffer: Vec<u64>,
}

impl Default for SSOStringHashSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SSOStringHashSet {
    fn new() -> Self {
        Self {
            map: hashbrown::raw::RawTable::new(),
            map_size: 0,
            buffer: BufferBuilder::new(0),
            offsets: vec![0], // first offset is always 0
            random_state: RandomState::new(),
            hashes_buffer: vec![],
        }
    }

    fn insert(&mut self, values: ArrayRef) {
        // step 1: compute hashes for the strings
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(values.len(), 0);
        create_hashes(&[values.clone()], &self.random_state, batch_hashes)
            // hash is supported for all string types and create_hashes only
            // returns errors for unsupported types
            .unwrap();

        // TODO make this generic (to support large strings)
        let values = values.as_string::<i32>();

        // step 2: insert each string into the set, if not already present

        // Assert for unsafe values call
        assert_eq!(values.len(), batch_hashes.len());

        for (value, &hash) in values.iter().zip(batch_hashes.iter()) {
            // count distinct ignores nulls
            let Some(value) = value else {
                continue;
            };

            // from here on only use bytes (not str/chars) for value
            let value = value.as_bytes();

            if value.len() <= SHORT_STRING_LEN {
                let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);

                // Check if the value is already present in the set
                let entry = self.map.get_mut(hash, |header| {
                    // if hash matches, must also compare the values
                    if header.len != value.len() {
                        return false;
                    }
                    inline == header.offset_or_inline
                });

                // Insert an entry for this value if it is not present
                if entry.is_none() {
                    // Put the small values into buffer and output so it appears
                    // the output array, but store the actual bytes inline
                    self.buffer.append_slice(value);
                    self.offsets.push(self.buffer.len() as i32);

                    // store the actual value inline
                    let new_header = SSOStringHeader {
                        hash,
                        len: value.len(),
                        offset_or_inline: inline,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                }
            }
            // handle large strings
            else {
                // Check if the value is already present in the set
                let entry = self.map.get_mut(hash, |header| {
                    // if hash matches, must also compare the values
                    if header.len != value.len() {
                        return false;
                    }
                    // SAFETY: buffer is only appended to, and we correctly inserted values
                    let existing_value =
                        unsafe { self.buffer.as_slice().get_unchecked(header.range()) };

                    value == existing_value
                });

                // Insert the value if it is not present
                if entry.is_none() {
                    // long strings are stored as a length/offset into the buffer
                    let offset = self.buffer.len(); // offset of start fof data
                    self.buffer.append_slice(value);
                    self.offsets.push(self.buffer.len() as i32);

                    let new_header = SSOStringHeader {
                        hash,
                        len: value.len(),
                        offset_or_inline: offset,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                }
            }
        }
    }

    /// Converts this set into a StringArray of the distinct string values
    fn into_state(self) -> StringArray {
        // The map contains entries that have offsets in some arbitrary order
        // but the buffer contains the actual strings in the order they were inserted
        // so we need to build offsets for the strings in the buffer in order
        // then append short strings, if any, and then build the StringArray
        // TODO a picture would be nice here
        let Self {
            map: _,
            map_size: _,
            offsets,
            mut buffer,
            random_state: _,
            hashes_buffer: _,
        } = self;

        // Add any
        let offsets: ScalarBuffer<_> = offsets.into();

        // get the values and reset self.buffer
        let values = buffer.finish();

        let nulls = None; // count distinct ignores nulls
                          // SAFETY: all the values that went in are coming
        unsafe { StringArray::new_unchecked(OffsetBuffer::new(offsets), values, nulls) }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn size(&self) -> usize {
        self.map_size + self.buffer.len()
    }
}

impl Debug for SSOStringHashSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSOStringHashSet")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("buffer", &self.buffer)
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}
#[cfg(test)]
mod tests {
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

    use crate::expressions::NoOp;

    use super::*;

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
    #[test]
    fn string_set_empty() {
        for values in [StringArray::new_null(0), StringArray::new_null(11)] {
            let mut set = SSOStringHashSet::new();
            set.insert(Arc::new(values));
            assert_set(set, &[]);
        }
    }

    #[test]
    fn string_set_basic() {
        // basic test for mixed small and large string values
        let values = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCC"), // 10 bytes
            Some(""),
            Some("cbcxx"), // 5 bytes
            None,
            Some("AAAAAAAA"),  // 8 bytes
            Some("BBBBBQBBB"), // 9 bytes
            Some("a"),
            Some("cbcxx"),
            Some("b"),
            Some("cbcxx"),
            Some(""),
            None,
            Some("BBBBBQBBB"),
            Some("BBBBBQBBB"),
            Some("AAAAAAAA"),
            Some("CXCCCCCCCC"),
        ]);

        let mut set = SSOStringHashSet::new();
        set.insert(Arc::new(values));
        assert_set(
            set,
            &[
                Some(""),
                Some("AAAAAAAA"),
                Some("BBBBBQBBB"),
                Some("CXCCCCCCCC"),
                Some("a"),
                Some("b"),
                Some("cbcxx"),
            ],
        );
    }

    #[test]
    fn string_set_non_utf8() {
        // basic test for mixed small and large string values
        let values = StringArray::from(vec![
            Some("a"),
            Some("✨🔥"),
            Some("🔥"),
            Some("✨✨✨"),
            Some("foobarbaz"),
            Some("🔥"),
            Some("✨🔥"),
        ]);

        let mut set = SSOStringHashSet::new();
        set.insert(Arc::new(values));
        assert_set(
            set,
            &[
                Some("a"),
                Some("foobarbaz"),
                Some("✨✨✨"),
                Some("✨🔥"),
                Some("🔥"),
            ],
        );
    }

    // asserts that the set contains the expected strings
    fn assert_set(set: SSOStringHashSet, expected: &[Option<&str>]) {
        let strings = set.into_state();
        let mut state = strings.into_iter().collect::<Vec<_>>();
        state.sort();
        assert_eq!(state, expected);
    }

    // TODO fuzz testing

    // inserting strings into the set does not increase reported memoyr
}
