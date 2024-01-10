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

//! [`ArrowStringMap`] and [`ArrowStringSet`] for storing maps/sets of values from
//! StringArray / LargeStringArray

use ahash::RandomState;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::{
    BooleanBufferBuilder, BufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use std::fmt::Debug;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

/// HashSet optimized for storing `String` and `LargeString` values
/// and producing the final set as a GenericStringArray with minimal copies.
#[derive(Debug, Default)]
pub struct ArrowStringSet<O: OffsetSizeTrait>(ArrowStringMap<O, ()>);

impl<O: OffsetSizeTrait> ArrowStringSet<O> {
    pub fn new() -> Self {
        Self(ArrowStringMap::new())
    }

    /// Inserts each string from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: ()) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
    }

    /// Converts this set into a `StringArray` or `LargeStringArray` containing each
    /// distinct string value. This is done without copying the values.
    pub fn into_state(self) -> ArrayRef {
        self.0.into_state()
    }

    /// Returns the total number of distinct strings (including nulls) seen so far
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// returns the total number of distinct strings (not including nulls) seen so far
    pub fn non_null_len(&self) -> usize {
        self.0.non_null_len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        self.0.size()
    }
}

/// Map optimized for storing `String` and `LargeString` values that can produce
/// that set of keys on output as a GenericStringArray.
///
/// # Generic Arguments
///
/// O: OffsetSize (String/LargeString)
/// V: payload type
///
/// # Description
///
/// This is a specialized HashMap that is
///
/// 1. optimized for storing and emitting Arrow StringArrays as efficiently by
/// minimizing copying of the string values themselves both when inserting and
/// when emitting the final array.
///
/// 2. The entries in the final array are in the same order as they were
/// inserted.
///
/// Note it can be used as a HashSet by specifying the value type as `()`.
///
/// This is used by the special `COUNT DISTINCT` string aggregate function to
/// store the distinct values and by the `GROUP BY` operator to store the
/// distinct values for each group when they are single strings/// Equivalent to
/// `HashSet<String>` but with better performance for arrow data.
///
/// # Example
///
/// The following diagram shows how the map would store the four strings
/// "Foo", NULL, "Bar", "TheQuickBrownFox":
///
/// * `hashtable` stores entries for each distinct string that has been
/// inserted. The entries contain the payload as well as information about the
/// string value (either an offset or the actual bytes, see [`Entry`] for more
/// details)
///
/// * `offsets` stores offsets into `buffer` for each distinct string value,
/// following the same convention as the offsets in a `StringArray` or
/// `LargeStringArray`.
///
/// * `buffer` stores the actual byte data
///
/// * `null`: stores the index and payload of the null value, in this case the
/// second value (index 1)
///
/// ```text
/// ┌───────────────────────────────────┐    ┌─────┐    ┌────┐
/// │                ...                │    │  0  │    │FooB│
/// │ ┌──────────────────────────────┐  │    │  0  │    │arTh│
/// │ │      <Entry for "Bar">       │  │    │  3  │    │eQui│
/// │ │            len: 3            │  │    │  3  │    │ckBr│
/// │ │   offset_or_inline: "Bar"    │  │    │  6  │    │ownF│
/// │ │         payload:...          │  │    │     │    │ox  │
/// │ └──────────────────────────────┘  │    │     │    │    │
/// │                ...                │    └─────┘    └────┘
/// │ ┌──────────────────────────────┐  │
/// │ │<Entry for "TheQuickBrownFox">│  │    offsets    buffer
/// │ │           len: 16            │  │
/// │ │     offset_or_inline: 6      │  │    ┌───────────────┐
/// │ │         payload: ...         │  │    │    Some(1)    │
/// │ └──────────────────────────────┘  │    │ payload: ...  │
/// │                ...                │    └───────────────┘
/// └───────────────────────────────────┘
///                                              null
///               HashTable
/// ```
pub struct ArrowStringMap<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Underlying hash set for each distinct string
    map: hashbrown::raw::RawTable<Entry<O, V>>,
    /// Total size of the map in bytes
    map_size: usize,
    /// In progress arrow `Buffer` containing all string values
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct string value. These offsets as
    /// used directly to create the final `GenericStringArray`. The `i`th string
    /// is stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null
    /// values are stored as a zero length string.
    offsets: Vec<O>,
    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, usize)>,
}

impl<O: OffsetSizeTrait, V> Default for ArrowStringMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 128;
/// The initial size, in bytes, of the string data
const INITIAL_BUFFER_CAPACITY: usize = 8 * 1024;
impl<O: OffsetSizeTrait, V> ArrowStringMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new() -> Self {
        Self {
            map: hashbrown::raw::RawTable::with_capacity(INITIAL_MAP_CAPACITY),
            map_size: 0,
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![O::default()], // first offset is always 0
            random_state: RandomState::new(),
            hashes_buffer: vec![],
            null: None,
        }
    }

    /// Inserts each string from `values` into the map, invoking `payload_fn`
    /// for each value if *not* already present, deferring the allocation of the
    /// payload until it is needed.
    ///
    /// Note that this is different than a normal map that would replace the
    /// existing entry
    ///
    /// # Arguments:
    ///
    /// `values`: array whose values are inserted
    ///
    /// `make_payload_fn`:  invoked for each value that is not already present to
    /// create the payload
    ///
    /// `observe_payload_fn`: invoked once, in order in `values`, for each
    /// element in the map, with corresponding payload value
    ///
    /// returns the payload value for the entry, either the existing value or
    /// the the newly inserted value
    ///
    /// # Safety:
    ///
    /// Note that `make_payload_fn` and `observe_payload_fn` are only invoked
    /// with valid values from `values`.
    pub fn insert_if_new<MP, OP>(
        &mut self,
        values: &ArrayRef,
        mut make_payload_fn: MP,
        mut observe_payload_fn: OP,
    ) where
        MP: FnMut(Option<&[u8]>) -> V,
        OP: FnMut(V),
    {
        // step 1: compute hashes for the strings
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(values.len(), 0);
        create_hashes(&[values.clone()], &self.random_state, batch_hashes)
            // hash is supported for all string types and create_hashes only
            // returns errors for unsupported types
            .unwrap();

        // step 2: insert each string into the set, if not already present
        let values = values.as_string::<O>();

        // Ensure lengths are equivalent
        assert_eq!(values.len(), batch_hashes.len());

        for (value, &hash) in values.iter().zip(batch_hashes.iter()) {
            // hande null value
            let Some(value) = value else {
                let payload = if let Some(&(payload, _offset)) = self.null.as_ref() {
                    payload
                } else {
                    let payload = make_payload_fn(None);
                    let null_index = self.offsets.len() - 1;
                    // nulls need a zero length in the offset buffer
                    let offset = self.buffer.len();
                    self.offsets.push(O::from_usize(offset).unwrap());
                    self.null = Some((payload, null_index));
                    payload
                };
                observe_payload_fn(payload);
                continue;
            };

            // from here on only use bytes (not str/chars) for value
            let value = value.as_bytes();
            let value_len = O::from_usize(value.len()).unwrap();

            // value is a "small" string
            let payload = if value.len() <= SHORT_STRING_LEN {
                let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);

                // is value is already present in the set?
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value_len {
                        return false;
                    }
                    // value is stored inline so no need to consult buffer
                    // (this is the "small string optimization")
                    inline == header.offset_or_inline
                });

                if let Some(entry) = entry {
                    entry.payload
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it appears
                    // the output array, but store the actual bytes inline for
                    // comparison
                    self.buffer.append_slice(value);
                    self.offsets.push(O::from_usize(self.buffer.len()).unwrap());
                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        len: value_len,
                        offset_or_inline: inline,
                        payload,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                    payload
                }
            }
            // value is not a "small" string
            else {
                // Check if the value is already present in the set
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value_len {
                        return false;
                    }
                    // Need to compare the bytes in the buffer
                    // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
                    let existing_value =
                        unsafe { self.buffer.as_slice().get_unchecked(header.range()) };
                    value == existing_value
                });

                if let Some(entry) = entry {
                    entry.payload
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it
                    // appears the output array, and store that offset
                    // so the bytes can be compared if needed
                    let offset = self.buffer.len(); // offset of start fof data
                    self.buffer.append_slice(value);
                    self.offsets.push(O::from_usize(self.buffer.len()).unwrap());

                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        len: value_len,
                        offset_or_inline: offset,
                        payload,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                    payload
                }
            };
            observe_payload_fn(payload);
        }
    }

    /// Removes the first n distinct values inserted into this set, in the order
    /// they were inserted, and "shifts" the remaining values to the start of
    /// this set.
    ///
    /// Returns the values as StringArray
    ///
    /// The set is left with the remaining values
    ///
    /// # Arguments
    /// n: size of the array to take
    ///
    /// remove_fn: Called once for each existing element in the map and can
    /// modify the values. returns true if the value is within the first n
    /// values of the array. This function MUST be accurate, otherwise the
    /// returned array will be incorrect.
    ///
    /// # Safety
    ///
    /// The remove_fn **must** accurately report if the value is within the
    /// first n values of the array as well as update the state to reflect the
    /// new map size.
    ///
    /// # Performance
    ///
    /// This operation requires O(n) in the size of the strings remaining in the
    /// set. The current buffer is used to return the new array, and all
    /// remaining values are copied into a new buffer. Thus, this operation
    /// should be called sparingly and `n` should be large relative to the
    /// overall length.
    pub unsafe fn take_first_n<F>(&mut self, n: usize, mut remove_fn: F) -> ArrayRef
    where
        F: FnMut(&mut V) -> bool,
    {
        if n == self.len() {
            // swap with default
            return std::mem::take(self).into_state();
        }
        assert!(n < self.len());

        // offset of the last value to be removed
        let last_offset = self.offsets[n];

        // figure out offsets for emitted array and update self.offsets
        let offsets: ScalarBuffer<O> = {
            // as offsets are always monotonically increasing, shift the remaining
            // values down by the offset of the last value to be removed
            let mut new_offsets: Vec<_> = self
                .offsets
                .iter()
                .skip(n)
                .map(|offset| *offset - last_offset)
                .collect();
            assert_eq!(new_offsets[0], O::default()); // first offset is always 0

            // replace current offsets with new offsets and create offsets for
            // emitted array
            std::mem::swap(&mut self.offsets, &mut new_offsets);
            new_offsets.truncate(n + 1);
            new_offsets.into()
        };

        // figure out the values to be emitted and update self.values
        let values = self.buffer.finish(); // resets self.buffer
        let last_offset = last_offset.to_usize().unwrap();
        self.buffer
            .append_slice(&values.as_slice()[last_offset..values.len()]);

        let mut num_removed = 0;

        // figure out null for emitted arrays, and update self.null
        let nulls = self.null.take().and_then(|(mut payload, null_index)| {
            let should_remove = remove_fn(&mut payload);
            // Since we keep track the null was in the first n values
            // we can check the output of remove_fn
            if should_remove {
                assert!(null_index < n, "remove_fn is incorrect. Said to remove null, but null not in first n");
                self.null = None;
                num_removed += 1;
                Some(single_null_buffer(n, null_index))
            } else {
                assert!(null_index >= n, "remove_fn is incorrect. Said not to remove null, but null was in first n");
                // null was not in first n values, adjust index
                self.null = Some((payload, null_index - n));
                None
            }
        });

        // Erase removed entries, and shift any existing offsets down
        // SAFETY: remove_fn is correct
        // SAFETY: self.map outlives iterator
        unsafe {
            for bucket in self.map.iter() {
                let entry = bucket.as_mut();
                if remove_fn(&mut entry.payload) {
                    num_removed += 1;
                    // todo: does removing the bucket actually free any memory?
                    // if so, we should update memory accounting
                    // it might if V had memory pointed like Box or Arc...
                    self.map.erase(bucket);
                } else {
                    // Decrement offset if needed
                    entry.shift_offset(last_offset);
                }
            }
        }
        assert_eq!(
            num_removed, n,
            "incorrect remove_fn: Expected to remove {n}, but got {num_removed}",
        );

        // SAFETY: all the values that went in were valid utf8 thus so are all the
        // values that come out
        let new_arr = Arc::new(unsafe {
            GenericStringArray::new_unchecked(OffsetBuffer::new(offsets), values, nulls)
        });
        assert_eq!(new_arr.len(), n);
        new_arr
    }

    /// Converts this set into a `StringArray` or `LargeStringArray` with each
    /// distinct string value without any copies
    ///
    /// The strings are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn into_state(self) -> ArrayRef {
        let Self {
            map: _,
            map_size: _,
            offsets,
            mut buffer,
            random_state: _,
            hashes_buffer: _,
            null,
        } = self;

        // Only make a `NullBuffer` if there was a null value
        let nulls = null.map(|(_payload, null_index)| {
            let num_values = offsets.len() - 1;
            single_null_buffer(num_values, null_index)
        });
        let offsets: ScalarBuffer<O> = offsets.into();
        let values = buffer.finish();

        // SAFETY: all the values that went in were valid utf8 so are all the values that come out
        let array = unsafe {
            GenericStringArray::new_unchecked(OffsetBuffer::new(offsets), values, nulls)
        };
        Arc::new(array)
    }

    /// Total number of entries (including null, if present)
    pub fn len(&self) -> usize {
        self.non_null_len() + self.null.map(|_| 1).unwrap_or(0)
    }

    /// Is the set empty?
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.null.is_none()
    }

    /// Number of non null entries
    pub fn non_null_len(&self) -> usize {
        self.map.len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        self.map_size
            + self.buffer.capacity() * std::mem::size_of::<u8>()
            + self.offsets.allocated_size()
            + self.hashes_buffer.allocated_size()
    }
}

/// Returns a `NullBuffer` with a single null value at the given index
fn single_null_buffer(num_values: usize, null_index: usize) -> NullBuffer {
    let mut bool_builder = BooleanBufferBuilder::new(num_values);
    bool_builder.append_n(num_values, true);
    bool_builder.set_bit(null_index, false);
    NullBuffer::from(bool_builder.finish())
}

impl<O: OffsetSizeTrait, V> Debug for ArrowStringMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowStringHashSet")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("buffer", &self.buffer)
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Maximum size of a string that can be inlined in the hash table
const SHORT_STRING_LEN: usize = mem::size_of::<usize>();

/// Entry that is stored in a `ArrowStringHashSet` that represents a string
/// that is either stored inline or in the buffer
///
/// This helps the case where there are many short (less than 8 bytes) strings
/// that are the same (e.g. "MA", "CA", "NY", "TX", etc)
///
/// ```text
///                                                                ┌──────────────────┐
///                                                  ─ ─ ─ ─ ─ ─ ─▶│...               │
///                                                 │              │TheQuickBrownFox  │
///                                                                │...               │
///                                                 │              │                  │
///                                                                └──────────────────┘
///                                                 │               buffer of u8
///
///                                                 │
///                        ┌────────────────┬───────────────┬───────────────┐
///  Storing               │                │ starting byte │  length, in   │
///  "TheQuickBrownFox"    │   hash value   │   offset in   │  bytes (not   │
///  (long string)         │                │    buffer     │  characters)  │
///                        └────────────────┴───────────────┴───────────────┘
///                              8 bytes          8 bytes       4 or 8
///
///
///                         ┌───────────────┬─┬─┬─┬─┬─┬─┬─┬─┬───────────────┐
/// Storing "foobar"        │               │ │ │ │ │ │ │ │ │  length, in   │
/// (short string)          │  hash value   │?│?│f│o│o│b│a│r│  bytes (not   │
///                         │               │ │ │ │ │ │ │ │ │  characters)  │
///                         └───────────────┴─┴─┴─┴─┴─┴─┴─┴─┴───────────────┘
///                              8 bytes         8 bytes        4 or 8
/// ```
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// hash of the string value (stored to avoid recomputing it in hash table
    /// check)
    hash: u64,
    /// if len =< SHORT_STRING_LEN: the string data inlined
    /// if len > SHORT_STRING_LEN, the offset of where the data starts
    offset_or_inline: usize,
    /// length of the string, in bytes (use O here so we use only i32 for
    /// strings, rather 64 bit usize)
    len: O,
    /// value stored by the entry
    payload: V,
}

impl<O, V> Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Updates this entry's offset down by the given amount
    /// if needed
    fn shift_offset(&mut self, amount: usize) {
        if self.len > O::from_usize(SHORT_STRING_LEN).unwrap() {
            if self.offset_or_inline < amount {
                panic!(
                    "underflow, offset_or_inline: {}, amount: {}",
                    self.offset_or_inline, amount
                );
            }
            self.offset_or_inline = self
                .offset_or_inline
                .checked_sub(amount)
                .expect("underflow");
        }
    }

    /// returns self.offset..self.offset + self.len
    fn range(&self) -> Range<usize> {
        self.offset_or_inline..self.offset_or_inline + self.len.to_usize().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow_array::StringArray;
    use hashbrown::HashMap;

    #[test]
    fn string_set_empty() {
        let mut set = ArrowStringSet::<i32>::new();
        let array: ArrayRef = Arc::new(StringArray::new_null(0));
        set.insert(&array);
        assert_eq!(set.len(), 0);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[]);
    }

    #[test]
    fn string_set_one_null() {
        let mut set = ArrowStringSet::<i32>::new();
        let array: ArrayRef = Arc::new(StringArray::new_null(1));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn string_set_many_null() {
        let mut set = ArrowStringSet::<i32>::new();
        let array: ArrayRef = Arc::new(StringArray::new_null(11));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn string_set_basic_i32() {
        test_string_set_basic::<i32>();
    }

    #[test]
    fn string_set_basic_i64() {
        test_string_set_basic::<i64>();
    }

    fn test_string_set_basic<O: OffsetSizeTrait>() {
        // basic test for mixed small and large string values
        let values = GenericStringArray::<O>::from(vec![
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

        let mut set = ArrowStringSet::<O>::new();
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // values mut appear be in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("b"),
                Some("CXCCCCCCCC"),
                Some(""),
                Some("cbcxx"),
                None,
                Some("AAAAAAAA"),
                Some("BBBBBQBBB"),
            ],
        );
    }

    #[test]
    fn string_set_non_utf8_32() {
        test_string_set_non_utf8::<i32>();
    }

    #[test]
    fn string_set_non_utf8_64() {
        test_string_set_non_utf8::<i64>();
    }

    fn test_string_set_non_utf8<O: OffsetSizeTrait>() {
        // basic test for mixed small and large string values
        let values = GenericStringArray::<O>::from(vec![
            Some("a"),
            Some("✨🔥"),
            Some("🔥"),
            Some("✨✨✨"),
            Some("foobarbaz"),
            Some("🔥"),
            Some("✨🔥"),
        ]);

        let mut set = ArrowStringSet::<O>::new();
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // strings mut appear be in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("✨🔥"),
                Some("🔥"),
                Some("✨✨✨"),
                Some("foobarbaz"),
            ],
        );
    }

    // asserts that the set contains the expected strings, in the same order
    fn assert_set<O: OffsetSizeTrait>(set: ArrowStringSet<O>, expected: &[Option<&str>]) {
        let strings = set.into_state();
        let strings = strings.as_string::<O>();
        let state = strings.into_iter().collect::<Vec<_>>();
        assert_eq!(state, expected);
    }

    // inserting strings into the set does not increase reported memoyr
    #[test]
    fn test_string_set_memory_usage() {
        let strings1 = GenericStringArray::<i32>::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCC"), // 10 bytes
            Some("AAAAAAAA"),   // 8 bytes
            Some("BBBBBQBBB"),  // 9 bytes
        ]);
        let total_strings1_len = strings1
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values1: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings1));

        // Much larger strings in strings2
        let strings2 = GenericStringArray::<i32>::from(vec![
            "FOO".repeat(1000),
            "BAR".repeat(2000),
            "BAZ".repeat(3000),
        ]);
        let total_strings2_len = strings2
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values2: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings2));

        let mut set = ArrowStringSet::<i32>::new();
        let size_empty = set.size();

        set.insert(&values1);
        let size_after_values1 = set.size();
        assert!(size_empty < size_after_values1);
        assert!(
            size_after_values1 > total_strings1_len,
            "expect {size_after_values1} to be more than {total_strings1_len}"
        );
        assert!(size_after_values1 < total_strings1_len + total_strings2_len);

        // inserting the same strings should not affect the size
        set.insert(&values1);
        assert_eq!(set.size(), size_after_values1);

        // inserting the large strings should increase the reported size
        set.insert(&values2);
        let size_after_values2 = set.size();
        assert!(size_after_values2 > size_after_values1);
        assert!(size_after_values2 > total_strings1_len + total_strings2_len);
    }

    #[test]
    fn test_map() {
        let input = vec![
            // Note mix of short/long strings
            Some("A"),
            Some("bcdefghijklmnop"),
            Some("X"),
            Some("Y"),
            None,
            Some("qrstuvqxyzhjwya"),
            Some("✨🔥"),
            Some("🔥"),
            Some("🔥🔥🔥🔥🔥🔥"),
        ];

        let mut test_map = TestMap::new();
        test_map.insert(&input);
        test_map.insert(&input); // put it in twice
        let expected_output: ArrayRef = Arc::new(StringArray::from(input));
        assert_eq!(&test_map.into_array(), &expected_output);
    }

    #[test]
    fn test_string_shrink_to_n() {
        let input = vec![
            // Note mix of short/long strings
            Some("A"),
            Some("bcdefghijklmnop"),
            Some("X"),
            None,
            Some("🔥🔥🔥🔥🔥🔥"),
            Some("qrstuvqxyzhjwya"),
            Some("Y"),
        ];

        for i in 0..input.len() {
            test_shrink_case(&input, i);
        }
    }

    // Creates a set with the given unique strings and then removes the first n
    // strings, validating the result
    fn test_shrink_case(strings: &[Option<&str>], n: usize) {
        let mut test_map = TestMap::new();
        test_map.insert(strings);
        let expected_first_n_strings: ArrayRef =
            Arc::new(StringArray::from_iter(strings.iter().take(n)));
        let expected_last_n_strings: ArrayRef =
            Arc::new(StringArray::from_iter(strings.iter().skip(n)));

        let first_n_strings = test_map.take_first_n(n);
        assert_eq!(&first_n_strings, &expected_first_n_strings);

        let last_n_strings = test_map.into_array();
        assert_eq!(&last_n_strings, &expected_last_n_strings);
    }

    #[test]
    fn test_string_shrink_and_insert() {
        let input = vec![
            // Note mix of short/long strings
            Some("A"),
            Some("bcdefghijklmnop"),
            Some("X"),
            None,
            Some("🔥🔥🔥🔥🔥🔥"),
            Some("qrstuvqxyzhjwya"),
            Some("Y"),
        ];

        for i in 0..input.len() {
            test_shrink_and_insert_case(&input, i);
        }
    }

    // Creates a set with the given unique strings and removes the first n, reinserts them, and validates the result
    fn test_shrink_and_insert_case(strings: &[Option<&str>], n: usize) {
        let mut test_map = TestMap::new();
        test_map.insert(strings);
        test_map.take_first_n(n);

        // insert the first n strings again (but this time they will be at the end)
        test_map.insert(&strings[0..n]);

        let expected: ArrayRef = Arc::new(StringArray::from_iter(
            strings.iter().skip(n).chain(strings.iter().take(n)),
        ));
        assert_eq!(&test_map.into_array(), &expected);
    }

    #[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
    struct TestPayload {
        // store the string value to check against input
        index: usize, // store the index of the string (each new string gets the next sequential input)
    }

    /// Wraps an [`ArrowStringMap`], validating its invariants
    struct TestMap {
        map: ArrowStringMap<i32, TestPayload>,
        // stores distinct strings seen, in order
        strings: Vec<Option<String>>,
        // map strings to index in strings
        indexes: HashMap<Option<String>, usize>,
    }

    impl Debug for TestMap {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestMap")
                .field("map", &"...")
                .field("strings", &self.strings)
                .field("indexes", &self.indexes)
                .finish()
        }
    }

    impl TestMap {
        /// creates a map with TestPayloads for the given strings and then
        /// validates the payloads
        fn new() -> Self {
            Self {
                map: ArrowStringMap::new(),
                strings: vec![],
                indexes: HashMap::new(),
            }
        }

        /// Inserts strings into the map
        fn insert(&mut self, strings: &[Option<&str>]) {
            let string_array = StringArray::from(strings.to_vec());
            let arr: ArrayRef = Arc::new(string_array);

            let mut next_index = self.indexes.len();
            let mut actual_new_strings = vec![];
            let mut actual_seen_indexes = vec![];
            // update self with new values, keeping track of newly added values
            for str in strings {
                let str = str.map(|s| s.to_string());
                let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
                    actual_new_strings.push(str.clone());
                    let index = self.strings.len();
                    self.strings.push(str.clone());
                    self.indexes.insert(str, index);
                    index
                });
                actual_seen_indexes.push(index);
            }

            // insert the values into the map, recording what we did
            let mut seen_new_strings = vec![];
            let mut seen_indexes = vec![];
            self.map.insert_if_new(
                &arr,
                |s| {
                    let value = s
                        .map(|s| String::from_utf8(s.to_vec()).expect("Non utf8 string"));
                    let index = next_index;
                    next_index += 1;
                    seen_new_strings.push(value);
                    TestPayload { index }
                },
                |payload| {
                    seen_indexes.push(payload.index);
                },
            );

            assert_eq!(actual_seen_indexes, seen_indexes);
            assert_eq!(actual_new_strings, seen_new_strings);
        }

        /// call `self.map.take_first_n()` validating that the strings are in the same
        fn take_first_n(&mut self, n: usize) -> ArrayRef {
            let arr = unsafe { self.map.take_first_n(n, |payload| payload.index < n) };

            let first_n = {
                let mut t = self.strings.split_off(n);
                std::mem::swap(&mut self.strings, &mut t);
                t
            };

            // reset indexes
            self.indexes = self
                .strings
                .iter()
                .enumerate()
                .map(|(i, s)| (s.clone(), i))
                .collect();

            let expected: ArrayRef = Arc::new(StringArray::from(first_n));
            assert_eq!(&arr, &expected);
            arr
        }

        /// Call `self.map.into_array()` validating that the strings are in the same
        /// order as they were inserted
        fn into_array(self) -> ArrayRef {
            let Self {
                map,
                strings,
                indexes: _,
            } = self;

            let arr = map.into_state();
            let expected: ArrayRef = Arc::new(StringArray::from(strings));
            assert_eq!(&arr, &expected);
            arr
        }
    }
}
