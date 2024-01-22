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

//! Specialized implementation of `COUNT DISTINCT` for `StringArray` and `LargeStringArray`

use ahash::RandomState;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::{BufferBuilder, OffsetBuffer, ScalarBuffer};
use datafusion_common::cast::as_list_array;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::ScalarValue;
use datafusion_execution::memory_pool::proxy::RawTableAllocExt;
use datafusion_expr::Accumulator;
use std::fmt::Debug;
use std::mem;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(super) struct StringDistinctCountAccumulator<O: OffsetSizeTrait>(
    Mutex<SSOStringHashSet<O>>,
);
impl<O: OffsetSizeTrait> StringDistinctCountAccumulator<O> {
    pub(super) fn new() -> Self {
        Self(Mutex::new(SSOStringHashSet::<O>::new()))
    }
}

impl<O: OffsetSizeTrait> Accumulator for StringDistinctCountAccumulator<O> {
    fn state(&self) -> datafusion_common::Result<Vec<ScalarValue>> {
        // TODO this should not need a lock/clone (should make
        // `Accumulator::state` take a mutable reference)
        // see https://github.com/apache/arrow-datafusion/pull/8925
        let mut lk = self.0.lock().unwrap();
        let set: &mut SSOStringHashSet<_> = &mut lk;
        // take the state out of the string set and replace with default
        let set = std::mem::take(set);
        let arr = set.into_state();
        let list = Arc::new(array_into_list_array(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.lock().unwrap().insert(values[0].clone());

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
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

    fn evaluate(&self) -> datafusion_common::Result<ScalarValue> {
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

/// Entry that is stored in a `SSOStringHashSet` that represents a string
/// that is either stored inline or in the buffer
///
/// This helps the case where there are many short (less than 8 bytes) strings
/// that are the same (e.g. "MA", "CA", "NY", "TX", etc)
///
/// ```text
///                                                                ┌──────────────────┐
///                                                                │...               │
///                                                                │TheQuickBrownFox  │
///                                                  ─ ─ ─ ─ ─ ─ ─▶│...               │
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
struct SSOStringHeader {
    /// hash of the string value (stored to avoid recomputing it in hash table
    /// check)
    hash: u64,
    /// if len =< SHORT_STRING_LEN: the string data inlined
    /// if len > SHORT_STRING_LEN, the offset of where the data starts
    offset_or_inline: usize,
    /// length of the string, in bytes
    len: usize,
}

impl SSOStringHeader {
    /// returns self.offset..self.offset + self.len
    fn range(&self) -> Range<usize> {
        self.offset_or_inline..self.offset_or_inline + self.len
    }
}

/// HashSet optimized for storing `String` and `LargeString` values
/// and producing the final set as a GenericStringArray with minimal copies.
///
/// Equivalent to `HashSet<String>` but with better performance for arrow data.
struct SSOStringHashSet<O> {
    /// Underlying hash set for each distinct string
    map: hashbrown::raw::RawTable<SSOStringHeader>,
    /// Total size of the map in bytes
    map_size: usize,
    /// In progress arrow `Buffer` containing all string values
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct string value. These offsets
    /// as used directly to create the final `GenericStringArray`
    offsets: Vec<O>,
    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
}

impl<O: OffsetSizeTrait> Default for SSOStringHashSet<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> SSOStringHashSet<O> {
    fn new() -> Self {
        Self {
            map: hashbrown::raw::RawTable::new(),
            map_size: 0,
            buffer: BufferBuilder::new(0),
            offsets: vec![O::default()], // first offset is always 0
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

        // step 2: insert each string into the set, if not already present
        let values = values.as_string::<O>();

        // Ensure lengths are equivalent (to guard unsafe values calls below)
        assert_eq!(values.len(), batch_hashes.len());

        for (value, &hash) in values.iter().zip(batch_hashes.iter()) {
            // count distinct ignores nulls
            let Some(value) = value else {
                continue;
            };

            // from here on only use bytes (not str/chars) for value
            let value = value.as_bytes();

            // value is a "small" string
            if value.len() <= SHORT_STRING_LEN {
                let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);

                // is value is already present in the set?
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value.len() {
                        return false;
                    }
                    // value is stored inline so no need to consult buffer
                    // (this is the "small string optimization")
                    inline == header.offset_or_inline
                });

                // if no existing entry, make a new one
                if entry.is_none() {
                    // Put the small values into buffer and offsets so it appears
                    // the output array, but store the actual bytes inline for
                    // comparison
                    self.buffer.append_slice(value);
                    self.offsets.push(O::from_usize(self.buffer.len()).unwrap());
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
            // value is not a "small" string
            else {
                // Check if the value is already present in the set
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value.len() {
                        return false;
                    }
                    // Need to compare the bytes in the buffer
                    // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
                    let existing_value =
                        unsafe { self.buffer.as_slice().get_unchecked(header.range()) };
                    value == existing_value
                });

                // if no existing entry, make a new one
                if entry.is_none() {
                    // Put the small values into buffer and offsets so it
                    // appears the output array, and store that offset
                    // so the bytes can be compared if needed
                    let offset = self.buffer.len(); // offset of start fof data
                    self.buffer.append_slice(value);
                    self.offsets.push(O::from_usize(self.buffer.len()).unwrap());

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

    /// Converts this set into a `StringArray` or `LargeStringArray` with each
    /// distinct string value without any copies
    fn into_state(self) -> ArrayRef {
        let Self {
            map: _,
            map_size: _,
            offsets,
            mut buffer,
            random_state: _,
            hashes_buffer: _,
        } = self;

        let offsets: ScalarBuffer<O> = offsets.into();
        let values = buffer.finish();
        let nulls = None; // count distinct ignores nulls so intermediate state never has nulls

        // SAFETY: all the values that went in were valid utf8 so are all the values that come out
        let array = unsafe {
            GenericStringArray::new_unchecked(OffsetBuffer::new(offsets), values, nulls)
        };
        Arc::new(array)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    fn size(&self) -> usize {
        self.map_size
            + self.buffer.capacity() * std::mem::size_of::<u8>()
            + self.offsets.capacity() * std::mem::size_of::<O>()
            + self.hashes_buffer.capacity() * std::mem::size_of::<u64>()
    }
}

impl<O: OffsetSizeTrait> Debug for SSOStringHashSet<O> {
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
    use super::*;
    use arrow::array::ArrayRef;
    use arrow_array::StringArray;
    #[test]
    fn string_set_empty() {
        for values in [StringArray::new_null(0), StringArray::new_null(11)] {
            let mut set = SSOStringHashSet::<i32>::new();
            set.insert(Arc::new(values.clone()));
            assert_set(set, &[]);
        }
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

        let mut set = SSOStringHashSet::<O>::new();
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

        let mut set = SSOStringHashSet::<O>::new();
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
    fn assert_set<O: OffsetSizeTrait>(
        set: SSOStringHashSet<O>,
        expected: &[Option<&str>],
    ) {
        let strings = set.into_state();
        let strings = strings.as_string::<O>();
        let mut state = strings.into_iter().collect::<Vec<_>>();
        state.sort();
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

        let mut set = SSOStringHashSet::<i32>::new();
        let size_empty = set.size();

        set.insert(values1.clone());
        let size_after_values1 = set.size();
        assert!(size_empty < size_after_values1);
        assert!(
            size_after_values1 > total_strings1_len,
            "expect {size_after_values1} to be more than {total_strings1_len}"
        );
        assert!(size_after_values1 < total_strings1_len + total_strings2_len);

        // inserting the same strings should not affect the size
        set.insert(values1.clone());
        assert_eq!(set.size(), size_after_values1);

        // inserting the large strings should increase the reported size
        set.insert(values2);
        let size_after_values2 = set.size();
        assert!(size_after_values2 > size_after_values1);
        assert!(size_after_values2 > total_strings1_len + total_strings2_len);
    }
}
