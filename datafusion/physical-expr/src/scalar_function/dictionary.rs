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

//! The dictionary side of a peeled scalar-function call: the fields the call
//! is made with, the compaction that narrows a dictionary to what a batch
//! references, and the memo that carries results across the batches of a
//! column chunk. [`super::ScalarFunctionExpr`] owns the locks and the tier
//! decisions; this module owns the policy under them.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, DictionaryArray, PrimitiveArray, UInt64Array,
    new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{concat, take};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, FieldRef};
use arrow::downcast_dictionary_array;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::ColumnarValue;

/// The two fields a peeled call needs, which depend only on the plan: the
/// dictionary argument re-typed to its values, and `return_field` with any
/// planned dictionary wrapper stripped.
#[derive(Debug, Clone)]
pub(super) struct PeeledFields {
    pub(super) index: usize,
    /// The argument field these were built from; a batch presenting a
    /// different one rebuilds them rather than reusing these.
    pub(super) source: FieldRef,
    pub(super) argument: FieldRef,
    pub(super) output: FieldRef,
}

/// What a lookup found: the stored result, or the identity built on the way,
/// so the caller can hash it without reading the array again.
pub(super) enum Lookup {
    Evaluated(ArrayRef),
    Absent(Option<ValuesIdentity>),
}

/// What an expression remembers about a dictionary it is handed.
pub(super) enum Recollection {
    /// Never seen; now recorded, so a repeat is recognisable.
    Unknown,
    /// Seen before, so evaluating all of its values will pay for itself.
    SeenBefore,
    /// Already evaluated.
    Evaluated(ArrayRef),
}

/// How many dictionaries results are kept for. One expression is shared by
/// every partition of a plan, so a single slot would be evicted by whichever
/// partition ran last.
pub(super) const MEMOIZED_DICTIONARIES: usize = 8;

/// How much one memo may hold alive, values and results together — sized to
/// admit a worst-case Parquet page dictionary (at most 1 MiB by default) with
/// its result.
pub(super) const MEMOIZED_BYTES: usize = 4 * 1024 * 1024;

/// What an expression keeps between batches: results for the dictionaries it
/// has evaluated, and a cheap note of the ones it has only glimpsed. Both are
/// evicted in insertion order and only ever hold dictionaries that proved
/// they repeat, so an eviction costs one re-evaluation, not a wrong result.
#[derive(Debug, Default)]
pub(super) struct Memo {
    evaluated: Vec<Memoized>,
    /// What the entries hold alive, maintained so admission is O(1).
    bytes: usize,
    /// Hashes of dictionaries seen once. A false match only costs evaluating
    /// a dictionary in full one batch early; results are matched exactly.
    glimpsed: Vec<u64>,
}

impl Memo {
    /// The stored result for `values`, if any.
    pub(super) fn find(&self, values: &ArrayRef, scalars: &[ScalarValue]) -> Lookup {
        if self.evaluated.is_empty() {
            return Lookup::Absent(None);
        }
        let known = ValuesIdentity::of(values);
        let found = self.evaluated.iter().find(|entry| {
            entry.scalars == scalars
                && (Arc::ptr_eq(&entry.values, values) || entry.identity == known)
        });
        match found {
            Some(entry) => Lookup::Evaluated(Arc::clone(&entry.output)),
            None => Lookup::Absent(Some(known)),
        }
    }

    /// Records a sighting; answers whether this dictionary was seen before.
    pub(super) fn note(&mut self, hash: u64) -> Recollection {
        if self.glimpsed.contains(&hash) {
            return Recollection::SeenBefore;
        }
        if self.glimpsed.len() == MEMOIZED_DICTIONARIES {
            self.glimpsed.remove(0);
        }
        self.glimpsed.push(hash);
        Recollection::Unknown
    }

    /// Keeps `output` for the next batch that arrives with the same values.
    /// The memo holds its entries alive, so admission is bounded by bytes as
    /// well as by count; what cannot fit is recomputed per batch instead.
    pub(super) fn keep(
        &mut self,
        values: &ArrayRef,
        scalars: &[ScalarValue],
        output: &ArrayRef,
    ) {
        let bytes = values.get_array_memory_size() + output.get_array_memory_size();
        if bytes > MEMOIZED_BYTES {
            return;
        }
        while self.evaluated.len() >= MEMOIZED_DICTIONARIES
            || self.bytes + bytes > MEMOIZED_BYTES
        {
            let evicted = self.evaluated.remove(0);
            self.bytes -= evicted.bytes;
        }
        self.bytes += bytes;
        self.evaluated.push(Memoized {
            values: Arc::clone(values),
            identity: ValuesIdentity::of(values),
            scalars: scalars.to_vec(),
            output: Arc::clone(output),
            bytes,
        });
    }
}

/// The result of evaluating a function over one dictionary's values.
#[derive(Debug)]
struct Memoized {
    /// The values these results came from, kept alive so the addresses
    /// [`ValuesIdentity`] compares cannot be reused by an unrelated array.
    values: ArrayRef,
    identity: ValuesIdentity,
    /// The other arguments at the time; a different format string is a
    /// different result.
    scalars: Vec<ScalarValue>,
    output: ArrayRef,
    /// What this entry holds alive, counted once at admission.
    bytes: usize,
}

/// The memory an array views, compared by address: two arrays whose data
/// agree on all of it — children included, so nested values are told apart
/// by more than the buffers of their top level — hold the same elements.
#[derive(Debug)]
pub(super) struct ValuesIdentity(ArrayData);

impl PartialEq for ValuesIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

impl ValuesIdentity {
    fn of(array: &ArrayRef) -> Self {
        Self(array.to_data())
    }

    /// This identity as a hash: equal identities hash alike.
    pub(super) fn hash(&self) -> u64 {
        fn hash_data(data: &ArrayData, hasher: &mut DefaultHasher) {
            // What `ArrayData::ptr_eq` compares.
            data.data_type().hash(hasher);
            hasher.write_usize(data.len());
            hasher.write_usize(data.offset());
            for buffer in data.buffers() {
                hasher.write_usize(buffer.as_ptr() as usize);
                hasher.write_usize(buffer.len());
            }
            if let Some(nulls) = data.nulls() {
                hasher.write_usize(nulls.buffer().as_ptr() as usize);
                hasher.write_usize(nulls.offset());
            }
            for child in data.child_data() {
                hash_data(child, hasher);
            }
        }
        let mut hasher = DefaultHasher::new();
        hash_data(&self.0, &mut hasher);
        hasher.finish()
    }

    /// The hash of `array`'s identity.
    pub(super) fn hash_of(array: &ArrayRef) -> u64 {
        Self::of(array).hash()
    }
}

/// Rewrites a dictionary so its values are exactly the ones referenced by this
/// batch: unreferenced values are dropped, and null keys are redirected to one
/// appended NULL value slot. Returns `None` when the batch references more
/// than half of `row_budget` distinct values, i.e. peeling would not pay off;
/// `None` as a budget disables that check.
///
/// [`DictionaryArray::occupancy`] answers the same question, but always scans
/// every key; this pass abandons high-cardinality batches part-way.
pub(super) fn compact_dictionary(
    array: &ArrayRef,
    row_budget: Option<usize>,
) -> Result<Option<ArrayRef>> {
    fn rebuild<K: ArrowDictionaryKeyType>(
        dictionary: &DictionaryArray<K>,
        row_budget: Option<usize>,
    ) -> Result<Option<ArrayRef>> {
        let values = dictionary.values();
        let keys = dictionary.keys();

        // Discovery over a bitmap: everything here is sized by the dictionary,
        // and nothing is built until the batch is known to be worth compacting.
        let mut referenced = vec![0u64; values.len().div_ceil(64)];
        let mut referenced_count = 0usize;
        let mut null_slots = 0usize;
        let mark = |key: usize, referenced: &mut [u64], count: &mut usize| {
            let bit = 1u64 << (key % 64);
            let word = &mut referenced[key / 64];
            let fresh = *word & bit == 0;
            *word |= bit;
            *count += usize::from(fresh);
            fresh
        };
        if keys.null_count() == 0 {
            // Hot path: raw key slice, no per-key validity checks.
            for key in keys.values() {
                if mark(key.as_usize(), &mut referenced, &mut referenced_count)
                    && row_budget.is_some_and(|rows| referenced_count * 2 > rows)
                {
                    return Ok(None);
                }
            }
        } else {
            for key in keys.iter() {
                let grew = match key {
                    None if null_slots == 0 => {
                        null_slots = 1;
                        true
                    }
                    None => false,
                    Some(key) => {
                        mark(key.as_usize(), &mut referenced, &mut referenced_count)
                    }
                };
                if grew
                    && row_budget
                        .is_some_and(|rows| (referenced_count + null_slots) * 2 > rows)
                {
                    return Ok(None);
                }
            }
        }
        if referenced_count == values.len() && null_slots == 0 {
            return Ok(Some(Arc::new(dictionary.clone())));
        }

        // A value's new position is the number of referenced values before it,
        // counted from the bitmap. Null keys go to the appended NULL slot,
        // which sits one past the referenced values and can overflow a narrow
        // key type — such a batch is left unpeeled rather than failed.
        let null_slot = K::Native::from_usize(referenced_count);
        if null_slots > 0 && null_slot.is_none() {
            return Ok(None);
        }
        let null_slot = null_slot.unwrap_or_default();
        let mut preceding = Vec::with_capacity(referenced.len());
        let mut compacted_indices: Vec<u64> = Vec::with_capacity(referenced_count);
        for (index, word) in referenced.iter().enumerate() {
            preceding.push(compacted_indices.len());
            let mut bits = *word;
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                compacted_indices.push((index * 64 + bit) as u64);
                bits &= bits - 1;
            }
        }
        let position = |key: usize| {
            let before = referenced[key / 64] & ((1u64 << (key % 64)) - 1);
            // In range: compacted positions only ever shrink.
            K::Native::from_usize(preceding[key / 64] + before.count_ones() as usize)
                .unwrap_or_default()
        };
        // Garbage under a null key is not a valid position, so those batches
        // take the checked path.
        let new_keys: PrimitiveArray<K> = if keys.null_count() == 0 {
            keys.unary(|key| position(key.as_usize()))
        } else {
            PrimitiveArray::from_iter_values(
                keys.iter()
                    .map(|key| key.map_or(null_slot, |key| position(key.as_usize()))),
            )
        };

        let compacted =
            take(values.as_ref(), &UInt64Array::from(compacted_indices), None)?;
        let new_values = if null_slots > 0 {
            concat(&[
                compacted.as_ref(),
                new_null_array(values.data_type(), 1).as_ref(),
            ])?
        } else {
            compacted
        };
        Ok(Some(Arc::new(DictionaryArray::<K>::try_new(
            new_keys, new_values,
        )?)))
    }

    downcast_dictionary_array!(
        array => rebuild(array, row_budget),
        other => internal_err!("expected a dictionary array, got {other:?}")
    )
}

/// `dictionary` with `nulls` as its keys' null buffer. Compaction redirects
/// null keys to a value slot so `f` can answer for them; where it answered
/// NULL, the original nulls go back on the keys so the result's physical
/// nulls agree with its logical ones — [`Array::is_null`] on a dictionary
/// consults only the keys.
pub(super) fn with_key_nulls(
    dictionary: &ArrayRef,
    nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    fn restore<K: ArrowDictionaryKeyType>(
        dictionary: &DictionaryArray<K>,
        nulls: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let keys =
            PrimitiveArray::<K>::new(dictionary.keys().values().clone(), nulls.cloned());
        Ok(Arc::new(DictionaryArray::try_new(
            keys,
            Arc::clone(dictionary.values()),
        )?))
    }

    downcast_dictionary_array!(
        dictionary => restore(dictionary, nulls),
        other => internal_err!("expected a dictionary array, got {other:?}")
    )
}

/// The scalar arguments a memoized result was computed with; a different trim
/// set or format string makes it a different result.
pub(super) fn scalar_arguments(
    args: &[ColumnarValue],
    dictionary_index: usize,
) -> Vec<ScalarValue> {
    args.iter()
        .enumerate()
        .filter_map(|(index, arg)| match arg {
            ColumnarValue::Scalar(scalar) if index != dictionary_index => {
                Some(scalar.clone())
            }
            _ => None,
        })
        .collect()
}

/// Replaces dictionary-encoded scalars, and their fields' data types, with the
/// single value each of them wraps.
pub(super) fn unwrap_scalar_dictionaries(
    args: &[ColumnarValue],
    arg_fields: &[FieldRef],
) -> (Vec<ColumnarValue>, Vec<FieldRef>) {
    let mut args = args.to_vec();
    let mut arg_fields = arg_fields.to_vec();
    for (index, arg) in args.iter_mut().enumerate() {
        if let ColumnarValue::Scalar(ScalarValue::Dictionary(_, value)) = arg {
            let value = value.as_ref().clone();
            arg_fields[index] = Arc::new(
                arg_fields[index]
                    .as_ref()
                    .clone()
                    .with_data_type(value.data_type()),
            );
            *arg = ColumnarValue::Scalar(value);
        }
    }
    (args, arg_fields)
}
