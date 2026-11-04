// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::aggregates::group_values::multi_group_by::GroupColumn;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, DictionaryArray, Int64Array,
    NullBufferBuilder, PrimitiveArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::compute::take;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use arrow::error::ArrowError;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{DataFusionError, Result, exec_err};
use datafusion_execution::memory_pool::proxy::HashTableAllocExt;
use datafusion_expr::GroupSelection;
use hashbrown::{HashMap, hash_table::HashTable};
use std::mem::size_of;
use std::sync::{Arc, Weak};

use crate::aggregates::AGGREGATION_HASH_SEED;

// `usize::MAX` is already the unresolved marker in `val_to_inner`.
const NULL_INNER_SLOT: usize = usize::MAX - 1;

/// [`GroupColumn`] for dictionary-encoded columns with key type `K`.
///
/// `inner` holds one slot per distinct value seen across all batches.
/// Each group stores the dictionary key for its slot in `inner`, so groups
/// with the same value share a slot rather than duplicating data.
pub struct DictionaryGroupValuesColumn<K: ArrowDictionaryKeyType + Send + Sync> {
    /// Deduplicated store of distinct values.
    inner: Box<dyn GroupColumn>,
    /// Dictionary key for each group.
    group_to_inner: Vec<K::Native>,
    /// Validity of `group_to_inner`.
    group_nulls: NullBufferBuilder,
    /// Lookup table mapping `(value_hash, inner_slot)` for each non-null distinct value.
    value_dedup: HashTable<(u64, usize)>,
    /// Tracked allocation size of `value_dedup` for memory accounting via `size()`.
    value_dedup_size: usize,
    /// Hash seed — must match `create_hashes` so hashes are consistent across calls.
    random_state: RandomState,
    /// Maps `val_idx → inner_slot` for the values array in `cached_values`.
    ///
    /// Valid only while `cached_values` is `Some` and matches the incoming
    /// values array; `usize::MAX` means "not yet resolved". Entries are only
    /// ever filled in, never invalidated, because `inner` slots are stable
    /// under append. `take_n` remaps `inner` and therefore drops the cache.
    val_to_inner: Vec<usize>,
    /// Hashes of the values array in `cached_values`, one per `val_idx`.
    val_hashes: Vec<u64>,
    /// Weak reference to the values array that `val_hashes` and `val_to_inner`
    /// were built for. A matching live array reuses both caches without
    /// retaining an input dictionary after its batch is released.
    cached_values: Option<Weak<dyn Array>>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> DictionaryGroupValuesColumn<K> {
    pub fn new(inner: Box<dyn GroupColumn>) -> Self {
        Self {
            inner,
            group_to_inner: Vec::new(),
            group_nulls: NullBufferBuilder::new(0),
            value_dedup: HashTable::new(),
            value_dedup_size: 0,
            random_state: AGGREGATION_HASH_SEED,
            val_to_inner: Vec::default(),
            val_hashes: Vec::default(),
            cached_values: None,
        }
    }

    fn key_type_fits(num_values: usize) -> bool {
        let max: usize = match K::DATA_TYPE {
            DataType::Int8 => i8::MAX as usize,
            DataType::Int16 => i16::MAX as usize,
            DataType::Int32 => i32::MAX as usize,
            DataType::Int64 => i64::MAX as usize,
            DataType::UInt8 => u8::MAX as usize,
            DataType::UInt16 => u16::MAX as usize,
            DataType::UInt32 => u32::MAX as usize,
            DataType::UInt64 => usize::MAX,
            _ => return false,
        };
        num_values == 0 || num_values - 1 <= max
    }

    #[inline]
    fn inner_slot_at(
        group_to_inner: &[K::Native],
        group_nulls: &NullBufferBuilder,
        group_idx: usize,
    ) -> usize {
        let key = group_to_inner[group_idx];
        // Null groups use the all-ones key. Narrow unsigned keys need the
        // bitmap to distinguish a null from their maximum valid key.
        if key == Self::null_key()
            && (!Self::null_key_needs_validity() || !group_nulls.is_valid(group_idx))
        {
            NULL_INNER_SLOT
        } else {
            key.as_usize()
        }
    }

    #[inline]
    fn null_key() -> K::Native {
        K::Native::usize_as(usize::MAX)
    }

    #[inline]
    fn null_key_needs_validity() -> bool {
        Self::null_key().as_usize() != usize::MAX
    }

    #[inline]
    fn inner_slot(&self, group_idx: usize) -> usize {
        Self::inner_slot_at(&self.group_to_inner, &self.group_nulls, group_idx)
    }

    #[inline]
    fn inner_slot_eq(
        group_to_inner: &[K::Native],
        group_nulls: &NullBufferBuilder,
        group_idx: usize,
        inner_slot: usize,
    ) -> bool {
        if inner_slot == NULL_INNER_SLOT {
            group_to_inner[group_idx] == Self::null_key()
                && (!Self::null_key_needs_validity() || !group_nulls.is_valid(group_idx))
        } else {
            Self::non_null_inner_slot_eq(
                group_to_inner,
                group_nulls,
                group_idx,
                inner_slot,
            )
        }
    }

    #[inline]
    fn non_null_inner_slot_eq(
        group_to_inner: &[K::Native],
        group_nulls: &NullBufferBuilder,
        group_idx: usize,
        inner_slot: usize,
    ) -> bool {
        debug_assert_ne!(inner_slot, NULL_INNER_SLOT);
        let key = K::Native::usize_as(inner_slot);
        group_to_inner[group_idx] == key
            && (!Self::null_key_needs_validity()
                || key != Self::null_key()
                || group_nulls.is_valid(group_idx))
    }

    #[inline]
    fn push_inner_slot(&mut self, inner_slot: usize) {
        let valid = inner_slot != NULL_INNER_SLOT;
        let key = if valid {
            K::Native::usize_as(inner_slot)
        } else {
            Self::null_key()
        };
        self.group_to_inner.push(key);
        self.group_nulls.append(valid);
    }

    fn hash_values(&mut self, values: &ArrayRef) {
        self.cached_values = None;
        self.val_hashes.clear();
        self.val_hashes.resize(values.len(), 0);
        create_hashes(
            std::slice::from_ref(values),
            &self.random_state,
            &mut self.val_hashes,
        )
        .unwrap();
    }

    fn is_cached(&self, values: &ArrayRef) -> bool {
        self.cached_values
            .as_ref()
            .is_some_and(|cached| std::ptr::addr_eq(cached.as_ptr(), Arc::as_ptr(values)))
    }

    /// Makes `val_hashes` and `val_to_inner` correspond to `dict_values`,
    /// reusing them when they were already built for this same values array.
    ///
    /// `take`, `filter` and repartitioning clone the values `Arc` and rewrite
    /// only the keys, so batches in a partition usually share one values
    /// array; without this the per-batch cost is O(dictionary cardinality)
    /// rather than O(rows). `take_n` remaps `inner` and clears
    /// `cached_values`, forcing a rebuild.
    fn sync_value_cache(&mut self, dict_values: &ArrayRef) {
        if self.is_cached(dict_values) {
            return;
        }
        self.hash_values(dict_values);
        self.val_to_inner.clear();
        self.val_to_inner.resize(dict_values.len(), usize::MAX);
        self.cached_values = Some(Arc::downgrade(dict_values));
    }

    fn find_or_insert_value(
        &mut self,
        dict_values: &ArrayRef,
        val_idx: usize,
        hash: u64,
    ) -> Result<usize> {
        let inner = &*self.inner;
        let existing = self
            .value_dedup
            .find(hash, |&(entry_hash, slot)| {
                entry_hash == hash && inner.equal_to(slot, dict_values, val_idx)
            })
            .map(|&(_, slot)| slot);

        match existing {
            Some(slot) => Ok(slot),
            None => {
                let slot = self.inner.len();
                if !Self::key_type_fits(slot + 1) {
                    return exec_err!(
                        "Dictionary key type {:?} cannot represent {} distinct values",
                        K::DATA_TYPE,
                        slot + 1
                    );
                }
                self.inner.append_val(dict_values, val_idx)?;
                self.value_dedup.insert_accounted(
                    (hash, slot),
                    |&(entry_hash, _)| entry_hash,
                    &mut self.value_dedup_size,
                );
                Ok(slot)
            }
        }
    }

    /// Compares the value at `val_idx` against `inner[lhs_slot]` directly,
    /// memorizing `lhs_slot` in `val_to_inner` when they are equal.
    ///
    /// Each distinct value occupies exactly one slot in `inner`, so an equal
    /// comparison proves `lhs_slot` is the slot for `val_idx` without probing
    /// `value_dedup` or hashing. An unequal result is deliberately not
    /// memoized: it says which slot the value is *not* in, not which one it
    /// is. That is what keeps `val_to_inner` fill-only and never stale.
    ///
    /// Callers must have run [`Self::sync_value_cache`] for `dict_values`.
    ///
    /// Only called on a `val_to_inner` miss. It takes the fields it needs
    /// rather than `&mut self`, so the caller's hot loop can keep its slices
    /// of `val_to_inner` and `group_to_inner` in registers across the call.
    #[cold]
    #[inline(never)]
    fn equal_to_uncached(
        val_to_inner: &mut [usize],
        inner: &dyn GroupColumn,
        dict_values: &ArrayRef,
        val_idx: usize,
        lhs_slot: usize,
    ) -> bool {
        let rhs_null = dict_values.is_null(val_idx);
        let equal = if lhs_slot == NULL_INNER_SLOT {
            rhs_null
        } else if rhs_null {
            false
        } else {
            inner.equal_to(lhs_slot, dict_values, val_idx)
        };

        if equal {
            val_to_inner[val_idx] = lhs_slot;
        }
        equal
    }

    /// Per-row fallback for `vectorized_equal_to` used when the values array is
    /// not cached and the number of rows to check is smaller than the
    /// dictionary cardinality, making the O(D) hashing in `sync_value_cache`
    /// more expensive than direct value comparison.
    ///
    /// `#[cold]` + `#[inline(never)]` keeps this code out of the hot
    /// `val_to_inner` loops in `vectorized_equal_to` so LLVM can pipeline them.
    #[cold]
    #[inline(never)]
    fn equal_to_per_row(
        &self,
        lhs_rows: &[usize],
        dict_values: &ArrayRef,
        dict: &DictionaryArray<K>,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let group_to_inner = self.group_to_inner.as_slice();
        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            if !equal_to_results.get_bit(idx) {
                continue;
            }
            let lhs_slot =
                Self::inner_slot_at(group_to_inner, &self.group_nulls, lhs_row);
            let lhs_null = lhs_slot == NULL_INNER_SLOT;
            let equal = match dict.key(rhs_row) {
                None => lhs_null,
                Some(val_idx) if dict_values.is_null(val_idx) => lhs_null,
                Some(val_idx) => {
                    !lhs_null && self.inner.equal_to(lhs_slot, dict_values, val_idx)
                }
            };
            if !equal {
                equal_to_results.set_bit(idx, false);
            }
        }
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupColumn
    for DictionaryGroupValuesColumn<K>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let lhs_slot = self.inner_slot(lhs_row);
        let lhs_null = lhs_slot == NULL_INNER_SLOT;
        let dict = array.as_dictionary::<K>();
        match dict.key(rhs_row) {
            None => lhs_null,
            Some(val_idx) if dict.values().is_null(val_idx) => lhs_null,
            Some(val_idx) => {
                !lhs_null && self.inner.equal_to(lhs_slot, dict.values(), val_idx)
            }
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        let inner_slot = match dict.key(row) {
            None => NULL_INNER_SLOT,
            Some(val_idx) if dict.values().is_null(val_idx) => NULL_INNER_SLOT,
            Some(val_idx) => {
                let dict_values = dict.values();
                self.sync_value_cache(dict_values);
                self.find_or_insert_value(dict_values, val_idx, self.val_hashes[val_idx])?
            }
        };
        self.push_inner_slot(inner_slot);
        Ok(())
    }

    fn vectorized_equal_to(
        &mut self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let dict = array.as_dictionary::<K>();
        let dict_keys = dict.keys();
        let dict_values = dict.values();

        let cached = self.is_cached(dict_values);
        if !cached && rhs_rows.len() < dict_values.len() {
            self.equal_to_per_row(
                lhs_rows,
                dict_values,
                dict,
                rhs_rows,
                equal_to_results,
            );
            return;
        }
        self.sync_value_cache(dict_values);

        let raw_keys = dict_keys.values();
        let group_to_inner = self.group_to_inner.as_slice();
        let group_nulls = &self.group_nulls;
        let val_to_inner = self.val_to_inner.as_mut_slice();
        let inner = &*self.inner;
        if dict_keys.null_count() == 0
            && dict_values.null_count() == 0
            && group_nulls.as_slice().is_none()
        {
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                let val_idx = raw_keys[rhs_row].as_usize();
                let equal = match val_to_inner[val_idx] {
                    usize::MAX => Self::equal_to_uncached(
                        val_to_inner,
                        inner,
                        dict_values,
                        val_idx,
                        group_to_inner[lhs_row].as_usize(),
                    ),
                    slot => group_to_inner[lhs_row] == K::Native::usize_as(slot),
                };
                if !equal {
                    equal_to_results.set_bit(idx, false);
                }
            }
        } else if dict_keys.null_count() == 0 {
            // No null keys : skip the get_bit guard: we only ever write false,
            // so overwriting an already-false bit is a no-op.
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                let val_idx = raw_keys[rhs_row].as_usize();
                let equal = match val_to_inner[val_idx] {
                    usize::MAX => {
                        let lhs_slot =
                            Self::inner_slot_at(group_to_inner, group_nulls, lhs_row);
                        Self::equal_to_uncached(
                            val_to_inner,
                            inner,
                            dict_values,
                            val_idx,
                            lhs_slot,
                        )
                    }
                    slot => {
                        Self::inner_slot_eq(group_to_inner, group_nulls, lhs_row, slot)
                    }
                };
                if !equal {
                    equal_to_results.set_bit(idx, false);
                }
            }
        } else if dict_values.null_count() == 0 {
            let null_buf = dict_keys.nulls().unwrap();
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                if equal_to_results.get_bit(idx) {
                    let equal = if null_buf.is_null(rhs_row) {
                        Self::inner_slot_eq(
                            group_to_inner,
                            group_nulls,
                            lhs_row,
                            NULL_INNER_SLOT,
                        )
                    } else {
                        let val_idx = raw_keys[rhs_row].as_usize();
                        match val_to_inner[val_idx] {
                            usize::MAX => {
                                let lhs_slot = Self::inner_slot_at(
                                    group_to_inner,
                                    group_nulls,
                                    lhs_row,
                                );
                                Self::equal_to_uncached(
                                    val_to_inner,
                                    inner,
                                    dict_values,
                                    val_idx,
                                    lhs_slot,
                                )
                            }
                            slot => Self::non_null_inner_slot_eq(
                                group_to_inner,
                                group_nulls,
                                lhs_row,
                                slot,
                            ),
                        }
                    };
                    if !equal {
                        equal_to_results.set_bit(idx, false);
                    }
                }
            }
        } else {
            let null_buf = dict_keys.nulls().unwrap();
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                if equal_to_results.get_bit(idx) {
                    // A null key is not a position in the values array, so it
                    // resolves straight to the null slot instead of going
                    // through `val_to_inner`.
                    let equal = if null_buf.is_null(rhs_row) {
                        Self::inner_slot_eq(
                            group_to_inner,
                            group_nulls,
                            lhs_row,
                            NULL_INNER_SLOT,
                        )
                    } else {
                        let val_idx = raw_keys[rhs_row].as_usize();
                        match val_to_inner[val_idx] {
                            usize::MAX => {
                                let lhs_slot = Self::inner_slot_at(
                                    group_to_inner,
                                    group_nulls,
                                    lhs_row,
                                );
                                Self::equal_to_uncached(
                                    val_to_inner,
                                    inner,
                                    dict_values,
                                    val_idx,
                                    lhs_slot,
                                )
                            }
                            slot => Self::inner_slot_eq(
                                group_to_inner,
                                group_nulls,
                                lhs_row,
                                slot,
                            ),
                        }
                    };
                    if !equal {
                        equal_to_results.set_bit(idx, false);
                    }
                }
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        let dict_keys = dict.keys();
        let dict_values = dict.values();

        // Reuses `val_hashes`/`val_to_inner` when this batch carries the same
        // values array as the last one, which is the common case downstream of
        // a repartition or filter. Previously this unconditionally re-hashed
        // and re-initialised both buffers, costing O(D) per batch.
        self.sync_value_cache(dict_values);

        self.group_to_inner.try_reserve(rows.len()).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::MemoryError(e.to_string())),
                None,
            )
        })?;
        let raw_keys = dict_keys.values();
        if dict_keys.null_count() == 0 && dict_values.null_count() == 0 {
            let start_len = self.group_to_inner.len();
            let result: Result<()> = rows.iter().try_for_each(|&row| {
                let val_idx = raw_keys[row].as_usize();
                if self.val_to_inner[val_idx] == usize::MAX {
                    self.val_to_inner[val_idx] = self.find_or_insert_value(
                        dict_values,
                        val_idx,
                        self.val_hashes[val_idx],
                    )?;
                }
                self.group_to_inner
                    .push(K::Native::usize_as(self.val_to_inner[val_idx]));
                Ok(())
            });
            self.group_nulls
                .append_n_non_nulls(self.group_to_inner.len() - start_len);
            result?;
        } else if dict_keys.null_count() == 0 {
            for &row in rows {
                let val_idx = raw_keys[row].as_usize();
                if self.val_to_inner[val_idx] == usize::MAX {
                    // A non-null key can still point to a null value in the values array.
                    self.val_to_inner[val_idx] = if dict_values.is_null(val_idx) {
                        NULL_INNER_SLOT
                    } else {
                        self.find_or_insert_value(
                            dict_values,
                            val_idx,
                            self.val_hashes[val_idx],
                        )?
                    };
                }
                let slot = self.val_to_inner[val_idx];
                self.push_inner_slot(slot);
            }
        } else {
            let null_buf = dict_keys.nulls().unwrap();
            for &row in rows {
                let slot = if null_buf.is_null(row) {
                    NULL_INNER_SLOT
                } else {
                    let val_idx = raw_keys[row].as_usize();
                    if self.val_to_inner[val_idx] == usize::MAX {
                        self.val_to_inner[val_idx] = if dict_values.is_null(val_idx) {
                            NULL_INNER_SLOT
                        } else {
                            self.find_or_insert_value(
                                dict_values,
                                val_idx,
                                self.val_hashes[val_idx],
                            )?
                        };
                    }
                    self.val_to_inner[val_idx]
                };
                self.push_inner_slot(slot);
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.group_to_inner.len()
    }

    fn size(&self) -> usize {
        self.inner.size()
            + self.value_dedup_size
            + self.group_to_inner.capacity() * size_of::<K::Native>()
            + self.group_nulls.allocated_size()
            + self.val_to_inner.capacity() * size_of::<usize>()
            + self.val_hashes.capacity() * size_of::<u64>()
            + size_of::<Self>()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let this = *self;
        let values = this.inner.build();
        let keys = PrimitiveArray::<K>::new(
            ScalarBuffer::from(this.group_to_inner),
            this.group_nulls.build(),
        );
        // SAFETY: non-null group keys are created from slots in `inner`.
        Arc::new(unsafe { DictionaryArray::<K>::new_unchecked(keys, values) })
    }

    fn values_preserving(&self, selection: GroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.group_to_inner.len())?;

        let mut old_to_new = HashMap::with_capacity(selection.len());
        let mut selected_inner = Vec::with_capacity(selection.len());
        let mut selected_groups = Vec::with_capacity(selection.len());
        for group_index in selection.iter() {
            let old_slot = self.inner_slot(group_index);
            if old_slot == NULL_INNER_SLOT {
                selected_groups.push(None);
                continue;
            }
            let new_slot = if let Some(&new_slot) = old_to_new.get(&old_slot) {
                new_slot
            } else {
                let new_slot = selected_inner.len();
                selected_inner.push(old_slot);
                old_to_new.insert(old_slot, new_slot);
                new_slot
            };
            selected_groups.push(Some(K::Native::usize_as(new_slot)));
        }

        let inner_selection =
            GroupSelection::try_from_indices(&selected_inner, self.inner.len())?;
        let values = self.inner.values_preserving(inner_selection)?;
        let keys: PrimitiveArray<K> = selected_groups.into_iter().collect();
        // SAFETY: every non-null key was assigned from `selected_inner` above.
        Ok(Arc::new(unsafe {
            DictionaryArray::<K>::new_unchecked(keys, values)
        }))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let old_inner_len = self.inner.len();
        let all_inner_values = self.inner.take_n(old_inner_len);

        let mut emit_old_to_new = vec![usize::MAX; old_inner_len];
        let mut emit_new_to_old: Vec<usize> = Vec::new();
        for group_idx in 0..n {
            let old = self.inner_slot(group_idx);
            if old == NULL_INNER_SLOT {
                continue;
            }
            if emit_old_to_new[old] == usize::MAX {
                emit_old_to_new[old] = emit_new_to_old.len();
                emit_new_to_old.push(old);
            }
        }
        let emit_indices =
            Int64Array::from_iter(emit_new_to_old.iter().map(|&i| i as i64));
        let compact_emit_values =
            take(&*all_inner_values, &emit_indices, None).expect("take emit values");
        let emitted_keys: PrimitiveArray<K> = (0..n)
            .map(|group_idx| {
                let old = self.inner_slot(group_idx);
                if old == NULL_INNER_SLOT {
                    None
                } else {
                    Some(K::Native::usize_as(emit_old_to_new[old]))
                }
            })
            .collect();
        // SAFETY: `emit_old_to_new` maps every non-null emitted key to
        // `compact_emit_values`; null keys are ignored.
        let emitted: ArrayRef = Arc::new(unsafe {
            DictionaryArray::<K>::new_unchecked(emitted_keys, compact_emit_values)
        });

        let remaining: Vec<usize> = (n..self.group_to_inner.len())
            .map(|group_idx| self.inner_slot(group_idx))
            .collect();
        let mut old_to_new = vec![usize::MAX; old_inner_len];
        let mut new_to_old: Vec<usize> = Vec::new();
        for &old in &remaining {
            if old == NULL_INNER_SLOT {
                continue;
            }
            if old_to_new[old] == usize::MAX {
                old_to_new[old] = new_to_old.len();
                new_to_old.push(old);
            }
        }
        self.value_dedup = HashTable::new();
        self.value_dedup_size = 0;
        self.hash_values(&all_inner_values);

        for (new_slot, &old_slot) in new_to_old.iter().enumerate() {
            self.inner
                .append_val(&all_inner_values, old_slot)
                .expect("append value failed in take_n");
            self.value_dedup.insert_accounted(
                (self.val_hashes[old_slot], new_slot),
                |&(entry_hash, _)| entry_hash,
                &mut self.value_dedup_size,
            );
        }

        self.group_to_inner = Vec::with_capacity(remaining.len());
        self.group_nulls = NullBufferBuilder::new(remaining.len());
        for old in remaining {
            let slot = if old == NULL_INNER_SLOT {
                NULL_INNER_SLOT
            } else {
                old_to_new[old]
            };
            self.push_inner_slot(slot);
        }

        emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
    use arrow::array::{
        Array, ArrayRef, BooleanBufferBuilder, DictionaryArray, Int32Array, StringArray,
        UInt8Array,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int8Type, Int32Type, UInt8Type};
    use datafusion_physical_expr::binary_map::OutputType;
    use std::sync::Arc;

    fn utf8_col() -> DictionaryGroupValuesColumn<Int32Type> {
        DictionaryGroupValuesColumn::new(Box::new(ByteGroupValueBuilder::<i32>::new(
            OutputType::Utf8,
        )))
    }

    fn int8_col() -> DictionaryGroupValuesColumn<Int8Type> {
        DictionaryGroupValuesColumn::new(Box::new(ByteGroupValueBuilder::<i32>::new(
            OutputType::Utf8,
        )))
    }

    fn uint8_col() -> DictionaryGroupValuesColumn<UInt8Type> {
        DictionaryGroupValuesColumn::new(Box::new(ByteGroupValueBuilder::<i32>::new(
            OutputType::Utf8,
        )))
    }

    fn i32_dict(keys: &[Option<i32>], values: &[Option<&str>]) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    }

    fn i8_dict(keys: &[Option<i8>], values: &[Option<&str>]) -> ArrayRef {
        use arrow::array::Int8Array;
        Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(keys.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    }

    fn u8_dict(keys: &[Option<u8>], values: &[Option<&str>]) -> ArrayRef {
        Arc::new(DictionaryArray::<UInt8Type>::new(
            UInt8Array::from(keys.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    }

    fn str_values(arr: &ArrayRef) -> Vec<Option<String>> {
        let plain = cast(arr.as_ref(), &DataType::Utf8).unwrap();
        plain
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_owned()))
            .collect()
    }

    fn bool_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    #[test]
    fn hash_cache_does_not_retain_dictionary_values() {
        let mut column = utf8_col();
        let input = i32_dict(&[Some(0)], &[Some("retained")]);
        let values = Arc::clone(input.as_dictionary::<Int32Type>().values());
        let weak_values = Arc::downgrade(&values);
        column.append_val(&input, 0).unwrap();
        assert!(
            column
                .cached_values
                .as_ref()
                .is_some_and(|cached| cached.ptr_eq(&weak_values))
        );
        let size_with_input = column.size();

        // The cache is only an identity hint: it must not keep an input
        // dictionary allocation alive after its batch is released.
        drop(values);
        drop(input);
        assert!(weak_values.upgrade().is_none());
        assert_eq!(column.size(), size_with_input);
    }

    fn all_true(len: usize) -> BooleanBufferBuilder {
        let mut buf = BooleanBufferBuilder::new(len);
        buf.append_n(len, true);
        buf
    }

    // Builds an Int8-keyed dict of `end-start` distinct strings "v{start}".."v{end-1}".
    fn distinct_i8_dict(start: usize, end: usize) -> ArrayRef {
        let strs: Vec<String> = (start..end).map(|i| format!("v{i}")).collect();
        let refs: Vec<Option<&str>> = strs.iter().map(|s| Some(s.as_str())).collect();
        i8_dict(
            &(0..strs.len()).map(|i| Some(i as i8)).collect::<Vec<_>>(),
            &refs,
        )
    }

    // Builds a UInt8-keyed dict of `count` distinct strings "u0".."u{count-1}".
    fn distinct_u8_dict(count: usize) -> ArrayRef {
        let strs: Vec<String> = (0..count).map(|i| format!("u{i}")).collect();
        let refs: Vec<Option<&str>> = strs.iter().map(|s| Some(s.as_str())).collect();
        u8_dict(
            &(0..count).map(|i| Some(i as u8)).collect::<Vec<_>>(),
            &refs,
        )
    }

    #[test]
    fn repeated_values_are_deduplicated_in_inner_store() {
        let mut col = utf8_col();
        let arr = i32_dict(
            &[Some(0), Some(1), Some(0), Some(1), Some(0)],
            &[Some("a"), Some("b")],
        );
        col.vectorized_append(&arr, &[0, 1, 2, 3, 4]).unwrap();
        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<Int32Type>().values().len(), 2);
        assert_eq!(
            str_values(&out),
            vec![
                Some("a".into()),
                Some("b".into()),
                Some("a".into()),
                Some("b".into()),
                Some("a".into()),
            ]
        );
    }

    #[test]
    fn build_reuses_group_key_buffer() {
        let mut col = utf8_col();
        let input = i32_dict(&[Some(0), Some(1)], &[Some("a"), Some("b")]);
        col.vectorized_append(&input, &[0, 1]).unwrap();
        let keys_ptr = col.group_to_inner.as_ptr();

        let output = Box::new(col).build();
        assert_eq!(
            output.as_dictionary::<Int32Type>().keys().values().as_ptr(),
            keys_ptr
        );
    }

    #[test]
    fn values_preserving_reorders_and_reuses_dictionary_values() {
        let mut col = utf8_col();
        let input = i32_dict(&[Some(0), None, Some(1), Some(0)], &[Some("a"), Some("b")]);
        col.vectorized_append(&input, &[0, 1, 2, 3]).unwrap();

        let selection = GroupSelection::try_from_indices(&[2, 1, 0, 2], 4).unwrap();
        for _ in 0..2 {
            let selected = col.values_preserving(selection).unwrap();
            assert_eq!(
                str_values(&selected),
                vec![Some("b".into()), None, Some("a".into()), Some("b".into())]
            );
            assert_eq!(selected.as_dictionary::<Int32Type>().values().len(), 2);
        }

        col.append_val(&i32_dict(&[Some(0)], &[Some("c")]), 0)
            .unwrap();
        let out = Box::new(col).build();
        assert_eq!(
            str_values(&out),
            vec![
                Some("a".into()),
                None,
                Some("b".into()),
                Some("a".into()),
                Some("c".into()),
            ]
        );
    }

    #[test]
    fn null_key_and_null_valued_entry_both_map_to_null_group() {
        let mut col = utf8_col();
        let input = i32_dict(&[None, Some(0), Some(1)], &[None, Some("b")]);
        for row in 0..3 {
            col.append_val(&input, row).unwrap();
        }
        assert!(col.equal_to(0, &input, 1));
        assert!(!col.equal_to(0, &input, 2));
        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<Int32Type>().values().len(), 1);
        assert_eq!(str_values(&out), vec![None, None, Some("b".into())]);
    }

    #[test]
    fn take_n_compacts_emitted_values_and_remaps_remaining_slots() {
        let mut col = utf8_col();
        let b1 = i32_dict(
            &[Some(0), Some(1), None, Some(2)],
            &[Some("a"), Some("b"), Some("c")],
        );
        col.vectorized_append(&b1, &[0, 1, 2, 3]).unwrap();

        let emitted = col.take_n(2);
        assert_eq!(emitted.as_dictionary::<Int32Type>().values().len(), 2);
        assert_eq!(
            str_values(&emitted),
            vec![Some("a".into()), Some("b".into())]
        );

        let b2 = i32_dict(&[None, Some(0)], &[Some("z")]);
        col.vectorized_append(&b2, &[0, 1]).unwrap();

        let mut buf = all_true(2);
        col.vectorized_equal_to(&[0, 1], &b2, &[0, 1], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true, false]);

        let out = Box::new(col).build();
        assert_eq!(
            str_values(&out),
            vec![None, Some("c".into()), None, Some("z".into())]
        );
    }

    #[test]
    fn vectorized_equal_to_does_not_use_stale_hashes_from_prior_append() {
        let mut col = utf8_col();
        col.vectorized_append(&i32_dict(&[Some(0)], &[Some("a"), Some("b")]), &[0])
            .unwrap();
        let batch2 = i32_dict(&[Some(1)], &[Some("z"), Some("a")]);
        let mut buf = all_true(1);
        col.vectorized_equal_to(&[0], &batch2, &[0], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true]);
    }

    #[test]
    fn null_does_not_consume_a_key_slot_int8_null_first_mid_and_last() {
        let rows128 = (0..128).collect::<Vec<_>>();

        let mut col = int8_col(); // null-last: 128 non-null + null — ok
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();

        let mut col = int8_col(); // null-first: null + 128 non-null — ok; 129th — error
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();
        assert!(
            col.append_val(&i8_dict(&[Some(0)], &[Some("overflow")]), 0)
                .is_err()
        );

        let mut col = int8_col(); // null-mid: 100 + null + 28 = 128 total — ok; 129th — error
        col.vectorized_append(&distinct_i8_dict(0, 100), &(0..100).collect::<Vec<_>>())
            .unwrap();
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_i8_dict(100, 128), &(0..28).collect::<Vec<_>>())
            .unwrap();
        assert!(
            col.append_val(&i8_dict(&[Some(0)], &[Some("v128")]), 0)
                .is_err()
        );

        let mut col = int8_col(); // build() null-first: 128 values (null excluded), null → None
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();
        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<Int8Type>().values().len(), 128);
        assert_eq!(str_values(&out)[0], None);
        assert_eq!(str_values(&out)[1], Some("v0".into()));
    }

    #[test]
    fn null_does_not_consume_a_key_slot_uint8_null_first_and_last() {
        let rows256 = (0..256).collect::<Vec<_>>();

        let mut col = uint8_col(); // null-first: null + 256 non-null — ok; 257th — error
        col.append_val(&u8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_u8_dict(256), &rows256)
            .unwrap();
        assert!(
            col.append_val(&u8_dict(&[Some(0)], &[Some("overflow")]), 0)
                .is_err()
        );

        let mut col = uint8_col(); // build() null-first: 256 values (null excluded), last correct
        col.append_val(&u8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_u8_dict(256), &rows256)
            .unwrap();
        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<UInt8Type>().values().len(), 256);
        assert_eq!(str_values(&out)[0], None);
        assert_eq!(str_values(&out)[256], Some("u255".into()));
    }

    #[test]
    fn take_n_null_does_not_steal_key_slot_at_capacity() {
        let rows128 = (0..128).collect::<Vec<_>>();

        // Int8 null-first + 128 non-null; emit all 129 — no panic, null → None
        let mut col = int8_col();
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();
        let emitted = col.take_n(129);
        assert!(emitted.as_dictionary::<Int8Type>().key(0).is_none());
        assert_eq!(str_values(&emitted)[1], Some("v0".into()));
        assert_eq!(str_values(&emitted)[128], Some("v127".into()));

        // UInt8 null-first + 256 non-null; emit all 257 — last must be "u255" not "u0" (wrap guard)
        let mut col = uint8_col();
        col.append_val(&u8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_u8_dict(256), &(0..256).collect::<Vec<_>>())
            .unwrap();
        let emitted = col.take_n(257);
        assert!(emitted.as_dictionary::<UInt8Type>().key(0).is_none());
        assert_eq!(str_values(&emitted)[1], Some("u0".into()));
        assert_eq!(str_values(&emitted)[256], Some("u255".into()));
    }

    #[test]
    fn take_n_repeated_emissions_null_at_int8_capacity() {
        let rows128 = (0..128).collect::<Vec<_>>();
        let mut col = int8_col();
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();
        col.append_val(&i8_dict(&[None], &[Some("x")]), 0).unwrap();
        col.vectorized_append(&distinct_i8_dict(0, 128), &rows128)
            .unwrap();

        let first_half = col.take_n(64);
        assert_eq!(str_values(&first_half)[0], Some("v0".into()));
        assert_eq!(str_values(&first_half)[63], Some("v63".into()));
        assert_eq!(first_half.as_dictionary::<Int8Type>().values().len(), 64);

        let second_half = col.take_n(64);
        assert_eq!(str_values(&second_half)[0], Some("v64".into()));
        assert_eq!(str_values(&second_half)[63], Some("v127".into()));

        let null_group = col.take_n(1);
        assert!(null_group.as_dictionary::<Int8Type>().key(0).is_none());

        let out = Box::new(col).build();
        assert_eq!(str_values(&out)[0], Some("v0".into()));
        assert_eq!(str_values(&out)[127], Some("v127".into()));
        assert_eq!(out.as_dictionary::<Int8Type>().values().len(), 128);
    }

    fn dict_with_values(keys: &[Option<i32>], values: &ArrayRef) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys.to_vec()),
            Arc::clone(values),
        ))
    }

    /// A cache rebuild resets every `val_to_inner` entry to `usize::MAX`, so
    /// an entry resolved by an earlier batch surviving a later one proves the
    /// dictionary was not re-hashed in between.
    #[test]
    fn cache_survives_batches_sharing_a_values_array() {
        let values: ArrayRef =
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let mut col = utf8_col();

        col.vectorized_append(&dict_with_values(&[Some(0)], &values), &[0])
            .unwrap();
        let slot_a = col.val_to_inner[0];
        assert_ne!(slot_a, usize::MAX);

        col.vectorized_append(&dict_with_values(&[Some(1)], &values), &[0])
            .unwrap();

        assert_eq!(col.val_to_inner[0], slot_a, "cache was rebuilt");
        assert_ne!(col.val_to_inner[1], usize::MAX);
        assert_eq!(
            col.val_to_inner[2],
            usize::MAX,
            "unreferenced value resolved"
        );
        assert!(col.is_cached(&values));
        assert_eq!(col.inner.len(), 2);
    }

    /// `take` clones the values `Arc` and rewrites only the keys, which is the
    /// property the `ptr_eq` check relies on. Pin that arrow behaviour.
    #[test]
    fn take_preserves_values_arc_so_cache_hits() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let batch = dict_with_values(&[Some(0), Some(1), Some(0)], &values);
        let taken = take(&*batch, &Int32Array::from(vec![2, 0]), None).unwrap();
        assert!(
            Arc::ptr_eq(taken.as_dictionary::<Int32Type>().values(), &values),
            "take must not copy the values array"
        );

        let mut col = utf8_col();
        col.vectorized_append(&batch, &[0, 1, 2]).unwrap();
        let slot_b = col.val_to_inner[1];
        col.vectorized_append(&taken, &[0, 1]).unwrap();

        assert_eq!(col.val_to_inner[1], slot_b, "cache was rebuilt");
        assert_eq!(col.inner.len(), 2);
    }

    /// A genuinely different values array must rebuild the cache, and equal
    /// values across the two arrays must still dedup to one inner slot.
    #[test]
    fn distinct_values_arcs_rebuild_cache_but_still_dedup() {
        let mut col = utf8_col();
        let first = i32_dict(&[Some(0)], &[Some("a")]);
        let second = i32_dict(&[Some(0)], &[Some("a")]);
        col.vectorized_append(&first, &[0]).unwrap();
        col.vectorized_append(&second, &[0]).unwrap();

        assert!(
            col.is_cached(second.as_dictionary::<Int32Type>().values()),
            "cache should track the most recent values array"
        );
        assert_eq!(col.inner.len(), 1, "'a' must dedup across both arrays");
        assert_eq!(col.len(), 2);
    }

    /// `take_n` remaps inner slots, so the cached `val_idx → inner_slot` map
    /// must not survive it.
    #[test]
    fn take_n_invalidates_value_cache() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let mut col = utf8_col();
        col.vectorized_append(&dict_with_values(&[Some(0), Some(1)], &values), &[0, 1])
            .unwrap();

        let _emitted = col.take_n(1);
        assert!(
            col.cached_values.is_none(),
            "take_n remaps inner slots and must drop the cache"
        );

        col.vectorized_append(&dict_with_values(&[Some(0), Some(1)], &values), &[0, 1])
            .unwrap();

        let out = Box::new(col).build();
        let d = out.as_dictionary::<Int32Type>();
        let vals = d.values().as_string::<i32>();
        let got: Vec<&str> = d
            .keys()
            .iter()
            .map(|k| vals.value(k.unwrap() as usize))
            .collect();
        assert_eq!(got, vec!["b", "a", "b"]);
    }

    /// `append_val` fills `val_hashes` but not `val_to_inner`; mixing it with
    /// `vectorized_append` must not duplicate a value or reuse a stale slot.
    #[test]
    fn scalar_and_vectorized_append_share_cache_consistently() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let batch = dict_with_values(&[Some(0), Some(1)], &values);
        let mut col = utf8_col();

        col.append_val(&batch, 0).unwrap();
        assert!(col.is_cached(&values));

        col.vectorized_append(&batch, &[0, 1]).unwrap();
        assert_eq!(col.inner.len(), 2, "'a' must not be duplicated");
        assert_eq!(col.len(), 3);
    }

    /// On a cache miss `vectorized_equal_to` compares against the group's own
    /// slot: an equal result proves the slot and is cached, an unequal one
    /// only rules that slot out and must leave the entry unresolved.
    #[test]
    fn vectorized_equal_to_caches_only_equal_results() {
        let mut col = utf8_col();
        col.vectorized_append(
            &i32_dict(&[Some(0), Some(1)], &[Some("a"), Some("b")]),
            &[0, 1],
        )
        .unwrap();
        let (group_a, group_b) = (0, 1);
        let slot_a = col.inner_slot(group_a);
        let slot_b = col.inner_slot(group_b);

        // A new values array in the opposite order, so `val_idx` != slot.
        // Two rows for two values keeps the per-row fallback out of the way.
        let (idx_b, idx_a) = (0, 1);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("a")]));
        let a_then_b =
            dict_with_values(&[Some(idx_a as i32), Some(idx_b as i32)], &values);

        // "a" vs group a is equal, "b" vs group a is not.
        let mut buf = all_true(2);
        col.vectorized_equal_to(&[group_a, group_a], &a_then_b, &[0, 1], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true, false]);
        assert_eq!(col.val_to_inner[idx_a], slot_a);
        assert_eq!(
            col.val_to_inner[idx_b],
            usize::MAX,
            "unequal result was cached"
        );

        // Later, "b" vs group b fills the entry that was left unresolved.
        let only_b = dict_with_values(&[Some(idx_b as i32)], &values);
        let mut buf = all_true(1);
        col.vectorized_equal_to(&[group_b], &only_b, &[0], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true]);
        assert_eq!(col.val_to_inner[idx_b], slot_b);
    }

    /// A non-null key pointing at a null value and a null key must both
    /// compare equal to the null group, and only the former goes through
    /// `val_to_inner`.
    #[test]
    fn vectorized_equal_to_matches_null_value_and_null_key_to_null_group() {
        let mut col = utf8_col();
        col.vectorized_append(&i32_dict(&[None, Some(0)], &[Some("a")]), &[0, 1])
            .unwrap();
        let (null_group, group_a) = (0, 1);
        let idx_null = 0;
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a")]));
        let batch = dict_with_values(
            &[Some(idx_null as i32), Some(idx_null as i32), None],
            &values,
        );

        // null value vs null group, null value vs group a, null key vs null group.
        let mut buf = all_true(3);
        col.vectorized_equal_to(
            &[null_group, group_a, null_group],
            &batch,
            &[0, 1, 2],
            &mut buf,
        );
        assert_eq!(bool_vec(&buf), vec![true, false, true]);
        assert_eq!(col.val_to_inner[idx_null], NULL_INNER_SLOT);
    }

    /// Fewer rows than values in an uncached values array takes the per-row
    /// fallback, which must not rebuild the cache for that array.
    #[test]
    fn vectorized_equal_to_small_batch_of_new_values_skips_cache_rebuild() {
        let old_values: ArrayRef = Arc::new(StringArray::from(vec![Some("a")]));
        let mut col = utf8_col();
        col.vectorized_append(&dict_with_values(&[Some(0)], &old_values), &[0])
            .unwrap();
        let group_a = 0;

        // One row against three values in an array the cache has not seen.
        let new_values: ArrayRef =
            Arc::new(StringArray::from(vec![Some("x"), Some("a"), Some("y")]));
        let idx_a = 1;
        let only_a = dict_with_values(&[Some(idx_a)], &new_values);
        let mut buf = all_true(1);
        col.vectorized_equal_to(&[group_a], &only_a, &[0], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true]);
        assert!(
            col.is_cached(&old_values),
            "fallback must leave the cache on the previous values array"
        );
    }
}
