// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::{Arc, Weak};

use arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, PrimitiveArray, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow_ord::ord::{DynComparator, make_comparator};
use datafusion_common::float_semantics::normalize_floats;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupSelection};
use hashbrown::hash_table::HashTable;

use crate::aggregates::AGGREGATION_HASH_SEED;
use crate::aggregates::group_values::GroupValues;
use crate::aggregates::group_values::multi_group_by::GroupValuesColumn;

const UNRESOLVED: usize = usize::MAX;
const PENDING: usize = usize::MAX - 1;

/// Maximum ratio between dictionary values and rows in the first batch for
/// reserving the value table for the full dictionary.
///
/// Dense dictionaries benefit from paying for one allocation up front. Sparse
/// dictionaries only reserve the referenced keys found in the current batch.
const MAX_DENSE_DICTIONARY_RATIO: usize = 16;

/// Specialized [`GroupValues`] for a single dictionary-encoded group key.
///
/// While batches share one values array, dictionary keys cache their group IDs
/// directly. Only newly referenced keys are hashed, and group values remain in
/// the dictionary instead of being copied into a second value store. If the
/// values array changes, this transitions to the general columnar
/// implementation, which handles reconciliation across dictionaries.
pub struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send + Sync> {
    schema: SchemaRef,
    direct: Option<Box<DirectDictionaryGroups<K>>>,
    fallback: Option<Box<GroupValuesColumn<false>>>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupValuesDictionary<K> {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            direct: None,
            fallback: None,
        }
    }

    fn transition_to_fallback(&mut self) -> Result<()> {
        let direct = self.direct.take().expect("direct state exists");
        let expected_groups = direct.len();
        let existing = (*direct).into_array(EmitTo::All)?;
        let mut fallback = GroupValuesColumn::<false>::try_new(Arc::clone(&self.schema))?;
        let mut groups = Vec::new();
        fallback.intern(&[existing], &mut groups)?;
        if groups.iter().copied().ne(0..expected_groups) {
            return internal_err!(
                "dictionary group transition changed existing group indices"
            );
        }
        self.fallback = Some(Box::new(fallback));
        Ok(())
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupValues for GroupValuesDictionary<K> {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);

        if let Some(fallback) = &mut self.fallback {
            return fallback.intern(cols, groups);
        }

        let dict = cols[0].as_dictionary::<K>();
        let values = dict.values();
        match &self.direct {
            None => {
                self.direct = Some(Box::new(DirectDictionaryGroups::try_new(values)?));
            }
            Some(direct) if direct.has_source_values(values) => {}
            Some(direct) if direct.is_empty() => {
                self.direct = Some(Box::new(DirectDictionaryGroups::try_new(values)?));
            }
            Some(_) => {
                self.transition_to_fallback()?;
                return self
                    .fallback
                    .as_mut()
                    .expect("fallback initialized")
                    .intern(cols, groups);
            }
        }

        self.direct
            .as_mut()
            .expect("direct state initialized")
            .intern(dict, groups)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.direct.as_ref().map_or(0, |direct| direct.size())
            + self.fallback.as_ref().map_or(0, |fallback| fallback.size())
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.direct.as_ref().map_or_else(
            || self.fallback.as_ref().map_or(0, |fallback| fallback.len()),
            |v| v.len(),
        )
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        if let Some(fallback) = &mut self.fallback {
            let output = fallback.emit(emit_to)?;
            if fallback.is_empty() {
                self.fallback = None;
            }
            return Ok(output);
        }

        let Some(direct) = &mut self.direct else {
            let values_type = match self.schema.field(0).data_type() {
                DataType::Dictionary(_, values_type) => values_type.as_ref(),
                other => unreachable!("expected dictionary field, got {other}"),
            };
            let values = arrow::array::new_empty_array(values_type);
            let keys = PrimitiveArray::<K>::new_null(0);
            return Ok(vec![Arc::new(DictionaryArray::<K>::new(keys, values))]);
        };

        let output = direct.emit(emit_to)?;
        if direct.is_empty() {
            self.direct = None;
        }
        Ok(vec![output])
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        if let Some(fallback) = &mut self.fallback {
            return fallback.values_preserving(selection);
        }
        let Some(direct) = &self.direct else {
            selection.validate_num_groups(0)?;
            return self.emit(EmitTo::All);
        };
        Ok(vec![direct.values_preserving(selection)?])
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.direct = None;
        self.fallback = None;
    }

    fn shrink_to_fit(&mut self) {
        if let Some(direct) = &mut self.direct {
            direct.shrink_to_fit();
        }
        if let Some(fallback) = &mut self.fallback {
            fallback.shrink_to_fit();
        }
    }
}

struct DirectDictionaryGroups<K: ArrowDictionaryKeyType> {
    /// Original values-array identity used by the incoming dictionaries.
    source_values: Weak<dyn Array>,
    /// Normalized values retained for comparison and output.
    values: ArrayRef,
    comparator: DynComparator,
    /// Dictionary key index to group ID for referenced values.
    key_to_group: Vec<usize>,
    /// Dictionary key for each group ID. `None` represents the null group.
    group_to_key: Vec<Option<K::Native>>,
    /// Deduplicates different dictionary keys containing equal values.
    value_map: HashTable<(u64, usize)>,
    null_group: Option<usize>,
    random_state: RandomState,
    pending_keys: Vec<usize>,
    pending_events: Vec<Option<usize>>,
    pending_null_keys: Vec<usize>,
    hashes: Vec<u64>,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType> DirectDictionaryGroups<K> {
    fn try_new(source_values: &ArrayRef) -> Result<Self> {
        let values = normalize_floats(source_values);
        let comparator =
            make_comparator(values.as_ref(), values.as_ref(), Default::default())?;
        Ok(Self {
            source_values: Arc::downgrade(source_values),
            key_to_group: vec![UNRESOLVED; values.len()],
            values,
            comparator,
            group_to_key: Vec::new(),
            value_map: HashTable::new(),
            null_group: None,
            random_state: AGGREGATION_HASH_SEED,
            pending_keys: Vec::new(),
            pending_events: Vec::new(),
            pending_null_keys: Vec::new(),
            hashes: Vec::new(),
            _phantom: PhantomData,
        })
    }

    fn has_source_values(&self, values: &ArrayRef) -> bool {
        std::ptr::addr_eq(self.source_values.as_ptr(), Arc::as_ptr(values))
    }

    fn intern(
        &mut self,
        dict: &DictionaryArray<K>,
        groups: &mut Vec<usize>,
    ) -> Result<()> {
        self.pending_keys.clear();
        self.pending_events.clear();
        self.pending_null_keys.clear();

        let keys = dict.keys();
        let raw_keys = keys.values();
        let mut pending_null = self.null_group.is_none();

        for row in 0..dict.len() {
            let Some(key) = dict.key(row) else {
                if pending_null {
                    self.pending_events.push(None);
                    pending_null = false;
                }
                continue;
            };
            if self.key_to_group[key] != UNRESOLVED {
                continue;
            }
            self.key_to_group[key] = PENDING;
            if self.values.is_null(key) {
                self.pending_null_keys.push(key);
                if pending_null {
                    self.pending_events.push(None);
                    pending_null = false;
                }
            } else {
                self.pending_keys.push(key);
                self.pending_events.push(Some(key));
            }
        }

        self.hashes.clear();
        if !self.pending_keys.is_empty() {
            let indices = UInt64Array::from_iter_values(
                self.pending_keys.iter().map(|&key| key as u64),
            );
            let referenced_values = take(self.values.as_ref(), &indices, None)?;
            self.hashes.resize(referenced_values.len(), 0);
            create_hashes(
                std::slice::from_ref(&referenced_values),
                &self.random_state,
                &mut self.hashes,
            )?;

            let dense_dictionary = self.pending_keys.len().saturating_mul(2)
                >= dict.len()
                && self.values.len()
                    <= dict.len().saturating_mul(MAX_DENSE_DICTIONARY_RATIO);
            let additional = if self.value_map.is_empty() && dense_dictionary {
                self.values.len()
            } else {
                self.pending_keys.len()
            };
            self.value_map.reserve(additional, |&(hash, _)| hash);
        }

        let mut hash_idx = 0;
        for event in &self.pending_events {
            let Some(key) = *event else {
                self.null_group = Some(self.group_to_key.len());
                self.group_to_key.push(None);
                continue;
            };

            let hash = self.hashes[hash_idx];
            hash_idx += 1;
            let comparator = &self.comparator;
            let group_to_key = &self.group_to_key;
            let group = self
                .value_map
                .find(hash, |&(entry_hash, group)| {
                    entry_hash == hash
                        && comparator(
                            group_to_key[group]
                                .expect("value map excludes null")
                                .as_usize(),
                            key,
                        ) == Ordering::Equal
                })
                .map(|&(_, group)| group)
                .unwrap_or_else(|| {
                    let group = self.group_to_key.len();
                    self.group_to_key.push(Some(K::Native::usize_as(key)));
                    self.value_map.insert_unique(
                        hash,
                        (hash, group),
                        |&(entry_hash, _)| entry_hash,
                    );
                    group
                });
            self.key_to_group[key] = group;
        }

        if !self.pending_null_keys.is_empty() {
            let null_group = self.null_group.expect("null event creates a group");
            for &key in &self.pending_null_keys {
                self.key_to_group[key] = null_group;
            }
        }

        groups.clear();
        groups.try_reserve(dict.len()).map_err(|error| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::MemoryError(error.to_string())),
                None,
            )
        })?;
        if keys.null_count() == 0 {
            for &key in raw_keys {
                groups.push(self.key_to_group[key.as_usize()]);
            }
        } else {
            for row in 0..dict.len() {
                groups.push(match dict.key(row) {
                    Some(key) => self.key_to_group[key],
                    None => self.null_group.expect("null key creates a group"),
                });
            }
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.group_to_key.len()
    }

    fn is_empty(&self) -> bool {
        self.group_to_key.is_empty()
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.values.get_array_memory_size()
            + self.key_to_group.allocated_size()
            + self.group_to_key.allocated_size()
            + self.value_map.allocation_size()
            + self.pending_keys.allocated_size()
            + self.pending_events.allocated_size()
            + self.pending_null_keys.allocated_size()
            + self.hashes.allocated_size()
    }

    fn array_from_keys(&self, keys: Vec<Option<K::Native>>) -> ArrayRef {
        let keys: PrimitiveArray<K> = keys.into_iter().collect();
        Arc::new(DictionaryArray::<K>::new(keys, Arc::clone(&self.values)))
    }

    fn into_array(mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.emit(emit_to)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = match emit_to {
            EmitTo::All => self.len(),
            EmitTo::First(n) => n,
        };
        let emitted = self.array_from_keys(self.group_to_key[..n].to_vec());

        if n == self.len() {
            self.group_to_key.clear();
            self.key_to_group.fill(UNRESOLVED);
            self.value_map.clear();
            self.null_group = None;
            return Ok(emitted);
        }

        for group in &mut self.key_to_group {
            if *group < PENDING {
                *group = group.checked_sub(n).unwrap_or(UNRESOLVED);
            }
        }
        self.value_map.retain(|(_, group)| {
            if let Some(remaining) = group.checked_sub(n) {
                *group = remaining;
                true
            } else {
                false
            }
        });
        self.null_group = self.null_group.and_then(|group| group.checked_sub(n));
        let _ = split_vec_min_alloc(&mut self.group_to_key, n);
        Ok(emitted)
    }

    fn values_preserving(&self, selection: GroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.len())?;
        Ok(self.array_from_keys(
            selection
                .iter()
                .map(|group| self.group_to_key[group])
                .collect(),
        ))
    }

    fn shrink_to_fit(&mut self) {
        self.value_map.shrink_to_fit(|&(hash, _)| hash);
        self.key_to_group.shrink_to_fit();
        self.group_to_key.shrink_to_fit();
        self.pending_keys.shrink_to_fit();
        self.pending_events.shrink_to_fit();
        self.pending_null_keys.shrink_to_fit();
        self.hashes.shrink_to_fit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{Field, Float64Type, Int32Type, Schema};

    fn schema(value_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type)),
            true,
        )]))
    }

    fn dict(keys: Vec<Option<i32>>, values: &ArrayRef) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys),
            Arc::clone(values),
        ))
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        cast(array.as_ref(), &DataType::Utf8)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect()
    }

    #[test]
    fn shared_values_reuse_keys_and_only_process_references() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some("unused"),
        ]));
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        let mut groups = vec![];

        group_values.intern(
            &[dict(
                vec![Some(2), Some(1), Some(0), None, Some(3)],
                &values,
            )],
            &mut groups,
        )?;
        assert_eq!(groups, vec![0, 1, 0, 2, 2]);

        group_values.intern(&[dict(vec![Some(1), Some(2)], &values)], &mut groups)?;
        assert_eq!(groups, vec![1, 0]);
        assert_eq!(group_values.len(), 3);

        let direct = group_values.direct.as_ref().unwrap();
        assert_eq!(direct.value_map.len(), 2);
        assert_eq!(direct.key_to_group[4], UNRESOLVED);
        assert_eq!(
            strings(&group_values.emit(EmitTo::All)?[0]),
            vec![Some("a".into()), Some("b".into()), None,]
        );
        Ok(())
    }

    #[test]
    fn changed_values_array_falls_back_and_preserves_groups() -> Result<()> {
        let first_values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let second_values: ArrayRef = Arc::new(StringArray::from(vec!["b", "c", "a"]));
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        let mut groups = vec![];

        group_values
            .intern(&[dict(vec![Some(0), Some(1)], &first_values)], &mut groups)?;
        assert_eq!(groups, vec![0, 1]);
        group_values.intern(
            &[dict(vec![Some(0), Some(1), Some(2)], &second_values)],
            &mut groups,
        )?;
        assert_eq!(groups, vec![1, 2, 0]);
        assert!(group_values.fallback.is_some());
        assert_eq!(
            strings(&group_values.emit(EmitTo::All)?[0]),
            vec![Some("a".into()), Some("b".into()), Some("c".into()),]
        );
        Ok(())
    }

    #[test]
    fn partial_emit_remaps_cached_keys() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        let mut groups = vec![];
        group_values.intern(
            &[dict(vec![Some(0), Some(1), Some(2)], &values)],
            &mut groups,
        )?;

        let emitted = group_values.emit(EmitTo::First(1))?;
        assert_eq!(strings(&emitted[0]), vec![Some("a".into())]);
        group_values.intern(
            &[dict(vec![Some(1), Some(2), Some(0)], &values)],
            &mut groups,
        )?;
        assert_eq!(groups, vec![0, 1, 2]);
        assert_eq!(
            strings(&group_values.emit(EmitTo::All)?[0]),
            vec![Some("b".into()), Some("c".into()), Some("a".into()),]
        );
        Ok(())
    }

    #[test]
    fn preserving_values_reorders_without_changing_groups() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        let mut groups = vec![];
        group_values.intern(
            &[dict(vec![Some(0), Some(1), Some(2)], &values)],
            &mut groups,
        )?;

        let selected = group_values
            .values_preserving(GroupSelection::try_from_indices(&[2, 0, 2], 3)?)?;
        assert_eq!(
            strings(&selected[0]),
            vec![Some("c".into()), Some("a".into()), Some("c".into()),]
        );
        assert_eq!(group_values.len(), 3);
        Ok(())
    }

    #[test]
    fn normalizes_float_negative_zero() -> Result<()> {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.0]));
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::new(schema(DataType::Float64));
        let mut groups = vec![];
        group_values.intern(
            &[dict(vec![Some(0), Some(1), Some(2)], &values)],
            &mut groups,
        )?;
        assert_eq!(groups, vec![0, 0, 1]);

        let output = group_values.emit(EmitTo::All)?;
        let output = cast(output[0].as_ref(), &DataType::Float64)?;
        let output = output.as_primitive::<Float64Type>();
        assert_eq!(output.value(0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(output.value(1), 1.0);
        Ok(())
    }

    #[test]
    fn reserve_is_bounded_by_dictionary_density_and_clear_releases_it() -> Result<()> {
        fn values(len: usize) -> ArrayRef {
            Arc::new(StringArray::from_iter_values(
                (0..len).map(|index| format!("value_{index}")),
            ))
        }

        let mut dense = GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        let mut groups = vec![];
        dense.intern(
            &[dict((0..16).map(Some).collect(), &values(128))],
            &mut groups,
        )?;
        assert!(dense.direct.as_ref().unwrap().value_map.capacity() >= 128);

        let mut sparse = GroupValuesDictionary::<Int32Type>::new(schema(DataType::Utf8));
        sparse.intern(
            &[dict((0..16).map(Some).collect(), &values(1_024))],
            &mut groups,
        )?;
        assert!(sparse.direct.as_ref().unwrap().value_map.capacity() < 1_024);
        assert!(sparse.size() > size_of::<GroupValuesDictionary<Int32Type>>());

        sparse.clear_shrink(0);
        assert_eq!(sparse.size(), size_of::<GroupValuesDictionary<Int32Type>>());
        Ok(())
    }
}
