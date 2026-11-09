// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::{Arc, Weak};

use arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, Int64Array, PrimitiveArray,
};
use arrow::compute::take;
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Field, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{EmitTo, GroupSelection};
use hashbrown::HashMap;

use crate::aggregates::group_values::{
    GroupValues, new_group_values as new_inner_group_values,
};
use crate::aggregates::order::GroupClustering;

const UNRESOLVED: usize = usize::MAX;
const PENDING: usize = usize::MAX - 1;
// Sparse entries store both the dictionary index and group id plus hash-table
// metadata, so prefer the faster dense cache until it would be over 4x larger.
const SPARSE_CACHE_RATIO: usize = 4;
// Arrow arrays are bounded by `isize::MAX`, leaving the high bit available to
// tag unresolved row entries without allocating another row-sized buffer.
const PENDING_VALUE_BIT: usize = 1 << (usize::BITS - 1);

enum ValueToGroup {
    Dense(Vec<usize>),
    Sparse(HashMap<usize, usize>),
}

impl ValueToGroup {
    fn new(num_values: usize, num_keys: usize) -> Self {
        // A dense cache has a much cheaper hot-path lookup and less overhead per
        // entry. Use a sparse cache only when initializing the dense cache would
        // touch substantially more entries than this batch can reference.
        if num_values <= num_keys.saturating_mul(SPARSE_CACHE_RATIO) {
            Self::Dense(vec![UNRESOLVED; num_values])
        } else {
            Self::Sparse(HashMap::new())
        }
    }

    fn maybe_promote(&mut self, num_values: usize) {
        let Self::Sparse(values) = self else {
            return;
        };
        if num_values > values.len().saturating_mul(SPARSE_CACHE_RATIO) {
            return;
        }

        let mut dense = vec![UNRESOLVED; num_values];
        for (&value_index, &group_index) in values.iter() {
            dense[value_index] = group_index;
        }
        *self = Self::Dense(dense);
    }
    #[inline]
    fn get(&self, value_index: usize) -> usize {
        match self {
            Self::Dense(values) => values[value_index],
            Self::Sparse(values) => {
                values.get(&value_index).copied().unwrap_or(UNRESOLVED)
            }
        }
    }

    #[inline]
    fn insert(&mut self, value_index: usize, group_index: usize) {
        match self {
            Self::Dense(values) => values[value_index] = group_index,
            Self::Sparse(values) => {
                values.insert(value_index, group_index);
            }
        }
    }

    fn reset(&mut self, value_index: usize) {
        match self {
            Self::Dense(values) => values[value_index] = UNRESOLVED,
            Self::Sparse(values) => {
                values.remove(&value_index);
            }
        }
    }

    fn allocation_size(&self) -> usize {
        match self {
            Self::Dense(values) => values.capacity() * size_of::<usize>(),
            Self::Sparse(values) => values.allocation_size(),
        }
    }
}

impl Default for ValueToGroup {
    fn default() -> Self {
        Self::Dense(vec![])
    }
}

enum PendingValue {
    Value(usize),
    NullKey,
}

/// [`GroupValues`] for a single dictionary-encoded group key.
struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send + Sync> {
    /// Group values stored using the specialized single-column implementation
    /// for the dictionary's value type.
    values: Box<dyn GroupValues>,
    /// Maps a value index in `cached_values` to its group id.
    value_to_group: ValueToGroup,
    /// Weak identity cache for the current dictionary values array.
    cached_values: Option<Weak<dyn Array>>,
    /// Group id for both null keys and keys that point to null values.
    null_group: Option<usize>,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupValuesDictionary<K> {
    fn try_new(value_field: Field) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![value_field]));
        let values = new_inner_group_values(schema, &GroupClustering::None)?;
        Ok(Self {
            values,
            value_to_group: ValueToGroup::default(),
            cached_values: None,
            null_group: None,
            _phantom: PhantomData,
        })
    }

    fn sync_cache(&mut self, values: &ArrayRef, num_keys: usize) {
        if self
            .cached_values
            .as_ref()
            .is_some_and(|cached| std::ptr::addr_eq(cached.as_ptr(), Arc::as_ptr(values)))
        {
            return;
        }

        self.value_to_group = ValueToGroup::new(values.len(), num_keys);
        self.cached_values = Some(Arc::downgrade(values));
    }

    fn clear_cache(&mut self) {
        self.value_to_group = ValueToGroup::default();
        self.cached_values = None;
        self.null_group = None;
    }

    fn reset_pending(&mut self, pending: &[PendingValue]) {
        for value in pending {
            if let PendingValue::Value(value_index) = value {
                self.value_to_group.reset(*value_index);
            }
        }
    }

    #[inline]
    fn group_or_queue(
        &mut self,
        value_index: usize,
        pending: &mut Vec<PendingValue>,
    ) -> usize {
        match self.value_to_group.get(value_index) {
            UNRESOLVED => {
                self.value_to_group.insert(value_index, PENDING);
                pending.push(PendingValue::Value(value_index));
                PENDING_VALUE_BIT | value_index
            }
            PENDING => PENDING_VALUE_BIT | value_index,
            group_index => group_index,
        }
    }

    fn check_key_overflow(&self) -> Result<()> {
        let non_null_count = self.values.len() - self.null_group.is_some() as usize;
        let max = match K::DATA_TYPE {
            DataType::Int8 => i8::MAX as usize,
            DataType::Int16 => i16::MAX as usize,
            DataType::Int32 => i32::MAX as usize,
            DataType::Int64 => i64::MAX as usize,
            DataType::UInt8 => u8::MAX as usize,
            DataType::UInt16 => u16::MAX as usize,
            DataType::UInt32 => u32::MAX as usize,
            DataType::UInt64 => usize::MAX,
            _ => unreachable!("validated dictionary key type"),
        };
        if non_null_count != 0 && non_null_count - 1 > max {
            return exec_err!(
                "Dictionary key type {:?} cannot represent {} distinct values",
                K::DATA_TYPE,
                non_null_count
            );
        }
        Ok(())
    }

    fn build_dictionary(values: ArrayRef, groups: &[usize]) -> ArrayRef {
        let mut value_keys = Vec::with_capacity(values.len());
        let mut non_null_indices = Vec::with_capacity(values.len());
        for value_index in 0..values.len() {
            if values.is_null(value_index) {
                value_keys.push(None);
            } else {
                value_keys.push(Some(K::Native::usize_as(non_null_indices.len())));
                non_null_indices.push(value_index as i64);
            }
        }

        let values = if non_null_indices.len() == values.len() {
            values
        } else {
            let indices = Int64Array::from(non_null_indices);
            take(values.as_ref(), &indices, None).expect("compact dictionary values")
        };
        let keys: PrimitiveArray<K> =
            groups.iter().map(|&group| value_keys[group]).collect();
        Arc::new(DictionaryArray::<K>::new(keys, values))
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupValues for GroupValuesDictionary<K> {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        let dictionary = cols[0].as_dictionary::<K>();
        let values = dictionary.values();
        let keys = dictionary.keys();
        self.sync_cache(values, keys.len() - keys.null_count());

        groups.clear();
        groups.resize(dictionary.len(), PENDING);
        let mut pending = vec![];
        let mut null_key_pending = false;
        let raw_keys = keys.values();
        if keys.null_count() == 0 {
            for (group, key) in groups.iter_mut().zip(raw_keys) {
                *group = self.group_or_queue(key.as_usize(), &mut pending);
            }
        } else {
            let nulls = keys.nulls().unwrap();
            for (row, group) in groups.iter_mut().enumerate() {
                if nulls.is_null(row) {
                    if let Some(null_group) = self.null_group {
                        *group = null_group;
                    } else if !null_key_pending {
                        pending.push(PendingValue::NullKey);
                        null_key_pending = true;
                    }
                } else {
                    *group = self.group_or_queue(raw_keys[row].as_usize(), &mut pending);
                }
            }
        }

        if !pending.is_empty() {
            let indices: Int64Array = pending
                .iter()
                .map(|pending| match pending {
                    PendingValue::Value(value_index) => Some(*value_index as i64),
                    PendingValue::NullKey => None,
                })
                .collect();
            let pending_values = match take(values.as_ref(), &indices, None) {
                Ok(values) => values,
                Err(error) => {
                    self.reset_pending(&pending);
                    return Err(error.into());
                }
            };
            let mut pending_groups = Vec::with_capacity(pending.len());
            if let Err(error) = self.values.intern(&[pending_values], &mut pending_groups)
            {
                self.reset_pending(&pending);
                return Err(error);
            }
            debug_assert_eq!(pending_groups.len(), pending.len());

            for (pending, group_index) in pending.into_iter().zip(pending_groups) {
                match pending {
                    PendingValue::Value(value_index) => {
                        self.value_to_group.insert(value_index, group_index);
                        if values.is_null(value_index) {
                            self.null_group = Some(group_index);
                        }
                    }
                    PendingValue::NullKey => self.null_group = Some(group_index),
                }
            }
            self.value_to_group.maybe_promote(values.len());

            for group in groups.iter_mut() {
                if *group == PENDING {
                    *group = self.null_group.expect("null key was resolved");
                } else if *group & PENDING_VALUE_BIT != 0 {
                    let value_index = *group & !PENDING_VALUE_BIT;
                    *group = self.value_to_group.get(value_index);
                }
            }
        }

        debug_assert!(
            groups
                .iter()
                .all(|&group| group != PENDING && group & PENDING_VALUE_BIT == 0)
        );
        self.check_key_overflow()
    }

    fn size(&self) -> usize {
        self.values.size() + self.value_to_group.allocation_size() + size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut output = self.values.emit(emit_to)?;
        debug_assert_eq!(output.len(), 1);
        let values = output.pop().expect("one dictionary value column");
        self.clear_cache();
        let groups = (0..values.len()).collect::<Vec<_>>();
        Ok(vec![Self::build_dictionary(values, &groups)])
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.len())?;

        let mut group_to_unique = vec![UNRESOLVED; self.len()];
        let mut unique_groups = vec![];
        let mut selected_groups = Vec::with_capacity(selection.len());
        for group_index in selection.iter() {
            let unique_index = if group_to_unique[group_index] == UNRESOLVED {
                let unique_index = unique_groups.len();
                group_to_unique[group_index] = unique_index;
                unique_groups.push(group_index);
                unique_index
            } else {
                group_to_unique[group_index]
            };
            selected_groups.push(unique_index);
        }

        let unique_selection =
            GroupSelection::try_from_indices(&unique_groups, self.len())?;
        let mut output = self.values.values_preserving(unique_selection)?;
        debug_assert_eq!(output.len(), 1);
        let values = output.pop().expect("one dictionary value column");
        Ok(vec![Self::build_dictionary(values, &selected_groups)])
    }

    fn supports_values_preserving(&self) -> bool {
        self.values.supports_values_preserving()
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear_shrink(num_rows);
        self.value_to_group = ValueToGroup::default();
        self.cached_values = None;
        self.null_group = None;
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.value_to_group = ValueToGroup::default();
        self.cached_values = None;
    }
}

pub(in crate::aggregates::group_values) fn new_group_values(
    field: &Field,
) -> Result<Box<dyn GroupValues>> {
    let DataType::Dictionary(key_type, value_type) = field.data_type() else {
        unreachable!("single dictionary group values require a dictionary field")
    };
    let value_field = Field::new("", value_type.as_ref().clone(), true);

    macro_rules! single_dict {
        ($K:ty) => {
            Ok(Box::new(GroupValuesDictionary::<$K>::try_new(value_field)?)
                as Box<dyn GroupValues>)
        };
    }

    match key_type.as_ref() {
        DataType::Int8 => single_dict!(Int8Type),
        DataType::Int16 => single_dict!(Int16Type),
        DataType::Int32 => single_dict!(Int32Type),
        DataType::Int64 => single_dict!(Int64Type),
        DataType::UInt8 => single_dict!(UInt8Type),
        DataType::UInt16 => single_dict!(UInt16Type),
        DataType::UInt32 => single_dict!(UInt32Type),
        DataType::UInt64 => single_dict!(UInt64Type),
        _ => datafusion_common::not_impl_err!(
            "Dictionary key type {key_type} not supported in single-column group values"
        ),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, DictionaryArray, Int8Array, Int32Array, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{Int8Type, Int32Type};

    use super::*;

    fn i32_dict(keys: &[Option<i32>], values: &[Option<&str>]) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys.to_vec()),
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

    fn i8_dict(keys: Vec<Option<i8>>, values: Vec<String>) -> ArrayRef {
        Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(keys),
            Arc::new(StringArray::from(values)),
        ))
    }

    #[test]
    fn handles_dictionary_changes_and_emit() {
        let field = Field::new(
            "group",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let mut group_values = new_group_values(&field).unwrap();
        assert!(group_values.supports_values_preserving());

        let mut groups = vec![];
        let batch = i32_dict(
            &[Some(0), Some(1), Some(0), None, Some(2)],
            &[Some("a"), Some("b"), None, Some("unused")],
        );
        group_values.intern(&[batch], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2, 2]);
        assert_eq!(group_values.len(), 3);

        // A later batch can use a different dictionary and key ordering.
        let batch = i32_dict(
            &[Some(0), Some(1), Some(2), Some(3)],
            &[Some("b"), Some("c"), Some("a"), None],
        );
        group_values.intern(&[batch], &mut groups).unwrap();
        assert_eq!(groups, vec![1, 3, 0, 2]);

        let selected = group_values
            .values_preserving(
                GroupSelection::try_from_indices(&[3, 2, 0, 1, 3], 4).unwrap(),
            )
            .unwrap();
        assert_eq!(
            str_values(&selected[0]),
            vec![
                Some("c".into()),
                None,
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
            ]
        );

        let emitted = group_values.emit(EmitTo::First(2)).unwrap();
        assert_eq!(
            str_values(&emitted[0]),
            vec![Some("a".into()), Some("b".into())]
        );
        assert_eq!(group_values.len(), 2);

        // Group ids restart at zero for the retained groups after a partial emit.
        let batch = i32_dict(&[Some(0), Some(1), Some(2)], &[Some("c"), None, Some("d")]);
        group_values.intern(&[batch], &mut groups).unwrap();
        assert_eq!(groups, vec![1, 0, 2]);

        let emitted = group_values.emit(EmitTo::All).unwrap();
        assert_eq!(
            str_values(&emitted[0]),
            vec![None, Some("c".into()), Some("d".into())]
        );
        assert!(group_values.is_empty());
    }

    #[test]
    fn null_does_not_consume_dictionary_key_capacity() {
        let field = Field::new(
            "group",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let mut group_values = new_group_values(&field).unwrap();
        let mut keys = (0..=i8::MAX).map(Some).collect::<Vec<_>>();
        keys.push(None);
        let values = (0..=i8::MAX).map(|value| format!("v{value}")).collect();
        let mut groups = vec![];

        group_values
            .intern(&[i8_dict(keys, values)], &mut groups)
            .unwrap();
        assert_eq!(group_values.len(), i8::MAX as usize + 2);

        let selection = (0..group_values.len()).collect::<Vec<_>>();
        let selected = group_values
            .values_preserving(
                GroupSelection::try_from_indices(&selection, selection.len()).unwrap(),
            )
            .unwrap();
        assert_eq!(selected[0].len(), selection.len());

        let error = group_values
            .intern(
                &[i8_dict(vec![Some(0)], vec!["overflow".to_string()])],
                &mut groups,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot represent 129 distinct values")
        );
    }

    #[test]
    fn cache_does_not_retain_dictionary_values() {
        let field = Field::new(
            "group",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let mut group_values = new_group_values(&field).unwrap();
        let input = i32_dict(&[Some(0)], &[Some("value")]);
        let values = Arc::clone(input.as_dictionary::<Int32Type>().values());
        let weak_values = Arc::downgrade(&values);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups).unwrap();
        drop(values);
        assert!(weak_values.upgrade().is_none());
    }

    #[test]
    fn oversized_dictionary_uses_sparse_cache() {
        let value_field = Field::new("", DataType::Utf8, true);
        let mut group_values =
            GroupValuesDictionary::<Int32Type>::try_new(value_field).unwrap();
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..1_000).map(|value| format!("value_{value}")),
        ));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![999, 999]),
            Arc::clone(&values),
        ));
        let mut groups = vec![];

        group_values.intern(&[dictionary], &mut groups).unwrap();

        assert_eq!(groups, vec![0, 0]);
        let ValueToGroup::Sparse(cache) = &group_values.value_to_group else {
            panic!("oversized dictionary should not allocate a dense cache");
        };
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&999), Some(&0));

        // Reusing the same values array with enough referenced entries promotes
        // the cache, preserving the mappings learned while it was sparse.
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from_iter_values(0..249),
            values,
        ));
        group_values.intern(&[dictionary], &mut groups).unwrap();
        assert!(matches!(
            group_values.value_to_group,
            ValueToGroup::Dense(_)
        ));
        assert_eq!(groups, (1..250).collect::<Vec<_>>());
    }
}
