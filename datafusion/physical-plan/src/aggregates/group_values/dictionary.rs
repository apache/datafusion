// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::{Arc, Weak};

use arrow::array::{Array, ArrayRef, DictionaryArray, Int64Array, PrimitiveArray};
use arrow::compute::take;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};

pub(super) fn is_cached(cached: Option<&Weak<dyn Array>>, values: &ArrayRef) -> bool {
    cached.is_some_and(|cached| std::ptr::addr_eq(cached.as_ptr(), Arc::as_ptr(values)))
}

pub(super) fn key_type_fits<K: ArrowDictionaryKeyType>(num_values: usize) -> bool {
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

pub(super) fn into_dictionary<K: ArrowDictionaryKeyType>(
    values: ArrayRef,
    groups: Option<&[usize]>,
    null_slot: Option<usize>,
) -> ArrayRef {
    let Some(null_slot) = null_slot else {
        let keys: PrimitiveArray<K> = match groups {
            Some(groups) => groups
                .iter()
                .map(|&slot| Some(K::Native::usize_as(slot)))
                .collect(),
            None => (0..values.len())
                .map(|slot| Some(K::Native::usize_as(slot)))
                .collect(),
        };
        return Arc::new(DictionaryArray::<K>::new(keys, values));
    };

    let mut remap = vec![0usize; values.len()];
    let mut next = 0usize;
    for (slot, mapped) in remap.iter_mut().enumerate() {
        if slot != null_slot {
            *mapped = next;
            next += 1;
        }
    }

    let make_key = |slot: usize| {
        if slot == null_slot {
            None
        } else {
            Some(K::Native::usize_as(remap[slot]))
        }
    };
    let keys: PrimitiveArray<K> = match groups {
        Some(groups) => groups.iter().map(|&slot| make_key(slot)).collect(),
        None => (0..values.len()).map(make_key).collect(),
    };

    let compact_indices: Int64Array = (0..values.len())
        .filter(|&slot| slot != null_slot)
        .map(|slot| slot as i64)
        .collect();
    let compact =
        take(values.as_ref(), &compact_indices, None).expect("compact dictionary values");
    Arc::new(DictionaryArray::<K>::new(keys, compact))
}
