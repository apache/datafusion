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

//! Array caching utilities for scalar values

use std::iter::repeat_n;
use std::sync::{Arc, LazyLock, Mutex};

use arrow::array::{Array, ArrayRef, PrimitiveArray, new_null_array};
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Int8Type, Int16Type, Int32Type, Int64Type,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

/// Maximum number of rows to cache to be conservative on memory usage
const MAX_CACHE_SIZE: usize = 1024 * 1024;

/// Cache for dictionary key arrays to avoid repeated allocations
/// when the same size is used frequently.
///
/// Similar to PartitionColumnProjector's ZeroBufferGenerators, this cache
/// stores key arrays for different dictionary key types. The cache is
/// limited to 1 entry per type (the last size used) to prevent memory leaks
/// for extremely large array requests.
#[derive(Debug)]
struct KeyArrayCache<K: ArrowDictionaryKeyType> {
    cache: Option<(usize, bool, PrimitiveArray<K>)>, // (num_rows, is_null, key_array)
}

impl<K: ArrowDictionaryKeyType> Default for KeyArrayCache<K> {
    fn default() -> Self {
        Self { cache: None }
    }
}

impl<K: ArrowDictionaryKeyType> KeyArrayCache<K> {
    /// Get or create a cached key array for the given number of rows and null status
    fn get_or_create(&mut self, num_rows: usize, is_null: bool) -> PrimitiveArray<K> {
        // Check cache size limit to prevent memory leaks
        if num_rows > MAX_CACHE_SIZE {
            // For very large arrays, don't cache them - just create and return
            return self.create_key_array(num_rows, is_null);
        }

        match &self.cache {
            Some((cached_num_rows, cached_is_null, cached_array))
                if *cached_num_rows == num_rows && *cached_is_null == is_null =>
            {
                // Cache hit: reuse existing array if same size and null status
                cached_array.clone()
            }
            _ => {
                // Cache miss: create new array and cache it
                let key_array = self.create_key_array(num_rows, is_null);
                self.cache = Some((num_rows, is_null, key_array.clone()));
                key_array
            }
        }
    }

    /// Create a new key array with the specified number of rows and null status
    fn create_key_array(&self, num_rows: usize, is_null: bool) -> PrimitiveArray<K> {
        let key_array: PrimitiveArray<K> = repeat_n(
            if is_null {
                None
            } else {
                Some(K::default_value())
            },
            num_rows,
        )
        .collect();
        key_array
    }
}

/// Cache for null arrays to avoid repeated allocations
/// when the same size is used frequently.
#[derive(Debug, Default)]
struct NullArrayCache {
    cache: Option<(usize, ArrayRef)>, // (num_rows, null_array)
}

impl NullArrayCache {
    /// Get or create a cached null array for the given number of rows
    fn get_or_create(&mut self, num_rows: usize) -> ArrayRef {
        // Check cache size limit to prevent memory leaks
        if num_rows > MAX_CACHE_SIZE {
            // For very large arrays, don't cache them - just create and return
            return new_null_array(&DataType::Null, num_rows);
        }

        match &self.cache {
            Some((cached_num_rows, cached_array)) if *cached_num_rows == num_rows => {
                // Cache hit: reuse existing array if same size
                Arc::clone(cached_array)
            }
            _ => {
                // Cache miss: create new array and cache it
                let null_array = new_null_array(&DataType::Null, num_rows);
                self.cache = Some((num_rows, Arc::clone(&null_array)));
                null_array
            }
        }
    }
}

/// Global cache for dictionary key arrays and null arrays
#[derive(Debug, Default)]
struct ArrayCaches {
    cache_i8: KeyArrayCache<Int8Type>,
    cache_i16: KeyArrayCache<Int16Type>,
    cache_i32: KeyArrayCache<Int32Type>,
    cache_i64: KeyArrayCache<Int64Type>,
    cache_u8: KeyArrayCache<UInt8Type>,
    cache_u16: KeyArrayCache<UInt16Type>,
    cache_u32: KeyArrayCache<UInt32Type>,
    cache_u64: KeyArrayCache<UInt64Type>,
    null_cache: NullArrayCache,
}

static ARRAY_CACHES: LazyLock<Mutex<ArrayCaches>> =
    LazyLock::new(|| Mutex::new(ArrayCaches::default()));

/// Get the global cache for arrays
fn get_array_caches() -> &'static Mutex<ArrayCaches> {
    &ARRAY_CACHES
}

/// Get or create a cached null array for the given number of rows
pub(crate) fn get_or_create_cached_null_array(num_rows: usize) -> ArrayRef {
    let cache = get_array_caches();
    let mut caches = cache.lock().unwrap();
    caches.null_cache.get_or_create(num_rows)
}

/// Get or create a cached key array for a specific key type
pub(crate) fn get_or_create_cached_key_array<K: ArrowDictionaryKeyType>(
    num_rows: usize,
    is_null: bool,
) -> PrimitiveArray<K> {
    let cache = get_array_caches();
    let mut caches = cache.lock().unwrap();

    // Use the DATA_TYPE to dispatch to the correct cache, similar to original implementation
    match K::DATA_TYPE {
        DataType::Int8 => {
            let array = caches.cache_i8.get_or_create(num_rows, is_null);
            // Convert using ArrayData to avoid unsafe transmute
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::Int16 => {
            let array = caches.cache_i16.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::Int32 => {
            let array = caches.cache_i32.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::Int64 => {
            let array = caches.cache_i64.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::UInt8 => {
            let array = caches.cache_u8.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::UInt16 => {
            let array = caches.cache_u16.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::UInt32 => {
            let array = caches.cache_u32.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        DataType::UInt64 => {
            let array = caches.cache_u64.get_or_create(num_rows, is_null);
            let array_data = array.to_data();
            PrimitiveArray::<K>::from(array_data)
        }
        _ => {
            // Fallback for unsupported types - create array directly without caching
            let key_array: PrimitiveArray<K> = repeat_n(
                if is_null {
                    None
                } else {
                    Some(K::default_value())
                },
                num_rows,
            )
            .collect();
            key_array
        }
    }
}
