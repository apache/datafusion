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

//! Benchmarks for `with_hashes` function

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, DictionaryArray, GenericStringArray,
    NullBufferBuilder, OffsetSizeTrait, PrimitiveArray, StringViewArray, make_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ArrowDictionaryKeyType, Int32Type, Int64Type};
use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use datafusion_common::hash_utils::with_hashes;
use rand::Rng;
use rand::SeedableRng;
use rand::distr::{Alphanumeric, Distribution, StandardUniform};
use rand::prelude::StdRng;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

struct BenchData {
    name: &'static str,
    array: ArrayRef,
}

fn criterion_benchmark(c: &mut Criterion) {
    let pool = StringPool::new(100, 64);
    // poll with small strings for string view tests (<=12 bytes are inlined)
    let small_pool = StringPool::new(100, 5);
    let cases = [
        BenchData {
            name: "int64",
            array: primitive_array::<Int64Type>(BATCH_SIZE),
        },
        BenchData {
            name: "utf8",
            array: pool.string_array::<i32>(BATCH_SIZE),
        },
        BenchData {
            name: "large_utf8",
            array: pool.string_array::<i64>(BATCH_SIZE),
        },
        BenchData {
            name: "utf8_view",
            array: pool.string_view_array(BATCH_SIZE),
        },
        BenchData {
            name: "utf8_view (small)",
            array: small_pool.string_view_array(BATCH_SIZE),
        },
        BenchData {
            name: "dictionary_utf8_int32",
            array: pool.dictionary_array::<Int32Type>(BATCH_SIZE),
        },
    ];

    for BenchData { name, array } in cases {
        // with_hash has different code paths for single vs multiple arrays and nulls vs no nulls
        let nullable_array = add_nulls(&array);
        c.bench_function(&format!("{name}: single, no nulls"), |b| {
            do_hash_test(b, std::slice::from_ref(&array));
        });
        c.bench_function(&format!("{name}: single, nulls"), |b| {
            do_hash_test(b, std::slice::from_ref(&nullable_array));
        });
        c.bench_function(&format!("{name}: multiple, no nulls"), |b| {
            let arrays = vec![array.clone(), array.clone(), array.clone()];
            do_hash_test(b, &arrays);
        });
        c.bench_function(&format!("{name}: multiple, nulls"), |b| {
            let arrays = vec![
                nullable_array.clone(),
                nullable_array.clone(),
                nullable_array.clone(),
            ];
            do_hash_test(b, &arrays);
        });
    }
}

fn do_hash_test(b: &mut Bencher, arrays: &[ArrayRef]) {
    let state = RandomState::new();
    b.iter(|| {
        with_hashes(arrays, &state, |hashes| {
            assert_eq!(hashes.len(), BATCH_SIZE); // make sure the result is used
            Ok(())
        })
        .unwrap();
    });
}

fn create_null_mask(len: usize) -> NullBuffer
where
    StandardUniform: Distribution<bool>,
{
    let mut rng = make_rng();
    let null_density = 0.03;
    let mut builder = NullBufferBuilder::new(len);
    for _ in 0..len {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            builder.append_non_null();
        }
    }
    builder.finish().expect("should be nulls in buffer")
}

// Returns an new array that is the same as array, but with nulls
fn add_nulls(array: &ArrayRef) -> ArrayRef {
    let array_data = array
        .clone()
        .into_data()
        .into_builder()
        .nulls(Some(create_null_mask(array.len())))
        .build()
        .unwrap();
    make_array(array_data)
}

pub fn make_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

/// String pool for generating low cardinality data (for dictionaries and string views)
struct StringPool {
    strings: Vec<String>,
}

impl StringPool {
    /// Create a new string pool with the given number of random strings
    /// each having between 1 and max_length characters.
    fn new(pool_size: usize, max_length: usize) -> Self {
        let mut rng = make_rng();
        let mut strings = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let len = rng.random_range(1..=max_length);
            let value: Vec<u8> =
                rng.clone().sample_iter(&Alphanumeric).take(len).collect();
            strings.push(String::from_utf8(value).unwrap());
        }
        Self { strings }
    }

    /// Return an iterator over &str of the given length with values randomly chosen from the pool
    fn iter_strings(&self, len: usize) -> impl Iterator<Item = &str> {
        let mut rng = make_rng();
        (0..len).map(move |_| {
            let idx = rng.random_range(0..self.strings.len());
            self.strings[idx].as_str()
        })
    }

    /// Return a StringArray of the given length with values randomly chosen from the pool
    fn string_array<O: OffsetSizeTrait>(&self, array_length: usize) -> ArrayRef {
        Arc::new(GenericStringArray::<O>::from_iter_values(
            self.iter_strings(array_length),
        ))
    }

    /// Return a StringViewArray of the given length with values randomly chosen from the pool
    fn string_view_array(&self, array_length: usize) -> ArrayRef {
        Arc::new(StringViewArray::from_iter_values(
            self.iter_strings(array_length),
        ))
    }

    /// Return a DictionaryArray of the given length with values randomly chosen from the pool
    fn dictionary_array<T: ArrowDictionaryKeyType>(
        &self,
        array_length: usize,
    ) -> ArrayRef {
        Arc::new(DictionaryArray::<T>::from_iter(
            self.iter_strings(array_length),
        ))
    }
}

pub fn primitive_array<T>(array_len: usize) -> ArrayRef
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = make_rng();

    let array: PrimitiveArray<T> = (0..array_len)
        .map(|_| Some(rng.random::<T::Native>()))
        .collect();
    Arc::new(array)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
