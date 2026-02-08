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
    NullBufferBuilder, OffsetSizeTrait, PrimitiveArray, RunArray, StringViewArray,
    StructArray, make_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Field, Fields, Int32Type, Int64Type,
};
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
    supports_nulls: bool,
}

fn criterion_benchmark(c: &mut Criterion) {
    let pool = StringPool::new(100, 64);
    // poll with small strings for string view tests (<=12 bytes are inlined)
    let small_pool = StringPool::new(100, 5);
    let cases = [
        BenchData {
            name: "int64",
            array: primitive_array::<Int64Type>(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "utf8",
            array: pool.string_array::<i32>(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "large_utf8",
            array: pool.string_array::<i64>(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "utf8_view",
            array: pool.string_view_array(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "utf8_view (small)",
            array: small_pool.string_view_array(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "dictionary_utf8_int32",
            array: pool.dictionary_array::<Int32Type>(BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "struct_array",
            array: create_struct_array(&pool, BATCH_SIZE),
            supports_nulls: true,
        },
        BenchData {
            name: "run_array_int32",
            array: create_run_array::<Int32Type>(BATCH_SIZE),
            supports_nulls: true,
        },
    ];

    for BenchData {
        name,
        array,
        supports_nulls,
    } in cases
    {
        c.bench_function(&format!("{name}: single, no nulls"), |b| {
            do_hash_test(b, std::slice::from_ref(&array));
        });
        c.bench_function(&format!("{name}: multiple, no nulls"), |b| {
            let arrays = vec![array.clone(), array.clone(), array.clone()];
            do_hash_test(b, &arrays);
        });

        if supports_nulls {
            let nullable_array = add_nulls(&array);

            c.bench_function(&format!("{name}: single, nulls"), |b| {
                do_hash_test(b, std::slice::from_ref(&nullable_array));
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

// Returns a new array that is the same as array, but with nulls
// Handles the special case of RunArray where nulls must be in the values array
fn add_nulls(array: &ArrayRef) -> ArrayRef {
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::RunEndEncoded(_, _) => {
            // RunArray can't have top-level nulls, so apply nulls to the values array
            let run_array = array
                .as_any()
                .downcast_ref::<RunArray<Int32Type>>()
                .expect("Expected RunArray");

            let run_ends_buffer = run_array.run_ends().inner().clone();
            let run_ends_array = PrimitiveArray::<Int32Type>::new(run_ends_buffer, None);
            let values = run_array.values().clone();

            // Add nulls to the values array
            let values_with_nulls = {
                let array_data = values
                    .clone()
                    .into_data()
                    .into_builder()
                    .nulls(Some(create_null_mask(values.len())))
                    .build()
                    .unwrap();
                make_array(array_data)
            };

            Arc::new(
                RunArray::try_new(&run_ends_array, values_with_nulls.as_ref())
                    .expect("Failed to create RunArray with null values"),
            )
        }
        _ => {
            let array_data = array
                .clone()
                .into_data()
                .into_builder()
                .nulls(Some(create_null_mask(array.len())))
                .build()
                .unwrap();
            make_array(array_data)
        }
    }
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

fn boolean_array(array_len: usize) -> ArrayRef {
    let mut rng = make_rng();
    Arc::new(
        (0..array_len)
            .map(|_| Some(rng.random::<bool>()))
            .collect::<arrow::array::BooleanArray>(),
    )
}

/// Create a StructArray with multiple columns
fn create_struct_array(pool: &StringPool, array_len: usize) -> ArrayRef {
    let bool_array = boolean_array(array_len);
    let int32_array = primitive_array::<Int32Type>(array_len);
    let int64_array = primitive_array::<Int64Type>(array_len);
    let str_array = pool.string_array::<i32>(array_len);

    let fields = Fields::from(vec![
        Field::new("bool_col", DataType::Boolean, false),
        Field::new("int32_col", DataType::Int32, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("string_col", DataType::Utf8, false),
    ]);

    Arc::new(StructArray::new(
        fields,
        vec![bool_array, int32_array, int64_array, str_array],
        None,
    ))
}

/// Create a RunArray to test run array hashing.
fn create_run_array<T>(array_len: usize) -> ArrayRef
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = make_rng();

    // Create runs of varying lengths
    let mut run_ends = Vec::new();
    let mut values = Vec::new();
    let mut current_end = 0;

    while current_end < array_len {
        // Random run length between 1 and 50
        let run_length = rng.random_range(1..=50).min(array_len - current_end);
        current_end += run_length;
        run_ends.push(current_end as i32);
        values.push(Some(rng.random::<T::Native>()));
    }

    let run_ends_array = Arc::new(PrimitiveArray::<Int32Type>::from(run_ends));
    let values_array: Arc<dyn Array> =
        Arc::new(values.into_iter().collect::<PrimitiveArray<T>>());

    Arc::new(
        RunArray::try_new(&run_ends_array, values_array.as_ref())
            .expect("Failed to create RunArray"),
    )
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
