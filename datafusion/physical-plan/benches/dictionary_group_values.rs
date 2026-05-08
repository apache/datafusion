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

//! Benchmarks for `GroupValues` over a single `Dictionary<Int32, Utf8>`
//! column. Each iteration measures `intern` (once or N times) followed by
//! `emit(EmitTo::All)`. The `Box<dyn GroupValues>` returned by
//! `new_group_values` is constructed in the setup closure of
//! `iter_batched_ref` and is not included in the timing.

use arrow::array::{ArrayRef, DictionaryArray, PrimitiveArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_expr::EmitTo;
use datafusion_physical_plan::aggregates::group_values::new_group_values;
use datafusion_physical_plan::aggregates::order::GroupOrdering;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const SIZES: [usize; 2] = [8 * 1024, 64 * 1024];
const CARDS_RELATIVE: [usize; 4] = [20, 75, 300, 1000];
const N_BATCHES: usize = 4;
// Fixed for reproducibility.
const SEED: u64 = 0xD1C7;

fn dict_schema() -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(Schema::new(vec![Field::new("g", dict_ty, true)]))
}

/// Build a `Dictionary<Int32, Utf8>` column.
fn make_dict(size: usize, cardinality: usize, null_density: f32, seed: u64) -> ArrayRef {
    let strings: Vec<String> = (0..cardinality).map(|i| format!("v_{i:08}")).collect();
    let values = Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ));

    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<i32> = if cardinality == size {
        let mut perm: Vec<i32> = (0..size as i32).collect();
        perm.shuffle(&mut rng);
        perm
    } else {
        (0..size)
            .map(|_| rng.random_range(0..cardinality) as i32)
            .collect()
    };
    let keys_buf = Buffer::from_slice_ref(&keys);

    let nulls: Option<NullBuffer> = (null_density > 0.0).then(|| {
        (0..size)
            .map(|_| !rng.random_bool(null_density as f64))
            .collect()
    });

    let key_array = PrimitiveArray::<Int32Type>::new(keys_buf.into(), nulls);
    Arc::new(DictionaryArray::<Int32Type>::try_new(key_array, values).unwrap())
}

fn bench_id(
    label: &str,
    size: usize,
    cardinality: usize,
    null_density: f32,
) -> BenchmarkId {
    BenchmarkId::new(
        label,
        format!("size_{size}_card_{cardinality}_null_{null_density:.2}"),
    )
}

fn bench_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_intern_emit");
    let schema = dict_schema();
    let null_density = 0.0;

    for &size in &SIZES {
        let mut cards = CARDS_RELATIVE.to_vec();
        cards.push(size); // all-unique stress case
        for cardinality in cards {
            let array = make_dict(size, cardinality, null_density, SEED);
            group.throughput(Throughput::Elements(size as u64));
            group.bench_function(
                bench_id("intern_emit", size, cardinality, null_density),
                |b| {
                    b.iter_batched_ref(
                        || {
                            (
                                new_group_values(schema.clone(), &GroupOrdering::None)
                                    .unwrap(),
                                Vec::<usize>::with_capacity(size),
                            )
                        },
                        |(gv, groups)| {
                            gv.intern(std::slice::from_ref(&array), groups).unwrap();
                            black_box(&*groups);
                            black_box(gv.emit(EmitTo::All).unwrap());
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_repeated_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_repeated_intern_emit");
    let schema = dict_schema();
    let null_density = 0.10;

    for &size in &SIZES {
        let mut cards = CARDS_RELATIVE.to_vec();
        cards.push(size);
        for cardinality in cards {
            let batches: Vec<ArrayRef> = (0..N_BATCHES)
                .map(|i| {
                    make_dict(
                        size,
                        cardinality,
                        null_density,
                        SEED.wrapping_add(i as u64),
                    )
                })
                .collect();
            group.throughput(Throughput::Elements((size * N_BATCHES) as u64));
            group.bench_function(
                bench_id("repeated_intern_emit", size, cardinality, null_density),
                |b| {
                    b.iter_batched_ref(
                        || {
                            (
                                new_group_values(schema.clone(), &GroupOrdering::None)
                                    .unwrap(),
                                Vec::<usize>::with_capacity(size),
                            )
                        },
                        |(gv, groups)| {
                            for arr in &batches {
                                gv.intern(std::slice::from_ref(arr), groups).unwrap();
                                black_box(&*groups);
                            }
                            black_box(gv.emit(EmitTo::All).unwrap());
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_intern_emit, bench_repeated_intern_emit);
criterion_main!(benches);
