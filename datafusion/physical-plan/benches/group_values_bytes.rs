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

//! Benchmarks for single-column `GroupValues` backed by bytes/boolean types:
//! `GroupValuesBytes<i32>` (Utf8/Binary), `GroupValuesBytes<i64>`
//! (LargeUtf8/LargeBinary), `GroupValuesBytesView` (Utf8View/BinaryView), and
//! `GroupValuesBoolean`.
//!
//! Each benchmark covers two patterns:
//!   - `intern_emit`: one `intern` call followed by `emit(EmitTo::All)`.
//!   - `repeated_intern_emit`: N `intern` calls followed by `emit(EmitTo::All)`.

use arrow::array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_expr::EmitTo;
use datafusion_physical_plan::aggregates::group_values::new_group_values;
use datafusion_physical_plan::aggregates::order::GroupOrdering;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const SIZES: [usize; 1] = [8 * 1024];
const CARDINALITIES: [usize; 3] = [20, 300, 1000];
const N_BATCHES: usize = 4;
const SEED: u64 = 0xB175;

fn schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("g", data_type, true)]))
}

fn make_strings(size: usize, cardinality: usize, seed: u64) -> Vec<String> {
    let values: Vec<String> = (0..cardinality).map(|i| format!("val_{i:08}")).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    (0..size)
        .map(|_| values[rng.random_range(0..cardinality)].clone())
        .collect()
}

fn make_utf8(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn make_large_utf8(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(LargeStringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn make_utf8view(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(StringViewArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn make_binary(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(BinaryArray::from(
        strings.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
    ))
}

fn make_large_binary(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(LargeBinaryArray::from(
        strings.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
    ))
}

fn make_binary_view(size: usize, cardinality: usize, seed: u64) -> ArrayRef {
    let strings = make_strings(size, cardinality, seed);
    Arc::new(BinaryViewArray::from(
        strings.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
    ))
}

fn make_boolean(size: usize, seed: u64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    Arc::new(BooleanArray::from(
        (0..size).map(|_| rng.random_bool(0.5)).collect::<Vec<_>>(),
    ))
}

struct Case {
    name: &'static str,
    data_type: DataType,
}

fn byte_cases() -> Vec<Case> {
    vec![
        Case {
            name: "utf8",
            data_type: DataType::Utf8,
        },
        Case {
            name: "large_utf8",
            data_type: DataType::LargeUtf8,
        },
        Case {
            name: "utf8view",
            data_type: DataType::Utf8View,
        },
        Case {
            name: "binary",
            data_type: DataType::Binary,
        },
        Case {
            name: "large_binary",
            data_type: DataType::LargeBinary,
        },
        Case {
            name: "binary_view",
            data_type: DataType::BinaryView,
        },
    ]
}

fn make_array(
    data_type: &DataType,
    size: usize,
    cardinality: usize,
    seed: u64,
) -> ArrayRef {
    match data_type {
        DataType::Utf8 => make_utf8(size, cardinality, seed),
        DataType::LargeUtf8 => make_large_utf8(size, cardinality, seed),
        DataType::Utf8View => make_utf8view(size, cardinality, seed),
        DataType::Binary => make_binary(size, cardinality, seed),
        DataType::LargeBinary => make_large_binary(size, cardinality, seed),
        DataType::BinaryView => make_binary_view(size, cardinality, seed),
        _ => unreachable!(),
    }
}

fn bench_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_values_bytes_intern_emit");

    for case in byte_cases() {
        let s = schema(case.data_type.clone());
        for &size in &SIZES {
            for &card in &CARDINALITIES {
                let array = make_array(&case.data_type, size, card, SEED);
                group.throughput(Throughput::Elements(size as u64));
                group.bench_function(
                    BenchmarkId::new(case.name, format!("size_{size}_card_{card}")),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(s.clone(), &GroupOrdering::None)
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
    }

    group.finish();
}

fn bench_repeated_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_values_bytes_repeated_intern_emit");

    for case in byte_cases() {
        let s = schema(case.data_type.clone());
        for &size in &SIZES {
            for &card in &CARDINALITIES {
                let batches: Vec<ArrayRef> = (0..N_BATCHES)
                    .map(|i| {
                        make_array(
                            &case.data_type,
                            size,
                            card,
                            SEED.wrapping_add(i as u64),
                        )
                    })
                    .collect();
                group.throughput(Throughput::Elements((size * N_BATCHES) as u64));
                group.bench_function(
                    BenchmarkId::new(case.name, format!("size_{size}_card_{card}")),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(s.clone(), &GroupOrdering::None)
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
    }

    group.finish();
}

fn bench_boolean_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_values_boolean_intern_emit");
    let s = schema(DataType::Boolean);

    for &size in &SIZES {
        let array = make_boolean(size, SEED);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(BenchmarkId::new("boolean", format!("size_{size}")), |b| {
            b.iter_batched_ref(
                || {
                    (
                        new_group_values(s.clone(), &GroupOrdering::None).unwrap(),
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
        });
    }

    group.finish();
}

fn bench_boolean_repeated_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_values_boolean_repeated_intern_emit");
    let s = schema(DataType::Boolean);

    for &size in &SIZES {
        let batches: Vec<ArrayRef> = (0..N_BATCHES)
            .map(|i| make_boolean(size, SEED.wrapping_add(i as u64)))
            .collect();
        group.throughput(Throughput::Elements((size * N_BATCHES) as u64));
        group.bench_function(BenchmarkId::new("boolean", format!("size_{size}")), |b| {
            b.iter_batched_ref(
                || {
                    (
                        new_group_values(s.clone(), &GroupOrdering::None).unwrap(),
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
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_intern_emit,
    bench_repeated_intern_emit,
    bench_boolean_intern_emit,
    bench_boolean_repeated_intern_emit,
);
criterion_main!(benches);
