// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for `GroupValues` over `Dictionary<Int32, Utf8>` grouping
//! columns, optionally paired with an `Int32` column. Each iteration measures
//! `intern` (once or N times) followed by `emit(EmitTo::All)`. The
//! `Box<dyn GroupValues>` returned by
//! `new_group_values` is constructed in the setup closure of
//! `iter_batched_ref` and is not included in the timing.

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, PrimitiveArray, StringArray,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_expr::EmitTo;
use datafusion_physical_plan::aggregates::group_values::new_group_values;
use datafusion_physical_plan::aggregates::order::{GroupClustering, GroupClusteringFull};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const SIZES: [usize; 2] = [8 * 1024, 64 * 1024];
const CARDS_RELATIVE: [usize; 4] = [20, 75, 300, 1000];
const N_BATCHES: usize = 4;
// Values-array identity is checked once per batch, so use a longer stream than
// the general repeated-intern benchmarks above.
const VALUES_ARRAY_BATCHES: usize = 32;
// Fixed for reproducibility.
const SEED: u64 = 0xD1C7;

fn dict_schema() -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(Schema::new(vec![Field::new("g", dict_ty, true)]))
}

fn dict_int_schema() -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(Schema::new(vec![
        Field::new("g", dict_ty, false),
        Field::new("i", DataType::Int32, false),
    ]))
}

fn make_values(cardinality: usize) -> ArrayRef {
    let strings: Vec<String> = (0..cardinality).map(|i| format!("v_{i:08}")).collect();
    Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn make_dict_with_values(size: usize, values: ArrayRef, seed: u64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<i32> = (0..size)
        .map(|_| rng.random_range(0..values.len()) as i32)
        .collect();
    Arc::new(DictionaryArray::<Int32Type>::new(
        PrimitiveArray::<Int32Type>::from(keys),
        values,
    ))
}

fn make_int(size: usize) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(
        (0..size).map(|i| (i % 32) as i32),
    ))
}

fn bench_batches(
    c: &mut Criterion,
    group_name: &str,
    case: BenchmarkId,
    schema: &SchemaRef,
    batches: &[Vec<ArrayRef>],
) {
    let rows: usize = batches.iter().map(|batch| batch[0].len()).sum();
    let batch_size = batches[0][0].len();
    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function(case, |b| {
        b.iter_batched_ref(
            || {
                (
                    new_group_values(Arc::clone(schema), &GroupClustering::None).unwrap(),
                    Vec::<usize>::with_capacity(batch_size),
                )
            },
            |(group_values, groups)| {
                for batch in batches {
                    group_values.intern(batch, groups).unwrap();
                    black_box(&*groups);
                }
                black_box(group_values.emit(EmitTo::All).unwrap());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
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
                                new_group_values(schema.clone(), &GroupClustering::None)
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
                                new_group_values(schema.clone(), &GroupClustering::None)
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

// GroupClustering::Full -> GroupValuesColumn::<true>: scalar append_val/equal_to path.
fn bench_scalar_append_equal(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_scalar_append_equal");
    let schema = dict_schema();
    let null_density = 0.1;
    let size = SIZES[1];

    let mut cards = CARDS_RELATIVE.to_vec();
    cards.push(size);
    for cardinality in cards {
        let array = make_dict(size, cardinality, null_density, SEED);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(
            bench_id("scalar_append_equal", size, cardinality, null_density),
            |b| {
                b.iter_batched_ref(
                    || {
                        (
                            new_group_values(
                                schema.clone(),
                                &GroupClustering::Full(GroupClusteringFull::new()),
                            )
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
    group.finish();
}

//EmitTo::First exercises repeated
fn bench_take_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_take_n");
    let schema = dict_schema();
    let null_density = 0.10;
    let size = SIZES[1];

    let mut cards = CARDS_RELATIVE.to_vec();
    cards.push(size);
    for cardinality in cards {
        let batch = make_dict(size, cardinality, null_density, SEED);
        group.throughput(Throughput::Elements((size * N_BATCHES) as u64));
        group.bench_function(bench_id("take_n", size, cardinality, null_density), |b| {
            b.iter_batched_ref(
                || {
                    (
                        new_group_values(schema.clone(), &GroupClustering::None).unwrap(),
                        Vec::<usize>::with_capacity(size),
                    )
                },
                |(gv, groups)| {
                    for _ in 0..N_BATCHES {
                        gv.intern(std::slice::from_ref(&batch), groups).unwrap();
                        black_box(&*groups);
                        let emit_n = (gv.len() / 2).min(gv.len());
                        black_box(gv.emit(EmitTo::First(emit_n)).unwrap());
                    }
                    black_box(gv.emit(EmitTo::First(gv.len())).unwrap());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Batches that share one dictionary values array, as produced downstream of
/// a repartition or filter: `take`/`filter` clone the values `Arc` and rewrite
/// only the keys, so the values array is never compacted and its cardinality
/// reflects the whole upstream stream rather than a single batch.
///
/// The other benchmarks here always allocate a fresh values array per batch
/// and cap cardinality at the batch size, so neither the `Arc` reuse nor the
/// `cardinality >> rows` regime is covered by them.
fn bench_shared_values_arc(c: &mut Criterion) {
    // More batches than `N_BATCHES`: the cost this exercises is paid once per
    // batch, so a longer run per values array is what a real partition looks
    // like (thousands of batches sharing one dictionary).
    let mut group = c.benchmark_group("dict_shared_values_arc");
    let size = SIZES[0];

    for &cardinality in &[size, 32 * 1024, 64 * 1024, 100_000, 500_000] {
        // Seeded per cardinality so each configuration is reproducible on its
        // own and adding one does not change the data used by the others.
        let mut rng = StdRng::seed_from_u64(SEED);
        // One values array, shared by every batch.
        let strings: Vec<String> =
            (0..cardinality).map(|i| format!("v_{i:08}")).collect();
        let values: ArrayRef = Arc::new(StringArray::from(
            strings.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>(),
        ));

        let batches: Vec<ArrayRef> = (0..VALUES_ARRAY_BATCHES)
            .map(|_| {
                let keys: Vec<i32> = (0..size)
                    .map(|_| rng.random_range(0..cardinality) as i32)
                    .collect();
                Arc::new(DictionaryArray::<Int32Type>::new(
                    PrimitiveArray::<Int32Type>::from(keys),
                    Arc::clone(&values),
                )) as ArrayRef
            })
            .collect();

        let schema = dict_schema();
        group.throughput(Throughput::Elements((size * VALUES_ARRAY_BATCHES) as u64));
        group.bench_function(
            BenchmarkId::new(
                "shared_values_arc",
                format!("size_{size}_card_{cardinality}"),
            ),
            |b| {
                b.iter_batched_ref(
                    || {
                        (
                            new_group_values(schema.clone(), &GroupClustering::None)
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
    group.finish();
}

/// Alternate between two distinct values allocations with identical contents.
/// This changes dictionary identity without changing the logical group values.
fn bench_alternating_values_single(c: &mut Criterion) {
    let size = SIZES[0];
    let cardinality = 100_000;
    let values = [make_values(cardinality), make_values(cardinality)];
    let batches = (0..VALUES_ARRAY_BATCHES)
        .map(|i| {
            vec![make_dict_with_values(
                size,
                Arc::clone(&values[i % values.len()]),
                SEED.wrapping_add(i as u64),
            )]
        })
        .collect::<Vec<_>>();
    bench_batches(
        c,
        "dict_alternating_values_single",
        BenchmarkId::from_parameter("card_100000"),
        &dict_schema(),
        &batches,
    );
}

/// Pair the alternating dictionaries with an `Int32` key to cover hash folding
/// after the dictionary values allocation changes.
fn bench_alternating_values_multi(c: &mut Criterion) {
    let size = SIZES[0];
    let cardinality = 100_000;
    let values = [make_values(cardinality), make_values(cardinality)];
    let integers = make_int(size);
    let batches = (0..VALUES_ARRAY_BATCHES)
        .map(|i| {
            vec![
                make_dict_with_values(
                    size,
                    Arc::clone(&values[i % values.len()]),
                    SEED.wrapping_add(i as u64),
                ),
                Arc::clone(&integers),
            ]
        })
        .collect::<Vec<_>>();
    bench_batches(
        c,
        "dict_alternating_values_multi",
        BenchmarkId::from_parameter("card_100000"),
        &dict_int_schema(),
        &batches,
    );
}

/// Reuse one values allocation larger than a batch while an `Int32` key makes
/// the grouping multi-column.
fn bench_shared_values_multi(c: &mut Criterion) {
    let size = SIZES[0];
    for cardinality in [32 * 1024, 100_000] {
        let values = make_values(cardinality);
        let integers = make_int(size);
        let batches = (0..VALUES_ARRAY_BATCHES)
            .map(|i| {
                vec![
                    make_dict_with_values(
                        size,
                        Arc::clone(&values),
                        SEED.wrapping_add(i as u64),
                    ),
                    Arc::clone(&integers),
                ]
            })
            .collect::<Vec<_>>();
        bench_batches(
            c,
            "dict_shared_values_multi",
            BenchmarkId::from_parameter(format!("card_{cardinality}")),
            &dict_int_schema(),
            &batches,
        );
    }
}

/// Allocate the same 20 logical values for every batch without reusing their
/// values array, as can happen after a cast in a projection.
fn bench_fresh_values_per_batch(c: &mut Criterion) {
    let size = SIZES[0];
    let batches = (0..VALUES_ARRAY_BATCHES)
        .map(|i| vec![make_dict(size, 20, 0.0, SEED.wrapping_add(i as u64))])
        .collect::<Vec<_>>();
    bench_batches(
        c,
        "dict_fresh_values_per_batch",
        BenchmarkId::from_parameter("card_20"),
        &dict_schema(),
        &batches,
    );
}

criterion_group!(
    benches,
    bench_intern_emit,
    bench_repeated_intern_emit,
    bench_scalar_append_equal,
    bench_take_n,
    bench_shared_values_arc,
    bench_alternating_values_single,
    bench_alternating_values_multi,
    bench_shared_values_multi,
    bench_fresh_values_per_batch
);
criterion_main!(benches);
