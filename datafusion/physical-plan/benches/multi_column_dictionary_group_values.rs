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

//! Benchmarks for `GroupValues` over multiple `Dictionary<UInt64, Utf8>` columns.
//! Covers 4 and 8 group-by columns, batch sizes of 8 KiB and 64 KiB rows,
//! and cardinalities realistic for multi-column GROUP BY workloads (20 / 100 / 500 / 1 000).

use arrow::array::{Array, ArrayRef, DictionaryArray, PrimitiveArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
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
const N_COLS: [usize; 2] = [4, 8];
const CARDS: [usize; 4] = [20, 100, 500, 1_000];
const N_BATCHES: usize = 5;
const NULL_DENSITY: f32 = 0.15;
const SEED: u64 = 0xD1C7;

fn schema_for_cols(n_cols: usize) -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8));
    let fields: Vec<Field> = (0..n_cols)
        .map(|i| Field::new(format!("g{i}"), dict_ty.clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn count_distinct_tuples(cols: &[ArrayRef]) -> usize {
    use std::collections::HashSet;
    let n = cols[0].len();
    let mut seen: HashSet<Vec<Option<u64>>> = HashSet::new();
    for row in 0..n {
        let key: Vec<Option<u64>> = cols
            .iter()
            .map(|c| {
                let dict = c
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt64Type>>()
                    .unwrap();
                if dict.is_null(row) {
                    None
                } else {
                    Some(dict.keys().value(row))
                }
            })
            .collect();
        seen.insert(key);
    }
    seen.len()
}

fn make_dict_col(
    size: usize,
    group_ids: &[usize],
    col_idx: usize,
    per_col_card: usize,
    null_density: f32,
    seed: u64,
) -> ArrayRef {
    let strings: Vec<String> = (0..per_col_card)
        .map(|i| format!("dict_label_{i:012}"))
        .collect();
    let values = Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ));

    let divisor = per_col_card.pow(col_idx as u32);
    let keys: Vec<u64> = group_ids
        .iter()
        .map(|&g| ((g / divisor) % per_col_card) as u64)
        .collect();
    let keys_buf = Buffer::from_slice_ref(&keys);

    let nulls: Option<NullBuffer> = (null_density > 0.0).then(|| {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..size)
            .map(|_| !rng.random_bool(null_density as f64))
            .collect()
    });

    let key_array = PrimitiveArray::<UInt64Type>::new(keys_buf.into(), nulls);
    Arc::new(DictionaryArray::<UInt64Type>::try_new(key_array, values).unwrap())
        as ArrayRef
}

/// Each row is assigned a `group_id` (0..`target_distinct`). Column keys are
/// derived from `group_id` via mixed-radix decomposition (treating `group_id`
/// as a base-k number and reading off one digit per column), so rows with the
/// same `group_id` always produce the same tuple. This keeps distinct groups at
/// exactly `target_distinct` regardless of column count.
fn make_batch(
    n_cols: usize,
    size: usize,
    target_distinct: usize,
    null_density: f32,
    seed: u64,
) -> Vec<ArrayRef> {
    let mut rng = StdRng::seed_from_u64(seed);

    // When nulls are present all null rows coalesce into one extra group
    // (None, None, …), so we generate one fewer non-null group to keep the
    // total at exactly target_distinct.
    let n_groups = if null_density > 0.0 {
        target_distinct.saturating_sub(1).max(1)
    } else {
        target_distinct
    };

    let mut per_col_card = (n_groups as f64).powf(1.0 / n_cols as f64).ceil() as usize;
    per_col_card = per_col_card.max(1);
    while per_col_card.saturating_pow(n_cols as u32) < n_groups {
        per_col_card += 1;
    }

    let n_extra = size.saturating_sub(n_groups);
    let mut group_ids: Vec<usize> = (0..n_groups.min(size)).collect();
    group_ids.extend((0..n_extra).map(|_| rng.random_range(0..n_groups)));
    group_ids.shuffle(&mut rng);

    let cols: Vec<ArrayRef> = (0..n_cols)
        .map(|col| make_dict_col(size, &group_ids, col, per_col_card, null_density, seed))
        .collect();

    // run `BENCH_VALIDATE=1 cargo bench --bench multi_column_dictionary_group_values -- --list`  to validate that the generated batches have the expected number of distinct groups
    if std::env::var("BENCH_VALIDATE").is_ok() {
        let actual = count_distinct_tuples(&cols);
        eprintln!(
            "validate: cols={n_cols} size={size} target={target_distinct} actual={actual}"
        );
    }

    cols
}

/// Each column independently samples from its own `target_distinct` value pool
/// (like GROUP BY department, name, age), so actual distinct groups grow with
/// the cross-product of column cardinalities.
fn make_batch_independent(
    n_cols: usize,
    size: usize,
    target_distinct: usize,
    null_density: f32,
    seed: u64,
) -> Vec<ArrayRef> {
    let cols: Vec<ArrayRef> = (0..n_cols)
        .map(|col| {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(col as u64 * 0x9E37));
            let group_ids: Vec<usize> = (0..size)
                .map(|_| rng.random_range(0..target_distinct))
                .collect();
            // col_idx=0, per_col_card=target_distinct → key == group_id directly
            make_dict_col(size, &group_ids, 0, target_distinct, null_density, seed)
        })
        .collect();

    if std::env::var("BENCH_VALIDATE").is_ok() {
        let actual = count_distinct_tuples(&cols);
        eprintln!(
            "validate_independent: cols={n_cols} size={size} per_col_card={target_distinct} actual={actual}"
        );
    }

    cols
}

fn bench_id(
    label: &str,
    n_cols: usize,
    size: usize,
    target_distinct: usize,
) -> BenchmarkId {
    BenchmarkId::new(
        format!("{label}/cols_{n_cols}"),
        format!("size_{size}_card_{target_distinct}"),
    )
}

fn bench_multi_col_repeated_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_column_dictionary_group_values");

    for &n_cols in &N_COLS {
        let schema = schema_for_cols(n_cols);

        for &size in &SIZES {
            for &target_distinct in &CARDS {
                let batches: Vec<Vec<ArrayRef>> = (0..N_BATCHES)
                    .map(|i| {
                        make_batch(
                            n_cols,
                            size,
                            target_distinct,
                            NULL_DENSITY,
                            SEED.wrapping_add(i as u64 * 0x1F3D),
                        )
                    })
                    .collect();

                group.throughput(Throughput::Elements((size * N_BATCHES) as u64));

                group.bench_function(
                    bench_id("repeated", n_cols, size, target_distinct),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(
                                        schema.clone(),
                                        &GroupOrdering::None,
                                    )
                                    .unwrap(),
                                    Vec::<usize>::with_capacity(size),
                                )
                            },
                            |(gv, groups)| {
                                for batch in &batches {
                                    gv.intern(batch.as_slice(), groups).unwrap();
                                    black_box(&*groups);
                                }
                                black_box(gv.emit(EmitTo::All).unwrap());
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );

                group.bench_function(
                    bench_id("partial_emit", n_cols, size, target_distinct),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(
                                        schema.clone(),
                                        &GroupOrdering::None,
                                    )
                                    .unwrap(),
                                    Vec::<usize>::with_capacity(size),
                                )
                            },
                            |(gv, groups)| {
                                for batch in &batches {
                                    gv.intern(batch.as_slice(), groups).unwrap();
                                    black_box(&*groups);
                                    let half = gv.len() / 2;
                                    if half > 0 {
                                        black_box(gv.emit(EmitTo::First(half)).unwrap());
                                    }
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

fn bench_multi_col_independent_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_column_dictionary_independent");

    const INDEPENDENT_SIZE: usize = 8 * 1024;
    const INDEPENDENT_CARDS: [usize; 3] = [20, 100, 500];

    for &n_cols in &N_COLS {
        let schema = schema_for_cols(n_cols);

        for &target_distinct in &INDEPENDENT_CARDS {
            let size = INDEPENDENT_SIZE;
            {
                let batches: Vec<Vec<ArrayRef>> = (0..N_BATCHES)
                    .map(|i| {
                        make_batch_independent(
                            n_cols,
                            size,
                            target_distinct,
                            NULL_DENSITY,
                            SEED.wrapping_add(i as u64 * 0x1F3D),
                        )
                    })
                    .collect();

                group.throughput(Throughput::Elements((size * N_BATCHES) as u64));

                group.bench_function(
                    bench_id("repeated", n_cols, size, target_distinct),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(
                                        schema.clone(),
                                        &GroupOrdering::None,
                                    )
                                    .unwrap(),
                                    Vec::<usize>::with_capacity(size),
                                )
                            },
                            |(gv, groups)| {
                                for batch in &batches {
                                    gv.intern(batch.as_slice(), groups).unwrap();
                                    black_box(&*groups);
                                }
                                black_box(gv.emit(EmitTo::All).unwrap());
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );

                group.bench_function(
                    bench_id("partial_emit", n_cols, size, target_distinct),
                    |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    new_group_values(
                                        schema.clone(),
                                        &GroupOrdering::None,
                                    )
                                    .unwrap(),
                                    Vec::<usize>::with_capacity(size),
                                )
                            },
                            |(gv, groups)| {
                                for batch in &batches {
                                    gv.intern(batch.as_slice(), groups).unwrap();
                                    black_box(&*groups);
                                    let half = gv.len() / 2;
                                    if half > 0 {
                                        black_box(gv.emit(EmitTo::First(half)).unwrap());
                                    }
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

criterion_group!(
    benches,
    bench_multi_col_repeated_intern_emit,
    bench_multi_col_independent_columns
);
criterion_main!(benches);
