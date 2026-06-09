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

//! Benchmarks for `GroupValues` over multiple `Dictionary<Int32, Utf8>`
//! columns. Each iteration measures repeated `intern` (N_BATCHES times)
//! followed by `emit(EmitTo::All)`. Covers 2, 4, and 8 group-by columns,
//! batch sizes of 8 KiB and 64 KiB rows, fixed cardinalities (20 / 100 / 500
//! / 1 000) and an all-unique case where every row is distinct.

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
const N_COLS: [usize; 2] = [4, 8];
const CARDS: [usize; 3] = [20, 100, 500];
const N_BATCHES: usize = 5;
const NULL_DENSITY: f32 = 0.15;
const SEED: u64 = 0xD1C7;

fn schema_for_cols(n_cols: usize) -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let fields: Vec<Field> = (0..n_cols)
        .map(|i| Field::new(format!("g{i}"), dict_ty.clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Build a single `Dictionary<Int32, Utf8>` column.
/// `card_lo..=card_hi` is sampled per-column so each column in a batch has a
/// different cardinality, matching real-world multi-key GROUP BY data.
fn make_dict_col(size: usize, card_lo: usize, card_hi: usize, null_density: f32, seed: u64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let cardinality = if card_lo == card_hi {
        card_lo
    } else {
        rng.random_range(card_lo..=card_hi)
    };

    let strings: Vec<String> = (0..cardinality).map(|i| format!("dict_label_{i:012}")).collect();
    let values = Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ));

    // When the pool is at least as large as the batch, shuffle a prefix so
    // every row in this batch maps to a distinct key.
    let keys: Vec<i32> = if cardinality >= size {
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

/// Build one batch of `n_cols` dictionary columns.
/// Each column independently samples its cardinality from `card_lo..=card_hi`.
fn make_batch(
    n_cols: usize,
    size: usize,
    card_lo: usize,
    card_hi: usize,
    null_density: f32,
    seed: u64,
) -> Vec<ArrayRef> {
    (0..n_cols)
        .map(|col| {
            make_dict_col(
                size,
                card_lo,
                card_hi,
                null_density,
                seed.wrapping_add(col as u64 * 0x9E37),
            )
        })
        .collect()
}

fn bench_id(label: &str, n_cols: usize, size: usize, cardinality: usize) -> BenchmarkId {
    let card_label = if cardinality == size {
        "all_unique".to_string()
    } else {
        format!("{cardinality}")
    };
    BenchmarkId::new(
        format!("{label}/cols_{n_cols}"),
        format!("size_{size}_card_{card_label}_null_{NULL_DENSITY:.2}"),
    )
}

fn bench_multi_col_repeated_intern_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_col_dict_repeated_intern_emit");

    for &n_cols in &N_COLS {
        let schema = schema_for_cols(n_cols);

        for &size in &SIZES {
            let mut cardinalities = CARDS.to_vec();
            cardinalities.push(size);

            for cardinality in cardinalities {
                // For the all-unique case pin both bounds to size; otherwise
                // spread each column's cardinality across [card/2, card*2].
                let (card_lo, card_hi) = if cardinality == size {
                    (size, size)
                } else {
                    ((cardinality / 2).max(1), cardinality * 2)
                };

                // Pre-build all batches outside the timing loop.
                let batches: Vec<Vec<ArrayRef>> = (0..N_BATCHES)
                    .map(|i| {
                        make_batch(
                            n_cols,
                            size,
                            card_lo,
                            card_hi,
                            NULL_DENSITY,
                            SEED.wrapping_add(i as u64 * 0x1F3D),
                        )
                    })
                    .collect();

                group.throughput(Throughput::Elements((size * N_BATCHES) as u64));

                group.bench_function(bench_id("repeated", n_cols, size, cardinality), |b| {
                    b.iter_batched_ref(
                        || {
                            (
                                new_group_values(schema.clone(), &GroupOrdering::None)
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
                });

                // Partial-emit variant: after each intern spill the first half
                // of accumulated groups, then flush the remainder with All.
                group.bench_function(bench_id("partial_emit", n_cols, size, cardinality), |b| {
                    b.iter_batched_ref(
                        || {
                            (
                                new_group_values(schema.clone(), &GroupOrdering::None)
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
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_multi_col_repeated_intern_emit);
criterion_main!(benches);
