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

//! Benchmarks for `GroupValues` over multiple `Dictionary<Int32, Utf8>` columns.
//! Covers 4 and 8 group-by columns, batch sizes of 8 KiB and 64 KiB rows,
//! and cardinalities realistic for multi-column GROUP BY workloads (20 / 100 / 500 / 1 000).

use arrow::array::{ArrayRef, DictionaryArray, PrimitiveArray, StringArray};
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
/// Per-column cardinality variance as a percentage of the target (half above, half below).
/// each column's distinct count is sampled from [target*0.95, target*1.05].
const CARDINALITY_RANGE: usize = 10;

fn schema_for_cols(n_cols: usize) -> SchemaRef {
    let dict_ty =
        DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8));
    let fields: Vec<Field> = (0..n_cols)
        .map(|i| Field::new(format!("g{i}"), dict_ty.clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn make_dict_col(
    size: usize,
    num_distinct: usize,
    null_density: f32,
    seed: u64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);

    let strings: Vec<String> = (0..num_distinct)
        .map(|i| format!("dict_label_{i:012}"))
        .collect();
    let values = Arc::new(StringArray::from(
        strings.iter().map(String::as_str).collect::<Vec<_>>(),
    ));

    // When the pool is at least as large as the batch, shuffle a prefix so
    // every row in this batch maps to a distinct key.
    let keys: Vec<u64> = if num_distinct >= size {
        let mut perm: Vec<u64> = (0..size as u64).collect();
        perm.shuffle(&mut rng);
        perm
    } else {
        (0..size)
            .map(|_| rng.random_range(0..num_distinct) as u64)
            .collect()
    };
    let keys_buf = Buffer::from_slice_ref(&keys);

    let nulls: Option<NullBuffer> = (null_density > 0.0).then(|| {
        (0..size)
            .map(|_| !rng.random_bool(null_density as f64))
            .collect()
    });

    let key_array = PrimitiveArray::<UInt64Type>::new(keys_buf.into(), nulls);
    Arc::new(DictionaryArray::<UInt64Type>::try_new(key_array, values).unwrap())
}

fn make_batch(
    n_cols: usize,
    size: usize,
    target_distinct: usize,
    null_density: f32,
    seed: u64,
) -> Vec<ArrayRef> {
    let half = CARDINALITY_RANGE / 2;
    let lo = (target_distinct * (100 - half) / 100).max(1);
    let hi = (target_distinct * (100 + half) / 100).max(lo);
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n_cols)
        .map(|col| {
            let num_distinct = rng.random_range(lo..=hi);
            make_dict_col(
                size,
                num_distinct,
                null_density,
                seed.wrapping_add(col as u64 * 0x9E37),
            )
        })
        .collect()
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
                // Pre-build all batches outside the timing loop.
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

                // Partial-emit variant: after each intern spill the first half
                // of accumulated groups, then flush the remainder with All.
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

criterion_group!(benches, bench_multi_col_repeated_intern_emit);
criterion_main!(benches);
