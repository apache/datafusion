// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Int8Array,
    Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl, EmitTo};
use datafusion_functions_aggregate::count::Count;
use datafusion_physical_expr::expressions::col;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;

fn prepare_accumulator(data_type: DataType) -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new("f", data_type, true)]));
    let expr = col("f", &schema).unwrap();
    let accumulator_args = AccumulatorArgs {
        return_field: Field::new("f", DataType::Int64, true).into(),
        schema: &schema,
        expr_fields: &[expr.return_field(&schema).unwrap()],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "count(distinct f)",
        is_distinct: true,
        exprs: &[expr],
    };
    Count::new().accumulator(accumulator_args).unwrap()
}

fn create_i64_array(n_distinct: usize) -> Int64Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i64)))
        .collect()
}

fn create_u8_array(n_distinct: usize) -> UInt8Array {
    let mut rng = StdRng::seed_from_u64(42);
    let max_val = n_distinct.min(256) as u8;
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..max_val)))
        .collect()
}

fn create_i8_array(n_distinct: usize) -> Int8Array {
    let mut rng = StdRng::seed_from_u64(42);
    let max_val = (n_distinct.min(256) / 2) as i8;
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(-max_val..max_val)))
        .collect()
}

fn create_u16_array(n_distinct: usize) -> UInt16Array {
    let mut rng = StdRng::seed_from_u64(42);
    let max_val = n_distinct.min(65536) as u16;
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..max_val)))
        .collect()
}

fn create_i16_array(n_distinct: usize) -> Int16Array {
    let mut rng = StdRng::seed_from_u64(42);
    let max_val = (n_distinct.min(65536) / 2) as i16;
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(-max_val..max_val)))
        .collect()
}

fn create_u32_array(n_distinct: usize) -> UInt32Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as u32)))
        .collect()
}

fn create_i32_array(n_distinct: usize) -> Int32Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i32)))
        .collect()
}

fn create_decimal32_array(n_distinct: usize) -> Decimal32Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i32)))
        .collect::<Decimal32Array>()
        .with_precision_and_scale(9, 2)
        .unwrap()
}

fn create_decimal64_array(n_distinct: usize) -> Decimal64Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i64)))
        .collect::<Decimal64Array>()
        .with_precision_and_scale(18, 2)
        .unwrap()
}

fn create_decimal128_array(n_distinct: usize) -> Decimal128Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i128)))
        .collect::<Decimal128Array>()
        .with_precision_and_scale(38, 2)
        .unwrap()
}

fn prepare_args(data_type: DataType) -> (Arc<Schema>, AccumulatorArgs<'static>) {
    let schema = Arc::new(Schema::new(vec![Field::new("f", data_type, true)]));
    let schema_leaked: &'static Schema = Box::leak(Box::new((*schema).clone()));
    let expr = col("f", schema_leaked).unwrap();
    let expr_leaked: &'static _ = Box::leak(Box::new(expr));
    let return_field: Arc<Field> = Field::new("f", DataType::Int64, true).into();
    let return_field_leaked: &'static _ = Box::leak(Box::new(return_field.clone()));
    let expr_field = expr_leaked.return_field(schema_leaked).unwrap();
    let expr_field_leaked: &'static _ = Box::leak(Box::new(expr_field));

    let accumulator_args = AccumulatorArgs {
        return_field: return_field_leaked.clone(),
        schema: schema_leaked,
        expr_fields: std::slice::from_ref(expr_field_leaked),
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "count(distinct f)",
        is_distinct: true,
        exprs: std::slice::from_ref(expr_leaked),
    };
    (schema, accumulator_args)
}

fn count_distinct_benchmark(c: &mut Criterion) {
    for pct in [80, 99] {
        let n_distinct = BATCH_SIZE * pct / 100;

        // Int64
        let values = Arc::new(create_i64_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct i64 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Int64);
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });
    }

    // Small integer types

    // UInt8
    let values = Arc::new(create_u8_array(200)) as ArrayRef;
    c.bench_function("count_distinct u8 bitmap", |b| {
        b.iter(|| {
            let mut accumulator = prepare_accumulator(DataType::UInt8);
            accumulator
                .update_batch(std::slice::from_ref(&values))
                .unwrap()
        })
    });

    // Int8
    let values = Arc::new(create_i8_array(200)) as ArrayRef;
    c.bench_function("count_distinct i8 bitmap", |b| {
        b.iter(|| {
            let mut accumulator = prepare_accumulator(DataType::Int8);
            accumulator
                .update_batch(std::slice::from_ref(&values))
                .unwrap()
        })
    });

    // UInt16
    let values = Arc::new(create_u16_array(50000)) as ArrayRef;
    c.bench_function("count_distinct u16 bitmap", |b| {
        b.iter(|| {
            let mut accumulator = prepare_accumulator(DataType::UInt16);
            accumulator
                .update_batch(std::slice::from_ref(&values))
                .unwrap()
        })
    });

    // Int16
    let values = Arc::new(create_i16_array(50000)) as ArrayRef;
    c.bench_function("count_distinct i16 bitmap", |b| {
        b.iter(|| {
            let mut accumulator = prepare_accumulator(DataType::Int16);
            accumulator
                .update_batch(std::slice::from_ref(&values))
                .unwrap()
        })
    });

    // 32-bit integer types
    for pct in [80, 99] {
        let n_distinct = BATCH_SIZE * pct / 100;

        // UInt32
        let values = Arc::new(create_u32_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct u32 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::UInt32);
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });

        // Int32
        let values = Arc::new(create_i32_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct i32 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Int32);
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });
    }

    // Decimal types. Decimal128 already uses the native distinct accumulator
    // and serves as a reference for Decimal32 and Decimal64.
    for pct in [80, 99] {
        let n_distinct = BATCH_SIZE * pct / 100;

        // Decimal32
        let values = Arc::new(create_decimal32_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct decimal32 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Decimal32(9, 2));
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });

        // Decimal64
        let values = Arc::new(create_decimal64_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct decimal64 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Decimal64(18, 2));
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });

        // Decimal128
        let values = Arc::new(create_decimal128_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("count_distinct decimal128 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Decimal128(38, 2));
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });
    }
}

/// Create group indices with uniform distribution
fn create_uniform_groups(num_groups: usize) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| rng.random_range(0..num_groups))
        .collect()
}

/// Create group indices with skewed distribution (80% in 20% of groups)
fn create_skewed_groups(num_groups: usize) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(42);
    let hot_groups = (num_groups / 5).max(1);
    (0..BATCH_SIZE)
        .map(|_| {
            if rng.random_range(0..100) < 80 {
                rng.random_range(0..hot_groups)
            } else {
                rng.random_range(0..num_groups)
            }
        })
        .collect()
}

fn count_distinct_groups_benchmark(c: &mut Criterion) {
    let count_fn = Count::new();

    let group_counts = [100, 1000, 10000];
    let cardinalities = [("low", 20), ("mid", 80), ("high", 99)];
    let distributions = ["uniform", "skewed"];

    // i64 benchmarks
    for num_groups in group_counts {
        for (card_name, distinct_pct) in cardinalities {
            for dist in distributions {
                let name = format!("i64_g{num_groups}_{card_name}_{dist}");
                let n_distinct = BATCH_SIZE * distinct_pct / 100;
                let values = Arc::new(create_i64_array(n_distinct)) as ArrayRef;
                let group_indices = if dist == "uniform" {
                    create_uniform_groups(num_groups)
                } else {
                    create_skewed_groups(num_groups)
                };

                let (_schema, args) = prepare_args(DataType::Int64);

                if count_fn.groups_accumulator_supported(args.clone()) {
                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut acc =
                                count_fn.create_groups_accumulator(args.clone()).unwrap();
                            acc.update_batch(
                                std::slice::from_ref(&values),
                                &group_indices,
                                None,
                                num_groups,
                            )
                            .unwrap();
                            acc.evaluate(EmitTo::All).unwrap()
                        })
                    });
                } else {
                    let arr = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    let mut group_rows: Vec<Vec<i64>> = vec![Vec::new(); num_groups];
                    for (idx, &group_idx) in group_indices.iter().enumerate() {
                        if arr.is_valid(idx) {
                            group_rows[group_idx].push(arr.value(idx));
                        }
                    }
                    let group_arrays: Vec<ArrayRef> = group_rows
                        .iter()
                        .map(|rows| Arc::new(Int64Array::from(rows.clone())) as ArrayRef)
                        .collect();

                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut accumulators: Vec<_> = (0..num_groups)
                                .map(|_| prepare_accumulator(DataType::Int64))
                                .collect();

                            for (group_idx, batch) in group_arrays.iter().enumerate() {
                                if !batch.is_empty() {
                                    accumulators[group_idx]
                                        .update_batch(std::slice::from_ref(batch))
                                        .unwrap();
                                }
                            }

                            let _results: Vec<_> = accumulators
                                .iter_mut()
                                .map(|acc| acc.evaluate().unwrap())
                                .collect();
                        })
                    });
                }
            }
        }
    }

    // i32 benchmarks
    for num_groups in group_counts {
        for (card_name, distinct_pct) in cardinalities {
            for dist in distributions {
                let name = format!("i32_g{num_groups}_{card_name}_{dist}");
                let n_distinct = BATCH_SIZE * distinct_pct / 100;
                let values = Arc::new(create_i32_array(n_distinct)) as ArrayRef;
                let group_indices = if dist == "uniform" {
                    create_uniform_groups(num_groups)
                } else {
                    create_skewed_groups(num_groups)
                };

                let (_schema, args) = prepare_args(DataType::Int32);

                if count_fn.groups_accumulator_supported(args.clone()) {
                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut acc =
                                count_fn.create_groups_accumulator(args.clone()).unwrap();
                            acc.update_batch(
                                std::slice::from_ref(&values),
                                &group_indices,
                                None,
                                num_groups,
                            )
                            .unwrap();
                            acc.evaluate(EmitTo::All).unwrap()
                        })
                    });
                } else {
                    let arr = values.as_any().downcast_ref::<Int32Array>().unwrap();
                    let mut group_rows: Vec<Vec<i32>> = vec![Vec::new(); num_groups];
                    for (idx, &group_idx) in group_indices.iter().enumerate() {
                        if arr.is_valid(idx) {
                            group_rows[group_idx].push(arr.value(idx));
                        }
                    }
                    let group_arrays: Vec<ArrayRef> = group_rows
                        .iter()
                        .map(|rows| Arc::new(Int32Array::from(rows.clone())) as ArrayRef)
                        .collect();

                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut accumulators: Vec<_> = (0..num_groups)
                                .map(|_| prepare_accumulator(DataType::Int32))
                                .collect();

                            for (group_idx, batch) in group_arrays.iter().enumerate() {
                                if !batch.is_empty() {
                                    accumulators[group_idx]
                                        .update_batch(std::slice::from_ref(batch))
                                        .unwrap();
                                }
                            }

                            let _results: Vec<_> = accumulators
                                .iter_mut()
                                .map(|acc| acc.evaluate().unwrap())
                                .collect();
                        })
                    });
                }
            }
        }
    }

    // u32 benchmarks
    for num_groups in group_counts {
        for (card_name, distinct_pct) in cardinalities {
            for dist in distributions {
                let name = format!("u32_g{num_groups}_{card_name}_{dist}");
                let n_distinct = BATCH_SIZE * distinct_pct / 100;
                let values = Arc::new(create_u32_array(n_distinct)) as ArrayRef;
                let group_indices = if dist == "uniform" {
                    create_uniform_groups(num_groups)
                } else {
                    create_skewed_groups(num_groups)
                };

                let (_schema, args) = prepare_args(DataType::UInt32);

                if count_fn.groups_accumulator_supported(args.clone()) {
                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut acc =
                                count_fn.create_groups_accumulator(args.clone()).unwrap();
                            acc.update_batch(
                                std::slice::from_ref(&values),
                                &group_indices,
                                None,
                                num_groups,
                            )
                            .unwrap();
                            acc.evaluate(EmitTo::All).unwrap()
                        })
                    });
                } else {
                    let arr = values.as_any().downcast_ref::<UInt32Array>().unwrap();
                    let mut group_rows: Vec<Vec<u32>> = vec![Vec::new(); num_groups];
                    for (idx, &group_idx) in group_indices.iter().enumerate() {
                        if arr.is_valid(idx) {
                            group_rows[group_idx].push(arr.value(idx));
                        }
                    }
                    let group_arrays: Vec<ArrayRef> = group_rows
                        .iter()
                        .map(|rows| Arc::new(UInt32Array::from(rows.clone())) as ArrayRef)
                        .collect();

                    c.bench_function(&format!("count_distinct_groups {name}"), |b| {
                        b.iter(|| {
                            let mut accumulators: Vec<_> = (0..num_groups)
                                .map(|_| prepare_accumulator(DataType::UInt32))
                                .collect();

                            for (group_idx, batch) in group_arrays.iter().enumerate() {
                                if !batch.is_empty() {
                                    accumulators[group_idx]
                                        .update_batch(std::slice::from_ref(batch))
                                        .unwrap();
                                }
                            }

                            let _results: Vec<_> = accumulators
                                .iter_mut()
                                .map(|acc| acc.evaluate().unwrap())
                                .collect();
                        })
                    });
                }
            }
        }
    }
}

criterion_group!(
    benches,
    count_distinct_benchmark,
    count_distinct_groups_benchmark
);
criterion_main!(benches);
