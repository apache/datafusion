// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Measures primitive DISTINCT insertion and merging overlapping partial states.

use std::fmt::Debug;
use std::hash::Hash;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::datatypes::{Decimal128Type, Decimal256Type, Int32Type, Int64Type, i256};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_functions_aggregate_common::aggregate::count_distinct::PrimitiveDistinctCountAccumulator;

struct Workload {
    name: &'static str,
    rows: usize,
    ndv: usize,
    chunks: usize,
    nullable: bool,
}

fn bench_type<T>(c: &mut Criterion, name: &str, value: fn(usize) -> T::Native)
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash,
{
    let mut group = c.benchmark_group(name);
    for workload in [
        Workload {
            name: "small",
            rows: 16,
            ndv: 3,
            chunks: 1,
            nullable: false,
        },
        Workload {
            name: "duplicates",
            rows: 8192,
            ndv: 128,
            chunks: 1,
            nullable: false,
        },
        Workload {
            name: "unique",
            rows: 8192,
            ndv: 8192,
            chunks: 1,
            nullable: false,
        },
        Workload {
            name: "overlapping_chunks",
            rows: 8192,
            ndv: 128,
            chunks: 64,
            nullable: false,
        },
        Workload {
            name: "nullable",
            rows: 8192,
            ndv: 128,
            chunks: 1,
            nullable: true,
        },
    ] {
        let input: ArrayRef = Arc::new(PrimitiveArray::<T>::from_iter(
            (0..workload.rows).map(|i| {
                if workload.nullable && i % 17 == 0 {
                    None
                } else {
                    Some(value(i % workload.ndv))
                }
            }),
        ));
        let width = workload.rows / workload.chunks;
        let chunks: Vec<_> = (0..workload.chunks)
            .map(|i| input.slice(i * width, width))
            .collect();
        // Generate real partial states outside the timed region. A merge of
        // duplicate-heavy input therefore consumes NDV values per partial state.
        let partial_states = chunks.iter().map(|chunk| {
            let mut partial =
                PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
            partial.update_batch(std::slice::from_ref(chunk)).unwrap();
            let ScalarValue::List(state) = partial.state().unwrap().remove(0) else {
                unreachable!("primitive distinct state is a list")
            };
            // Hash-set iteration order is unspecified. Sorting outside timing
            // gives each container implementation the same merge input order.
            let values = arrow::compute::sort(state.value(0).as_ref(), None).unwrap();
            SingleRowListArrayBuilder::new(values).build_list_scalar()
        });
        let states = [ScalarValue::iter_to_array(partial_states).unwrap()];
        let mut direct = PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
        for chunk in &chunks {
            direct.update_batch(std::slice::from_ref(chunk)).unwrap();
        }
        let mut merged = PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
        merged.merge_batch(&states).unwrap();
        let expected = ScalarValue::Int64(Some(workload.ndv as i64));
        assert_eq!(direct.evaluate().unwrap(), expected);
        assert_eq!(merged.evaluate().unwrap(), expected);

        group.bench_function(BenchmarkId::new("update", workload.name), |b| {
            b.iter(|| {
                let mut accumulator =
                    PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
                for chunk in &chunks {
                    accumulator
                        .update_batch(black_box(std::slice::from_ref(chunk)))
                        .unwrap();
                }
                black_box(&accumulator);
            });
        });
        group.bench_function(BenchmarkId::new("merge", workload.name), |b| {
            b.iter(|| {
                let mut accumulator =
                    PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
                accumulator.merge_batch(black_box(&states)).unwrap();
                black_box(&accumulator);
            });
        });
    }
    group.finish();
}

fn count_distinct(c: &mut Criterion) {
    bench_type::<Int32Type>(c, "Int32", |i| i as i32);
    bench_type::<Int64Type>(c, "Int64", |i| i as i64);
    bench_type::<Decimal128Type>(c, "Decimal128", |i| (1_i128 << 100) + i as i128);
    bench_type::<Decimal256Type>(c, "Decimal256", |i| {
        (i256::from_i128(1) << 200_u32) + i256::from_i128(i as i128)
    });
}

criterion_group!(benches, count_distinct);
criterion_main!(benches);
