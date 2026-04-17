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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int8Array, Int16Array, Int64Array, UInt8Array, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl};
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
}

criterion_group!(benches, count_distinct_benchmark);
criterion_main!(benches);
