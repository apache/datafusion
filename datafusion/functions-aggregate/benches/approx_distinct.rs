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

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl};
use datafusion_functions_aggregate::approx_distinct::ApproxDistinct;
use datafusion_physical_expr::expressions::col;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;
const SHORT_STRING_LENGTH: usize = 8;
const LONG_STRING_LENGTH: usize = 20;

fn prepare_accumulator(data_type: DataType) -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new("f", data_type, true)]));
    let expr = col("f", &schema).unwrap();
    let accumulator_args = AccumulatorArgs {
        return_field: Field::new("f", DataType::UInt64, true).into(),
        schema: &schema,
        expr_fields: &[expr.return_field(&schema).unwrap()],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "approx_distinct(f)",
        is_distinct: false,
        exprs: &[expr],
    };
    ApproxDistinct::new().accumulator(accumulator_args).unwrap()
}

/// Creates an Int64Array where values are drawn from `0..n_distinct`.
fn create_i64_array(n_distinct: usize) -> Int64Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| Some(rng.random_range(0..n_distinct as i64)))
        .collect()
}

/// Creates a pool of `n_distinct` random strings of the given length.
fn create_string_pool(n_distinct: usize, string_length: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..n_distinct)
        .map(|_| {
            (0..string_length)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect()
        })
        .collect()
}

/// Creates a StringArray where values are drawn from the given pool.
fn create_string_array(pool: &[String]) -> StringArray {
    let mut rng = StdRng::seed_from_u64(99);
    (0..BATCH_SIZE)
        .map(|_| Some(pool[rng.random_range(0..pool.len())].as_str()))
        .collect()
}

/// Creates a StringViewArray where values are drawn from the given pool.
fn create_string_view_array(pool: &[String]) -> StringViewArray {
    let mut rng = StdRng::seed_from_u64(99);
    (0..BATCH_SIZE)
        .map(|_| Some(pool[rng.random_range(0..pool.len())].as_str()))
        .collect()
}

fn approx_distinct_benchmark(c: &mut Criterion) {
    for pct in [80, 99] {
        let n_distinct = BATCH_SIZE * pct / 100;

        // --- Int64 benchmarks ---
        let values = Arc::new(create_i64_array(n_distinct)) as ArrayRef;
        c.bench_function(&format!("approx_distinct i64 {pct}% distinct"), |b| {
            b.iter(|| {
                let mut accumulator = prepare_accumulator(DataType::Int64);
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap()
            })
        });

        for (label, str_len) in
            [("short", SHORT_STRING_LENGTH), ("long", LONG_STRING_LENGTH)]
        {
            let string_pool = create_string_pool(n_distinct, str_len);

            // --- Utf8 benchmarks ---
            let values = Arc::new(create_string_array(&string_pool)) as ArrayRef;
            c.bench_function(
                &format!("approx_distinct utf8 {label} {pct}% distinct"),
                |b| {
                    b.iter(|| {
                        let mut accumulator = prepare_accumulator(DataType::Utf8);
                        accumulator
                            .update_batch(std::slice::from_ref(&values))
                            .unwrap()
                    })
                },
            );

            // --- Utf8View benchmarks ---
            let values = Arc::new(create_string_view_array(&string_pool)) as ArrayRef;
            c.bench_function(
                &format!("approx_distinct utf8view {label} {pct}% distinct"),
                |b| {
                    b.iter(|| {
                        let mut accumulator = prepare_accumulator(DataType::Utf8View);
                        accumulator
                            .update_batch(std::slice::from_ref(&values))
                            .unwrap()
                    })
                },
            );
        }
    }
}

criterion_group!(benches, approx_distinct_benchmark);
criterion_main!(benches);
