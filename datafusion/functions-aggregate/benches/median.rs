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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl};
use datafusion_functions_aggregate::median::Median;
use datafusion_physical_expr::expressions::col;

const STEP_SIZE: usize = 128;
const SLIDES_PER_ITER: usize = 32;
const WINDOW_SIZES: [usize; 3] = [256, 4096, 16384];

fn prepare_accumulator() -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let expr = col("f", &schema).unwrap();
    let accumulator_args = AccumulatorArgs {
        return_field: Field::new("f", DataType::Float64, true).into(),
        schema: &schema,
        expr_fields: &[expr.return_field(&schema).unwrap()],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "median(f)",
        is_distinct: false,
        exprs: &[expr],
    };
    Median::new().accumulator(accumulator_args).unwrap()
}

fn stream_array(len: usize, null_stride: Option<usize>) -> ArrayRef {
    let values = (0..len)
        .map(|idx| {
            if null_stride.is_some_and(|stride| idx % stride == 0) {
                None
            } else {
                Some(idx as f64)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Float64Array::from(values)) as ArrayRef
}

/// Benchmark the sliding window cycle: retract + update + evaluate
fn sliding_window_bench(
    c: &mut Criterion,
    name: &str,
    window_size: usize,
    stream: &ArrayRef,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut accumulator = prepare_accumulator();
                let initial = stream.slice(0, window_size);
                accumulator
                    .update_batch(std::slice::from_ref(&initial))
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                for slide in 0..SLIDES_PER_ITER {
                    let offset = slide * STEP_SIZE;
                    let retract = stream.slice(offset, STEP_SIZE);
                    let update = stream.slice(offset + window_size, STEP_SIZE);
                    accumulator
                        .retract_batch(std::slice::from_ref(&retract))
                        .unwrap();
                    accumulator
                        .update_batch(std::slice::from_ref(&update))
                        .unwrap();
                    black_box(accumulator.evaluate().unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn median_benchmark(c: &mut Criterion) {
    for window_size in WINDOW_SIZES {
        let stream_len = window_size + STEP_SIZE * SLIDES_PER_ITER;
        let stream_no_nulls = stream_array(stream_len, None);
        let stream_with_nulls = stream_array(stream_len, Some(10));

        sliding_window_bench(
            c,
            &format!("median sliding_window f64 no_nulls window_size={window_size}"),
            window_size,
            &stream_no_nulls,
        );

        sliding_window_bench(
            c,
            &format!("median sliding_window f64 with_nulls window_size={window_size}"),
            window_size,
            &stream_with_nulls,
        );
    }
}

criterion_group!(benches, median_benchmark);
criterion_main!(benches);
