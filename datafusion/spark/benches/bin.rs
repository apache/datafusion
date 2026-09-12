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

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_spark::function::math::bin;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NULL_DENSITY: f32 = 0.2;

/// `range` controls how many binary digits each value renders to, which is what
/// drives the amount of work per row.
fn random_ints(size: usize, seed: u64, range: std::ops::Range<i64>) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let array: Int64Array = (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                None
            } else {
                Some(rng.random_range(range.clone()))
            }
        })
        .collect();
    Arc::new(array)
}

fn bench(c: &mut Criterion, name: &str, input: ArrayRef) {
    let bin_fn = bin();
    let size = input.len();
    let args = vec![ColumnarValue::Array(input)];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                bin_fn
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 8192] {
        // Small values render to a handful of digits; the common case.
        bench(
            c,
            &format!("bin/small/size={size}"),
            random_ints(size, 42, 0..10_000),
        );
        // Full-width values render to the maximum 64 digits.
        bench(
            c,
            &format!("bin/wide/size={size}"),
            random_ints(size, 7, i64::MIN..i64::MAX),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
