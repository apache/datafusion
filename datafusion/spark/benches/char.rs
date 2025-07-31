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

extern crate criterion;

use arrow::datatypes::{DataType, Field};
use arrow::{array::PrimitiveArray, datatypes::Int64Type};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_spark::function::string::char;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn criterion_benchmark(c: &mut Criterion) {
    let cot_fn = char();
    let size = 1024;
    let input: PrimitiveArray<Int64Type> = {
        let null_density = 0.2;
        let mut rng = StdRng::seed_from_u64(42);
        (0..size)
            .map(|_| {
                if rng.random::<f32>() < null_density {
                    None
                } else {
                    Some(rng.random_range::<i64, _>(1i64..10_000))
                }
            })
            .collect()
    };
    let input = Arc::new(input);
    let args = vec![ColumnarValue::Array(input)];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();

    c.bench_function("char", |b| {
        b.iter(|| {
            black_box(
                cot_fn
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
                    })
                    .unwrap(),
            )
        })
    });
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
