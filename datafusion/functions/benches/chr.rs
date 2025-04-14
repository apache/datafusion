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

use arrow::{array::PrimitiveArray, datatypes::Int64Type, util::test_util::seedable_rng};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string::chr;
use rand::Rng;

use arrow::datatypes::DataType;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let cot_fn = chr();
    let size = 1024;
    let input: PrimitiveArray<Int64Type> = {
        let null_density = 0.2;
        let mut rng = seedable_rng();
        (0..size)
            .map(|_| {
                if rng.gen::<f32>() < null_density {
                    None
                } else {
                    Some(rng.gen_range::<i64, _>(1i64..10_000))
                }
            })
            .collect()
    };
    let input = Arc::new(input);
    let args = vec![ColumnarValue::Array(input)];
    c.bench_function("chr", |b| {
        b.iter(|| {
            black_box(
                cot_fn
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        number_rows: size,
                        return_type: &DataType::Utf8,
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
