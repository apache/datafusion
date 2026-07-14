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

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::core::greatest;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const SIZE: usize = 1024;

fn int64_array(seed: u64, null_density: f32) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let values: Int64Array = (0..SIZE)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random_range(0i64..1000))
            }
        })
        .collect();
    Arc::new(values)
}

fn string_array(seed: u64, null_density: f32) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let values: StringArray = (0..SIZE)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(format!("value_{}", rng.random_range(0..1000)))
            }
        })
        .collect();
    Arc::new(values)
}

fn bench_greatest(c: &mut Criterion, func: &ScalarUDF, name: &str, args: Vec<ArrayRef>) {
    let return_type = args[0].data_type().clone();
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            Field::new(format!("arg_{idx}"), arg.data_type().clone(), true).into()
        })
        .collect::<Vec<_>>();
    let args = args
        .into_iter()
        .map(ColumnarValue::Array)
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                func.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: SIZE,
                    return_field: Arc::new(Field::new("f", return_type.clone(), true)),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let greatest_func = greatest();

    bench_greatest(
        c,
        &greatest_func,
        "greatest_i64_nullable",
        vec![int64_array(1, 0.2), int64_array(2, 0.2)],
    );
    bench_greatest(
        c,
        &greatest_func,
        "greatest_i64_no_nulls",
        vec![int64_array(3, 0.0), int64_array(4, 0.0)],
    );
    bench_greatest(
        c,
        &greatest_func,
        "greatest_i64_nullable_3_args",
        vec![
            int64_array(5, 0.2),
            int64_array(6, 0.2),
            int64_array(7, 0.2),
        ],
    );
    bench_greatest(
        c,
        &greatest_func,
        "greatest_utf8_nullable",
        vec![string_array(8, 0.2), string_array(9, 0.2)],
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
