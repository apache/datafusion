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

use arrow::datatypes::Field;
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::lcm;
use rand::Rng;
use std::hint::black_box;
use std::sync::Arc;

fn generate_i64_array(n_rows: usize) -> ArrayRef {
    let mut rng = rand::rng();
    let values = (0..n_rows)
        .map(|_| rng.random_range(0..1000))
        .collect::<Vec<_>>();
    Arc::new(Int64Array::from(values)) as ArrayRef
}

fn criterion_benchmark(c: &mut Criterion) {
    let n_rows = 100000;
    let array_a = ColumnarValue::Array(generate_i64_array(n_rows));
    let array_b = ColumnarValue::Array(generate_i64_array(n_rows));
    let udf = lcm();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("lcm both array", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), array_b.clone()],
                    arg_fields: vec![
                        Field::new("a", array_a.data_type(), true).into(),
                        Field::new("b", array_b.data_type(), true).into(),
                    ],
                    number_rows: n_rows,
                    return_field: Field::new("f", DataType::Int64, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("lcm should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
