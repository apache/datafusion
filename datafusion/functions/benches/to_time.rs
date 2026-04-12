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

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_time;
use rand::Rng;
use rand::rngs::ThreadRng;

fn random_time_string(rng: &mut ThreadRng) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:06}",
        rng.random_range(0..24u32),
        rng.random_range(0..60u32),
        rng.random_range(0..60u32),
        rng.random_range(0..1_000_000u32),
    )
}

fn time_strings(rng: &mut ThreadRng) -> StringArray {
    let strings: Vec<String> = (0..100_000).map(|_| random_time_string(rng)).collect();
    StringArray::from(strings)
}

fn time_strings_with_nulls(rng: &mut ThreadRng) -> StringArray {
    let values: Vec<Option<String>> = (0..100_000)
        .map(|_| {
            if rng.random_range(0..10u32) == 0 {
                None
            } else {
                Some(random_time_string(rng))
            }
        })
        .collect();
    StringArray::from(values)
}

fn bench_to_time(c: &mut Criterion, name: &str, array: ArrayRef) {
    let batch_len = array.len();
    let input = ColumnarValue::Array(array);
    let udf = to_time();
    let return_type = udf.return_type(&[input.data_type()]).unwrap();
    let return_field = Arc::new(Field::new("f", return_type, true));
    let arg_fields = vec![Field::new("a", input.data_type(), true).into()];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![input.clone()],
                    arg_fields: arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("to_time should work on valid values"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    bench_to_time(c, "to_time_no_nulls_100k", Arc::new(time_strings(&mut rng)));
    bench_to_time(
        c,
        "to_time_10pct_nulls_100k",
        Arc::new(time_strings_with_nulls(&mut rng)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
