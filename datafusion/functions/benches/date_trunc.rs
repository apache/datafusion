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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, TimestampSecondArray};
use arrow::datatypes::Field;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::date_trunc;
use rand::rngs::ThreadRng;
use rand::Rng;

fn timestamps(rng: &mut ThreadRng) -> TimestampSecondArray {
    let mut seconds = vec![];
    for _ in 0..1000 {
        seconds.push(rng.random_range(0..1_000_000));
    }

    TimestampSecondArray::from(seconds)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("date_trunc_minute_1000", |b| {
        let mut rng = rand::rng();
        let timestamps_array = Arc::new(timestamps(&mut rng)) as ArrayRef;
        let batch_len = timestamps_array.len();
        let precision =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("minute".to_string())));
        let timestamps = ColumnarValue::Array(timestamps_array);
        let udf = date_trunc();
        let args = vec![precision, timestamps];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        let return_type = udf
            .return_type(&args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>())
            .unwrap();
        let return_field = Arc::new(Field::new("f", return_type, true));
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_trunc should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
