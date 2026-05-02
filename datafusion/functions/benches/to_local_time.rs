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

use arrow::array::{Array, ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_local_time;
use rand::Rng;
use rand::rngs::ThreadRng;

fn timestamps(rng: &mut ThreadRng) -> TimestampNanosecondArray {
    let nanos: Vec<i64> = (0..100_000)
        .map(|_| rng.random_range(0..1_000_000_000_000_000_000i64))
        .collect();
    TimestampNanosecondArray::from(nanos).with_timezone("America/New_York")
}

fn timestamps_with_nulls(rng: &mut ThreadRng) -> TimestampNanosecondArray {
    let values: Vec<Option<i64>> = (0..100_000)
        .map(|_| {
            if rng.random_range(0..10u32) == 0 {
                None
            } else {
                Some(rng.random_range(0..1_000_000_000_000_000_000i64))
            }
        })
        .collect();
    TimestampNanosecondArray::from(values).with_timezone("America/New_York")
}

fn bench_to_local_time(c: &mut Criterion, name: &str, array: ArrayRef) {
    let batch_len = array.len();
    let input = ColumnarValue::Array(array);
    let udf = to_local_time();
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
                .expect("to_local_time should work on valid values"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    bench_to_local_time(
        c,
        "to_local_time_no_nulls_100k",
        Arc::new(timestamps(&mut rng)),
    );
    bench_to_local_time(
        c,
        "to_local_time_10pct_nulls_100k",
        Arc::new(timestamps_with_nulls(&mut rng)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
