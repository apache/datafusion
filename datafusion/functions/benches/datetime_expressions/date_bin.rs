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

use arrow::array::{Array, TimestampNanosecondArray, TimestampSecondArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::date_bin;
use rand::prelude::*;

const NUM_ROWS: usize = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
/// Roughly 1 year, so that values span many buckets.
const RANGE_SECONDS: i64 = 365 * 24 * 60 * 60;

fn second_timestamps() -> TimestampSecondArray {
    let mut rng = StdRng::seed_from_u64(42);
    (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(-RANGE_SECONDS..RANGE_SECONDS)))
        .collect()
}

fn nanosecond_timestamps() -> TimestampNanosecondArray {
    let mut rng = StdRng::seed_from_u64(42);
    (0..NUM_ROWS)
        .map(|_| {
            let seconds = rng.random_range(-RANGE_SECONDS..RANGE_SECONDS);
            Some(seconds * NANOS_PER_SECOND + rng.random_range(0..NANOS_PER_SECOND))
        })
        .collect()
}

fn run_benchmark(
    c: &mut Criterion,
    name: &str,
    stride_nanos: i64,
    timestamps: &ColumnarValue,
) {
    let batch_len = match timestamps {
        ColumnarValue::Array(a) => a.len(),
        _ => unreachable!(),
    };
    let interval = ColumnarValue::Scalar(ScalarValue::new_interval_dt(
        0,
        (stride_nanos / 1_000_000) as i32,
    ));
    let udf = date_bin();
    let return_type = udf
        .return_type(&[interval.data_type(), timestamps.data_type()])
        .unwrap();
    let return_field = Arc::new(Field::new("f", return_type, true));
    let arg_fields = vec![
        Field::new("a", interval.data_type(), true).into(),
        Field::new("b", timestamps.data_type(), true).into(),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![interval.clone(), timestamps.clone()],
                    arg_fields: arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });
}

/// Bench cases: second and nanosecond precision, 1-hour stride, 1-year time range.
fn criterion_benchmark(c: &mut Criterion) {
    run_benchmark(
        c,
        &format!("date_bin_seconds_{NUM_ROWS}"),
        3_600 * NANOS_PER_SECOND,
        &ColumnarValue::Array(Arc::new(second_timestamps())),
    );
    run_benchmark(
        c,
        &format!("date_bin_nanos_{NUM_ROWS}"),
        3_600 * NANOS_PER_SECOND,
        &ColumnarValue::Array(Arc::new(nanosecond_timestamps())),
    );
}

criterion_group!(benches, criterion_benchmark);
