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

use arrow::array::{Array, ArrayRef, TimestampNanosecondArray, TimestampSecondArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_functions::datetime::date_trunc;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 1000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
/// Roughly 30 years, so that values span many months, quarters and years.
const RANGE_SECONDS: i64 = 30 * 365 * 24 * 60 * 60;

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn second_timestamps() -> TimestampSecondArray {
    let mut rng = seedable_rng();
    (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(0..1_000_000i64)))
        .collect()
}

fn nanosecond_timestamps() -> TimestampNanosecondArray {
    let mut rng = seedable_rng();
    (0..NUM_ROWS)
        .map(|_| {
            let seconds = rng.random_range(-RANGE_SECONDS..RANGE_SECONDS);
            Some(seconds * NANOS_PER_SECOND + rng.random_range(0..NANOS_PER_SECOND))
        })
        .collect()
}

fn run_benchmark(c: &mut Criterion, name: &str, granularity: &str, array: ArrayRef) {
    let batch_len = array.len();
    let precision =
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(granularity.to_string())));
    let udf = date_trunc();
    let args = vec![precision, ColumnarValue::Array(array)];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();

    let scalar_arguments = vec![None; arg_fields.len()];
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })
        .unwrap();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
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

fn criterion_benchmark(c: &mut Criterion) {
    let seconds: ArrayRef = Arc::new(second_timestamps());
    run_benchmark(c, "date_trunc_minute_1000", "minute", Arc::clone(&seconds));
    run_benchmark(c, "date_trunc_month_second_1000", "month", seconds);

    // Coarse granularities on an untimezoned array: these need calendar
    // arithmetic rather than a plain division.
    let nanos: ArrayRef = Arc::new(nanosecond_timestamps());
    for granularity in ["week", "month", "quarter", "year"] {
        run_benchmark(
            c,
            &format!("date_trunc_{granularity}_nanos_1000"),
            granularity,
            Arc::clone(&nanos),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
