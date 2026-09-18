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

use arrow::array::IntervalMonthDayNanoArray;
use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNanoType, IntervalUnit, TimeUnit,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::range::Range;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 100_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

fn range_timestamp(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    // ~2016-era timestamps; each row generates a 1-second range (1 element),
    // so the per-row timestamp-range setup dominates the measurement.
    let start: TimestampNanosecondArray = (0..NUM_ROWS)
        .map(|_| {
            Some(1_452_499_200_000_000_000i64 + rng.random_range(0..NANOS_PER_SECOND))
        })
        .collect();
    let stop: TimestampNanosecondArray = start
        .iter()
        .map(|v| v.map(|v| v + NANOS_PER_SECOND))
        .collect();
    let step = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(
            0,
            0,
            NANOS_PER_SECOND
        );
        NUM_ROWS
    ]);

    let udf = Range::new();
    let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let return_type = udf
        .return_type(&[
            ts_type.clone(),
            ts_type.clone(),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ])
        .unwrap();
    let return_field = Arc::new(Field::new("f", return_type, true));

    let arg_fields = vec![
        Field::new("a", ts_type.clone(), true).into(),
        Field::new("b", ts_type, true).into(),
        Field::new("c", DataType::Interval(IntervalUnit::MonthDayNano), true).into(),
    ];
    let config_options = Arc::new(ConfigOptions::default());
    let args = vec![
        ColumnarValue::Array(Arc::new(start)),
        ColumnarValue::Array(Arc::new(stop)),
        ColumnarValue::Array(Arc::new(step)),
    ];

    c.bench_function(&format!("range_timestamp_{NUM_ROWS}"), |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: NUM_ROWS,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("range should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, range_timestamp);
criterion_main!(benches);
