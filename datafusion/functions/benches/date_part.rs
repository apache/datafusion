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

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::{
    Array, ArrayRef, Date32Array, Date64Array, DurationNanosecondArray,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::datetime::date_part;
use rand::Rng;
use rand::rngs::ThreadRng;

const BATCH_SIZE: usize = 1000;
const TS_BOUND: i64 = 2_006_463_600;
const SEC_DAY: i64 = 86_400;

fn generate_timestamp_ns_array(rng: &mut ThreadRng) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND * 1_000_000_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_timestamp_us_array(rng: &mut ThreadRng) -> TimestampMicrosecondArray {
    TimestampMicrosecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND * 1_000_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_timestamp_ms_array(rng: &mut ThreadRng) -> TimestampMillisecondArray {
    TimestampMillisecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND * 1_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_timestamp_s_array(rng: &mut ThreadRng) -> TimestampSecondArray {
    TimestampSecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND))
            .collect::<Vec<_>>(),
    )
}

fn generate_date32_array(rng: &mut StdRng) -> Date32Array {
    let days_since_epoch = (TS_BOUND as i32) / SEC_DAY as i32;
    Date32Array::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..days_since_epoch))
            .collect::<Vec<_>>(),
    )
}

fn generate_date64_array(rng: &mut ThreadRng) -> Date64Array {
    Date64Array::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND * 1_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_time32_second_array(rng: &mut ThreadRng) -> Time32SecondArray {
    Time32SecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..SEC_DAY as i32))
            .collect::<Vec<_>>(),
    )
}

fn generate_time32_millisecond_array(rng: &mut ThreadRng) -> Time32MillisecondArray {
    Time32MillisecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..(SEC_DAY * 1_000) as i32))
            .collect::<Vec<_>>(),
    )
}

fn generate_time64_microsecond_array(rng: &mut ThreadRng) -> Time64MicrosecondArray {
    Time64MicrosecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..SEC_DAY * 1_000_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_time64_nanosecond_array(rng: &mut ThreadRng) -> Time64NanosecondArray {
    Time64NanosecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..SEC_DAY * 1_000_000_000))
            .collect::<Vec<_>>(),
    )
}

fn generate_interval_year_month_array(rng: &mut StdRng) -> IntervalYearMonthArray {
    let years = 10;
    IntervalYearMonthArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..12 * years))
            .collect::<Vec<_>>(),
    )
}

fn generate_interval_day_time_array(rng: &mut ThreadRng) -> IntervalDayTimeArray {
    IntervalDayTimeArray::from(
        (0..BATCH_SIZE)
            .map(|_| IntervalDayTime {
                days: rng.random_range(0..365),
                milliseconds: rng.random_range(0..(SEC_DAY * 1_000) as i32),
            })
            .collect::<Vec<_>>(),
    )
}

fn generate_interval_mdn_array(rng: &mut ThreadRng) -> IntervalMonthDayNanoArray {
    IntervalMonthDayNanoArray::from(
        (0..BATCH_SIZE)
            .map(|_| IntervalMonthDayNano {
                months: rng.random_range(0..12),
                days: rng.random_range(0..365),
                nanoseconds: rng.random_range(0..SEC_DAY * 1_000_000_000),
            })
            .collect::<Vec<_>>(),
    )
}

fn generate_duration_nanosecond_array(rng: &mut ThreadRng) -> DurationNanosecondArray {
    DurationNanosecondArray::from(
        (0..BATCH_SIZE)
            .map(|_| rng.random_range(0..TS_BOUND * 1_000_000_000))
            .collect::<Vec<_>>(),
    )
}

fn bench_date_part(
    c: &mut Criterion,
    udf: &Arc<ScalarUDF>,
    bench_name: &str,
    part: &str,
    array: ArrayRef,
    return_type: DataType,
) {
    let batch_len = array.len();
    let part_cv = ColumnarValue::Scalar(ScalarValue::Utf8(Some(part.to_string())));
    let array_cv = ColumnarValue::Array(array);
    let return_field = Arc::new(Field::new("date_part", return_type, true));
    let arg_fields = vec![
        Field::new("a", part_cv.data_type(), true).into(),
        Field::new("b", array_cv.data_type(), true).into(),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(bench_name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![part_cv.clone(), array_cv.clone()],
                    arg_fields: arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_part should work on valid values"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();

    let ts_s = Arc::new(generate_timestamp_s_array(&mut rng)) as ArrayRef;
    let ts_ms = Arc::new(generate_timestamp_ms_array(&mut rng)) as ArrayRef;
    let ts_us = Arc::new(generate_timestamp_us_array(&mut rng)) as ArrayRef;
    let ts_ns = Arc::new(generate_timestamp_ns_array(&mut rng)) as ArrayRef;
    let time32_s = Arc::new(generate_time32_second_array(&mut rng)) as ArrayRef;
    let time32_ms = Arc::new(generate_time32_millisecond_array(&mut rng)) as ArrayRef;
    let time64_us = Arc::new(generate_time64_microsecond_array(&mut rng)) as ArrayRef;
    let time64_ns = Arc::new(generate_time64_nanosecond_array(&mut rng)) as ArrayRef;
    let interval_ym = Arc::new(generate_interval_year_month_array(&mut rng)) as ArrayRef;
    let interval_dt = Arc::new(generate_interval_day_time_array(&mut rng)) as ArrayRef;
    let interval_mdn = Arc::new(generate_interval_mdn_array(&mut rng)) as ArrayRef;
    let duration_ns = Arc::new(generate_duration_nanosecond_array(&mut rng)) as ArrayRef;
    let date32 = Arc::new(generate_date32_array(&mut rng)) as ArrayRef;
    let date64 = Arc::new(generate_date64_array(&mut rng)) as ArrayRef;

    let udf = date_part();

    for part in ["year", "month", "week", "day", "hour", "minute"] {
        for (name, array) in
            [("s", &ts_s), ("ms", &ts_ms), ("us", &ts_us), ("ns", &ts_ns)]
        {
            bench_date_part(
                c,
                &udf,
                &format!("date_part_{part}_{name}_1000"),
                part,
                Arc::clone(array),
                DataType::Int32,
            );
        }
    }
    for part in ["year", "month", "week", "day"] {
        bench_date_part(
            c,
            &udf,
            &format!("date_part_{part}_date32_1000"),
            part,
            Arc::clone(&date32),
            DataType::Int32,
        );
        bench_date_part(
            c,
            &udf,
            &format!("date_part_{part}_date64_1000"),
            part,
            Arc::clone(&date64),
            DataType::Int32,
        );
    }

    for part in ["second", "millisecond", "microsecond"] {
        for (name, array) in
            [("s", &ts_s), ("ms", &ts_ms), ("us", &ts_us), ("ns", &ts_ns)]
        {
            bench_date_part(
                c,
                &udf,
                &format!("date_part_{part}_{name}_1000"),
                part,
                Arc::clone(array),
                DataType::Int32,
            );
        }
        bench_date_part(
            c,
            &udf,
            &format!("date_part_{part}_date32_1000"),
            part,
            Arc::clone(&date32),
            DataType::Int32,
        );
        bench_date_part(
            c,
            &udf,
            &format!("date_part_{part}_date64_1000"),
            part,
            Arc::clone(&date64),
            DataType::Int32,
        );
    }

    for (name, array) in [("s", &ts_s), ("ms", &ts_ms), ("us", &ts_us), ("ns", &ts_ns)] {
        bench_date_part(
            c,
            &udf,
            &format!("date_part_nanosecond_{name}_1000"),
            "nanosecond",
            Arc::clone(array),
            DataType::Int64,
        );
    }
    bench_date_part(
        c,
        &udf,
        "date_part_nanosecond_date32_1000",
        "nanosecond",
        Arc::clone(&date32),
        DataType::Int64,
    );
    bench_date_part(
        c,
        &udf,
        "date_part_nanosecond_date64_1000",
        "nanosecond",
        Arc::clone(&date64),
        DataType::Int64,
    );

    for (name, array) in [
        ("s", &ts_s),
        ("ms", &ts_ms),
        ("us", &ts_us),
        ("ns", &ts_ns),
        ("date32", &date32),
        ("date64", &date64),
        ("time32_s", &time32_s),
        ("time32_ms", &time32_ms),
        ("time64_us", &time64_us),
        ("time64_ns", &time64_ns),
        ("interval_ym", &interval_ym),
        ("interval_dt", &interval_dt),
        ("interval_mdn", &interval_mdn),
        ("duration_ns", &duration_ns),
    ] {
        bench_date_part(
            c,
            &udf,
            &format!("date_part_epoch_{name}_1000"),
            "epoch",
            Arc::clone(array),
            DataType::Float64,
        );
    }

    for part in ["quarter", "isoyear", "doy", "dow", "isodow"] {
        bench_date_part(
            c,
            &udf,
            &format!("date_part_{part}_timestamp_ns_1000"),
            part,
            Arc::clone(&ts_ns),
            DataType::Int32,
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
