// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_local_time;
use rand::prelude::*;

fn timestamps(rng: &mut StdRng) -> TimestampNanosecondArray {
    let nanos: Vec<i64> = (0..100_000)
        .map(|_| rng.random_range(0..1_000_000_000_000_000_000i64))
        .collect();
    TimestampNanosecondArray::from(nanos).with_timezone("America/New_York")
}

fn timestamps_with_nulls(rng: &mut StdRng) -> TimestampNanosecondArray {
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
    let mut rng = StdRng::seed_from_u64(0);
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
