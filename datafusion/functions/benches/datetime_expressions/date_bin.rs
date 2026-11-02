// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, TimestampSecondArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::date_bin;
use rand::prelude::*;

fn timestamps(rng: &mut StdRng) -> TimestampSecondArray {
    let mut seconds = vec![];
    for _ in 0..1000 {
        seconds.push(rng.random_range(0..1_000_000));
    }

    TimestampSecondArray::from(seconds)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("date_bin_1000", |b| {
        let mut rng = StdRng::seed_from_u64(0);
        let timestamps_array = Arc::new(timestamps(&mut rng)) as ArrayRef;
        let batch_len = timestamps_array.len();
        let interval = ColumnarValue::Scalar(ScalarValue::new_interval_dt(0, 1_000_000));
        let timestamps = ColumnarValue::Array(timestamps_array);
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

criterion_group!(benches, criterion_benchmark);
