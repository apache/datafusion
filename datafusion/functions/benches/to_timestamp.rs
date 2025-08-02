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

use arrow::array::builder::StringBuilder;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, TimeUnit};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_timestamp;

fn data() -> StringArray {
    let data: Vec<&str> = vec![
        "1997-01-31T09:26:56.123Z",
        "1997-01-31T09:26:56.123-05:00",
        "1997-01-31 09:26:56.123-05:00",
        "2023-01-01 04:05:06.789 -08",
        "1997-01-31T09:26:56.123",
        "1997-01-31 09:26:56.123",
        "1997-01-31 09:26:56",
        "1997-01-31 13:26:56",
        "1997-01-31 13:26:56+04:00",
        "1997-01-31",
    ];

    StringArray::from(data)
}

fn data_with_formats() -> (StringArray, StringArray, StringArray, StringArray) {
    let mut inputs = StringBuilder::new();
    let mut format1_builder = StringBuilder::with_capacity(2, 10);
    let mut format2_builder = StringBuilder::with_capacity(2, 10);
    let mut format3_builder = StringBuilder::with_capacity(2, 10);

    inputs.append_value("1997-01-31T09:26:56.123Z");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%Z");

    inputs.append_value("1997-01-31T09:26:56.123-05:00");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%z");

    inputs.append_value("1997-01-31 09:26:56.123-05:00");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f%Z");

    inputs.append_value("2023-01-01 04:05:06.789 -08");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f %#z");

    inputs.append_value("1997-01-31T09:26:56.123");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f");

    inputs.append_value("1997-01-31 09:26:56.123");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f");

    inputs.append_value("1997-01-31 09:26:56");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S");

    inputs.append_value("1997-01-31 092656");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H%M%S");

    inputs.append_value("1997-01-31 092656+04:00");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d %H%M%S%:z");

    inputs.append_value("Sun Jul  8 00:34:60 2001");
    format1_builder.append_value("%+");
    format2_builder.append_value("%c");
    format3_builder.append_value("%Y-%m-%d 00:00:00");

    (
        inputs.finish(),
        format1_builder.finish(),
        format2_builder.finish(),
        format3_builder.finish(),
    )
}
fn criterion_benchmark(c: &mut Criterion) {
    let return_field =
        Field::new("f", DataType::Timestamp(TimeUnit::Nanosecond, None), true).into();
    let arg_field = Field::new("a", DataType::Utf8, false).into();
    let arg_fields = vec![arg_field];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("to_timestamp_no_formats_utf8", |b| {
        let arr_data = data();
        let batch_len = arr_data.len();
        let string_array = ColumnarValue::Array(Arc::new(arr_data) as ArrayRef);

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![string_array.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_no_formats_largeutf8", |b| {
        let data = cast(&data(), &DataType::LargeUtf8).unwrap();
        let batch_len = data.len();
        let string_array = ColumnarValue::Array(Arc::new(data) as ArrayRef);

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![string_array.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_no_formats_utf8view", |b| {
        let data = cast(&data(), &DataType::Utf8View).unwrap();
        let batch_len = data.len();
        let string_array = ColumnarValue::Array(Arc::new(data) as ArrayRef);

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![string_array.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_with_formats_utf8", |b| {
        let (inputs, format1, format2, format3) = data_with_formats();
        let batch_len = inputs.len();

        let args = vec![
            ColumnarValue::Array(Arc::new(inputs) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3) as ArrayRef),
        ];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_with_formats_largeutf8", |b| {
        let (inputs, format1, format2, format3) = data_with_formats();
        let batch_len = inputs.len();

        let args = vec![
            ColumnarValue::Array(
                Arc::new(cast(&inputs, &DataType::LargeUtf8).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format1, &DataType::LargeUtf8).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format2, &DataType::LargeUtf8).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format3, &DataType::LargeUtf8).unwrap()) as ArrayRef
            ),
        ];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_with_formats_utf8view", |b| {
        let (inputs, format1, format2, format3) = data_with_formats();

        let batch_len = inputs.len();

        let args = vec![
            ColumnarValue::Array(
                Arc::new(cast(&inputs, &DataType::Utf8View).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format1, &DataType::Utf8View).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format2, &DataType::Utf8View).unwrap()) as ArrayRef
            ),
            ColumnarValue::Array(
                Arc::new(cast(&format3, &DataType::Utf8View).unwrap()) as ArrayRef
            ),
        ];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
