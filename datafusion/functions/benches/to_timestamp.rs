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

use arrow::array::builder::StringBuilder;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, TimeUnit};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::parser::DateTimeParser;
use datafusion_functions::datetime::parser::chrono::ChronoDateTimeParser;
use datafusion_functions::datetime::parser::jiff::JiffDateTimeParser;
use datafusion_functions::datetime::to_timestamp;
use itertools::izip;

fn data() -> StringArray {
    let data: Vec<&str> = vec![
        "1997-01-31T09:26:56.123Z",
        "1997-01-31T09:26:56.123-05:00",
        "1997-01-31 09:26:56.123-05:00",
        "2023-01-01 04:05:06.789-08",
        "1997-01-31T09:26:56.123",
        "1997-01-31 09:26:56.123",
        "1997-01-31 09:26:56",
        "1997-01-31 13:26:56",
        "1997-01-31 13:26:56+04:00",
        "1997-01-31",
    ];

    StringArray::from(data)
}

fn data_with_formats() -> (
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
    StringArray,
) {
    let mut inputs = StringBuilder::new();
    let mut format1_builder = StringBuilder::with_capacity(2, 10);
    let mut format2_builder = StringBuilder::with_capacity(2, 10);
    let mut format3_builder = StringBuilder::with_capacity(2, 10);
    let mut jiff_format1_builder = StringBuilder::with_capacity(2, 10);
    let mut jiff_format2_builder = StringBuilder::with_capacity(2, 10);
    let mut jiff_format3_builder = StringBuilder::with_capacity(2, 10);

    inputs.append_value("1997-01-31T09:26:56UTC");

    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%+");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%dT%H:%M:%S%Q");

    inputs.append_value("1997-01-31T09:26:56.123-05:00");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%:z");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%:z");

    inputs.append_value("1997-01-31 09:26:56.123-05:00");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f%:z");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f%:z");

    inputs.append_value("2023-01-01 04:05:06.789 -0800");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f %#z");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f %z");

    inputs.append_value("1997-01-31T09:26:56.123");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f");

    inputs.append_value("1997-01-31 09:26:56.123");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f");

    inputs.append_value("1997-01-31 09:26:56");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H:%M:%S");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H:%M:%S");

    inputs.append_value("1997-01-31 092656");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H%M%S");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H%M%S");

    inputs.append_value("1997-01-31 092656+04:00");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%Y-%m-%d %H%M%S%:z");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%Y-%m-%d %H%M%S%:z");

    inputs.append_value("Sun Jul  8 00:34:60 2001");
    format1_builder.append_value("%m");
    format2_builder.append_value("%H");
    format3_builder.append_value("%c");

    jiff_format1_builder.append_value("%m");
    jiff_format2_builder.append_value("%H");
    jiff_format3_builder.append_value("%a %b %e %H:%M:%S %Y");

    (
        inputs.finish(),
        format1_builder.finish(),
        format2_builder.finish(),
        format3_builder.finish(),
        jiff_format1_builder.finish(),
        jiff_format2_builder.finish(),
        jiff_format3_builder.finish(),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    for &parser in ["jiff", "chrono"].iter() {
        c.bench_function(
            &format!("string_to_timestamp_nanos_formatted_single_format_utf8_{parser}"),
            |b| {
                let datetime_parser = match parser {
                    "chrono" => {
                        Box::new(ChronoDateTimeParser::new()) as Box<dyn DateTimeParser>
                    }
                    "jiff" => {
                        Box::new(JiffDateTimeParser::new()) as Box<dyn DateTimeParser>
                    }
                    _ => unreachable!(),
                };

                let (inputs, _, _, format3, _, _, jiffformat3) = data_with_formats();

                b.iter(|| {
                    for (input, format3, jiff_format3) in
                        izip!(inputs.iter(), format3.iter(), jiffformat3.iter())
                    {
                        let _ = black_box(match parser {
                            "chrono" => {
                                let t = datetime_parser
                                    .string_to_timestamp_nanos_formatted(
                                        "UTC",
                                        input.unwrap(),
                                        &[format3.unwrap()],
                                    );
                                if t.is_err() {
                                    println!("Error: {t:?}");
                                }

                                t
                            }
                            "jiff" => {
                                let t = datetime_parser
                                    .string_to_timestamp_nanos_formatted(
                                        "UTC",
                                        input.unwrap(),
                                        &[jiff_format3.unwrap()],
                                    );
                                if t.is_err() {
                                    println!("Error: {t:?}");
                                }

                                t
                            }
                            _ => unreachable!(),
                        });
                    }
                })
            },
        );

        c.bench_function(
            &format!(
                "string_to_timestamp_nanos_formatted_multiple_formats_utf8_{parser}"
            ),
            |b| {
                let datetime_parser = match parser {
                    "chrono" => {
                        Box::new(ChronoDateTimeParser::new()) as Box<dyn DateTimeParser>
                    }
                    "jiff" => {
                        Box::new(JiffDateTimeParser::new()) as Box<dyn DateTimeParser>
                    }
                    _ => unreachable!(),
                };

                let (
                    inputs,
                    format1,
                    format2,
                    format3,
                    jiff_format1,
                    jiff_format2,
                    jiff_format3,
                ) = data_with_formats();

                b.iter(|| {
                    for (
                        input,
                        format1,
                        format2,
                        format3,
                        jiff_format1,
                        jiff_format2,
                        jiff_format3,
                    ) in izip!(
                        inputs.iter(),
                        format1.iter(),
                        format2.iter(),
                        format3.iter(),
                        jiff_format1.iter(),
                        jiff_format2.iter(),
                        jiff_format3.iter()
                    ) {
                        let _ = black_box(match parser {
                            "chrono" => {
                                let t = datetime_parser
                                    .string_to_timestamp_nanos_formatted(
                                        "UTC",
                                        input.unwrap(),
                                        &[
                                            format1.unwrap(),
                                            format2.unwrap(),
                                            format3.unwrap(),
                                        ],
                                    );
                                if t.is_err() {
                                    println!("Error: {t:?}");
                                }

                                t
                            }
                            "jiff" => {
                                let t = datetime_parser
                                    .string_to_timestamp_nanos_formatted(
                                        "UTC",
                                        input.unwrap(),
                                        &[
                                            jiff_format1.unwrap(),
                                            jiff_format2.unwrap(),
                                            jiff_format3.unwrap(),
                                        ],
                                    );
                                if t.is_err() {
                                    println!("Error: {t:?}");
                                }

                                t
                            }
                            _ => unreachable!(),
                        });
                    }
                })
            },
        );

        let return_field =
            Field::new("f", DataType::Timestamp(TimeUnit::Nanosecond, None), true).into();
        let arg_field = Field::new("a", DataType::Utf8, false).into();
        let arg_fields = vec![arg_field];

        c.bench_function(&format!("to_timestamp_no_formats_utf8_{parser}"), |b| {
            let mut config_options = ConfigOptions::default();
            if parser == "jiff" {
                config_options.execution.date_time_parser = Some("jiff".to_string());
            }
            let config = Arc::new(config_options.clone());
            let to_timestamp_udf = to_timestamp(&config_options);
            let to_timestamp_udf = Arc::clone(&to_timestamp_udf);
            let arr_data = data();
            let batch_len = arr_data.len();
            let string_array = ColumnarValue::Array(Arc::new(arr_data) as ArrayRef);

            b.iter(|| {
                black_box(
                    to_timestamp_udf
                        .invoke_with_args(ScalarFunctionArgs {
                            args: vec![string_array.clone()],
                            arg_fields: arg_fields.clone(),
                            number_rows: batch_len,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config),
                        })
                        .expect("to_timestamp should work on valid values"),
                )
            })
        });

        c.bench_function(
            &format!("to_timestamp_no_formats_largeutf8_{parser}"),
            |b| {
                let mut config_options = ConfigOptions::default();
                if parser == "jiff" {
                    config_options.execution.date_time_parser = Some("jiff".to_string());
                }
                let config = Arc::new(config_options.clone());
                let to_timestamp_udf = to_timestamp(&config_options);
                let to_timestamp_udf = Arc::clone(&to_timestamp_udf);
                let data = cast(&data(), &DataType::LargeUtf8).unwrap();
                let batch_len = data.len();
                let string_array = ColumnarValue::Array(Arc::new(data) as ArrayRef);

                b.iter(|| {
                    black_box(
                        to_timestamp_udf
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![string_array.clone()],
                                arg_fields: arg_fields.clone(),
                                number_rows: batch_len,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config),
                            })
                            .expect("to_timestamp should work on valid values"),
                    )
                })
            },
        );

        c.bench_function(&format!("to_timestamp_no_formats_utf8view_{parser}"), |b| {
            let mut config_options = ConfigOptions::default();
            if parser == "jiff" {
                config_options.execution.date_time_parser = Some("jiff".to_string());
            }
            let config = Arc::new(config_options.clone());
            let to_timestamp_udf = to_timestamp(&config_options);
            let data = cast(&data(), &DataType::Utf8View).unwrap();
            let batch_len = data.len();
            let string_array = ColumnarValue::Array(Arc::new(data) as ArrayRef);

            b.iter(|| {
                black_box(
                    to_timestamp_udf
                        .invoke_with_args(ScalarFunctionArgs {
                            args: vec![string_array.clone()],
                            arg_fields: arg_fields.clone(),
                            number_rows: batch_len,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config),
                        })
                        .expect("to_timestamp should work on valid values"),
                )
            })
        });

        c.bench_function(&format!("to_timestamp_with_formats_utf8_{parser}"), |b| {
            let mut config_options = ConfigOptions::default();
            if parser == "jiff" {
                config_options.execution.date_time_parser = Some("jiff".to_string());
            }
            let config = Arc::new(config_options.clone());
            let to_timestamp_udf = to_timestamp(&config_options);
            let to_timestamp_udf = Arc::clone(&to_timestamp_udf);
            let (
                inputs,
                format1,
                format2,
                format3,
                jiffformat1,
                jiffformat2,
                jiffformat3,
            ) = data_with_formats();
            let batch_len = inputs.len();

            let args = match parser {
                "chrono" => vec![
                    ColumnarValue::Array(Arc::new(inputs) as ArrayRef),
                    ColumnarValue::Array(Arc::new(format1) as ArrayRef),
                    ColumnarValue::Array(Arc::new(format2) as ArrayRef),
                    ColumnarValue::Array(Arc::new(format3) as ArrayRef),
                ],
                "jiff" => {
                    vec![
                        ColumnarValue::Array(Arc::new(inputs) as ArrayRef),
                        ColumnarValue::Array(Arc::new(jiffformat1) as ArrayRef),
                        ColumnarValue::Array(Arc::new(jiffformat2) as ArrayRef),
                        ColumnarValue::Array(Arc::new(jiffformat3) as ArrayRef),
                    ]
                }
                _ => unreachable!(),
            };

            let arg_fields = args
                .iter()
                .enumerate()
                .map(|(idx, arg)| {
                    Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                })
                .collect::<Vec<_>>();

            b.iter(|| {
                black_box(
                    to_timestamp_udf
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: batch_len,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config),
                        })
                        .expect("to_timestamp should work on valid values"),
                )
            })
        });

        c.bench_function(
            &format!("to_timestamp_with_formats_largeutf8_{parser}"),
            |b| {
                let mut config_options = ConfigOptions::default();
                if parser == "jiff" {
                    config_options.execution.date_time_parser = Some("jiff".to_string());
                }
                let config = Arc::new(config_options.clone());
                let to_timestamp_udf = to_timestamp(&config_options);
                let to_timestamp_udf = Arc::clone(&to_timestamp_udf);
                let (
                    inputs,
                    format1,
                    format2,
                    format3,
                    jiffformat1,
                    jiffformat2,
                    jiffformat3,
                ) = data_with_formats();
                let batch_len = inputs.len();

                let args = match parser {
                    "chrono" => vec![
                        ColumnarValue::Array(Arc::new(
                            cast(&inputs, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format1, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format2, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format3, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                    ],
                    "jiff" => vec![
                        ColumnarValue::Array(Arc::new(
                            cast(&inputs, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat1, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat2, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat3, &DataType::LargeUtf8).unwrap(),
                        ) as ArrayRef),
                    ],
                    _ => unreachable!(),
                };
                let arg_fields = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect::<Vec<_>>();

                b.iter(|| {
                    black_box(
                        to_timestamp_udf
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: batch_len,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config),
                            })
                            .expect("to_timestamp should work on valid values"),
                    )
                })
            },
        );

        c.bench_function(
            &format!("to_timestamp_with_formats_utf8view_{parser}"),
            |b| {
                let mut config_options = ConfigOptions::default();
                if parser == "jiff" {
                    config_options.execution.date_time_parser = Some("jiff".to_string());
                }
                let config = Arc::new(config_options.clone());
                let to_timestamp_udf = to_timestamp(&config_options);
                let to_timestamp_udf = Arc::clone(&to_timestamp_udf);
                let (
                    inputs,
                    format1,
                    format2,
                    format3,
                    jiffformat1,
                    jiffformat2,
                    jiffformat3,
                ) = data_with_formats();

                let batch_len = inputs.len();

                let args = match parser {
                    "chrono" => vec![
                        ColumnarValue::Array(Arc::new(
                            cast(&inputs, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format1, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format2, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&format3, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                    ],
                    "jiff" => vec![
                        ColumnarValue::Array(Arc::new(
                            cast(&inputs, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat1, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat2, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                        ColumnarValue::Array(Arc::new(
                            cast(&jiffformat3, &DataType::Utf8View).unwrap(),
                        ) as ArrayRef),
                    ],
                    _ => unreachable!(),
                };
                let arg_fields = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect::<Vec<_>>();

                b.iter(|| {
                    black_box(
                        to_timestamp_udf
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: batch_len,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config),
                            })
                            .expect("to_timestamp should work on valid values"),
                    )
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
