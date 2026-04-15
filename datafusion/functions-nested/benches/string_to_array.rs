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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::string::StringToArray;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1000;
const SEED: u64 = 42;

fn criterion_benchmark(c: &mut Criterion) {
    // Single-char delimiter
    let comma = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
    bench_string_to_array(
        c,
        "string_to_array_single_char_delim",
        create_csv_strings,
        &comma,
        None,
    );

    // Multi-char delimiter
    let double_colon = ColumnarValue::Scalar(ScalarValue::Utf8(Some("::".to_string())));
    bench_string_to_array(
        c,
        "string_to_array_multi_char_delim",
        create_multi_delim_strings,
        &double_colon,
        None,
    );

    // With null_str argument
    let null_str = ColumnarValue::Scalar(ScalarValue::Utf8(Some("NULL".to_string())));
    bench_string_to_array(
        c,
        "string_to_array_with_null_str",
        create_csv_strings_with_nulls,
        &comma,
        Some(&null_str),
    );

    // NULL delimiter
    let null_delim = ColumnarValue::Scalar(ScalarValue::Utf8(None));
    bench_string_to_array(
        c,
        "string_to_array_null_delim",
        create_short_strings,
        &null_delim,
        None,
    );

    // Columnar delimiter (fall-back path)
    bench_string_to_array_columnar_delim(c);
}

fn bench_string_to_array_columnar_delim(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_to_array_columnar_delim");

    for &num_elements in &[5, 20, 100] {
        let string_array = create_csv_strings(num_elements);
        let delimiter_array: ArrayRef =
            Arc::new(StringArray::from(vec![Some(","); NUM_ROWS]));

        let args = vec![
            ColumnarValue::Array(string_array.clone()),
            ColumnarValue::Array(delimiter_array),
        ];
        let arg_fields = vec![
            Field::new("str", DataType::Utf8, true).into(),
            Field::new("delimiter", DataType::Utf8, false).into(),
        ];

        let return_field = Field::new(
            "result",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(num_elements),
            &num_elements,
            |b, _| {
                let udf = StringToArray::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: NUM_ROWS,
                            return_field: return_field.clone().into(),
                            config_options: Arc::new(ConfigOptions::default()),
                        })
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_string_to_array(
    c: &mut Criterion,
    group_name: &str,
    make_strings: fn(usize) -> ArrayRef,
    delimiter: &ColumnarValue,
    null_str: Option<&ColumnarValue>,
) {
    let mut group = c.benchmark_group(group_name);

    for &num_elements in &[5, 20, 100] {
        let string_array = make_strings(num_elements);

        let mut args = vec![
            ColumnarValue::Array(string_array.clone()),
            delimiter.clone(),
        ];
        let mut arg_fields = vec![
            Field::new("str", DataType::Utf8, true).into(),
            Field::new("delimiter", DataType::Utf8, true).into(),
        ];
        if let Some(ns) = null_str {
            args.push(ns.clone());
            arg_fields.push(Field::new("null_str", DataType::Utf8, true).into());
        }

        let return_field = Field::new(
            "result",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(num_elements),
            &num_elements,
            |b, _| {
                let udf = StringToArray::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: NUM_ROWS,
                            return_field: return_field.clone().into(),
                            config_options: Arc::new(ConfigOptions::default()),
                        })
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

/// Creates strings like "val1,val2,val3,...,valN" with `num_elements` elements.
fn create_csv_strings(num_elements: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let strings: StringArray = (0..NUM_ROWS)
        .map(|_| {
            let parts: Vec<String> = (0..num_elements)
                .map(|_| format!("val{}", rng.random_range(0..1000)))
                .collect();
            Some(parts.join(","))
        })
        .collect();
    Arc::new(strings)
}

/// Creates strings like "val1::val2::val3::...::valN".
fn create_multi_delim_strings(num_elements: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let strings: StringArray = (0..NUM_ROWS)
        .map(|_| {
            let parts: Vec<String> = (0..num_elements)
                .map(|_| format!("val{}", rng.random_range(0..1000)))
                .collect();
            Some(parts.join("::"))
        })
        .collect();
    Arc::new(strings)
}

/// Creates CSV strings where ~10% of elements are the literal "NULL".
fn create_csv_strings_with_nulls(num_elements: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let strings: StringArray = (0..NUM_ROWS)
        .map(|_| {
            let parts: Vec<String> = (0..num_elements)
                .map(|_| {
                    if rng.random::<f64>() < 0.1 {
                        "NULL".to_string()
                    } else {
                        format!("val{}", rng.random_range(0..1000))
                    }
                })
                .collect();
            Some(parts.join(","))
        })
        .collect();
    Arc::new(strings)
}

/// Creates short strings (length = `num_chars`) for the NULL-delimiter
/// (split-into-characters) benchmark.
fn create_short_strings(num_chars: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let strings: StringArray = (0..NUM_ROWS)
        .map(|_| {
            let s: String = (0..num_chars)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            Some(s)
        })
        .collect();
    Arc::new(strings)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
