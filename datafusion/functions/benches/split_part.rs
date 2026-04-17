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

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::string::split_part;
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const N_ROWS: usize = 8192;

/// Creates an array of strings with `num_parts` random alphanumeric segments
/// of `part_len` bytes each, joined by `delimiter`.
fn gen_string_array(
    n_rows: usize,
    num_parts: usize,
    part_len: usize,
    delimiter: &str,
    use_string_view: bool,
) -> ColumnarValue {
    let mut rng = StdRng::seed_from_u64(42);

    let mut strings: Vec<String> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let mut parts: Vec<String> = Vec::with_capacity(num_parts);
        for _ in 0..num_parts {
            let part: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(part_len)
                .map(char::from)
                .collect();
            parts.push(part);
        }
        strings.push(parts.join(delimiter));
    }

    if use_string_view {
        let string_array: StringViewArray = strings.into_iter().map(Some).collect();
        ColumnarValue::Array(Arc::new(string_array) as ArrayRef)
    } else {
        let string_array: StringArray = strings.into_iter().map(Some).collect();
        ColumnarValue::Array(Arc::new(string_array) as ArrayRef)
    }
}

#[expect(clippy::too_many_arguments)]
fn bench_split_part(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    func: &ScalarUDF,
    config_options: &Arc<ConfigOptions>,
    name: &str,
    tag: &str,
    strings: ColumnarValue,
    delimiter: ColumnarValue,
    position: ColumnarValue,
) {
    let args = vec![strings, delimiter, position];
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect();
    let return_type = match args[0].data_type() {
        DataType::Utf8View => DataType::Utf8View,
        _ => DataType::Utf8,
    };
    let return_field = Field::new("f", return_type, true).into();

    group.bench_function(BenchmarkId::new(name, tag), |b| {
        b.iter(|| {
            black_box(
                func.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: N_ROWS,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(config_options),
                })
                .expect("split_part should work"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let split_part_func = split_part();
    let config_options = Arc::new(ConfigOptions::default());
    let mut group = c.benchmark_group("split_part");

    // ── Scalar delimiter and position ────────────────

    // Utf8, single-char delimiter, scalar args
    {
        let strings = gen_string_array(N_ROWS, 10, 8, ".", false);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8_single_char",
            "pos_first",
            strings,
            delimiter,
            position,
        );
    }

    {
        let strings = gen_string_array(N_ROWS, 10, 8, ".", false);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(5)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8_single_char",
            "pos_middle",
            strings,
            delimiter,
            position,
        );
    }

    {
        let strings = gen_string_array(N_ROWS, 10, 8, ".", false);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(-1)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8_single_char",
            "pos_negative",
            strings,
            delimiter,
            position,
        );
    }

    // Utf8, multi-char delimiter, scalar args
    {
        let strings = gen_string_array(N_ROWS, 10, 8, "~@~", false);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some("~@~".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(5)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8_multi_char",
            "pos_middle",
            strings,
            delimiter,
            position,
        );
    }

    // Utf8, long strings, scalar args
    {
        let strings = gen_string_array(N_ROWS, 50, 16, ".", false);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(25)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8_long_strings",
            "pos_middle",
            strings,
            delimiter,
            position,
        );
    }

    // Utf8View, long parts, scalar args
    {
        let strings = gen_string_array(N_ROWS, 10, 32, ".", true);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8View(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(5)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8view_long_parts",
            "pos_middle",
            strings,
            delimiter,
            position,
        );
    }

    // Utf8View, very long parts (256 bytes), position 1
    {
        let strings = gen_string_array(N_ROWS, 5, 256, ".", true);
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8View(Some(".".into())));
        let position = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "scalar_utf8view_very_long_parts",
            "pos_first",
            strings,
            delimiter,
            position,
        );
    }

    // ── Array delimiter and position ─────────────────

    // Utf8, single-char delimiter, array args
    {
        let strings = gen_string_array(N_ROWS, 10, 8, ".", false);
        let delimiters: StringArray = vec![Some("."); N_ROWS].into_iter().collect();
        let delimiter = ColumnarValue::Array(Arc::new(delimiters) as ArrayRef);
        let positions = ColumnarValue::Array(Arc::new(Int64Array::from(vec![5; N_ROWS])));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "array_utf8_single_char",
            "pos_middle",
            strings,
            delimiter,
            positions,
        );
    }

    // Utf8, multi-char delimiter, array args
    {
        let strings = gen_string_array(N_ROWS, 10, 8, "~@~", false);
        let delimiters: StringArray = vec![Some("~@~"); N_ROWS].into_iter().collect();
        let delimiter = ColumnarValue::Array(Arc::new(delimiters) as ArrayRef);
        let positions = ColumnarValue::Array(Arc::new(Int64Array::from(vec![5; N_ROWS])));
        bench_split_part(
            &mut group,
            &split_part_func,
            &config_options,
            "array_utf8_multi_char",
            "pos_middle",
            strings,
            delimiter,
            positions,
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
