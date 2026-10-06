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
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use arrow::array::StringViewArray;
use arrow::datatypes::{Field, Int64Type};
use arrow::util::bench_util::{
    create_primitive_array_range, create_string_array_with_len_range_and_prefix_and_seed,
};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_functions::unicode::substr;

const BATCH_SIZE: usize = 8192;

/// How an integer argument is passed.
enum IntArg {
    /// The same value for every row, as in `substr(s, 1, 5)`.
    Scalar(i64),
    /// A different value for each row, drawn from the range.
    PerRow(Range<i64>),
}

impl IntArg {
    fn to_columnar_value(&self) -> ColumnarValue {
        match self {
            IntArg::Scalar(value) => {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(*value)))
            }
            IntArg::PerRow(range) => ColumnarValue::Array(Arc::new(
                create_primitive_array_range::<Int64Type>(BATCH_SIZE, 0.0, range.clone()),
            )),
        }
    }
}

fn create_args(
    str_len: &RangeInclusive<usize>,
    start: &IntArg,
    count: Option<&IntArg>,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let strings = create_string_array_with_len_range_and_prefix_and_seed::<i32>(
        BATCH_SIZE,
        0.1,
        *str_len.start(),
        *str_len.end(),
        "",
        42,
    );
    let string_arg = if is_string_view {
        ColumnarValue::Array(Arc::new(strings.iter().collect::<StringViewArray>()))
    } else {
        ColumnarValue::Array(Arc::new(strings))
    };

    let mut args = vec![string_arg, start.to_columnar_value()];
    args.extend(count.map(IntArg::to_columnar_value));
    args
}

fn criterion_benchmark(c: &mut Criterion) {
    // Input lengths vary within each case, as in real data: with fixed-length
    // inputs, per-row work that depends on the input length is unrealistically
    // predictable.
    let cases = [
        // Results of up to 5 chars, stored inline in StringView arrays (≤12 bytes).
        (
            "short_result",
            1..=32,
            IntArg::Scalar(1),
            Some(IntArg::Scalar(5)),
        ),
        // 25-char results, stored out of line.
        (
            "long_result",
            32..=64,
            IntArg::Scalar(5),
            Some(IntArg::Scalar(25)),
        ),
        // Short results from long inputs.
        (
            "short_result_long_input",
            96..=256,
            IntArg::Scalar(1),
            Some(IntArg::Scalar(5)),
        ),
        // No count, so each result runs to the end of the input.
        ("no_count", 1..=64, IntArg::Scalar(5), None),
        // `start` computed per row, as in `substr(s, strpos(s, ':') + 1, 8)`.
        (
            "per_row_start",
            1..=32,
            IntArg::PerRow(1..11),
            Some(IntArg::Scalar(8)),
        ),
    ];
    let function = substr();
    let config_options = Arc::new(ConfigOptions::default());
    let mut group = c.benchmark_group(function.name().to_string());

    for is_string_view in [false, true] {
        let array_type = if is_string_view {
            "string_view"
        } else {
            "string"
        };

        for (case_name, str_len, start, count) in &cases {
            let args = create_args(str_len, start, count.as_ref(), is_string_view);
            let arg_fields: Vec<_> = args
                .iter()
                .enumerate()
                .map(|(idx, arg)| {
                    Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                })
                .collect();
            let scalar_arguments = vec![None; arg_fields.len()];
            let return_field = function
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &arg_fields,
                    scalar_arguments: &scalar_arguments,
                })
                .expect("should resolve return field");

            group.bench_function(format!("{array_type} {case_name}"), |b| {
                b.iter(|| {
                    black_box(
                        function
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: BATCH_SIZE,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("should work"),
                    )
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
