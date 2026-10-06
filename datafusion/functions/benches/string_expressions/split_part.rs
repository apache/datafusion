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

use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::{Field, Int64Type};
use arrow::util::bench_util::create_primitive_array_range;
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_functions::string::split_part;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;

/// How the position argument is passed.
enum Position {
    /// The same position for every row, as in `split_part(s, '.', 2)`.
    Scalar(i64),
    /// A different position for each row, drawn from the range.
    PerRow(Range<i64>),
}

/// Returns strings of alphanumeric fields joined by `delimiter`. Each row's
/// number of fields and each field's length are drawn from the given ranges.
fn create_strings(
    field_len: &RangeInclusive<usize>,
    num_fields: &RangeInclusive<usize>,
    delimiter: &str,
) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..BATCH_SIZE)
        .map(|_| {
            if rng.random::<f32>() < 0.1 {
                return None;
            }
            let fields: Vec<String> = (0..rng.random_range(num_fields.clone()))
                .map(|_| {
                    let len = rng.random_range(field_len.clone());
                    (&mut rng)
                        .sample_iter(&Alphanumeric)
                        .take(len)
                        .map(char::from)
                        .collect()
                })
                .collect();
            Some(fields.join(delimiter))
        })
        .collect()
}

fn create_args(
    field_len: &RangeInclusive<usize>,
    num_fields: &RangeInclusive<usize>,
    delimiter: &str,
    position: &Position,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let strings = create_strings(field_len, num_fields, delimiter);
    let string_arg = if is_string_view {
        ColumnarValue::Array(Arc::new(strings.into_iter().collect::<StringViewArray>()))
    } else {
        ColumnarValue::Array(Arc::new(strings.into_iter().collect::<StringArray>()))
    };

    let position_arg = match position {
        Position::Scalar(n) => ColumnarValue::Scalar(ScalarValue::Int64(Some(*n))),
        Position::PerRow(range) => ColumnarValue::Array(Arc::new(
            create_primitive_array_range::<Int64Type>(BATCH_SIZE, 0.0, range.clone()),
        )),
    };

    vec![
        string_arg,
        ColumnarValue::Scalar(ScalarValue::from(delimiter)),
        position_arg,
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    // Field lengths and counts vary within each case, as in real data: with
    // fixed lengths, per-row work that depends on them is unrealistically
    // predictable.
    let cases = [
        // Fields of up to 8 bytes, stored inline in StringView arrays (≤12 bytes).
        ("short_fields", 1..=8, 3..=6, ".", Position::Scalar(2)),
        // Fields of more than 12 bytes, stored out of line.
        ("long_fields", 16..=48, 3..=6, ".", Position::Scalar(2)),
        // A field near the middle of many fields.
        ("many_fields", 1..=16, 20..=50, ".", Position::Scalar(10)),
        // A negative position counts fields from the end.
        (
            "negative_position",
            1..=32,
            3..=6,
            ".",
            Position::Scalar(-1),
        ),
        // A delimiter of more than one character.
        (
            "multi_char_delimiter",
            1..=32,
            3..=6,
            "~@~",
            Position::Scalar(2),
        ),
        // The position computed per row.
        (
            "per_row_position",
            1..=32,
            3..=6,
            ".",
            Position::PerRow(1..4),
        ),
    ];
    let function = split_part();
    let config_options = Arc::new(ConfigOptions::default());
    let mut group = c.benchmark_group(function.name().to_string());

    for is_string_view in [false, true] {
        let array_type = if is_string_view {
            "string_view"
        } else {
            "string"
        };

        for (case_name, field_len, num_fields, delimiter, position) in &cases {
            let args =
                create_args(field_len, num_fields, delimiter, position, is_string_view);

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
