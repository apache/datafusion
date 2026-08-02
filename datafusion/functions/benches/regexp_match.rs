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

//! Benchmarks `regexp_match` through `invoke_with_args`, which is how a query
//! plan calls it. The pattern (and flags) are literals, as in
//! `regexp_match(col, '[a-z]+')`.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::regex::regexpmatch::RegexpMatchFunc;
use rand::Rng;
use rand::distr::Alphanumeric;
use rand::rngs::ThreadRng;

const SIZE: usize = 1000;
const PATTERN: &str = ".*([A-Z]{1}).*";

fn data(rng: &mut ThreadRng) -> StringArray {
    (0..SIZE)
        .map(|_| {
            rng.sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .into()
}

fn run(c: &mut Criterion, name: &str, values: &ArrayRef, args: &[ColumnarValue]) {
    let func = RegexpMatchFunc::new();
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect();
    let return_field = Arc::new(Field::new_list(
        "f",
        Field::new_list_field(values.data_type().clone(), true),
        true,
    ));
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                func.invoke_with_args(ScalarFunctionArgs {
                    args: args.to_vec(),
                    arg_fields: arg_fields.clone(),
                    number_rows: SIZE,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("regexp_match should work on valid values"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    let utf8 = Arc::new(data(&mut rng)) as ArrayRef;
    let utf8view = cast(&utf8, &DataType::Utf8View).unwrap();

    run(
        c,
        "regexp_match_1000 literal pattern",
        &utf8,
        &[
            ColumnarValue::Array(Arc::clone(&utf8)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(PATTERN.to_string()))),
        ],
    );

    run(
        c,
        "regexp_match_1000 literal pattern and flags",
        &utf8,
        &[
            ColumnarValue::Array(Arc::clone(&utf8)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(PATTERN.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("i".to_string()))),
        ],
    );

    run(
        c,
        "regexp_match_1000 literal pattern utf8view",
        &utf8view,
        &[
            ColumnarValue::Array(Arc::clone(&utf8view)),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(PATTERN.to_string()))),
        ],
    );

    // Covers the path where the pattern varies per row and so cannot be
    // compiled once for the whole array.
    let patterns = Arc::new(StringArray::from(
        (0..SIZE)
            .map(|i| if i % 2 == 0 { PATTERN } else { "^(A).*" })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    run(
        c,
        "regexp_match_1000 pattern array",
        &utf8,
        &[
            ColumnarValue::Array(Arc::clone(&utf8)),
            ColumnarValue::Array(patterns),
        ],
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
