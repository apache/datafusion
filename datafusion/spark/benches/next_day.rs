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

use arrow::array::{Date32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::datetime::next_day::SparkNextDay;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NULL_DENSITY: f32 = 0.2;

/// Day names in mixed case, so the case-insensitive parse is actually exercised.
const DAY_NAMES: &[&str] = &[
    "MO",
    "mon",
    "Monday",
    "TU",
    "tue",
    "WEDNESDAY",
    "Th",
    "fri",
    "SAT",
    "su",
];

fn random_dates(size: usize) -> Date32Array {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                None
            } else {
                // Roughly 1970-2040.
                Some(rng.random_range(0i32..25_000))
            }
        })
        .collect()
}

fn bench(c: &mut Criterion, name: &str, args: Vec<ColumnarValue>, size: usize) {
    let func = SparkNextDay::new();
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                func.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: size,
                    return_field: Arc::new(Field::new("f", DataType::Date32, true)),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 8192] {
        let dates: Date32Array = random_dates(size);

        // The common form: `next_day(col, 'MONDAY')`. The day name is constant,
        // so it need only be parsed once per batch.
        bench(
            c,
            &format!("next_day/scalar_day/size={size}"),
            vec![
                ColumnarValue::Array(Arc::new(dates.clone())),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("MONDAY".to_string()))),
            ],
            size,
        );

        // Both arguments as columns, so the day name is parsed per row.
        let mut rng = StdRng::seed_from_u64(7);
        let days: StringArray = (0..size)
            .map(|_| Some(DAY_NAMES[rng.random_range(0..DAY_NAMES.len())]))
            .collect();
        bench(
            c,
            &format!("next_day/array_day/size={size}"),
            vec![
                ColumnarValue::Array(Arc::new(dates.clone())),
                ColumnarValue::Array(Arc::new(days)),
            ],
            size,
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
