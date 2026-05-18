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

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::substr_index;
use rand::Rng;
use rand::SeedableRng;
use rand::distr::{Alphanumeric, Uniform};
use rand::prelude::Distribution;
use rand::rngs::StdRng;

const ARRAY_DATA_SEED: u64 = 0x5EED_AAAA;
const SCALAR_DATA_SEED: u64 = 0x5EED_BBBB;

struct Filter<Dist, Test> {
    dist: Dist,
    test: Test,
}

impl<T, Dist, Test> Distribution<T> for Filter<Dist, Test>
where
    Dist: Distribution<T>,
    Test: Fn(&T) -> bool,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        loop {
            let x = self.dist.sample(rng);
            if (self.test)(&x) {
                return x;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum StringRep {
    Utf8,
    Utf8View,
}

impl StringRep {
    fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Utf8View => "utf8view",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Utf8View => DataType::Utf8View,
        }
    }

    fn array(self, values: &[String]) -> ArrayRef {
        match self {
            Self::Utf8 => Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Self::Utf8View => Arc::new(
                values
                    .iter()
                    .map(|value| Some(value.as_str()))
                    .collect::<StringViewArray>(),
            ) as ArrayRef,
        }
    }

    fn scalar(self, value: &str) -> ScalarValue {
        match self {
            Self::Utf8 => ScalarValue::Utf8(Some(value.to_string())),
            Self::Utf8View => ScalarValue::Utf8View(Some(value.to_string())),
        }
    }
}

fn random_token<R: Rng + ?Sized>(rng: &mut R, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn array_data(
    batch_size: usize,
    single_char_delimiter: bool,
) -> (Vec<String>, Vec<String>, Vec<i64>) {
    let count_dist = Filter {
        dist: Uniform::new(-4, 5).expect("valid count distribution"),
        test: |x: &i64| x != &0,
    };
    let mut rng = StdRng::seed_from_u64(ARRAY_DATA_SEED);
    let mut strings = Vec::with_capacity(batch_size);
    let mut delimiters = Vec::with_capacity(batch_size);
    let mut counts = Vec::with_capacity(batch_size);

    for _ in 0..batch_size {
        let length = rng.random_range(20..50);
        let base = random_token(&mut rng, length);

        let (string_value, delimiter) = if single_char_delimiter {
            let char_idx = rng.random_range(0..base.chars().count());
            let delimiter = base.chars().nth(char_idx).unwrap().to_string();
            (base, delimiter)
        } else {
            let long_delimiters = ["|||", "***", "&&&", "###", "@@@", "$$$"];
            let delimiter =
                long_delimiters[rng.random_range(0..long_delimiters.len())].to_string();

            let delimiter_count = rng.random_range(1..4);
            let mut result = String::new();
            for i in 0..delimiter_count {
                result.push_str(&base);
                if i < delimiter_count - 1 {
                    result.push_str(&delimiter);
                }
            }
            (result, delimiter)
        };

        strings.push(string_value);
        delimiters.push(delimiter);
        counts.push(count_dist.sample(&mut rng));
    }

    (strings, delimiters, counts)
}

fn scalar_data(batch_size: usize, delimiter: &str) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(SCALAR_DATA_SEED);
    let mut strings = Vec::with_capacity(batch_size);

    for _ in 0..batch_size {
        let left_len = rng.random_range(12..24);
        let middle_len = rng.random_range(12..24);
        let right_len = rng.random_range(12..24);
        let left = random_token(&mut rng, left_len);
        let middle = random_token(&mut rng, middle_len);
        let right = random_token(&mut rng, right_len);
        strings.push(format!("{left}{delimiter}{middle}{delimiter}{right}"));
    }

    strings
}

fn run_benchmark(
    b: &mut criterion::Bencher,
    args: &[ColumnarValue],
    return_type: &DataType,
    number_rows: usize,
) {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            Field::new(format!("arg_{idx}"), arg.data_type().clone(), true).into()
        })
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    b.iter(|| {
        black_box(
            substr_index()
                .invoke_with_args(ScalarFunctionArgs {
                    args: args.to_vec(),
                    arg_fields: arg_fields.clone(),
                    number_rows,
                    return_field: Field::new("f", return_type.clone(), true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("substr_index should work on valid values"),
        )
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("substr_index");
    let batch_sizes = [100, 1000, 10_000];

    for batch_size in batch_sizes {
        for rep in [StringRep::Utf8, StringRep::Utf8View] {
            let rep_name = rep.name();

            group.bench_function(
                format!("substr_index_{rep_name}_{batch_size}_array_single_delimiter"),
                |b| {
                    let (strings, delimiters, counts) = array_data(batch_size, true);
                    let args = vec![
                        ColumnarValue::Array(rep.array(&strings)),
                        ColumnarValue::Array(rep.array(&delimiters)),
                        ColumnarValue::Array(
                            Arc::new(Int64Array::from(counts)) as ArrayRef
                        ),
                    ];
                    run_benchmark(b, &args, &rep.data_type(), batch_size);
                },
            );

            group.bench_function(
                format!("substr_index_{rep_name}_{batch_size}_array_long_delimiter"),
                |b| {
                    let (strings, delimiters, counts) = array_data(batch_size, false);
                    let args = vec![
                        ColumnarValue::Array(rep.array(&strings)),
                        ColumnarValue::Array(rep.array(&delimiters)),
                        ColumnarValue::Array(
                            Arc::new(Int64Array::from(counts)) as ArrayRef
                        ),
                    ];
                    run_benchmark(b, &args, &rep.data_type(), batch_size);
                },
            );

            for (name, delimiter, count) in [
                ("single_delimiter_pos", ".", 1_i64),
                ("single_delimiter_neg", ".", -1_i64),
                ("long_delimiter_pos", "|||", 1_i64),
                ("long_delimiter_neg", "|||", -1_i64),
            ] {
                group.bench_function(
                    format!("substr_index_{rep_name}_{batch_size}_scalar_{name}"),
                    |b| {
                        let strings = scalar_data(batch_size, delimiter);
                        let args = vec![
                            ColumnarValue::Array(rep.array(&strings)),
                            ColumnarValue::Scalar(rep.scalar(delimiter)),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(count))),
                        ];
                        run_benchmark(b, &args, &rep.data_type(), batch_size);
                    },
                );
            }
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
