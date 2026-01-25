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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::substr_index;
use rand::Rng;
use rand::distr::{Alphanumeric, Uniform};
use rand::prelude::Distribution;

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

fn data(
    batch_size: usize,
    single_char_delimiter: bool,
) -> (StringArray, StringArray, Int64Array) {
    let dist = Filter {
        dist: Uniform::new(-4, 5),
        test: |x: &i64| x != &0,
    };
    let mut rng = rand::rng();
    let mut strings: Vec<String> = vec![];
    let mut delimiters: Vec<String> = vec![];
    let mut counts: Vec<i64> = vec![];

    for _ in 0..batch_size {
        let length = rng.random_range(20..50);
        let base: String = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect();

        let (string_value, delimiter): (String, String) = if single_char_delimiter {
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

        let count = rng.sample(dist.dist.unwrap());

        strings.push(string_value);
        delimiters.push(delimiter);
        counts.push(count);
    }

    (
        StringArray::from(strings),
        StringArray::from(delimiters),
        Int64Array::from(counts),
    )
}

fn run_benchmark(
    b: &mut criterion::Bencher,
    strings: StringArray,
    delimiters: StringArray,
    counts: Int64Array,
    batch_size: usize,
) {
    let strings = ColumnarValue::Array(Arc::new(strings) as ArrayRef);
    let delimiters = ColumnarValue::Array(Arc::new(delimiters) as ArrayRef);
    let counts = ColumnarValue::Array(Arc::new(counts) as ArrayRef);

    let args = vec![strings, delimiters, counts];
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
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: batch_size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
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
        group.bench_function(
            format!("substr_index_{batch_size}_single_delimiter"),
            |b| {
                let (strings, delimiters, counts) = data(batch_size, true);
                run_benchmark(b, strings, delimiters, counts, batch_size);
            },
        );

        group.bench_function(format!("substr_index_{batch_size}_long_delimiter"), |b| {
            let (strings, delimiters, counts) = data(batch_size, false);
            run_benchmark(b, strings, delimiters, counts, batch_size);
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
