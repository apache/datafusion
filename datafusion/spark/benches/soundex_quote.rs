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

use arrow::array::{ArrayRef, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_spark::function::string::{quote, soundex};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NULL_DENSITY: f32 = 0.2;

/// Words of varying length, so soundex sees both codes it truncates and codes it pads.
fn random_words(size: usize) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                return None;
            }
            let len = rng.random_range(3..12);
            Some(
                (0..len)
                    .map(|_| rng.random_range(b'a'..=b'z') as char)
                    .collect(),
            )
        })
        .collect()
}

/// Sentences that mostly contain no quote at all, plus a minority that do, so the
/// escaping path is exercised without dominating.
fn random_quotable(size: usize) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(7);
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                return None;
            }
            let len = rng.random_range(8..40);
            let has_quote = rng.random::<f32>() < 0.25;
            Some(
                (0..len)
                    .map(|i| {
                        if has_quote && i % 11 == 0 {
                            '\''
                        } else {
                            rng.random_range(b'a'..=b'z') as char
                        }
                    })
                    .collect(),
            )
        })
        .collect()
}

fn bench(c: &mut Criterion, name: &str, func: Arc<ScalarUDF>, input: ArrayRef) {
    let size = input.len();
    let args = vec![ColumnarValue::Array(input)];
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
                    return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 8192] {
        let words = random_words(size);
        bench(
            c,
            &format!("soundex/utf8/size={size}"),
            soundex(),
            Arc::new(words.iter().cloned().collect::<StringArray>()),
        );
        bench(
            c,
            &format!("soundex/utf8view/size={size}"),
            soundex(),
            Arc::new(
                words
                    .iter()
                    .map(|s| s.as_deref())
                    .collect::<StringViewArray>(),
            ),
        );

        let quotable = random_quotable(size);
        bench(
            c,
            &format!("quote/utf8/size={size}"),
            quote(),
            Arc::new(quotable.iter().cloned().collect::<StringArray>()),
        );
        bench(
            c,
            &format!("quote/utf8view/size={size}"),
            quote(),
            Arc::new(
                quotable
                    .iter()
                    .map(|s| s.as_deref())
                    .collect::<StringViewArray>(),
            ),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
