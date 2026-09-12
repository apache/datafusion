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

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_spark::function::url::url_decode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NULL_DENSITY: f32 = 0.2;

/// Percent-escapes that appear in real encoded input.
const ESCAPES: &[&str] = &["%20", "%2F", "%3A", "%3F", "%26", "%3D", "%7E"];

/// Builds encoded strings where `escape_ratio` of the segments are percent-escapes.
///
/// `escape_ratio == 0.0` produces input that needs no decoding at all, which is
/// the case where the decoded value can borrow rather than allocate.
fn random_encoded(size: usize, seed: u64, escape_ratio: f32) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                return None;
            }
            let segments = rng.random_range(4..12);
            let mut s = String::new();
            for _ in 0..segments {
                if rng.random::<f32>() < escape_ratio {
                    s.push_str(ESCAPES[rng.random_range(0..ESCAPES.len())]);
                } else {
                    for _ in 0..rng.random_range(2..8) {
                        s.push(rng.random_range(b'a'..=b'z') as char);
                    }
                }
            }
            Some(s)
        })
        .collect()
}

fn bench(c: &mut Criterion, name: &str, input: ArrayRef) {
    let func = url_decode();
    let size = input.len();
    let return_type = input.data_type().clone();
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
                    return_field: Arc::new(Field::new("f", return_type.clone(), true)),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 8192] {
        // Nothing to unescape: the decoded value can borrow its input.
        let plain = random_encoded(size, 42, 0.0);
        bench(
            c,
            &format!("url_decode/plain_utf8/size={size}"),
            Arc::new(plain.iter().cloned().collect::<StringArray>()),
        );
        bench(
            c,
            &format!("url_decode/plain_utf8view/size={size}"),
            Arc::new(
                plain
                    .iter()
                    .map(|s| s.as_deref())
                    .collect::<StringViewArray>(),
            ),
        );

        // A realistic mix, where roughly a third of segments need unescaping.
        let escaped = random_encoded(size, 7, 0.33);
        bench(
            c,
            &format!("url_decode/escaped_utf8/size={size}"),
            Arc::new(escaped.iter().cloned().collect::<StringArray>()),
        );
        bench(
            c,
            &format!("url_decode/escaped_largeutf8/size={size}"),
            Arc::new(escaped.iter().cloned().collect::<LargeStringArray>()),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
