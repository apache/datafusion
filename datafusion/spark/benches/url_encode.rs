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
use datafusion_spark::function::url::url_encode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NULL_DENSITY: f32 = 0.2;

/// Characters drawn from a set where most are passed through unencoded and a
/// minority need percent-escaping, which is the realistic mix for URL input.
const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_.~ &=/?#%+";

fn random_urls(size: usize) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < NULL_DENSITY {
                return None;
            }
            let len = rng.random_range(10..60);
            Some(
                (0..len)
                    .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
                    .collect(),
            )
        })
        .collect()
}

fn bench(c: &mut Criterion, name: &str, input: ArrayRef) {
    let func = url_encode();
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
        let urls = random_urls(size);
        bench(
            c,
            &format!("url_encode/utf8/size={size}"),
            Arc::new(urls.iter().cloned().collect::<StringArray>()),
        );
        bench(
            c,
            &format!("url_encode/largeutf8/size={size}"),
            Arc::new(urls.iter().cloned().collect::<LargeStringArray>()),
        );
        bench(
            c,
            &format!("url_encode/utf8view/size={size}"),
            Arc::new(
                urls.iter()
                    .map(|s| s.as_deref())
                    .collect::<StringViewArray>(),
            ),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
