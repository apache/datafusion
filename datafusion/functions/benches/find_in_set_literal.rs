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

//! Benchmarks the `find_in_set(column, constant_list)` path where the set is a
//! scalar literal. A long list exercises the pre-built lookup; a short list
//! stays on the per-row linear scan.

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const N_ROWS: usize = 8192;

/// Builds a string column whose values are drawn from `entries` plus a small
/// fraction of misses, so both hits and misses are exercised.
fn build_column(entries: &[String]) -> StringArray {
    let mut rng = StdRng::seed_from_u64(42);
    let values: Vec<Option<String>> = (0..N_ROWS)
        .map(|_| {
            let r = rng.random::<f32>();
            if r < 0.1 {
                None
            } else if r < 0.4 {
                Some("__miss__".to_string())
            } else {
                let idx = rng.random_range(0..entries.len());
                Some(entries[idx].clone())
            }
        })
        .collect();
    StringArray::from(values)
}

fn bench_case(c: &mut Criterion, label: &str, num_entries: usize) {
    let find_in_set = datafusion_functions::unicode::find_in_set();
    let entries: Vec<String> = (0..num_entries).map(|i| format!("item{i}")).collect();
    let list = entries.join(",");

    let column = build_column(&entries);
    let args = vec![
        ColumnarValue::Array(Arc::new(column)),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(list))),
    ];
    let arg_fields = args
        .iter()
        .map(|arg| Field::new("a", arg.data_type().clone(), true).into())
        .collect::<Vec<_>>();
    let return_field = Arc::new(Field::new("f", DataType::Int32, true));
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_with_input(
        BenchmarkId::new("find_in_set_literal", label),
        &num_entries,
        |b, _| {
            b.iter(|| {
                black_box(find_in_set.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: N_ROWS,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                }))
            })
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    // Short list stays on the linear scan (below the lookup threshold).
    bench_case(c, "short_list_4", 4);
    // Long lists exercise the pre-built lookup.
    bench_case(c, "long_list_64", 64);
    bench_case(c, "long_list_256", 256);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
