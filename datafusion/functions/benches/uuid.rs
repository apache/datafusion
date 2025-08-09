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

use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_functions::string;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let uuid = string::uuid();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("uuid", |b| {
        b.iter(|| {
            black_box(uuid.invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 1024,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::clone(&config_options),
            }))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
