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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::ThreadRng;
use rand::Rng;

use datafusion_common::ScalarValue;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::Expr;
use datafusion_functions_array::planner::ArrayFunctionPlanner;

fn keys(rng: &mut ThreadRng) -> Vec<String> {
    let mut keys = vec![];
    for _ in 0..1000 {
        keys.push(rng.gen_range(0..9999).to_string());
    }
    keys
}

fn values(rng: &mut ThreadRng) -> Vec<i32> {
    let mut values = vec![];
    for _ in 0..1000 {
        values.push(rng.gen_range(0..9999));
    }
    values
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_map_1000", |b| {
        let mut rng = rand::thread_rng();
        let keys = keys(&mut rng);
        let values = values(&mut rng);
        let mut buffer = Vec::new();
        for i in 0..1000 {
            buffer.push(Expr::Literal(ScalarValue::Utf8(Some(keys[i].clone()))));
            buffer.push(Expr::Literal(ScalarValue::Int32(Some(values[i]))));
        }

        let planner = ArrayFunctionPlanner {};

        b.iter(|| {
            black_box(
                planner
                    .plan_make_map(buffer.clone())
                    .expect("map should work on valid values"),
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
