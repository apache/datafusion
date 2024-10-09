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

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parking_lot::Mutex;
use rand::prelude::ThreadRng;
use rand::Rng;
use tokio::runtime::Runtime;

use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_functions_nested::map::map;

mod data_utils;

fn build_keys(rng: &mut ThreadRng) -> Vec<String> {
    let mut keys = vec![];
    for _ in 0..1000 {
        keys.push(rng.gen_range(0..9999).to_string());
    }
    keys
}

fn build_values(rng: &mut ThreadRng) -> Vec<i32> {
    let mut values = vec![];
    for _ in 0..1000 {
        values.push(rng.gen_range(0..9999));
    }
    values
}

fn t_batch(num: i32) -> RecordBatch {
    let value: Vec<i32> = (0..num).collect();
    let c1: ArrayRef = Arc::new(Int32Array::from(value));
    RecordBatch::try_from_iter(vec![("c1", c1)]).unwrap()
}

fn create_context(num: i32) -> datafusion_common::Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    ctx.register_batch("t", t_batch(num))?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = create_context(1).unwrap();
    let rt = Runtime::new().unwrap();
    let df = rt.block_on(ctx.lock().table("t")).unwrap();

    let mut rng = rand::thread_rng();
    let keys = build_keys(&mut rng);
    let values = build_values(&mut rng);
    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();

    for i in 0..1000 {
        key_buffer.push(Expr::from(ScalarValue::Utf8(Some(keys[i].clone()))));
        value_buffer.push(Expr::from(ScalarValue::Int32(Some(values[i]))));
    }
    c.bench_function("map_1000_1", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(
                    df.clone()
                        .select(vec![map(key_buffer.clone(), value_buffer.clone())])
                        .unwrap()
                        .collect(),
                )
                .unwrap(),
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
