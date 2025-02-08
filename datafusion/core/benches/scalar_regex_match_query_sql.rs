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

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_array::StringArray;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn query(ctx: &SessionContext, sql: &str) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    rt.block_on(df.collect()).unwrap();
}

fn generate_random_string(rng: &mut StdRng, length: usize, charset: &[u8]) -> String {
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..charset.len());
            charset[idx] as char
        })
        .collect()
}

fn create_context(
    batch_iter: usize,
    batch_size: usize,
    string_len: usize,
    rand_seed: u64,
    correct: &str,
) -> Result<SessionContext> {
    let mut rng = StdRng::seed_from_u64(rand_seed);
    let charset = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.,:/\\+-_!@#$%^&*()~'\"{}[]?";

    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));

    // define data.
    let batches = (0..batch_iter)
        .map(|_| {
            let mut array = (0..batch_size - 128)
                .map(|_| Some(generate_random_string(&mut rng, string_len, charset)))
                .collect::<Vec<_>>();
            for _ in 0..128 {
                array.push(Some(correct.to_string()));
            }
            let array = StringArray::from(array);
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
        })
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let batch_iter = 128;
    let batch_size = 4096;
    c.bench_function("test email address pattern", |b| {
        let correct = "test@eaxample.com";
        let sql = "select s from t where s ~ '^[a-zA-Z0-9_\\+\\-]+@[a-zA-Z0-9\\-]+\\.[a-zA-Z]{2,}$'";
        let ctx = create_context(batch_iter, batch_size, 64, 11111, correct).unwrap();
        b.iter(|| query(&ctx, sql))
    });

    c.bench_function("test ip pattern", |b| {
        let correct = "23.7.9.9";
        let ctx = create_context(batch_iter, batch_size, 16, 22222, correct).unwrap();
        let sql = "select s from t where s ~ '^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'";
        b.iter(|| query(&ctx, sql))
    });

    c.bench_function("test phone number pattern", |b| {
        let correct = "1236788899";
        let sql = "select s from t where s ~ '^(\\+\\d{1,2}\\s?)?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$'";
        let ctx = create_context(batch_iter, batch_size, 16, 33333, correct).unwrap();
        b.iter(|| query(&ctx, sql))
    });

    c.bench_function("test html tag pattern", |b| {
        let correct = "<div>Hello World</div>";
        let sql = "select s from t where s ~ '<([a-z1-6]+)>[^<]+</([a-z1-6]+)>'";
        let ctx = create_context(batch_iter, batch_size, 64, 44444, correct).unwrap();
        b.iter(|| query(&ctx, sql))
    });

    c.bench_function("test url pattern", |b| {
        let correct = "https://www.example.com";
        let sql = "select s from t where s ~ '^(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]$'";
        let ctx = create_context(batch_iter, batch_size, 64, 55555, correct).unwrap();
        b.iter(|| query(&ctx, sql))
    });

    c.bench_function("test date pattern", |b| {
        let correct = "2024-02-03";
        let sql = "select s from t where s ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}'";
        let ctx = create_context(batch_iter, batch_size, 16, 66666, correct).unwrap();
        b.iter(|| query(&ctx, sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
