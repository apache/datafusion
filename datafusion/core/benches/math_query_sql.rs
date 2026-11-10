// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::{Criterion, criterion_group, criterion_main};

use parking_lot::Mutex;
use std::sync::Arc;

use tokio::runtime::Runtime;

use arrow::{
    array::{Float32Array, Float64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    // execute the query
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    rt.block_on(df.collect()).unwrap();
}

fn create_context(
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));

    // define data.
    let batches = (0..array_len / batch_size)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                    Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 1048576; // 2^20
    let batch_size = 512; // 2^9
    let ctx = create_context(array_len, batch_size).unwrap();
    let rt = Runtime::new().unwrap();

    c.bench_function("sqrt_20_9", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT sqrt(f32) FROM t"))
    });

    let array_len = 1048576; // 2^20
    let batch_size = 4096; // 2^12
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("sqrt_20_12", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT sqrt(f32) FROM t"))
    });

    let array_len = 4194304; // 2^22
    let batch_size = 4096; // 2^12
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("sqrt_22_12", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT sqrt(f32) FROM t"))
    });

    let array_len = 4194304; // 2^22
    let batch_size = 16384; // 2^14
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("sqrt_22_14", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT sqrt(f32) FROM t"))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
