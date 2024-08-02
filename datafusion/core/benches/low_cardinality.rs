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
use arrow_array::{Int64Array, StringArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use futures::executor::block_on;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn query(ctx: &mut SessionContext, sql: &str) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(array_len: usize, batch_size: usize) -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // define data.
    let batches = (0..array_len / batch_size)
        .map(|_i| {
            let data1 = (0..batch_size).into_iter().map(|x| (x % 4 > 1) as i64).collect::<Vec<_>>();	
            let data2 = (0..batch_size).into_iter().map(|j| format!("a{}", (j % 2))).collect::<Vec<_>>();	

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(data1)),
                    Arc::new(StringArray::from(data2)),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 2000000; // 2M rows
    let batch_size = array_len;

    c.bench_function("benchmark", |b| {
        let mut ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| block_on(query(&mut ctx, "select a, b, count(*) from t group by a, b order by count(*) desc limit 10")))
    });
}

criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);

// single-multi-groupby-v2
// Gnuplot not found, using plotters backend
// benchmark               time:   [41.512 ms 41.931 ms 42.318 ms]
//                         change: [-6.5657% -5.6393% -4.6452%] (p = 0.00 < 0.05)
//                         Performance has improved.
// Found 1 outliers among 10 measurements (10.00%)
//   1 (10.00%) high mild