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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn plan(ctx: Arc<Mutex<SessionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    criterion::black_box(rt.block_on(ctx.lock().sql(sql)).unwrap());
}

/// Create schema representing a large table
pub fn create_schema(column_prefix: &str) -> Schema {
    let fields = (0..200)
        .map(|i| Field::new(&format!("{}{}", column_prefix, i), DataType::Int32, true))
        .collect();
    Schema::new(fields)
}

pub fn create_table_provider(column_prefix: &str) -> Result<Arc<MemTable>> {
    let schema = Arc::new(create_schema(column_prefix));
    MemTable::try_new(schema, vec![]).map(Arc::new)
}

fn create_context() -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    ctx.register_table("t1", create_table_provider("a")?)?;
    ctx.register_table("t2", create_table_provider("b")?)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = create_context().unwrap();

    c.bench_function("trivial join low numbered columns", |b| {
        b.iter(|| {
            plan(
                ctx.clone(),
                "SELECT t1.a2, t2.b2  \
                 FROM t1, t2 WHERE a1 = b1",
            )
        })
    });

    c.bench_function("trivial join high numbered columns", |b| {
        b.iter(|| {
            plan(
                ctx.clone(),
                "SELECT t1.a99, t2.b99  \
                 FROM t1, t2 WHERE a199 = b199",
            )
        })
    });

    c.bench_function("aggregate with join", |b| {
        b.iter(|| {
            plan(
                ctx.clone(),
                "SELECT t1.a99, MIN(t2.b1), MAX(t2.b199), AVG(t2.b123), COUNT(t2.b73)  \
                 FROM t1 JOIN t2 ON t1.a199 = t2.b199 GROUP BY t1.a99",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
