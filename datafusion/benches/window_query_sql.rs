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
use data_utils::create_table_provider;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    let df = ctx.lock().unwrap().sql(sql).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<ExecutionContext>>> {
    let mut ctx = ExecutionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 1024 * 1024;
    let batch_size = 8 * 1024;
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("window empty over, aggregate functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                    MAX(f64) OVER (), \
                    MIN(f32) OVER (), \
                    SUM(u64_narrow) OVER () \
                FROM t",
            )
        })
    });

    c.bench_function("window empty over, built-in functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                    FIRST_VALUE(f64) OVER (), \
                    LAST_VALUE(f32) OVER (), \
                    NTH_VALUE(u64_narrow, 50) OVER () \
                FROM t",
            )
        })
    });

    c.bench_function("window order by, aggregate functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                    MAX(f64) OVER (ORDER BY u64_narrow), \
                    MIN(f32) OVER (ORDER BY u64_narrow DESC), \
                    SUM(u64_narrow) OVER (ORDER BY u64_narrow ASC) \
                FROM t",
            )
        })
    });

    c.bench_function("window order by, built-in functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                  FIRST_VALUE(f64) OVER (ORDER BY u64_narrow), \
                  LAST_VALUE(f32) OVER (ORDER BY u64_narrow DESC), \
                  NTH_VALUE(u64_narrow, 50) OVER (ORDER BY u64_narrow ASC) \
                FROM t",
            )
        })
    });

    c.bench_function("window partition by, u64_wide, aggregate functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                  MAX(f64) OVER (PARTITION BY u64_wide), \
                  MIN(f32) OVER (PARTITION BY u64_wide), \
                  SUM(u64_narrow) OVER (PARTITION BY u64_wide) \
                FROM t",
            )
        })
    });

    c.bench_function(
        "window partition by, u64_narrow, aggregate functions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    "SELECT \
                  MAX(f64) OVER (PARTITION BY u64_narrow), \
                  MIN(f32) OVER (PARTITION BY u64_narrow), \
                  SUM(u64_narrow) OVER (PARTITION BY u64_narrow) \
                FROM t",
                )
            })
        },
    );

    c.bench_function("window partition by, u64_wide, built-in functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                  FIRST_VALUE(f64) OVER (PARTITION BY u64_wide), \
                  LAST_VALUE(f32) OVER (PARTITION BY u64_wide), \
                  NTH_VALUE(u64_narrow, 50) OVER (PARTITION BY u64_wide) \
                FROM t",
            )
        })
    });

    c.bench_function("window partition by, u64_narrow, built-in functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT \
                  FIRST_VALUE(f64) OVER (PARTITION BY u64_narrow), \
                  LAST_VALUE(f32) OVER (PARTITION BY u64_narrow), \
                  NTH_VALUE(u64_narrow, 50) OVER (PARTITION BY u64_narrow) \
                FROM t",
            )
        })
    });

    c.bench_function(
        "window partition and order by, u64_wide, aggregate functions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    "SELECT \
                        MAX(f64) OVER (PARTITION BY u64_wide ORDER by f64), \
                        MIN(f32) OVER (PARTITION BY u64_wide ORDER by f64), \
                        SUM(u64_narrow) OVER (PARTITION BY u64_wide ORDER by f64) \
                    FROM t",
                )
            })
        },
    );

    c.bench_function(
        "window partition and order by, u64_narrow, aggregate functions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    "SELECT \
                        MAX(f64) OVER (PARTITION BY u64_narrow ORDER by f64), \
                        MIN(f32) OVER (PARTITION BY u64_narrow ORDER by f64), \
                        SUM(u64_narrow) OVER (PARTITION BY u64_narrow ORDER by f64) \
                    FROM t",
                )
            })
        },
    );

    c.bench_function(
        "window partition and order by, u64_wide, built-in functions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    "SELECT \
                        FIRST_VALUE(f64) OVER (PARTITION BY u64_wide ORDER by f64), \
                        LAST_VALUE(f32) OVER (PARTITION BY u64_wide ORDER by f64), \
                        NTH_VALUE(u64_narrow, 50) OVER (PARTITION BY u64_wide ORDER by f64) \
                    FROM t",
                )
            })
        },
    );

    c.bench_function(
        "window partition and order by, u64_narrow, built-in functions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    "SELECT \
                        FIRST_VALUE(f64) OVER (PARTITION BY u64_narrow ORDER by f64), \
                        LAST_VALUE(f32) OVER (PARTITION BY u64_narrow ORDER by f64), \
                        NTH_VALUE(u64_narrow, 50) OVER (PARTITION BY u64_narrow ORDER by f64) \
                    FROM t",
                )
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
