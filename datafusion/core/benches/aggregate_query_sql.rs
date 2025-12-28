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
use datafusion::execution::context::SessionContext;
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
    let batch_size = 2048; // 2^11
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();
    let rt = Runtime::new().unwrap();

    c.bench_function("aggregate_query_no_group_by 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_min_max_f64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MIN(f64), MAX(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_wide", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT COUNT(DISTINCT u64_wide) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_narrow", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT COUNT(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_group_by", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64_multiple_keys", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_wide, utf8",
            )
        })
    });

    c.bench_function(
        "aggregate_query_group_by_wide_u64_and_string_without_aggregate_expressions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    &rt,
                    // Due to the large number of distinct values in u64_wide,
                    // this query test the actual grouping performance for more than 1 column
                    "SELECT u64_wide, utf8 \
                 FROM t GROUP BY u64_wide, utf8",
                )
            })
        },
    );

    c.bench_function(
        "aggregate_query_group_by_wide_u64_and_f32_without_aggregate_expressions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    &rt,
                    // Due to the large number of distinct values in u64_wide,
                    // this query test the actual grouping performance for more than 1 column
                    "SELECT u64_wide, f32 \
                 FROM t GROUP BY u64_wide, f32",
                )
            })
        },
    );

    c.bench_function("aggregate_query_approx_percentile_cont_on_u64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, approx_percentile_cont(0.5, 2500) WITHIN GROUP (ORDER BY u64_wide)  \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_approx_percentile_cont_on_f32", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, approx_percentile_cont(0.5, 2500) WITHIN GROUP (ORDER BY f32)  \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_distinct_median", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MEDIAN(DISTINCT u64_wide), MEDIAN(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("first_last_many_columns", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide order by f64, u64_narrow, utf8),\
                            last_value(u64_wide order by f64, u64_narrow, utf8)  \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("first_last_ignore_nulls", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide ignore nulls order by f64, u64_narrow, utf8),  \
                            last_value(u64_wide ignore nulls order by f64, u64_narrow, utf8)    \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("first_last_one_column", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide order by f64), \
                            last_value(u64_wide order by f64)   \
                FROM t GROUP BY u64_narrow",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
