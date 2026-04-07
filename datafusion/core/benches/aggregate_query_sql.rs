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

mod data_utils;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_utils::{
    Utf8PayloadProfile, create_table_provider, create_table_provider_with_payload,
};
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
    create_context_with_payload(
        partitions_len,
        array_len,
        batch_size,
        Utf8PayloadProfile::Small,
    )
}

fn create_context_with_payload(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
    utf8_payload_profile: Utf8PayloadProfile,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = if matches!(utf8_payload_profile, Utf8PayloadProfile::Small) {
        create_table_provider(partitions_len, array_len, batch_size)?
    } else {
        create_table_provider_with_payload(
            partitions_len,
            array_len,
            batch_size,
            utf8_payload_profile,
        )?
    };
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn payload_label(profile: Utf8PayloadProfile) -> &'static str {
    match profile {
        Utf8PayloadProfile::Small => "small_3b",
        Utf8PayloadProfile::Medium => "medium_64b",
        Utf8PayloadProfile::Large => "large_1024b",
    }
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

    c.bench_function("array_agg_query_group_by_few_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, array_agg(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("array_agg_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, array_agg(f64) \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    c.bench_function("array_agg_query_group_by_many_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, array_agg(f64) \
                 FROM t GROUP BY u64_wide",
            )
        })
    });

    c.bench_function("array_agg_struct_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, array_agg(named_struct('market', dict10, 'price', f64)) \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    // These payload sizes keep the original 4-value cardinality while changing
    // only the bytes copied into grouped `string_agg` state:
    // - small_3b preserves the existing `hi0`..`hi3` baseline
    // - medium_64b makes copy costs measurable without overwhelming the query
    // - large_1024b stresses both CPU and memory behavior
    let string_agg_profiles = [
        Utf8PayloadProfile::Small,
        Utf8PayloadProfile::Medium,
        Utf8PayloadProfile::Large,
    ]
    .into_iter()
    .map(|profile| {
        (
            payload_label(profile),
            create_context_with_payload(partitions_len, array_len, batch_size, profile)
                .unwrap(),
        )
    })
    .collect::<Vec<_>>();

    let string_agg_queries = [
        (
            "few_groups",
            "SELECT u64_narrow, string_agg(utf8, ',') FROM t GROUP BY u64_narrow",
        ),
        (
            "mid_groups",
            "SELECT u64_mid, string_agg(utf8, ',') FROM t GROUP BY u64_mid",
        ),
        (
            "many_groups",
            "SELECT u64_wide, string_agg(utf8, ',') FROM t GROUP BY u64_wide",
        ),
    ];

    let mut string_agg_group = c.benchmark_group("string_agg_payloads");
    for (query_name, sql) in string_agg_queries {
        for (payload_name, payload_ctx) in &string_agg_profiles {
            string_agg_group
                .bench_function(BenchmarkId::new(query_name, payload_name), |b| {
                    b.iter(|| query(payload_ctx.clone(), &rt, sql))
                });
        }
    }
    string_agg_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
