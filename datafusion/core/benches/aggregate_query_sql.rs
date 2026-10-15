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

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use criterion::{Criterion, criterion_group, criterion_main};
use data_utils::create_table_provider;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_plan::{collect, displayable};
use datafusion_expr::col;
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

    c.bench_function("string_agg_query_group_by_few_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("string_agg_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    c.bench_function("string_agg_query_group_by_many_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_wide",
            )
        })
    });
}

/// Measure the multi-column fully ordered grouping path, with data generation
/// and sorting outside timing. Both keys vary, and the nullable second key
/// changes type while the shape of the input stays the same.
fn fully_ordered(c: &mut Criterion) {
    use DataType::*;
    use IntervalUnit::{DayTime, MonthDayNano};
    use TimeUnit::{Microsecond, Millisecond, Nanosecond};

    let cases = [
        ("int64", "signed_key", Int64, 31),
        (
            "utf8_view",
            "arrow_cast(lpad(CAST(key_value AS VARCHAR), 4, '0'), 'Utf8View')",
            Utf8View,
            31,
        ),
        (
            "timestamp_ns",
            "arrow_cast(signed_key, 'Timestamp(Nanosecond)')",
            Timestamp(Nanosecond, None),
            31,
        ),
        (
            "timestamp_us_utc",
            "arrow_cast(signed_key, 'Timestamp(Microsecond, Some(\"UTC\"))')",
            Timestamp(Microsecond, Some("UTC".into())),
            31,
        ),
        (
            "date32",
            "arrow_cast(CAST(signed_key AS INTEGER), 'Date32')",
            Date32,
            31,
        ),
        (
            "time32_ms",
            "arrow_cast(CAST(key_value AS INTEGER), 'Time32(Millisecond)')",
            Time32(Millisecond),
            31,
        ),
        (
            "time64_ns",
            "arrow_cast(key_value, 'Time64(Nanosecond)')",
            Time64(Nanosecond),
            31,
        ),
        (
            "duration_ns",
            "arrow_cast(signed_key, 'Duration(Nanosecond)')",
            Duration(Nanosecond),
            31,
        ),
        (
            "interval_day_time",
            "arrow_cast(concat(CAST(signed_key AS VARCHAR), ' days'), 'Interval(DayTime)')",
            Interval(DayTime),
            31,
        ),
        (
            "interval_month_day_nano",
            "arrow_cast(arrow_cast(signed_key, 'Duration(Nanosecond)'), 'Interval(MonthDayNano)')",
            Interval(MonthDayNano),
            31,
        ),
        (
            "decimal128",
            "arrow_cast(concat('123456789012345678901', lpad(CAST(key_value AS VARCHAR), 4, '0'), '.1250'), 'Decimal128(30, 4)')",
            Decimal128(30, 4),
            31,
        ),
        (
            "decimal256",
            "arrow_cast(concat('123456789012345678901234567890123456789012345678901', lpad(CAST(key_value AS VARCHAR), 4, '0'), '.1250'), 'Decimal256(60, 4)')",
            Decimal256(60, 4),
            31,
        ),
        (
            "fixed_binary16",
            "arrow_cast(decode(lpad(to_hex(key_value), 32, '0'), 'hex'), 'FixedSizeBinary(16)')",
            FixedSizeBinary(16),
            31,
        ),
        (
            "fixed_binary7",
            "arrow_cast(decode(lpad(to_hex(key_value), 14, '0'), 'hex'), 'FixedSizeBinary(7)')",
            FixedSizeBinary(7),
            31,
        ),
        (
            "timestamp_ns_singleton",
            "arrow_cast(signed_key, 'Timestamp(Nanosecond)')",
            Timestamp(Nanosecond, None),
            1,
        ),
        (
            "timestamp_ns_long_runs",
            "arrow_cast(signed_key, 'Timestamp(Nanosecond)')",
            Timestamp(Nanosecond, None),
            512,
        ),
    ];

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fully_ordered");
    for (name, key_expr, key_type, run_length) in cases {
        let ctx = rt
            .block_on(fully_ordered_context(key_expr, &key_type, run_length))
            .unwrap();
        group.bench_function(name, |b| {
            b.iter(|| query(ctx.clone(), &rt, FULLY_ORDERED_QUERY))
        });
    }
    group.finish();
}

const FULLY_ORDERED_QUERY: &str = "SELECT k1, k2 FROM ordered_input GROUP BY k1, k2";

async fn fully_ordered_context(
    key_expr: &str,
    key_type: &DataType,
    run_length: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    const ROWS: usize = 2_097_152;
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(8192)
        .with_prefer_existing_sort(true);
    let ctx = SessionContext::new_with_config(config);
    // Runs of 31 cross batch boundaries; singleton and long-run cases vary
    // cardinality without multiplying every type by every input shape.
    let input = ctx
        .sql(&format!(
            "SELECT CAST(group_id / 32 AS INTEGER) AS k1, \
             CASE WHEN key_value = 0 THEN NULL ELSE {key_expr} END AS k2 \
             FROM (SELECT value / {run_length} AS group_id, \
                          value / {run_length} % 32 AS key_value, \
                          value / {run_length} % 32 - 16 AS signed_key \
                   FROM range({ROWS})) \
             ORDER BY k1 ASC NULLS FIRST, k2 ASC NULLS FIRST"
        ))
        .await?
        .create_physical_plan()
        .await?;
    let schema = input.schema();
    assert_eq!(schema.field(1).data_type(), key_type);
    let batches = collect(input, ctx.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        ROWS
    );
    // The setup query sorts both keys before materialization. Retain that
    // known ordering explicitly when registering its results as a table.
    let table = MemTable::try_new(schema, vec![batches])?.with_sort_order(vec![vec![
        col("k1").sort(true, true),
        col("k2").sort(true, true),
    ]]);
    ctx.register_table("ordered_input", Arc::new(table))?;

    let plan = ctx
        .sql(FULLY_ORDERED_QUERY)
        .await?
        .create_physical_plan()
        .await?;
    let description = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        description.contains("group_clustering_mode=Full"),
        "{description}"
    );
    // Input is already sorted in one partition. Fail if the planner adds
    // sorting or repartitioning to the measured query.
    assert!(!description.contains("SortExec"), "{description}");
    assert!(!description.contains("RepartitionExec"), "{description}");
    let result = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        ROWS.div_ceil(run_length)
    );
    Ok(Arc::new(Mutex::new(ctx)))
}

criterion_group!(benches, criterion_benchmark, fully_ordered);
criterion_main!(benches);
