// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod data_utils;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use data_utils::create_table_provider;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{SessionConfig, col};
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
    let array_len = 1024 * 1024;
    let batch_size = 8 * 1024;
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();
    let rt = Runtime::new().unwrap();

    c.bench_function("window empty over, aggregate functions", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
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
                &rt,
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
                &rt,
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
                &rt,
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
                &rt,
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
                    &rt,
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
                &rt,
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
                &rt,
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
                    &rt,
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
                    &rt,
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
                    &rt,
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
                    &rt,
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

/// A table of `SERIES_ROWS` rows sorted by `(k, ts)`, `partition_rows` rows per value of `k`,
/// with `extra_columns` more columns that no window function reads.
fn create_series_context(
    partition_rows: usize,
    extra_columns: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    const SERIES_ROWS: usize = 1024 * 1024;
    const BATCH_ROWS: usize = 8 * 1024;
    let mut fields = vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ];
    fields.extend(
        (0..extra_columns).map(|i| Field::new(format!("c{i}"), DataType::Int64, false)),
    );
    let schema = Arc::new(Schema::new(fields));
    let batches = (0..SERIES_ROWS)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let rows = start as i64..(start + BATCH_ROWS).min(SERIES_ROWS) as i64;
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(
                    rows.clone().map(|i| i / partition_rows as i64),
                )),
                Arc::new(Int64Array::from_iter_values(rows.clone())),
                Arc::new(Int64Array::from_iter_values(
                    rows.clone().map(|i| (i * 7919) % 2000 - 1000),
                )),
            ];
            for c in 0..extra_columns as i64 {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    rows.clone().map(|i| i + c),
                )));
            }
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        })
        .collect::<Result<Vec<_>>>()?;
    let table = MemTable::try_new(schema, vec![batches])?.with_sort_order(vec![vec![
        col("k").sort(true, false),
        col("ts").sort(true, false),
    ]]);
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("s", Arc::new(table))?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn frames_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("window frames");
    group.sample_size(10);

    let over = "PARTITION BY k ORDER BY ts";
    let frame =
        |units: &str| format!("{over} {units} BETWEEN 5 PRECEDING AND 5 FOLLOWING");
    let shapes = [
        ("row_number", format!("row_number() OVER ({over})")),
        ("rank", format!("rank() OVER ({over})")),
        ("lag", format!("lag(v, 1) OVER ({over})")),
        ("sum, default frame", format!("sum(v) OVER ({over})")),
        (
            "sum, rows 5 preceding to 5 following",
            format!("sum(v) OVER ({})", frame("ROWS")),
        ),
        (
            "max, rows 5 preceding to 5 following",
            format!("max(v) OVER ({})", frame("ROWS")),
        ),
        (
            "sum, range 5 preceding to 5 following",
            format!("sum(v) OVER ({})", frame("RANGE")),
        ),
        (
            "sum, whole partition",
            "sum(v) OVER (PARTITION BY k)".to_string(),
        ),
    ];
    for (partition_rows, label) in [(10, "10"), (1000, "1000")] {
        let ctx = create_series_context(partition_rows, 0).unwrap();
        for (name, window) in &shapes {
            let sql = format!("SELECT sum(w) FROM (SELECT {window} AS w FROM s)");
            group.bench_function(format!("partitions of {label} rows, {name}"), |b| {
                b.iter(|| query(ctx.clone(), &rt, &sql))
            });
        }
    }

    // Columns that no window function reads still travel through the operator: every one is counted
    // above the window, so that projection pushdown cannot drop it.
    const EXTRA_COLUMNS: usize = 100;
    let ctx = create_series_context(10, EXTRA_COLUMNS).unwrap();
    let carried = (0..EXTRA_COLUMNS)
        .map(|c| format!("c{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let counted = (0..EXTRA_COLUMNS)
        .map(|c| format!("count(c{c})"))
        .collect::<Vec<_>>()
        .join(", ");
    let groups = format!("{over} GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING");
    let wide = [
        (
            "sum, rows 5 preceding to 5 following",
            format!("sum(v) OVER ({})", frame("ROWS")),
        ),
        (
            "three groups frames",
            format!(
                "sum(v) OVER ({groups}) + max(v) OVER ({groups}) + min(v) OVER ({groups})"
            ),
        ),
    ];
    for (name, window) in &wide {
        let sql = format!(
            "SELECT sum(w), {counted} FROM (SELECT {window} AS w, {carried} FROM s)"
        );
        group.bench_function(format!("{EXTRA_COLUMNS} more columns, {name}"), |b| {
            b.iter(|| query(ctx.clone(), &rt, &sql))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark, frames_benchmark);
criterion_main!(benches);
