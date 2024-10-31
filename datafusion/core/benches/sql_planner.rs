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

extern crate arrow;
#[macro_use]
extern crate criterion;
extern crate datafusion;

mod data_utils;

use crate::criterion::Criterion;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use test_utils::tpcds::tpcds_schemas;
use test_utils::tpch::tpch_schemas;
use test_utils::TableDef;
use tokio::runtime::Runtime;

const BENCHMARKS_PATH_1: &str = "../../benchmarks/";
const BENCHMARKS_PATH_2: &str = "./benchmarks/";
const CLICKBENCH_DATA_PATH: &str = "data/hits_partitioned/";

/// Create a logical plan from the specified sql
fn logical_plan(ctx: &SessionContext, sql: &str) {
    let rt = Runtime::new().unwrap();
    criterion::black_box(rt.block_on(ctx.sql(sql)).unwrap());
}

/// Create a physical ExecutionPlan (by way of logical plan)
fn physical_plan(ctx: &SessionContext, sql: &str) {
    let rt = Runtime::new().unwrap();
    criterion::black_box(rt.block_on(async {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }));
}

/// Create schema with the specified number of columns
fn create_schema(column_prefix: &str, num_columns: usize) -> Schema {
    let fields: Fields = (0..num_columns)
        .map(|i| Field::new(format!("{column_prefix}{i}"), DataType::Int32, true))
        .collect();
    Schema::new(fields)
}

fn create_table_provider(column_prefix: &str, num_columns: usize) -> Arc<MemTable> {
    let schema = Arc::new(create_schema(column_prefix, num_columns));
    MemTable::try_new(schema, vec![vec![]])
        .map(Arc::new)
        .unwrap()
}

fn create_context() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table("t1", create_table_provider("a", 200))
        .unwrap();
    ctx.register_table("t2", create_table_provider("b", 200))
        .unwrap();
    ctx.register_table("t700", create_table_provider("c", 700))
        .unwrap();
    ctx.register_table("t1000", create_table_provider("d", 1000))
        .unwrap();
    ctx
}

/// Register the table definitions as a MemTable with the context and return the
/// context
fn register_defs(ctx: SessionContext, defs: Vec<TableDef>) -> SessionContext {
    defs.iter().for_each(|TableDef { name, schema }| {
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(Arc::new(schema.clone()), vec![vec![]]).unwrap()),
        )
        .unwrap();
    });
    ctx
}

fn register_clickbench_hits_table() -> SessionContext {
    let ctx = SessionContext::new();
    let rt = Runtime::new().unwrap();

    // use an external table for clickbench benchmarks
    let path =
        if PathBuf::from(format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")).exists() {
            format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")
        } else {
            format!("{BENCHMARKS_PATH_2}{CLICKBENCH_DATA_PATH}")
        };

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{path}'");

    rt.block_on(ctx.sql(&sql)).unwrap();

    let count =
        rt.block_on(async { ctx.table("hits").await.unwrap().count().await.unwrap() });
    assert!(count > 0);
    ctx
}

fn criterion_benchmark(c: &mut Criterion) {
    // verify that we can load the clickbench data prior to running the benchmark
    if !PathBuf::from(format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")).exists()
        && !PathBuf::from(format!("{BENCHMARKS_PATH_2}{CLICKBENCH_DATA_PATH}")).exists()
    {
        panic!("benchmarks/data/hits_partitioned/ could not be loaded. Please run \
         'benchmarks/bench.sh data clickbench_partitioned' prior to running this benchmark")
    }

    let ctx = create_context();

    // Test simplest
    // https://github.com/apache/datafusion/issues/5157
    c.bench_function("logical_select_one_from_700", |b| {
        b.iter(|| logical_plan(&ctx, "SELECT c1 FROM t700"))
    });

    // Test simplest
    // https://github.com/apache/datafusion/issues/5157
    c.bench_function("physical_select_one_from_700", |b| {
        b.iter(|| physical_plan(&ctx, "SELECT c1 FROM t700"))
    });

    // Test simplest
    c.bench_function("logical_select_all_from_1000", |b| {
        b.iter(|| logical_plan(&ctx, "SELECT * FROM t1000"))
    });

    // Test simplest
    c.bench_function("physical_select_all_from_1000", |b| {
        b.iter(|| physical_plan(&ctx, "SELECT * FROM t1000"))
    });

    c.bench_function("logical_trivial_join_low_numbered_columns", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                "SELECT t1.a2, t2.b2  \
                 FROM t1, t2 WHERE a1 = b1",
            )
        })
    });

    c.bench_function("logical_trivial_join_high_numbered_columns", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                "SELECT t1.a99, t2.b99  \
                 FROM t1, t2 WHERE a199 = b199",
            )
        })
    });

    c.bench_function("logical_aggregate_with_join", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                "SELECT t1.a99, MIN(t2.b1), MAX(t2.b199), AVG(t2.b123), COUNT(t2.b73)  \
                 FROM t1 JOIN t2 ON t1.a199 = t2.b199 GROUP BY t1.a99",
            )
        })
    });

    c.bench_function("physical_select_aggregates_from_200", |b| {
        let mut aggregates = String::new();
        for i in 0..200 {
            if i > 0 {
                aggregates.push_str(", ");
            }
            aggregates.push_str(format!("MAX(a{})", i).as_str());
        }
        let query = format!("SELECT {} FROM t1", aggregates);
        b.iter(|| {
            physical_plan(&ctx, &query);
        });
    });

    // Benchmark for Physical Planning Joins
    c.bench_function("physical_join_consider_sort", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                "SELECT t1.a7, t2.b8  \
                 FROM t1, t2 WHERE a7 = b7 \
                 ORDER BY a7",
            );
        });
    });

    c.bench_function("physical_theta_join_consider_sort", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                "SELECT t1.a7, t2.b8  \
                 FROM t1, t2 WHERE a7 < b7 \
                 ORDER BY a7",
            );
        });
    });

    c.bench_function("physical_many_self_joins", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                "SELECT ta.a9, tb.a10, tc.a11, td.a12, te.a13, tf.a14 \
                 FROM t1 AS ta, t1 AS tb, t1 AS tc, t1 AS td, t1 AS te, t1 AS tf \
                 WHERE ta.a9 = tb.a10 AND tb.a10 = tc.a11 AND tc.a11 = td.a12 AND \
                 td.a12 = te.a13 AND te.a13 = tf.a14",
            );
        });
    });

    c.bench_function("physical_unnest_to_join", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                "SELECT t1.a7  \
                 FROM t1 WHERE a7 = (SELECT b8 FROM t2)",
            );
        });
    });

    c.bench_function("physical_intersection", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                "SELECT t1.a7 FROM t1  \
                 INTERSECT SELECT t2.b8 FROM t2",
            );
        });
    });
    // these two queries should be equivalent
    c.bench_function("physical_join_distinct", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                "SELECT DISTINCT t1.a7  \
                 FROM t1, t2 WHERE t1.a7 = t2.b8",
            );
        });
    });

    // --- TPC-H ---

    let tpch_ctx = register_defs(SessionContext::new(), tpch_schemas());

    let tpch_queries = [
        "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11", "q12", "q13",
        "q14", // "q15", q15 has multiple SQL statements which is not supported
        "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ];

    let benchmarks_path = if PathBuf::from(BENCHMARKS_PATH_1).exists() {
        BENCHMARKS_PATH_1
    } else {
        BENCHMARKS_PATH_2
    };

    for q in tpch_queries {
        let sql =
            std::fs::read_to_string(format!("{benchmarks_path}queries/{q}.sql")).unwrap();
        c.bench_function(&format!("physical_plan_tpch_{}", q), |b| {
            b.iter(|| physical_plan(&tpch_ctx, &sql))
        });
    }

    let all_tpch_sql_queries = tpch_queries
        .iter()
        .map(|q| {
            std::fs::read_to_string(format!("{benchmarks_path}queries/{q}.sql")).unwrap()
        })
        .collect::<Vec<_>>();

    c.bench_function("physical_plan_tpch_all", |b| {
        b.iter(|| {
            for sql in &all_tpch_sql_queries {
                physical_plan(&tpch_ctx, sql)
            }
        })
    });

    // c.bench_function("logical_plan_tpch_all", |b| {
    //     b.iter(|| {
    //         for sql in &all_tpch_sql_queries {
    //             logical_plan(&tpch_ctx, sql)
    //         }
    //     })
    // });

    // --- TPC-DS ---

    let tpcds_ctx = register_defs(SessionContext::new(), tpcds_schemas());
    let tests_path = if PathBuf::from("./tests/").exists() {
        "./tests/"
    } else {
        "datafusion/core/tests/"
    };

    let raw_tpcds_sql_queries = (1..100)
        .map(|q| std::fs::read_to_string(format!("{tests_path}tpc-ds/{q}.sql")).unwrap())
        .collect::<Vec<_>>();

    // some queries have multiple statements
    let all_tpcds_sql_queries = raw_tpcds_sql_queries
        .iter()
        .flat_map(|sql| sql.split(';').filter(|s| !s.trim().is_empty()))
        .collect::<Vec<_>>();

    c.bench_function("physical_plan_tpcds_all", |b| {
        b.iter(|| {
            for sql in &all_tpcds_sql_queries {
                physical_plan(&tpcds_ctx, sql)
            }
        })
    });

    // c.bench_function("logical_plan_tpcds_all", |b| {
    //     b.iter(|| {
    //         for sql in &all_tpcds_sql_queries {
    //             logical_plan(&tpcds_ctx, sql)
    //         }
    //     })
    // });

    // -- clickbench --

    let queries_file =
        File::open(format!("{benchmarks_path}queries/clickbench/queries.sql")).unwrap();
    let extended_file =
        File::open(format!("{benchmarks_path}queries/clickbench/extended.sql")).unwrap();

    let clickbench_queries: Vec<String> = BufReader::new(queries_file)
        .lines()
        .chain(BufReader::new(extended_file).lines())
        .map(|l| l.expect("Could not parse line"))
        .collect_vec();

    let clickbench_ctx = register_clickbench_hits_table();

    // for (i, sql) in clickbench_queries.iter().enumerate() {
    //     c.bench_function(&format!("logical_plan_clickbench_q{}", i + 1), |b| {
    //         b.iter(|| logical_plan(&clickbench_ctx, sql))
    //     });
    // }

    for (i, sql) in clickbench_queries.iter().enumerate() {
        c.bench_function(&format!("physical_plan_clickbench_q{}", i + 1), |b| {
            b.iter(|| physical_plan(&clickbench_ctx, sql))
        });
    }

    // c.bench_function("logical_plan_clickbench_all", |b| {
    //     b.iter(|| {
    //         for sql in &clickbench_queries {
    //             logical_plan(&clickbench_ctx, sql)
    //         }
    //     })
    // });

    c.bench_function("physical_plan_clickbench_all", |b| {
        b.iter(|| {
            for sql in &clickbench_queries {
                physical_plan(&clickbench_ctx, sql)
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
