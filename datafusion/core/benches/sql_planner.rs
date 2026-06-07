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

use arrow::array::PrimitiveArray;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use criterion::Bencher;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::{ScalarValue, config::Dialect};
use datafusion_expr::col;
use rand_distr::num_traits::NumCast;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use test_utils::TableDef;
use test_utils::tpcds::tpcds_schemas;
use test_utils::tpch::tpch_schemas;
use tokio::runtime::Runtime;

const BENCHMARKS_PATH_1: &str = "../../benchmarks/";
const BENCHMARKS_PATH_2: &str = "./benchmarks/";
const CLICKBENCH_DATA_PATH: &str = "data/hits_partitioned/";

/// Create a logical plan from the specified sql (parse + analyze only, NO optimization)
fn logical_plan(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    black_box(rt.block_on(ctx.sql(sql)).unwrap());
}

/// Parse SQL and run the analyzer to get an analyzed (but unoptimized) LogicalPlan.
/// This is the input to the optimizer.
fn analyzed_plan(
    ctx: &SessionContext,
    rt: &Runtime,
    sql: &str,
) -> datafusion_expr::LogicalPlan {
    let state = ctx.state();
    let plan = rt.block_on(state.create_logical_plan(sql)).unwrap();
    state
        .analyzer()
        .execute_and_check(plan, state.config().options(), |_, _| {})
        .unwrap()
}

/// Run ONLY the optimizer on a pre-analyzed plan. Measures optimizer cost in isolation.
fn optimize_plan(ctx: &SessionContext, plan: &datafusion_expr::LogicalPlan) {
    let state = ctx.state();
    black_box(
        state
            .optimizer()
            .optimize(plan.clone(), &state, |_, _| {})
            .unwrap(),
    );
}

/// Create a physical ExecutionPlan (by way of logical plan)
fn physical_plan(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    black_box(rt.block_on(async {
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

/// Create a table provider with a struct column: `id` (Int32) and `props` (Struct { value: Int32, label: Utf8 })
fn create_struct_table_provider() -> Arc<MemTable> {
    let struct_fields = Fields::from(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("props", DataType::Struct(struct_fields), true),
    ]));
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
    ctx.register_table("struct_t1", create_struct_table_provider())
        .unwrap();
    ctx.register_table("struct_t2", create_struct_table_provider())
        .unwrap();
    ctx
}

/// Register the table definitions as a MemTable with the context and return the
/// context
fn register_defs(ctx: SessionContext, defs: Vec<TableDef>) -> SessionContext {
    for TableDef {
        name,
        schema,
        constraints,
    } in defs
    {
        ctx.register_table(
            &name,
            Arc::new(
                MemTable::try_new(Arc::new(schema), vec![vec![]])
                    .unwrap()
                    .with_constraints(constraints),
            ),
        )
        .unwrap();
    }
    ctx
}

fn register_clickbench_hits_table(rt: &Runtime) -> SessionContext {
    let ctx = SessionContext::new();

    // use an external table for clickbench benchmarks
    let path =
        if PathBuf::from(format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")).exists() {
            format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")
        } else {
            format!("{BENCHMARKS_PATH_2}{CLICKBENCH_DATA_PATH}")
        };

    let sql =
        format!("CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '{path}'");

    // ClickBench partitioned dataset was written by an ancient version of pyarrow that
    // that wrote strings with the wrong logical type. To read it correctly, we must
    // automatically convert binary to string.
    rt.block_on(ctx.sql("SET datafusion.execution.parquet.binary_as_string  = true;"))
        .unwrap();
    rt.block_on(ctx.sql(&sql)).unwrap();

    // ClickBench stores EventDate as UInt16 (days since 1970-01-01). Create a view
    // that exposes it as SQL DATE so that queries comparing it with date literals
    // (e.g. "EventDate >= '2013-07-01'") work correctly during planning.
    rt.block_on(ctx.sql(
        "CREATE VIEW hits AS \
         SELECT * EXCEPT (\"EventDate\"), \
                CAST(CAST(\"EventDate\" AS INTEGER) AS DATE) AS \"EventDate\" \
         FROM hits_raw",
    ))
    .unwrap();

    let count =
        rt.block_on(async { ctx.table("hits").await.unwrap().count().await.unwrap() });
    assert!(count > 0);
    ctx
}

/// Target of this benchmark: control that placeholders replacing does not get slower,
/// if the query does not contain placeholders at all.
fn benchmark_with_param_values_many_columns(
    ctx: &SessionContext,
    rt: &Runtime,
    b: &mut Bencher,
) {
    const COLUMNS_NUM: usize = 200;
    let mut aggregates = String::new();
    for i in 0..COLUMNS_NUM {
        if i > 0 {
            aggregates.push_str(", ");
        }
        aggregates.push_str(format!("MAX(a{i})").as_str());
    }
    // SELECT max(attr0), ..., max(attrN) FROM t1.
    let query = format!("SELECT {aggregates} FROM t1");
    let statement = ctx
        .state()
        .sql_to_statement(&query, &Dialect::Generic)
        .unwrap();
    let plan =
        rt.block_on(async { ctx.state().statement_to_plan(statement).await.unwrap() });
    b.iter(|| {
        let plan = plan.clone();
        black_box(plan.with_param_values(vec![ScalarValue::from(1)]).unwrap());
    });
}

/// Registers a table like this:
/// c0,c1,c2...,c99
/// 0,100...9900
/// 0,200...19800
/// 0,300...29700
fn register_union_order_table_generic<T>(
    ctx: &SessionContext,
    num_columns: usize,
    num_rows: usize,
) where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp + NumCast,
{
    let iter = (0..num_columns).map(|i| {
        let array_data: Vec<T::Native> = (0..num_rows)
            .map(|j| {
                let value = (j as u64) * 100 + (i as u64);
                <T::Native as NumCast>::from(value).unwrap_or_else(|| {
                    panic!("Failed to cast numeric value to Native type")
                })
            })
            .collect();

        // Use PrimitiveArray which is generic over the ArrowPrimitiveType T
        let array: ArrayRef = Arc::new(PrimitiveArray::<T>::from_iter_values(array_data));

        (format!("c{i}"), array)
    });

    let batch = RecordBatch::try_from_iter(iter).unwrap();
    let schema = batch.schema();
    let partitions = vec![vec![batch]];

    // tell DataFusion that the table is sorted by all columns
    let sort_order = (0..num_columns)
        .map(|i| col(format!("c{i}")).sort(true, true))
        .collect::<Vec<_>>();

    // create the table
    let table = MemTable::try_new(schema, partitions)
        .unwrap()
        .with_sort_order(vec![sort_order]);

    ctx.register_table("t", Arc::new(table)).unwrap();
}
/// return a query like
/// ```sql
/// select c1, 2 as c2, ... n as cn from t ORDER BY c1
///   UNION ALL
/// select 1 as c1, c2, ... n as cn from t ORDER BY c2
/// ...
/// select 1 as c1, 2 as c2, ... cn from t ORDER BY cn
///  ORDER BY c1, c2 ... CN
/// ```
fn union_orderby_query(n: usize) -> String {
    let mut query = String::new();
    for i in 0..n {
        if i != 0 {
            query.push_str("\n  UNION ALL \n");
        }
        let select_list = (0..n)
            .map(|j| {
                if i == j {
                    format!("c{j}")
                } else {
                    format!("{j} as c{j}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        query.push_str(&format!("(SELECT {select_list} FROM t ORDER BY c{i})"));
    }
    query.push_str(&format!(
        "\nORDER BY {}",
        (0..n)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    query
}

fn criterion_benchmark(c: &mut Criterion) {
    // verify that we can load the clickbench data prior to running the benchmark
    if !PathBuf::from(format!("{BENCHMARKS_PATH_1}{CLICKBENCH_DATA_PATH}")).exists()
        && !PathBuf::from(format!("{BENCHMARKS_PATH_2}{CLICKBENCH_DATA_PATH}")).exists()
    {
        panic!(
            "benchmarks/data/hits_partitioned/ could not be loaded. Please run \
         'benchmarks/bench.sh data clickbench_partitioned' prior to running this benchmark"
        )
    }

    let ctx = create_context();
    let rt = Runtime::new().unwrap();

    // Test simplest
    // https://github.com/apache/datafusion/issues/5157
    c.bench_function("logical_select_one_from_700", |b| {
        b.iter(|| logical_plan(&ctx, &rt, "SELECT c1 FROM t700"))
    });

    // Test simplest
    // https://github.com/apache/datafusion/issues/5157
    c.bench_function("physical_select_one_from_700", |b| {
        b.iter(|| physical_plan(&ctx, &rt, "SELECT c1 FROM t700"))
    });

    // Test simplest
    c.bench_function("logical_select_all_from_1000", |b| {
        b.iter(|| logical_plan(&ctx, &rt, "SELECT * FROM t1000"))
    });

    // Test simplest
    c.bench_function("physical_select_all_from_1000", |b| {
        b.iter(|| physical_plan(&ctx, &rt, "SELECT * FROM t1000"))
    });

    c.bench_function("logical_trivial_join_low_numbered_columns", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT t1.a2, t2.b2  \
                 FROM t1, t2 WHERE a1 = b1",
            )
        })
    });

    c.bench_function("logical_trivial_join_high_numbered_columns", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT t1.a99, t2.b99  \
                 FROM t1, t2 WHERE a199 = b199",
            )
        })
    });

    c.bench_function("logical_aggregate_with_join", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
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
            aggregates.push_str(format!("MAX(a{i})").as_str());
        }
        let query = format!("SELECT {aggregates} FROM t1");
        b.iter(|| {
            physical_plan(&ctx, &rt, &query);
        });
    });

    // It was observed in production that queries with window functions sometimes partition over more than 30 columns
    for partitioning_columns in [4, 7, 8, 12, 30] {
        c.bench_function(
            &format!(
                "physical_window_function_partition_by_{partitioning_columns}_on_values"
            ),
            |b| {
                let source = format!(
                    "SELECT 1 AS n{}",
                    (0..partitioning_columns)
                        .map(|i| format!(", {i} AS c{i}"))
                        .collect::<String>()
                );
                let window = format!(
                    "SUM(n) OVER (PARTITION BY {}) AS sum_n",
                    (0..partitioning_columns)
                        .map(|i| format!("c{i}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let query = format!("SELECT {window} FROM ({source})");
                b.iter(|| {
                    physical_plan(&ctx, &rt, &query);
                });
            },
        );
    }

    // Benchmark for Physical Planning Joins
    c.bench_function("physical_join_consider_sort", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                &rt,
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
                &rt,
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
                &rt,
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
                &rt,
                "SELECT t1.a7  \
                 FROM t1 WHERE a7 = (SELECT b8 FROM t2)",
            );
        });
    });

    c.bench_function("physical_intersection", |b| {
        b.iter(|| {
            physical_plan(
                &ctx,
                &rt,
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
                &rt,
                "SELECT DISTINCT t1.a7  \
                 FROM t1, t2 WHERE t1.a7 = t2.b8",
            );
        });
    });

    let struct_agg_sort_query = "SELECT \
         struct_t1.props['label'], \
         SUM(struct_t1.props['value']), \
         MAX(struct_t2.props['value']), \
         COUNT(*) \
     FROM struct_t1 \
     JOIN struct_t2 ON struct_t1.id = struct_t2.id \
     WHERE struct_t1.props['value'] > 50 \
     GROUP BY struct_t1.props['label'] \
     ORDER BY SUM(struct_t1.props['value']) DESC";

    // -- Struct column benchmarks --
    c.bench_function("logical_plan_struct_join_agg_sort", |b| {
        b.iter(|| logical_plan(&ctx, &rt, struct_agg_sort_query))
    });
    c.bench_function("physical_plan_struct_join_agg_sort", |b| {
        b.iter(|| physical_plan(&ctx, &rt, struct_agg_sort_query))
    });

    // -- Sorted Queries --
    // 100, 200 && 300 is taking too long - https://github.com/apache/datafusion/issues/18366
    // Logical Plan for datatype Int64 and UInt64 differs, UInt64 Logical Plan's Union are wrapped
    // up in Projection, and EliminateNestedUnion OptimezerRule is not applied leading to significantly
    // longer execution time.
    // https://github.com/apache/datafusion/issues/17261

    for column_count in [10, 50 /* 100, 200, 300 */] {
        register_union_order_table_generic::<arrow::datatypes::Int64Type>(
            &ctx,
            column_count,
            1000,
        );

        // this query has many expressions in its sort order so stresses
        // order equivalence validation
        c.bench_function(
            &format!("physical_sorted_union_order_by_{column_count}_int64"),
            |b| {
                // SELECT ... UNION ALL ...
                let query = union_orderby_query(column_count);
                b.iter(|| physical_plan(&ctx, &rt, &query))
            },
        );

        let _ = ctx.deregister_table("t");
    }

    for column_count in [10, 50 /* 100, 200, 300 */] {
        register_union_order_table_generic::<arrow::datatypes::UInt64Type>(
            &ctx,
            column_count,
            1000,
        );
        c.bench_function(
            &format!("physical_sorted_union_order_by_{column_count}_uint64"),
            |b| {
                // SELECT ... UNION ALL ...
                let query = union_orderby_query(column_count);
                b.iter(|| physical_plan(&ctx, &rt, &query))
            },
        );

        let _ = ctx.deregister_table("t");
    }

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
        c.bench_function(&format!("physical_plan_tpch_{q}"), |b| {
            b.iter(|| physical_plan(&tpch_ctx, &rt, &sql))
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
                physical_plan(&tpch_ctx, &rt, sql)
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
                physical_plan(&tpcds_ctx, &rt, sql)
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
    let clickbench_queries = (0..=42)
        .map(|q| {
            std::fs::read_to_string(format!(
                "{benchmarks_path}queries/clickbench/queries/q{q}.sql"
            ))
            .unwrap()
        })
        .chain((0..=7).map(|q| {
            std::fs::read_to_string(format!(
                "{benchmarks_path}queries/clickbench/extended/q{q}.sql"
            ))
            .unwrap()
        }))
        .collect::<Vec<_>>();

    let clickbench_ctx = register_clickbench_hits_table(&rt);

    // for (i, sql) in clickbench_queries.iter().enumerate() {
    //     c.bench_function(&format!("logical_plan_clickbench_q{}", i + 1), |b| {
    //         b.iter(|| logical_plan(&clickbench_ctx, sql))
    //     });
    // }

    for (i, sql) in clickbench_queries.iter().enumerate() {
        c.bench_function(&format!("physical_plan_clickbench_q{}", i + 1), |b| {
            b.iter(|| physical_plan(&clickbench_ctx, &rt, sql))
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
                physical_plan(&clickbench_ctx, &rt, sql)
            }
        })
    });

    c.bench_function("with_param_values_many_columns", |b| {
        benchmark_with_param_values_many_columns(&ctx, &rt, b);
    });

    // ==========================================================================
    // Optimizer-focused benchmarks
    // These benchmarks are designed to stress the logical optimizer with
    // varying plan sizes, expression counts, and node type distributions.
    // ==========================================================================

    // --- Deep join trees (many plan nodes, few expressions) ---
    // Tests optimizer traversal cost as plan node count grows.
    // Each join adds ~3 nodes (Join, TableScan, CrossJoin/Filter).

    // Register additional tables for join benchmarks
    for i in 3..=16 {
        ctx.register_table(format!("j{i}"), create_table_provider("x", 10))
            .unwrap();
    }

    c.bench_function("logical_join_chain_4", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT j3.x0 FROM j3 \
                 JOIN j4 ON j3.x0 = j4.x0 \
                 JOIN j5 ON j4.x0 = j5.x0 \
                 JOIN j6 ON j5.x0 = j6.x0",
            )
        })
    });

    c.bench_function("logical_join_chain_8", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT j3.x0 FROM j3 \
                 JOIN j4 ON j3.x0 = j4.x0 \
                 JOIN j5 ON j4.x0 = j5.x0 \
                 JOIN j6 ON j5.x0 = j6.x0 \
                 JOIN j7 ON j6.x0 = j7.x0 \
                 JOIN j8 ON j7.x0 = j8.x0 \
                 JOIN j9 ON j8.x0 = j9.x0 \
                 JOIN j10 ON j9.x0 = j10.x0",
            )
        })
    });

    c.bench_function("logical_join_chain_16", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT j3.x0 FROM j3 \
                 JOIN j4 ON j3.x0 = j4.x0 \
                 JOIN j5 ON j4.x0 = j5.x0 \
                 JOIN j6 ON j5.x0 = j6.x0 \
                 JOIN j7 ON j6.x0 = j7.x0 \
                 JOIN j8 ON j7.x0 = j8.x0 \
                 JOIN j9 ON j8.x0 = j9.x0 \
                 JOIN j10 ON j9.x0 = j10.x0 \
                 JOIN j11 ON j10.x0 = j11.x0 \
                 JOIN j12 ON j11.x0 = j12.x0 \
                 JOIN j13 ON j12.x0 = j13.x0 \
                 JOIN j14 ON j13.x0 = j14.x0 \
                 JOIN j15 ON j14.x0 = j15.x0 \
                 JOIN j16 ON j15.x0 = j16.x0 \
                 JOIN j3 AS j3b ON j16.x0 = j3b.x0 \
                 JOIN j4 AS j4b ON j3b.x0 = j4b.x0",
            )
        })
    });

    // --- Wide expressions (few plan nodes, many expressions) ---
    // Tests expression processing overhead in optimizer rules like
    // SimplifyExpressions, CommonSubexprEliminate, OptimizeProjections.

    // Many WHERE clauses (filter expressions)
    {
        let predicates: Vec<String> = (0..50).map(|i| format!("a{i} > 0")).collect();
        let query = format!("SELECT a0 FROM t1 WHERE {}", predicates.join(" AND "));
        c.bench_function("logical_wide_filter_50_predicates", |b| {
            b.iter(|| logical_plan(&ctx, &rt, &query))
        });
    }

    {
        let predicates: Vec<String> = (0..200).map(|i| format!("a{i} > 0")).collect();
        let query = format!("SELECT a0 FROM t1 WHERE {}", predicates.join(" AND "));
        c.bench_function("logical_wide_filter_200_predicates", |b| {
            b.iter(|| logical_plan(&ctx, &rt, &query))
        });
    }

    // Many aggregate expressions
    {
        let aggs: Vec<String> =
            (0..50).map(|i| format!("SUM(a{i}), AVG(a{i})")).collect();
        let query = format!("SELECT {} FROM t1", aggs.join(", "));
        c.bench_function("logical_wide_aggregate_100_exprs", |b| {
            b.iter(|| logical_plan(&ctx, &rt, &query))
        });
    }

    // Many CASE WHEN expressions (complex expressions)
    {
        let cases: Vec<String> = (0..50)
            .map(|i| {
                format!("CASE WHEN a{i} > 0 THEN a{i} * 2 ELSE a{i} + 1 END AS r{i}")
            })
            .collect();
        let query = format!("SELECT {} FROM t1", cases.join(", "));
        c.bench_function("logical_wide_case_50_exprs", |b| {
            b.iter(|| logical_plan(&ctx, &rt, &query))
        });
    }

    // --- Mixed: deep plan + wide expressions ---
    // This is the worst case for optimizer: many nodes AND many expressions.

    c.bench_function("logical_join_4_with_agg_and_filter", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT j3.x0, SUM(j4.x1), AVG(j5.x2), COUNT(j6.x3), \
                        MIN(j3.x4), MAX(j4.x5) \
                 FROM j3 \
                 JOIN j4 ON j3.x0 = j4.x0 \
                 JOIN j5 ON j4.x0 = j5.x0 \
                 JOIN j6 ON j5.x0 = j6.x0 \
                 WHERE j3.x1 > 0 AND j4.x2 < 100 AND j5.x3 != j6.x4 \
                 GROUP BY j3.x0 \
                 HAVING SUM(j4.x1) > 10 \
                 ORDER BY j3.x0",
            )
        })
    });

    c.bench_function("logical_join_8_with_agg_sort_limit", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT j3.x0, j4.x1, j5.x2, \
                        SUM(j6.x3), AVG(j7.x4), COUNT(j8.x5), \
                        MIN(j9.x6), MAX(j10.x7) \
                 FROM j3 \
                 JOIN j4 ON j3.x0 = j4.x0 \
                 JOIN j5 ON j4.x0 = j5.x0 \
                 JOIN j6 ON j5.x0 = j6.x0 \
                 JOIN j7 ON j6.x0 = j7.x0 \
                 JOIN j8 ON j7.x0 = j8.x0 \
                 JOIN j9 ON j8.x0 = j9.x0 \
                 JOIN j10 ON j9.x0 = j10.x0 \
                 WHERE j3.x1 > 0 AND j5.x2 < 100 \
                 GROUP BY j3.x0, j4.x1, j5.x2 \
                 ORDER BY j3.x0 DESC \
                 LIMIT 100",
            )
        })
    });

    // --- Subqueries (trigger decorrelation rules) ---
    // Tests rules like DecorrelatePredicateSubquery, ScalarSubqueryToJoin.

    c.bench_function("logical_correlated_subquery_exists", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, a1 FROM t1 \
                 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b0 = t1.a0)",
            )
        })
    });

    c.bench_function("logical_correlated_subquery_in", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, a1 FROM t1 \
                 WHERE a0 IN (SELECT b0 FROM t2 WHERE t2.b1 = t1.a1)",
            )
        })
    });

    c.bench_function("logical_scalar_subquery", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, (SELECT MAX(b1) FROM t2 WHERE t2.b0 = t1.a0) AS max_b \
                 FROM t1",
            )
        })
    });

    c.bench_function("logical_multiple_subqueries", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, a1 FROM t1 \
                 WHERE a0 IN (SELECT b0 FROM t2 WHERE b1 > 0) \
                   AND EXISTS (SELECT 1 FROM t2 WHERE t2.b0 = t1.a0 AND t2.b1 < 100) \
                   AND a1 > (SELECT AVG(b1) FROM t2)",
            )
        })
    });

    // --- UNION queries (test OptimizeUnions, PropagateEmptyRelation) ---

    c.bench_function("logical_union_4_branches", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, a1 FROM t1 WHERE a0 > 0 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 10 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 20 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 30",
            )
        })
    });

    c.bench_function("logical_union_8_branches", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "SELECT a0, a1 FROM t1 WHERE a0 > 0 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 10 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 20 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 30 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 40 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 50 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 60 \
                 UNION ALL SELECT a0, a1 FROM t1 WHERE a0 > 70",
            )
        })
    });

    // --- DISTINCT (test ReplaceDistinctWithAggregate) ---

    c.bench_function("logical_distinct_many_columns", |b| {
        let cols: Vec<String> = (0..50).map(|i| format!("a{i}")).collect();
        let query = format!("SELECT DISTINCT {} FROM t1", cols.join(", "));
        b.iter(|| logical_plan(&ctx, &rt, &query))
    });

    // --- Nested views / CTEs (deeper plan trees) ---

    c.bench_function("logical_nested_cte_4_levels", |b| {
        b.iter(|| {
            logical_plan(
                &ctx,
                &rt,
                "WITH \
                   cte1 AS (SELECT a0, a1, a2 FROM t1 WHERE a0 > 0), \
                   cte2 AS (SELECT a0, a1 FROM cte1 WHERE a1 > 0), \
                   cte3 AS (SELECT a0 FROM cte2 WHERE a0 < 100), \
                   cte4 AS (SELECT a0, COUNT(*) AS cnt FROM cte3 GROUP BY a0) \
                 SELECT * FROM cte4 ORDER BY a0 LIMIT 10",
            )
        })
    });

    // --- TPC-H logical plans (uncommented from existing code) ---
    // These test real-world query patterns with moderate plan complexity.

    c.bench_function("logical_plan_tpch_all", |b| {
        b.iter(|| {
            for sql in &all_tpch_sql_queries {
                logical_plan(&tpch_ctx, &rt, sql)
            }
        })
    });

    c.bench_function("logical_plan_tpcds_all", |b| {
        b.iter(|| {
            for sql in &all_tpcds_sql_queries {
                logical_plan(&tpcds_ctx, &rt, sql)
            }
        })
    });

    // ==========================================================================
    // Optimizer-only benchmarks
    // These measure ONLY the optimizer, not SQL parsing or analysis.
    // Plans are pre-parsed and pre-analyzed in setup, then only optimization
    // is measured in the benchmark loop.
    // ==========================================================================

    // Simple select (baseline: few nodes, few expressions)
    {
        let plan = analyzed_plan(&ctx, &rt, "SELECT c1 FROM t700");
        c.bench_function("optimizer_select_one_from_700", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Wide select (many expressions, few nodes)
    {
        let plan = analyzed_plan(&ctx, &rt, "SELECT * FROM t1000");
        c.bench_function("optimizer_select_all_from_1000", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Deep join chains (many nodes, few expressions)
    {
        let plan = analyzed_plan(
            &ctx,
            &rt,
            "SELECT j3.x0 FROM j3 \
             JOIN j4 ON j3.x0 = j4.x0 \
             JOIN j5 ON j4.x0 = j5.x0 \
             JOIN j6 ON j5.x0 = j6.x0",
        );
        c.bench_function("optimizer_join_chain_4", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    {
        let plan = analyzed_plan(
            &ctx,
            &rt,
            "SELECT j3.x0 FROM j3 \
             JOIN j4 ON j3.x0 = j4.x0 \
             JOIN j5 ON j4.x0 = j5.x0 \
             JOIN j6 ON j5.x0 = j6.x0 \
             JOIN j7 ON j6.x0 = j7.x0 \
             JOIN j8 ON j7.x0 = j8.x0 \
             JOIN j9 ON j8.x0 = j9.x0 \
             JOIN j10 ON j9.x0 = j10.x0",
        );
        c.bench_function("optimizer_join_chain_8", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Wide filter (many expressions)
    {
        let predicates: Vec<String> = (0..200).map(|i| format!("a{i} > 0")).collect();
        let query = format!("SELECT a0 FROM t1 WHERE {}", predicates.join(" AND "));
        let plan = analyzed_plan(&ctx, &rt, &query);
        c.bench_function("optimizer_wide_filter_200", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Wide aggregate (many expressions)
    {
        let aggs: Vec<String> =
            (0..50).map(|i| format!("SUM(a{i}), AVG(a{i})")).collect();
        let query = format!("SELECT {} FROM t1", aggs.join(", "));
        let plan = analyzed_plan(&ctx, &rt, &query);
        c.bench_function("optimizer_wide_aggregate_100", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Subquery (tests decorrelation rules)
    {
        let plan = analyzed_plan(
            &ctx,
            &rt,
            "SELECT a0, a1 FROM t1 \
             WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b0 = t1.a0)",
        );
        c.bench_function("optimizer_correlated_exists", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // Mixed: joins + aggregates + filter
    {
        let plan = analyzed_plan(
            &ctx,
            &rt,
            "SELECT j3.x0, SUM(j4.x1), AVG(j5.x2), COUNT(j6.x3), \
                    MIN(j3.x4), MAX(j4.x5) \
             FROM j3 \
             JOIN j4 ON j3.x0 = j4.x0 \
             JOIN j5 ON j4.x0 = j5.x0 \
             JOIN j6 ON j5.x0 = j6.x0 \
             WHERE j3.x1 > 0 AND j4.x2 < 100 AND j5.x3 != j6.x4 \
             GROUP BY j3.x0 \
             HAVING SUM(j4.x1) > 10 \
             ORDER BY j3.x0",
        );
        c.bench_function("optimizer_join_4_with_agg_filter", |b| {
            b.iter(|| optimize_plan(&ctx, &plan))
        });
    }

    // TPC-H all queries (optimizer only)
    {
        let plans: Vec<_> = all_tpch_sql_queries
            .iter()
            .map(|sql| analyzed_plan(&tpch_ctx, &rt, sql))
            .collect();
        c.bench_function("optimizer_tpch_all", |b| {
            b.iter(|| {
                for plan in &plans {
                    optimize_plan(&tpch_ctx, plan)
                }
            })
        });
    }

    // TPC-DS all queries (optimizer only)
    {
        let plans: Vec<_> = all_tpcds_sql_queries
            .iter()
            .map(|sql| analyzed_plan(&tpcds_ctx, &rt, sql))
            .collect();
        c.bench_function("optimizer_tpcds_all", |b| {
            b.iter(|| {
                for plan in &plans {
                    optimize_plan(&tpcds_ctx, plan)
                }
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
