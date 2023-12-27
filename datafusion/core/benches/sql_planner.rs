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
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

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
pub fn create_schema(column_prefix: &str, num_columns: usize) -> Schema {
    let fields: Fields = (0..num_columns)
        .map(|i| Field::new(format!("{column_prefix}{i}"), DataType::Int32, true))
        .collect();
    Schema::new(fields)
}

pub fn create_table_provider(column_prefix: &str, num_columns: usize) -> Arc<MemTable> {
    let schema = Arc::new(create_schema(column_prefix, num_columns));
    MemTable::try_new(schema, vec![]).map(Arc::new).unwrap()
}

pub fn create_tpch_schemas() -> [(String, Schema); 2] {
    let lineitem_schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int32, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]);

    let orders_schema = Schema::new(vec![
        Field::new("o_orderkey", DataType::Int32, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]);

    return [
        ("lineitem".to_string(), lineitem_schema),
        ("orders".to_string(), orders_schema),
    ];
}

fn create_context() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table("t1", create_table_provider("a", 200))
        .unwrap();
    ctx.register_table("t2", create_table_provider("b", 200))
        .unwrap();
    ctx.register_table("t700", create_table_provider("c", 700))
        .unwrap();

    let tpch_schemas = create_tpch_schemas();
    tpch_schemas.iter().for_each(|(name, schema)| {
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(Arc::new(schema.clone()), vec![]).unwrap()),
        )
        .unwrap();
    });

    ctx
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = create_context();

    // Test simplest
    // https://github.com/apache/arrow-datafusion/issues/5157
    c.bench_function("logical_select_one_from_700", |b| {
        b.iter(|| logical_plan(&ctx, "SELECT c1 FROM t700"))
    });

    // Test simplest
    // https://github.com/apache/arrow-datafusion/issues/5157
    c.bench_function("physical_select_one_from_700", |b| {
        b.iter(|| physical_plan(&ctx, "SELECT c1 FROM t700"))
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

    let q1_sql = std::fs::read_to_string("../../benchmarks/queries/q1.sql").unwrap();
    c.bench_function("physical_plan_tpch_q1", |b| {
        b.iter(|| physical_plan(&ctx, &q1_sql))
    });

    let q12_sql = std::fs::read_to_string("../../benchmarks/queries/q12.sql").unwrap();
    c.bench_function("physical_plan_tpch_q12", |b| {
        b.iter(|| physical_plan(&ctx, &q12_sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
