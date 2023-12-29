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

pub fn create_tpch_schemas() -> [(String, Schema); 8] {
    let lineitem_schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_tax", DataType::Decimal128(15, 2), false),
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
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]);

    let part_schema = Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
        Field::new("p_comment", DataType::Utf8, false),
    ]);

    let supplier_schema = Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("s_comment", DataType::Utf8, false),
    ]);

    let partsupp_schema = Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]);

    let customer_schema = Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]);

    let nation_schema = Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]);

    let region_schema = Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]);

    return [
        ("lineitem".to_string(), lineitem_schema),
        ("orders".to_string(), orders_schema),
        ("part".to_string(), part_schema),
        ("supplier".to_string(), supplier_schema),
        ("partsupp".to_string(), partsupp_schema),
        ("customer".to_string(), customer_schema),
        ("nation".to_string(), nation_schema),
        ("region".to_string(), region_schema),
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
    let q2_sql = std::fs::read_to_string("../../benchmarks/queries/q2.sql").unwrap();
    let q3_sql = std::fs::read_to_string("../../benchmarks/queries/q3.sql").unwrap();
    let q4_sql = std::fs::read_to_string("../../benchmarks/queries/q4.sql").unwrap();
    let q5_sql = std::fs::read_to_string("../../benchmarks/queries/q5.sql").unwrap();
    let q6_sql = std::fs::read_to_string("../../benchmarks/queries/q6.sql").unwrap();
    let q7_sql = std::fs::read_to_string("../../benchmarks/queries/q7.sql").unwrap();
    let q8_sql = std::fs::read_to_string("../../benchmarks/queries/q8.sql").unwrap();
    let q9_sql = std::fs::read_to_string("../../benchmarks/queries/q9.sql").unwrap();
    let q10_sql = std::fs::read_to_string("../../benchmarks/queries/q10.sql").unwrap();
    let q11_sql = std::fs::read_to_string("../../benchmarks/queries/q11.sql").unwrap();
    let q12_sql = std::fs::read_to_string("../../benchmarks/queries/q12.sql").unwrap();
    let q13_sql = std::fs::read_to_string("../../benchmarks/queries/q13.sql").unwrap();
    let q14_sql = std::fs::read_to_string("../../benchmarks/queries/q14.sql").unwrap();
    // let q15_sql = std::fs::read_to_string("../../benchmarks/queries/q15.sql").unwrap();
    let q16_sql = std::fs::read_to_string("../../benchmarks/queries/q16.sql").unwrap();
    let q17_sql = std::fs::read_to_string("../../benchmarks/queries/q17.sql").unwrap();
    let q18_sql = std::fs::read_to_string("../../benchmarks/queries/q18.sql").unwrap();
    let q19_sql = std::fs::read_to_string("../../benchmarks/queries/q19.sql").unwrap();
    let q20_sql = std::fs::read_to_string("../../benchmarks/queries/q20.sql").unwrap();
    let q21_sql = std::fs::read_to_string("../../benchmarks/queries/q21.sql").unwrap();
    let q22_sql = std::fs::read_to_string("../../benchmarks/queries/q22.sql").unwrap();

    c.bench_function("physical_plan_tpch", |b| {
        b.iter(|| physical_plan(&ctx, &q1_sql));
        b.iter(|| physical_plan(&ctx, &q2_sql));
        b.iter(|| physical_plan(&ctx, &q3_sql));
        b.iter(|| physical_plan(&ctx, &q4_sql));
        b.iter(|| physical_plan(&ctx, &q5_sql));
        b.iter(|| physical_plan(&ctx, &q6_sql));
        b.iter(|| physical_plan(&ctx, &q7_sql));
        b.iter(|| physical_plan(&ctx, &q8_sql));
        b.iter(|| physical_plan(&ctx, &q9_sql));
        b.iter(|| physical_plan(&ctx, &q10_sql));
        b.iter(|| physical_plan(&ctx, &q11_sql));
        b.iter(|| physical_plan(&ctx, &q12_sql));
        b.iter(|| physical_plan(&ctx, &q13_sql));
        b.iter(|| physical_plan(&ctx, &q14_sql));
        // b.iter(|| physical_plan(&ctx, &q15_sql));
        b.iter(|| physical_plan(&ctx, &q16_sql));
        b.iter(|| physical_plan(&ctx, &q17_sql));
        b.iter(|| physical_plan(&ctx, &q18_sql));
        b.iter(|| physical_plan(&ctx, &q19_sql));
        b.iter(|| physical_plan(&ctx, &q20_sql));
        b.iter(|| physical_plan(&ctx, &q21_sql));
        b.iter(|| physical_plan(&ctx, &q22_sql));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
