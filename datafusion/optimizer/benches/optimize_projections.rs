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

//! Micro-benchmarks for the `OptimizeProjections` logical optimizer rule.
//!
//! Each case models a plan shape typical of TPC-H, TPC-DS, or ClickBench.
//! Schemas use realistic widths and the rule operates on a fresh
//! `LogicalPlan` per iteration (construction is in the criterion setup
//! closure and excluded from measurement).

use std::hint::black_box;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr::{
    JoinType, LogicalPlan, LogicalPlanBuilder, col, lit, logical_plan::table_scan,
};
use datafusion_functions_aggregate::expr_fn::sum;
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::{OptimizerContext, OptimizerRule};

fn table(name: &str, cols: usize) -> LogicalPlan {
    let fields: Vec<Field> = (0..cols)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
        .collect();
    table_scan(Some(name), &Schema::new(fields), None)
        .unwrap()
        .build()
        .unwrap()
}

fn scan_with_filter(name: &str, cols: usize, filter_col: usize) -> LogicalPlan {
    LogicalPlanBuilder::from(table(name, cols))
        .filter(col(format!("{name}.c{filter_col}")).gt(lit(0i32)))
        .unwrap()
        .build()
        .unwrap()
}

/// TPC-H Q3-like: customer ⨝ orders ⨝ lineitem with filters above each scan,
/// GROUP BY 3 keys, 1 SUM aggregate. Models the canonical filter→join→aggregate
/// analytical shape after PushDownFilter.
fn plan_tpch_q3() -> LogicalPlan {
    let customer = scan_with_filter("customer", 8, 6);
    let orders = scan_with_filter("orders", 9, 4);
    let lineitem = scan_with_filter("lineitem", 16, 10);

    LogicalPlanBuilder::from(customer)
        .join_on(
            orders,
            JoinType::Inner,
            vec![col("customer.c0").eq(col("orders.c1"))],
        )
        .unwrap()
        .join_on(
            lineitem,
            JoinType::Inner,
            vec![col("lineitem.c0").eq(col("orders.c0"))],
        )
        .unwrap()
        .aggregate(
            vec![col("lineitem.c0"), col("orders.c4"), col("orders.c7")],
            vec![sum(col("lineitem.c5") - col("lineitem.c6"))],
        )
        .unwrap()
        .build()
        .unwrap()
}

/// TPC-H Q5-like: 6-way join through region→nation→customer→orders→lineitem
/// →supplier, GROUP BY 1 key, 1 SUM. Exercises nested-join pruning depth.
fn plan_tpch_q5() -> LogicalPlan {
    let region = scan_with_filter("region", 3, 1);
    let nation = table("nation", 4);
    let customer = table("customer", 8);
    let orders = table("orders", 9);
    let lineitem = table("lineitem", 16);
    let supplier = table("supplier", 7);

    LogicalPlanBuilder::from(region)
        .join_on(
            nation,
            JoinType::Inner,
            vec![col("region.c0").eq(col("nation.c2"))],
        )
        .unwrap()
        .join_on(
            customer,
            JoinType::Inner,
            vec![col("nation.c0").eq(col("customer.c3"))],
        )
        .unwrap()
        .join_on(
            orders,
            JoinType::Inner,
            vec![col("customer.c0").eq(col("orders.c1"))],
        )
        .unwrap()
        .join_on(
            lineitem,
            JoinType::Inner,
            vec![col("lineitem.c0").eq(col("orders.c0"))],
        )
        .unwrap()
        .join_on(
            supplier,
            JoinType::Inner,
            vec![col("lineitem.c2").eq(col("supplier.c0"))],
        )
        .unwrap()
        .aggregate(
            vec![col("nation.c1")],
            vec![sum(col("lineitem.c5") - col("lineitem.c6"))],
        )
        .unwrap()
        .build()
        .unwrap()
}

/// ClickBench-style: single wide `hits` table (100 cols), conjunctive filter,
/// GROUP BY 2 keys, 2 SUM aggregates. Stresses wide-schema column lookup.
fn plan_clickbench_groupby() -> LogicalPlan {
    let hits = table("hits", 100);
    let predicate = col("hits.c5")
        .gt(lit(100i32))
        .and(col("hits.c12").lt(lit(1000i32)));
    LogicalPlanBuilder::from(hits)
        .filter(predicate)
        .unwrap()
        .aggregate(
            vec![col("hits.c3"), col("hits.c7")],
            vec![sum(col("hits.c42")), sum(col("hits.c60"))],
        )
        .unwrap()
        .build()
        .unwrap()
}

/// TPC-DS-style CTE shape: a SubqueryAlias wrapping a filter+projection over
/// a wide fact table, joined back on two dimension tables and aggregated.
fn plan_tpcds_subquery() -> LogicalPlan {
    let store_sales = table("store_sales", 23);
    let customer = table("customer", 18);
    let item = table("item", 22);

    let sub = LogicalPlanBuilder::from(store_sales)
        .filter(col("store_sales.c5").gt(lit(0i32)))
        .unwrap()
        .project(vec![
            col("store_sales.c0"),
            col("store_sales.c3"),
            col("store_sales.c13"),
        ])
        .unwrap()
        .alias("sub")
        .unwrap()
        .build()
        .unwrap();

    LogicalPlanBuilder::from(customer)
        .join_on(
            sub,
            JoinType::Inner,
            vec![col("customer.c0").eq(col("sub.c3"))],
        )
        .unwrap()
        .join_on(
            item,
            JoinType::Inner,
            vec![col("item.c0").eq(col("sub.c0"))],
        )
        .unwrap()
        .aggregate(vec![col("customer.c2")], vec![sum(col("sub.c13"))])
        .unwrap()
        .build()
        .unwrap()
}

/// Narrow 10-column table, single filter, project 3 cols. Guards against
/// regressions on the common small-schema case where a lookup-map fix for
/// wide schemas might hurt by adding hashing overhead.
fn plan_small_schema() -> LogicalPlan {
    LogicalPlanBuilder::from(table("t", 10))
        .filter(col("t.c3").gt(lit(0i32)))
        .unwrap()
        .project(vec![col("t.c0"), col("t.c1"), col("t.c5")])
        .unwrap()
        .build()
        .unwrap()
}

type BenchCase = (&'static str, fn() -> LogicalPlan);

fn bench_optimize_projections(c: &mut Criterion) {
    let rule = OptimizeProjections::new();
    let config = OptimizerContext::new();
    let mut group = c.benchmark_group("optimize_projections");

    let cases: &[BenchCase] = &[
        ("tpch_q3", plan_tpch_q3),
        ("tpch_q5", plan_tpch_q5),
        ("clickbench_groupby", plan_clickbench_groupby),
        ("tpcds_subquery", plan_tpcds_subquery),
        ("small_schema", plan_small_schema),
    ];

    for (name, build) in cases {
        group.bench_function(*name, |b| {
            b.iter_batched(
                build,
                |plan| black_box(rule.rewrite(plan, &config).unwrap()),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_optimize_projections);
criterion_main!(benches);
