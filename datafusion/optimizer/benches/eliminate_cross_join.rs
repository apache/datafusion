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

//! Microbenchmarks for the [`EliminateCrossJoin`] optimizer rule.
//!
//! Two scenarios are covered, each over `n` three-column tables:
//!
//! 1. **comma_join** – `FROM t0, t1, ..., tn WHERE t0.a = t1.a AND t1.a =
//!    t2.a AND ...`: a `Filter` over a chain of cross joins that the rule
//!    rewrites into a chain of inner joins.
//!
//! 2. **inner_join** – `t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.a = t2.a ...`:
//!    a chain of inner joins that already carry their keys. The rule still
//!    flattens and reassembles it.

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::logical_plan::{JoinType, table_scan};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, and, col};
use datafusion_optimizer::OptimizerContext;
use datafusion_optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion_optimizer::{Optimizer, OptimizerRule};
use std::hint::black_box;
use std::sync::Arc;

/// Build a three-column table scan for `name`.
fn scan(name: &str) -> LogicalPlan {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]);
    table_scan(Some(name), &schema, None)
        .unwrap()
        .build()
        .unwrap()
}

/// `t{i - 1}.a = t{i}.a`
fn chain_key(i: usize) -> Expr {
    col(format!("t{}.a", i - 1)).eq(col(format!("t{i}.a")))
}

/// `SELECT * FROM t0, t1, ... WHERE t0.a = t1.a AND t1.a = t2.a AND ...`
fn build_comma_join_plan(n: usize) -> LogicalPlan {
    let mut builder = LogicalPlanBuilder::from(scan("t0"));
    for i in 1..n {
        builder = builder.cross_join(scan(&format!("t{i}"))).unwrap();
    }
    let predicate = (1..n).map(chain_key).reduce(and).unwrap();
    builder.filter(predicate).unwrap().build().unwrap()
}

/// `SELECT * FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.a = t2.a ...`
fn build_inner_join_plan(n: usize) -> LogicalPlan {
    let mut builder = LogicalPlanBuilder::from(scan("t0"));
    for i in 1..n {
        builder = builder
            .join(
                scan(&format!("t{i}")),
                JoinType::Inner,
                (vec![format!("t{}.a", i - 1)], vec![format!("t{i}.a")]),
                None,
            )
            .unwrap();
    }
    builder.build().unwrap()
}

fn run_optimizer(plan: &LogicalPlan, ctx: &OptimizerContext) -> LogicalPlan {
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(EliminateCrossJoin::new())];
    Optimizer::with_rules(rules)
        .optimize(plan.clone(), ctx, |_, _| {})
        .unwrap()
}

fn bench_eliminate_cross_join(c: &mut Criterion) {
    let ctx = OptimizerContext::new().with_max_passes(1);

    for (name, build) in [
        (
            "comma_join",
            build_comma_join_plan as fn(usize) -> LogicalPlan,
        ),
        ("inner_join", build_inner_join_plan),
    ] {
        let mut group = c.benchmark_group(format!("eliminate_cross_join/{name}"));
        for n in [8, 32, 64, 128] {
            let plan = build(n);
            group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
                b.iter(|| black_box(run_optimizer(p, &ctx)));
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_eliminate_cross_join);
criterion_main!(benches);
