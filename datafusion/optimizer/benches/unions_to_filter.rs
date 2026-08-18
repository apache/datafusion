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

//! Microbenchmarks for the [`UnionsToFilter`] optimizer rule.
//!
//! Three scenarios are covered:
//!
//! 1. **merge** – N branches over the *same* table, each with a simple
//!    equality filter.  All branches should be merged into a single
//!    `DISTINCT(Filter(OR …))` plan.
//!
//! 2. **no_merge** – N branches over *different* tables.  The rule must
//!    recognise that no merge is possible and leave the plan unchanged.
//!    This exercises the "cold path" without any rewrite work.
//!
//! 3. **merge_with_projection** – N branches over the same table but each
//!    branch wraps the filter in a `Projection`.  This exercises the wrapper-
//!    peeling and re-wrapping paths in addition to the core merge logic.
//!
//! To generate a flamegraph (requires `cargo-flamegraph`):
//! ```text
//! cargo flamegraph -p datafusion-optimizer --bench unions_to_filter \
//!     --flamechart --root --profile profiling --freq 1000 -- --bench
//! ```

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, logical_plan::table_scan};
use datafusion_expr::{col, lit};
use datafusion_optimizer::OptimizerContext;
use datafusion_optimizer::unions_to_filter::UnionsToFilter;
use datafusion_optimizer::{Optimizer, OptimizerRule};
use std::hint::black_box;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Build a `DISTINCT (UNION ALL …)` plan whose `n` branches all filter over
/// the *same* table (`t`), so the rule can merge them.
fn build_merge_plan(n: usize) -> LogicalPlan {
    assert!(n >= 2);
    let mut builder: Option<LogicalPlanBuilder> = None;
    for i in 0..n {
        let branch = LogicalPlanBuilder::from(scan("t"))
            .filter(col("a").eq(lit(i as i32)))
            .unwrap()
            .build()
            .unwrap();
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(b) => b.union(branch).unwrap(),
        });
    }
    builder.unwrap().distinct().unwrap().build().unwrap()
}

/// Build a `DISTINCT (UNION ALL …)` plan whose `n` branches each filter over a
/// *different* table, so no merge is possible.
fn build_no_merge_plan(n: usize) -> LogicalPlan {
    assert!(n >= 2);
    let mut builder: Option<LogicalPlanBuilder> = None;
    for i in 0..n {
        let branch = LogicalPlanBuilder::from(scan(&format!("t{i}")))
            .filter(col("a").eq(lit(i as i32)))
            .unwrap()
            .build()
            .unwrap();
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(b) => b.union(branch).unwrap(),
        });
    }
    builder.unwrap().distinct().unwrap().build().unwrap()
}

/// Build a `DISTINCT (UNION ALL …)` plan whose `n` branches each wrap the
/// filter inside a `Projection` over the *same* table.
fn build_merge_with_projection_plan(n: usize) -> LogicalPlan {
    assert!(n >= 2);
    let mut builder: Option<LogicalPlanBuilder> = None;
    for i in 0..n {
        let branch = LogicalPlanBuilder::from(scan("t"))
            .filter(col("a").eq(lit(i as i32)))
            .unwrap()
            .project(vec![col("a"), col("b")])
            .unwrap()
            .build()
            .unwrap();
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(b) => b.union(branch).unwrap(),
        });
    }
    builder.unwrap().distinct().unwrap().build().unwrap()
}

/// Run the [`UnionsToFilter`] rule through the full [`Optimizer`] pipeline
/// (single pass, feature flag enabled).
fn run_optimizer(plan: &LogicalPlan, ctx: &OptimizerContext) -> LogicalPlan {
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(UnionsToFilter::new())];
    Optimizer::with_rules(rules)
        .optimize(plan.clone(), ctx, |_, _| {})
        .unwrap()
}

// ---------------------------------------------------------------------------
// Benchmark functions
// ---------------------------------------------------------------------------

fn bench_merge(c: &mut Criterion) {
    let mut options = ConfigOptions::default();
    options.optimizer.enable_unions_to_filter = true;
    let ctx =
        OptimizerContext::new_with_config_options(Arc::new(options)).with_max_passes(1);

    let mut group = c.benchmark_group("unions_to_filter/merge");
    for n in [2, 8, 32, 128] {
        let plan = build_merge_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

fn bench_no_merge(c: &mut Criterion) {
    let mut options = ConfigOptions::default();
    options.optimizer.enable_unions_to_filter = true;
    let ctx =
        OptimizerContext::new_with_config_options(Arc::new(options)).with_max_passes(1);

    let mut group = c.benchmark_group("unions_to_filter/no_merge");
    for n in [2, 8, 32, 128] {
        let plan = build_no_merge_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

fn bench_merge_with_projection(c: &mut Criterion) {
    let mut options = ConfigOptions::default();
    options.optimizer.enable_unions_to_filter = true;
    let ctx =
        OptimizerContext::new_with_config_options(Arc::new(options)).with_max_passes(1);

    let mut group = c.benchmark_group("unions_to_filter/merge_with_projection");
    for n in [2, 8, 32, 128] {
        let plan = build_merge_with_projection_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_merge,
    bench_no_merge,
    bench_merge_with_projection
);
criterion_main!(benches);
