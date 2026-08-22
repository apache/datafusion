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

//! Microbenchmarks for the experimental [`QueryFusion`] optimizer rule.

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    LogicalPlan, LogicalPlanBuilder, col, lit, logical_plan::table_scan,
};
use datafusion_optimizer::query_fusion::QueryFusion;
use datafusion_optimizer::{Optimizer, OptimizerContext, OptimizerRule};

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
    builder.unwrap().build().unwrap()
}

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
    builder.unwrap().build().unwrap()
}

fn build_merge_with_projection_plan(n: usize) -> LogicalPlan {
    assert!(n >= 2);
    let mut builder: Option<LogicalPlanBuilder> = None;
    for i in 0..n {
        let branch = LogicalPlanBuilder::from(scan("t"))
            .filter(col("a").eq(lit(i as i32)))
            .unwrap()
            .project(vec![col("a").alias("a_out"), col("b").alias("b_out")])
            .unwrap()
            .build()
            .unwrap();
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(b) => b.union(branch).unwrap(),
        });
    }
    builder.unwrap().build().unwrap()
}

fn context(enabled: bool) -> OptimizerContext {
    let mut options = ConfigOptions::default();
    options.optimizer.enable_query_fusion = enabled;
    OptimizerContext::new_with_config_options(Arc::new(options)).with_max_passes(1)
}

fn run_optimizer(plan: &LogicalPlan, ctx: &OptimizerContext) -> LogicalPlan {
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(QueryFusion::new())];
    Optimizer::with_rules(rules)
        .optimize(plan.clone(), ctx, |_, _| {})
        .unwrap()
}

fn bench_disabled(c: &mut Criterion) {
    let ctx = context(false);

    let mut group = c.benchmark_group("query_fusion/disabled");
    for n in [2, 8, 32, 128] {
        let plan = build_merge_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let ctx = context(true);

    let mut group = c.benchmark_group("query_fusion/merge");
    for n in [2, 8, 32, 128] {
        let plan = build_merge_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

fn bench_no_merge(c: &mut Criterion) {
    let ctx = context(true);

    let mut group = c.benchmark_group("query_fusion/no_merge");
    for n in [2, 8, 32, 128] {
        let plan = build_no_merge_plan(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, p| {
            b.iter(|| black_box(run_optimizer(p, &ctx)));
        });
    }
    group.finish();
}

fn bench_merge_with_projection(c: &mut Criterion) {
    let ctx = context(true);

    let mut group = c.benchmark_group("query_fusion/merge_with_projection");
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
    bench_disabled,
    bench_merge,
    bench_no_merge,
    bench_merge_with_projection
);
criterion_main!(benches);
