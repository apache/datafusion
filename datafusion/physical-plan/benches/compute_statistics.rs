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

//! Benchmarks for `compute_statistics` with `StatsCache`.
//!
//! Demonstrates that caching eliminates redundant subtree walks in plans
//! containing partition-merging operators (CoalescePartitionsExec) and
//! binary join trees (CrossJoinExec).
//!
//! The plan shapes here mirror the reproducers from the planning-speed
//! EPIC (<https://github.com/apache/datafusion/issues/19795>):
//! - Coalesce chain: deep linear plans (e.g. deeply nested subqueries)
//! - Cross-join tree: balanced binary trees from multi-way joins
//!   (mirrors the `physical_many_self_joins` sql_planner benchmark)

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::{Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::{
    Boundedness, EmissionType, ExecutionPlan, PlanProperties,
};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::CrossJoinExec;
use datafusion_physical_plan::statistics::StatisticsArgs;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, Partitioning, SendableRecordBatchStream,
};

/// Minimal leaf node for benchmarking
#[derive(Debug)]
struct BenchLeaf {
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl BenchLeaf {
    fn new(col_name: &str) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Int32,
            false,
        )]));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(2),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { schema, cache }
    }
}

impl DisplayAs for BenchLeaf {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BenchLeaf")
    }
}

impl ExecutionPlan for BenchLeaf {
    fn name(&self) -> &str {
        "BenchLeaf"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }

    fn statistics_with_args(&self, _args: &StatisticsArgs) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

/// Build: CoalescePartitions^depth -> BenchLeaf
fn build_coalesce_chain(depth: usize) -> Arc<dyn ExecutionPlan> {
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(BenchLeaf::new("a"));
    for _ in 0..depth {
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }
    plan
}

/// Build a balanced binary tree of CrossJoinExec with 2^depth leaves.
/// Mirrors the plan shape produced by multi-way self-joins like the
/// `physical_many_self_joins` benchmark in sql_planner.rs (#19795).
fn build_cross_join_tree(depth: usize, next_col: &mut usize) -> Arc<dyn ExecutionPlan> {
    if depth == 0 {
        let col_name = format!("c{next_col}");
        *next_col += 1;
        return Arc::new(BenchLeaf::new(&col_name));
    }
    let left = build_cross_join_tree(depth - 1, next_col);
    let right = build_cross_join_tree(depth - 1, next_col);
    Arc::new(CrossJoinExec::new(left, right))
}

/// Build: Filter^depth -> BenchLeaf (always-true predicate).
fn build_filter_chain(depth: usize) -> Arc<dyn ExecutionPlan> {
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(BenchLeaf::new("a"));
    let predicate: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
    for _ in 0..depth {
        plan = Arc::new(
            FilterExec::try_new(Arc::clone(&predicate), plan)
                .expect("FilterExec::try_new failed"),
        );
    }
    plan
}

/// Build a mixed chain alternating partition-merging and partition-preserving
/// operators: (Coalesce -> Filter -> Filter) repeated `groups` times -> BenchLeaf.
/// Exercises the cache with both None and Some(p) lookups in the same walk.
fn build_mixed_chain(groups: usize) -> Arc<dyn ExecutionPlan> {
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(BenchLeaf::new("a"));
    let predicate: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
    for _ in 0..groups {
        // Two partition-preserving filters
        for _ in 0..2 {
            plan = Arc::new(
                FilterExec::try_new(Arc::clone(&predicate), plan)
                    .expect("FilterExec::try_new failed"),
            );
        }
        // One partition-merging coalesce
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }
    plan
}

/// Recursive walk without a shared cross-node cache, simulating pre-cache behavior.
/// Each operator's internal `compute_child_statistics` call triggers a fresh
/// subtree walk, resulting in O(n^2) total node visits for a chain of depth n.
///
/// Note: each `compute_child_statistics` re-walk still benefits from its own
/// ephemeral cache; only the cross-node sharing is removed.
fn compute_statistics_without_shared_cache(
    plan: &dyn ExecutionPlan,
    partition: Option<usize>,
) -> Result<Arc<Statistics>> {
    for child in plan.children() {
        compute_statistics_without_shared_cache(child.as_ref(), None)?;
    }
    let args = StatisticsArgs::new().with_partition(partition);
    plan.statistics_with_args(&args)
}

fn bench_compute_statistics(c: &mut Criterion) {
    // --- Coalesce chain (linear plan) ---
    // Deep linear plans arise from deeply nested subqueries, CTEs, etc.
    let mut group = c.benchmark_group("compute_statistics_coalesce_chain");
    for depth in [10, 20, 50] {
        let plan = build_coalesce_chain(depth);
        group.bench_with_input(BenchmarkId::new("cached", depth), &plan, |b, plan| {
            b.iter(|| plan.statistics_with_args(&StatisticsArgs::new()).unwrap());
        });
        group.bench_with_input(
            BenchmarkId::new("no_shared_cache", depth),
            &plan,
            |b, plan| {
                b.iter(|| {
                    compute_statistics_without_shared_cache(plan.as_ref(), None).unwrap()
                });
            },
        );
    }
    group.finish();

    // --- Cross-join tree (balanced binary plan) ---
    // Binary trees arise from multi-way joins (e.g. physical_many_self_joins
    // in sql_planner.rs, see #19795). CrossJoinExec calls
    // compute_child_statistics for per-partition stats, re-walking the left
    // subtree at each node. The gap between cached/uncached is smaller than
    // the linear chain because only the left child triggers a re-walk.
    let mut group = c.benchmark_group("compute_statistics_cross_join_tree");
    for depth in [3, 5, 7] {
        let mut next_col = 0;
        let plan = build_cross_join_tree(depth, &mut next_col);
        let label = format!("depth={depth}_leaves={}", 1usize << depth);
        group.bench_with_input(BenchmarkId::new("cached", &label), &plan, |b, plan| {
            b.iter(|| {
                plan.statistics_with_args(&StatisticsArgs::new().with_partition(Some(0)))
                    .unwrap()
            });
        });
        group.bench_with_input(
            BenchmarkId::new("no_shared_cache", &label),
            &plan,
            |b, plan| {
                b.iter(|| {
                    compute_statistics_without_shared_cache(plan.as_ref(), Some(0))
                        .unwrap()
                });
            },
        );
    }
    group.finish();

    // --- Filter chain (partition-preserving linear plan) ---
    // When called with Some(0), the framework first walks the entire tree
    // computing None stats, then each filter requests Some(0) on demand.
    // Both walks are cached, so the total cost is ~2n vs n node visits for None.
    let mut group = c.benchmark_group("compute_statistics_filter_chain");
    for depth in [10, 20, 50] {
        let plan = build_filter_chain(depth);
        group.bench_with_input(
            BenchmarkId::new("cached_partition", depth),
            &plan,
            |b, plan| {
                b.iter(|| {
                    plan.statistics_with_args(
                        &StatisticsArgs::new().with_partition(Some(0)),
                    )
                    .unwrap()
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cached_overall", depth),
            &plan,
            |b, plan| {
                b.iter(|| plan.statistics_with_args(&StatisticsArgs::new()).unwrap());
            },
        );
        group.bench_with_input(
            BenchmarkId::new("no_shared_cache", depth),
            &plan,
            |b, plan| {
                b.iter(|| {
                    compute_statistics_without_shared_cache(plan.as_ref(), Some(0))
                        .unwrap()
                });
            },
        );
    }
    group.finish();

    // --- Mixed chain (partition-preserving + partition-merging) ---
    // Alternates Filter (preserving) and CoalescePartitions (merging) to
    // exercise the cache with both None and Some(p) lookups in a single walk.
    let mut group = c.benchmark_group("compute_statistics_mixed_chain");
    for groups in [3, 5, 10] {
        let plan = build_mixed_chain(groups);
        let depth = groups * 3; // 2 filters + 1 coalesce per group
        group.bench_with_input(BenchmarkId::new("cached", depth), &plan, |b, plan| {
            b.iter(|| {
                plan.statistics_with_args(&StatisticsArgs::new().with_partition(Some(0)))
                    .unwrap()
            });
        });
        group.bench_with_input(
            BenchmarkId::new("no_shared_cache", depth),
            &plan,
            |b, plan| {
                b.iter(|| {
                    compute_statistics_without_shared_cache(plan.as_ref(), Some(0))
                        .unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_compute_statistics);
criterion_main!(benches);
