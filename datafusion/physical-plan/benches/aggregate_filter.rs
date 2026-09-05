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

//! Benchmarks grouped aggregate `FILTER` processing through `AggregateExec`.
//!
//! The benchmark includes group-key, filter, and aggregate-argument evaluation,
//! group interning, and `GroupsAccumulator` updates. It intentionally uses an
//! in-memory physical plan to exclude SQL planning and file I/O.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{BinaryExpr, col};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 65_536;
const BATCH_SIZE: usize = 8_192;
const GROUP_COUNTS: &[usize] = &[16, 1_024, NUM_ROWS];
const FILTER_CASES: &[Option<usize>] = &[
    None,
    Some(1),
    Some(10),
    Some(50),
    Some(90),
    Some(99),
    Some(100),
];

#[derive(Clone, Copy)]
enum ArgumentKind {
    Column,
    Multiply,
}

impl ArgumentKind {
    fn name(self) -> &'static str {
        match self {
            Self::Column => "column",
            Self::Multiply => "multiply",
        }
    }
}

#[derive(Clone, Copy)]
enum AggregateLayout {
    OneAggregate,
    TwoAggregatesSharedFilter,
    TwoAggregatesDistinctFilters,
}

impl AggregateLayout {
    fn name(self) -> &'static str {
        match self {
            Self::OneAggregate => "one_aggregate",
            Self::TwoAggregatesSharedFilter => "two_aggregates_shared_filter",
            Self::TwoAggregatesDistinctFilters => "two_aggregates_distinct_filters",
        }
    }

    fn aggregate_count(self) -> usize {
        match self {
            Self::OneAggregate => 1,
            Self::TwoAggregatesSharedFilter | Self::TwoAggregatesDistinctFilters => 2,
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::UInt32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("include_a", DataType::Boolean, false),
        Field::new("include_b", DataType::Boolean, false),
    ]))
}

fn make_filter_mask(filter_percent: usize, seed: u64) -> Vec<bool> {
    let selected_rows = NUM_ROWS * filter_percent / 100;
    let mut mask = vec![false; NUM_ROWS];
    mask[..selected_rows].fill(true);
    mask.shuffle(&mut StdRng::seed_from_u64(seed));
    mask
}

fn make_batches(filter_percent: Option<usize>, num_groups: usize) -> Vec<RecordBatch> {
    let filter_percent = filter_percent.unwrap_or(100);
    // Select different rows while keeping both filters' distributions comparable.
    let include_a = make_filter_mask(filter_percent, 42);
    let include_b = make_filter_mask(filter_percent, 43);
    (0..NUM_ROWS)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = start + BATCH_SIZE;
            let group_key = UInt32Array::from_iter_values(
                (start..end).map(|row| (row % num_groups) as u32),
            );
            let value = Float64Array::from_iter_values(
                (start..end).map(|row| (row % 1_000) as f64),
            );
            let include_a = BooleanArray::from(include_a[start..end].to_vec());
            let include_b = BooleanArray::from(include_b[start..end].to_vec());

            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(group_key),
                    Arc::new(value),
                    Arc::new(include_a),
                    Arc::new(include_b),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn aggregate_expr(
    schema: &SchemaRef,
    aggregate_index: usize,
    argument_kind: ArgumentKind,
) -> Arc<AggregateFunctionExpr> {
    let value = col("value", schema).unwrap();
    let argument: Arc<dyn PhysicalExpr> = match argument_kind {
        ArgumentKind::Column => value,
        ArgumentKind::Multiply => Arc::new(BinaryExpr::new(
            Arc::clone(&value),
            Operator::Multiply,
            value,
        )),
    };

    Arc::new(
        AggregateExprBuilder::new(sum_udaf(), vec![argument])
            .schema(Arc::clone(schema))
            .alias(format!("sum_{}_{aggregate_index}", argument_kind.name()))
            .build()
            .unwrap(),
    )
}

fn make_plan(
    layout: AggregateLayout,
    argument_kind: ArgumentKind,
    filter_percent: Option<usize>,
    num_groups: usize,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let batches = make_batches(filter_percent, num_groups);
    let input =
        TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("group_key", &schema).unwrap(),
        "group_key".to_string(),
    )]);
    let aggregates = (0..layout.aggregate_count())
        .map(|index| aggregate_expr(&schema, index, argument_kind))
        .collect::<Vec<_>>();

    let filters = match filter_percent {
        None => vec![None; layout.aggregate_count()],
        Some(_) => {
            let include_a = col("include_a", &schema).unwrap();
            match layout {
                AggregateLayout::OneAggregate => vec![Some(include_a)],
                AggregateLayout::TwoAggregatesSharedFilter => {
                    vec![Some(Arc::clone(&include_a)), Some(include_a)]
                }
                AggregateLayout::TwoAggregatesDistinctFilters => {
                    vec![Some(include_a), Some(col("include_b", &schema).unwrap())]
                }
            }
        }
    };

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            aggregates,
            filters,
            input,
            schema,
        )
        .unwrap(),
    )
}

fn benchmark_case(
    c: &mut Criterion,
    runtime: &Runtime,
    layout: AggregateLayout,
    argument_kinds: &[ArgumentKind],
) {
    for &num_groups in GROUP_COUNTS {
        let mut group = c.benchmark_group(format!(
            "grouped_aggregate_filter/{}/{}_groups",
            layout.name(),
            num_groups
        ));

        for &argument_kind in argument_kinds {
            for &filter_percent in FILTER_CASES {
                let case_name = match filter_percent {
                    None => "unfiltered".to_string(),
                    Some(percent) => format!("{percent}_percent"),
                };
                let plan = make_plan(layout, argument_kind, filter_percent, num_groups);
                let task_ctx = Arc::new(TaskContext::default());

                group.bench_function(
                    BenchmarkId::new(argument_kind.name(), case_name),
                    |b| {
                        b.iter(|| {
                            let output = runtime
                                .block_on(collect(
                                    Arc::clone(&plan),
                                    Arc::clone(&task_ctx),
                                ))
                                .unwrap();
                            black_box(output);
                        });
                    },
                );
            }
        }

        group.finish();
    }
}

fn aggregate_filter_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    benchmark_case(
        c,
        &runtime,
        AggregateLayout::OneAggregate,
        &[ArgumentKind::Column, ArgumentKind::Multiply],
    );
    benchmark_case(
        c,
        &runtime,
        AggregateLayout::TwoAggregatesSharedFilter,
        &[ArgumentKind::Multiply],
    );
    benchmark_case(
        c,
        &runtime,
        AggregateLayout::TwoAggregatesDistinctFilters,
        &[ArgumentKind::Multiply],
    );
}

criterion_group!(benches, aggregate_filter_benchmark);
criterion_main!(benches);
