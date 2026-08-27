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

use arrow::array::{Array, BooleanArray, Float64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
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
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 65_536;
const BATCH_SIZE: usize = 8_192;
const NUM_GROUPS: usize = 1_024;
const FILTER_PERCENTS: &[usize] = &[1, 10, 50, 90, 99, 100];

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

fn include_a(row: usize, filter_percent: usize) -> bool {
    row % 100 < filter_percent
}

fn include_b(row: usize, filter_percent: usize) -> bool {
    (row * 37 + 17) % 100 < filter_percent
}

fn make_batches(filter_percent: usize) -> Vec<RecordBatch> {
    (0..NUM_ROWS)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = start + BATCH_SIZE;
            let group_key = UInt32Array::from_iter_values(
                (start..end).map(|row| (row % NUM_GROUPS) as u32),
            );
            let value = Float64Array::from_iter_values(
                (start..end).map(|row| (row % 1_000) as f64),
            );
            let include_a = BooleanArray::from(
                (start..end)
                    .map(|row| include_a(row, filter_percent))
                    .collect::<Vec<_>>(),
            );
            let include_b = BooleanArray::from(
                (start..end)
                    .map(|row| include_b(row, filter_percent))
                    .collect::<Vec<_>>(),
            );

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

fn sum_squared(schema: &SchemaRef, aggregate_index: usize) -> Arc<AggregateFunctionExpr> {
    let value = col("value", schema).unwrap();
    let argument = Arc::new(BinaryExpr::new(
        Arc::clone(&value),
        Operator::Multiply,
        value,
    )) as Arc<dyn PhysicalExpr>;

    Arc::new(
        AggregateExprBuilder::new(sum_udaf(), vec![argument])
            .schema(Arc::clone(schema))
            .alias(format!("sum_squared_{aggregate_index}"))
            .build()
            .unwrap(),
    )
}

fn make_plan(
    layout: AggregateLayout,
    filter_percent: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let batches = make_batches(filter_percent.unwrap_or(50));
    let input =
        TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("group_key", &schema).unwrap(),
        "group_key".to_string(),
    )]);
    let aggregates = (0..layout.aggregate_count())
        .map(|index| sum_squared(&schema, index))
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

fn expected_sums(
    layout: AggregateLayout,
    filter_percent: Option<usize>,
) -> Vec<Vec<Option<f64>>> {
    let mut expected = vec![vec![None; NUM_GROUPS]; layout.aggregate_count()];

    for row in 0..NUM_ROWS {
        let group = row % NUM_GROUPS;
        let value = (row % 1_000) as f64;
        let squared = value * value;

        for (aggregate_index, aggregate) in expected.iter_mut().enumerate() {
            let selected = filter_percent.is_none_or(|filter_percent| {
                if aggregate_index == 0
                    || matches!(layout, AggregateLayout::TwoAggregatesSharedFilter)
                {
                    include_a(row, filter_percent)
                } else {
                    include_b(row, filter_percent)
                }
            });

            if selected {
                *aggregate[group].get_or_insert(0.0) += squared;
            }
        }
    }

    expected
}

fn validate_output(
    output: &[RecordBatch],
    layout: AggregateLayout,
    filter_percent: Option<usize>,
) {
    let expected = expected_sums(layout, filter_percent);
    let mut seen_groups = vec![false; NUM_GROUPS];

    for batch in output {
        let group_keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let sums = (0..layout.aggregate_count())
            .map(|aggregate_index| {
                batch
                    .column(aggregate_index + 1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for row in 0..batch.num_rows() {
            let group = group_keys.value(row) as usize;
            assert!(!seen_groups[group], "duplicate group {group}");
            seen_groups[group] = true;

            for (aggregate_index, sum) in sums.iter().enumerate() {
                match expected[aggregate_index][group] {
                    Some(expected) => {
                        assert!(!sum.is_null(row));
                        assert_eq!(sum.value(row), expected);
                    }
                    None => assert!(sum.is_null(row)),
                }
            }
        }
    }

    assert!(seen_groups.into_iter().all(|seen| seen));
}

fn benchmark_case(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    runtime: &Runtime,
    layout: AggregateLayout,
    case_name: &str,
    filter_percent: Option<usize>,
) {
    let plan = make_plan(layout, filter_percent);
    let task_ctx = Arc::new(TaskContext::default());

    let output = runtime
        .block_on(collect(Arc::clone(&plan), Arc::clone(&task_ctx)))
        .unwrap();
    validate_output(&output, layout, filter_percent);

    group.bench_function(case_name, |b| {
        b.iter(|| {
            let output = runtime
                .block_on(collect(Arc::clone(&plan), Arc::clone(&task_ctx)))
                .unwrap();
            black_box(output);
        });
    });
}

fn aggregate_filter_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    for layout in [
        AggregateLayout::OneAggregate,
        AggregateLayout::TwoAggregatesSharedFilter,
        AggregateLayout::TwoAggregatesDistinctFilters,
    ] {
        let mut group =
            c.benchmark_group(format!("grouped_aggregate_filter/{}", layout.name()));
        group.throughput(Throughput::Elements(NUM_ROWS as u64));

        benchmark_case(&mut group, &runtime, layout, "unfiltered", None);
        for &filter_percent in FILTER_PERCENTS {
            benchmark_case(
                &mut group,
                &runtime,
                layout,
                &format!("{filter_percent}_percent"),
                Some(filter_percent),
            );
        }

        group.finish();
    }
}

criterion_group!(benches, aggregate_filter_benchmark);
criterion_main!(benches);
