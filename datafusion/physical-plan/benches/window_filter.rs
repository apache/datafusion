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

//! Microbenchmark for window aggregates with `FILTER`. The benchmark uses
//! pre-ordered input to exclude sorting and query planning, but executes the
//! physical window plan so both stateful and whole-partition evaluation paths
//! are represented.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_execution::TaskContext;
use datafusion_expr::{
    Operator, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions::math::power;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, create_window_expr,
};
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

const BATCH_SIZE: usize = 8192;
const NUM_BATCHES: usize = 4;
const NUM_ROWS: usize = BATCH_SIZE * NUM_BATCHES;
const FILTER_CASES: &[Option<usize>] = &[None, Some(10), Some(30), Some(50)];

#[derive(Clone, Copy)]
enum ArgumentKind {
    Column,
    Divide,
    Power,
}

impl ArgumentKind {
    fn name(self) -> &'static str {
        match self {
            Self::Column => "column",
            Self::Divide => "divide",
            Self::Power => "power_udf",
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("include", DataType::Boolean, false),
    ]))
}

fn make_filter_mask(filter_percent: usize) -> Vec<bool> {
    let selected_rows = NUM_ROWS * filter_percent / 100;
    let mut mask = vec![false; NUM_ROWS];
    mask[..selected_rows].fill(true);
    mask.shuffle(&mut StdRng::seed_from_u64(42));
    mask
}

fn make_batches(filter_percent: Option<usize>) -> Vec<RecordBatch> {
    let include = make_filter_mask(filter_percent.unwrap_or(100));
    (0..NUM_BATCHES)
        .map(|batch_index| {
            let start = batch_index * BATCH_SIZE;
            let end = start + BATCH_SIZE;
            let id = UInt64Array::from_iter_values((start..end).map(|i| i as u64));
            let value = Float64Array::from_iter_values((start..end).map(|i| i as f64));
            let include = BooleanArray::from(include[start..end].to_vec());

            RecordBatch::try_new(
                schema(),
                vec![Arc::new(id), Arc::new(value), Arc::new(include)],
            )
            .unwrap()
        })
        .collect()
}

fn window_argument(kind: ArgumentKind, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    let column = col("value", schema).unwrap();
    match kind {
        ArgumentKind::Column => column,
        ArgumentKind::Divide => {
            Arc::new(BinaryExpr::new(column, Operator::Divide, lit(10.0_f64)))
        }
        ArgumentKind::Power => Arc::new(
            ScalarFunctionExpr::try_new(
                power(),
                vec![column, lit(1.5_f64)],
                schema,
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        ),
    }
}

fn cumulative_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    )
}

fn sliding_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(10))),
        WindowFrameBound::CurrentRow,
    )
}

fn whole_partition_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::Following(ScalarValue::UInt64(None)),
    )
}

fn make_window_plan(
    filter_percent: Option<usize>,
    argument_kind: ArgumentKind,
    window_frame: WindowFrame,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let order_by = vec![PhysicalSortExpr {
        expr: col("id", &schema).unwrap(),
        options: Default::default(),
    }];
    let window_expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        match filter_percent {
            Some(_) => format!("sum({}) FILTER (WHERE include)", argument_kind.name()),
            None => format!("sum({})", argument_kind.name()),
        },
        &[window_argument(argument_kind, &schema)],
        &[],
        &order_by,
        Arc::new(window_frame),
        Arc::clone(&schema),
        false,
        false,
        filter_percent.map(|_| col("include", &schema).unwrap()),
    )
    .unwrap();

    let source = TestMemoryExec::try_new(&[make_batches(filter_percent)], schema, None)
        .unwrap()
        .try_with_sort_information(LexOrdering::new(order_by).into_iter().collect())
        .unwrap();
    let input: Arc<dyn ExecutionPlan> =
        Arc::new(TestMemoryExec::update_cache(&Arc::new(source)));

    if window_expr.uses_bounded_memory() {
        Arc::new(
            BoundedWindowAggExec::try_new(
                vec![window_expr],
                input,
                InputOrderMode::Sorted,
                false,
            )
            .unwrap(),
        )
    } else {
        Arc::new(WindowAggExec::try_new(vec![window_expr], input, false).unwrap())
    }
}

fn benchmark_window_case(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    name: &str,
    window_frame: &WindowFrame,
    argument_kinds: &[ArgumentKind],
    sample_size: usize,
) {
    let mut group = c.benchmark_group(format!("window_aggregate_filter/{name}"));
    group.sample_size(sample_size);

    for &argument_kind in argument_kinds {
        for &filter_percent in FILTER_CASES {
            let case_name = match filter_percent {
                None => "no_filter".to_string(),
                Some(percent) => format!("{percent}_percent"),
            };
            let plan =
                make_window_plan(filter_percent, argument_kind, window_frame.clone());
            let task_ctx = Arc::new(TaskContext::default());
            group.bench_function(
                BenchmarkId::new(argument_kind.name(), case_name),
                |b| {
                    b.iter(|| {
                        let batches = runtime
                            .block_on(collect(Arc::clone(&plan), Arc::clone(&task_ctx)))
                            .unwrap();
                        black_box(batches);
                    })
                },
            );
        }
    }

    group.finish();
}

fn window_filter_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    benchmark_window_case(
        c,
        &runtime,
        "bounded_cumulative",
        &cumulative_frame(),
        &[
            ArgumentKind::Column,
            ArgumentKind::Divide,
            ArgumentKind::Power,
        ],
        10,
    );
    benchmark_window_case(
        c,
        &runtime,
        "bounded_sliding_10_rows",
        &sliding_frame(),
        &[
            ArgumentKind::Column,
            ArgumentKind::Divide,
            ArgumentKind::Power,
        ],
        10,
    );
    benchmark_window_case(
        c,
        &runtime,
        "window_whole_partition",
        &whole_partition_frame(),
        &[
            ArgumentKind::Column,
            ArgumentKind::Divide,
            ArgumentKind::Power,
        ],
        100,
    );
}

criterion_group!(benches, window_filter_benchmark);
criterion_main!(benches);
