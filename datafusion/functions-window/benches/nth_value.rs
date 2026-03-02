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

use std::hint::black_box;
use std::ops::Range;
use std::slice;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use arrow::util::bench_util::create_primitive_array;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_expr::{PartitionEvaluator, WindowUDFImpl};
use datafusion_functions_window::nth_value::{NthValue, NthValueKind};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

const ARRAY_SIZE: usize = 8192;

/// Creates a partition evaluator for FIRST_VALUE, LAST_VALUE, or NTH_VALUE
fn create_evaluator(
    kind: NthValueKind,
    ignore_nulls: bool,
    n: Option<i64>,
) -> Box<dyn PartitionEvaluator> {
    let expr = Arc::new(Column::new("c", 0)) as Arc<dyn PhysicalExpr>;
    let input_field: FieldRef = Field::new("c", DataType::Int64, true).into();
    let input_fields = vec![input_field];

    let (nth_value, exprs): (NthValue, Vec<Arc<dyn PhysicalExpr>>) = match kind {
        NthValueKind::First => (NthValue::first(), vec![expr]),
        NthValueKind::Last => (NthValue::last(), vec![expr]),
        NthValueKind::Nth => {
            let n_value =
                Arc::new(Literal::new(ScalarValue::Int64(n))) as Arc<dyn PhysicalExpr>;
            (NthValue::nth(), vec![expr, n_value])
        }
    };

    let args = PartitionEvaluatorArgs::new(&exprs, &input_fields, false, ignore_nulls);
    nth_value.partition_evaluator(args).unwrap()
}

fn bench_nth_value_ignore_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("nth_value_ignore_nulls");

    // Test different null densities
    let null_densities = [0.0, 0.3, 0.5, 0.8];

    for null_density in null_densities {
        let values = Arc::new(create_primitive_array::<Int64Type>(
            ARRAY_SIZE,
            null_density,
        )) as ArrayRef;
        let null_pct = (null_density * 100.0) as u32;

        // FIRST_VALUE with ignore_nulls - expanding window
        group.bench_function(
            BenchmarkId::new("first_value_expanding", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator = create_evaluator(NthValueKind::First, true, None);
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let range = Range {
                            start: 0,
                            end: i + 1,
                        };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );

        // LAST_VALUE with ignore_nulls - expanding window
        group.bench_function(
            BenchmarkId::new("last_value_expanding", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator = create_evaluator(NthValueKind::Last, true, None);
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let range = Range {
                            start: 0,
                            end: i + 1,
                        };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );

        // NTH_VALUE(col, 10) with ignore_nulls - get 10th non-null value
        group.bench_function(
            BenchmarkId::new("nth_value_10_expanding", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator =
                        create_evaluator(NthValueKind::Nth, true, Some(10));
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let range = Range {
                            start: 0,
                            end: i + 1,
                        };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );

        // NTH_VALUE(col, -10) with ignore_nulls - get 10th from last non-null value
        group.bench_function(
            BenchmarkId::new("nth_value_neg10_expanding", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator =
                        create_evaluator(NthValueKind::Nth, true, Some(-10));
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let range = Range {
                            start: 0,
                            end: i + 1,
                        };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );

        // Sliding window benchmarks with 100-row window
        let window_size: usize = 100;

        group.bench_function(
            BenchmarkId::new("first_value_sliding_100", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator = create_evaluator(NthValueKind::First, true, None);
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let start = i.saturating_sub(window_size - 1);
                        let range = Range { start, end: i + 1 };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );

        group.bench_function(
            BenchmarkId::new("last_value_sliding_100", format!("{null_pct}%_nulls")),
            |b| {
                b.iter(|| {
                    let mut evaluator = create_evaluator(NthValueKind::Last, true, None);
                    let values_slice = slice::from_ref(&values);
                    for i in 0..values.len() {
                        let start = i.saturating_sub(window_size - 1);
                        let range = Range { start, end: i + 1 };
                        black_box(evaluator.evaluate(values_slice, &range).unwrap());
                    }
                })
            },
        );
    }

    group.finish();

    // Comparison benchmarks: ignore_nulls vs respect_nulls
    let mut comparison_group = c.benchmark_group("nth_value_nulls_comparison");
    let values_with_nulls =
        Arc::new(create_primitive_array::<Int64Type>(ARRAY_SIZE, 0.5)) as ArrayRef;

    // FIRST_VALUE comparison
    comparison_group.bench_function(
        BenchmarkId::new("first_value", "ignore_nulls"),
        |b| {
            b.iter(|| {
                let mut evaluator = create_evaluator(NthValueKind::First, true, None);
                let values_slice = slice::from_ref(&values_with_nulls);
                for i in 0..values_with_nulls.len() {
                    let range = Range {
                        start: 0,
                        end: i + 1,
                    };
                    black_box(evaluator.evaluate(values_slice, &range).unwrap());
                }
            })
        },
    );

    comparison_group.bench_function(
        BenchmarkId::new("first_value", "respect_nulls"),
        |b| {
            b.iter(|| {
                let mut evaluator = create_evaluator(NthValueKind::First, false, None);
                let values_slice = slice::from_ref(&values_with_nulls);
                for i in 0..values_with_nulls.len() {
                    let range = Range {
                        start: 0,
                        end: i + 1,
                    };
                    black_box(evaluator.evaluate(values_slice, &range).unwrap());
                }
            })
        },
    );

    // NTH_VALUE comparison
    comparison_group.bench_function(
        BenchmarkId::new("nth_value_10", "ignore_nulls"),
        |b| {
            b.iter(|| {
                let mut evaluator = create_evaluator(NthValueKind::Nth, true, Some(10));
                let values_slice = slice::from_ref(&values_with_nulls);
                for i in 0..values_with_nulls.len() {
                    let range = Range {
                        start: 0,
                        end: i + 1,
                    };
                    black_box(evaluator.evaluate(values_slice, &range).unwrap());
                }
            })
        },
    );

    comparison_group.bench_function(
        BenchmarkId::new("nth_value_10", "respect_nulls"),
        |b| {
            b.iter(|| {
                let mut evaluator = create_evaluator(NthValueKind::Nth, false, Some(10));
                let values_slice = slice::from_ref(&values_with_nulls);
                for i in 0..values_with_nulls.len() {
                    let range = Range {
                        start: 0,
                        end: i + 1,
                    };
                    black_box(evaluator.evaluate(values_slice, &range).unwrap());
                }
            })
        },
    );

    comparison_group.finish();
}

criterion_group!(benches, bench_nth_value_ignore_nulls);
criterion_main!(benches);
