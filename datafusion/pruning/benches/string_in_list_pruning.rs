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

//! Compare string IN-list pruning with per-value min/max expansion.
//!
//! Both cases raise `max_in_list_size` to the domain size. `in_list` follows the
//! production representation for each domain size, while the explicit
//! `expanded_or` remains a stable balanced tree of equalities. This makes the
//! small-domain cases suitable for measuring compact representation threshold
//! changes without changing the comparison baseline. Half of the statistics
//! intervals hit a domain member and half fall in a sparse gap. Bloom filters
//! are not involved.
//!
//! Run with `cargo bench -p datafusion-pruning --bench string_in_list_pruning`.
//! Construction is independent of the statistics batch size, so it varies only
//! the IN-list domain size. Evaluation varies both the domain size and number
//! of containers, reusing already-built pruning predicates and statistics.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringViewArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, col, in_list, lit};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder, PruningStatistics};

const DOMAIN_SIZES: [usize; 9] = [1, 2, 4, 8, 16, 20, 21, 256, 1024];
const CONTAINER_COUNTS: [usize; 3] = [16, 256, 4096];
const BASELINE_CONTAINER_COUNT: usize = 4096;

fn value(index: usize) -> String {
    format!("key{index:08}")
}

fn balanced_or(expressions: &[PhysicalExprRef]) -> PhysicalExprRef {
    if expressions.len() == 1 {
        return Arc::clone(&expressions[0]);
    }
    let middle = expressions.len() / 2;
    Arc::new(BinaryExpr::new(
        balanced_or(&expressions[..middle]),
        Operator::Or,
        balanced_or(&expressions[middle..]),
    ))
}

fn build_predicate(
    expression: &PhysicalExprRef,
    schema: &SchemaRef,
    max_in_list_size: usize,
) -> PruningPredicate {
    PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(schema))
        .with_max_in_list_size(max_in_list_size)
        .try_build(Arc::clone(expression))
        .unwrap()
}

struct IntervalStatistics {
    min: ArrayRef,
    max: ArrayRef,
    null_counts: ArrayRef,
    row_counts: ArrayRef,
}

impl IntervalStatistics {
    fn new(domain_size: usize, container_count: usize) -> Self {
        let min = StringViewArray::from_iter_values((0..container_count).map(|index| {
            let start = (index / 2 % domain_size) * 10;
            value(start + if index % 2 == 0 { 0 } else { 3 })
        }));
        let max = StringViewArray::from_iter_values((0..container_count).map(|index| {
            let start = (index / 2 % domain_size) * 10;
            value(start + if index % 2 == 0 { 0 } else { 7 })
        }));
        Self {
            min: Arc::new(min),
            max: Arc::new(max),
            null_counts: Arc::new(UInt64Array::from(vec![0; container_count])),
            row_counts: Arc::new(UInt64Array::from(vec![128; container_count])),
        }
    }
}

impl PruningStatistics for IntervalStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == "value").then(|| Arc::clone(&self.min))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == "value").then(|| Arc::clone(&self.max))
    }

    fn num_containers(&self) -> usize {
        self.min.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == "value").then(|| Arc::clone(&self.null_counts))
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(Arc::clone(&self.row_counts))
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

struct BenchmarkCase {
    size: usize,
    schema: SchemaRef,
    in_list: PhysicalExprRef,
    expanded_or: PhysicalExprRef,
    in_list_predicate: PruningPredicate,
    expanded_or_predicate: PruningPredicate,
}

impl BenchmarkCase {
    fn new(size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8View,
            false,
        )]));
        let column = col("value", &schema).unwrap();
        let values = (0..size)
            .map(|index| lit(ScalarValue::new_utf8view(value(index * 10))))
            .collect::<Vec<_>>();
        let in_list =
            in_list(Arc::clone(&column), values.clone(), &false, &schema).unwrap();
        let equalities = values
            .into_iter()
            .map(|value| {
                Arc::new(BinaryExpr::new(Arc::clone(&column), Operator::Eq, value))
                    as PhysicalExprRef
            })
            .collect::<Vec<_>>();
        let expanded_or = balanced_or(&equalities);
        let in_list_predicate = build_predicate(&in_list, &schema, size);
        let expanded_or_predicate = build_predicate(&expanded_or, &schema, size);
        eprintln!(
            "string_in_list_pruning: {size} values, compact={}",
            in_list_predicate
                .predicate_expr()
                .to_string()
                .contains("IN_SET_INTERSECTS")
        );
        Self {
            size,
            schema,
            in_list,
            expanded_or,
            in_list_predicate,
            expanded_or_predicate,
        }
    }
}

fn expected_results(container_count: usize) -> Vec<bool> {
    (0..container_count).map(|index| index % 2 == 0).collect()
}

fn assert_equivalent_results(case: &BenchmarkCase, statistics: &IntervalStatistics) {
    let expected = expected_results(statistics.num_containers());
    assert_eq!(case.in_list_predicate.prune(statistics).unwrap(), expected);
    assert_eq!(
        case.expanded_or_predicate.prune(statistics).unwrap(),
        expected
    );
}

fn evaluation_group_name(container_count: usize) -> String {
    // Keep the original benchmark IDs for the pre-existing 4,096-container
    // cases so Criterion baselines remain directly comparable.
    if container_count == BASELINE_CONTAINER_COUNT {
        "string_in_list_pruning/evaluate".to_string()
    } else {
        format!("string_in_list_pruning/evaluate/{container_count}_containers")
    }
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let cases = DOMAIN_SIZES.map(BenchmarkCase::new);
    let mut construction = criterion.benchmark_group("string_in_list_pruning/construct");
    for case in &cases {
        construction.throughput(Throughput::Elements(case.size as u64));
        for (name, expression) in [
            ("in_list", &case.in_list),
            ("expanded_or", &case.expanded_or),
        ] {
            construction.bench_with_input(
                BenchmarkId::new(name, case.size),
                expression,
                |bencher, expression| {
                    bencher.iter(|| {
                        black_box(build_predicate(
                            black_box(expression),
                            &case.schema,
                            case.size,
                        ))
                    });
                },
            );
        }
    }
    construction.finish();

    for container_count in CONTAINER_COUNTS {
        let mut evaluation =
            criterion.benchmark_group(evaluation_group_name(container_count));
        evaluation.throughput(Throughput::Elements(container_count as u64));
        for case in &cases {
            let statistics = IntervalStatistics::new(case.size, container_count);

            // Check every matrix cell outside the timed loop so both paths do
            // the same useful work rather than measuring an always-true fallback.
            assert_equivalent_results(case, &statistics);

            for (name, predicate) in [
                ("in_list", &case.in_list_predicate),
                ("expanded_or", &case.expanded_or_predicate),
            ] {
                evaluation.bench_with_input(
                    BenchmarkId::new(name, case.size),
                    predicate,
                    |bencher, predicate| {
                        bencher.iter(|| {
                            black_box(predicate.prune(black_box(&statistics)).unwrap())
                        });
                    },
                );
            }
        }
        evaluation.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
