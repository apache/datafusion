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

//! Compare compact string IN-list pruning with per-value min/max expansion.
//!
//! Every case raises `max_in_list_size` to the domain size. On a baseline
//! without compact pruning, `in_list` and `not_in_list` measure the ordinary
//! raised-cap path. The explicit `expanded_or` and `expanded_and` are balanced
//! trees of (in)equalities, which produce the same per-value statistics checks
//! without making the baseline depend on a deeply nested expression.
//!
//! Half of the statistics intervals are pinned to a domain member and half span
//! a sparse gap, so `IN` keeps the first half and `NOT IN` keeps the second.
//! Both directions therefore prune real containers rather than measuring an
//! always-true fallback. Bloom filters are not involved.
//!
//! Run with `cargo bench -p datafusion-pruning --bench string_in_list_pruning`.
//! The construction benchmarks reuse their input physical expressions; the
//! evaluation benchmarks reuse their already-built pruning predicates.

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

const DOMAIN_SIZES: [usize; 4] = [20, 21, 256, 1024];
const CONTAINERS: usize = 4096;

fn value(index: usize) -> String {
    format!("key{index:08}")
}

fn balanced(expressions: &[PhysicalExprRef], op: Operator) -> PhysicalExprRef {
    if expressions.len() == 1 {
        return Arc::clone(&expressions[0]);
    }
    let middle = expressions.len() / 2;
    Arc::new(BinaryExpr::new(
        balanced(&expressions[..middle], op),
        op,
        balanced(&expressions[middle..], op),
    ))
}

/// A balanced tree of `column <op> value`, one branch per domain member.
fn expanded(
    column: &PhysicalExprRef,
    values: &[PhysicalExprRef],
    op: Operator,
    combine: Operator,
) -> PhysicalExprRef {
    let comparisons = values
        .iter()
        .map(|value| {
            Arc::new(BinaryExpr::new(Arc::clone(column), op, Arc::clone(value)))
                as PhysicalExprRef
        })
        .collect::<Vec<_>>();
    balanced(&comparisons, combine)
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
    fn new(domain_size: usize) -> Self {
        let min = StringViewArray::from_iter_values((0..CONTAINERS).map(|index| {
            let start = (index / 2 % domain_size) * 10;
            value(start + if index % 2 == 0 { 0 } else { 3 })
        }));
        let max = StringViewArray::from_iter_values((0..CONTAINERS).map(|index| {
            let start = (index / 2 % domain_size) * 10;
            value(start + if index % 2 == 0 { 0 } else { 7 })
        }));
        Self {
            min: Arc::new(min),
            max: Arc::new(max),
            null_counts: Arc::new(UInt64Array::from(vec![0; CONTAINERS])),
            row_counts: Arc::new(UInt64Array::from(vec![128; CONTAINERS])),
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
        CONTAINERS
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
    not_in_list: PhysicalExprRef,
    expanded_and: PhysicalExprRef,
    in_list_predicate: PruningPredicate,
    expanded_or_predicate: PruningPredicate,
    not_in_list_predicate: PruningPredicate,
    expanded_and_predicate: PruningPredicate,
    statistics: IntervalStatistics,
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
        let not_in_list =
            in_list(Arc::clone(&column), values.clone(), &true, &schema).unwrap();
        let in_list =
            in_list(Arc::clone(&column), values.clone(), &false, &schema).unwrap();
        let expanded_or = expanded(&column, &values, Operator::Eq, Operator::Or);
        let expanded_and = expanded(&column, &values, Operator::NotEq, Operator::And);
        let in_list_predicate = build_predicate(&in_list, &schema, size);
        let expanded_or_predicate = build_predicate(&expanded_or, &schema, size);
        let not_in_list_predicate = build_predicate(&not_in_list, &schema, size);
        let expanded_and_predicate = build_predicate(&expanded_and, &schema, size);
        eprintln!(
            "string_in_list_pruning: {size} values, compact in={}, compact not in={}",
            in_list_predicate
                .predicate_expr()
                .to_string()
                .contains("IN_SET_INTERSECTS"),
            not_in_list_predicate
                .predicate_expr()
                .to_string()
                .contains("NOT_IN_SET_MAY_MATCH")
        );
        let statistics = IntervalStatistics::new(size);

        // Check that every benchmark path does the same useful work, rather
        // than comparing compact pruning with an always-true fallback.
        let kept = (0..CONTAINERS)
            .map(|index| index % 2 == 0)
            .collect::<Vec<_>>();
        let negated_kept = kept.iter().map(|keep| !keep).collect::<Vec<_>>();
        assert_eq!(in_list_predicate.prune(&statistics).unwrap(), kept);
        assert_eq!(expanded_or_predicate.prune(&statistics).unwrap(), kept);
        assert_eq!(
            not_in_list_predicate.prune(&statistics).unwrap(),
            negated_kept
        );
        assert_eq!(
            expanded_and_predicate.prune(&statistics).unwrap(),
            negated_kept
        );

        Self {
            size,
            schema,
            in_list,
            expanded_or,
            not_in_list,
            expanded_and,
            in_list_predicate,
            expanded_or_predicate,
            not_in_list_predicate,
            expanded_and_predicate,
            statistics,
        }
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
            ("not_in_list", &case.not_in_list),
            ("expanded_and", &case.expanded_and),
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

    let mut evaluation = criterion.benchmark_group("string_in_list_pruning/evaluate");
    evaluation.throughput(Throughput::Elements(CONTAINERS as u64));
    for case in &cases {
        for (name, predicate) in [
            ("in_list", &case.in_list_predicate),
            ("expanded_or", &case.expanded_or_predicate),
            ("not_in_list", &case.not_in_list_predicate),
            ("expanded_and", &case.expanded_and_predicate),
        ] {
            evaluation.bench_with_input(
                BenchmarkId::new(name, case.size),
                predicate,
                |bencher, predicate| {
                    bencher.iter(|| {
                        black_box(predicate.prune(black_box(&case.statistics)).unwrap())
                    });
                },
            );
        }
    }
    evaluation.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
