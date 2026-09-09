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
//! The main cases alternate intervals pinned to a domain member with intervals
//! that span a sparse gap. NULL-containing variants cover the optimized
//! filter-semantics paths, including the constant-false `NOT IN` rewrite.
//! Supplemental `NOT IN` cases measure uniform singleton containers, long bounds,
//! and long literals that share bounds' prefixes. Bloom filters are not involved.
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

    fn uniform_singleton() -> Self {
        let value = value(0);
        Self::singleton(&value)
    }

    fn singleton(value: &str) -> Self {
        let min =
            StringViewArray::from_iter_values(std::iter::repeat_n(value, CONTAINERS));
        let max = min.clone();
        Self {
            min: Arc::new(min),
            max: Arc::new(max),
            null_counts: Arc::new(UInt64Array::from(vec![0; CONTAINERS])),
            row_counts: Arc::new(UInt64Array::from(vec![128; CONTAINERS])),
        }
    }

    fn long_common_prefix() -> Self {
        let prefix = "z".repeat(16 * 1024);
        let min = format!("{prefix}a");
        let max = format!("{prefix}z");
        Self {
            min: Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                min.as_str(),
                CONTAINERS,
            ))),
            max: Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                max.as_str(),
                CONTAINERS,
            ))),
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
    in_list_with_null: PhysicalExprRef,
    expanded_or: PhysicalExprRef,
    not_in_list: PhysicalExprRef,
    not_in_list_with_null: PhysicalExprRef,
    expanded_and: PhysicalExprRef,
    in_list_predicate: PruningPredicate,
    in_list_with_null_predicate: PruningPredicate,
    expanded_or_predicate: PruningPredicate,
    not_in_list_predicate: PruningPredicate,
    not_in_list_with_null_predicate: PruningPredicate,
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
        let mut values_with_null = values.clone();
        values_with_null.push(lit(ScalarValue::Utf8View(None)));
        let not_in_list =
            in_list(Arc::clone(&column), values.clone(), &true, &schema).unwrap();
        let not_in_list_with_null = in_list(
            Arc::clone(&column),
            values_with_null.clone(),
            &true,
            &schema,
        )
        .unwrap();
        let in_list_with_null =
            in_list(Arc::clone(&column), values_with_null, &false, &schema).unwrap();
        let in_list =
            in_list(Arc::clone(&column), values.clone(), &false, &schema).unwrap();
        let expanded_or = expanded(&column, &values, Operator::Eq, Operator::Or);
        let expanded_and = expanded(&column, &values, Operator::NotEq, Operator::And);
        let in_list_predicate = build_predicate(&in_list, &schema, size);
        let in_list_with_null_predicate =
            build_predicate(&in_list_with_null, &schema, size + 1);
        let expanded_or_predicate = build_predicate(&expanded_or, &schema, size);
        let not_in_list_predicate = build_predicate(&not_in_list, &schema, size);
        let not_in_list_with_null_predicate =
            build_predicate(&not_in_list_with_null, &schema, size + 1);
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
        assert_eq!(
            in_list_with_null_predicate.prune(&statistics).unwrap(),
            kept
        );
        assert_eq!(expanded_or_predicate.prune(&statistics).unwrap(), kept);
        assert_eq!(
            not_in_list_predicate.prune(&statistics).unwrap(),
            negated_kept
        );
        assert_eq!(
            expanded_and_predicate.prune(&statistics).unwrap(),
            negated_kept
        );
        assert!(
            not_in_list_with_null_predicate
                .prune(&statistics)
                .unwrap()
                .iter()
                .all(|keep| !keep)
        );

        Self {
            size,
            schema,
            in_list,
            in_list_with_null,
            expanded_or,
            not_in_list,
            not_in_list_with_null,
            expanded_and,
            in_list_predicate,
            in_list_with_null_predicate,
            expanded_or_predicate,
            not_in_list_predicate,
            not_in_list_with_null_predicate,
            expanded_and_predicate,
            statistics,
        }
    }
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let cases = DOMAIN_SIZES.map(BenchmarkCase::new);
    let mut construction = criterion.benchmark_group("string_in_list_pruning/construct");
    for case in &cases {
        for (name, expression, max_in_list_size) in [
            ("in_list", &case.in_list, case.size),
            ("in_list_with_null", &case.in_list_with_null, case.size + 1),
            ("expanded_or", &case.expanded_or, case.size),
            ("not_in_list", &case.not_in_list, case.size),
            (
                "not_in_list_with_null_constant_false",
                &case.not_in_list_with_null,
                case.size + 1,
            ),
            ("expanded_and", &case.expanded_and, case.size),
        ] {
            construction.throughput(Throughput::Elements(max_in_list_size as u64));
            construction.bench_with_input(
                BenchmarkId::new(name, max_in_list_size),
                expression,
                |bencher, expression| {
                    bencher.iter(|| {
                        black_box(build_predicate(
                            black_box(expression),
                            &case.schema,
                            max_in_list_size,
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
        for (name, predicate, list_size) in [
            ("in_list", &case.in_list_predicate, case.size),
            (
                "in_list_with_null",
                &case.in_list_with_null_predicate,
                case.size + 1,
            ),
            ("expanded_or", &case.expanded_or_predicate, case.size),
            ("not_in_list", &case.not_in_list_predicate, case.size),
            (
                "not_in_list_with_null_constant_false",
                &case.not_in_list_with_null_predicate,
                case.size + 1,
            ),
            ("expanded_and", &case.expanded_and_predicate, case.size),
        ] {
            evaluation.bench_with_input(
                BenchmarkId::new(name, list_size),
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

    let mut distributions =
        criterion.benchmark_group("string_in_list_pruning/not_in_distributions");
    distributions.throughput(Throughput::Elements(CONTAINERS as u64));
    let singleton = IntervalStatistics::uniform_singleton();
    for case in cases.iter().filter(|case| case.size > 20) {
        let compact = case.not_in_list_predicate.prune(&singleton).unwrap();
        assert!(compact.iter().all(|keep| !keep));
        assert_eq!(
            compact,
            case.expanded_and_predicate.prune(&singleton).unwrap()
        );
        for (name, predicate) in [
            ("uniform_singleton/not_in_list", &case.not_in_list_predicate),
            (
                "uniform_singleton/expanded_and",
                &case.expanded_and_predicate,
            ),
        ] {
            distributions.bench_with_input(
                BenchmarkId::new(name, case.size),
                predicate,
                |bencher, predicate| {
                    bencher.iter(|| {
                        black_box(predicate.prune(black_box(&singleton)).unwrap())
                    });
                },
            );
        }
    }

    let long_bounds = IntervalStatistics::long_common_prefix();
    let case = cases
        .iter()
        .find(|case| case.size == 21)
        .expect("DOMAIN_SIZES contains 21");
    let compact = case.not_in_list_predicate.prune(&long_bounds).unwrap();
    assert!(compact.iter().all(|keep| *keep));
    assert_eq!(
        compact,
        case.expanded_and_predicate.prune(&long_bounds).unwrap()
    );
    for (name, predicate) in [
        (
            "long_common_prefix/not_in_list",
            &case.not_in_list_predicate,
        ),
        (
            "long_common_prefix/expanded_and",
            &case.expanded_and_predicate,
        ),
    ] {
        distributions.bench_with_input(
            BenchmarkId::new(name, case.size),
            predicate,
            |bencher, predicate| {
                bencher.iter(|| {
                    black_box(predicate.prune(black_box(&long_bounds)).unwrap())
                });
            },
        );
    }

    // The bound and literals share a long prefix but have different lengths,
    // so exact membership can reject the bound without comparing their bytes.
    let prefix = "z".repeat(16 * 1024);
    let bound = format!("{prefix}a");
    let long_literal_bounds = IntervalStatistics::singleton(&bound);
    let values = (0..case.size)
        .map(|index| {
            lit(ScalarValue::new_utf8view(format!(
                "{prefix}domain{index:08}"
            )))
        })
        .collect::<Vec<_>>();
    let column = col("value", &case.schema).unwrap();
    let not_in_list =
        in_list(Arc::clone(&column), values.clone(), &true, &case.schema).unwrap();
    let expanded_and = expanded(&column, &values, Operator::NotEq, Operator::And);
    let not_in_list_predicate = build_predicate(&not_in_list, &case.schema, case.size);
    let expanded_and_predicate = build_predicate(&expanded_and, &case.schema, case.size);
    let compact = not_in_list_predicate.prune(&long_literal_bounds).unwrap();
    assert!(compact.iter().all(|keep| *keep));
    assert_eq!(
        compact,
        expanded_and_predicate.prune(&long_literal_bounds).unwrap()
    );
    for (name, predicate) in [
        ("long_literal_prefix/not_in_list", &not_in_list_predicate),
        ("long_literal_prefix/expanded_and", &expanded_and_predicate),
    ] {
        distributions.bench_with_input(
            BenchmarkId::new(name, case.size),
            predicate,
            |bencher, predicate| {
                bencher.iter(|| {
                    black_box(predicate.prune(black_box(&long_literal_bounds)).unwrap())
                });
            },
        );
    }
    distributions.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
