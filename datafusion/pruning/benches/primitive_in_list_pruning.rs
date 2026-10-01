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

//! Compare primitive IN-list pruning with per-value min/max expansion.
//!
//! The domain and container matrices match `string_in_list_pruning`. The
//! compact form uses a typed contiguous domain, while `expanded_or` and
//! `expanded_and` measure balanced per-value comparison trees.
//!
//! Run with `cargo bench -p datafusion-pruning --bench primitive_in_list_pruning`.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{
    BinaryExpr, col, in_list as make_in_list, lit,
};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder, PruningStatistics};

const DOMAIN_SIZES: [usize; 9] = [1, 2, 4, 8, 16, 20, 21, 256, 1024];
const CONTAINER_COUNTS: [usize; 3] = [16, 256, 4096];

fn sampled_domain_index(
    pair_index: usize,
    pair_count: usize,
    domain_size: usize,
) -> usize {
    let sampled_positions = pair_count.min(domain_size);
    let position = pair_index % sampled_positions;
    if sampled_positions == 1 {
        0
    } else {
        position * (domain_size - 1) / (sampled_positions - 1)
    }
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
    fn new(domain_size: usize, container_count: usize) -> Self {
        let pair_count = container_count.div_ceil(2);
        let min = Int64Array::from_iter_values((0..container_count).map(|index| {
            let start =
                sampled_domain_index(index / 2, pair_count, domain_size) as i64 * 10;
            start + if index % 2 == 0 { 0 } else { 3 }
        }));
        let max = Int64Array::from_iter_values((0..container_count).map(|index| {
            let start =
                sampled_domain_index(index / 2, pair_count, domain_size) as i64 * 10;
            start + if index % 2 == 0 { 0 } else { 7 }
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
    not_in_list: PhysicalExprRef,
    expanded_and: PhysicalExprRef,
    in_list_predicate: PruningPredicate,
    expanded_or_predicate: PruningPredicate,
    not_in_list_predicate: PruningPredicate,
    expanded_and_predicate: PruningPredicate,
}

impl BenchmarkCase {
    fn new(size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let column = col("value", &schema).unwrap();
        let values = (0..size)
            .map(|index| lit(ScalarValue::Int64(Some(index as i64 * 10))))
            .collect::<Vec<_>>();
        let in_list =
            make_in_list(Arc::clone(&column), values.clone(), &false, &schema).unwrap();
        let not_in_list =
            make_in_list(Arc::clone(&column), values.clone(), &true, &schema).unwrap();
        let expanded_or = expanded(&column, &values, Operator::Eq, Operator::Or);
        let expanded_and = expanded(&column, &values, Operator::NotEq, Operator::And);
        let in_list_predicate = build_predicate(&in_list, &schema, size);
        let expanded_or_predicate = build_predicate(&expanded_or, &schema, size);
        let not_in_list_predicate = build_predicate(&not_in_list, &schema, size);
        let expanded_and_predicate = build_predicate(&expanded_and, &schema, size);
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
        }
    }
}

fn assert_equivalent_results(case: &BenchmarkCase, statistics: &IntervalStatistics) {
    let expected = (0..statistics.num_containers())
        .map(|index| index % 2 == 0)
        .collect::<Vec<_>>();
    assert_eq!(case.in_list_predicate.prune(statistics).unwrap(), expected);
    assert_eq!(
        case.expanded_or_predicate.prune(statistics).unwrap(),
        expected
    );
    let negated = expected.into_iter().map(|value| !value).collect::<Vec<_>>();
    assert_eq!(
        case.not_in_list_predicate.prune(statistics).unwrap(),
        negated
    );
    assert_eq!(
        case.expanded_and_predicate.prune(statistics).unwrap(),
        negated
    );
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let cases = DOMAIN_SIZES.map(BenchmarkCase::new);
    let mut construction =
        criterion.benchmark_group("primitive_in_list_pruning/construct");
    for case in &cases {
        for (name, expression) in [
            ("in_list", &case.in_list),
            ("expanded_or", &case.expanded_or),
            ("not_in_list", &case.not_in_list),
            ("expanded_and", &case.expanded_and),
        ] {
            construction.throughput(Throughput::Elements(case.size as u64));
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
        let mut evaluation = criterion.benchmark_group(format!(
            "primitive_in_list_pruning/evaluate/{container_count}_containers"
        ));
        evaluation.throughput(Throughput::Elements(container_count as u64));
        for case in &cases {
            let statistics = IntervalStatistics::new(case.size, container_count);
            assert_equivalent_results(case, &statistics);
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
