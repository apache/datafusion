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

//! Benchmarks for the ordering satisfaction checks on [`EquivalenceProperties`].
//!
//! These are called repeatedly during physical optimization (sort removal,
//! `EnforceSorting`, `EnforceDistribution`, and the requirement checks for
//! windows, joins and aggregates), so their cost shows up directly in planning
//! time.
//!
//! The benchmarks are parameterized by the number of equivalence classes, since
//! that -- not the schema width, which is behind an `Arc` -- is what these
//! checks carry around.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};

fn schema(n_cols: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..n_cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ))
}

fn col(i: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(&format!("c{i}"), i))
}

fn asc(i: usize) -> PhysicalSortExpr {
    PhysicalSortExpr::new(col(i), SortOptions::default())
}

/// Properties with three equivalent orderings and `n_classes` equivalence
/// classes, i.e. roughly what a scan feeding a join and a window function looks
/// like. Columns `c0..c7` carry the orderings; the equivalence classes are built
/// from the columns above them.
fn properties(n_classes: usize) -> EquivalenceProperties {
    let schema = schema(8 + 2 * n_classes);
    let mut props = EquivalenceProperties::new(schema);
    props.add_orderings([
        vec![asc(0), asc(1), asc(2), asc(3)],
        vec![asc(4), asc(5)],
        vec![asc(6)],
    ]);
    for i in 0..n_classes {
        props
            .add_equal_conditions(col(8 + 2 * i), col(9 + 2 * i))
            .unwrap();
    }
    props
}

fn bench_ordering_satisfaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("equivalence_properties");

    for n_classes in [2, 8, 32] {
        let props = properties(n_classes);

        // A single sort key: the most common shape by far.
        group.bench_with_input(
            BenchmarkId::new("ordering_satisfy/1_key", n_classes),
            &n_classes,
            |b, _| b.iter(|| props.ordering_satisfy([asc(0)]).unwrap()),
        );
        // A single sort key that is not satisfied: exits on the first key.
        group.bench_with_input(
            BenchmarkId::new("ordering_satisfy/1_key_unsatisfied", n_classes),
            &n_classes,
            |b, _| b.iter(|| props.ordering_satisfy([asc(7)]).unwrap()),
        );
        // Four sort keys: exercises the per-key constant registration.
        group.bench_with_input(
            BenchmarkId::new("ordering_satisfy/4_keys", n_classes),
            &n_classes,
            |b, _| {
                b.iter(|| {
                    props
                        .ordering_satisfy([asc(0), asc(1), asc(2), asc(3)])
                        .unwrap()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordering_satisfy_requirement/1_key", n_classes),
            &n_classes,
            |b, _| {
                b.iter(|| {
                    props
                        .ordering_satisfy_requirement([PhysicalSortRequirement::new(
                            col(0),
                            None,
                        )])
                        .unwrap()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordering_satisfy_requirement/4_keys", n_classes),
            &n_classes,
            |b, _| {
                b.iter(|| {
                    props
                        .ordering_satisfy_requirement(
                            (0..4).map(|i| PhysicalSortRequirement::new(col(i), None)),
                        )
                        .unwrap()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("extract_common_sort_prefix/4_keys", n_classes),
            &n_classes,
            |b, _| {
                b.iter(|| {
                    let ordering =
                        LexOrdering::new((0..4).map(asc).collect::<Vec<_>>()).unwrap();
                    props.extract_common_sort_prefix(ordering).unwrap()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_ordering_satisfaction);
criterion_main!(benches);
