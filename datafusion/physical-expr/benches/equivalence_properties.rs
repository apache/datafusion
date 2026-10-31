// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for the ordering satisfaction checks on [`EquivalenceProperties`].
//!
//! These are called repeatedly during physical optimization (sort removal,
//! `EnforceSorting`, `EnforceDistribution`, and the requirement checks for
//! windows, joins and aggregates), so their cost shows up directly in planning
//! time.
//!
//! # Scope
//!
//! These measure the satisfaction check itself, not the cost of assembling its
//! arguments. The sort expressions, requirements and orderings are built once,
//! up front. Because the checks take their input by value, each iteration gets a
//! fresh copy from the untimed setup step of `iter_batched`; only the call is
//! timed. Any copying the check does internally is part of what is measured.
//!
//! The benchmarks are parameterized by the number of equivalence classes, since
//! that -- not the schema width, which sits behind an `Arc` -- is what these
//! checks carry around.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, lit};
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

    // Built once; see the "Scope" note at the top of this file.
    let one_key = vec![asc(0)];
    // `c7` leads none of the orderings, so this exits on the first key.
    let one_key_unsatisfied = vec![asc(7)];
    let four_keys = (0..4).map(asc).collect::<Vec<_>>();
    let four_key_ordering = LexOrdering::new(four_keys.clone()).unwrap();
    let one_req = vec![PhysicalSortRequirement::new(col(0), None)];
    let four_reqs = (0..4)
        .map(|i| PhysicalSortRequirement::new(col(i), None))
        .collect::<Vec<_>>();

    for n_classes in [2, 8, 32] {
        let props = properties(n_classes);
        // Check each answer once, untimed, so that a wrong answer cannot pass
        // for a faster one.
        assert!(props.ordering_satisfy(one_key.clone()).unwrap());
        assert!(!props.ordering_satisfy(one_key_unsatisfied.clone()).unwrap());
        assert!(props.ordering_satisfy(four_keys.clone()).unwrap());
        assert!(props.ordering_satisfy_requirement(one_req.clone()).unwrap());
        assert!(
            props
                .ordering_satisfy_requirement(four_reqs.clone())
                .unwrap()
        );
        let (_, all_satisfied) = props
            .extract_common_sort_prefix(four_key_ordering.clone())
            .unwrap();
        assert!(all_satisfied);

        // A single sort key: the most common shape by far.
        group.bench_function(
            BenchmarkId::new("ordering_satisfy/1_key", n_classes),
            |b| {
                b.iter_batched(
                    || one_key.clone(),
                    |keys| props.ordering_satisfy(keys).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("ordering_satisfy/1_key_unsatisfied", n_classes),
            |b| {
                b.iter_batched(
                    || one_key_unsatisfied.clone(),
                    |keys| props.ordering_satisfy(keys).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
        // Four sort keys: each key only has to be sorted among the rows that tie
        // on the keys before it.
        group.bench_function(
            BenchmarkId::new("ordering_satisfy/4_keys", n_classes),
            |b| {
                b.iter_batched(
                    || four_keys.clone(),
                    |keys| props.ordering_satisfy(keys).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("ordering_satisfy_requirement/1_key", n_classes),
            |b| {
                b.iter_batched(
                    || one_req.clone(),
                    |reqs| props.ordering_satisfy_requirement(reqs).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("ordering_satisfy_requirement/4_keys", n_classes),
            |b| {
                b.iter_batched(
                    || four_reqs.clone(),
                    |reqs| props.ordering_satisfy_requirement(reqs).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("extract_common_sort_prefix/4_keys", n_classes),
            |b| {
                b.iter_batched(
                    || four_key_ordering.clone(),
                    |ordering| props.extract_common_sort_prefix(ordering).unwrap(),
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Other shapes: `find_longest_permutation`, the orderings that window
/// aggregates add, and a deeply nested sort key.
fn bench_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("equivalence_properties_shapes");
    let opts = [(false, false), (false, true), (true, false), (true, true)].map(
        |(descending, nulls_first)| SortOptions {
            descending,
            nulls_first,
        },
    );
    let asc_nulls_last = opts[0];

    // `find_longest_permutation` with one expression, and with five: `c0`,
    // `c1`, `c2` and `c3` are found one per round, `c7` never.
    for n_classes in [2, 32] {
        let props = properties(n_classes);
        let one = vec![col(0)];
        let five = vec![col(3), col(2), col(1), col(0), col(7)];
        assert_eq!(props.find_longest_permutation(&one).unwrap().1, [0]);
        assert_eq!(
            props.find_longest_permutation(&five).unwrap().1,
            [3, 2, 1, 0]
        );
        group.bench_function(
            BenchmarkId::new("find_longest_permutation/1_expr", n_classes),
            |b| b.iter(|| props.find_longest_permutation(&one).unwrap()),
        );
        group.bench_function(
            BenchmarkId::new("find_longest_permutation/5_exprs", n_classes),
            |b| b.iter(|| props.find_longest_permutation(&five).unwrap()),
        );
    }

    // `w` window aggregates over a whole partition, such as
    // `SUM(x) OVER (PARTITION BY c0)`: each is constant per `c0`, which is
    // recorded by adding `[c0, w_i]` in all four directions of `w_i`
    // (columns `c1..=cw`). The requirement is `[c0, c1]`.
    for w in [4, 16, 32] {
        let mut props = EquivalenceProperties::new(schema(w + 1));
        let mut orderings = vec![];
        for i in 1..=w {
            for o in opts {
                orderings.push(vec![
                    PhysicalSortExpr::new(col(0), asc_nulls_last),
                    PhysicalSortExpr::new(col(i), o),
                ]);
            }
        }
        props.add_orderings(orderings);
        let keys = vec![
            PhysicalSortExpr::new(col(0), asc_nulls_last),
            PhysicalSortExpr::new(col(1), asc_nulls_last),
        ];
        assert!(props.ordering_satisfy(keys.clone()).unwrap());
        group.bench_function(BenchmarkId::new("window", w), |b| {
            b.iter_batched(
                || keys.clone(),
                |keys| props.ordering_satisfy(keys).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    // Deep key: `[c0, e, c1]` with `e = c1 + 1 + ... + 1` nested `depth` deep.
    // `+` may overflow, so `e` is unordered: the check computes the properties
    // of `e` and stops there.
    for depth in [4, 8, 16, 32] {
        let props = properties(8);
        let mut e = col(1);
        for _ in 0..depth {
            e = Arc::new(BinaryExpr::new(e, Operator::Plus, lit(1i32)));
        }
        let keys = vec![
            asc(0),
            PhysicalSortExpr::new(e, SortOptions::default()),
            asc(1),
        ];
        assert!(!props.ordering_satisfy(keys.clone()).unwrap());
        group.bench_function(BenchmarkId::new("deep_key", depth), |b| {
            b.iter_batched(
                || keys.clone(),
                |keys| props.ordering_satisfy(keys).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ordering_satisfaction, bench_shapes);
criterion_main!(benches);
