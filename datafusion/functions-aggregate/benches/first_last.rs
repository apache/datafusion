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

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, MapArray, StringArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Fields, Float64Type, Int64Type, Schema};
use arrow::util::bench_util::{
    create_boolean_array, create_primitive_array, create_string_array_with_len,
};
use datafusion_common::instant::Instant;
use std::hint::black_box;
use std::sync::Arc;

use datafusion_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, function::AccumulatorArgs,
};
use datafusion_functions_aggregate::first_last::{
    FirstValue, LastValue, TrivialFirstValueAccumulator, TrivialLastValueAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::GroupsAccumulatorAdapter;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::col;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

/// Build a `GroupsAccumulator` for an arbitrary value type, so the nested-type
/// (`Struct` / `List`) fast paths added for `first_value` / `last_value` can be
/// exercised with the same harness as the primitive ones.
fn prepare_typed_groups_accumulator(
    is_first: bool,
    value_type: DataType,
) -> Box<dyn GroupsAccumulator> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", value_type.clone(), true),
        Field::new("ord", DataType::Int64, true),
    ]));

    let order_expr = col("ord", &schema).unwrap();
    let sort_expr = PhysicalSortExpr {
        expr: order_expr,
        options: SortOptions::default(),
    };

    let value_field: Arc<Field> = Field::new("value", value_type.clone(), true).into();
    let value_expr = col("value", &schema).unwrap();
    let make_args = || AccumulatorArgs {
        return_field: Arc::clone(&value_field),
        schema: &schema,
        expr_fields: std::slice::from_ref(&value_field),
        ignore_nulls: false,
        order_bys: std::slice::from_ref(&sort_expr),
        is_reversed: false,
        name: if is_first {
            "FIRST_VALUE(value ORDER BY ord)"
        } else {
            "LAST_VALUE(value ORDER BY ord)"
        },
        is_distinct: false,
        exprs: std::slice::from_ref(&value_expr),
    };

    // Mirror the planner: use the native GroupsAccumulator when this value type
    // is supported and otherwise fall back to a GroupsAccumulatorAdapter around
    // one per-group Accumulator. Deciding with `groups_accumulator_supported`
    // (rather than catching `create_groups_accumulator` errors) keeps genuine
    // construction failures loud. The same case then runs the fallback on a
    // build without native nested support and the native path on one with it,
    // so a before/after benchmark run surfaces the win directly.
    let supported = if is_first {
        FirstValue::new().groups_accumulator_supported(make_args())
    } else {
        LastValue::new().groups_accumulator_supported(make_args())
    };
    if !supported {
        return build_fallback_adapter(is_first, value_type);
    }
    if is_first {
        FirstValue::new()
            .create_groups_accumulator(make_args())
            .unwrap()
    } else {
        LastValue::new()
            .create_groups_accumulator(make_args())
            .unwrap()
    }
}

/// Build the *fallback* grouped accumulator for a value type: a
/// `GroupsAccumulatorAdapter` wrapping one per-group `Accumulator`. This is
/// exactly what nested value types (`List` / `Struct` / `Map`) used before
/// they gained a native `GroupsAccumulator`, and it is what the planner still
/// selects when `groups_accumulator_supported` returns `false`. Benching this
/// side by side with `prepare_typed_groups_accumulator` (the native path)
/// shows the win from the native `GroupsAccumulator`.
fn build_fallback_adapter(
    is_first: bool,
    value_type: DataType,
) -> Box<dyn GroupsAccumulator> {
    Box::new(GroupsAccumulatorAdapter::new(move || {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", value_type.clone(), true),
            Field::new("ord", DataType::Int64, true),
        ]));
        let sort_expr = PhysicalSortExpr {
            expr: col("ord", &schema)?,
            options: SortOptions::default(),
        };
        let value_field: Arc<Field> =
            Field::new("value", value_type.clone(), true).into();
        let value_expr = col("value", &schema)?;
        let accumulator_args = AccumulatorArgs {
            return_field: Arc::clone(&value_field),
            schema: &schema,
            expr_fields: std::slice::from_ref(&value_field),
            ignore_nulls: false,
            order_bys: std::slice::from_ref(&sort_expr),
            is_reversed: false,
            name: if is_first {
                "FIRST_VALUE(value ORDER BY ord)"
            } else {
                "LAST_VALUE(value ORDER BY ord)"
            },
            is_distinct: false,
            exprs: std::slice::from_ref(&value_expr),
        };
        if is_first {
            FirstValue::new().accumulator(accumulator_args)
        } else {
            LastValue::new().accumulator(accumulator_args)
        }
    }))
}

fn create_trivial_accumulator(
    is_first: bool,
    ignore_nulls: bool,
) -> Box<dyn Accumulator> {
    if is_first {
        Box::new(
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, ignore_nulls)
                .unwrap(),
        )
    } else {
        Box::new(
            TrivialLastValueAccumulator::try_new(&DataType::Int64, ignore_nulls).unwrap(),
        )
    }
}

#[expect(clippy::needless_pass_by_value)]
#[expect(clippy::too_many_arguments)]
fn evaluate_bench(
    c: &mut Criterion,
    is_first: bool,
    emit_to: EmitTo,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();
    let value_type = values.data_type().clone();

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut accumulator =
                    prepare_typed_groups_accumulator(is_first, value_type.clone());
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&ord)],
                        &group_indices,
                        opt_filter,
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                black_box(accumulator.evaluate(emit_to).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn update_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();
    let value_type = values.data_type().clone();

    // Initialize with worst-case ordering so update_batch forces rows comparison for all groups.
    let worst_ord: ArrayRef = Arc::new(Int64Array::from(vec![
        if is_first {
            i64::MAX
        } else {
            i64::MIN
        };
        n
    ]));

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut accumulator =
                    prepare_typed_groups_accumulator(is_first, value_type.clone());
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&worst_ord)],
                        &group_indices,
                        None, // no filter: ensure all groups are initialised
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                for _ in 0..100 {
                    #[expect(clippy::unit_arg)]
                    black_box(
                        accumulator
                            .update_batch(
                                &[Arc::clone(&values), Arc::clone(&ord)],
                                &group_indices,
                                opt_filter,
                                num_groups,
                            )
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn merge_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();
    let is_set: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
    let value_type = values.data_type().clone();

    // Initialize with worst-case ordering so update_batch forces rows comparison for all groups.
    let worst_ord: ArrayRef = Arc::new(Int64Array::from(vec![
        if is_first {
            i64::MAX
        } else {
            i64::MIN
        };
        n
    ]));

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                // Prebuild accumulator
                let mut accumulator =
                    prepare_typed_groups_accumulator(is_first, value_type.clone());
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&worst_ord)],
                        &group_indices,
                        opt_filter,
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                for _ in 0..100 {
                    #[expect(clippy::unit_arg)]
                    black_box(
                        accumulator
                            .merge_batch(
                                &[
                                    Arc::clone(&values),
                                    Arc::clone(&ord),
                                    Arc::clone(&is_set),
                                ],
                                &group_indices,
                                num_groups,
                            )
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn trivial_update_bench(
    c: &mut Criterion,
    is_first: bool,
    ignore_nulls: bool,
    name: &str,
    values: ArrayRef,
) {
    c.bench_function(name, |b| {
        b.iter_custom(|iters| {
            // The bench is way too fast, so apply scaling factor
            let mut accumulators: Vec<Box<dyn Accumulator>> = (0..iters * 100)
                .map(|_| create_trivial_accumulator(is_first, ignore_nulls))
                .collect();
            let start = Instant::now();
            for acc in &mut accumulators {
                #[expect(clippy::unit_arg)]
                black_box(acc.update_batch(&[Arc::clone(&values)]).unwrap());
            }
            start.elapsed()
        })
    });
}

/// A top-level validity buffer with roughly `null_density` nulls, so the
/// generated nested arrays have null *values* (not just null inner
/// fields/elements) — matching the `nulls={pct}%` semantics of the primitive
/// benchmarks, where the value itself is null. Returns `None` at 0% so the
/// arrays stay fully valid. Derived from arrow's own null generator for a
/// deterministic, density-accurate pattern.
fn top_level_nulls(n: usize, null_density: f32) -> Option<NullBuffer> {
    create_primitive_array::<Int64Type>(n, null_density)
        .nulls()
        .cloned()
}

/// A 3-field struct value column `Struct<Int64, Utf8, Float64>`. `null_density`
/// controls both the struct-level null values and the inner field nulls.
fn create_struct_array(n: usize, null_density: f32) -> ArrayRef {
    let a = Arc::new(create_primitive_array::<Int64Type>(n, null_density)) as ArrayRef;
    let b =
        Arc::new(create_string_array_with_len::<i32>(n, null_density, 16)) as ArrayRef;
    let d = Arc::new(create_primitive_array::<Float64Type>(n, null_density)) as ArrayRef;
    let fields = Fields::from(vec![
        Field::new("c0", DataType::Int64, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Float64, true),
    ]);
    Arc::new(StructArray::new(
        fields,
        vec![a, b, d],
        top_level_nulls(n, null_density),
    ))
}

/// A `List<Int64>` value column with fixed-size lists of `list_len` elements.
fn create_list_array(n: usize, list_len: usize, null_density: f32) -> ArrayRef {
    let child = Arc::new(create_primitive_array::<Int64Type>(
        n * list_len,
        null_density,
    )) as ArrayRef;
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(list_len, n));
    let field = Arc::new(Field::new_list_field(DataType::Int64, true));
    Arc::new(ListArray::new(
        field,
        offsets,
        child,
        top_level_nulls(n, null_density),
    ))
}

/// A `Map<Utf8, Int64>` value column with `entries_per_row` entries per row.
/// Values carry `null_density` nulls (keys are never null), matching the null
/// treatment of the struct / list generators.
fn create_map_array(n: usize, entries_per_row: usize, null_density: f32) -> ArrayRef {
    let total = n * entries_per_row;
    let values =
        Arc::new(create_primitive_array::<Int64Type>(total, null_density)) as ArrayRef;
    let keys = Arc::new(StringArray::from_iter_values(
        (0..total).map(|idx| format!("k{}", idx % entries_per_row)),
    )) as ArrayRef;
    let entry_fields = Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int64, true),
    ]);
    let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(entries_per_row, n));
    let map_field =
        Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    Arc::new(MapArray::new(
        map_field,
        offsets,
        entries,
        top_level_nulls(n, null_density),
        false,
    ))
}

/// A composite `List<Struct<a: Int64, b: Utf8>>` column — a list whose
/// elements are structs (the "array of records" shape). Exercises the
/// nested-within-nested case, which the generic value-state path must also
/// handle.
fn create_list_of_struct_array(n: usize, list_len: usize, null_density: f32) -> ArrayRef {
    let total = n * list_len;
    let a =
        Arc::new(create_primitive_array::<Int64Type>(total, null_density)) as ArrayRef;
    let b =
        Arc::new(create_string_array_with_len::<i32>(total, null_density, 8)) as ArrayRef;
    let struct_fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let child =
        Arc::new(StructArray::new(struct_fields.clone(), vec![a, b], None)) as ArrayRef;
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(list_len, n));
    let list_field =
        Arc::new(Field::new_list_field(DataType::Struct(struct_fields), true));
    Arc::new(ListArray::new(
        list_field,
        offsets,
        child,
        top_level_nulls(n, null_density),
    ))
}

/// Drive one coalesce-peers variant: `separate xN` (pre-rewrite, one primitive
/// accumulator per column) vs `coalesced struct` (post-rewrite, one struct
/// accumulator). Each accumulator is primed with `worst_ord` (the always-losing
/// ordering value), then fed one `ord` per iteration from `iter_ords`. Varying
/// `iter_ords` selects which path dominates — see [`coalesce_comparison_bench`].
#[expect(clippy::too_many_arguments)]
fn run_coalesce_variant(
    c: &mut Criterion,
    name: &str,
    variant: &str,
    column_values: &[ArrayRef],
    struct_values: &ArrayRef,
    worst_ord: &ArrayRef,
    iter_ords: &[ArrayRef],
    group_indices: &[usize],
    num_groups: usize,
) {
    // Pre-rewrite: one accumulator per column.
    c.bench_function(
        &format!("{name} separate x{} {variant}", column_values.len()),
        |b| {
            b.iter_batched(
                || {
                    column_values
                        .iter()
                        .map(|values| {
                            let mut acc = prepare_typed_groups_accumulator(
                                true,
                                values.data_type().clone(),
                            );
                            acc.update_batch(
                                &[Arc::clone(values), Arc::clone(worst_ord)],
                                group_indices,
                                None,
                                num_groups,
                            )
                            .unwrap();
                            acc
                        })
                        .collect::<Vec<_>>()
                },
                |mut accumulators| {
                    for ord in iter_ords {
                        for (acc, values) in accumulators.iter_mut().zip(column_values) {
                            #[expect(clippy::unit_arg)]
                            black_box(
                                acc.update_batch(
                                    &[Arc::clone(values), Arc::clone(ord)],
                                    group_indices,
                                    None,
                                    num_groups,
                                )
                                .unwrap(),
                            );
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );

    // Post-rewrite: a single struct-valued accumulator.
    c.bench_function(&format!("{name} coalesced struct {variant}"), |b| {
        b.iter_batched(
            || {
                let mut acc = prepare_typed_groups_accumulator(
                    true,
                    struct_values.data_type().clone(),
                );
                acc.update_batch(
                    &[Arc::clone(struct_values), Arc::clone(worst_ord)],
                    group_indices,
                    None,
                    num_groups,
                )
                .unwrap();
                acc
            },
            |mut accumulator| {
                for ord in iter_ords {
                    #[expect(clippy::unit_arg)]
                    black_box(
                        accumulator
                            .update_batch(
                                &[Arc::clone(struct_values), Arc::clone(ord)],
                                group_indices,
                                None,
                                num_groups,
                            )
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

/// Head-to-head for the coalesce-peers rewrite: N independent primitive
/// `first_value` accumulators (the pre-rewrite plan) vs one struct-valued
/// accumulator carrying the same N columns (the post-rewrite plan). Runs two
/// variants, since the two plans differ on two separable costs:
///
/// - `(winner stable)` reuses one `ord` every iteration. After the first
///   iteration the running winner already holds the best value in `ord`, so
///   this is dominated by compare-and-reject — the per-row ordering comparison
///   the rewrite collapses (N compares -> 1).
/// - `(winner changes)` feeds a distinct, strictly-decreasing `ord` per
///   iteration so every row becomes a new winner (smaller ord wins; `worst_ord`
///   = i64::MAX primes the loser). This forces the running value to be
///   replaced+copied on every row, exercising the update path the winner-stable
///   case skips — where the struct plan copies one wider row vs N narrow ones.
#[expect(clippy::needless_pass_by_value)]
fn coalesce_comparison_bench(
    c: &mut Criterion,
    name: &str,
    column_values: Vec<ArrayRef>,
    struct_values: ArrayRef,
    ord: ArrayRef,
    num_groups: usize,
) {
    const ITERS: usize = 100;
    let n = ord.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();
    let worst_ord: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX; n]));

    // Winner-stable: the same `ord` every iteration.
    let stable_ords: Vec<ArrayRef> = (0..ITERS).map(|_| Arc::clone(&ord)).collect();

    // Winner-changing: strictly decreasing across (iteration, row), and below
    // `worst_ord`, so every row wins and updates the running value. Built once,
    // outside the timed loop.
    let changing_ords: Vec<ArrayRef> = (0..ITERS)
        .map(|k| {
            let base = i64::MAX - 1 - (k as i64) * (n as i64);
            Arc::new(Int64Array::from(
                (0..n as i64).map(|i| base - i).collect::<Vec<i64>>(),
            )) as ArrayRef
        })
        .collect();

    run_coalesce_variant(
        c,
        name,
        "(winner stable)",
        &column_values,
        &struct_values,
        &worst_ord,
        &stable_ords,
        &group_indices,
        num_groups,
    );
    run_coalesce_variant(
        c,
        name,
        "(winner changes)",
        &column_values,
        &struct_values,
        &worst_ord,
        &changing_ords,
        &group_indices,
        num_groups,
    );
}

fn first_last_nested_benchmark(c: &mut Criterion) {
    const N: usize = 65536;
    const NUM_GROUPS: usize = 1024;

    let ord = Arc::new(create_primitive_array::<Int64Type>(N, 0.0)) as ArrayRef;

    for pct in [0, 90] {
        let null_density = (pct as f32) / 100.0;

        // One column per nested value type. Each type gets the same treatment
        // as the primitive first_value / last_value benchmarks: update and
        // merge (both first and last) plus evaluate, at 0% and 90% nulls. On a
        // build without native nested support these run the fallback adapter;
        // with this PR they run the native GroupsAccumulator, so the benchmark
        // bot's before/after diff shows the win per type.
        let columns: [(&str, ArrayRef); 4] = [
            ("struct(i64,utf8,f64)", create_struct_array(N, null_density)),
            ("list<i64>[4]", create_list_array(N, 4, null_density)),
            ("map<utf8,i64>", create_map_array(N, 4, null_density)),
            (
                "list<struct(i64,utf8)>[4]",
                create_list_of_struct_array(N, 4, null_density),
            ),
        ];

        for (type_label, values) in columns {
            for (fn_label, is_first) in [("first_value", true), ("last_value", false)] {
                update_bench(
                    c,
                    is_first,
                    &format!("{fn_label} update_bench {type_label} nulls={pct}%"),
                    values.clone(),
                    ord.clone(),
                    None,
                    NUM_GROUPS,
                );
                merge_bench(
                    c,
                    is_first,
                    &format!("{fn_label} merge_bench {type_label} nulls={pct}%"),
                    values.clone(),
                    ord.clone(),
                    None,
                    NUM_GROUPS,
                );
            }
            evaluate_bench(
                c,
                true,
                EmitTo::All,
                &format!("first_value evaluate_bench {type_label} nulls={pct}%, all"),
                values.clone(),
                ord.clone(),
                None,
                NUM_GROUPS,
            );
        }
    }
    // Coalesce-peers head-to-head on null-free columns.
    let a = Arc::new(create_primitive_array::<Int64Type>(N, 0.0)) as ArrayRef;
    let b = Arc::new(create_string_array_with_len::<i32>(N, 0.0, 16)) as ArrayRef;
    let d = Arc::new(create_primitive_array::<Float64Type>(N, 0.0)) as ArrayRef;
    let struct_values = create_struct_array(N, 0.0);
    coalesce_comparison_bench(
        c,
        "first_value coalesce_peers(i64,utf8,f64)",
        vec![a, b, d],
        struct_values,
        ord,
        NUM_GROUPS,
    );
}

fn first_last_benchmark(c: &mut Criterion) {
    const N: usize = 65536;
    const NUM_GROUPS: usize = 1024;

    assert_eq!(N % NUM_GROUPS, 0);

    for is_first in [true, false] {
        for pct in [0, 90] {
            let fn_name = if is_first {
                "first_value"
            } else {
                "last_value"
            };

            let null_density = (pct as f32) / 100.0;
            let values = Arc::new(create_primitive_array::<Int64Type>(N, null_density))
                as ArrayRef;
            let ord = Arc::new(create_primitive_array::<Int64Type>(N, null_density))
                as ArrayRef;

            for with_filter in [false, true] {
                let filter = create_boolean_array(N, 0.0, 0.5);
                let opt_filter = if with_filter { Some(&filter) } else { None };

                evaluate_bench(
                    c,
                    is_first,
                    EmitTo::First(2),
                    &format!(
                        "{fn_name} evaluate_bench nulls={pct}%, filter={with_filter}, first(2)"
                    ),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
                evaluate_bench(
                    c,
                    is_first,
                    EmitTo::All,
                    &format!(
                        "{fn_name} evaluate_bench nulls={pct}%, filter={with_filter}, all"
                    ),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );

                update_bench(
                    c,
                    is_first,
                    &format!("{fn_name} update_bench nulls={pct}%, filter={with_filter}"),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
                merge_bench(
                    c,
                    is_first,
                    &format!("{fn_name} merge_bench nulls={pct}%, filter={with_filter}"),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
            }

            for ignore_nulls in [false, true] {
                trivial_update_bench(
                    c,
                    is_first,
                    ignore_nulls,
                    &format!(
                        "{fn_name} trivial_update_bench nulls={pct}%, ignore_nulls={ignore_nulls}"
                    ),
                    values.clone(),
                );
            }
        }
    }
}

criterion_group!(benches, first_last_benchmark, first_last_nested_benchmark);
criterion_main!(benches);
