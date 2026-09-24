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

//! Checks that the intermediate state produced by an aggregate's
//! [`Accumulator`] and its [`GroupsAccumulator`] are interchangeable.
//!
//! For every function in [`all_default_aggregate_functions`], every argument
//! shape that the function's signature accepts (from a fixed menu of candidate
//! types and literals), with and without `DISTINCT` and, for functions that are
//! not order insensitive, with and without `ORDER BY`, the test builds both
//! accumulator kinds and checks that state produced by one can be merged by the
//! other with the same result as the ungrouped two-phase path:
//!
//! * `Accumulator::state` -> `Accumulator::merge_batch` (the reference)
//! * `GroupsAccumulator::state` -> `Accumulator::merge_batch`
//! * `Accumulator::state` -> `GroupsAccumulator::merge_batch`
//! * `GroupsAccumulator::state` -> `GroupsAccumulator::merge_batch`
//! * `GroupsAccumulator::convert_to_state` -> both merges
//!
//! It also checks that every state matches the types declared by
//! `state_fields`.
//!
//! The input has groups of very different sizes (including an empty group),
//! since state encodings often depend on how much data a group has seen.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{AggregateUDF, EmitTo};
use datafusion_functions_aggregate::all_default_aggregate_functions;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Why a registered function is not exercised.
#[derive(Clone, Copy, Debug)]
enum Reason {
    /// No native `GroupsAccumulator`: `groups_accumulator_supported` returns
    /// false and `create_groups_accumulator` returns the trait's default error.
    NoGroupsAccumulator,
    /// Replaced during planning, so `accumulator` always fails.
    NoAccumulator,
}

/// Functions that are not expected to be exercised by this test, with the
/// reason.
const NOT_EXERCISED: &[(&str, Reason)] = &[
    ("any_value", Reason::NoGroupsAccumulator),
    ("approx_median", Reason::NoGroupsAccumulator),
    ("approx_percentile_cont", Reason::NoGroupsAccumulator),
    (
        "approx_percentile_cont_with_weight",
        Reason::NoGroupsAccumulator,
    ),
    ("covar_pop", Reason::NoGroupsAccumulator),
    ("covar_samp", Reason::NoGroupsAccumulator),
    ("grouping", Reason::NoAccumulator),
    ("nth_value", Reason::NoGroupsAccumulator),
    ("regr_avgx", Reason::NoGroupsAccumulator),
    ("regr_avgy", Reason::NoGroupsAccumulator),
    ("regr_count", Reason::NoGroupsAccumulator),
    ("regr_intercept", Reason::NoGroupsAccumulator),
    ("regr_r2", Reason::NoGroupsAccumulator),
    ("regr_slope", Reason::NoGroupsAccumulator),
    ("regr_sxx", Reason::NoGroupsAccumulator),
    ("regr_sxy", Reason::NoGroupsAccumulator),
    ("regr_syy", Reason::NoGroupsAccumulator),
];

/// What the candidate cases revealed about one function.
#[derive(Default)]
struct Coverage {
    /// Cases that built an `AggregateFunctionExpr`.
    built: usize,
    /// Cases for which `create_accumulator` succeeded.
    with_accumulator: usize,
    /// Cases for which `groups_accumulator_supported` returned true or
    /// `create_groups_accumulator` returned something other than the trait's
    /// default error.
    with_groups_accumulator: usize,
    /// Cases that were checked for state compatibility.
    exercised: usize,
}

/// Whether `result` is the error returned by the default implementation of
/// `AggregateUDFImpl::create_groups_accumulator`.
fn is_default_groups_error<T>(result: &Result<T>) -> bool {
    matches!(
        result,
        Err(DataFusionError::NotImplemented(msg))
            if msg.starts_with("GroupsAccumulator hasn't been implemented for")
    )
}

fn candidate_types() -> Vec<DataType> {
    vec![
        DataType::Boolean,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt64,
        DataType::Float64,
        DataType::Decimal128(10, 2),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ]
}

fn candidate_literals() -> Vec<ScalarValue> {
    vec![
        ScalarValue::Float64(Some(0.5)),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Utf8(Some(",".to_string())),
    ]
}

/// Number of rows in each group. Group 0 is intentionally empty.
const GROUP_SIZES: &[usize] = &[0, 1, 5, 300, 3000];

/// Maximum number of arguments tried per function.
const MAX_ARITY: usize = 3;

#[derive(Clone, Debug)]
enum ArgKind {
    Column(DataType),
    Literal(ScalarValue),
}

/// A concrete way to call an aggregate function.
#[derive(Clone, Debug)]
struct Case {
    args: Vec<ArgKind>,
    distinct: bool,
    ordered: bool,
}

impl Case {
    fn describe(&self, name: &str) -> String {
        let args = self
            .args
            .iter()
            .map(|a| match a {
                ArgKind::Column(dt) => format!("col {dt}"),
                ArgKind::Literal(v) => format!("lit {v:?}"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let distinct = if self.distinct { "DISTINCT " } else { "" };
        let order_by = if self.ordered { " ORDER BY o" } else { "" };
        format!("{name}({distinct}{args}{order_by})")
    }
}

#[test]
fn accumulator_and_groups_accumulator_states_are_compatible() {
    // Panics are caught and reported as failures; keep them from also being
    // printed by the default hook.
    std::panic::set_hook(Box::new(|_| {}));

    let mut failures: Vec<String> = vec![];
    let mut coverage: BTreeMap<String, Coverage> = BTreeMap::new();

    for udaf in all_default_aggregate_functions() {
        let name = udaf.name().to_string();
        let cov = coverage.entry(name.clone()).or_default();

        for case in candidate_cases(&udaf) {
            let Some(expr) = build_expr(&udaf, &case) else {
                continue;
            };
            cov.built += 1;

            let has_accumulator = guard(|| expr.create_accumulator()).is_ok();
            let supported =
                guard(|| Ok(expr.groups_accumulator_supported())).unwrap_or(false);
            let groups_accumulator = guard(|| expr.create_groups_accumulator());
            if has_accumulator {
                cov.with_accumulator += 1;
            }
            if supported || !is_default_groups_error(&groups_accumulator) {
                cov.with_groups_accumulator += 1;
            }

            // Only aggregates with a native GroupsAccumulator are interesting:
            // otherwise `GroupsAccumulatorAdapter` wraps the `Accumulator` and
            // the state formats agree by construction.
            if !(supported && has_accumulator && groups_accumulator.is_ok()) {
                continue;
            }

            cov.exercised += 1;
            let desc = case.describe(&name);
            let errors = guard(|| Ok(check_case(&expr, &case))).unwrap_or_else(|e| {
                let mut errors = Errors::default();
                errors.push(&format!("{e}"));
                errors
            });
            failures.extend(errors.lines().into_iter().map(|e| format!("{desc}: {e}")));
        }
    }

    let not_exercised: BTreeMap<&str, Reason> = NOT_EXERCISED.iter().copied().collect();
    for (name, cov) in &coverage {
        if let Some(msg) = check_coverage(cov, not_exercised.get(name.as_str())) {
            failures.push(format!("{name}: {msg}"));
        }
    }
    for name in not_exercised.keys() {
        if !coverage.contains_key(*name) {
            failures.push(format!(
                "{name}: listed in NOT_EXERCISED but not registered"
            ));
        }
    }

    let summary = coverage
        .iter()
        .filter(|(_, cov)| cov.exercised > 0)
        .map(|(name, cov)| format!("  {name}: {} case(s)", cov.exercised))
        .collect::<Vec<_>>()
        .join("\n");
    println!("exercised:\n{summary}");

    // Restore the default hook so the assertion below is reported.
    let _ = std::panic::take_hook();
    assert!(
        failures.is_empty(),
        "{} state compatibility failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Checks a function's coverage against its `NOT_EXERCISED` entry, returning a
/// failure message if they disagree.
fn check_coverage(cov: &Coverage, reason: Option<&Reason>) -> Option<String> {
    let Some(reason) = reason else {
        if cov.exercised > 0 {
            return None;
        }
        return Some(if cov.with_groups_accumulator > 0 {
            format!(
                "has a native GroupsAccumulator ({} case(s)) but no case exercised \
                 it; extend the candidate types/literals",
                cov.with_groups_accumulator
            )
        } else {
            "no case exercised a native GroupsAccumulator; extend the candidate \
             types/literals or add it to NOT_EXERCISED"
                .to_string()
        });
    };

    if cov.exercised > 0 {
        return Some(format!(
            "listed in NOT_EXERCISED as {reason:?} but {} case(s) were exercised; \
             remove it from the list",
            cov.exercised
        ));
    }
    if cov.built == 0 {
        return Some(format!(
            "listed in NOT_EXERCISED as {reason:?} but no candidate case builds, \
             so the reason cannot be checked"
        ));
    }
    match reason {
        Reason::NoGroupsAccumulator if cov.with_groups_accumulator > 0 => Some(format!(
            "listed in NOT_EXERCISED as {reason:?} but {} case(s) report or \
                 create a native GroupsAccumulator; remove it from the list and \
                 extend the candidate types/literals so it is exercised",
            cov.with_groups_accumulator
        )),
        Reason::NoAccumulator if cov.with_accumulator > 0 => Some(format!(
            "listed in NOT_EXERCISED as {reason:?} but {} case(s) create an \
             Accumulator",
            cov.with_accumulator
        )),
        _ => None,
    }
}

/// Enumerates the argument shapes that the function's signature accepts, after
/// the same coercion the planner applies.
fn candidate_cases(udaf: &AggregateUDF) -> Vec<Case> {
    let mut seen = BTreeSet::new();
    let mut cases = vec![];
    let orderings: &[bool] = if udaf.order_sensitivity().is_insensitive() {
        &[false]
    } else {
        &[false, true]
    };

    for first in candidate_types() {
        for arity in 1..=MAX_ARITY {
            for rest in arg_shapes(&first, arity - 1) {
                let mut args = vec![ArgKind::Column(first.clone())];
                args.extend(rest);

                let Some(args) = coerce(udaf, &args) else {
                    continue;
                };
                if !seen.insert(format!("{args:?}")) {
                    continue;
                }
                for distinct in [false, true] {
                    for &ordered in orderings {
                        cases.push(Case {
                            args: args.clone(),
                            distinct,
                            ordered,
                        });
                    }
                }
            }
        }
    }
    cases
}

/// All combinations of `n` trailing arguments, each either a column of the same
/// type as the first argument or one of the candidate literals.
fn arg_shapes(first: &DataType, n: usize) -> Vec<Vec<ArgKind>> {
    let mut options = vec![ArgKind::Column(first.clone())];
    options.extend(candidate_literals().into_iter().map(ArgKind::Literal));

    let mut shapes: Vec<Vec<ArgKind>> = vec![vec![]];
    for _ in 0..n {
        shapes = shapes
            .into_iter()
            .flat_map(|prefix| {
                options.iter().map(move |o| {
                    let mut next = prefix.clone();
                    next.push(o.clone());
                    next
                })
            })
            .collect();
    }
    shapes
}

/// Applies signature coercion, returning the coerced arguments or `None` if the
/// function does not accept them.
fn coerce(udaf: &AggregateUDF, args: &[ArgKind]) -> Option<Vec<ArgKind>> {
    let fields: Vec<FieldRef> = args
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let dt = match a {
                ArgKind::Column(dt) => dt.clone(),
                ArgKind::Literal(v) => v.data_type(),
            };
            Arc::new(Field::new(format!("c{i}"), dt, true))
        })
        .collect();
    let coerced = fields_with_udf(&fields, udaf).ok()?;

    args.iter()
        .zip(coerced)
        .map(|(a, f)| match a {
            ArgKind::Column(_) => Some(ArgKind::Column(f.data_type().clone())),
            ArgKind::Literal(v) => v.cast_to(f.data_type()).ok().map(ArgKind::Literal),
        })
        .collect()
}

/// Builds the physical aggregate expression. Column arguments refer to columns
/// `c0..cN` of a schema that contains only the column arguments.
fn build_expr(udaf: &Arc<AggregateUDF>, case: &Case) -> Option<AggregateFunctionExpr> {
    let schema = Arc::new(input_schema(case));
    let mut col_idx = 0;
    let exprs: Vec<Arc<dyn PhysicalExpr>> = case
        .args
        .iter()
        .map(|a| match a {
            ArgKind::Column(_) => {
                let e = Arc::new(Column::new(&format!("c{col_idx}"), col_idx));
                col_idx += 1;
                e as Arc<dyn PhysicalExpr>
            }
            ArgKind::Literal(v) => Arc::new(Literal::new(v.clone())) as _,
        })
        .collect();

    let mut builder = AggregateExprBuilder::new(Arc::clone(udaf), exprs)
        .schema(Arc::clone(&schema))
        .alias("agg");
    if case.distinct {
        builder = builder.distinct();
    }
    if case.ordered {
        let o = Column::new_with_schema(ORDER_COLUMN, &schema).ok()?;
        builder = builder.order_by(vec![PhysicalSortExpr::new_default(Arc::new(o))]);
    }
    guard(|| builder.build()).ok()
}

/// Name of the column used by `ORDER BY` cases.
const ORDER_COLUMN: &str = "o";

/// Column arguments `c0..cN`, followed by the ordering column if the case is
/// ordered.
fn input_schema(case: &Case) -> Schema {
    let mut fields: Vec<Field> = case
        .args
        .iter()
        .filter_map(|a| match a {
            ArgKind::Column(dt) => Some(dt.clone()),
            ArgKind::Literal(_) => None,
        })
        .enumerate()
        .map(|(i, dt)| Field::new(format!("c{i}"), dt, true))
        .collect();
    if case.ordered {
        fields.push(Field::new(ORDER_COLUMN, DataType::Int64, false));
    }
    Schema::new(fields)
}

/// Input data: one array per aggregate argument (literals included, as
/// `AggregateExec` passes them) plus the group index of each row. Rows of
/// different groups are interleaved.
struct Input {
    args: Vec<ArrayRef>,
    group_indices: Vec<usize>,
    num_groups: usize,
}

impl Input {
    fn new(expr: &AggregateFunctionExpr, case: &Case) -> Result<Self> {
        // Round-robin over the groups that still need rows.
        let mut remaining = GROUP_SIZES.to_vec();
        let mut group_indices = vec![];
        while remaining.iter().any(|r| *r > 0) {
            for (g, r) in remaining.iter_mut().enumerate() {
                if *r > 0 {
                    group_indices.push(g);
                    *r -= 1;
                }
            }
        }

        // Every 7th row is null. Column k is scaled differently so that
        // multi-column aggregates (covariance, correlation) see different inputs.
        let num_rows = group_indices.len();
        let schema = Arc::new(input_schema(case));
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(k, field)| {
                if field.name() == ORDER_COLUMN {
                    return Ok(
                        Arc::new(Int64Array::from_iter_values(0..num_rows as i64))
                            as ArrayRef,
                    );
                }
                let values: Int64Array = (0..num_rows as i64)
                    .map(|i| (i % 7 != 3).then_some(i * (k as i64 + 1) % 1009))
                    .collect();
                cast_values(&(Arc::new(values) as ArrayRef), field.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        // Evaluate the arguments the same way `AggregateExec` does: the
        // argument expressions followed by the ordering expressions.
        let args = expr
            .expressions()
            .iter()
            .chain(expr.order_bys().iter().map(|s| &s.expr))
            .map(|e| e.evaluate(&batch)?.into_array(num_rows))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            args,
            group_indices,
            num_groups: GROUP_SIZES.len(),
        })
    }

    /// The indices of the rows of group `g`, in input order.
    fn rows_of(&self, g: usize) -> UInt32Array {
        self.group_indices
            .iter()
            .enumerate()
            .filter(|(_, gi)| **gi == g)
            .map(|(i, _)| i as u32)
            .collect()
    }
}

/// Casts generated integers to the target type, going through `Utf8` for the
/// binary types, which have no direct cast from integers.
fn cast_values(values: &ArrayRef, dt: &DataType) -> Result<ArrayRef> {
    Ok(match dt {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            cast(&cast(values, &DataType::Utf8)?, dt)?
        }
        _ => cast(values, dt)?,
    })
}

fn take_all(arrays: &[ArrayRef], indices: &UInt32Array) -> Result<Vec<ArrayRef>> {
    Ok(arrays
        .iter()
        .map(|a| take(a.as_ref(), indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

/// Runs every state exchange for one case.
fn check_case(expr: &AggregateFunctionExpr, case: &Case) -> Errors {
    let mut errors = Errors::default();
    let input = match Input::new(expr, case) {
        Ok(input) => input,
        Err(e) => {
            errors.push(&format!("generating input: {e}"));
            return errors;
        }
    };
    let state_types: Vec<DataType> = match expr.state_fields() {
        Ok(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        Err(e) => {
            errors.push(&format!("state_fields: {e}"));
            return errors;
        }
    };
    let all_groups: Vec<usize> = (0..input.num_groups).collect();

    // Reference: per-group Accumulator -> state -> Accumulator::merge_batch.
    let mut acc_states: Vec<Vec<ArrayRef>> = vec![];
    let mut expected: Vec<ScalarValue> = vec![];
    for g in 0..input.num_groups {
        let reference = guard(|| {
            let mut acc = expr.create_accumulator()?;
            acc.update_batch(&take_all(&input.args, &input.rows_of(g))?)?;
            let state = acc
                .state()?
                .iter()
                .map(|s| s.to_array())
                .collect::<Result<Vec<_>>>()?;
            let value = merge_into_accumulator(expr, &state)?;
            Ok((state, value))
        });
        match reference {
            Ok((state, value)) => {
                errors.check_types("Accumulator::state", &state, &state_types);
                acc_states.push(state);
                expected.push(value);
            }
            Err(e) => {
                errors.push_group(&format!("reference Accumulator path: {e}"), g);
                return errors;
            }
        }
    }

    // Accumulator::state -> GroupsAccumulator::merge_batch, all groups at once.
    errors.compare_all(
        "Accumulator::state -> GroupsAccumulator::merge_batch",
        guard(|| {
            let stacked = stack_states(&acc_states)?;
            merge_into_groups(expr, &stacked, &all_groups, input.num_groups)
        }),
        &expected,
    );

    // GroupsAccumulator::state, emitted for all groups.
    let groups_state = guard(|| {
        let mut gacc = expr.create_groups_accumulator()?;
        gacc.update_batch(&input.args, &input.group_indices, None, input.num_groups)?;
        gacc.state(EmitTo::All)
    });
    match groups_state {
        Ok(groups_state) => {
            errors.check_types("GroupsAccumulator::state", &groups_state, &state_types);

            for (g, want) in expected.iter().enumerate() {
                errors.compare(
                    "GroupsAccumulator::state -> Accumulator::merge_batch",
                    g,
                    guard(|| {
                        let state: Vec<ArrayRef> =
                            groups_state.iter().map(|a| a.slice(g, 1)).collect();
                        merge_into_accumulator(expr, &state)
                    }),
                    want,
                );
            }

            errors.compare_all(
                "GroupsAccumulator::state -> GroupsAccumulator::merge_batch",
                guard(|| {
                    merge_into_groups(expr, &groups_state, &all_groups, input.num_groups)
                }),
                &expected,
            );
        }
        Err(e) => errors.push(&format!("GroupsAccumulator::state: {e}")),
    }

    // convert_to_state produces one state row per input row.
    let converted = guard(|| {
        let gacc = expr.create_groups_accumulator()?;
        gacc.convert_to_state(&input.args, None)
    });
    match converted {
        Ok(converted) => {
            errors.check_types(
                "GroupsAccumulator::convert_to_state",
                &converted,
                &state_types,
            );

            for (g, want) in expected.iter().enumerate() {
                errors.compare(
                    "convert_to_state -> Accumulator::merge_batch",
                    g,
                    guard(|| {
                        let state = take_all(&converted, &input.rows_of(g))?;
                        merge_into_accumulator(expr, &state)
                    }),
                    want,
                );
            }

            errors.compare_all(
                "convert_to_state -> GroupsAccumulator::merge_batch",
                guard(|| {
                    merge_into_groups(
                        expr,
                        &converted,
                        &input.group_indices,
                        input.num_groups,
                    )
                }),
                &expected,
            );
        }
        Err(e) => errors.push(&format!("GroupsAccumulator::convert_to_state: {e}")),
    }

    errors
}

fn merge_into_accumulator(
    expr: &AggregateFunctionExpr,
    state: &[ArrayRef],
) -> Result<ScalarValue> {
    let mut acc = expr.create_accumulator()?;
    acc.merge_batch(state)?;
    acc.evaluate()
}

fn merge_into_groups(
    expr: &AggregateFunctionExpr,
    state: &[ArrayRef],
    group_indices: &[usize],
    num_groups: usize,
) -> Result<ArrayRef> {
    let mut gacc = expr.create_groups_accumulator()?;
    gacc.merge_batch(state, group_indices, num_groups)?;
    gacc.evaluate(EmitTo::All)
}

/// Concatenates one-row states into one array per state column.
fn stack_states(states: &[Vec<ArrayRef>]) -> Result<Vec<ArrayRef>> {
    let num_cols = states.first().map(|s| s.len()).unwrap_or(0);
    (0..num_cols)
        .map(|c| {
            let arrays: Vec<&dyn Array> = states.iter().map(|s| s[c].as_ref()).collect();
            Ok(concat(&arrays)?)
        })
        .collect()
}

/// Runs `f`, turning a panic into an error.
fn guard<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|panic| {
        let msg = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "<non-string panic>".to_string());
        Err(DataFusionError::Execution(format!("panicked: {msg}")))
    })
}

/// Failures for one case. The same message for several groups is reported
/// once, listing the groups.
#[derive(Default)]
struct Errors(Vec<(String, Vec<usize>)>);

impl Errors {
    fn push(&mut self, msg: &str) {
        self.add(msg, None);
    }

    fn push_group(&mut self, msg: &str, group: usize) {
        self.add(msg, Some(group));
    }

    fn add(&mut self, msg: &str, group: Option<usize>) {
        // Only keep the first line, dropping e.g. the "please file a bug" text
        // of internal errors.
        let msg = msg.lines().next().unwrap_or_default().to_string();
        match self.0.iter_mut().find(|(m, _)| *m == msg) {
            Some((_, groups)) => groups.extend(group),
            None => self.0.push((msg, group.into_iter().collect())),
        }
    }

    fn lines(&self) -> Vec<String> {
        self.0
            .iter()
            .map(|(msg, groups)| {
                if groups.is_empty() {
                    return msg.clone();
                }
                let sizes = groups
                    .iter()
                    .map(|g| GROUP_SIZES[*g].to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{msg} [groups with {sizes} rows]")
            })
            .collect()
    }

    fn check_types(&mut self, what: &str, state: &[ArrayRef], expected: &[DataType]) {
        let actual: Vec<DataType> = state.iter().map(|a| a.data_type().clone()).collect();
        if actual != expected {
            self.push(&format!(
                "{what} produced types {actual:?} but state_fields declares {expected:?}"
            ));
        }
    }

    fn compare_all(
        &mut self,
        what: &str,
        got: Result<ArrayRef>,
        expected: &[ScalarValue],
    ) {
        let got = match got {
            Ok(arr) => arr,
            Err(e) => return self.push(&format!("{what}: {e}")),
        };
        if got.len() != expected.len() {
            return self.push(&format!(
                "{what}: expected {} groups, got {}",
                expected.len(),
                got.len()
            ));
        }
        for (g, want) in expected.iter().enumerate() {
            self.compare(what, g, ScalarValue::try_from_array(got.as_ref(), g), want);
        }
    }

    fn compare(
        &mut self,
        what: &str,
        group: usize,
        got: Result<ScalarValue>,
        want: &ScalarValue,
    ) {
        match got {
            Err(e) => self.push_group(&format!("{what}: {e}"), group),
            Ok(got) if !scalars_match(&got, want) => {
                self.push_group(&format!("{what}: got {got:?}, expected {want:?}"), group)
            }
            Ok(_) => {}
        }
    }
}

/// Exact equality, except that floats are compared with a relative tolerance
/// because merging in a different order can change rounding.
fn scalars_match(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => {
            floats_match(*x, *y)
        }
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => {
            floats_match(*x as f64, *y as f64)
        }
        _ => a == b,
    }
}

fn floats_match(x: f64, y: f64) -> bool {
    if x.is_nan() || y.is_nan() {
        return x.is_nan() && y.is_nan();
    }
    (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
}
