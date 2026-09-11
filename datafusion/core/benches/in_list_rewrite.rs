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

//! Compares short `IN` lists with equivalent `OR`/`AND` chains. The explicit
//! chain SQL uses the same left-deep shape produced when the logical short-list
//! rewrite is selected; planning is deliberately unoptimized so both candidate
//! forms remain available for comparison.
//!
//! Both forms start as SQL and are planned once. Criterion measures only
//! physical-expression evaluation so table scans, scheduling, and planning do
//! not hide the predicate cost this benchmark is intended to compare.
//! [`BinaryExpr`] evaluates its vectorized children eagerly, so a first-value
//! hit does not provide scalar-style short-circuiting.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, AsArray, FixedSizeBinaryArray, Float64Array, Int32Array, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, LogicalPlan, Operator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, InListExpr};
use rand::prelude::*;
use tokio::runtime::Runtime;

const ALL_LIST_LENGTHS: &[usize] = &[1, 2, 3, 4];
const NULL_LIST_LENGTHS: &[usize] = &[2, 3];
const MISS_VALUE_BASE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    Int32,
    Float64,
    Utf8,
    Utf8View,
    FixedSizeBinary8,
}

impl ValueKind {
    const ALL: [Self; 5] = [
        Self::Int32,
        Self::Float64,
        Self::Utf8,
        Self::Utf8View,
        Self::FixedSizeBinary8,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Int32 => "int32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Utf8View => "utf8view_inline",
            Self::FixedSizeBinary8 => "fixed_size_binary_8",
        }
    }

    fn table_name(self) -> &'static str {
        match self {
            Self::Int32 => "in_list_i32",
            Self::Float64 => "in_list_f64",
            Self::Utf8 => "in_list_utf8",
            Self::Utf8View => "in_list_utf8view",
            Self::FixedSizeBinary8 => "in_list_fsb8",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Int32 => DataType::Int32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::Utf8View => DataType::Utf8View,
            Self::FixedSizeBinary8 => DataType::FixedSizeBinary(8),
        }
    }

    fn seed_tag(self) -> u64 {
        match self {
            Self::Int32 => 1,
            Self::Float64 => 2,
            Self::Utf8 => 3,
            Self::Utf8View => 4,
            Self::FixedSizeBinary8 => 5,
        }
    }

    fn sql_literal(self, value: usize) -> String {
        match self {
            Self::Int32 => value.to_string(),
            Self::Float64 => format!("{value}.0"),
            Self::Utf8 | Self::Utf8View => format!("'{}'", string_value(value)),
            Self::FixedSizeBinary8 => format!("${}", value + 1),
        }
    }

    fn make_array(self, values: &[Option<usize>]) -> ArrayRef {
        match self {
            Self::Int32 => Arc::new(Int32Array::from_iter(
                values
                    .iter()
                    .copied()
                    .map(|value| value.map(|value| value as i32)),
            )),
            Self::Float64 => Arc::new(Float64Array::from_iter(
                values
                    .iter()
                    .copied()
                    .map(|value| value.map(|value| value as f64)),
            )),
            Self::Utf8 => Arc::new(StringArray::from_iter(
                values.iter().copied().map(|value| value.map(string_value)),
            )),
            Self::Utf8View => Arc::new(StringViewArray::from_iter(
                values.iter().copied().map(|value| value.map(string_value)),
            )),
            Self::FixedSizeBinary8 => {
                let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values
                        .iter()
                        .copied()
                        .map(|value| value.map(|value| (value as u64).to_be_bytes())),
                    8,
                )
                .unwrap();
                assert!(
                    array.values().as_ptr().cast::<u64>().is_aligned(),
                    "builder-created FixedSizeBinary(8) buffer is unexpectedly unaligned"
                );
                Arc::new(array)
            }
        }
    }

    fn param_values(
        self,
        list_len: usize,
        list_has_null: bool,
    ) -> Option<Vec<ScalarValue>> {
        (self == Self::FixedSizeBinary8).then(|| {
            let value_count = list_len - usize::from(list_has_null);
            (0..value_count)
                .map(|value| {
                    ScalarValue::FixedSizeBinary(
                        8,
                        Some((value as u64).to_be_bytes().to_vec()),
                    )
                })
                .collect()
        })
    }
}

/// All generated strings are eight bytes and therefore use Arrow's inline
/// byte-view representation, which is the specialized path under test.
fn string_value(value: usize) -> String {
    let value = format!("v{value:07}");
    assert_eq!(value.len(), 8);
    value
}

#[derive(Debug, Clone, Copy)]
struct Profile {
    name: &'static str,
    batch_size: usize,
    null_percent: usize,
    match_percent: usize,
    first_value_hits: bool,
    list_has_null: bool,
}

const PROFILES: [Profile; 8] = [
    Profile {
        name: "miss",
        batch_size: 8192,
        null_percent: 0,
        match_percent: 0,
        first_value_hits: false,
        list_has_null: false,
    },
    Profile {
        name: "balanced",
        batch_size: 8192,
        null_percent: 0,
        match_percent: 50,
        first_value_hits: false,
        list_has_null: false,
    },
    Profile {
        name: "skewed_hit",
        batch_size: 8192,
        null_percent: 0,
        match_percent: 90,
        first_value_hits: true,
        list_has_null: false,
    },
    Profile {
        name: "balanced_nullable",
        batch_size: 8192,
        null_percent: 20,
        match_percent: 50,
        first_value_hits: false,
        list_has_null: false,
    },
    Profile {
        name: "balanced_small_batch",
        batch_size: 64,
        null_percent: 0,
        match_percent: 50,
        first_value_hits: false,
        list_has_null: false,
    },
    Profile {
        name: "single_row_miss",
        batch_size: 1,
        null_percent: 0,
        match_percent: 0,
        first_value_hits: false,
        list_has_null: false,
    },
    Profile {
        name: "single_row_hit",
        batch_size: 1,
        null_percent: 0,
        match_percent: 100,
        first_value_hits: true,
        list_has_null: false,
    },
    Profile {
        name: "list_null_balanced",
        batch_size: 8192,
        null_percent: 0,
        match_percent: 50,
        first_value_hits: false,
        list_has_null: true,
    },
];

struct PlannedPair {
    list_len: usize,
    negated: bool,
    list_has_null: bool,
    chain: Arc<dyn PhysicalExpr>,
    in_list: Arc<dyn PhysicalExpr>,
}

fn register_table(ctx: &SessionContext, kind: ValueKind) {
    // A real row prevents an optimizer from replacing the scan with an empty
    // relation. Benchmark batches are evaluated directly and are not stored in
    // this table.
    let schema = Arc::new(Schema::new(vec![Field::new("a", kind.data_type(), true)]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![kind.make_array(&[Some(0)])])
            .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table(kind.table_name(), Arc::new(table))
        .unwrap();
}

fn list_literals(kind: ValueKind, list_len: usize, list_has_null: bool) -> Vec<String> {
    let value_count = list_len - usize::from(list_has_null);
    let mut literals = (0..value_count)
        .map(|value| kind.sql_literal(value))
        .collect::<Vec<_>>();
    if list_has_null {
        literals.push("NULL".to_owned());
    }
    literals
}

fn in_list_sql(
    kind: ValueKind,
    list_len: usize,
    negated: bool,
    list_has_null: bool,
) -> String {
    let literals = list_literals(kind, list_len, list_has_null).join(", ");
    let not = if negated { "NOT " } else { "" };
    format!(
        "SELECT * FROM {} WHERE a {not}IN ({literals})",
        kind.table_name()
    )
}

fn chain_sql(
    kind: ValueKind,
    list_len: usize,
    negated: bool,
    list_has_null: bool,
) -> String {
    let comparison = if negated { "<>" } else { "=" };
    let conjunction = if negated { "AND" } else { "OR" };
    let mut literals = list_literals(kind, list_len, list_has_null).into_iter();
    let first = literals.next().expect("an IN list is never empty");
    let mut predicate = format!("a {comparison} {first}");
    for literal in literals {
        predicate = format!("({predicate}) {conjunction} (a {comparison} {literal})");
    }
    format!("SELECT * FROM {} WHERE {predicate}", kind.table_name())
}

fn find_filter(plan: &LogicalPlan) -> Option<(&Expr, &LogicalPlan)> {
    if let LogicalPlan::Filter(filter) = plan {
        return Some((&filter.predicate, filter.input.as_ref()));
    }

    for input in plan.inputs() {
        if let Some(filter) = find_filter(input) {
            return Some(filter);
        }
    }
    None
}

/// Plan a SQL filter predicate through SQL parsing and analysis before
/// converting it to a physical expression.
///
/// Logical optimization is deliberately skipped: the one-row planning table
/// has exact statistics that can simplify a comparison chain for those stored
/// values. The benchmark evaluates different batches, so that table-specific
/// simplification would not be a valid counterfactual.
fn plan_filter_expr(
    ctx: &SessionContext,
    runtime: &Runtime,
    sql: &str,
    param_values: Option<&[ScalarValue]>,
) -> Arc<dyn PhysicalExpr> {
    let dataframe = runtime
        .block_on(ctx.sql(sql))
        .unwrap_or_else(|error| panic!("failed to plan SQL `{sql}`: {error}"));
    let dataframe = if let Some(param_values) = param_values {
        dataframe
            .with_param_values(param_values.to_vec())
            .unwrap_or_else(|error| panic!("failed to bind SQL `{sql}`: {error}"))
    } else {
        dataframe
    };
    let plan = dataframe.into_unoptimized_plan();
    let (predicate, input) = find_filter(&plan)
        .unwrap_or_else(|| panic!("planned SQL contains no Filter: `{sql}`\n{plan}"));
    ctx.create_physical_expr(predicate.clone(), input.schema().as_ref())
        .unwrap_or_else(|error| {
            panic!("failed to create physical predicate for `{sql}`: {error}")
        })
}

fn assert_static_in_list(expr: &Arc<dyn PhysicalExpr>, list_len: usize, negated: bool) {
    let in_list = expr
        .downcast_ref::<InListExpr>()
        .unwrap_or_else(|| panic!("expected retained InListExpr, got `{expr}`"));
    assert_eq!(in_list.len(), list_len);
    assert_eq!(in_list.negated(), negated);
    assert!(
        expr.to_string().contains("IN (SET)"),
        "literal list did not produce a static filter: `{expr}`"
    );
}

fn assert_comparison(expr: &dyn PhysicalExpr, comparison: Operator) {
    let binary = expr
        .downcast_ref::<BinaryExpr>()
        .unwrap_or_else(|| panic!("expected comparison BinaryExpr, got `{expr}`"));
    assert_eq!(binary.op(), &comparison, "unexpected leaf `{expr}`");
}

fn assert_left_deep_chain(expr: &dyn PhysicalExpr, list_len: usize, negated: bool) {
    let comparison = if negated {
        Operator::NotEq
    } else {
        Operator::Eq
    };
    if list_len == 1 {
        assert_comparison(expr, comparison);
        return;
    }

    let conjunction = if negated { Operator::And } else { Operator::Or };
    let binary = expr
        .downcast_ref::<BinaryExpr>()
        .unwrap_or_else(|| panic!("expected logical BinaryExpr, got `{expr}`"));
    assert_eq!(binary.op(), &conjunction, "unexpected chain `{expr}`");
    assert_left_deep_chain(binary.left().as_ref(), list_len - 1, negated);
    assert_comparison(binary.right().as_ref(), comparison);
}

fn plan_pairs(
    ctx: &SessionContext,
    runtime: &Runtime,
    kind: ValueKind,
) -> Vec<PlannedPair> {
    let mut pairs = Vec::with_capacity(12);
    for list_has_null in [false, true] {
        let list_lengths = if list_has_null {
            NULL_LIST_LENGTHS
        } else {
            ALL_LIST_LENGTHS
        };
        for &list_len in list_lengths {
            for negated in [false, true] {
                let in_sql = in_list_sql(kind, list_len, negated, list_has_null);
                let param_values = kind.param_values(list_len, list_has_null);
                let in_list =
                    plan_filter_expr(ctx, runtime, &in_sql, param_values.as_deref());
                assert_static_in_list(&in_list, list_len, negated);

                // SQL-plan an explicit left-deep comparison-chain baseline.
                // Optimizing it would merge it back into an InList or use
                // planning-table statistics to remove comparisons.
                let chain = plan_filter_expr(
                    ctx,
                    runtime,
                    &chain_sql(kind, list_len, negated, list_has_null),
                    param_values.as_deref(),
                );
                assert_left_deep_chain(chain.as_ref(), list_len, negated);

                pairs.push(PlannedPair {
                    list_len,
                    negated,
                    list_has_null,
                    chain,
                    in_list,
                });
            }
        }
    }
    pairs
}

fn make_batch(
    kind: ValueKind,
    profile: Profile,
    profile_index: usize,
    list_len: usize,
    list_has_null: bool,
) -> RecordBatch {
    let null_count = profile.batch_size * profile.null_percent / 100;
    let non_null_count = profile.batch_size - null_count;
    let match_count = non_null_count * profile.match_percent / 100;
    let miss_count = non_null_count - match_count;

    let mut values = Vec::with_capacity(profile.batch_size);
    values.extend(std::iter::repeat_n(None, null_count));
    values.extend((0..match_count).map(|index| {
        Some(if profile.first_value_hits {
            0
        } else {
            index % (list_len - usize::from(list_has_null))
        })
    }));
    values.extend((0..miss_count).map(|index| Some(MISS_VALUE_BASE + index)));

    let seed = 0x1A11_1575_5EED_u64
        ^ (kind.seed_tag() << 48)
        ^ ((profile_index as u64) << 32)
        ^ list_len as u64;
    values.shuffle(&mut StdRng::seed_from_u64(seed));

    let schema = Arc::new(Schema::new(vec![Field::new("a", kind.data_type(), true)]));
    RecordBatch::try_new(schema, vec![kind.make_array(&values)]).unwrap()
}

fn assert_same_output(pair: &PlannedPair, batch: &RecordBatch) {
    let chain = pair
        .chain
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let in_list = pair
        .in_list
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(
        chain.as_boolean(),
        in_list.as_boolean(),
        "chain and InList disagree for list={}, negated={}, list_has_null={}",
        pair.list_len,
        pair.negated,
        pair.list_has_null
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let ctx = SessionContext::new();
    for kind in ValueKind::ALL {
        register_table(&ctx, kind);
    }

    for kind in ValueKind::ALL {
        let pairs = plan_pairs(&ctx, &runtime, kind);
        for (profile_index, profile) in PROFILES.into_iter().enumerate() {
            let mut group = c.benchmark_group(format!(
                "in_list_rewrite/{}/{}/batch={}/nulls={}%/match={}%",
                kind.name(),
                profile.name,
                profile.batch_size,
                profile.null_percent,
                profile.match_percent
            ));
            group.throughput(Throughput::Elements(profile.batch_size as u64));

            for pair in pairs
                .iter()
                .filter(|pair| pair.list_has_null == profile.list_has_null)
            {
                let batch = make_batch(
                    kind,
                    profile,
                    profile_index,
                    pair.list_len,
                    pair.list_has_null,
                );
                assert_same_output(pair, &batch);

                let operation = if pair.negated { "not_in" } else { "in" };
                let case = format!("{operation}/list={}", pair.list_len);
                group.bench_function(BenchmarkId::new(&case, "chain"), |b| {
                    b.iter(|| black_box(pair.chain.evaluate(black_box(&batch)).unwrap()))
                });
                group.bench_function(BenchmarkId::new(&case, "in_list"), |b| {
                    b.iter(|| {
                        black_box(pair.in_list.evaluate(black_box(&batch)).unwrap())
                    })
                });
            }
            group.finish();
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500));
    targets = criterion_benchmark
}
criterion_main!(benches);
