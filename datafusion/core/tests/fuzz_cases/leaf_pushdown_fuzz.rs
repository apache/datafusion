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

//! Differential fuzz for the leaf expression pushdown rules.
//!
//! `ExtractLeafExpressions` and `PushDownLeafProjections` (the option
//! `datafusion.optimizer.enable_leaf_expression_pushdown`, default `true`) move
//! cheap expressions such as `s['b']` out of a filter, sort, limit, aggregate or
//! join, and push them towards the scan. The rules must not change the result of
//! a query. This test generates random SQL over struct typed tables, runs it two
//! times with the option on and off, and compares the two results.
//!
//! A second test does the same for the Parquet row filter. It compares
//! `datafusion.execution.parquet.pushdown_filters` on and off, with the leaf
//! rules on in both runs.
//!
//! Every case is derived from a seed. To reproduce a reported failure, run:
//!
//! ```text
//! LEAF_PUSHDOWN_FUZZ_SEED=<seed> LEAF_PUSHDOWN_FUZZ_CASES=1 \
//!   cargo test --profile ci -p datafusion --features extended_tests \
//!   --test fuzz -- leaf_pushdown --nocapture
//! ```
//!
//! `LEAF_PUSHDOWN_FUZZ_CASES` also raises the case count of a normal run.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::Result;
use parquet::arrow::ArrowWriter;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shapes that fail on `main` today. Set the switch to `true` when the linked
// issue is fixed. Keep the link in the comment.
// ---------------------------------------------------------------------------

/// A computed column with the same name as its input (`-a AS a`) is dropped when
/// a filter, limit or aggregate is above it.
/// <https://github.com/apache/datafusion/issues/25414>
const INCLUDE_SAME_NAME_ALIAS_SHAPES: bool = false;

/// A sub-query projection that swaps two column names (`b AS a, a AS b`) under
/// a struct field read gives an "ambiguous" planning error.
/// <https://github.com/apache/datafusion/issues/25446>
const INCLUDE_SWAP_ALIAS_SHAPES: bool = false;

/// A `UNION ALL` branch with a provably false predicate over a CTE gives an
/// "ambiguous" planning error.
/// <https://github.com/apache/datafusion/pull/25412>
const INCLUDE_FALSE_BRANCH_UNION_SHAPES: bool = false;

/// Cases of the short run. Keep this low: the whole test must stay in a few
/// seconds.
const DEFAULT_CASES: usize = 250;
/// Cases of the long run, gated behind the `extended_tests` feature.
const EXTENDED_CASES: usize = 5000;
/// Highest share of cases that may fail to plan on both sides. A higher share
/// means the generator makes junk.
const MAX_SKIP_RATIO: f64 = 0.30;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

const TABLES: [&str; 3] = ["t1", "t2", "t3"];
/// A table with two columns only. The rules compare sets of column names, so a
/// narrow table reaches shapes that a wide one hides.
const NARROW: &str = "n1";
const ROWS: i32 = 8;

fn struct_s() -> DataType {
    // Field `a` and field `b` have the same name as a top level column.
    DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]))
}

fn struct_t() -> DataType {
    // Field `c` has the same name as a top level column.
    DataType::Struct(Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("c", DataType::Utf8, true),
    ]))
}

fn fields_of(data_type: DataType) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields,
        other => unreachable!("expected a struct, got {other}"),
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Utf8, true),
        Field::new("s", struct_s(), true),
        Field::new("t", struct_t(), true),
    ]))
}

fn ints(offset: i32, step: i32) -> ArrayRef {
    Arc::new(Int32Array::from(
        (0..ROWS)
            .map(|i| {
                if (i + offset) % 5 == 0 {
                    None
                } else {
                    Some((i * step + offset) % 7 - 3)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn strs(offset: i32) -> ArrayRef {
    let words = ["x", "y", "zz", "q"];
    Arc::new(StringArray::from(
        (0..ROWS)
            .map(|i| {
                if (i + offset) % 7 == 0 {
                    None
                } else {
                    Some(words[((i + offset) % 4) as usize])
                }
            })
            .collect::<Vec<_>>(),
    ))
}

/// One batch per table. The three tables share a schema, so every generated
/// query is valid against any of them.
fn table_batch(offset: i32) -> RecordBatch {
    let s: ArrayRef = Arc::new(StructArray::new(
        fields_of(struct_s()),
        vec![ints(offset + 1, 3), strs(offset + 2)],
        None,
    ));
    let t: ArrayRef = Arc::new(StructArray::new(
        fields_of(struct_t()),
        vec![ints(offset + 3, 5), strs(offset + 1)],
        None,
    ));
    RecordBatch::try_new(
        schema(),
        vec![ints(offset, 2), ints(offset + 4, 1), strs(offset), s, t],
    )
    .unwrap()
}

fn config(leaf_pushdown: bool, parquet_pushdown: bool) -> SessionConfig {
    let mut config = SessionConfig::new().with_target_partitions(2);
    let options = config.options_mut();
    options.optimizer.enable_leaf_expression_pushdown = leaf_pushdown;
    options.execution.parquet.pushdown_filters = parquet_pushdown;
    options.optimizer.skip_failed_rules = false;
    config
}

/// The columns `a` and `s` of [`table_batch`], and nothing else.
fn narrow_batch() -> RecordBatch {
    table_batch(0).project(&[0, 3]).unwrap()
}

fn memory_ctx(leaf_pushdown: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(config(leaf_pushdown, false));
    for (i, name) in TABLES.iter().enumerate() {
        ctx.register_batch(*name, table_batch(i as i32 * 3))
            .unwrap();
    }
    ctx.register_batch(NARROW, narrow_batch()).unwrap();
    ctx
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// How the two runs of one case are compared.
enum Check {
    /// The two runs must give the same rows.
    Differential,
    /// The single boolean output column must be `true` on every row of both
    /// runs. Used for volatile expressions, whose value differs between runs by
    /// design.
    AllTrue,
}

struct Case {
    sql: String,
    check: Check,
}

fn pick<'a>(rng: &mut SmallRng, items: &[&'a str]) -> &'a str {
    items[rng.random_range(0..items.len())]
}

/// An expression of type Int32 that reads one leaf of a struct column.
fn int_leaf(rng: &mut SmallRng, q: &str) -> String {
    match rng.random_range(0..3) {
        0 => format!("{q}s['a']"),
        1 => format!("get_field({q}s, 'a')"),
        _ => format!("{q}t['x']"),
    }
}

/// An expression of type Utf8 that reads one leaf of a struct column.
fn str_leaf(rng: &mut SmallRng, q: &str) -> String {
    match rng.random_range(0..3) {
        0 => format!("{q}s['b']"),
        1 => format!("{q}t['c']"),
        _ => format!("get_field({q}t, 'c')"),
    }
}

/// One column of a pass through projection, in a random spelling.
fn spell(rng: &mut SmallRng, q: &str, c: &str) -> String {
    match rng.random_range(0..4) {
        0 => c.to_string(),
        1 => format!("{q}.{c}"),
        2 => format!("{q}.{c} AS {c}"),
        _ => format!("{c} AS {c}"),
    }
}

/// A pass through projection of every column, in a random spelling.
fn all_columns(rng: &mut SmallRng, q: &str) -> String {
    ["a", "b", "c", "s", "t"]
        .iter()
        .map(|c| spell(rng, q, c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `FROM` item of the outer query. It always exposes the five columns of
/// [`schema`] under the alias `src`. Returns the `WITH` clause and the item.
fn gen_source(rng: &mut SmallRng) -> (String, String) {
    let mut kinds = vec![0, 1, 2, 3, 4];
    if INCLUDE_SAME_NAME_ALIAS_SHAPES {
        kinds.push(5);
    }
    if INCLUDE_SWAP_ALIAS_SHAPES {
        kinds.push(6);
    }
    if INCLUDE_FALSE_BRANCH_UNION_SHAPES {
        kinds.push(7);
    }
    let table = pick(rng, &TABLES);
    let other = pick(rng, &TABLES);
    match kinds[rng.random_range(0..kinds.len())] {
        // Base table.
        0 => (String::new(), format!("{table} AS src")),
        // Derived table under a filter.
        1 => {
            let cols = all_columns(rng, table);
            let pred = pick(rng, &["a IS NOT NULL", "s['a'] > -3", "c <> 'q'"]);
            (
                String::new(),
                format!("(SELECT {cols} FROM {table} WHERE {pred}) AS src"),
            )
        }
        // CTE, referenced two times.
        2 => {
            let cols = all_columns(rng, table);
            let with = format!("WITH samples AS (SELECT {cols} FROM {table}) ");
            let inner = all_columns(rng, "samples");
            (
                with,
                format!(
                    "(SELECT {inner} FROM samples UNION ALL SELECT {inner} FROM samples) AS src"
                ),
            )
        }
        // UNION ALL of two base tables.
        3 => {
            let left = all_columns(rng, table);
            let right = all_columns(rng, other);
            (
                String::new(),
                format!(
                    "(SELECT {left} FROM {table} UNION ALL SELECT {right} FROM {other}) AS src"
                ),
            )
        }
        // Join with a struct leaf in the ON clause.
        4 => {
            let join = pick(rng, &["INNER", "LEFT", "FULL"]);
            let on = if rng.random_bool(0.5) {
                "l.s['a'] = r.s['a']"
            } else {
                "l.t['c'] = r.s['b']"
            };
            (
                String::new(),
                format!(
                    "(SELECT l.a AS a, r.b AS b, l.c AS c, l.s AS s, r.t AS t \
                     FROM {table} l {join} JOIN {other} r ON {on}) AS src"
                ),
            )
        }
        // A computed column with the same name as its input.
        // https://github.com/apache/datafusion/issues/25414
        5 => {
            let expr = pick(rng, &["-a", "a * 10", "a + 1"]);
            (
                String::new(),
                format!("(SELECT {expr} AS a, b, c, s, t FROM {table}) AS src"),
            )
        }
        // A swap of two column names.
        // https://github.com/apache/datafusion/issues/25446
        6 => (
            String::new(),
            format!("(SELECT b AS a, a AS b, c, s, t FROM {table}) AS src"),
        ),
        // A UNION ALL branch with a provably false predicate.
        // https://github.com/apache/datafusion/pull/25412
        _ => {
            let dead = pick(rng, &["1 = 2", "false"]);
            if rng.random_bool(0.5) {
                // Over a CTE, the shape of the issue.
                let cols = all_columns(rng, table);
                let with = format!("WITH samples AS (SELECT {cols} FROM {table}) ");
                let inner = all_columns(rng, "samples");
                (
                    with,
                    format!(
                        "(SELECT {inner} FROM samples UNION ALL SELECT {inner} FROM samples WHERE {dead}) AS src"
                    ),
                )
            } else {
                // Over two base tables.
                let left = all_columns(rng, table);
                let right = all_columns(rng, other);
                (
                    String::new(),
                    format!(
                        "(SELECT {left} FROM {table} UNION ALL SELECT {right} FROM {other} WHERE {dead}) AS src"
                    ),
                )
            }
        }
    }
}

fn gen_where(rng: &mut SmallRng) -> Option<String> {
    let other = pick(rng, &TABLES);
    Some(match rng.random_range(0..7) {
        0 => return None,
        1 => format!("{} > -2", int_leaf(rng, "src.")),
        2 => format!("{} IS NOT NULL", str_leaf(rng, "src.")),
        3 => format!("{} <> 'zz'", str_leaf(rng, "src.")),
        4 => format!("src.a >= -3 AND {} IS NOT NULL", int_leaf(rng, "src.")),
        // Subquery in the predicate.
        5 => format!("src.a IN (SELECT {} FROM {other})", int_leaf(rng, "")),
        _ => format!(
            "EXISTS (SELECT 1 FROM {other} WHERE {} = src.a)",
            int_leaf(rng, "")
        ),
    })
}

/// Aggregate query: leaf expressions in the group key, in the aggregate
/// argument and in `HAVING`.
fn gen_aggregate(rng: &mut SmallRng, from: &str) -> String {
    let key = if rng.random_bool(0.6) {
        str_leaf(rng, "src.")
    } else {
        "src.c".to_string()
    };
    let arg = int_leaf(rng, "src.");
    let mut parts = vec![format!(
        "SELECT {key} AS k, count(*) AS n, sum({arg}) AS sm FROM {from}"
    )];
    if let Some(w) = gen_where(rng) {
        parts.push(format!("WHERE {w}"));
    }
    parts.push(format!("GROUP BY {key}"));
    if rng.random_bool(0.4) {
        parts.push(format!("HAVING count(*) > 0 AND sum({arg}) IS NOT NULL"));
    }
    if rng.random_bool(0.3) {
        parts.push("ORDER BY 1, 2, 3".to_string());
    }
    parts.join(" ")
}

/// Row query: leaf expressions in the projection, the filter, the sort and
/// under a limit.
fn gen_rows(rng: &mut SmallRng, from: &str) -> String {
    let limit = rng.random_bool(0.25);
    let mut items = vec![
        "src.a".to_string(),
        format!("{} AS f1", int_leaf(rng, "src.")),
        format!("{} AS f2", str_leaf(rng, "src.")),
    ];
    if rng.random_bool(0.4) {
        items.push("src.c".to_string());
    }
    // A struct column is not orderable, so keep it out of a limited query.
    if !limit && rng.random_bool(0.3) {
        items.push("src.s".to_string());
    }
    let mut parts = vec![format!("SELECT {} FROM {from}", items.join(", "))];
    if let Some(w) = gen_where(rng) {
        parts.push(format!("WHERE {w}"));
    }
    if limit {
        // A total order over every output column makes the limited result
        // deterministic.
        let keys = (1..=items.len())
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("ORDER BY {keys} LIMIT {}", rng.random_range(1..6)));
    } else if rng.random_bool(0.4) {
        parts.push(format!(
            "ORDER BY {} NULLS FIRST, src.a",
            int_leaf(rng, "src.")
        ));
    }
    parts.join(" ")
}

/// Volatile shapes. The two runs cannot be compared with each other, so each
/// query reports its own consistency in one boolean column.
fn gen_volatile(rng: &mut SmallRng) -> Case {
    let table = pick(rng, &TABLES);
    let sql = match rng.random_range(0..3) {
        // A volatile value built in a subquery and read two times above.
        // https://github.com/apache/datafusion/issues/24678
        0 => format!(
            "SELECT s['r'] = f AS eq FROM \
             (SELECT s, s['r'] AS f FROM \
             (SELECT named_struct('r', random()) AS s FROM {table}) i) o"
        ),
        1 => format!(
            "SELECT s['r'] = f AS eq FROM \
             (SELECT s, s['r'] AS f FROM \
             (SELECT named_struct('r', random(), 'c', c) AS s FROM {table}) i \
             WHERE s['c'] <> 'zz') o"
        ),
        // A filter on a volatile group key.
        // https://github.com/apache/datafusion/issues/25415
        _ => format!(
            "SELECT k FROM (SELECT random() < 0.5 AS k, count(*) AS n \
             FROM {table} GROUP BY random() < 0.5) v WHERE k"
        ),
    };
    Case {
        sql,
        check: Check::AllTrue,
    }
}

/// A struct returning function used bare and through `['f']` in the same query.
fn gen_struct_function(rng: &mut SmallRng) -> Case {
    let table = pick(rng, &TABLES);
    let sql = match rng.random_range(0..3) {
        0 => format!(
            "SELECT a, arrow_field(a) AS f, arrow_field(a)['name'] AS n FROM {table}"
        ),
        1 => format!(
            "SELECT a FROM {table} WHERE CASE WHEN arrow_field(a) IS NOT NULL \
             THEN arrow_field(a)['nullable'] IS NOT NULL END"
        ),
        // A struct built in a subquery and read above.
        _ => format!(
            "SELECT ns['x'] AS x, ns['y'] AS y FROM \
             (SELECT named_struct('x', a, 'y', c) AS ns FROM {table}) i \
             WHERE ns['y'] <> 'q' ORDER BY 1 NULLS FIRST, 2 NULLS FIRST"
        ),
    };
    Case {
        sql,
        check: Check::Differential,
    }
}

/// A computed column whose alias is the name of one of its inputs. The rules
/// resolve columns by name, so the computed column and the table column become
/// the same thing, and the computed column is lost.
/// <https://github.com/apache/datafusion/issues/25414>
fn gen_same_name_alias(rng: &mut SmallRng) -> Case {
    let expr = pick(rng, &["-a", "a * 10", "a + 1"]);
    // Every table holds 8 rows, so this limit keeps all of them.
    // The last arm is the swap shape of
    // <https://github.com/apache/datafusion/issues/25446>.
    let arms = if INCLUDE_SWAP_ALIAS_SHAPES { 6 } else { 5 };
    let sql = match rng.random_range(0..arms) {
        0 => format!(
            "SELECT a, s['b'] FROM (SELECT {expr} AS a, s FROM {NARROW} WHERE a > -3)"
        ),
        1 => format!(
            "SELECT a, s['b'] FROM (SELECT {expr} AS a, s FROM {NARROW} LIMIT 100)"
        ),
        2 => format!(
            "SELECT a, s['b'] FROM (SELECT {expr} AS a, s FROM {NARROW}) WHERE a < 0"
        ),
        3 => format!(
            "SELECT a, count(s['b']) FROM \
             (SELECT {expr} AS a, s FROM {NARROW} WHERE a > -3) GROUP BY a"
        ),
        4 => format!(
            "SELECT a, s['b'] FROM (SELECT {expr} AS a, s FROM {NARROW} ORDER BY a)"
        ),
        _ => {
            let table = pick(rng, &TABLES);
            format!(
                "SELECT a, b, s['b'] FROM (SELECT b AS a, a AS b, s FROM {table} LIMIT 100)"
            )
        }
    };
    Case {
        sql,
        check: Check::Differential,
    }
}

fn gen_case(rng: &mut SmallRng) -> Case {
    match rng.random_range(0..10) {
        0 => gen_volatile(rng),
        1 => gen_struct_function(rng),
        2 if INCLUDE_SAME_NAME_ALIAS_SHAPES => gen_same_name_alias(rng),
        _ => {
            let (with, from) = gen_source(rng);
            let body = if rng.random_bool(0.35) {
                gen_aggregate(rng, &from)
            } else {
                gen_rows(rng, &from)
            };
            Case {
                sql: format!("{with}{body}"),
                check: Check::Differential,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// The column header, then the data rows in a stable order.
fn normalize(batches: &[RecordBatch]) -> Vec<String> {
    let text = pretty_format_batches(batches).unwrap().to_string();
    let mut lines = text.lines().filter(|l| !l.starts_with('+'));
    let header = lines.next().unwrap_or_default().to_string();
    let mut rows: Vec<String> = lines.map(str::to_string).collect();
    rows.sort();
    rows.insert(0, header);
    rows
}

/// `Err` names the first row that is not `true`.
fn all_true(batches: &[RecordBatch]) -> std::result::Result<(), String> {
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let column = batch.column(0).as_boolean();
        for i in 0..column.len() {
            if column.is_null(i) {
                return Err(format!("row {i} is NULL"));
            }
            if !column.value(i) {
                return Err(format!("row {i} is false"));
            }
        }
    }
    Ok(())
}

async fn collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Compares one case. `Some` holds the failure report.
async fn check_case(
    ctx_left: &SessionContext,
    ctx_right: &SessionContext,
    left: &str,
    right: &str,
    case: &Case,
    skipped: &mut usize,
) -> Option<String> {
    let out_left = collect(ctx_left, &case.sql).await;
    let out_right = collect(ctx_right, &case.sql).await;
    match (out_left, out_right) {
        (Err(_), Err(_)) => {
            *skipped += 1;
            None
        }
        (Ok(_), Err(e)) => Some(format!("{right} failed, {left} did not: {e}")),
        (Err(e), Ok(_)) => Some(format!("{left} failed, {right} did not: {e}")),
        (Ok(l), Ok(r)) => match case.check {
            Check::Differential => {
                let (nl, nr) = (normalize(&l), normalize(&r));
                (nl != nr).then(|| {
                    format!("{left}:\n{}\n{right}:\n{}", nl.join("\n"), nr.join("\n"))
                })
            }
            Check::AllTrue => all_true(&l)
                .err()
                .map(|e| format!("{left} is not self consistent: {e}"))
                .or_else(|| {
                    all_true(&r)
                        .err()
                        .map(|e| format!("{right} is not self consistent: {e}"))
                }),
        },
    }
}

/// Runs `cases` seeded cases against the two contexts. `left` and `right` name
/// the two settings in the failure report. Every case runs, so the report holds
/// the share of the seeds that fail, not only the first one.
async fn run(
    ctx_left: &SessionContext,
    ctx_right: &SessionContext,
    left: &str,
    right: &str,
    cases: usize,
    seed0: u64,
) {
    let mut skipped = 0;
    let mut failed = 0;
    let mut first: Option<String> = None;
    for seed in seed0..seed0 + cases as u64 {
        let case = gen_case(&mut SmallRng::seed_from_u64(seed));
        let Some(detail) =
            check_case(ctx_left, ctx_right, left, right, &case, &mut skipped).await
        else {
            continue;
        };
        failed += 1;
        first.get_or_insert_with(|| {
            format!(
                "  seed: {seed}\n  sql: {}\n  {detail}\n  reproduce with \
                 LEAF_PUSHDOWN_FUZZ_SEED={seed} LEAF_PUSHDOWN_FUZZ_CASES=1",
                case.sql
            )
        });
    }
    let ratio = skipped as f64 / cases as f64;
    println!(
        "leaf pushdown fuzz: {left} vs {right}, seeds {seed0}..{}, ok {}, \
         skipped {skipped} ({:.1}%), failed {failed}",
        seed0 + cases as u64,
        cases - skipped - failed,
        ratio * 100.0
    );
    assert!(
        first.is_none(),
        "leaf pushdown fuzz: {failed} of {cases} seeds failed, first one:\n{}",
        first.unwrap_or_default()
    );
    assert!(
        ratio <= MAX_SKIP_RATIO,
        "{:.1}% of the cases failed to plan on both sides, the generator makes junk",
        ratio * 100.0
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn seed() -> u64 {
    env_usize("LEAF_PUSHDOWN_FUZZ_SEED", 0) as u64
}

async fn run_memory(cases: usize) {
    let on = memory_ctx(true);
    let off = memory_ctx(false);
    run(&on, &off, "pushdown on", "pushdown off", cases, seed()).await;
}

// ---------------------------------------------------------------------------
// Parquet
// ---------------------------------------------------------------------------

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Writes one Parquet file per batch into `dir/name`, and registers that
/// directory as the table `name`.
async fn register_parquet(
    ctx: &SessionContext,
    dir: &TempDir,
    name: &str,
    batches: &[RecordBatch],
    schema: Option<&Schema>,
) {
    let sub = dir.path().join(name);
    std::fs::create_dir_all(&sub).unwrap();
    for (i, batch) in batches.iter().enumerate() {
        write_parquet(&sub.join(format!("{i}.parquet")), batch);
    }
    let mut options = ParquetReadOptions::default();
    if let Some(schema) = schema {
        options = options.schema(schema);
    }
    ctx.register_parquet(name, sub.to_string_lossy().as_ref(), options)
        .await
        .unwrap();
}

async fn parquet_ctx(dir: &TempDir, parquet_pushdown: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(config(true, parquet_pushdown));
    for (i, name) in TABLES.iter().enumerate() {
        register_parquet(&ctx, dir, name, &[table_batch(i as i32 * 3)], None).await;
    }
    register_parquet(&ctx, dir, NARROW, &[narrow_batch()], None).await;
    ctx
}

async fn run_parquet(cases: usize) {
    let dir = TempDir::new().unwrap();
    let on = parquet_ctx(&dir, true).await;
    let off = parquet_ctx(&dir, false).await;
    run(
        &on,
        &off,
        "parquet pushdown on",
        "parquet pushdown off",
        cases,
        seed(),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn leaf_pushdown_fuzz() {
    run_memory(env_usize("LEAF_PUSHDOWN_FUZZ_CASES", DEFAULT_CASES)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leaf_pushdown_parquet_fuzz() {
    run_parquet(env_usize("LEAF_PUSHDOWN_FUZZ_CASES", DEFAULT_CASES / 2)).await;
}

/// The whole `fuzz_cases` tree is behind the `extended_tests` feature today.
/// The attribute keeps the long run gated if that changes.
#[cfg(feature = "extended_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn leaf_pushdown_fuzz_extended() {
    run_memory(env_usize("LEAF_PUSHDOWN_FUZZ_CASES", EXTENDED_CASES)).await;
    run_parquet(env_usize("LEAF_PUSHDOWN_FUZZ_CASES", EXTENDED_CASES / 5)).await;
}

/// Two Parquet files of one table, one of which stores a column with a type
/// that differs from the table schema. The schema adapter casts it, which is
/// the path of <https://github.com/apache/datafusion/issues/25268>.
#[tokio::test(flavor = "multi_thread")]
async fn leaf_pushdown_parquet_schema_evolution() {
    // The table schema says `b` is Int64. One file of every table stores Int32.
    let wide = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Utf8, true),
        Field::new("s", struct_s(), true),
        Field::new("t", struct_t(), true),
    ]);
    let dir = TempDir::new().unwrap();
    let mut contexts = vec![];
    for pushdown in [true, false] {
        let ctx = SessionContext::new_with_config(config(true, pushdown));
        for (i, name) in TABLES.iter().enumerate() {
            let narrow = table_batch(i as i32 * 3);
            let b: ArrayRef = Arc::new(Int64Array::from(
                narrow
                    .column(1)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|v| v.map(i64::from))
                    .collect::<Vec<_>>(),
            ));
            let mut columns = narrow.columns().to_vec();
            columns[1] = b;
            let widened = RecordBatch::try_new(Arc::new(wide.clone()), columns).unwrap();
            register_parquet(&ctx, &dir, name, &[narrow, widened], Some(&wide)).await;
        }
        register_parquet(&ctx, &dir, NARROW, &[narrow_batch()], None).await;
        contexts.push(ctx);
    }
    run(
        &contexts[0],
        &contexts[1],
        "parquet pushdown on",
        "parquet pushdown off",
        DEFAULT_CASES / 5,
        seed(),
    )
    .await;
}
