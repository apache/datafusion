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

//! End-to-end checks that reordering a helix does not change results.
//!
//! The unit tests for the rule inspect plan structure: schemas, relation
//! counts, column indices. They verify our own reasoning about the rewrite.
//! These run the query instead, which is the only way to catch a column index
//! that is wrong in a way the structural reasoning did not anticipate — the
//! failure mode where a query returns plausible but incorrect rows rather than
//! raising an error.
//!
//! A diamond is the case worth running: the two paths between `p0` and `p1`
//! mean one join carries a key pair from each, and the cost model prices the
//! subset as though the two predicates were independent. Neither happens in a
//! join tree, so neither is covered by tests of tree-shaped graphs.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;

/// A single diamond: `p0` and `p1` linked by both `a0` and `b0`.
///
/// ```text
///        a0
///       /  \
///     p0    p1
///       \  /
///        b0
/// ```
///
/// Column counts differ per table on purpose: with uniform widths a mistaken
/// offset can land on a valid column of the wrong table and still produce
/// well-formed output.
const QUERY: &str = "
    SELECT p0.ka, p0.kb, p0.tag,
           a0.to_p1, b0.to_p1, b0.note,
           p1.val, p1.extra
    FROM p0, a0, b0, p1
    WHERE p0.ka      = a0.from_p0
      AND p0.kb      = b0.from_p0
      AND a0.to_p1   = p1.ka
      AND b0.to_p1   = p1.kb
    ORDER BY p0.ka, a0.to_p1, b0.to_p1
";

fn int_column(values: &[i32]) -> Arc<Int32Array> {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn text_column(values: &[&str]) -> Arc<StringArray> {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Register the four tables of the diamond.
///
/// `p1` is deliberately much the largest: that is what makes the order the
/// planner produces a poor one, and so gives the rule something to improve.
fn register_tables(context: &SessionContext) -> Result<()> {
    // p0: 3 rows, 3 columns
    let p0_schema = Arc::new(Schema::new(vec![
        Field::new("ka", DataType::Int32, false),
        Field::new("kb", DataType::Int32, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    let p0 = RecordBatch::try_new(
        Arc::clone(&p0_schema),
        vec![
            int_column(&[1, 2, 3]),
            int_column(&[10, 20, 30]),
            text_column(&["x", "y", "z"]),
        ],
    )?;

    // a0: 4 rows, 2 columns — one path from p0 to p1
    let a0_schema = Arc::new(Schema::new(vec![
        Field::new("from_p0", DataType::Int32, false),
        Field::new("to_p1", DataType::Int32, false),
    ]));
    let a0 = RecordBatch::try_new(
        Arc::clone(&a0_schema),
        vec![int_column(&[1, 2, 3, 1]), int_column(&[100, 200, 300, 400])],
    )?;

    // b0: 5 rows, 3 columns — the other path
    let b0_schema = Arc::new(Schema::new(vec![
        Field::new("from_p0", DataType::Int32, false),
        Field::new("to_p1", DataType::Int32, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let b0 = RecordBatch::try_new(
        Arc::clone(&b0_schema),
        vec![
            int_column(&[10, 20, 30, 10, 20]),
            int_column(&[1000, 2000, 3000, 4000, 5000]),
            text_column(&["b1", "b2", "b3", "b4", "b5"]),
        ],
    )?;

    // p1: 44 rows, 4 columns. Four of them close a path of the diamond; the
    // rest are there to make this the table worth joining last.
    let mut ka = vec![100, 400, 200, 300];
    let mut kb = vec![1000, 4000, 2000, 3000];
    for filler in 0..40 {
        ka.push(9_000 + filler);
        kb.push(9_000 + filler);
    }
    let val: Vec<i32> = (0..ka.len() as i32).map(|row| 500 + row).collect();
    let extra: Vec<i32> = (0..ka.len() as i32).map(|row| 700 + row).collect();

    let p1_schema = Arc::new(Schema::new(vec![
        Field::new("ka", DataType::Int32, false),
        Field::new("kb", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
        Field::new("extra", DataType::Int32, false),
    ]));
    let p1 = RecordBatch::try_new(
        Arc::clone(&p1_schema),
        vec![
            int_column(&ka),
            int_column(&kb),
            int_column(&val),
            int_column(&extra),
        ],
    )?;

    for (name, schema, batch) in [
        ("p0", p0_schema, p0),
        ("a0", a0_schema, a0),
        ("b0", b0_schema, b0),
        ("p1", p1_schema, p1),
    ] {
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        context.register_table(name, Arc::new(table))?;
    }

    Ok(())
}

/// Run [`QUERY`] with the rule in the given state, returning the rows and the
/// physical plan text.
async fn run(reorder: bool) -> Result<(String, String)> {
    let mut config = SessionConfig::new();
    config.options_mut().optimizer.helix_join_reorder = reorder;
    // Single partition keeps the plan text focused on join order rather than
    // repartitioning, and keeps row order deterministic before the sort.
    config.options_mut().execution.target_partitions = 1;

    let context = SessionContext::new_with_config(config);
    register_tables(&context)?;

    let dataframe = context.sql(QUERY).await?;
    let plan = dataframe.clone().create_physical_plan().await?;
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();

    let batches = dataframe.collect().await?;
    let rows = pretty_format_batches(&batches)?.to_string();

    Ok((rows, plan_text))
}

#[tokio::test]
async fn reordering_a_diamond_returns_identical_rows() -> Result<()> {
    let (baseline_rows, baseline_plan) = run(false).await?;
    let (reordered_rows, reordered_plan) = run(true).await?;

    // The rule must actually have fired, or this test proves nothing.
    assert_ne!(
        baseline_plan, reordered_plan,
        "expected the rule to reorder this plan;\nbaseline:\n{baseline_plan}"
    );

    // The query has an ORDER BY, so the rows are directly comparable.
    assert_eq!(
        baseline_rows, reordered_rows,
        "reordering changed the result\nbaseline plan:\n{baseline_plan}\nreordered plan:\n{reordered_plan}"
    );

    // A non-empty result, otherwise equality is vacuous.
    assert!(
        baseline_rows.lines().count() > 4,
        "expected the query to return rows, got:\n{baseline_rows}"
    );

    Ok(())
}

#[tokio::test]
async fn the_rule_is_inert_when_disabled() -> Result<()> {
    // Guards against the rule doing anything at all behind the flag, which is
    // what makes shipping it disabled a safe default.
    let mut config = SessionConfig::new();
    config.options_mut().optimizer.helix_join_reorder = false;
    config.options_mut().execution.target_partitions = 1;
    let context = SessionContext::new_with_config(config);
    register_tables(&context)?;
    let disabled = context.sql(QUERY).await?.create_physical_plan().await?;

    let context = SessionContext::new_with_config({
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 1;
        config
    });
    register_tables(&context)?;
    let default = context.sql(QUERY).await?.create_physical_plan().await?;

    assert_eq!(
        datafusion::physical_plan::displayable(disabled.as_ref())
            .indent(false)
            .to_string(),
        datafusion::physical_plan::displayable(default.as_ref())
            .indent(false)
            .to_string(),
    );

    Ok(())
}
