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

//! End-to-end checks that reordering a double star does not change results.
//!
//! The unit tests for the rule inspect plan structure: schemas, relation
//! counts, column indices. They verify our own reasoning about the rewrite.
//! These run the query instead, which is the only way to catch a column index
//! that is wrong in a way the structural reasoning did not anticipate — the
//! failure mode where a query returns plausible but incorrect rows rather than
//! raising an error.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;

/// A bowtie over two fact tables bridged by a link table, each fact table
/// carrying its own dimension.
///
/// ```text
///   dim_a          dim_b
///     |              |
///   fact_a -- link -- fact_b
/// ```
///
/// Column counts differ per table on purpose: with uniform widths a mistaken
/// offset can land on a valid column of the wrong table and still produce
/// well-formed output.
const QUERY: &str = "
    SELECT fact_a.id, fact_a.a_val, dim_a.label,
           link.a_id, link.b_id,
           fact_b.id, fact_b.b_val, fact_b.extra, dim_b.label
    FROM fact_a, dim_a, link, fact_b, dim_b
    WHERE fact_a.dim_id = dim_a.id
      AND fact_a.id     = link.a_id
      AND link.b_id     = fact_b.id
      AND fact_b.dim_id = dim_b.id
    ORDER BY fact_a.id, fact_b.id
";

fn int_column(values: &[i32]) -> Arc<Int32Array> {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn text_column(values: &[&str]) -> Arc<StringArray> {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Register the five tables of the bowtie with a deliberately uneven shape:
/// the fact tables have several rows per dimension key, which is what makes
/// join order affect intermediate sizes.
async fn register_tables(context: &SessionContext) -> Result<()> {
    // dim_a: 3 rows, 2 columns
    let dim_a_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let dim_a = RecordBatch::try_new(
        Arc::clone(&dim_a_schema),
        vec![int_column(&[1, 2, 3]), text_column(&["ay", "bee", "cee"])],
    )?;

    // fact_a: 6 rows, 3 columns
    let fact_a_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("dim_id", DataType::Int32, false),
        Field::new("a_val", DataType::Int32, false),
    ]));
    let fact_a = RecordBatch::try_new(
        Arc::clone(&fact_a_schema),
        vec![
            int_column(&[10, 11, 12, 13, 14, 15]),
            int_column(&[1, 1, 2, 2, 3, 3]),
            int_column(&[100, 101, 102, 103, 104, 105]),
        ],
    )?;

    // link: 5 rows, 2 columns — the bridge
    let link_schema = Arc::new(Schema::new(vec![
        Field::new("a_id", DataType::Int32, false),
        Field::new("b_id", DataType::Int32, false),
    ]));
    let link = RecordBatch::try_new(
        Arc::clone(&link_schema),
        vec![
            int_column(&[10, 11, 12, 13, 15]),
            int_column(&[20, 21, 21, 22, 23]),
        ],
    )?;

    // fact_b: 120 rows, 4 columns. Deliberately the largest table: it makes
    // dim_b's fanout small enough that absorbing dim_b before the big merge
    // beats deferring it, which is an order the planner does not produce.
    let fact_b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("dim_id", DataType::Int32, false),
        Field::new("b_val", DataType::Int32, false),
        Field::new("extra", DataType::Int32, false),
    ]));
    let fact_b = RecordBatch::try_new(
        Arc::clone(&fact_b_schema),
        vec![
            int_column(&(20..140).collect::<Vec<_>>()),
            int_column(&(0..120).map(|i| 7 + i % 3).collect::<Vec<_>>()),
            int_column(&(0..120).map(|i| 200 + i).collect::<Vec<_>>()),
            int_column(&(0..120).map(|i| 900 + i).collect::<Vec<_>>()),
        ],
    )?;

    // dim_b: 3 rows, 2 columns
    let dim_b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let dim_b = RecordBatch::try_new(
        Arc::clone(&dim_b_schema),
        vec![
            int_column(&[7, 8, 9]),
            text_column(&["seven", "eight", "nine"]),
        ],
    )?;

    for (name, schema, batch) in [
        ("dim_a", dim_a_schema, dim_a),
        ("fact_a", fact_a_schema, fact_a),
        ("link", link_schema, link),
        ("fact_b", fact_b_schema, fact_b),
        ("dim_b", dim_b_schema, dim_b),
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
    config.options_mut().optimizer.double_star_join_reorder = reorder;
    // Single partition keeps the plan text focused on join order rather than
    // repartitioning, and keeps row order deterministic before the sort.
    config.options_mut().execution.target_partitions = 1;

    let context = SessionContext::new_with_config(config);
    register_tables(&context).await?;

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
async fn reordering_a_double_star_returns_identical_rows() -> Result<()> {
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
    config.options_mut().optimizer.double_star_join_reorder = false;
    config.options_mut().execution.target_partitions = 1;
    let context = SessionContext::new_with_config(config);
    register_tables(&context).await?;
    let disabled = context.sql(QUERY).await?.create_physical_plan().await?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.target_partitions = 1;
    let context = SessionContext::new_with_config(config);
    register_tables(&context).await?;
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
