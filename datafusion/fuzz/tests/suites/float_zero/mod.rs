// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks that `-0.0` and `0.0` give the same query results.
//!
//! Each query runs on two copies of a table: `raw` has some `-0.0` values and
//! `canon` has them replaced by `0.0`. The results must be the same, apart
//! from how `-0.0` is printed. See
//! <https://github.com/apache/datafusion/issues/26138>.

use std::future::Future;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_common::float_semantics::normalize;
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_common_runtime::SpawnedTask;
use futures::future::{join, join_all};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use test_utils::fuzz::FuzzTest;

mod data;
mod queries;
mod settings;

use queries::{QUERIES, Query};
use settings::Setting;

/// One query under one setting.
pub(crate) struct Pair {
    query: &'static Query,
    setting: Setting,
    ctx: SessionContext,
}

/// Every pair to run on `data`.
fn pairs(data: &data::Dataset) -> Result<Vec<Pair>> {
    let mut pairs = vec![];
    for setting in Setting::all() {
        let ctx = setting.context(data)?;
        for query in QUERIES.iter().filter(|q| runs(q, &setting)) {
            pairs.push(Pair {
                query,
                setting,
                ctx: ctx.clone(),
            });
        }
    }
    Ok(pairs)
}

/// A query without a zero in it would only repeat itself under the
/// `negative_literals` settings.
fn runs(query: &Query, setting: &Setting) -> bool {
    !setting.negative_literals || query.sql.contains("{z}")
}

/// Why a pair failed.
enum Failure {
    /// The results differ.
    Mismatch(String),
    /// A query returned an error.
    Error(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Mismatch(msg) | Failure::Error(msg) => f.write_str(msg),
        }
    }
}

/// Runs the query on both tables and compares the results.
async fn check(pair: &Pair) -> Result<(), Failure> {
    let Pair {
        query,
        setting,
        ctx,
    } = pair;
    let raw_sql = query.render("raw", setting.raw_zero());
    let canon_sql = query.render("canon", "0.0");
    let describe = || {
        format!(
            "{} under {setting:?}\n  raw:   {raw_sql}\n  canon: {canon_sql}",
            query.name
        )
    };

    let (raw, canon) = join(run_sql(&raw_sql, ctx), run_sql(&canon_sql, ctx)).await;
    let (mut raw, mut canon) = match (raw, canon) {
        (Ok(raw), Ok(canon)) => (format_rows(&raw), format_rows(&canon)),
        (raw, canon) => {
            let outcome = |r: &Result<_>| match r {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("fails: {e}"),
            };
            return Err(Failure::Error(format!(
                "{}\n  raw {}\n  canon {}",
                describe(),
                outcome(&raw),
                outcome(&canon)
            )));
        }
    };

    if !query.ordered {
        raw.sort_unstable();
        canon.sort_unstable();
    }
    if raw == canon {
        return Ok(());
    }
    let diff = first_difference(&raw, &canon, query.ordered);
    Err(Failure::Mismatch(format!("{}\n{diff}", describe())))
}

async fn run_sql(sql: &str, ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// One line per row, with every `-0.0` shown as `0.0`.
fn format_rows(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = vec![];
    for batch in batches {
        let columns: Vec<ArrayRef> =
            batch.columns().iter().cloned().map(normalize).collect();
        let formatters = columns
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row).to_string())
                .collect();
            rows.push(values.join(" | "));
        }
    }
    rows
}

/// The first row where the results differ.
fn first_difference(raw: &[String], canon: &[String], ordered: bool) -> String {
    let row = if ordered { "row" } else { "sorted row" };
    match raw.iter().zip(canon).position(|(r, c)| r != c) {
        Some(i) => format!("{row} {i}: raw {}, canon {}", raw[i], canon[i]),
        None => format!("raw {} rows, canon {} rows", raw.len(), canon.len()),
    }
}

/// Runs random queries on random tables. A query with a known failure may
/// give a wrong result, but must not return an error.
pub(crate) struct FloatZeroFuzzer;

impl FuzzTest for FloatZeroFuzzer {
    type Dataset = Vec<Pair>;

    fn name(&self) -> &str {
        "float_zero"
    }

    fn prepare_dataset(&self, seed: u64) -> impl Future<Output = Result<Self::Dataset>> {
        let data = data::random(&mut StdRng::seed_from_u64(seed));
        std::future::ready(pairs(&data))
    }

    async fn run_case(&self, pairs: &Self::Dataset, seed: u64) -> Result<()> {
        let pair = pairs
            .choose(&mut StdRng::seed_from_u64(seed))
            .ok_or_else(|| exec_datafusion_err!("no pair runs"))?;
        match (check(pair).await, pair.query.known_issue(&pair.setting)) {
            (Ok(()), _) | (Err(Failure::Mismatch(_)), Some(_)) => Ok(()),
            (Err(failure), None) => exec_err!("{failure}"),
            (Err(failure), Some(issue)) => exec_err!(
                "{failure}\nknown to give a wrong result, but now fails ({issue})"
            ),
        }
    }
}

/// On the fixed table, every query with a known failure must give a wrong
/// result and every other query must pass.
#[tokio::test(flavor = "multi_thread")]
async fn float_zero_fixed() {
    let mut errors = vec![];
    for (pair, result) in check_all(&data::fixed()).await {
        let Pair { query, setting, .. } = pair;
        match (result, query.known_issue(&setting)) {
            (Ok(()), None) | (Err(Failure::Mismatch(_)), Some(_)) => {}
            (Err(failure), None) => errors.push(failure.to_string()),
            (Err(Failure::Error(e)), Some(issue)) => errors.push(format!(
                "{e}\nknown to give a wrong result, but now fails ({issue})"
            )),
            (Ok(()), Some(issue)) => errors.push(format!(
                "{} under {setting:?} now passes, remove its known failure ({issue})",
                query.name
            )),
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

/// Runs all pairs at the same time, which keeps the test fast.
async fn check_all(data: &data::Dataset) -> Vec<(Pair, Result<(), Failure>)> {
    let tasks = pairs(data).unwrap().into_iter().map(|pair| {
        SpawnedTask::spawn(async move {
            let result = check(&pair).await;
            (pair, result)
        })
        .join_unwind()
    });
    join_all(tasks)
        .await
        .into_iter()
        .map(|joined| joined.unwrap())
        .collect()
}

/// A `-0.0` written in a query must keep its sign, or the
/// `negative_literals` settings test nothing.
#[tokio::test]
async fn float_zero_negative_literal() {
    let mut errors = vec![];
    for pair in pairs(&data::fixed()).unwrap() {
        if !pair.setting.negative_literals {
            continue;
        }
        let sql = pair.query.render("raw", "-0.0");
        let plan = pair.ctx.sql(&sql).await.unwrap();
        let plan = plan.create_physical_plan().await.unwrap();
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        if !plan.contains("-0") {
            errors.push(format!("no `-0` in the plan of {sql}\n{plan}"));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}
