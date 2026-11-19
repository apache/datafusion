// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Differential fuzz test for `-0.0`.
//!
//! DataFusion treats `-0.0` and `0.0` as equal, so changing a `-0.0` to
//! `0.0` must not change any query result, apart from how `-0.0` prints. Each
//! query runs on `raw`, a table with some `-0.0` values, and on `canon`, the
//! same table with every `-0.0` replaced by `0.0`, under the same settings.
//! The results must match once floats in the output are normalized. See
//! <https://github.com/apache/datafusion/issues/26138>.
//!
//! The unit of work is a [`Pair`]: one query template under one setting.
//! [`check`] runs a pair on both tables and compares the results.
//!
//! - [`FloatZeroFuzzer`] checks random pairs on random tables.
//! - [`float_zero_fixed`] checks every pair on a fixed table, on which every
//!   known failure reproduces.
//! - [`known_failures_apply`] checks that every known failure applies to some
//!   setting.
//! - [`float_zero_negative_literal`] checks that `-0.0` literals keep their
//!   sign.
//!
//! The other files:
//!
//! - `data.rs` builds the two tables.
//! - `queries.rs` holds the query templates and the known failures.
//! - `settings.rs` holds the settings each query runs under.

use std::future::Future;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::float_semantics::normalize;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_common_runtime::SpawnedTask;
use futures::future::{join, join_all};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use test_utils::fuzz::FuzzTest;

mod data;
mod queries;
mod settings;

use queries::{TEMPLATES, Template};
use settings::Setting;

/// One template under one setting, with a context that has the `raw` and
/// `canon` tables registered under that setting.
pub(crate) struct Pair {
    template: &'static Template,
    setting: Setting,
    ctx: SessionContext,
}

/// Every pair that runs on `data`.
fn pairs(data: &data::Dataset) -> Result<Vec<Pair>> {
    let mut pairs = vec![];
    for setting in Setting::all() {
        let ctx = setting.context(data)?;
        for template in TEMPLATES.iter().filter(|t| runs(t, &setting)) {
            pairs.push(Pair {
                template,
                setting,
                ctx: ctx.clone(),
            });
        }
    }
    Ok(pairs)
}

/// Templates without a zero literal skip the `negative_literals` settings,
/// which would run the same queries again.
fn runs(template: &Template, setting: &Setting) -> bool {
    !setting.negative_literals || template.sql.contains("{z}")
}

/// Why a pair failed.
enum Failure {
    /// Both queries ran, but the results differ.
    Mismatch(String),
    /// One or both of the queries failed. A failure on both tables usually
    /// means the template itself is broken.
    Error(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Mismatch(msg) | Failure::Error(msg) => f.write_str(msg),
        }
    }
}

/// Runs the pair's template on `raw` and `canon` and compares the results
/// with all floats normalized.
async fn check(pair: &Pair) -> Result<(), Failure> {
    let Pair {
        template,
        setting,
        ctx,
    } = pair;
    let raw_sql = template.render("raw", setting.raw_zero());
    let canon_sql = template.render("canon", "0.0");
    let describe = || {
        format!(
            "{} under {setting:?}\n  raw:   {raw_sql}\n  canon: {canon_sql}",
            template.name
        )
    };

    let (canon, raw) = join(run_sql(&canon_sql, ctx), run_sql(&raw_sql, ctx)).await;
    let (mut canon, mut raw) = match (canon, raw) {
        (Ok(canon), Ok(raw)) => (format_rows(&canon), format_rows(&raw)),
        (canon, raw) => {
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

    if !template.ordered {
        canon.sort_unstable();
        raw.sort_unstable();
    }
    if canon == raw {
        return Ok(());
    }
    let diff = first_difference(&raw, &canon, template.ordered);
    Err(Failure::Mismatch(format!("{}\n{diff}", describe())))
}

async fn run_sql(sql: &str, ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// One string per row, with every float, also nested ones, normalized so
/// that `-0.0` prints as `0.0`.
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

/// Describes the first row where `raw` and `canon` differ.
fn first_difference(raw: &[String], canon: &[String], ordered: bool) -> String {
    let row = if ordered { "row" } else { "sorted row" };
    match raw.iter().zip(canon).position(|(r, c)| r != c) {
        Some(i) => format!("{row} {i}: raw {}, canon {}", raw[i], canon[i]),
        None => format!("raw {} rows, canon {} rows", raw.len(), canon.len()),
    }
}

/// Number of rows a random table may have.
const TABLE_SIZES: &[usize] = &[8, 16, 32, 64, 128];

/// Each dataset is a random table registered under every setting. Each case
/// runs one random pair. A known failure may give a wrong result on random
/// data too, but it must not error or panic.
pub(crate) struct FloatZeroFuzzer;

impl FuzzTest for FloatZeroFuzzer {
    type Dataset = Vec<Pair>;

    fn name(&self) -> &str {
        "float_zero"
    }

    fn prepare_dataset(&self, seed: u64) -> impl Future<Output = Result<Self::Dataset>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let num_rows = *TABLE_SIZES.choose(&mut rng).unwrap();
        let data = data::random(&mut rng, num_rows);
        std::future::ready(pairs(&data))
    }

    async fn run_case(&self, pairs: &Self::Dataset, seed: u64) -> Result<()> {
        let pair = pairs
            .choose(&mut StdRng::seed_from_u64(seed))
            .ok_or_else(|| exec_datafusion_err!("no pair runs"))?;
        match (check(pair).await, pair.template.known_issue(&pair.setting)) {
            (Ok(()), _) | (Err(Failure::Mismatch(_)), Some(_)) => Ok(()),
            (Err(failure), None) => exec_err!("{failure}"),
            (Err(failure), Some(issue)) => exec_err!(
                "{failure}\nknown to give a wrong result, but now fails ({issue})"
            ),
        }
    }
}

/// Every listed pair must give a wrong result on the fixed data and every
/// other pair must pass.
#[tokio::test(flavor = "multi_thread")]
async fn float_zero_fixed() {
    let mut errors = vec![];
    for (pair, result) in check_all(&data::fixed()).await {
        let Pair {
            template, setting, ..
        } = pair;
        match (result, template.known_issue(&setting)) {
            (Ok(()), None) | (Err(Failure::Mismatch(_)), Some(_)) => {}
            (Err(failure), None) => errors.push(failure.to_string()),
            (Err(Failure::Error(e)), Some(issue)) => errors.push(format!(
                "{e}\nknown to give a wrong result, but now fails ({issue})"
            )),
            (Ok(()), Some(issue)) => errors.push(format!(
                "{} under {setting:?} now passes, remove its known failure ({issue})",
                template.name
            )),
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

/// Checks every pair that runs on `data`. Each pair runs in its own task:
/// the queries are tiny, so planning dominates and running them in parallel
/// is what makes the test fast.
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

/// A known failure whose `applies` matches no setting that runs would never
/// be checked.
#[test]
fn known_failures_apply() {
    for template in TEMPLATES {
        let applies = Setting::all()
            .iter()
            .any(|s| runs(template, s) && template.known_issue(s).is_some());
        assert!(
            template.known_failure.is_none() || applies,
            "the known failure of {} applies to no setting that runs",
            template.name
        );
    }
}

/// A `-0.0` literal in a `raw` query must reach the physical plan with its
/// sign, or the `negative_literals` settings test nothing.
#[tokio::test]
async fn float_zero_negative_literal() {
    let mut errors = vec![];
    for Pair {
        template,
        setting,
        ctx,
    } in pairs(&data::fixed()).unwrap()
    {
        if !setting.negative_literals {
            continue;
        }
        let sql = template.render("raw", setting.raw_zero());
        let plan = ctx.sql(&sql).await.unwrap();
        let plan = plan.create_physical_plan().await.unwrap();
        if !has_negative_zero_literal(&plan) {
            errors.push(format!(
                "{} under {setting:?}: no `-0.0` literal in the plan of {sql}\n{}",
                template.name,
                displayable(plan.as_ref()).indent(true)
            ));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

/// Whether a literal in a projection of `plan` is `-0.0` or a list that
/// holds one.
fn has_negative_zero_literal(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    plan.apply(|node| {
        let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        for projected in projection.expr() {
            projected.expr.apply(|expr| {
                found |= expr
                    .downcast_ref::<Literal>()
                    .is_some_and(|literal| is_negative_zero(literal.value()));
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

fn is_negative_zero(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float64(Some(v)) => *v == 0.0 && v.is_sign_negative(),
        ScalarValue::Float32(Some(v)) => *v == 0.0 && v.is_sign_negative(),
        ScalarValue::List(list) => {
            data::has_negative_zero(&(Arc::clone(list) as ArrayRef))
        }
        _ => false,
    }
}
