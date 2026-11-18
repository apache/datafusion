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
use datafusion_common_runtime::JoinSet;
use futures::future::join;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use test_utils::fuzz::FuzzTest;

mod data;
mod queries;
mod settings;

use queries::{KNOWN_FAILURES, KnownFailure, TEMPLATES, Template};
use settings::Setting;

/// Number of rows a random table may have.
const TABLE_SIZES: &[usize] = &[8, 16, 32, 64, 128];

/// Each dataset is a random table registered under every setting. Each case
/// runs one random (template, setting) pair. A known failure may give a wrong
/// result on random data too, but it must not error or panic.
pub(crate) struct FloatZeroFuzzer;

impl FuzzTest for FloatZeroFuzzer {
    /// Every pair that runs, with a context that has the random table.
    type Dataset = Vec<(&'static Template, Setting, SessionContext)>;

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
        let (template, setting, ctx) = pairs
            .choose(&mut StdRng::seed_from_u64(seed))
            .ok_or_else(|| exec_datafusion_err!("no (template, setting) pair runs"))?;
        match (
            check(ctx, template, setting).await,
            KnownFailure::matches(template, setting),
        ) {
            (Ok(()), _) | (Err(Failure::Mismatch(_)), Some(_)) => Ok(()),
            (Err(failure), None) => exec_err!("{failure}"),
            (Err(failure), Some(known)) => exec_err!(
                "{failure}\nknown to give a wrong result, but now fails ({})",
                known.issue
            ),
        }
    }
}

/// Every (template, setting) pair that runs, each with a context that has
/// `data` registered under that setting.
fn pairs(
    data: &data::Dataset,
) -> Result<Vec<(&'static Template, Setting, SessionContext)>> {
    let mut pairs = vec![];
    for setting in Setting::all() {
        let ctx = setting.context(data)?;
        for template in TEMPLATES.iter().filter(|t| runs(t, &setting)) {
            pairs.push((template, setting, ctx.clone()));
        }
    }
    Ok(pairs)
}

/// Templates without a zero literal skip the `negative_literals` settings,
/// which would run the same queries again.
fn runs(template: &Template, setting: &Setting) -> bool {
    !setting.negative_literals || template.sql.contains("{z}")
}

/// Every listed (template, setting) pair must fail on the fixed data and
/// every other pair must pass.
#[tokio::test(flavor = "multi_thread")]
async fn float_zero_fixed() {
    let mut errors = vec![];
    // Which KNOWN_FAILURES entries apply to some pair that ran.
    let mut used = vec![false; KNOWN_FAILURES.len()];
    for (template, setting, result) in check_all(&data::fixed()).await {
        let applying: Vec<usize> = (0..KNOWN_FAILURES.len())
            .filter(|&i| {
                let known = &KNOWN_FAILURES[i];
                known.template == template.name && (known.applies)(&setting)
            })
            .collect();
        if applying.len() > 1 {
            errors.push(format!(
                "{} under {setting:?} matches {} KNOWN_FAILURES entries, \
                 only the first one counts",
                template.name,
                applying.len()
            ));
        }
        for &i in &applying {
            used[i] = true;
        }
        match (result, KnownFailure::matches(template, &setting)) {
            (Ok(()), None) | (Err(Failure::Mismatch(_)), Some(_)) => {}
            (Err(failure), None) => errors.push(failure.to_string()),
            (Err(Failure::Error(e)), Some(known)) => errors.push(format!(
                "{e}\nknown to give a wrong result, but now fails ({})",
                known.issue
            )),
            (Ok(()), Some(known)) => errors.push(format!(
                "{} under {setting:?} now passes, remove it from KNOWN_FAILURES ({})",
                template.name, known.issue
            )),
        }
    }
    for (known, used) in KNOWN_FAILURES.iter().zip(used) {
        if !used {
            errors.push(format!(
                "KNOWN_FAILURES entry for {} applies to no pair that runs: \
                 the template does not exist or `applies` matches no setting ({})",
                known.template, known.issue
            ));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

/// A `-0.0` literal in a `raw` query must reach the physical plan with its
/// sign, or the `negative_literals` settings test nothing.
#[tokio::test]
async fn float_zero_negative_literal() {
    let mut errors = vec![];
    for (template, setting, ctx) in pairs(&data::fixed()).unwrap() {
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

/// Why a (template, setting) pair failed.
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

/// Runs every template under every setting on `data` and returns the
/// results in a fixed order. Each pair runs in its own task: the queries are
/// tiny, so planning dominates and running them in parallel is what makes the
/// test fast.
async fn check_all(
    data: &data::Dataset,
) -> Vec<(&'static Template, Setting, Result<(), Failure>)> {
    let mut join_set = JoinSet::new();
    let pairs = pairs(data).unwrap();
    let num_pairs = pairs.len();
    for (index, (template, setting, ctx)) in pairs.into_iter().enumerate() {
        join_set.spawn(async move {
            let result = check(&ctx, template, &setting).await;
            (index, (template, setting, result))
        });
    }
    let mut results = Vec::with_capacity(num_pairs);
    while let Some(joined) = join_set.join_next().await {
        // A panic inside a query is a bug of its own: re-raise it with its
        // own message.
        results
            .push(joined.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic())));
    }
    // Tasks finish in any order; report failures in the order they ran.
    results.sort_unstable_by_key(|(index, _)| *index);
    results.into_iter().map(|(_, result)| result).collect()
}

/// Runs `template` on `raw` and `canon` and compares the results with all
/// floats normalized.
async fn check(
    ctx: &SessionContext,
    template: &Template,
    setting: &Setting,
) -> Result<(), Failure> {
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
        (Err(canon), Err(raw)) => {
            return Err(Failure::Error(format!(
                "{}\nfails on both tables\n  raw:   {raw}\n  canon: {canon}",
                describe()
            )));
        }
        (Ok(_), Err(e)) => {
            return Err(Failure::Error(format!("{}\nraw fails: {e}", describe())));
        }
        (Err(e), Ok(_)) => {
            return Err(Failure::Error(format!("{}\ncanon fails: {e}", describe())));
        }
    };
    if !template.ordered {
        canon.sort_unstable();
        raw.sort_unstable();
    }
    if canon == raw {
        return Ok(());
    }
    let diff = canon
        .iter()
        .zip(&raw)
        .position(|(c, r)| c != r)
        .map(|i| {
            let row = if template.ordered {
                "row"
            } else {
                "sorted row"
            };
            format!("{row} {i}: raw {}, canon {}", raw[i], canon[i])
        })
        .unwrap_or_else(|| format!("raw {} rows, canon {} rows", raw.len(), canon.len()));
    Err(Failure::Mismatch(format!("{}\n{diff}", describe())))
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

async fn run_sql(sql: &str, ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}
