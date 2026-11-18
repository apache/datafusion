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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::prelude::SessionContext;
use datafusion_common::float_semantics::normalize;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_common_runtime::JoinSet;
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
/// checks one random (template, setting) pair that is not a known failure.
pub(crate) struct FloatZeroFuzzer;

impl FuzzTest for FloatZeroFuzzer {
    type Dataset = Vec<(Setting, SessionContext)>;

    fn name(&self) -> &str {
        "float_zero"
    }

    fn prepare_dataset(&self, seed: u64) -> impl Future<Output = Result<Self::Dataset>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let num_rows = *TABLE_SIZES.choose(&mut rng).unwrap();
        let data = data::random(&mut rng, num_rows);
        std::future::ready(
            Setting::all()
                .into_iter()
                .map(|setting| Ok((setting, setting.context(&data)?)))
                .collect(),
        )
    }

    async fn run_case(&self, contexts: &Self::Dataset, seed: u64) -> Result<()> {
        let pairs: Vec<_> = contexts
            .iter()
            .flat_map(|(setting, ctx)| {
                TEMPLATES
                    .iter()
                    .filter(|t| {
                        runs(t, setting) && KnownFailure::matches(t, setting).is_none()
                    })
                    .map(move |t| (t, setting, ctx))
            })
            .collect();
        let (template, setting, ctx) = *pairs
            .choose(&mut StdRng::seed_from_u64(seed))
            .expect("some pair is not a known failure");
        check(ctx, template, setting)
            .await
            .map_err(|failure| exec_datafusion_err!("{failure}"))
    }
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

/// The `{z}` literal must reach the plan as a Float64 `-0.0`, or the
/// `negative_literals` setting tests nothing.
#[tokio::test]
async fn float_zero_negative_literal() {
    let ctx = SessionContext::new();
    let batches = run_sql("SELECT -0.0", &ctx).await.unwrap();
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    let ScalarValue::Float64(Some(v)) = value else {
        panic!("expected a Float64, got {value:?}");
    };
    assert!(v == 0.0 && v.is_sign_negative(), "expected -0.0, got {v}");
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
/// results in a fixed order. Each
/// pair runs in its own task: the queries are tiny, so planning dominates and
/// running them in parallel is what makes the test fast.
async fn check_all(
    data: &data::Dataset,
) -> Vec<(&'static Template, Setting, Result<(), Failure>)> {
    let mut join_set = JoinSet::new();
    let mut num_pairs = 0;
    for setting in Setting::all() {
        let ctx = setting.context(data).unwrap();
        for template in TEMPLATES {
            if !runs(template, &setting) {
                continue;
            }
            let ctx = ctx.clone();
            let index = num_pairs;
            num_pairs += 1;
            join_set.spawn(async move {
                let result = check(&ctx, template, &setting).await;
                (index, (template, setting, result))
            });
        }
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
    let canon = run_sql(&canon_sql, ctx).await;
    let raw = run_sql(&raw_sql, ctx).await;
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
        .map(|i| format!("row {i}: raw {}, canon {}", raw[i], canon[i]))
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
