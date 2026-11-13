// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! One integration entrypoint for DataFusion's registered fuzz suites.

use std::future::Future;
use std::pin::Pin;

use datafusion_common::Result;
use test_utils::fuzz::{FuzzTest, RunConfig, run};

mod suites;

/// The single registration point for project-wide fuzz campaigns.
///
/// Migration of the remaining fuzz tests is tracked in [#27082].
///
/// [#27082]: https://github.com/apache/datafusion/issues/27082
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_tests() -> Result<()> {
    let spill_pool = suites::spill_pool::SpillPoolFuzzer;
    let suites = std::iter::once(Suite::new(
        &spill_pool,
        RunConfig {
            run_seed: Some(0),
            max_cases: Some(1000),
            time_limit: None,
            cases_per_dataset: 1000,
        },
    ))
    .chain(
        suites::equivalence::FUZZERS
            .iter()
            .map(|fuzzer| Suite::new(fuzzer, fuzzer.run_config())),
    );
    run_suites(suites).await
}

async fn run_suites<'a>(suites: impl IntoIterator<Item = Suite<'a>>) -> Result<()> {
    for suite in suites {
        eprintln!("Running fuzz suite: {}", suite.name);
        suite.run.await?;
    }
    Ok(())
}

/// Wrapper for one fuzzing test suite
struct Suite<'a> {
    name: &'a str,
    /// Run the test suite. Report an error on failure or execution statistics on success.
    run: Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
}

impl<'a> Suite<'a> {
    fn new<T: FuzzTest>(test: &'a T, config: RunConfig) -> Self {
        Self {
            name: test.name(),
            run: Box::pin(async move {
                let stats = run(test, config).await?;
                eprintln!(
                    "Fuzz suite {}: {} cases, {} datasets, {:?}",
                    test.name(),
                    stats.cases_passed,
                    stats.datasets_prepared,
                    stats.elapsed
                );
                Ok(())
            }),
        }
    }
}
