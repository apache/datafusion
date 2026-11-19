// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! One integration entrypoint for DataFusion's registered fuzz suites.
//!
//! Each suite is a separate test, so the test runner can run suites in parallel.
//!
//! Migration of the remaining fuzz tests is tracked in [#27082].
//!
//! [#27082]: https://github.com/apache/datafusion/issues/27082

use datafusion_common::Result;
use test_utils::fuzz::{FuzzTest, RunConfig, run};

mod suites;

#[tokio::test(flavor = "multi_thread")]
async fn spill_pool_scenario() -> Result<()> {
    run_suite(
        &suites::spill_pool::SpillPoolFuzzer,
        RunConfig {
            run_seed: Some(0),
            max_cases: Some(1000),
            time_limit: None,
            cases_per_dataset: 1000,
        },
    )
    .await
}

#[tokio::test]
async fn sort_topk() -> Result<()> {
    run_suite(
        &suites::limit::LimitFuzzer,
        RunConfig {
            run_seed: None,
            max_cases: Some(100),
            time_limit: None,
            cases_per_dataset: 100,
        },
    )
    .await
}

/// Run one suite. Report an error on failure or execution statistics on success.
async fn run_suite<T: FuzzTest>(test: &T, config: RunConfig) -> Result<()> {
    let stats = run(test, config).await?;
    eprintln!(
        "Fuzz suite {}: {} cases, {} datasets, {:?}",
        test.name(),
        stats.cases_passed,
        stats.datasets_prepared,
        stats.elapsed
    );
    Ok(())
}
