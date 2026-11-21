// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Central registration for DataFusion's fuzzing unit tests.
//!
//! # Running the tests
//!
//! Run all fuzz tests with:
//!     `cargo xtask ci step test fuzz [--explain]`.
//!
//! Add `--explain` to print the command without running the tests.
//!
//! # Implementation Notes
//!
//! Migration of the remaining fuzz tests is tracked in [#27082].
//!
//! [#27082]: https://github.com/apache/datafusion/issues/27082

use std::time::Duration;

use datafusion_common::Result;
use test_utils::fuzz::{FuzzTest, RunConfig, run};

mod suites;

// Fuzz test runners.
//
// Each suite is a separate test so the test runner can schedule suites in parallel.
#[tokio::test(flavor = "multi_thread")]
async fn spill_pool_scenario() -> Result<()> {
    run_suite(
        &suites::spill_pool::SpillPoolFuzzer,
        RunConfig {
            run_seed: None,
            max_cases: None,
            time_limit: Some(Duration::from_secs(10)),
            cases_per_dataset: 1000,
        },
    )
    .await
}

#[tokio::test]
async fn lru_queue() -> Result<()> {
    run_suite(
        &suites::lru_queue::LruQueueFuzzer,
        RunConfig {
            run_seed: Some(0),
            max_cases: Some(100),
            time_limit: None,
            cases_per_dataset: 100,
        },
    )
    .await
}

#[tokio::test]
async fn key_range_bitmap_union() -> Result<()> {
    run_suite(
        &suites::key_range_bitmap::KeyRangeBitmapFuzzer,
        RunConfig {
            run_seed: Some(0),
            max_cases: None,
            time_limit: Some(Duration::from_secs(5)),
            cases_per_dataset: 10000,
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

#[tokio::test]
async fn equivalence_ordering_satisfy_with_equivalence() -> Result<()> {
    let fuzzer = &suites::equivalence::ORDERING_SATISFY_WITH_EQUIVALENCE;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_ordering_satisfy_with_equivalence_complex() -> Result<()> {
    let fuzzer = &suites::equivalence::ORDERING_SATISFY_WITH_EQUIVALENCE_COMPLEX;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_ordering_satisfy_with_casts_and_windows() -> Result<()> {
    let fuzzer = &suites::equivalence::ORDERING_SATISFY_WITH_CASTS_AND_WINDOWS;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_project_orderings() -> Result<()> {
    let fuzzer = &suites::equivalence::PROJECT_ORDERINGS;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_ordering_satisfy_after_projection() -> Result<()> {
    let fuzzer = &suites::equivalence::ORDERING_SATISFY_AFTER_PROJECTION;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_find_longest_permutation() -> Result<()> {
    let fuzzer = &suites::equivalence::FIND_LONGEST_PERMUTATION;
    run_suite(fuzzer, fuzzer.run_config()).await
}

#[tokio::test]
async fn equivalence_find_longest_permutation_with_casts_and_windows() -> Result<()> {
    let fuzzer = &suites::equivalence::FIND_LONGEST_PERMUTATION_WITH_CASTS_AND_WINDOWS;
    run_suite(fuzzer, fuzzer.run_config()).await
}

async fn run_suite<T: FuzzTest>(test: &T, config: RunConfig) -> Result<()> {
    eprintln!("Running fuzz suite: {}", test.name());
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
