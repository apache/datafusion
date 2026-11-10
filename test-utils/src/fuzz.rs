// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Shared execution and replay for [`FuzzTest`] experiments.

use std::future::Future;
use std::time::Duration;

use datafusion_common::instant::Instant;
use datafusion_common::{Result, exec_err};

/// Implement this trait to add a fuzzing unit test to DataFusion.
///
/// The fuzz implementation prepares the data and checks one case. The [centralized driver]
/// repeats cases, manages the time budget, and reports failures.
///
/// See the [centralized driver] for how to use it. At a high level:
/// 1. Generate one dataset with `prepare_dataset()`.
/// 2. Run several cases on that dataset with `run_case()`.
/// 3. Repeat steps 1 and 2 until the time budget is reached.
///
/// (All configuration, such as the total time budget, lives in [`RunConfig`].)
///
/// [centralized driver]: https://github.com/apache/datafusion/blob/main/datafusion/fuzz/tests/fuzz.rs
#[expect(async_fn_in_trait)] // Serial execution deliberately does not require Send futures.
pub trait FuzzTest {
    /// Reusable fixtures; `()` is valid when no preparation can be shared.
    type Dataset;

    /// Test identity, including any static variant needed for replay.
    fn name(&self) -> &str;

    /// Build all fixtures from this seed and the test's fixed configuration.
    async fn prepare_dataset(&self, dataset_seed: u64) -> Result<Self::Dataset>;

    /// Execute one complete, independent validation and clean up its resources.
    ///
    /// The implementation should ensure:
    /// - The test case is reproducible given the same dataset and seed.
    /// - If the test fails, the error message includes all the information needed
    ///   to reproduce it (by providing the data seed and case seed).
    async fn run_case(&self, dataset: &Self::Dataset, case_seed: u64) -> Result<()>;

    /// Reproduce the failing case deterministically.
    ///
    /// The input seed should be printed in the error message from [`Self::run_case`]
    /// when the test fails.
    async fn reproduce(&self, dataset_seed: u64, case_seed: u64) -> Result<()> {
        let dataset = with_context(self.name(), None, dataset_seed, None, || {
            self.prepare_dataset(dataset_seed)
        })
        .await?;
        with_context(self.name(), None, dataset_seed, Some(case_seed), || {
            self.run_case(&dataset, case_seed)
        })
        .await
    }
}

/// Execution limits for [`run`]. At least one stopping condition is required.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Initial seed, or `None` to choose a fresh seed once per run.
    pub run_seed: Option<u64>,
    /// Maximum successful cases; must be positive when supplied.
    pub max_cases: Option<usize>,
    /// Soft budget including preparation. The first complete case always runs,
    /// even with a zero budget. Active preparation and cases are never cancelled.
    pub time_limit: Option<Duration>,
    /// Maximum cases sharing one fixture; must be positive.
    pub cases_per_dataset: usize,
}

impl RunConfig {
    fn validate(&self) -> Result<()> {
        if self.max_cases.is_none() && self.time_limit.is_none() {
            return exec_err!("Fuzz run requires max_cases or time_limit");
        }
        if self.max_cases == Some(0) {
            return exec_err!("Fuzz run max_cases must be greater than zero");
        }
        if self.cases_per_dataset == 0 {
            return exec_err!("Fuzz run cases_per_dataset must be greater than zero");
        }
        Ok(())
    }

    // Keep admission separate from the clock so deadline boundaries can be
    // tested without sleeping. Only the first case may start past the deadline.
    fn should_stop(&self, cases_passed: usize, elapsed: Duration) -> bool {
        self.max_cases.is_some_and(|limit| cases_passed >= limit)
            || (cases_passed > 0 && self.time_limit.is_some_and(|limit| elapsed >= limit))
    }
}

/// Counters returned by a successful campaign. Failures stop the run immediately.
#[derive(Debug)]
pub struct RunStats {
    /// Resolved seed, including when the caller requested a fresh one.
    pub run_seed: u64,
    /// Number of successfully prepared fixtures.
    pub datasets_prepared: usize,
    /// Number of complete validations that returned successfully.
    pub cases_passed: usize,
    /// Total execution time, including fixture preparation and cleanup.
    pub elapsed: Duration,
}

/// Run cases serially until either limit is reached or a failure occurs.
///
/// Case seeds are `run_seed + case_index`, wrapping at `u64::MAX`. Dataset seeds
/// independently use `run_seed + dataset_index`. Neither sequence depends on
/// randomness consumed by the test. This also lets existing consecutive-seed
/// fuzzers retain their coverage. A timed run may finish different case counts
/// on different machines; use a fixed count for a repeatable seed sequence.
pub async fn run<T: FuzzTest>(test: &T, config: RunConfig) -> Result<RunStats> {
    config.validate()?;
    let run_seed = config.run_seed.unwrap_or_else(rand::random);
    let start = Instant::now();
    let mut stats = RunStats {
        run_seed,
        datasets_prepared: 0,
        cases_passed: 0,
        elapsed: Duration::ZERO,
    };
    while !config.should_stop(stats.cases_passed, start.elapsed()) {
        let dataset_seed = run_seed.wrapping_add(stats.datasets_prepared as u64);
        let dataset =
            with_context(test.name(), Some(run_seed), dataset_seed, None, || {
                test.prepare_dataset(dataset_seed)
            })
            .await?;
        stats.datasets_prepared += 1;
        for _ in 0..config.cases_per_dataset {
            if config.should_stop(stats.cases_passed, start.elapsed()) {
                break;
            }
            let case_seed = run_seed.wrapping_add(stats.cases_passed as u64);
            with_context(
                test.name(),
                Some(run_seed),
                dataset_seed,
                Some(case_seed),
                || test.run_case(&dataset, case_seed),
            )
            .await?;
            stats.cases_passed += 1;
        }
    }
    stats.elapsed = start.elapsed();
    Ok(stats)
}

/// Print before invoking the operation so libtest captures context even for
/// assertion panics. Returned errors retain their original source; panics unwind
/// normally with their original payload and hook, without process-global changes.
async fn with_context<T, F: Future<Output = Result<T>>>(
    name: &str,
    run_seed: Option<u64>,
    dataset_seed: u64,
    case_seed: Option<u64>,
    operation: impl FnOnce() -> F,
) -> Result<T> {
    let phase = if case_seed.is_some() {
        "run_case"
    } else {
        "prepare_dataset"
    };
    let run_context = run_seed
        .map(|seed| format!("Run seed: {seed}\n"))
        .unwrap_or_default();
    let instruction = if let Some(case_seed) = case_seed {
        format!(
            "Case seed: {case_seed}\nReproduce with this test's configuration: \
             test.reproduce({dataset_seed}, {case_seed}).await"
        )
    } else {
        format!(
            "Retry with this test's configuration: \
             test.prepare_dataset({dataset_seed}).await"
        )
    };
    let context = format!(
        "Fuzz test: {name}\nPhase: {phase}\n{run_context}Dataset seed: {dataset_seed}\n{instruction}"
    );
    eprintln!("{context}");
    operation().await.map_err(|error| error.context(context))
}
