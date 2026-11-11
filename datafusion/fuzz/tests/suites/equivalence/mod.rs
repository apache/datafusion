// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! `EquivalenceProperties` fuzz testing

mod ordering;
mod projection;
mod properties;
mod utils;

use datafusion_common::Result;
use test_utils::fuzz::{FuzzTest, RunConfig};
use utils::NULL_PCTS;

/// Runs `check` once for each pair of a table seed and a NULL percentage.
/// Case seed `k` selects table seed `k / NULL_PCTS.len()` and NULL percentage
/// `NULL_PCTS[k % NULL_PCTS.len()]`.
pub(crate) struct EquivalenceFuzzer {
    name: &'static str,
    /// The number of table seeds that one run uses.
    n_seeds: u64,
    check: fn(u64, f64) -> Result<()>,
}

impl EquivalenceFuzzer {
    /// Runs every pair of the table seeds `0..n_seeds` and `NULL_PCTS`.
    pub(crate) fn run_config(&self) -> RunConfig {
        let n_cases = self.n_seeds as usize * NULL_PCTS.len();
        RunConfig {
            run_seed: Some(0),
            max_cases: Some(n_cases),
            time_limit: None,
            cases_per_dataset: n_cases,
        }
    }
}

impl FuzzTest for EquivalenceFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        self.name
    }

    fn prepare_dataset(&self, _: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    fn run_case(&self, _: &(), case_seed: u64) -> impl Future<Output = Result<()>> {
        let n_null_pcts = NULL_PCTS.len() as u64;
        let null_pct = NULL_PCTS[(case_seed % n_null_pcts) as usize];
        std::future::ready((self.check)(case_seed / n_null_pcts, null_pct))
    }
}

pub(crate) static FUZZERS: [EquivalenceFuzzer; 7] = [
    EquivalenceFuzzer {
        name: "test_ordering_satisfy_with_equivalence_random",
        n_seeds: 5,
        check: ordering::test_ordering_satisfy_with_equivalence_random,
    },
    EquivalenceFuzzer {
        name: "test_ordering_satisfy_with_equivalence_complex_random",
        n_seeds: 100,
        check: ordering::test_ordering_satisfy_with_equivalence_complex_random,
    },
    EquivalenceFuzzer {
        name: "test_ordering_satisfy_with_casts_and_windows_random",
        n_seeds: 100,
        check: ordering::test_ordering_satisfy_with_casts_and_windows_random,
    },
    EquivalenceFuzzer {
        name: "project_orderings_random",
        n_seeds: 20,
        check: projection::project_orderings_random,
    },
    EquivalenceFuzzer {
        name: "ordering_satisfy_after_projection_random",
        n_seeds: 20,
        check: projection::ordering_satisfy_after_projection_random,
    },
    EquivalenceFuzzer {
        name: "test_find_longest_permutation_random",
        n_seeds: 100,
        check: properties::test_find_longest_permutation_random,
    },
    EquivalenceFuzzer {
        name: "test_find_longest_permutation_with_casts_and_windows_random",
        n_seeds: 100,
        check: properties::test_find_longest_permutation_with_casts_and_windows_random,
    },
];
