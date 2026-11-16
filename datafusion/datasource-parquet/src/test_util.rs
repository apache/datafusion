// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Test helpers shared across the Parquet datasource crate.

use crate::row_group_filter::RowGroupAccessPlanFilter;

/// What row groups are expected to be left after pruning.
#[derive(Debug)]
pub(crate) enum ExpectedPruning {
    /// All row groups are expected to be pruned.
    All,
    /// Only the specified row groups are expected to REMAIN (not what is pruned).
    Some(Vec<usize>),
    /// No row groups are expected to be pruned.
    None,
}

impl ExpectedPruning {
    /// Asserts that the pruned row groups match this expectation.
    pub(crate) fn assert(&self, row_groups: &RowGroupAccessPlanFilter) {
        let access_plan = row_groups.access_plan();
        let num_row_groups = access_plan.len();
        assert!(num_row_groups > 0);
        let num_pruned = (0..num_row_groups)
            .filter_map(|i| {
                if access_plan.should_scan(i) {
                    None
                } else {
                    Some(1)
                }
            })
            .sum::<usize>();

        match self {
            Self::All => {
                assert_eq!(
                    num_row_groups, num_pruned,
                    "Expected all row groups to be pruned, but got {row_groups:?}"
                );
            }
            ExpectedPruning::None => {
                assert_eq!(
                    num_pruned, 0,
                    "Expected no row groups to be pruned, but got {row_groups:?}"
                );
            }
            ExpectedPruning::Some(expected) => {
                let actual = access_plan.row_group_indexes();
                assert_eq!(
                    expected, &actual,
                    "Unexpected row groups pruned. Expected {expected:?}, got {actual:?}"
                );
            }
        }
    }
}
