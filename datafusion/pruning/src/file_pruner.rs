// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! File-level pruning based on partition values and file-level statistics

use std::sync::Arc;

use arrow::datatypes::{FieldRef, SchemaRef};
use datafusion_common::{Result, internal_datafusion_err, pruning::PrunableStatistics};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::DynamicFilterTracking;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::Count;
use log::debug;

use crate::build_pruning_predicate;

/// Prune based on file-level statistics.
///
/// Note: Partition column pruning is handled earlier via `replace_columns_with_literals`
/// which substitutes partition column references with their literal values before
/// the predicate reaches this pruner.
pub struct FilePruner {
    predicate: Arc<dyn PhysicalExpr>,
    /// Tracks the dynamic filters inside `predicate` so we only rebuild the
    /// pruning predicate when one of them has actually moved.
    tracking: DynamicFilterTracking,
    /// Whether [`Self::should_prune`] has built+evaluated the pruning predicate
    /// at least once. The first check always runs; subsequent checks only run
    /// when a watched dynamic filter changed.
    checked_once: bool,
    /// Schema used for pruning (the logical file schema).
    file_schema: SchemaRef,
    file_stats_pruning: PrunableStatistics,
    predicate_creation_errors: Count,
}

impl FilePruner {
    #[deprecated(
        since = "52.0.0",
        note = "Use `try_new` instead which returns None if no statistics are available"
    )]
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        logical_file_schema: &SchemaRef,
        _partition_fields: Vec<FieldRef>,
        partitioned_file: PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Result<Self> {
        Self::try_new(
            predicate,
            logical_file_schema,
            &partitioned_file,
            predicate_creation_errors,
        )
        .ok_or_else(|| {
            internal_datafusion_err!(
                "FilePruner::new called on a file without statistics: {:?}",
                partitioned_file
            )
        })
    }

    /// Create a file pruner for this file, or `None` when pruning it cannot
    /// help.
    ///
    /// Returns `None` when the file has no statistics struct to evaluate a
    /// pruning predicate against, or when the predicate is purely static and the
    /// file has no usable column statistics — in that case planning already did
    /// everything such a pruner could. A predicate carrying a dynamic filter is
    /// always accepted (given a statistics struct), since it may prune via
    /// partition-value folding even without column statistics.
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        partitioned_file: &PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Option<Self> {
        // A pruning predicate is evaluated against a statistics struct, so one
        // must exist (its columns may all be `Absent`).
        let file_stats = partitioned_file.statistics.as_ref()?;
        let tracking = DynamicFilterTracking::classify(&predicate);
        // Only build a pruner when it could prune something planning didn't
        // already: the file has real column statistics, or the predicate carries
        // a dynamic filter (whose value, or folded partition columns, can prune
        // even without column statistics). For a purely static predicate with no
        // usable stats there is nothing to gain.
        if !partitioned_file.has_statistics() && !tracking.contains_dynamic_filter() {
            return None;
        }
        let file_stats_pruning =
            PrunableStatistics::new(vec![file_stats.clone()], Arc::clone(file_schema));
        Some(Self {
            predicate,
            tracking,
            checked_once: false,
            file_schema: Arc::clone(file_schema),
            file_stats_pruning,
            predicate_creation_errors,
        })
    }

    /// Returns `true` if this pruner watches a dynamic filter that can still
    /// change, meaning [`Self::should_prune`] is worth re-checking as the scan
    /// progresses. When `false`, the predicate is effectively static for the
    /// remainder of the scan and the caller can avoid wrapping the stream in a
    /// per-batch re-pruning adapter.
    pub fn is_watching(&self) -> bool {
        matches!(self.tracking, DynamicFilterTracking::Watching(_))
    }

    pub fn should_prune(&mut self) -> Result<bool> {
        // Building the pruning predicate is expensive (it involves expression
        // analysis), so we only do it on the first check and whenever a dynamic
        // filter inside the predicate has actually moved.
        //
        // Dynamic filter expressions can change their values during query
        // execution; `DynamicFilterTracking` watches the still-incomplete
        // filters and reports a change at most once per update. A purely static
        // predicate (or one whose dynamic filters have all completed) is checked
        // exactly once.
        let should_build = if self.checked_once {
            self.tracking.watcher().is_some_and(|w| w.changed())
        } else {
            self.checked_once = true;
            true
        };
        if !should_build {
            return Ok(false);
        }
        let pruning_predicate = build_pruning_predicate(
            Arc::clone(&self.predicate),
            &self.file_schema,
            &self.predicate_creation_errors,
        );
        let Some(pruning_predicate) = pruning_predicate else {
            return Ok(false);
        };
        match pruning_predicate.prune(&self.file_stats_pruning) {
            Ok(values) => {
                assert!(values.len() == 1);
                // We expect a single container -> if all containers are false skip this file
                if values.into_iter().all(|v| !v) {
                    return Ok(true);
                }
            }
            // Stats filter array could not be built, so we can't prune
            Err(e) => {
                debug!("Ignoring error building pruning predicate for file: {e}");
                self.predicate_creation_errors.add(1);
            }
        }

        Ok(false)
    }
}
