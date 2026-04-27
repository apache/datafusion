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

//! Sort pushdown helpers for [`FileScanConfig`].
//!
//! This module contains the statistics-based file sorting, non-overlapping
//! validation, and NULL handling logic used by
//! [`FileScanConfig::try_pushdown_sort`](super::FileScanConfig::try_pushdown_sort).
//!
//! Extracted from `file_scan_config.rs` to keep that module focused on
//! core configuration and data-source plumbing.

use super::FileScanConfig;
use crate::file::FileSource;
use crate::file_groups::FileGroup;
use crate::source::DataSource;
use crate::statistics::MinMaxStatistics;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::stats::Precision;
use datafusion_physical_expr::equivalence::project_orderings;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::SortOrderPushdownResult;
use log::debug;
use std::sync::Arc;

/// Result of sorting files within groups by their min/max statistics.
pub(crate) struct SortedFileGroups {
    file_groups: Vec<FileGroup>,
    any_reordered: bool,
    all_non_overlapping: bool,
}

impl FileScanConfig {
    ///
    /// This is the core of sort pushdown for file-based sources. It performs
    /// three optimizations depending on the pushdown result:
    ///
    /// ```text
    /// ┌─────────────────────────────────────────────────────────────┐
    /// │                 rebuild_with_source                         │
    /// │                                                             │
    /// │  1. Reverse file groups (if DESC matches reversed ordering) │
    /// │  2. Sort files within groups by min/max statistics          │
    /// │  3. If Exact + non-overlapping:                             │
    /// │     Keep output_ordering → SortExec eliminated              │
    /// │     Otherwise: clear output_ordering → SortExec stays       │
    /// └─────────────────────────────────────────────────────────────┘
    /// ```
    ///
    /// # Why sort files by statistics?
    ///
    /// Files within a partition (file group) are read sequentially. By sorting
    /// them so that file_i.max <= file_{i+1}.min, the combined output stream
    /// is already in order — no SortExec needed for that partition.
    ///
    /// Even when files overlap (Inexact), statistics-based ordering helps
    /// TopK/LIMIT queries: reading low-value files first lets dynamic filters
    /// prune high-value files earlier.
    pub(crate) fn rebuild_with_source(
        &self,
        new_file_source: Arc<dyn FileSource>,
        is_exact: bool,
        order: &[PhysicalSortExpr],
    ) -> Result<FileScanConfig> {
        let mut new_config = self.clone();

        // Reverse file order (within each group) if the caller is requesting a reversal of this
        // scan's declared output ordering.
        let reverse_file_groups = if self.output_ordering.is_empty() {
            false
        } else if let Some(requested) = LexOrdering::new(order.iter().cloned()) {
            let projected_schema = self.projected_schema()?;
            let orderings = project_orderings(&self.output_ordering, &projected_schema);
            orderings
                .iter()
                .any(|ordering| ordering.is_reverse(&requested))
        } else {
            false
        };

        if reverse_file_groups {
            new_config.file_groups = new_config
                .file_groups
                .into_iter()
                .map(|group| {
                    let mut files = group.into_inner();
                    files.reverse();
                    files.into()
                })
                .collect();
        }

        new_config.file_source = new_file_source;

        // Sort files within groups by statistics when not reversing
        let all_non_overlapping = if !reverse_file_groups {
            if let Some(sort_order) = LexOrdering::new(order.iter().cloned()) {
                let projected_schema = new_config.projected_schema()?;
                let projection_indices = new_config
                    .file_source
                    .projection()
                    .as_ref()
                    .and_then(|p| ordered_column_indices_from_projection(p));
                let result = sort_files_within_groups_by_statistics(
                    &new_config.file_groups,
                    &sort_order,
                    &projected_schema,
                    projection_indices.as_deref(),
                );
                new_config.file_groups = result.file_groups;
                result.all_non_overlapping
            } else {
                false
            }
        } else {
            // When reversing, files are already reversed above. We skip
            // statistics-based sorting here because it would undo the reversal.
            // Note: reverse path is always Inexact, so all_non_overlapping
            // is not used (is_exact is false).
            false
        };

        if is_exact && all_non_overlapping {
            // Truly exact: within-file ordering guaranteed and files are non-overlapping.
            // Keep output_ordering so SortExec can be eliminated for each partition.
            //
            // We intentionally do NOT redistribute files across groups here.
            // The planning-phase bin-packing may interleave file ranges across groups:
            //
            //   Group 0: [f1(1-10), f3(21-30)]   ← interleaved with group 1
            //   Group 1: [f2(11-20), f4(31-40)]
            //
            // This interleaving is actually beneficial because SPM pulls from both
            // partitions concurrently, keeping parallel I/O active:
            //
            //   SPM: pull P0 [1-10] → pull P1 [11-20] → pull P0 [21-30] → pull P1 [31-40]
            //        ^^^^^^^^^^^^     ^^^^^^^^^^^^
            //        both partitions scanning files simultaneously
            //
            // If we were to redistribute files consecutively:
            //   Group 0: [f1(1-10), f2(11-20)]   ← all values < group 1
            //   Group 1: [f3(21-30), f4(31-40)]
            //
            // SPM would read ALL of group 0 first (values always smaller), then group 1.
            // This degrades to single-threaded sequential I/O — the other partition
            // sits idle the entire time, losing the parallelism benefit.
        } else {
            new_config.output_ordering = vec![];
        }

        Ok(new_config)
    }

    /// Last-resort optimization when FileSource returns `Unsupported`.
    ///
    /// FileSource may return `Unsupported` because `eq_properties` had no
    /// ordering — which happens when `validated_output_ordering()` stripped
    /// the ordering because files were in the wrong order. After sorting
    /// files by statistics, the ordering may become valid again.
    ///
    /// This method:
    /// 1. Sorts files within groups by min/max statistics
    /// 2. Re-checks if the sorted file order makes `output_ordering` valid
    /// 3. If valid AND non-overlapping → `Exact` (SortExec eliminated!)
    /// 4. If files were reordered but ordering not valid → `Inexact`
    /// 5. If no files were reordered → `Unsupported`
    ///
    /// This handles the key case where files have correct within-file ordering
    /// (e.g., Parquet sorting_columns metadata) but were listed in wrong order
    /// (e.g., alphabetical order doesn't match sort key order).
    pub(crate) fn try_sort_file_groups_by_statistics(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        let Some(sort_order) = LexOrdering::new(order.iter().cloned()) else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };

        let projected_schema = self.projected_schema()?;
        let projection_indices = self
            .file_source
            .projection()
            .as_ref()
            .and_then(|p| ordered_column_indices_from_projection(p));

        let result = sort_files_within_groups_by_statistics(
            &self.file_groups,
            &sort_order,
            &projected_schema,
            projection_indices.as_deref(),
        );

        if !result.any_reordered {
            return Ok(SortOrderPushdownResult::Unsupported);
        }

        let mut new_config = self.clone();
        new_config.file_groups = result.file_groups;

        // Re-check: now that files are sorted, does output_ordering become valid?
        // This handles the case where validated_output_ordering() previously
        // stripped the ordering because files were in the wrong order.
        //
        // IMPORTANT: We cannot claim Exact if any file in a non-last position
        // contains NULLs in the sort columns. With NULLS LAST, NULLs within
        // a file are placed after all non-null values. If the next file has
        // non-null values smaller than the previous file's max, those values
        // would incorrectly appear after the NULLs. Similarly for NULLS FIRST.
        //
        // Conservative approach: if any file has nulls in the sort columns,
        // do not claim Exact. The SortExec will handle NULL ordering correctly.
        if result.all_non_overlapping
            && !self.output_ordering.is_empty()
            && !any_file_has_nulls_in_sort_columns(
                &new_config.file_groups,
                order,
                &projected_schema,
                projection_indices.as_deref(),
            )
        {
            // Files are now non-overlapping, no NULLs in sort columns.
            // Re-ask the FileSource if this ordering satisfies the request,
            // using eq_properties computed from the NEW (sorted) file groups.
            let new_eq_props = new_config.eq_properties();
            if new_eq_props.ordering_satisfy(order.iter().cloned())? {
                // The sorted file order makes the ordering valid → Exact!
                return Ok(SortOrderPushdownResult::Exact {
                    inner: Arc::new(new_config),
                });
            }
        }

        new_config.output_ordering = vec![];
        Ok(SortOrderPushdownResult::Inexact {
            inner: Arc::new(new_config),
        })
    }
}

/// Sort files within each file group by their min/max statistics.
///
/// No files are moved between groups — parallelism and group composition
/// are unchanged. Groups where statistics are unavailable are kept as-is.
///
/// ```text
/// Before:  Group [file_c(20-30), file_a(0-9), file_b(10-19)]
/// After:   Group [file_a(0-9), file_b(10-19), file_c(20-30)]
///                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
///                 sorted by min value, non-overlapping → Exact
/// ```
pub(crate) fn sort_files_within_groups_by_statistics(
    file_groups: &[FileGroup],
    sort_order: &LexOrdering,
    projected_schema: &SchemaRef,
    projection_indices: Option<&[usize]>,
) -> SortedFileGroups {
    let mut any_reordered = false;
    let mut confirmed_non_overlapping: usize = 0;
    let mut new_groups = Vec::with_capacity(file_groups.len());

    for group in file_groups {
        if group.len() <= 1 {
            new_groups.push(group.clone());
            confirmed_non_overlapping += 1;
            continue;
        }

        let files: Vec<_> = group.iter().collect();

        let statistics = match MinMaxStatistics::new_from_files(
            sort_order,
            projected_schema,
            projection_indices,
            files.iter().copied(),
        ) {
            Ok(stats) => stats,
            Err(e) => {
                log::trace!(
                    "Cannot sort file group by statistics: {e}. Keeping original order."
                );
                new_groups.push(group.clone());
                continue;
            }
        };

        let sorted_indices = statistics.min_values_sorted();

        let already_sorted = sorted_indices
            .iter()
            .enumerate()
            .all(|(pos, (idx, _))| pos == *idx);

        let sorted_group: FileGroup = if already_sorted {
            group.clone()
        } else {
            any_reordered = true;
            sorted_indices
                .iter()
                .map(|(idx, _)| files[*idx].clone())
                .collect()
        };

        let sorted_files: Vec<_> = sorted_group.iter().collect();
        let is_non_overlapping = match MinMaxStatistics::new_from_files(
            sort_order,
            projected_schema,
            projection_indices,
            sorted_files.iter().copied(),
        ) {
            Ok(stats) => stats.is_sorted(),
            Err(_) => false,
        };

        if is_non_overlapping {
            confirmed_non_overlapping += 1;
        }

        new_groups.push(sorted_group);
    }

    SortedFileGroups {
        file_groups: new_groups,
        any_reordered,
        all_non_overlapping: confirmed_non_overlapping == file_groups.len(),
    }
}

/// Check if any file in any group has nulls in the sort columns.
pub(crate) fn any_file_has_nulls_in_sort_columns(
    file_groups: &[FileGroup],
    order: &[PhysicalSortExpr],
    projected_schema: &SchemaRef,
    projection_indices: Option<&[usize]>,
) -> bool {
    let Some(sort_columns) =
        sort_columns_from_physical_sort_exprs_nullable(order, projected_schema)
    else {
        return true; // Can't determine, assume nulls exist
    };

    for group in file_groups {
        for file in group.iter() {
            let Some(stats) = file.statistics.as_ref() else {
                return true; // No stats, assume nulls exist
            };
            for col in &sort_columns {
                let stat_idx = projection_indices
                    .map(|p| p[col.index()])
                    .unwrap_or_else(|| col.index());
                if stat_idx >= stats.column_statistics.len() {
                    return true;
                }
                let col_stats = &stats.column_statistics[stat_idx];
                match &col_stats.null_count {
                    Precision::Exact(0) => {}           // No nulls, safe
                    Precision::Exact(_) => return true, // Has nulls
                    _ => return true, // Unknown null count, assume nulls
                }
            }
        }
    }
    false
}

/// Get the indices of columns in a projection if the projection is a simple
/// list of columns.
/// If there are any expressions other than columns, returns None.
pub(crate) fn ordered_column_indices_from_projection(
    projection: &ProjectionExprs,
) -> Option<Vec<usize>> {
    projection
        .expr_iter()
        .map(|e| {
            let index = e.downcast_ref::<Column>()?.index();
            Some(index)
        })
        .collect::<Option<Vec<usize>>>()
}

/// Extract Column references from sort expressions for null checking.
fn sort_columns_from_physical_sort_exprs_nullable(
    order: &[PhysicalSortExpr],
    _schema: &SchemaRef,
) -> Option<Vec<Column>> {
    order
        .iter()
        .map(|expr| expr.expr.downcast_ref::<Column>().cloned())
        .collect()
}

/// Check whether a given ordering is valid for all file groups by verifying
/// that files within each group are sorted according to their min/max statistics.
///
/// For single-file (or empty) groups, the ordering is trivially valid.
/// For multi-file groups, we check that the min/max statistics for the sort
/// columns are in order and non-overlapping (or touching at boundaries).
///
/// `projection` maps projected column indices back to table-schema indices
/// when validating after projection; pass `None` when validating at
/// table-schema level.
pub(crate) fn is_ordering_valid_for_file_groups(
    file_groups: &[FileGroup],
    ordering: &LexOrdering,
    schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> bool {
    file_groups.iter().all(|group| {
        if group.len() <= 1 {
            return true; // single-file groups are trivially sorted
        }
        match MinMaxStatistics::new_from_files(ordering, schema, projection, group.iter())
        {
            Ok(stats) => stats.is_sorted(),
            Err(_) => false, // can't prove sorted → reject
        }
    })
}

/// Filters orderings to retain only those valid for all file groups,
/// verified via min/max statistics.
pub(crate) fn validate_orderings(
    orderings: &[LexOrdering],
    schema: &SchemaRef,
    file_groups: &[FileGroup],
    projection: Option<&[usize]>,
) -> Vec<LexOrdering> {
    orderings
        .iter()
        .filter(|ordering| {
            is_ordering_valid_for_file_groups(file_groups, ordering, schema, projection)
        })
        .cloned()
        .collect()
}

/// The various listing tables do not attempt to read all files
/// concurrently, instead they will read files in sequence within a
/// partition.  This is an important property as it allows plans to
/// run against 1000s of files and not try to open them all
/// concurrently.
///
/// However, it means if we assign more than one file to a partition
/// the output sort order will not be preserved as illustrated in the
/// following diagrams:
///
/// When only 1 file is assigned to each partition, each partition is
/// correctly sorted on `(A, B, C)`
///
/// ```text
/// ┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┓
///   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// ┃   ┌───────────────┐     ┌──────────────┐ │   ┌──────────────┐ │   ┌─────────────┐   ┃
///   │ │   1.parquet   │ │ │ │  2.parquet   │   │ │  3.parquet   │   │ │  4.parquet  │ │
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │   │Sort: A, B, C │ │   │Sort: A, B, C│   ┃
///   │ └───────────────┘ │ │ └──────────────┘   │ └──────────────┘   │ └─────────────┘ │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///      DataFusion           DataFusion           DataFusion           DataFusion
/// ┃    Partition 1          Partition 2          Partition 3          Partition 4       ┃
///  ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///
///                                      DataSourceExec
/// ```
///
/// However, when more than 1 file is assigned to each partition, each
/// partition is NOT correctly sorted on `(A, B, C)`. Once the second
/// file is scanned, the same values for A, B and C can be repeated in
/// the same sorted stream
///
///```text
/// ┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
/// ┃   ┌───────────────┐     ┌──────────────┐ │
///   │ │   1.parquet   │ │ │ │  2.parquet   │   ┃
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///   │ └───────────────┘ │ │ └──────────────┘   ┃
/// ┃   ┌───────────────┐     ┌──────────────┐ │
///   │ │   3.parquet   │ │ │ │  4.parquet   │   ┃
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///   │ └───────────────┘ │ │ └──────────────┘   ┃
/// ┃                                          │
///   │                   │ │                    ┃
/// ┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///      DataFusion           DataFusion         ┃
/// ┃    Partition 1          Partition 2
///  ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┛
///
///              DataSourceExec
/// ```
///
/// **Exception**: When files within a partition are **non-overlapping** (verified
/// via min/max statistics) and each file is internally sorted, the combined
/// output is still correctly sorted. Sort pushdown
/// ([`FileScanConfig::try_pushdown_sort`]) detects this case and preserves
/// `output_ordering`, allowing `SortExec` to be eliminated entirely.
///
/// ```text
///   Partition 1 (files sorted by stats, non-overlapping):
///   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
///   │   1.parquet      │  │   2.parquet      │  │   3.parquet      │
///   │ A: [1..100]      │  │ A: [101..200]    │  │ A: [201..300]    │
///   │ Sort: A, B, C    │  │ Sort: A, B, C    │  │ Sort: A, B, C    │
///   └──────────────────┘  └──────────────────┘  └──────────────────┘
///   max(1) <= min(2) ✓    max(2) <= min(3) ✓   → output_ordering preserved
/// ```
pub(crate) fn get_projected_output_ordering(
    base_config: &FileScanConfig,
    projected_schema: &SchemaRef,
) -> Vec<LexOrdering> {
    let projected_orderings =
        project_orderings(&base_config.output_ordering, projected_schema);

    let indices = base_config
        .file_source
        .projection()
        .as_ref()
        .map(|p| ordered_column_indices_from_projection(p));

    match indices {
        Some(Some(indices)) => {
            // Simple column projection — validate with statistics
            validate_orderings(
                &projected_orderings,
                projected_schema,
                &base_config.file_groups,
                Some(indices.as_slice()),
            )
        }
        None => {
            // No projection — validate with statistics (no remapping needed)
            validate_orderings(
                &projected_orderings,
                projected_schema,
                &base_config.file_groups,
                None,
            )
        }
        Some(None) => {
            // Complex projection (expressions, not simple columns) — can't
            // determine column indices for statistics. Still valid if all
            // file groups have at most one file.
            if base_config.file_groups.iter().all(|g| g.len() <= 1) {
                projected_orderings
            } else {
                debug!(
                    "Skipping specified output orderings. \
                     Some file groups couldn't be determined to be sorted: {:?}",
                    base_config.file_groups
                );
                vec![]
            }
        }
    }
}
