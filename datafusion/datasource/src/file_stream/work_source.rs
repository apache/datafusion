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

use std::collections::VecDeque;
use std::sync::Arc;

use crate::file_groups::FileGroup;
use crate::file_scan_config::FileScanConfig;
use crate::{FileRange, PartitionedFile, SplittableExt};
use datafusion_common::Statistics;
use parking_lot::Mutex;

/// Minimum morsel size in bytes. Morsels smaller than this are combined;
/// files are only split when each half would be at least this large.
const MIN_MORSEL_SIZE: usize = 1024 * 1024; // 1 MiB

/// Source of work for `ScanState`.
///
/// Streams that may share work across siblings use [`WorkSource::Shared`],
/// while streams that can not share work (e.g. because they must preserve file
/// order) use  [`WorkSource::Local`].
#[derive(Debug, Clone)]
pub(super) enum WorkSource {
    /// Files this stream will plan locally without sharing them.
    Local(VecDeque<PartitionedFile>),
    /// Files shared with sibling streams.
    Shared(SharedWorkSource),
}

impl WorkSource {
    /// Pop the next file to plan from this work source.
    ///
    /// For shared sources, large files may be split in half when the queue
    /// is running low, so idle siblings have work to steal.
    pub(super) fn pop_front(&mut self) -> Option<PartitionedFile> {
        match self {
            Self::Local(files) => files.pop_front(),
            Self::Shared(shared) => shared.pop_front(),
        }
    }

    /// Return the number of files that are still waiting to be planned.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Local(files) => files.len(),
            Self::Shared(shared) => shared.len(),
        }
    }
}

/// Shared source of work for sibling `FileStream`s
///
/// The queue is created once per execution and shared by all reorderable
/// sibling streams for that execution. Whichever stream becomes idle first may
/// take the next unopened file from the front of the queue.
///
/// When the queue is running low (fewer than `2 * target_partitions` items),
/// large files are split in half so idle siblings have work to steal.
/// Conversely, very small files are batched together so each stream processes
/// at least ~1 MiB of data per round-trip.
///
/// It uses a [`Mutex`] internally to provide thread-safe access
/// to the shared file queue.
#[derive(Debug, Clone)]
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug)]
pub(super) struct SharedWorkSourceInner {
    files: Mutex<VecDeque<PartitionedFile>>,
    target_partitions: usize,
    /// Column indices (into the table schema) that the scan projects.
    /// `None` means all columns are read.
    projected_columns: Option<Vec<usize>>,
    /// Fallback ratio when per-column byte_size stats are absent.
    projection_ratio: f64,
}

impl SharedWorkSource {
    /// Create a shared work source for the unopened files in `config`.
    pub(crate) fn from_config(config: &FileScanConfig) -> Self {
        let target_partitions = config.file_groups.len();
        let total_file_columns = config.file_schema().fields().len().max(1);
        let projected_columns = config.file_source.projection().map(|p| {
            p.expr_iter()
                .filter_map(|e| {
                    e.as_any()
                        .downcast_ref::<datafusion_physical_expr::expressions::Column>()
                        .map(|c| c.index())
                })
                .collect::<Vec<_>>()
        });
        let projection_ratio = projected_columns
            .as_ref()
            .map(|cols| cols.len() as f64 / total_file_columns as f64)
            .unwrap_or(1.0);

        let files = config
            .file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .cloned()
            .collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                files: Mutex::new(files),
                target_partitions,
                projected_columns,
                projection_ratio,
            }),
        }
    }

    /// Pop the next file from the shared work queue.
    ///
    /// **Splitting**: when the remaining queue depth is below
    /// `2 * target_partitions` and the file's projected size is at least
    /// `2 * MIN_MORSEL_SIZE`, it is split in half and the second half is
    /// pushed back onto the queue.
    ///
    /// **Merging**: when the popped file is below `MIN_MORSEL_SIZE`,
    /// adjacent queue entries that refer to the same underlying file are
    /// merged (their byte ranges are combined) until the merged result
    /// reaches `MIN_MORSEL_SIZE` or no more same-file entries remain.
    fn pop_front(&self) -> Option<PartitionedFile> {
        let mut files = self.inner.files.lock();
        let mut file = files.pop_front()?;

        let projected_size = self.inner.projected_byte_size(&file);

        // Split large files when the queue is shallow.
        let should_split = files.len() < 2 * self.inner.target_partitions
            && projected_size >= 2 * MIN_MORSEL_SIZE;
        if let (true, Some((first, second))) = (should_split, split_file(&file)) {
            files.push_back(second);
            return Some(first);
        }

        // Merge small same-file ranges until we reach MIN_MORSEL_SIZE.
        if projected_size < MIN_MORSEL_SIZE {
            merge_same_file(&mut file, &mut files, &self.inner, MIN_MORSEL_SIZE);
        }

        Some(file)
    }

    /// Return the number of files still waiting in the shared queue.
    fn len(&self) -> usize {
        self.inner.files.lock().len()
    }
}

impl SharedWorkSourceInner {
    /// Estimate the projected byte size for `file`.
    ///
    /// Uses per-column `byte_size` from [`PartitionedFile::statistics`] when
    /// available, otherwise falls back to
    /// `raw_file_size * projection_ratio`.
    fn projected_byte_size(&self, file: &PartitionedFile) -> usize {
        if let (Some(cols), Some(stats)) =
            (&self.projected_columns, file.statistics.as_ref())
        {
            let col_stats = &stats.column_statistics;
            let sum: Option<usize> = cols
                .iter()
                .map(|&idx| {
                    col_stats
                        .get(idx)
                        .and_then(|cs| cs.byte_size.get_value().copied())
                })
                .collect::<Option<Vec<_>>>()
                .map(|v| v.into_iter().sum());
            if let Some(size) = sum {
                return size;
            }
        }
        // Fallback: raw file/range size scaled by projection ratio.
        let raw = raw_file_byte_size(file);
        (raw as f64 * self.projection_ratio) as usize
    }
}

/// Return the raw on-disk byte size of a [`PartitionedFile`].
fn raw_file_byte_size(file: &PartitionedFile) -> usize {
    let (start, end) = file_range(file);
    (end - start) as usize
}

/// Merge entries from `queue` into `file` while they refer to the same
/// underlying path and the projected size stays below `target`.
fn merge_same_file(
    file: &mut PartitionedFile,
    queue: &mut VecDeque<PartitionedFile>,
    inner: &SharedWorkSourceInner,
    target: usize,
) {
    let path = &file.object_meta.location;
    while inner.projected_byte_size(file) < target {
        let same_file = queue
            .front()
            .is_some_and(|next| next.object_meta.location == *path);
        if !same_file {
            break;
        }
        let next = queue.pop_front().unwrap();
        // Extend the byte range to cover both entries.
        let (a_start, a_end) = file_range(file);
        let (b_start, b_end) = file_range(&next);
        file.range = Some(FileRange {
            start: a_start.min(b_start),
            end: a_end.max(b_end),
        });
        // Drop per-file statistics — they no longer match the merged range.
        file.statistics = None;
    }
}

/// Return the effective (start, end) byte range of a file.
fn file_range(file: &PartitionedFile) -> (i64, i64) {
    match &file.range {
        Some(range) => (range.start, range.end),
        None => (0, file.object_meta.size as i64),
    }
}

/// Split a file into two halves.
///
/// If the file's extension is a [`SplittableExt`] (e.g.
/// Parquet row-group selection), the extension is split instead of using
/// byte ranges. Otherwise falls back to byte-range splitting.
///
/// Statistics are scaled to 50% (inexact) on each half.
fn split_file(file: &PartitionedFile) -> Option<(PartitionedFile, PartitionedFile)> {
    let half_stats = file.statistics.as_ref().map(|s| Arc::new(scale_stats(s, 0.5)));

    // Try extension-based splitting (e.g. Parquet row groups).
    let ext_split = file
        .extensions
        .as_ref()
        .and_then(|ext| ext.downcast_ref::<SplittableExt>())
        .and_then(|wrapper| {
            let (ext_a, ext_b) = (wrapper.split_fn)(wrapper.inner.as_ref())?;
            let wrap = |inner| {
                Arc::new(SplittableExt {
                    inner,
                    split_fn: wrapper.split_fn,
                }) as Arc<dyn std::any::Any + Send + Sync>
            };
            Some((wrap(ext_a), wrap(ext_b)))
        });
    if let Some((ext_a, ext_b)) = ext_split {
        let mut first = file.clone();
        first.extensions = Some(ext_a);
        first.statistics.clone_from(&half_stats);

        let mut second = file.clone();
        second.extensions = Some(ext_b);
        second.statistics = half_stats;

        return Some((first, second));
    }

    // Fallback: byte-range splitting.
    let (start, end) = file_range(file);
    if end - start < 2 {
        return None;
    }
    let mid = start + (end - start) / 2;

    let mut first = file.clone();
    first.range = Some(FileRange { start, end: mid });
    first.statistics.clone_from(&half_stats);

    let mut second = file.clone();
    second.range = Some(FileRange { start: mid, end });
    second.statistics = half_stats;

    Some((first, second))
}

/// Scale row counts and byte sizes in `stats` by `ratio`, marking
/// everything inexact.
fn scale_stats(stats: &Statistics, ratio: f64) -> Statistics {
    use datafusion_common::stats::Precision;
    let scale = |p: &Precision<usize>| match p.get_value() {
        Some(&v) => Precision::Inexact((v as f64 * ratio) as usize),
        None => Precision::Absent,
    };
    Statistics {
        num_rows: scale(&stats.num_rows),
        total_byte_size: scale(&stats.total_byte_size),
        column_statistics: stats
            .column_statistics
            .iter()
            .map(|cs| {
                let mut cs = cs.clone().to_inexact();
                cs.byte_size = scale(&cs.byte_size);
                cs
            })
            .collect(),
    }
}
