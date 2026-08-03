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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::file_groups::FileGroup;
use crate::file_scan_config::FileScanConfig;
use crate::shared_file_state::SharedFileState;
use crate::{FileRange, PartitionedFile};
use datafusion_physical_expr::utils::collect_columns;
use parking_lot::Mutex;

/// Minimum size of a morsel produced by lazy splitting, in bytes of data the
/// scan actually reads (i.e. projected, compressed bytes).
///
/// Each piece pays a fixed cost to open (metadata load, pruning, stream
/// setup), which smaller pieces amortize poorly. Since work items are split
/// by whole-file byte ranges, this floor is translated into file-range bytes
/// by dividing by the estimated fraction of columns the scan reads (see
/// [`projected_fraction`]): reading 1 of 10 columns makes the file-range
/// floor 10x larger so a morsel still carries ~this many bytes of real work.
const MORSEL_MIN_PROJECTED_SIZE: i64 = 1024 * 1024;

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
    pub(super) fn pop_front(&mut self) -> Option<PartitionedFile> {
        match self {
            Self::Local(files) => files.pop_front(),
            Self::Shared(shared) => shared.pop_front(),
        }
    }

    /// Return how many queued files should be counted as already processed
    /// when this stream stops early after hitting a global limit.
    pub(super) fn skipped_on_limit(&self) -> usize {
        match self {
            Self::Local(files) => files.len(),
            Self::Shared(_) => 0,
        }
    }
}

/// Shared source of work for sibling `FileStream`s
///
/// The queue is created once per execution and shared by all reorderable
/// sibling streams for that execution. Whichever stream becomes idle first may
/// take the next unopened file from the front of the queue.
///
/// It uses a [`Mutex`] internally to provide thread-safe access
/// to the shared file queue.
#[derive(Debug, Clone)]
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    state: Mutex<VecDeque<PartitionedFile>>,
    /// Number of sibling streams consuming from this queue. `0` disables
    /// tail splitting of popped byte ranges.
    consumers: usize,
    /// Minimum byte-range size of a tail-split morsel
    /// ([`MORSEL_MIN_PROJECTED_SIZE`] scaled by the projected column
    /// fraction).
    min_morsel_size: i64,
}

fn range_size(file: &PartitionedFile) -> i64 {
    file.range.as_ref().map_or(0, |r| r.end - r.start)
}

impl SharedWorkSource {
    /// Create a shared work source containing the provided unopened files,
    /// without lazy morsel splitting.
    #[cfg(test)]
    pub(crate) fn new(files: impl IntoIterator<Item = PartitionedFile>) -> Self {
        Self::with_consumers(files, 0)
    }

    /// Create a shared work source consumed by `consumers` sibling streams
    /// that lazily splits popped byte ranges into morsels sized to the
    /// remaining work.
    #[cfg(test)]
    pub(crate) fn with_consumers(
        files: impl IntoIterator<Item = PartitionedFile>,
        consumers: usize,
    ) -> Self {
        Self::with_consumers_and_min_morsel_size(
            files,
            consumers,
            MORSEL_MIN_PROJECTED_SIZE,
        )
    }

    fn with_consumers_and_min_morsel_size(
        files: impl IntoIterator<Item = PartitionedFile>,
        consumers: usize,
        min_morsel_size: i64,
    ) -> Self {
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                state: Mutex::new(files.into_iter().collect()),
                consumers,
                min_morsel_size,
            }),
        }
    }

    /// Create a shared work source for the unopened files in `config`.
    ///
    /// Files are reordered by the file source (e.g. by statistics for TopK)
    /// before being placed in the shared queue, so the most promising files
    /// are processed first across all partitions.
    pub(crate) fn from_config(config: &FileScanConfig) -> Self {
        let files: Vec<_> = config
            .file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .cloned()
            .collect();
        let mut files = config.file_source.reorder_files(files);
        attach_shared_file_states(&mut files);
        // A morsel should carry ~MORSEL_MIN_PROJECTED_SIZE bytes of data the
        // scan actually reads; scale the file-range floor by the fraction of
        // columns referenced.
        let min_morsel_size =
            (MORSEL_MIN_PROJECTED_SIZE as f64 / projected_fraction(config)) as i64;
        Self::with_consumers_and_min_morsel_size(
            files,
            config.file_groups.len(),
            min_morsel_size,
        )
    }

    /// Pop the next file from the shared work queue.
    ///
    /// Returns `None` if the queue is empty
    ///
    /// # Tail morsel splitting
    ///
    /// While the queue is deep, popped work items are returned exactly as
    /// the planner sized them. Only at the tail of the scan — when a pop
    /// would leave the queue empty — is the final byte range split into
    /// `min_morsel_size` morsels (the excess halves are pushed back), so
    /// sibling streams that finish their pieces early steal a share of the
    /// last piece instead of idling behind one straggler.
    ///
    /// Splitting to morsel granularity unconditionally was measured slower
    /// even with pieces sharing their opened-file state (see
    /// [`crate::shared_file_state::SharedFileState`]): the residual
    /// per-piece cost (decoder and filter setup, per-piece page reads)
    /// outweighs the balance gain on already even-sized pieces.
    fn pop_front(&self) -> Option<PartitionedFile> {
        let mut files = self.inner.state.lock();
        let mut file = files.pop_front()?;

        if self.inner.consumers > 0 && files.is_empty() {
            while range_size(&file) >= 2 * self.inner.min_morsel_size {
                let second_half = split_off_second_half(&mut file)
                    .expect("file with a non-empty byte range is splittable");
                files.push_front(second_half);
            }
        }
        Some(file)
    }
}

/// Attach one [`SharedFileState`] to all byte-range pieces of the same file
/// so the pieces can share their opened-file state (parsed metadata, pruning
/// results, page index) instead of each repeating that work. Files that are
/// not split into multiple ranged pieces get no shared state.
///
/// Pieces created later by morsel splitting clone their source piece and
/// therefore share the same state.
fn attach_shared_file_states(files: &mut [PartitionedFile]) {
    let mut pieces_per_file: HashMap<&str, usize> = HashMap::new();
    for file in files.iter() {
        if file.range.is_some() {
            *pieces_per_file
                .entry(file.object_meta.location.as_ref())
                .or_default() += 1;
        }
    }
    let states: HashMap<String, Arc<SharedFileState>> = pieces_per_file
        .into_iter()
        .filter(|(_, pieces)| *pieces > 1)
        .map(|(path, _)| (path.to_string(), Arc::default()))
        .collect();
    for file in files.iter_mut() {
        if file.range.is_some()
            && let Some(state) = states.get(file.object_meta.location.as_ref())
        {
            file.extensions.insert_arc(Arc::clone(state));
        }
    }
}

/// Estimate the fraction of a file's bytes this scan reads, as the number of
/// distinct file columns referenced by its projection and filter over the
/// file schema width. This is a column-count approximation (columns are
/// assumed equally wide), used to translate the projected-byte morsel floor
/// into file-range bytes.
fn projected_fraction(config: &FileScanConfig) -> f64 {
    let num_fields = config
        .file_source
        .table_schema()
        .file_schema()
        .fields()
        .len();
    if num_fields == 0 {
        return 1.0;
    }
    let Some(projection) = config.file_source.projection() else {
        return 1.0;
    };
    let mut columns = HashSet::new();
    for expr in projection.iter() {
        columns.extend(collect_columns(&expr.expr));
    }
    if let Some(filter) = config.file_source.filter() {
        columns.extend(collect_columns(&filter));
    }
    // Count at least one column (a bare `count(*)` still reads something)
    // and at most the file schema width (the projection may also reference
    // partition or virtual columns).
    (columns.len().clamp(1, num_fields) as f64) / num_fields as f64
}

/// Shrink `file` to the first half of its byte range and return a clone
/// covering the second half. Returns `None` (leaving `file` untouched) for
/// unranged files, which the planner has not marked as byte-splittable.
fn split_off_second_half(file: &mut PartitionedFile) -> Option<PartitionedFile> {
    let FileRange { start, end } = *file.range.as_ref()?;
    let mid = start + (end - start) / 2;
    let mut second_half = file.clone();
    second_half.range = Some(FileRange { start: mid, end });
    file.range = Some(FileRange { start, end: mid });
    Some(second_half)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: i64 = 1024 * 1024;

    fn ranged_file(size: i64) -> PartitionedFile {
        PartitionedFile::new("test.parquet".to_string(), size as u64).with_range(0, size)
    }

    /// Pop everything and return the byte ranges in pop order.
    fn drain(source: &SharedWorkSource) -> Vec<FileRange> {
        std::iter::from_fn(|| source.pop_front().map(|f| f.range.unwrap())).collect()
    }

    #[test]
    fn lazy_split_tiles_range_into_morsels() {
        let source = SharedWorkSource::with_consumers([ranged_file(8 * MIB)], 2);
        let ranges = drain(&source);

        // The single range was split into several morsels
        assert!(ranges.len() > 1, "expected splitting, got {ranges:?}");
        // Morsels are contiguous, ascending, and tile [0, 8MiB) exactly
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, 8 * MIB);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "gap or overlap: {ranges:?}");
        }
        // No morsel is smaller than the floor
        for r in &ranges {
            assert!(
                r.end - r.start >= MORSEL_MIN_PROJECTED_SIZE,
                "too small: {ranges:?}"
            );
        }
    }

    #[test]
    fn no_split_without_consumers() {
        let source = SharedWorkSource::new([ranged_file(8 * MIB)]);
        assert_eq!(
            drain(&source),
            vec![FileRange {
                start: 0,
                end: 8 * MIB
            }]
        );
    }

    #[test]
    fn only_the_tail_splits_into_morsels() {
        let files = (0..4).map(|i| {
            PartitionedFile::new("test.parquet".to_string(), 32 * MIB as u64)
                .with_range(i * 8 * MIB, (i + 1) * 8 * MIB)
        });
        let source = SharedWorkSource::with_consumers(files, 4);
        let ranges = drain(&source);
        // The first pieces pop unsplit; the final piece splits into morsels
        assert!(ranges.len() > 4, "expected tail splitting, got {ranges:?}");
        for r in &ranges[..3] {
            assert_eq!(r.end - r.start, 8 * MIB, "planner-sized piece: {ranges:?}");
        }
        let last = ranges.last().unwrap();
        assert!(
            (MORSEL_MIN_PROJECTED_SIZE..2 * MORSEL_MIN_PROJECTED_SIZE)
                .contains(&(last.end - last.start)),
            "tail morsel near the floor: {ranges:?}"
        );
    }

    #[test]
    fn unranged_files_are_not_split() {
        let file = PartitionedFile::new("test.parquet".to_string(), 8 * MIB as u64);
        let source = SharedWorkSource::with_consumers([file], 4);
        let popped = source.pop_front().unwrap();
        assert_eq!(popped.range, None);
        assert!(source.pop_front().is_none());
    }

    #[test]
    fn small_ranges_are_not_split() {
        let source = SharedWorkSource::with_consumers([ranged_file(MIB)], 4);
        assert_eq!(drain(&source), vec![FileRange { start: 0, end: MIB }]);
    }

    #[test]
    fn shared_file_states_attached_to_multi_piece_files() {
        let piece = |path: &str, start: i64, end: i64| {
            PartitionedFile::new(path.to_string(), 4 * MIB as u64).with_range(start, end)
        };
        let mut files = vec![
            piece("split.parquet", 0, 2 * MIB),
            piece("split.parquet", 2 * MIB, 4 * MIB),
            piece("solo.parquet", 0, 4 * MIB),
            PartitionedFile::new("unranged.parquet".to_string(), 4 * MIB as u64),
        ];
        attach_shared_file_states(&mut files);

        let state = |f: &PartitionedFile| f.extensions.get_arc::<SharedFileState>();
        // Both pieces of the split file share the same state
        let (a, b) = (state(&files[0]).unwrap(), state(&files[1]).unwrap());
        assert!(Arc::ptr_eq(&a, &b));
        // Files that are not split into multiple pieces get no shared state
        assert!(state(&files[2]).is_none());
        assert!(state(&files[3]).is_none());
    }
}
