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

use crate::PartitionedFile;
use crate::file_groups::FileGroup;
use crate::file_scan_config::FileScanConfig;
use crate::morsel::{Morsel, SplitHint};
use parking_lot::Mutex;

/// A unit of work handed to a `ScanState`.
#[derive(Debug)]
pub(super) enum WorkItem {
    /// An unopened file that still needs to be planned.
    File(Box<PartitionedFile>),
    /// A ready morsel donated by a sibling stream that split its remaining
    /// work.
    Morsel(Box<dyn Morsel>),
}

/// Source of work for `ScanState`.
///
/// Streams that may share work across siblings use [`WorkSource::Shared`],
/// while streams that can not share work (e.g. because they must preserve file
/// order) use  [`WorkSource::Local`].
#[derive(Debug)]
pub(super) enum WorkSource {
    /// Files this stream will plan locally without sharing them.
    Local(VecDeque<PartitionedFile>),
    /// Files shared with sibling streams.
    Shared(SharedWorkSource),
}

impl WorkSource {
    /// Pop the next work item from this work source.
    ///
    /// Returns [`PopResult::Pending`] for [`WorkSource::Shared`] when the
    /// queues are empty but a sibling is still planning a file that may yet
    /// be split and donated. The caller must re-schedule its waker and yield
    /// instead of terminating.
    pub(super) fn pop_work(&mut self) -> PopResult {
        match self {
            Self::Local(files) => match files.pop_front() {
                Some(file) => PopResult::Ready(WorkItem::File(Box::new(file)), None),
                None => PopResult::Done,
            },
            Self::Shared(shared) => shared.pop_work(),
        }
    }

    /// Record that the consuming stream has finished and will no longer pick
    /// up work, so the split heuristic only considers still-active streams.
    pub(super) fn mark_finished(&self) {
        if let Self::Shared(shared) = self {
            shared.mark_finished();
        }
    }

    /// Donate all but the first ready morsel to sibling streams.
    ///
    /// This is a no-op for local sources: morsels stay queued on the stream
    /// that produced them.
    pub(super) fn donate_surplus_morsels(
        &self,
        ready_morsels: &mut VecDeque<Box<dyn Morsel>>,
    ) {
        if let Self::Shared(shared) = self
            && ready_morsels.len() > 1
        {
            shared.donate_morsels(ready_morsels.drain(1..));
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

/// Outcome of a pop attempt against a [`WorkSource`].
pub(super) enum PopResult {
    /// Work popped. The optional [`FileLease`] must be held while the file
    /// may still donate morsels; while it is alive, idle siblings keep
    /// polling instead of treating the shared source as drained.
    Ready(WorkItem, Option<FileLease>),
    /// No work currently available, but a sibling is still planning a file
    /// that may donate morsels. The caller must re-schedule its waker and
    /// yield.
    Pending,
    /// No work available and no donors in flight — fully drained.
    Done,
}

/// RAII guard for a file popped from a [`SharedWorkSource`].
///
/// While alive, idle sibling streams keep polling for donated morsels
/// instead of terminating: the holder is still planning the file and may yet
/// split it and donate the surplus. Dropped once the donation window closes
/// (the first morsel starts streaming) or the file is abandoned.
pub(super) struct FileLease {
    inner: Arc<SharedWorkSourceInner>,
}

impl std::fmt::Debug for FileLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLease").finish_non_exhaustive()
    }
}

impl Drop for FileLease {
    fn drop(&mut self) {
        self.inner.queues.lock().in_flight -= 1;
    }
}

/// Shared source of work for sibling `FileStream`s
///
/// The queue is created once per execution and shared by all reorderable
/// sibling streams for that execution. Whichever stream becomes idle first may
/// take the next work item from the front of the queue.
///
/// It uses a [`Mutex`] internally to provide thread-safe access
/// to the shared work queues.
#[derive(Debug, Clone)]
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    queues: Mutex<WorkQueues>,
    /// Number of sibling streams consuming from this source.
    workers: usize,
}

#[derive(Debug, Default)]
struct WorkQueues {
    /// Unopened files that still need to be planned.
    files: VecDeque<PartitionedFile>,
    /// Morsels donated by sibling streams that split their remaining work.
    ///
    /// Preferred over opening new files so partially-planned work finishes
    /// first.
    morsels: VecDeque<Box<dyn Morsel>>,
    /// Number of sibling streams that have already run out of work and
    /// finished.
    finished_workers: usize,
    /// Number of popped files whose [`FileLease`] is still alive. Non-zero
    /// means a donor is still planning a file and may yet donate morsels, so
    /// idle siblings must keep polling rather than terminate.
    in_flight: usize,
}

impl SharedWorkSource {
    /// Create a shared work source consumed by `workers` sibling streams.
    pub(crate) fn with_workers(
        files: impl IntoIterator<Item = PartitionedFile>,
        workers: usize,
    ) -> Self {
        let files = files.into_iter().collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                queues: Mutex::new(WorkQueues {
                    files,
                    ..Default::default()
                }),
                workers,
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
        let files = config.file_source.reorder_files(files);
        Self::with_workers(files, config.file_groups.len())
    }

    /// Pop the next work item from the shared queues.
    ///
    /// Donated morsels are preferred over unopened files so partially-planned
    /// work finishes first. Popping a file registers a [`FileLease`]: while
    /// it is alive, siblings that find the queues empty get
    /// [`PopResult::Pending`] (the file may still be split and donated)
    /// rather than [`PopResult::Done`].
    fn pop_work(&self) -> PopResult {
        let mut queues = self.inner.queues.lock();
        if let Some(morsel) = queues.morsels.pop_front() {
            return PopResult::Ready(WorkItem::Morsel(morsel), None);
        }
        if let Some(file) = queues.files.pop_front() {
            queues.in_flight += 1;
            return PopResult::Ready(
                WorkItem::File(Box::new(file)),
                Some(FileLease {
                    inner: Arc::clone(&self.inner),
                }),
            );
        }
        if queues.in_flight > 0 {
            return PopResult::Pending;
        }
        PopResult::Done
    }

    /// Record that a consuming stream has finished (ran out of work or hit
    /// its limit) and can no longer pick up donated morsels.
    fn mark_finished(&self) {
        self.inner.queues.lock().finished_workers += 1;
    }

    /// Add morsels donated by a stream that split its remaining work.
    fn donate_morsels(&self, morsels: impl Iterator<Item = Box<dyn Morsel>>) {
        self.inner.queues.lock().morsels.extend(morsels);
    }

    /// Return a [`SplitHint`] that asks for splitting only when the remaining
    /// shared work is too small to keep all still-active sibling streams busy.
    pub(crate) fn split_hint(&self) -> SplitHint {
        let inner = Arc::clone(&self.inner);
        SplitHint::new(move || {
            let queues = inner.queues.lock();
            let active_workers = inner.workers.saturating_sub(queues.finished_workers);
            // With a single active stream splitting only adds overhead: there
            // is no idle sibling to pick up the donated morsels.
            active_workers > 1
                && queues.files.len() + queues.morsels.len() < active_workers
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::morsel::Morsel;
    use arrow::array::RecordBatch;
    use datafusion_common::Result;
    use futures::stream::BoxStream;

    #[derive(Debug)]
    struct DummyMorsel;

    impl Morsel for DummyMorsel {
        fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
            Box::pin(futures::stream::empty())
        }
    }

    fn test_file() -> PartitionedFile {
        PartitionedFile::new("test.parquet", 1024)
    }

    #[test]
    fn split_hint_disabled_for_single_worker() {
        let source = SharedWorkSource::with_workers(vec![], 1);
        assert!(!source.split_hint().should_split());
    }

    #[test]
    fn split_hint_tracks_remaining_work_and_active_workers() {
        let files = vec![test_file(), test_file(), test_file()];
        let source = SharedWorkSource::with_workers(files, 2);
        let hint = source.split_hint();

        // 3 files remaining >= 2 workers: no split
        assert!(!hint.should_split());

        assert!(matches!(source.pop_work(), PopResult::Ready(..)));
        assert!(matches!(source.pop_work(), PopResult::Ready(..)));
        // 1 file remaining < 2 workers: split
        assert!(hint.should_split());

        assert!(matches!(source.pop_work(), PopResult::Ready(..)));
        // queue empty, both workers still active: split
        assert!(hint.should_split());

        // one worker finishes; only one active worker remains
        assert!(matches!(source.pop_work(), PopResult::Done));
        source.mark_finished();
        assert!(!hint.should_split());
    }

    #[test]
    fn donated_morsels_are_popped_before_files() {
        let source = SharedWorkSource::with_workers(vec![test_file()], 2);
        source.donate_morsels(vec![Box::new(DummyMorsel) as Box<dyn Morsel>].into_iter());

        assert!(matches!(
            source.pop_work(),
            PopResult::Ready(WorkItem::Morsel(_), None)
        ));
        assert!(matches!(
            source.pop_work(),
            PopResult::Ready(WorkItem::File(_), Some(_))
        ));
        assert!(matches!(source.pop_work(), PopResult::Done));
    }

    #[test]
    fn empty_queues_with_live_file_lease_are_pending() {
        let source = SharedWorkSource::with_workers(vec![test_file()], 2);
        let PopResult::Ready(WorkItem::File(_), Some(lease)) = source.pop_work() else {
            panic!("expected a file with a lease");
        };

        // The donor is still planning its file and may donate morsels, so an
        // idle sibling must keep polling rather than terminate.
        assert!(matches!(source.pop_work(), PopResult::Pending));

        // The donor splits the file, donates a morsel, and (once the first
        // morsel starts streaming) releases its lease.
        source.donate_morsels(vec![Box::new(DummyMorsel) as Box<dyn Morsel>].into_iter());
        drop(lease);

        assert!(matches!(
            source.pop_work(),
            PopResult::Ready(WorkItem::Morsel(_), None)
        ));
        assert!(matches!(source.pop_work(), PopResult::Done));
    }

    #[test]
    fn donate_surplus_keeps_first_morsel_local() {
        let shared = SharedWorkSource::with_workers(vec![], 2);
        let work_source = WorkSource::Shared(shared.clone());

        let mut ready: VecDeque<Box<dyn Morsel>> = VecDeque::from([
            Box::new(DummyMorsel) as Box<dyn Morsel>,
            Box::new(DummyMorsel) as Box<dyn Morsel>,
            Box::new(DummyMorsel) as Box<dyn Morsel>,
        ]);
        work_source.donate_surplus_morsels(&mut ready);

        assert_eq!(ready.len(), 1);
        assert!(matches!(
            shared.pop_work(),
            PopResult::Ready(WorkItem::Morsel(_), None)
        ));
        assert!(matches!(
            shared.pop_work(),
            PopResult::Ready(WorkItem::Morsel(_), None)
        ));
        assert!(matches!(shared.pop_work(), PopResult::Done));
    }
}
