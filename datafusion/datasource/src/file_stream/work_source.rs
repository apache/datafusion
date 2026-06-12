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
    pub(super) fn pop_work(&mut self) -> Option<WorkItem> {
        match self {
            Self::Local(files) => {
                files.pop_front().map(|file| WorkItem::File(Box::new(file)))
            }
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
    /// work finishes first.
    fn pop_work(&self) -> Option<WorkItem> {
        let mut queues = self.inner.queues.lock();
        if let Some(morsel) = queues.morsels.pop_front() {
            return Some(WorkItem::Morsel(morsel));
        }
        queues
            .files
            .pop_front()
            .map(|file| WorkItem::File(Box::new(file)))
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

        assert!(source.pop_work().is_some());
        assert!(source.pop_work().is_some());
        // 1 file remaining < 2 workers: split
        assert!(hint.should_split());

        assert!(source.pop_work().is_some());
        // queue empty, both workers still active: split
        assert!(hint.should_split());

        // one worker finishes; only one active worker remains
        assert!(source.pop_work().is_none());
        source.mark_finished();
        assert!(!hint.should_split());
    }

    #[test]
    fn donated_morsels_are_popped_before_files() {
        let source = SharedWorkSource::with_workers(vec![test_file()], 2);
        source.donate_morsels(vec![Box::new(DummyMorsel) as Box<dyn Morsel>].into_iter());

        assert!(matches!(source.pop_work(), Some(WorkItem::Morsel(_))));
        assert!(matches!(source.pop_work(), Some(WorkItem::File(_))));
        assert!(source.pop_work().is_none());
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
        assert!(matches!(shared.pop_work(), Some(WorkItem::Morsel(_))));
        assert!(matches!(shared.pop_work(), Some(WorkItem::Morsel(_))));
        assert!(shared.pop_work().is_none());
    }
}
