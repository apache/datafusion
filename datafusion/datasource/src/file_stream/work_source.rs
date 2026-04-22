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
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::PartitionedFile;
use crate::file_groups::FileGroup;
use crate::file_scan_config::FileScanConfig;
use parking_lot::Mutex;

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
    /// Try to pop the next item of work.
    ///
    /// Returns [`PopResult::Pending`] for [`WorkSource::Shared`] when both
    /// queues are empty but a sibling is still processing a file that may
    /// donate more work. The caller must yield with its waker re-scheduled.
    pub(super) fn pop_front(&mut self) -> PopResult {
        match self {
            Self::Local(files) => match files.pop_front() {
                Some(file) => PopResult::Ready(file, None),
                None => PopResult::Done,
            },
            Self::Shared(shared) => shared.pop_front_tracked(),
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
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Ready carries a PartitionedFile on the common path; boxing would add a heap alloc per pop and the other variants are markers"
)]
pub(super) enum PopResult {
    /// Work popped. The optional [`FileLease`] must be held until the file
    /// is fully processed; while it is alive, idle siblings treat the
    /// shared source as "donor may still publish" instead of drained.
    Ready(PartitionedFile, Option<FileLease>),
    /// No work currently available, but a sibling is still pre-scan on a
    /// file that may donate. The caller must register its waker to be
    /// re-polled and yield [`Poll::Pending`].
    Pending,
    /// No work available and no donors in flight — fully drained.
    Done,
}

/// Shared source of work for sibling `FileStream`s.
///
/// Created once per execution and shared by all reorderable sibling streams.
/// Holds two queues:
///
/// - **morsels**: pre-prepared sub-file work items (e.g. parquet row-group
///   chunks donated mid-open by a sibling). Always popped first.
/// - **files**: whole unopened files — the initial scan units.
///
/// A FileStream that picks up a morsel has finalized state attached to it
/// (via `PartitionedFile::extensions`) and can skip most of the per-file
/// state machine. Draining morsels first keeps their latency low and
/// prevents siblings from starting fresh whole files while half-processed
/// sub-file work sits idle.
///
/// Also tracks an in-flight donor count: every file popped from `files` is
/// backed by a [`FileLease`] whose `Drop` decrements the count. While the
/// count is non-zero, an idle sibling that sees both queues empty must wait
/// rather than declare the source drained — the donor is still pre-scan
/// and may yet push morsels.
#[derive(Debug, Clone)]
pub struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    morsels: Mutex<VecDeque<PartitionedFile>>,
    files: Mutex<VecDeque<PartitionedFile>>,
    /// Number of files popped from `files` whose [`FileLease`] has not yet
    /// been dropped. Non-zero means "donor may still publish morsels."
    in_flight: AtomicUsize,
}

impl Default for SharedWorkSource {
    fn default() -> Self {
        Self::new(std::iter::empty())
    }
}

impl SharedWorkSource {
    /// Create a shared work source containing the provided unopened files.
    pub(crate) fn new(files: impl IntoIterator<Item = PartitionedFile>) -> Self {
        let files: VecDeque<PartitionedFile> = files.into_iter().collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                morsels: Mutex::new(VecDeque::new()),
                files: Mutex::new(files),
                in_flight: AtomicUsize::new(0),
            }),
        }
    }

    /// Create a shared work source for the unopened files in `config`.
    pub(crate) fn from_config(config: &FileScanConfig) -> Self {
        Self::new(config.file_groups.iter().flat_map(FileGroup::iter).cloned())
    }

    /// Pop the next item of work — morsels (pre-prepared sub-file chunks)
    /// first, then whole files.
    ///
    /// Returns `None` if both queues are empty. Does *not* track in-flight
    /// donors; intended for tests and callers that only observe morsel
    /// donations. `ScanState` uses [`Self::pop_front_tracked`].
    pub fn pop_front(&self) -> Option<PartitionedFile> {
        if let Some(morsel) = self.inner.morsels.lock().pop_front() {
            return Some(morsel);
        }
        self.inner.files.lock().pop_front()
    }

    /// Pop the next item of work for a sibling `FileStream`, returning a
    /// [`PopResult`] that distinguishes "nothing right now but donors may
    /// publish" from "truly drained."
    pub(super) fn pop_front_tracked(&self) -> PopResult {
        if let Some(morsel) = self.inner.morsels.lock().pop_front() {
            return PopResult::Ready(morsel, None);
        }
        // Increment before releasing the `files` lock so a concurrent peer
        // cannot observe empty-files && zero-counter while this donor is
        // about to register itself.
        let mut files = self.inner.files.lock();
        if let Some(file) = files.pop_front() {
            self.inner.in_flight.fetch_add(1, Ordering::Release);
            drop(files);
            return PopResult::Ready(
                file,
                Some(FileLease {
                    inner: Arc::clone(&self.inner),
                }),
            );
        }
        drop(files);
        // Both queues empty. If any donor is still in flight, wait — it may
        // yet donate morsels.
        if self.inner.in_flight.load(Ordering::Acquire) > 0 {
            return PopResult::Pending;
        }
        // Counter observed zero. A donor that donated before dropping its
        // lease must have pushed morsels before the decrement; re-peek the
        // morsel queue to pick them up rather than exit prematurely.
        if let Some(morsel) = self.inner.morsels.lock().pop_front() {
            return PopResult::Ready(morsel, None);
        }
        PopResult::Done
    }

    /// Push pre-prepared morsels onto the morsel queue.
    ///
    /// Used when an in-flight file is sub-divided (e.g. parquet row-group
    /// splitting): each donated chunk carries its finalized state via
    /// `PartitionedFile::extensions` so the stealer can skip most of the
    /// per-file state machine. Items preserve their order.
    pub fn push_morsels(&self, items: impl IntoIterator<Item = PartitionedFile>) {
        let mut queue = self.inner.morsels.lock();
        queue.extend(items);
    }
}

/// RAII guard tracking a file popped from a [`SharedWorkSource`]'s `files`
/// queue. While alive, the counter on the source stays non-zero, which
/// keeps idle sibling streams waiting for potential donations instead of
/// declaring the source drained. Dropped when the donor finishes (or
/// gives up on) the file.
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
        self.inner.in_flight.fetch_sub(1, Ordering::Release);
    }
}
