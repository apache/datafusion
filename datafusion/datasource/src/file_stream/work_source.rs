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
#[derive(Debug, Clone)]
pub struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    morsels: Mutex<VecDeque<PartitionedFile>>,
    files: Mutex<VecDeque<PartitionedFile>>,
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
    /// Returns `None` if both queues are empty.
    pub fn pop_front(&self) -> Option<PartitionedFile> {
        if let Some(morsel) = self.inner.morsels.lock().pop_front() {
            return Some(morsel);
        }
        self.inner.files.lock().pop_front()
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
