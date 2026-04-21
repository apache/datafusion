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

/// Shared source of work for sibling `FileStream`s
///
/// The queue is created once per execution and shared by all reorderable
/// sibling streams for that execution. Whichever stream becomes idle first may
/// take the next unopened file from the front of the queue.
///
/// It uses a [`Mutex`] internally to provide thread-safe access
/// to the shared file queue.
#[derive(Debug, Clone)]
pub struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
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
        let files = files.into_iter().collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                files: Mutex::new(files),
            }),
        }
    }

    /// Create a shared work source for the unopened files in `config`.
    pub(crate) fn from_config(config: &FileScanConfig) -> Self {
        Self::new(config.file_groups.iter().flat_map(FileGroup::iter).cloned())
    }

    /// Pop the next file from the shared work queue.
    ///
    /// Returns `None` if the queue is empty.
    pub fn pop_front(&self) -> Option<PartitionedFile> {
        self.inner.files.lock().pop_front()
    }

    /// Push files to the front of the shared work queue.
    ///
    /// Used when an in-flight file is sub-divided (e.g. into row-group-sized
    /// chunks): the donor keeps one chunk and pushes the rest to the front so
    /// any idle sibling picks them up before starting work on an unrelated
    /// whole file. Items preserve their order: the first element of `items`
    /// ends up at the very front of the queue.
    pub fn push_front(&self, items: impl IntoIterator<Item = PartitionedFile>) {
        let items: Vec<PartitionedFile> = items.into_iter().collect();
        let mut queue = self.inner.files.lock();
        for file in items.into_iter().rev() {
            queue.push_front(file);
        }
    }
}
