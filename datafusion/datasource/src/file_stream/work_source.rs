// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    files: Mutex<VecDeque<PartitionedFile>>,
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
        Self::new(files)
    }

    /// Pop the next file from the shared work queue.
    ///
    /// Returns `None` if the queue is empty
    fn pop_front(&self) -> Option<PartitionedFile> {
        self.inner.files.lock().pop_front()
    }
}
