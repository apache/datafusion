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

//! Runtime tracing for [`crate::file_stream::FileStream`].
//!
//! This module provides a lightweight, opt-in stderr trace for observing
//! `FileStream` behavior during a real query. The trace is intended for
//! performance debugging and scheduler analysis rather than correctness tests.
//!
//! # Enabling
//!
//! Set:
//!
//! ```text
//! DATAFUSION_FILE_STREAM_TRACE=true
//! ```
//!
//! before running the CLI or any DataFusion-based application.
//!
//! Example:
//!
//! ```text
//! DATAFUSION_FILE_STREAM_TRACE=true ../datafusion-cli-morsels -f q23.sql
//! ```
//!
//! # Output
//!
//! The trace writes one structured line per event to stderr. Events include:
//!
//! - file admission
//! - planner CPU steps
//! - planner I/O scheduling and completion
//! - ready work publication and stealing
//! - morsel start
//! - batch emission
//! - waiting / idle points
//!
//! Timestamps are relative to the first traced `FileStream` event in the
//! process so traces from sibling partitions can be compared directly.

use crate::PartitionedFile;
use crate::file_stream::FileStreamId;
use datafusion_common::instant::Instant;
use std::sync::OnceLock;

static FILE_STREAM_TRACE_START: OnceLock<Instant> = OnceLock::new();

/// Return true if file stream tracing is enabled for this process.
pub(super) fn file_stream_trace_enabled() -> bool {
    std::env::var("DATAFUSION_FILE_STREAM_TRACE")
        .ok()
        .map(|value| {
            let value = value.trim();
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

/// Lightweight stderr trace for observing `FileStream` runtime behavior.
///
/// The trace is intentionally simple: when disabled it returns immediately,
/// and when enabled it emits timestamped scheduler events to stderr so runtime
/// behavior can be inspected after a real query finishes.
#[derive(Debug, Clone)]
pub(super) struct ReadTrace {
    enabled: bool,
    partition: usize,
    stream_id: String,
}

impl ReadTrace {
    /// Create a new trace for one `FileStream`.
    pub(super) fn new(enabled: bool, partition: usize, stream_id: FileStreamId) -> Self {
        Self {
            enabled,
            partition,
            stream_id: format!("{stream_id:?}"),
        }
    }

    /// Emit one structured scheduler event to stderr.
    fn emit(&self, event: &str, details: impl AsRef<str>) {
        if !self.enabled {
            return;
        }

        let start = FILE_STREAM_TRACE_START.get_or_init(Instant::now);
        let elapsed = start.elapsed();
        let details = details.as_ref();
        if details.is_empty() {
            eprintln!(
                "+{:>8.3}s partition={} stream={} event={}",
                elapsed.as_secs_f64(),
                self.partition,
                self.stream_id,
                event
            );
        } else {
            eprintln!(
                "+{:>8.3}s partition={} stream={} event={} {}",
                elapsed.as_secs_f64(),
                self.partition,
                self.stream_id,
                event,
                details
            );
        }
    }

    /// Emit an event for one admitted file.
    pub(super) fn file_opened(&self, file: &PartitionedFile) {
        self.emit("file_opened", format!("file={}", file.object_meta.location));
    }

    /// Emit an event for ready work produced by planning.
    pub(super) fn plan_result(&self, morsels: usize, planners: usize, has_io: bool) {
        self.emit(
            "planner_step",
            format!("morsels={morsels} planners={planners} io={has_io}"),
        );
    }

    /// Emit an event when a new planner I/O future is scheduled.
    pub(super) fn io_scheduled(&self, waiting_planners: usize) {
        self.emit(
            "io_scheduled",
            format!("waiting_planners={waiting_planners}"),
        );
    }

    /// Emit an event when a planner I/O future completes.
    pub(super) fn io_completed(&self, ready_planners: usize) {
        self.emit("io_completed", format!("ready_planners={ready_planners}"));
    }

    /// Emit an event when ready morsels are shared or queued locally.
    pub(super) fn morsels_ready(&self, morsels: usize, shared: bool) {
        self.emit("morsels_ready", format!("count={morsels} shared={shared}"));
    }

    /// Emit an event when ready planners are shared or queued locally.
    pub(super) fn planners_ready(&self, planners: usize, shared: bool) {
        self.emit(
            "planners_ready",
            format!("count={planners} shared={shared}"),
        );
    }

    /// Emit an event when shared work is stolen by this stream.
    pub(super) fn stole_work(&self, kind: &str) {
        self.emit("stole_work", format!("kind={kind}"));
    }

    /// Emit an event when a morsel becomes the active reader.
    pub(super) fn morsel_started(&self, buffered_morsels: usize) {
        self.emit(
            "morsel_started",
            format!("buffered_morsels={buffered_morsels}"),
        );
    }

    /// Emit an event when a batch is produced to the consumer.
    pub(super) fn batch_emitted(&self, rows: usize) {
        self.emit("batch_emitted", format!("rows={rows}"));
    }

    /// Emit an event when the stream is blocked waiting for more work.
    pub(super) fn waiting(&self, reason: &str) {
        self.emit("waiting", reason);
    }
}
