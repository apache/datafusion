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

//! Shared state for sibling [`crate::file_stream::FileStream`] instances.
//!
//! A single `DataSourceExec` may create multiple sibling `FileStream`s, one per
//! output partition. These streams need a place to coordinate shared scan
//! resources such as:
//!
//! - the total number of outstanding planner I/O operations
//! - future work-stealing state
//!
//! [`SharedFileStreamState`] is that shared home.
//!
//! # Outstanding I/O Scheduling Modes
//!
//! The shared state currently controls planner I/O in one of two modes:
//!
//! - [`SharedFileStreamMode::Unordered`]: Every registered stream has a chance
//!   to hold one outstanding request before another stream is allowed to start
//!   a second. After that first request per stream, remaining capacity may be
//!   used in any order.
//!
//! - [`SharedFileStreamMode::PreserveOrder`]: Outstanding I/Os are split fairly
//!   across all active streams, even if some streams do not currently want to
//!   issue I/O. This prevents a subset of streams from consuming the full budget
//!   and is intended for scans that require stable cross-stream ordering.
//!
//! Streams can call [`SharedFileStreamState::unregister_stream`] once they know
//! they will never need another I/O permit. Unregistered streams are removed
//! from future fairness calculations so their share of the budget can be
//! redistributed.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::Waker;

/// Shared scheduling mode for sibling `FileStream`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedFileStreamMode {
    /// Allow streams to run in any order after each active, I/O-hungry stream
    /// has had a chance to start one outstanding I/O.
    Unordered,
    /// Split outstanding I/O budget fairly across all active streams.
    PreserveOrder,
}

/// Stable identifier for one sibling `FileStream` registered with a shared
/// state object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileStreamId(usize);

/// Shared state for all sibling `FileStream`s that belong to one `DataSourceExec`.
///
/// # Intended Usage
///
/// Create one `SharedFileStreamState` for the whole `DataSourceExec` and clone
/// it into each sibling `FileStream`.
///
/// Each stream should register itself with [`Self::register_stream`] and then:
///
/// 1. call [`Self::try_acquire_io_permit`] before moving a planner into a
///    waiting-I/O state
/// 2. keep the returned [`OutstandingIoPermit`] alive for as long as that I/O
///    operation is outstanding
/// 3. call [`Self::unregister_stream`] once the stream knows it will never need
///    another I/O permit
///
/// If no permit is available, the caller should typically:
///
/// 1. keep the planner in a CPU-ready state locally
/// 2. register the current task's waker with [`Self::register_waker`]
/// 3. return `Poll::Pending`
///
/// The shared state will wake waiting tasks whenever shared capacity may have
/// become available again.
#[derive(Clone, Debug)]
pub struct SharedFileStreamState {
    inner: Arc<Mutex<SharedFileStreamStateInner>>,
}

#[derive(Debug)]
struct SharedFileStreamStateInner {
    mode: SharedFileStreamMode,
    outstanding_ios: usize,
    max_outstanding_ios: usize,
    next_stream_id: usize,
    streams: BTreeMap<FileStreamId, StreamIOState>,
    waiters: VecDeque<Waker>,
}

#[derive(Debug, Default)]
struct StreamIOState {
    outstanding_ios: usize,
    waker: Option<Waker>,
}

impl SharedFileStreamState {
    /// Create a new shared state object with the provided global I/O limit and
    /// scheduling mode.
    ///
    /// The limit applies across all sibling `FileStream`s that share this
    /// state, not per individual stream.
    pub fn new(max_outstanding_ios: usize, mode: SharedFileStreamMode) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedFileStreamStateInner {
                mode,
                outstanding_ios: 0,
                max_outstanding_ios,
                next_stream_id: 0,
                streams: BTreeMap::new(),
                waiters: VecDeque::new(),
            })),
        }
    }

    /// Register a sibling stream with this shared state and return its stable id.
    pub fn register_stream(&self) -> FileStreamId {
        let mut inner = self
            .inner
            .lock()
            .expect("shared file stream state poisoned");

        let id = FileStreamId(inner.next_stream_id);
        inner.next_stream_id += 1;
        inner.streams.insert(id, StreamIOState::default());
        id
    }

    /// Returns the configured shared scheduling mode.
    pub fn mode(&self) -> SharedFileStreamMode {
        self.inner
            .lock()
            .expect("shared file stream state poisoned")
            .mode
    }

    /// Returns the maximum number of outstanding planner I/O operations
    /// allowed across all sibling streams.
    pub fn max_outstanding_ios(&self) -> usize {
        self.inner
            .lock()
            .expect("shared file stream state poisoned")
            .max_outstanding_ios
    }

    /// Returns the number of currently outstanding planner I/O operations
    /// across all sibling streams.
    pub fn outstanding_ios(&self) -> usize {
        self.inner
            .lock()
            .expect("shared file stream state poisoned")
            .outstanding_ios
    }

    /// Unregister a stream that will never request another I/O permit.
    ///
    /// This removes the stream from future fairness calculations, allowing its
    /// share of the I/O budget to be redistributed to sibling streams.
    ///
    /// The stream must not have any outstanding permits when it unregisters.
    pub fn unregister_stream(&self, stream_id: FileStreamId) {
        let waiters = {
            let mut inner = self
                .inner
                .lock()
                .expect("shared file stream state poisoned");
            if let Some(stream) = inner.streams.remove(&stream_id) {
                assert_eq!(
                    stream.outstanding_ios, 0,
                    "stream must not unregister while it still holds I/O permits"
                );
            }
            Self::take_waiters_locked(&mut inner)
        };

        Self::wake_waiters(waiters);
    }

    /// Register a task waker to be notified when shared capacity may have
    /// become available.
    ///
    /// Callers should typically register a waker after failing to acquire an
    /// I/O permit and before returning `Poll::Pending`.
    pub fn register_waker(&self, stream_id: FileStreamId, waker: &Waker) {
        let mut inner = self
            .inner
            .lock()
            .expect("shared file stream state poisoned");

        inner.waiters.push_back(waker.clone());
        if let Some(stream) = inner.streams.get_mut(&stream_id) {
            stream.waker = Some(waker.clone());
        }
    }

    /// Try to reserve one shared outstanding-I/O slot for `stream_id`.
    ///
    /// Returns `Some(permit)` if the stream is currently eligible to issue a
    /// new I/O under the configured fairness mode, or `None` otherwise.
    pub fn try_acquire_io_permit(
        &self,
        stream_id: FileStreamId,
    ) -> Option<OutstandingIoPermit> {
        let mut inner = self
            .inner
            .lock()
            .expect("shared file stream state poisoned");

        if !inner.can_issue_io(stream_id) {
            return None;
        }

        inner.outstanding_ios += 1;
        inner
            .streams
            .get_mut(&stream_id)
            .expect("unregistered file stream")
            .outstanding_ios += 1;
        drop(inner);

        Some(OutstandingIoPermit {
            state: Some(self.clone()),
            stream_id,
        })
    }

    fn release_io_permit(&self, stream_id: FileStreamId) {
        let waiters = {
            let mut inner = self
                .inner
                .lock()
                .expect("shared file stream state poisoned");
            inner.outstanding_ios = inner
                .outstanding_ios
                .checked_sub(1)
                .expect("outstanding I/O count underflow");
            inner
                .streams
                .get_mut(&stream_id)
                .expect("unregistered file stream")
                .outstanding_ios = inner
                .streams
                .get(&stream_id)
                .expect("unregistered file stream")
                .outstanding_ios
                .checked_sub(1)
                .expect("per-stream outstanding I/O count underflow");
            Self::take_waiters_locked(&mut inner)
        };

        Self::wake_waiters(waiters);
    }

    fn take_waiters_locked(inner: &mut SharedFileStreamStateInner) -> Vec<Waker> {
        let mut waiters = inner.waiters.drain(..).collect::<Vec<_>>();
        for stream in inner.streams.values_mut() {
            if let Some(waker) = stream.waker.take() {
                waiters.push(waker);
            }
        }
        waiters
    }

    fn wake_waiters(waiters: Vec<Waker>) {
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl SharedFileStreamStateInner {
    fn can_issue_io(&self, stream_id: FileStreamId) -> bool {
        if self.outstanding_ios >= self.max_outstanding_ios {
            return false;
        }

        if !self.streams.contains_key(&stream_id) {
            return false;
        }

        match self.mode {
            SharedFileStreamMode::Unordered => self.can_issue_unordered(stream_id),
            SharedFileStreamMode::PreserveOrder => {
                self.can_issue_preserve_order(stream_id)
            }
        }
    }

    fn can_issue_unordered(&self, stream_id: FileStreamId) -> bool {
        let stream = self
            .streams
            .get(&stream_id)
            .expect("unregistered file stream");

        if stream.outstanding_ios == 0 {
            return true;
        }

        // Once a stream already has one outstanding I/O, it may only start a
        // second if every other registered sibling stream has also reached at
        // least one outstanding request.
        self.streams.values().all(|state| state.outstanding_ios > 0)
    }

    fn can_issue_preserve_order(&self, stream_id: FileStreamId) -> bool {
        let share = self.fair_share_for(stream_id);
        let stream = self
            .streams
            .get(&stream_id)
            .expect("unregistered file stream");
        stream.outstanding_ios < share
    }

    fn fair_share_for(&self, stream_id: FileStreamId) -> usize {
        let active_streams = self.streams.iter().map(|(id, _)| *id).collect::<Vec<_>>();

        if active_streams.is_empty() {
            return 0;
        }

        let active_count = active_streams.len();
        let base_share = self.max_outstanding_ios / active_count;
        let remainder = self.max_outstanding_ios % active_count;

        let position = active_streams
            .iter()
            .position(|id| *id == stream_id)
            .expect("stream should be active");

        base_share + usize::from(position < remainder)
    }
}

/// RAII guard representing one shared outstanding-I/O slot.
///
/// Hold this permit for exactly as long as the corresponding planner I/O
/// future remains outstanding. Dropping the permit releases the slot back to
/// the shared state.
#[derive(Debug)]
pub struct OutstandingIoPermit {
    state: Option<SharedFileStreamState>,
    stream_id: FileStreamId,
}

impl Drop for OutstandingIoPermit {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.release_io_permit(self.stream_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    #[test]
    /// In unordered mode, each stream that currently wants I/O must get a
    /// chance to issue its first request before another stream is allowed to
    /// issue a second. Once every interested stream has one outstanding I/O,
    /// any remaining permits may be consumed in any order.
    fn unordered_every_stream_gets_one_before_extras() {
        let state = SharedFileStreamState::new(3, SharedFileStreamMode::Unordered);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        let permit1 = state.try_acquire_io_permit(stream1).unwrap();
        assert!(state.try_acquire_io_permit(stream1).is_none());

        let permit2 = state.try_acquire_io_permit(stream2).unwrap();
        let permit3 = state.try_acquire_io_permit(stream1).unwrap();
        assert_eq!(state.outstanding_ios(), 3);
        assert!(state.try_acquire_io_permit(stream2).is_none());

        drop(permit2);
        drop(permit3);
        drop(permit1);
    }

    #[test]
    /// In unordered mode, unregistering a stream removes it from the
    /// "everyone gets one first" rule, so its reserved share may be
    /// immediately reused by remaining active streams.
    fn unordered_closed_stream_releases_capacity_to_others() {
        let state = SharedFileStreamState::new(2, SharedFileStreamMode::Unordered);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        let _permit1 = state.try_acquire_io_permit(stream1).unwrap();
        state.unregister_stream(stream2);

        assert!(state.try_acquire_io_permit(stream1).is_some());
    }

    #[test]
    /// In preserve-order mode, the outstanding-I/O budget is split fairly
    /// across all active streams. With three streams and three permits total,
    /// each stream is capped at one concurrent I/O.
    fn preserve_order_splits_evenly() {
        let state = SharedFileStreamState::new(3, SharedFileStreamMode::PreserveOrder);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();
        let stream3 = state.register_stream();

        let _permit1 = state.try_acquire_io_permit(stream1).unwrap();
        let _permit2 = state.try_acquire_io_permit(stream2).unwrap();
        let _permit3 = state.try_acquire_io_permit(stream3).unwrap();

        assert!(state.try_acquire_io_permit(stream1).is_none());
        assert!(state.try_acquire_io_permit(stream2).is_none());
        assert!(state.try_acquire_io_permit(stream3).is_none());
    }

    #[test]
    /// In preserve-order mode, once a stream has released its final permit and
    /// unregisters itself, its fair share is redistributed to the remaining
    /// active streams.
    fn preserve_order_redistributes_closed_stream_share() {
        let state = SharedFileStreamState::new(3, SharedFileStreamMode::PreserveOrder);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();
        let stream3 = state.register_stream();

        let _permit1 = state.try_acquire_io_permit(stream1).unwrap();
        let _permit2 = state.try_acquire_io_permit(stream2).unwrap();
        let permit3 = state.try_acquire_io_permit(stream3).unwrap();

        drop(permit3);
        state.unregister_stream(stream3);

        // Shares rebalance from [1,1,1] to [2,1] in registration order.
        assert!(state.try_acquire_io_permit(stream1).is_some());
        assert!(state.try_acquire_io_permit(stream2).is_none());
    }

    #[test]
    /// Releasing an outstanding permit should wake blocked sibling streams so
    /// they can retry permit acquisition on a future poll.
    fn releasing_permit_wakes_waiters() {
        let state = SharedFileStreamState::new(1, SharedFileStreamMode::Unordered);
        let stream = state.register_stream();
        let permit = state.try_acquire_io_permit(stream).unwrap();

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        state.register_waker(stream, &waker);

        drop(permit);

        assert_eq!(wake_counter.wake_count.load(Ordering::SeqCst), 2);
        assert_eq!(state.outstanding_ios(), 0);
    }

    #[derive(Default)]
    struct WakeCounter {
        wake_count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}
