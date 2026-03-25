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
//! - ready morsels and planners that may be stolen by idle siblings
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
//! # Ready Work Stealing
//!
//! In unordered mode, sibling streams may also exchange CPU-ready work through
//! this shared state:
//!
//! - ready morsels may be published into a shared morsel queue
//! - ready planners may be published into a shared planner queue
//! - idle siblings will try to steal a ready morsel first, then a ready
//!   planner
//!
//! In preserve-order mode, streams keep their ready morsels and planners on
//! their local per-stream queues so later siblings cannot overtake earlier
//! output.
//!
//! Streams stay registered with the shared state for the lifetime of the scan.
//! Each stream can mark itself as "done producing" once it has no more local
//! work in its queues. A stream that is done producing may still continue
//! running work that it steals from siblings.

use crate::morsel::{Morsel, MorselPlanner};
use crossbeam_queue::SegQueue;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};
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
/// # IO Behavior
///
/// Each stream should register itself with [`Self::register_stream`] and then:
///
/// 1. call [`Self::try_acquire_io_permit`] before moving a planner into a
///    waiting-I/O state
/// 2. keep the returned [`OutstandingIoPermit`] alive for as long as that I/O
///    operation is outstanding
/// 3. optionally call [`Self::mark_done_producing`] once the stream has no
///    more local work in its queues
///
/// # Ready Work Behavior
///
/// In unordered mode, streams may also publish ready morsels and planners into
/// the shared queues via [`Self::push_ready_morsel`] and
/// [`Self::push_ready_planner`]. Idle siblings can then steal that ready work
/// with [`Self::pop_ready_morsel`] and [`Self::pop_ready_planner`].
///
/// In preserve-order mode, streams should keep ready morsels and planners on
/// their local per-stream queues rather than publishing them into the shared
/// queues.
///
/// If no permit is available, the caller should typically:
///
/// 1. keep the planner in a CPU-ready state locally
/// 2. register the current task's waker with [`Self::register_waker`]
/// 3. return `Poll::Pending`
///
/// The shared state will wake waiting tasks whenever shared capacity may have
/// become available again, or when new shared ready work is published.
#[derive(Clone)]
pub struct SharedFileStreamState {
    /// Shared outstanding-I/O accounting and fairness state.
    io_state: Arc<RwLock<IoState>>,
    /// Shared ready morsels used for unordered morsel stealing.
    ready_morsels: Arc<SegQueue<Box<dyn Morsel>>>,
    /// Shared ready planners used for unordered morsel stealing.
    ready_planners: Arc<SegQueue<Box<dyn MorselPlanner>>>,
    /// Tasks waiting to be woken when shared capacity or ready work appears.
    waiters: Arc<SegQueue<Waker>>,
}

impl fmt::Debug for SharedFileStreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFileStreamState")
            .field("io_state", &self.io_state)
            .finish_non_exhaustive()
    }
}

/// Shared outstanding-I/O accounting and fairness state for sibling streams.
#[derive(Debug)]
struct IoState {
    /// Shared scheduling policy for sibling streams.
    mode: SharedFileStreamMode,
    /// Total number of planner I/O phases currently in flight.
    outstanding_ios: usize,
    /// Global cap on outstanding planner I/O phases across sibling streams.
    max_outstanding_ios: usize,
    /// Monotonic counter used to assign stable `FileStreamId`s.
    next_stream_id: usize,
    /// Per-stream scheduling state for each registered sibling stream.
    streams: BTreeMap<FileStreamId, StreamIOState>,
}

#[derive(Debug, Default)]
struct StreamIOState {
    outstanding_ios: usize,
    done_producing: bool,
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
            io_state: Arc::new(RwLock::new(IoState {
                mode,
                outstanding_ios: 0,
                max_outstanding_ios,
                next_stream_id: 0,
                streams: BTreeMap::new(),
            })),
            ready_morsels: Arc::new(SegQueue::new()),
            ready_planners: Arc::new(SegQueue::new()),
            waiters: Arc::new(SegQueue::new()),
        }
    }

    /// Register a sibling stream with this shared state and return its stable id.
    pub fn register_stream(&self) -> FileStreamId {
        self.io_state
            .write()
            .expect("shared file stream state poisoned")
            .register_stream()
    }

    /// Returns the configured shared scheduling mode.
    pub fn mode(&self) -> SharedFileStreamMode {
        self.io_state
            .read()
            .expect("shared file stream state poisoned")
            .mode
    }

    /// Returns the number of currently registered sibling streams.
    pub fn registered_stream_count(&self) -> usize {
        self.io_state
            .read()
            .expect("shared file stream state poisoned")
            .streams
            .len()
    }

    /// Returns the maximum number of outstanding planner I/O operations
    /// allowed across all sibling streams.
    pub fn max_outstanding_ios(&self) -> usize {
        self.io_state
            .read()
            .expect("shared file stream state poisoned")
            .max_outstanding_ios
    }

    /// Returns the number of currently outstanding planner I/O operations
    /// across all sibling streams.
    pub fn outstanding_ios(&self) -> usize {
        self.io_state
            .read()
            .expect("shared file stream state poisoned")
            .outstanding_ios
    }

    /// Mark `stream_id` as done producing local work.
    ///
    /// This is a one-way transition. A stream that is done producing may still
    /// keep running stolen work and may still hold or release outstanding I/O
    /// permits that were already issued.
    pub fn mark_done_producing(&self, stream_id: FileStreamId) {
        let waiters = {
            let mut io_state = self
                .io_state
                .write()
                .expect("shared file stream state poisoned");
            let Some(stream) = io_state.streams.get_mut(&stream_id) else {
                return;
            };
            if stream.done_producing {
                return;
            }
            stream.done_producing = true;
            Self::take_waiters(&mut io_state, &self.waiters)
        };

        Self::wake_waiters(waiters);
    }

    /// Returns true if all registered sibling streams have marked themselves as
    /// done producing local work.
    pub fn all_streams_done_producing(&self) -> bool {
        self.io_state
            .read()
            .expect("shared file stream state poisoned")
            .streams
            .values()
            .all(|stream| stream.done_producing)
    }

    /// Register a task waker to be notified when shared capacity may have
    /// become available.
    ///
    /// Callers should typically register a waker after failing to acquire an
    /// I/O permit and before returning `Poll::Pending`.
    pub fn register_waker(&self, stream_id: FileStreamId, waker: &Waker) {
        let mut io_state = self
            .io_state
            .write()
            .expect("shared file stream state poisoned");

        self.waiters.push(waker.clone());
        if let Some(stream) = io_state.streams.get_mut(&stream_id) {
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
        let mut io_state = self
            .io_state
            .write()
            .expect("shared file stream state poisoned");

        if !io_state.can_issue_io(stream_id) {
            return None;
        }

        io_state.acquire_io(stream_id);
        drop(io_state);

        Some(OutstandingIoPermit {
            state: Some(self.clone()),
            stream_id,
        })
    }

    /// Publish one ready morsel into the shared queue.
    pub fn push_ready_morsel(&self, morsel: Box<dyn Morsel>) {
        let waiters = {
            self.ready_morsels.push(morsel);
            let mut io_state = self
                .io_state
                .write()
                .expect("shared file stream state poisoned");
            Self::take_waiters(&mut io_state, &self.waiters)
        };
        Self::wake_waiters(waiters);
    }

    /// Publish one ready planner into the shared queue.
    pub fn push_ready_planner(&self, planner: Box<dyn MorselPlanner>) {
        let waiters = {
            self.ready_planners.push(planner);
            let mut io_state = self
                .io_state
                .write()
                .expect("shared file stream state poisoned");
            Self::take_waiters(&mut io_state, &self.waiters)
        };
        Self::wake_waiters(waiters);
    }

    /// Try to steal one ready morsel from the shared queue.
    pub fn pop_ready_morsel(&self) -> Option<Box<dyn Morsel>> {
        self.ready_morsels.pop()
    }

    /// Try to steal one ready planner from the shared queue.
    pub fn pop_ready_planner(&self) -> Option<Box<dyn MorselPlanner>> {
        self.ready_planners.pop()
    }

    fn release_io_permit(&self, stream_id: FileStreamId) {
        let waiters = {
            let mut io_state = self
                .io_state
                .write()
                .expect("shared file stream state poisoned");
            io_state.release_io(stream_id);
            Self::take_waiters(&mut io_state, &self.waiters)
        };

        Self::wake_waiters(waiters);
    }

    fn take_waiters(
        io_state: &mut IoState,
        shared_waiters: &SegQueue<Waker>,
    ) -> Vec<Waker> {
        let mut waiters = Vec::new();
        while let Some(waiter) = shared_waiters.pop() {
            waiters.push(waiter);
        }
        for stream in io_state.streams.values_mut() {
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

impl IoState {
    fn register_stream(&mut self) -> FileStreamId {
        let id = FileStreamId(self.next_stream_id);
        self.next_stream_id += 1;
        self.streams.insert(id, StreamIOState::default());
        id
    }

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

    fn acquire_io(&mut self, stream_id: FileStreamId) {
        self.outstanding_ios += 1;
        self.streams
            .get_mut(&stream_id)
            .expect("unregistered file stream")
            .outstanding_ios += 1;
    }

    fn release_io(&mut self, stream_id: FileStreamId) {
        self.outstanding_ios = self
            .outstanding_ios
            .checked_sub(1)
            .expect("outstanding I/O count underflow");
        self.streams
            .get_mut(&stream_id)
            .expect("unregistered file stream")
            .outstanding_ios = self
            .streams
            .get(&stream_id)
            .expect("unregistered file stream")
            .outstanding_ios
            .checked_sub(1)
            .expect("per-stream outstanding I/O count underflow");
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
        // second if every other sibling that may still produce more work has
        // also reached at least one outstanding request. Streams that are done
        // producing and have no outstanding I/O no longer participate in the
        // fairness gate.
        self.streams.values().all(|state| {
            state.outstanding_ios > 0
                || (state.done_producing && state.outstanding_ios == 0)
        })
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
        let active_streams = self
            .streams
            .iter()
            .filter_map(|(id, state)| {
                if *id != stream_id && state.done_producing && state.outstanding_ios == 0
                {
                    None
                } else {
                    Some(*id)
                }
            })
            .collect::<Vec<_>>();

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
    /// Marking streams as done producing is a one-way state transition and the
    /// shared state can report once every registered stream has transitioned.
    fn mark_done_producing_is_one_way() {
        let state = SharedFileStreamState::new(2, SharedFileStreamMode::Unordered);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        assert!(!state.all_streams_done_producing());

        state.mark_done_producing(stream1);
        assert!(!state.all_streams_done_producing());

        // Re-marking a stream is a no-op.
        state.mark_done_producing(stream1);
        assert!(!state.all_streams_done_producing());

        state.mark_done_producing(stream2);
        assert!(state.all_streams_done_producing());
    }

    #[test]
    /// A stream that is done producing and has no outstanding I/O must not
    /// block a sibling from acquiring an extra unordered permit.
    fn done_producing_stream_does_not_block_unordered_extra_permit() {
        let state = SharedFileStreamState::new(2, SharedFileStreamMode::Unordered);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        state.mark_done_producing(stream2);

        let permit1 = state.try_acquire_io_permit(stream1).unwrap();
        let permit2 = state.try_acquire_io_permit(stream1).unwrap();
        assert!(state.try_acquire_io_permit(stream1).is_none());

        drop(permit2);
        drop(permit1);
    }

    #[test]
    /// In preserve-order mode, a stream that is done producing and has no
    /// outstanding I/O should not reduce the fair share available to active
    /// siblings.
    fn done_producing_stream_does_not_reduce_preserve_order_share() {
        let state = SharedFileStreamState::new(2, SharedFileStreamMode::PreserveOrder);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        state.mark_done_producing(stream2);

        let permit1 = state.try_acquire_io_permit(stream1).unwrap();
        let permit2 = state.try_acquire_io_permit(stream1).unwrap();
        assert!(state.try_acquire_io_permit(stream1).is_none());

        drop(permit2);
        drop(permit1);
    }

    #[test]
    /// A done-producing stream may still issue I/O for stolen work, so it must
    /// remain eligible for a preserve-order share calculation when it is the
    /// requesting stream.
    fn done_producing_requester_can_still_acquire_preserve_order_permit() {
        let state = SharedFileStreamState::new(2, SharedFileStreamMode::PreserveOrder);
        let stream1 = state.register_stream();
        let stream2 = state.register_stream();

        state.mark_done_producing(stream1);
        state.mark_done_producing(stream2);

        let permit = state.try_acquire_io_permit(stream1).unwrap();
        drop(permit);
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
