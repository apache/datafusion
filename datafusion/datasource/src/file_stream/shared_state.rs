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

//! Shared state for cooperating [`crate::file_stream::FileStream`] instances.
//!
//! A single `DataSourceExec` may create multiple sibling `FileStream`s, one per
//! output partition. These streams use [`SharedFileStreamState`]  to coordinate
//! shared resources such as:
//!
//! - the total number of outstanding planner I/O operations
//! - shared ready work for work-stealing

use std::sync::{Arc, Mutex};
use std::task::Waker;

/// Shared state for all sibling `FileStream`s that belong to one `DataSourceExec`.
///
/// # Intended Usage
///
/// Create one `SharedFileStreamState` for the whole `DataSourceExec` and clone
/// it into each sibling `FileStream`.
///
/// Streams that want to start a new planner I/O operation should first call
/// [`SharedFileStreamState::try_acquire_io_permit`]. If a permit is returned,
/// keep the permit alive for as long as the I/O operation remains outstanding.
/// When the permit is dropped, the shared I/O slot is released automatically.
///
/// If no permit is available, the caller should typically:
///
/// 1. keep the planner in a CPU-ready state locally
/// 2. register the current task's waker with
///    [`SharedFileStreamState::register_waker`]
/// 3. return `Poll::Pending`
///
/// The shared state will wake waiting tasks whenever an I/O permit is released.
#[derive(Clone, Debug)]
pub struct SharedFileStreamState {
    inner: Arc<Mutex<SharedFileStreamStateInner>>,
}

#[derive(Debug)]
struct SharedFileStreamStateInner {
    outstanding_ios: usize,
    max_outstanding_ios: usize,
    waiters: Vec<Waker>,
}

impl SharedFileStreamState {
    /// Create a new shared state object with the provided global I/O limit.
    ///
    /// The limit applies across all sibling `FileStream`s that share this
    /// state, not per individual stream.
    pub fn new(max_outstanding_ios: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedFileStreamStateInner {
                outstanding_ios: 0,
                max_outstanding_ios,
                waiters: Vec::new(),
            })),
        }
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

    /// Try to reserve one shared outstanding-I/O slot.
    ///
    /// If successful, returns an [`OutstandingIoPermit`]. The caller should
    /// hold this permit for as long as the corresponding I/O operation remains
    /// in flight. Dropping the permit releases the slot and wakes any waiting
    /// sibling streams.
    ///
    /// Returns `None` if a permit can not be acquired
    pub fn try_acquire_io_permit(&self) -> Option<OutstandingIoPermit> {
        let mut inner = self
            .inner
            .lock()
            .expect("shared file stream state poisoned");

        if inner.outstanding_ios >= inner.max_outstanding_ios {
            return None;
        }

        inner.outstanding_ios += 1;
        drop(inner);

        Some(OutstandingIoPermit {
            state: Some(self.clone()),
        })
    }

    /// Register a task waker to be notified when shared capacity may have
    /// become available.
    ///
    /// Callers should typically register a waker after failing to acquire an
    /// I/O permit and before returning `Poll::Pending`.
    pub fn register_waker(&self, waker: &Waker) {
        let mut inner = self
            .inner
            .lock()
            .expect("shared file stream state poisoned");
        inner.waiters.push(waker.clone());
    }

    fn release_io_permit(&self) {
        let waiters = {
            let mut inner = self
                .inner
                .lock()
                .expect("shared file stream state poisoned");
            inner.outstanding_ios = inner
                .outstanding_ios
                .checked_sub(1)
                .expect("outstanding I/O count underflow");
            std::mem::take(&mut inner.waiters)
        };

        for waiter in waiters {
            waiter.wake();
        }
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
}

impl Drop for OutstandingIoPermit {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.release_io_permit();
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
    fn permits_obey_global_limit() {
        let state = SharedFileStreamState::new(2);

        let permit1 = state.try_acquire_io_permit().unwrap();
        let permit2 = state.try_acquire_io_permit().unwrap();
        let permit3 = state.try_acquire_io_permit();

        assert_eq!(state.outstanding_ios(), 2);
        assert!(permit3.is_none());

        drop(permit1);
        assert_eq!(state.outstanding_ios(), 1);
        assert!(state.try_acquire_io_permit().is_some());

        drop(permit2);
    }

    #[test]
    fn releasing_permit_wakes_waiters() {
        let state = SharedFileStreamState::new(1);
        let permit = state.try_acquire_io_permit().unwrap();

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        state.register_waker(&waker);

        drop(permit);

        assert_eq!(wake_counter.wake_count.load(Ordering::SeqCst), 1);
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
