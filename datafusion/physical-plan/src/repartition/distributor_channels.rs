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

//! Special channel construction to distribute data from various inputs into N outputs
//! using per-channel backpressure to prevent unbounded buffering.
//!
//! # Design
//!
//! Each channel independently enforces a capacity limit. When a channel's buffer
//! is full, only senders targeting that specific channel are blocked. Senders to
//! other channels with available capacity can continue making progress.
//!
//! ```text
//! +----+      +-------------------+
//! | TX |==||  | Channel 0        |
//! +----+  ||  | [capacity limit] |  +----+
//!         ====| Buffer <=2       |==| RX |
//! +----+  ||  +-------------------+  +----+
//! | TX |==||
//! +----+
//!             +-------------------+
//! +----+      | Channel 1        |  +----+
//! | TX |======| Buffer <=2       |==| RX |
//! +----+      +-------------------+  +----+
//! ```
//!
//! There are `N` virtual MPSC (multi-producer, single consumer) channels, each with a
//! bounded capacity of [`CHANNEL_CAPACITY`]. When a channel's buffer reaches capacity,
//! senders to that channel will be [pending](Poll::Pending) until the receiver drains
//! data from that specific channel. This provides per-channel backpressure without
//! head-of-line blocking across channels.
use std::{
    collections::VecDeque,
    future::Future,
    ops::DerefMut,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// Per-channel backpressure capacity.
///
/// Each channel can buffer up to this many items before blocking senders.
/// A value of 2 allows sender and receiver to overlap operations for better
/// throughput while still providing meaningful backpressure.
const CHANNEL_CAPACITY: usize = 2;

/// Create `n` empty channels with per-channel backpressure.
pub fn channels<T>(
    n: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|_| Arc::new(Channel::new_with_one_sender(CHANNEL_CAPACITY)))
        .collect::<Vec<_>>();
    let senders = channels
        .iter()
        .map(|channel| DistributionSender {
            channel: Arc::clone(channel),
        })
        .collect();
    let receivers = channels
        .into_iter()
        .map(|channel| DistributionReceiver { channel })
        .collect();
    (senders, receivers)
}

type PartitionAwareSenders<T> = Vec<Vec<DistributionSender<T>>>;
type PartitionAwareReceivers<T> = Vec<Vec<DistributionReceiver<T>>>;

/// Create `n_out` empty channels for each of the `n_in` inputs.
/// This way, each distinct partition will communicate via a dedicated channel.
/// This SPSC structure enables us to track which partition input data comes from.
pub fn partition_aware_channels<T>(
    n_in: usize,
    n_out: usize,
) -> (PartitionAwareSenders<T>, PartitionAwareReceivers<T>) {
    (0..n_in).map(|_| channels(n_out)).unzip()
}

/// Erroring during [send](DistributionSender::send).
///
/// This occurs when the [receiver](DistributionReceiver) is gone.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SendError").finish()
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot send data, receiver is gone")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Sender side of distribution [channels].
///
/// This handle can be cloned. All clones will write into the same channel. Dropping the last sender will close the
/// channel. In this case, the [receiver](DistributionReceiver) will still be able to poll the remaining data, but will
/// receive `None` afterwards.
#[derive(Debug)]
pub struct DistributionSender<T> {
    channel: SharedChannel<T>,
}

impl<T> DistributionSender<T> {
    /// Send data.
    ///
    /// This fails if the [receiver](DistributionReceiver) is gone.
    ///
    /// If the channel's buffer is at capacity, the returned future will be
    /// [pending](Poll::Pending) until the receiver drains data from this channel.
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            channel: &self.channel,
            element: Box::new(Some(element)),
        }
    }
}

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        self.channel.n_senders.fetch_add(1, Ordering::SeqCst);

        Self {
            channel: Arc::clone(&self.channel),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let n_senders_pre = self.channel.n_senders.fetch_sub(1, Ordering::SeqCst);
        // is the last copy of the sender side?
        if n_senders_pre > 1 {
            return;
        }

        let receivers = {
            let mut state = self.channel.state.lock();

            // make sure that nobody can add wakers anymore
            state.recv_wakers.take().expect("not closed yet")
        };

        // wake outside of lock scope
        for recv in receivers {
            recv.wake();
        }
    }
}

/// Future backing [send](DistributionSender::send).
#[derive(Debug)]
pub struct SendFuture<'a, T> {
    channel: &'a SharedChannel<T>,
    // the additional Box is required for `Self: Unpin`
    element: Box<Option<T>>,
}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(this.element.is_some(), "polled ready future");

        // lock scope
        let to_wake = {
            let mut guard = this.channel.state.lock();

            let Some(data) = guard.data.as_mut() else {
                // receiver end dead
                return Poll::Ready(Err(SendError(
                    this.element.take().expect("just checked"),
                )));
            };

            // Per-channel backpressure: block if this channel is at capacity
            if data.len() >= this.channel.capacity {
                guard.send_wakers.push(cx.waker().clone());
                return Poll::Pending;
            }

            let was_empty = data.is_empty();
            data.push_back(this.element.take().expect("just checked"));

            if was_empty {
                guard.take_recv_wakers()
            } else {
                Vec::with_capacity(0)
            }
        };

        // wake outside of lock scope
        for receiver in to_wake {
            receiver.wake();
        }

        Poll::Ready(Ok(()))
    }
}

/// Receiver side of distribution [channels].
#[derive(Debug)]
pub struct DistributionReceiver<T> {
    channel: SharedChannel<T>,
}

impl<T> DistributionReceiver<T> {
    /// Receive data from channel.
    ///
    /// Returns `None` if the channel is empty and no [senders](DistributionSender) are left.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            channel: &mut self.channel,
            rdy: false,
        }
    }
}

impl<T> Drop for DistributionReceiver<T> {
    fn drop(&mut self) {
        let mut guard = self.channel.state.lock();
        guard.data.take().expect("not dropped yet");

        // Wake all blocked senders so they see the receiver is gone and return SendError
        let send_wakers = std::mem::take(&mut guard.send_wakers);
        drop(guard);

        for waker in send_wakers {
            waker.wake();
        }
    }
}

/// Future backing [recv](DistributionReceiver::recv).
pub struct RecvFuture<'a, T> {
    channel: &'a mut SharedChannel<T>,
    rdy: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.rdy, "polled ready future");

        let mut guard_channel_state = this.channel.state.lock();
        let channel_state = guard_channel_state.deref_mut();
        let data = channel_state.data.as_mut().expect("not dropped yet");

        match data.pop_front() {
            Some(element) => {
                // Wake blocked senders if the buffer was at capacity before this pop.
                // After popping, data.len() == old_len - 1, so old_len == data.len() + 1.
                // If old_len >= capacity, senders may have been blocked.
                let to_wake =
                    if data.len() + 1 >= this.channel.capacity {
                        std::mem::take(&mut channel_state.send_wakers)
                    } else {
                        Vec::with_capacity(0)
                    };

                drop(guard_channel_state);

                // wake outside of lock scope
                for waker in to_wake {
                    waker.wake();
                }

                this.rdy = true;
                Poll::Ready(Some(element))
            }
            None => {
                if let Some(recv_wakers) = channel_state.recv_wakers.as_mut() {
                    recv_wakers.push(cx.waker().clone());
                    Poll::Pending
                } else {
                    this.rdy = true;
                    Poll::Ready(None)
                }
            }
        }
    }
}

/// Links senders and receivers.
#[derive(Debug)]
struct Channel<T> {
    /// Reference counter for the sender side.
    n_senders: AtomicUsize,

    /// Per-channel capacity limit for backpressure.
    capacity: usize,

    /// Mutable state.
    state: Mutex<ChannelState<T>>,
}

impl<T> Channel<T> {
    /// Create new channel with one sender and the given capacity.
    fn new_with_one_sender(capacity: usize) -> Self {
        Channel {
            n_senders: AtomicUsize::new(1),
            capacity,
            state: Mutex::new(ChannelState {
                data: Some(VecDeque::default()),
                recv_wakers: Some(Vec::default()),
                send_wakers: Vec::default(),
            }),
        }
    }
}

#[derive(Debug)]
struct ChannelState<T> {
    /// Buffered data.
    ///
    /// This is [`None`] when the receiver is gone.
    data: Option<VecDeque<T>>,

    /// Wakers for the receiver side.
    ///
    /// The receiver will be pending if the [buffer](Self::data) is empty and
    /// there are senders left (otherwise this is set to [`None`]).
    recv_wakers: Option<Vec<Waker>>,

    /// Wakers for blocked senders.
    ///
    /// Senders are blocked when the channel buffer reaches [capacity](Channel::capacity).
    /// They are woken when the receiver consumes data, making space in the buffer.
    send_wakers: Vec<Waker>,
}

impl<T> ChannelState<T> {
    /// Get all [`recv_wakers`](Self::recv_wakers) and replace with identically-sized buffer.
    ///
    /// The wakers should be woken AFTER the lock to [this state](Self) was dropped.
    ///
    /// # Panics
    /// Assumes that channel is NOT closed yet, i.e. that [`recv_wakers`](Self::recv_wakers) is not [`None`].
    fn take_recv_wakers(&mut self) -> Vec<Waker> {
        let to_wake = self.recv_wakers.as_mut().expect("not closed");
        let mut tmp = Vec::with_capacity(to_wake.capacity());
        std::mem::swap(to_wake, &mut tmp);
        tmp
    }
}

/// Shared channel.
///
/// One or multiple senders and a single receiver will share a channel.
type SharedChannel<T> = Arc<Channel<T>>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use futures::{FutureExt, task::ArcWake};

    use super::*;

    #[test]
    fn test_single_channel_send_recv() {
        let (txs, mut rxs) = channels(1);

        let mut recv_fut = rxs[0].recv();
        let waker = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("foo")).unwrap();
        assert!(waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("foo"));

        // Send up to capacity (2)
        poll_ready(&mut txs[0].send("bar")).unwrap();
        poll_ready(&mut txs[0].send("baz")).unwrap();

        // Channel at capacity - next send blocks
        let mut send_fut = txs[0].send("end");
        let send_waker = poll_pending(&mut send_fut);

        // Consume one to make room
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"));
        assert!(send_waker.woken());
        poll_ready(&mut send_fut).unwrap();

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("baz"));
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("end"));
    }

    #[test]
    fn test_multi_sender() {
        let (txs, mut rxs) = channels(2);

        let tx_clone = txs[0].clone();

        poll_ready(&mut txs[0].send("foo")).unwrap();
        poll_ready(&mut tx_clone.send("bar")).unwrap();

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("foo"));
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"));
    }

    #[test]
    fn test_per_channel_backpressure() {
        let (txs, mut rxs) = channels(2);

        // Fill channel 0 to capacity
        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[0].send("0_b")).unwrap();

        // Channel 0 at capacity - blocks
        let mut send_fut_0 = txs[0].send("0_c");
        let waker_0 = poll_pending(&mut send_fut_0);

        // Channel 1 is independent - can still send!
        poll_ready(&mut txs[1].send("1_a")).unwrap();
        poll_ready(&mut txs[1].send("1_b")).unwrap();

        // Channel 1 now also at capacity - blocks
        let mut send_fut_1 = txs[1].send("1_c");
        let waker_1 = poll_pending(&mut send_fut_1);

        // Drain channel 0 - only channel 0 senders wake
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_a"));
        assert!(waker_0.woken());
        assert!(!waker_1.woken());

        // Channel 0 sender can now proceed
        poll_ready(&mut send_fut_0).unwrap();

        // Drain channel 1
        assert_eq!(poll_ready(&mut rxs[1].recv()), Some("1_a"));
        assert!(waker_1.woken());
        poll_ready(&mut send_fut_1).unwrap();
    }

    #[test]
    fn test_close_channel_by_dropping_tx() {
        let (mut txs, mut rxs) = channels::<&str>(2);

        let tx0 = txs.remove(0);
        let _tx1 = txs.remove(0);
        let tx0_clone = tx0.clone();

        let mut recv_fut = rxs[0].recv();

        let recv_waker = poll_pending(&mut recv_fut);

        // drop original sender
        drop(tx0);

        // not yet closed (there's a clone left)
        assert!(!recv_waker.woken());
        let recv_waker = poll_pending(&mut recv_fut);

        // create new clone
        let tx0_clone2 = tx0_clone.clone();
        assert!(!recv_waker.woken());
        let recv_waker = poll_pending(&mut recv_fut);

        // drop first clone
        drop(tx0_clone);
        assert!(!recv_waker.woken());
        let recv_waker = poll_pending(&mut recv_fut);

        // drop last clone
        drop(tx0_clone2);

        // channel closed
        assert!(recv_waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), None);
    }

    #[test]
    fn test_close_channel_by_dropping_rx() {
        let (txs, mut rxs) = channels(2);

        let rx0 = rxs.remove(0);
        let _rx1 = rxs.remove(0);

        // drop receiver
        drop(rx0);

        assert_eq!(poll_ready(&mut txs[0].send("foo")), Err(SendError("foo")));
    }

    #[test]
    fn test_close_channel_by_dropping_rx_wakes_blocked_senders() {
        let (txs, mut rxs) = channels(1);

        let rx0 = rxs.remove(0);

        // Fill channel to capacity
        poll_ready(&mut txs[0].send("a")).unwrap();
        poll_ready(&mut txs[0].send("b")).unwrap();

        // Sender blocked at capacity
        let mut send_fut = txs[0].send("c");
        let waker = poll_pending(&mut send_fut);

        // Drop receiver - should wake blocked sender
        drop(rx0);

        assert!(waker.woken());
        assert_eq!(poll_ready(&mut send_fut), Err(SendError("c")));
    }

    #[test]
    fn test_drop_rx_three_channels() {
        let (mut txs, mut rxs) = channels(3);

        let tx0 = txs.remove(0);
        let tx1 = txs.remove(0);
        let tx2 = txs.remove(0);
        let mut rx0 = rxs.remove(0);
        let rx1 = rxs.remove(0);
        let _rx2 = rxs.remove(0);

        // fill channels (one item each, below capacity)
        poll_ready(&mut tx0.send("0_a")).unwrap();
        poll_ready(&mut tx1.send("1_a")).unwrap();
        poll_ready(&mut tx2.send("2_a")).unwrap();

        // drop / close one channel
        drop(rx1);

        // receive data
        assert_eq!(poll_ready(&mut rx0.recv()), Some("0_a"));

        // use senders again
        poll_ready(&mut tx0.send("0_b")).unwrap();
        assert_eq!(poll_ready(&mut tx1.send("1_b")), Err(SendError("1_b")));
        // Per-channel: tx2 can still send (channel 2 has capacity)
        poll_ready(&mut tx2.send("2_b")).unwrap();
    }

    #[test]
    fn test_close_channel_by_dropping_rx_clears_data() {
        let (txs, rxs) = channels(1);

        let obj = Arc::new(());
        let counter = Arc::downgrade(&obj);
        assert_eq!(counter.strong_count(), 1);

        // add object to channel
        poll_ready(&mut txs[0].send(obj)).unwrap();
        assert_eq!(counter.strong_count(), 1);

        // drop receiver
        drop(rxs);

        assert_eq!(counter.strong_count(), 0);
    }

    /// Ensure that polling "pending" futures work even when you poll them too often (which happens under some circumstances).
    #[test]
    fn test_poll_empty_channel_twice() {
        let (txs, mut rxs) = channels(1);

        let mut recv_fut = rxs[0].recv();
        let waker_1a = poll_pending(&mut recv_fut);
        let waker_1b = poll_pending(&mut recv_fut);

        let mut recv_fut = rxs[0].recv();
        let waker_2 = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("a")).unwrap();
        assert!(waker_1a.woken());
        assert!(waker_1b.woken());
        assert!(waker_2.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("a"));

        // Send up to capacity
        poll_ready(&mut txs[0].send("b")).unwrap();
        poll_ready(&mut txs[0].send("c")).unwrap();

        // Channel at capacity, next send blocks
        let mut send_fut = txs[0].send("d");
        let waker_3 = poll_pending(&mut send_fut);

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("b"));
        assert!(waker_3.woken());
        poll_ready(&mut send_fut).unwrap();

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("c"));
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("d"));

        let mut recv_fut = rxs[0].recv();
        let waker_4 = poll_pending(&mut recv_fut);

        let mut recv_fut = rxs[0].recv();
        let waker_5 = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("e")).unwrap();

        assert!(waker_4.woken());
        assert!(waker_5.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("e"));
    }

    #[test]
    fn test_poll_send_blocked_twice() {
        let (txs, mut rxs) = channels(1);

        // Fill to capacity
        poll_ready(&mut txs[0].send("a")).unwrap();
        poll_ready(&mut txs[0].send("b")).unwrap();

        // Blocked - poll twice with different wakers
        let mut send_fut = txs[0].send("c");
        let waker_a = poll_pending(&mut send_fut);
        let waker_b = poll_pending(&mut send_fut);

        // Drain to make space
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("a"));

        // Both wakers should be notified
        assert!(waker_a.woken());
        assert!(waker_b.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_send_future_after_ready_ok() {
        let (txs, _rxs) = channels(1);
        let mut fut = txs[0].send("foo");
        poll_ready(&mut fut).unwrap();
        poll_ready(&mut fut).ok();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_send_future_after_ready_err() {
        let (txs, rxs) = channels(1);

        drop(rxs);

        let mut fut = txs[0].send("foo");
        poll_ready(&mut fut).unwrap_err();
        poll_ready(&mut fut).ok();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_recv_future_after_ready_some() {
        let (txs, mut rxs) = channels(1);

        poll_ready(&mut txs[0].send("foo")).unwrap();

        let mut fut = rxs[0].recv();
        poll_ready(&mut fut).unwrap();
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_recv_future_after_ready_none() {
        let (txs, mut rxs) = channels::<u8>(1);

        drop(txs);

        let mut fut = rxs[0].recv();
        assert!(poll_ready(&mut fut).is_none());
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "future is pending")]
    fn test_meta_poll_ready_wrong_state() {
        let mut fut = futures::future::pending::<u8>();
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "future is ready")]
    fn test_meta_poll_pending_wrong_state() {
        let mut fut = futures::future::ready(1);
        poll_pending(&mut fut);
    }

    /// Test [`poll_pending`] (i.e. the testing utils, not the actual library code).
    #[test]
    fn test_meta_poll_pending_waker() {
        let (tx, mut rx) = futures::channel::oneshot::channel();
        let waker = poll_pending(&mut rx);
        assert!(!waker.woken());
        tx.send(1).unwrap();
        assert!(waker.woken());
    }

    /// Poll a given [`Future`] and ensure it is [ready](Poll::Ready).
    #[track_caller]
    fn poll_ready<F>(fut: &mut F) -> F::Output
    where
        F: Future + Unpin,
    {
        match poll(fut).0 {
            Poll::Ready(x) => x,
            Poll::Pending => panic!("future is pending"),
        }
    }

    /// Poll a given [`Future`] and ensure it is [pending](Poll::Pending).
    ///
    /// Returns a waker that can later be checked.
    #[track_caller]
    fn poll_pending<F>(fut: &mut F) -> Arc<TestWaker>
    where
        F: Future + Unpin,
    {
        let (res, waker) = poll(fut);
        match res {
            Poll::Ready(_) => panic!("future is ready"),
            Poll::Pending => waker,
        }
    }

    fn poll<F>(fut: &mut F) -> (Poll<F::Output>, Arc<TestWaker>)
    where
        F: Future + Unpin,
    {
        let test_waker = Arc::new(TestWaker::default());
        let waker = futures::task::waker(Arc::clone(&test_waker));
        let mut cx = Context::from_waker(&waker);
        let res = fut.poll_unpin(&mut cx);
        (res, test_waker)
    }

    /// A test [`Waker`] that signal if [`wake`](Waker::wake) was called.
    #[derive(Debug, Default)]
    struct TestWaker {
        woken: AtomicBool,
    }

    impl TestWaker {
        /// Was [`wake`](Waker::wake) called?
        fn woken(&self) -> bool {
            self.woken.load(Ordering::SeqCst)
        }
    }

    impl ArcWake for TestWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.woken.store(true, Ordering::SeqCst);
        }
    }
}
