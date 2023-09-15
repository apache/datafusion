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
//! minimizing buffering but preventing deadlocks when repartitoning
//!
//! # Design
//!
//! ```text
//! +----+      +------+
//! | TX |==||  | Gate |
//! +----+  ||  |      |  +--------+  +----+
//!         ====|      |==| Buffer |==| RX |
//! +----+  ||  |      |  +--------+  +----+
//! | TX |==||  |      |
//! +----+      |      |
//!             |      |
//! +----+      |      |  +--------+  +----+
//! | TX |======|      |==| Buffer |==| RX |
//! +----+      +------+  +--------+  +----+
//! ```
//!
//! There are `N` virtual MPSC (multi-producer, single consumer) channels with unbounded capacity. However, if all
//! buffers/channels are non-empty, than a global gate will be closed preventing new data from being written (the
//! sender futures will be [pending](Poll::Pending)) until at least one channel is empty (and not closed).
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// Create `n` empty channels.
pub fn channels<T>(
    n: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|id| {
            Arc::new(Mutex::new(Channel {
                data: VecDeque::default(),
                n_senders: 1,
                recv_alive: true,
                recv_wakers: Vec::default(),
                id,
            }))
        })
        .collect::<Vec<_>>();
    let gate = Arc::new(Mutex::new(Gate {
        empty_channels: n,
        send_wakers: Vec::default(),
    }));
    let senders = channels
        .iter()
        .map(|channel| DistributionSender {
            channel: Arc::clone(channel),
            gate: Arc::clone(&gate),
        })
        .collect();
    let receivers = channels
        .into_iter()
        .map(|channel| DistributionReceiver {
            channel,
            gate: Arc::clone(&gate),
        })
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
    /// To prevent lock inversion / deadlock, channel lock is always acquired prior to gate lock
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionSender<T> {
    /// Send data.
    ///
    /// This fails if the [receiver](DistributionReceiver) is gone.
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            channel: &self.channel,
            gate: &self.gate,
            element: Box::new(Some(element)),
        }
    }
}

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        let mut guard = self.channel.lock();
        guard.n_senders += 1;

        Self {
            channel: Arc::clone(&self.channel),
            gate: Arc::clone(&self.gate),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let mut guard_channel = self.channel.lock();
        guard_channel.n_senders -= 1;

        if guard_channel.n_senders == 0 {
            // Note: the recv_alive check is so that we don't double-clear the status
            if guard_channel.data.is_empty() && guard_channel.recv_alive {
                // channel is gone, so we need to clear our signal
                let mut guard_gate = self.gate.lock();
                guard_gate.empty_channels -= 1;
            }

            // receiver may be waiting for data, but should return `None` now since the channel is closed
            guard_channel.wake_receivers();
        }
    }
}

/// Future backing [send](DistributionSender::send).
#[derive(Debug)]
pub struct SendFuture<'a, T> {
    channel: &'a SharedChannel<T>,
    gate: &'a SharedGate,
    // the additional Box is required for `Self: Unpin`
    element: Box<Option<T>>,
}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(this.element.is_some(), "polled ready future");

        let mut guard_channel = this.channel.lock();

        // receiver end still alive?
        if !guard_channel.recv_alive {
            return Poll::Ready(Err(SendError(
                this.element.take().expect("just checked"),
            )));
        }

        let mut guard_gate = this.gate.lock();

        // does ANY receiver need data?
        // if so, allow sender to create another
        if guard_gate.empty_channels == 0 {
            guard_gate
                .send_wakers
                .push((cx.waker().clone(), guard_channel.id));
            return Poll::Pending;
        }

        let was_empty = guard_channel.data.is_empty();
        guard_channel
            .data
            .push_back(this.element.take().expect("just checked"));
        if was_empty {
            guard_gate.empty_channels -= 1;
            guard_channel.wake_receivers();
        }

        Poll::Ready(Ok(()))
    }
}

/// Receiver side of distribution [channels].
#[derive(Debug)]
pub struct DistributionReceiver<T> {
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionReceiver<T> {
    /// Receive data from channel.
    ///
    /// Returns `None` if the channel is empty and no [senders](DistributionSender) are left.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            channel: &mut self.channel,
            gate: &mut self.gate,
            rdy: false,
        }
    }
}

impl<T> Drop for DistributionReceiver<T> {
    fn drop(&mut self) {
        let mut guard_channel = self.channel.lock();
        let mut guard_gate = self.gate.lock();
        guard_channel.recv_alive = false;

        // Note: n_senders check is here so we don't double-clear the signal
        if guard_channel.data.is_empty() && (guard_channel.n_senders > 0) {
            // channel is gone, so we need to clear our signal
            guard_gate.empty_channels -= 1;
        }

        // senders may be waiting for gate to open but should error now that the channel is closed
        guard_gate.wake_channel_senders(guard_channel.id);

        // clear potential remaining data from channel
        guard_channel.data.clear();
    }
}

/// Future backing [recv](DistributionReceiver::recv).
pub struct RecvFuture<'a, T> {
    channel: &'a mut SharedChannel<T>,
    gate: &'a mut SharedGate,
    rdy: bool,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.rdy, "polled ready future");

        let mut guard_channel = this.channel.lock();

        match guard_channel.data.pop_front() {
            Some(element) => {
                // change "empty" signal for this channel?
                if guard_channel.data.is_empty() && (guard_channel.n_senders > 0) {
                    let mut guard_gate = this.gate.lock();

                    // update counter
                    let old_counter = guard_gate.empty_channels;
                    guard_gate.empty_channels += 1;

                    // open gate?
                    if old_counter == 0 {
                        guard_gate.wake_all_senders();
                    }

                    drop(guard_gate);
                    drop(guard_channel);
                }

                this.rdy = true;
                Poll::Ready(Some(element))
            }
            None if guard_channel.n_senders == 0 => {
                this.rdy = true;
                Poll::Ready(None)
            }
            None => {
                guard_channel.recv_wakers.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Links senders and receivers.
#[derive(Debug)]
struct Channel<T> {
    /// Buffered data.
    data: VecDeque<T>,

    /// Reference counter for the sender side.
    n_senders: usize,

    /// Reference "counter"/flag for the single receiver.
    recv_alive: bool,

    /// Wakers for the receiver side.
    ///
    /// The receiver will be pending if the [buffer](Self::data) is empty and
    /// there are senders left (according to the [reference counter](Self::n_senders)).
    recv_wakers: Vec<Waker>,

    /// Channel ID.
    ///
    /// This is used to address [send wakers](Gate::send_wakers).
    id: usize,
}

impl<T> Channel<T> {
    fn wake_receivers(&mut self) {
        for waker in self.recv_wakers.drain(..) {
            waker.wake();
        }
    }
}

/// Shared channel.
///
/// One or multiple senders and a single receiver will share a channel.
type SharedChannel<T> = Arc<Mutex<Channel<T>>>;

/// The "all channels have data" gate.
#[derive(Debug)]
struct Gate {
    /// Number of currently empty (and still open) channels.
    empty_channels: usize,

    /// Wakers for the sender side, including their channel IDs.
    send_wakers: Vec<(Waker, usize)>,
}

impl Gate {
    //// Wake all senders.
    ///
    /// This is helpful to signal that there are some channels empty now and hence the gate was opened.
    fn wake_all_senders(&mut self) {
        for (waker, _id) in self.send_wakers.drain(..) {
            waker.wake();
        }
    }

    /// Wake senders for a specific channel.
    ///
    /// This is helpful to signal that the receiver side is gone and the senders shall now error.
    fn wake_channel_senders(&mut self, id: usize) {
        // `drain_filter` is unstable, so implement our own
        let (wake, keep) = self
            .send_wakers
            .drain(..)
            .partition(|(_waker, id2)| id == *id2);
        self.send_wakers = keep;
        for (waker, _id) in wake {
            waker.wake();
        }
    }
}

/// Gate shared by all senders and receivers.
type SharedGate = Arc<Mutex<Gate>>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::{task::ArcWake, FutureExt};

    use super::*;

    #[test]
    fn test_single_channel_no_gate() {
        // use two channels so that the first one never hits the gate
        let (mut txs, mut rxs) = channels(2);

        let mut recv_fut = rxs[0].recv();
        let waker = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("foo")).unwrap();
        assert!(waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("foo"),);

        poll_ready(&mut txs[0].send("bar")).unwrap();
        poll_ready(&mut txs[0].send("baz")).unwrap();
        poll_ready(&mut txs[0].send("end")).unwrap();
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("baz"),);

        // close channel
        txs.remove(0);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("end"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
        assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
    }

    #[test]
    fn test_multi_sender() {
        // use two channels so that the first one never hits the gate
        let (txs, mut rxs) = channels(2);

        let tx_clone = txs[0].clone();

        poll_ready(&mut txs[0].send("foo")).unwrap();
        poll_ready(&mut tx_clone.send("bar")).unwrap();

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("foo"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
    }

    #[test]
    fn test_gate() {
        let (txs, mut rxs) = channels(2);

        // gate initially open
        poll_ready(&mut txs[0].send("0_a")).unwrap();

        // gate still open because channel 1 is still empty
        poll_ready(&mut txs[0].send("0_b")).unwrap();

        // gate still open because channel 1 is still empty prior to this call, so this call still goes through
        poll_ready(&mut txs[1].send("1_a")).unwrap();

        // both channels non-empty => gate closed

        let mut send_fut = txs[1].send("1_b");
        let waker = poll_pending(&mut send_fut);

        // drain channel 0
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_a"),);
        poll_pending(&mut send_fut);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_b"),);

        // channel 0 empty => gate open
        assert!(waker.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    fn test_close_channel_by_dropping_tx() {
        let (mut txs, mut rxs) = channels(2);

        let tx0 = txs.remove(0);
        let tx1 = txs.remove(0);
        let tx0_clone = tx0.clone();

        let mut recv_fut = rxs[0].recv();

        poll_ready(&mut tx1.send("a")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop original sender
        drop(tx0);

        // not yet closed (there's a clone left)
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("b")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // create new clone
        let tx0_clone2 = tx0_clone.clone();
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("c")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop first clone
        drop(tx0_clone);
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("d")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop last clone
        drop(tx0_clone2);

        // channel closed => also close gate
        poll_pending(&mut tx1.send("e"));
        assert!(recv_waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), None,);
    }

    #[test]
    fn test_close_channel_by_dropping_rx_on_open_gate() {
        let (txs, mut rxs) = channels(2);

        let rx0 = rxs.remove(0);
        let _rx1 = rxs.remove(0);

        poll_ready(&mut txs[1].send("a")).unwrap();

        // drop receiver => also close gate
        drop(rx0);

        poll_pending(&mut txs[1].send("b"));
        assert_eq!(poll_ready(&mut txs[0].send("foo")), Err(SendError("foo")),);
    }

    #[test]
    fn test_close_channel_by_dropping_rx_on_closed_gate() {
        let (txs, mut rxs) = channels(2);

        let rx0 = rxs.remove(0);
        let mut rx1 = rxs.remove(0);

        // fill both channels
        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[1].send("1_a")).unwrap();

        let mut send_fut0 = txs[0].send("0_b");
        let mut send_fut1 = txs[1].send("1_b");
        let waker0 = poll_pending(&mut send_fut0);
        let waker1 = poll_pending(&mut send_fut1);

        // drop receiver
        drop(rx0);

        assert!(waker0.woken());
        assert!(!waker1.woken());
        assert_eq!(poll_ready(&mut send_fut0), Err(SendError("0_b")),);

        // gate closed, so cannot send on channel 1
        poll_pending(&mut send_fut1);

        // channel 1 can still receive data
        assert_eq!(poll_ready(&mut rx1.recv()), Some("1_a"),);
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

        // fill channels
        poll_ready(&mut tx0.send("0_a")).unwrap();
        poll_ready(&mut tx1.send("1_a")).unwrap();
        poll_ready(&mut tx2.send("2_a")).unwrap();

        // drop / close one channel
        drop(rx1);

        // receive data
        assert_eq!(poll_ready(&mut rx0.recv()), Some("0_a"),);

        // use senders again
        poll_ready(&mut tx0.send("0_b")).unwrap();
        assert_eq!(poll_ready(&mut tx1.send("1_b")), Err(SendError("1_b")),);
        poll_pending(&mut tx2.send("2_b"));
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
        let mut cx = std::task::Context::from_waker(&waker);
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
