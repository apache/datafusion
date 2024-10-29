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
    ops::DerefMut,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// Create `n` empty channels.
pub fn channels<T>(
    n: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|id| Arc::new(Channel::new_with_one_sender(id)))
        .collect::<Vec<_>>();
    let gate = Arc::new(Gate {
        empty_channels: AtomicUsize::new(n),
        send_wakers: Mutex::new(None),
    });
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
        self.channel.n_senders.fetch_add(1, Ordering::SeqCst);

        Self {
            channel: Arc::clone(&self.channel),
            gate: Arc::clone(&self.gate),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let n_senders_pre = self.channel.n_senders.fetch_sub(1, Ordering::SeqCst);
        // is the the last copy of the sender side?
        if n_senders_pre > 1 {
            return;
        }

        let receivers = {
            let mut state = self.channel.state.lock();

            // During the shutdown of a empty channel, both the sender and the receiver side will be dropped. However we
            // only want to decrement the "empty channels" counter once.
            //
            // We are within a critical section here, so we we can safely assume that either the last sender or the
            // receiver (there's only one) will be dropped first.
            //
            // If the last sender is dropped first, `state.data` will still exists and the sender side decrements the
            // signal. The receiver side then MUST check the `n_senders` counter during the section and if it is zero,
            // it inferres that it is dropped afterwards and MUST NOT decrement the counter.
            //
            // If the receiver end is dropped first, it will inferr -- based on `n_senders` -- that there are still
            // senders and it will decrement the `empty_channels` counter. It will also set `data` to `None`. The sender
            // side will then see that `data` is `None` and can therefore inferr that the receiver end was dropped, and
            // hence it MUST NOT decrement the `empty_channels` counter.
            if state
                .data
                .as_ref()
                .map(|data| data.is_empty())
                .unwrap_or_default()
            {
                // channel is gone, so we need to clear our signal
                self.gate.decr_empty_channels();
            }

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
    gate: &'a SharedGate,
    // the additional Box is required for `Self: Unpin`
    element: Box<Option<T>>,
}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(this.element.is_some(), "polled ready future");

        // lock scope
        let to_wake = {
            let mut guard_channel_state = this.channel.state.lock();

            let Some(data) = guard_channel_state.data.as_mut() else {
                // receiver end dead
                return Poll::Ready(Err(SendError(
                    this.element.take().expect("just checked"),
                )));
            };

            // does ANY receiver need data?
            // if so, allow sender to create another
            if this.gate.empty_channels.load(Ordering::SeqCst) == 0 {
                let mut guard = this.gate.send_wakers.lock();
                if let Some(send_wakers) = guard.deref_mut() {
                    send_wakers.push((cx.waker().clone(), this.channel.id));
                    return Poll::Pending;
                }
            }

            let was_empty = data.is_empty();
            data.push_back(this.element.take().expect("just checked"));

            if was_empty {
                this.gate.decr_empty_channels();
                guard_channel_state.take_recv_wakers()
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
        let mut guard_channel_state = self.channel.state.lock();
        let data = guard_channel_state.data.take().expect("not dropped yet");

        // See `DistributedSender::drop` for an explanation of the drop order and when the "empty channels" counter is
        // decremented.
        if data.is_empty() && (self.channel.n_senders.load(Ordering::SeqCst) > 0) {
            // channel is gone, so we need to clear our signal
            self.gate.decr_empty_channels();
        }

        // senders may be waiting for gate to open but should error now that the channel is closed
        self.gate.wake_channel_senders(self.channel.id);
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

        let mut guard_channel_state = this.channel.state.lock();
        let channel_state = guard_channel_state.deref_mut();
        let data = channel_state.data.as_mut().expect("not dropped yet");

        match data.pop_front() {
            Some(element) => {
                // change "empty" signal for this channel?
                if data.is_empty() && channel_state.recv_wakers.is_some() {
                    // update counter
                    let old_counter =
                        this.gate.empty_channels.fetch_add(1, Ordering::SeqCst);

                    // open gate?
                    let to_wake = if old_counter == 0 {
                        let mut guard = this.gate.send_wakers.lock();

                        // check after lock to see if we should still change the state
                        if this.gate.empty_channels.load(Ordering::SeqCst) > 0 {
                            guard.take().unwrap_or_default()
                        } else {
                            Vec::with_capacity(0)
                        }
                    } else {
                        Vec::with_capacity(0)
                    };

                    drop(guard_channel_state);

                    // wake outside of lock scope
                    for (waker, _channel_id) in to_wake {
                        waker.wake();
                    }
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

    /// Channel ID.
    ///
    /// This is used to address [send wakers](Gate::send_wakers).
    id: usize,

    /// Mutable state.
    state: Mutex<ChannelState<T>>,
}

impl<T> Channel<T> {
    /// Create new channel with one sender (so we don't need to [fetch-add](AtomicUsize::fetch_add) directly afterwards).
    fn new_with_one_sender(id: usize) -> Self {
        Channel {
            n_senders: AtomicUsize::new(1),
            id,
            state: Mutex::new(ChannelState {
                data: Some(VecDeque::default()),
                recv_wakers: Some(Vec::default()),
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

/// The "all channels have data" gate.
#[derive(Debug)]
struct Gate {
    /// Number of currently empty (and still open) channels.
    empty_channels: AtomicUsize,

    /// Wakers for the sender side, including their channel IDs.
    ///
    /// This is `None` if the there are non-empty channels.
    send_wakers: Mutex<Option<Vec<(Waker, usize)>>>,
}

impl Gate {
    /// Wake senders for a specific channel.
    ///
    /// This is helpful to signal that the receiver side is gone and the senders shall now error.
    fn wake_channel_senders(&self, id: usize) {
        // lock scope
        let to_wake = {
            let mut guard = self.send_wakers.lock();

            if let Some(send_wakers) = guard.deref_mut() {
                // `drain_filter` is unstable, so implement our own
                let (wake, keep) =
                    send_wakers.drain(..).partition(|(_waker, id2)| id == *id2);

                *send_wakers = keep;

                wake
            } else {
                Vec::with_capacity(0)
            }
        };

        // wake outside of lock scope
        for (waker, _id) in to_wake {
            waker.wake();
        }
    }

    fn decr_empty_channels(&self) {
        let old_count = self.empty_channels.fetch_sub(1, Ordering::SeqCst);

        if old_count == 1 {
            let mut guard = self.send_wakers.lock();

            // double-check state during lock
            if self.empty_channels.load(Ordering::SeqCst) == 0 && guard.is_none() {
                *guard = Some(Vec::new());
            }
        }
    }
}

/// Gate shared by all senders and receivers.
type SharedGate = Arc<Gate>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

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
        assert_eq!(poll_ready(&mut recv_fut), Some("a"),);

        poll_ready(&mut txs[0].send("b")).unwrap();
        let mut send_fut = txs[0].send("c");
        let waker_3 = poll_pending(&mut send_fut);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("b"),);
        assert!(waker_3.woken());
        poll_ready(&mut send_fut).unwrap();
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("c"));

        let mut recv_fut = rxs[0].recv();
        let waker_4 = poll_pending(&mut recv_fut);

        let mut recv_fut = rxs[0].recv();
        let waker_5 = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("d")).unwrap();
        let mut send_fut = txs[0].send("e");
        let waker_6a = poll_pending(&mut send_fut);
        let waker_6b = poll_pending(&mut send_fut);

        assert!(waker_4.woken());
        assert!(waker_5.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("d"),);

        assert!(waker_6a.woken());
        assert!(waker_6b.woken());
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
