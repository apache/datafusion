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

//! Channel based on flume

use flume::r#async::RecvStream;
use flume::{unbounded, Sender};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub(super) struct DistributionSender<T>(Sender<T>);

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> DistributionSender<T> {
    pub fn send(&self, item: T) -> flume::r#async::SendFut<'_, T> {
        self.0.send_async(item)
    }
}

pub(super) struct DistributionReceiver<T: 'static>(RecvStream<'static, T>);

impl<T: 'static> std::fmt::Debug for DistributionReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistributionReceiver")
    }
}

impl<T: 'static> DistributionReceiver<T> {
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// Create `n` empty channels.
pub(super) fn channels<T>(
    n: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    (0..n)
        .map(|_| {
            let (tx, rx) = unbounded();
            (
                DistributionSender(tx),
                DistributionReceiver(rx.into_stream()),
            )
        })
        .unzip()
}

pub(super) type PartitionAwareSenders<T> = Vec<Vec<DistributionSender<T>>>;
pub(super) type PartitionAwareReceivers<T> = Vec<Vec<DistributionReceiver<T>>>;

pub(super) fn partition_aware_channels<T>(
    n_in: usize,
    n_out: usize,
) -> (PartitionAwareSenders<T>, PartitionAwareReceivers<T>) {
    (0..n_in).map(|_| channels(n_out)).unzip()
}
