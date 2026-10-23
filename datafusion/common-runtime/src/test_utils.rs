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

use std::sync::mpsc::{Receiver, Sender, channel};

use tokio::runtime::Runtime;

pub(crate) struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("destructor panic");
    }
}

pub(crate) struct BlockInDrop {
    entered: Sender<()>,
    resume: Receiver<()>,
}

impl Drop for BlockInDrop {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        let _ = self.resume.recv();
    }
}

/// Returns a guard whose destructor signals the first receiver and then
/// blocks until the second sender sends or is dropped
pub(crate) fn block_in_drop() -> (BlockInDrop, Receiver<()>, Sender<()>) {
    let (entered_tx, entered_rx) = channel();
    let (resume_tx, resume_rx) = channel();
    let guard = BlockInDrop {
        entered: entered_tx,
        resume: resume_rx,
    };
    (guard, entered_rx, resume_tx)
}

pub(crate) fn one_worker_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap()
}
