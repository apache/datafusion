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

//! Dedicated io_uring worker thread.
//!
//! The worker drives a single `IoUring` and pipelines **multiple**
//! [`IoCommand`] requests through it concurrently. New requests are pulled
//! from a bounded mpsc channel as ring capacity allows; completions are
//! routed back to the originating `oneshot::Sender`s via a per-request
//! slot id encoded in the SQE `user_data`.
//!
//! Key points:
//! * SQE `user_data` = `(slot_id as u64) << 32 | (range_idx as u64)` so each
//!   completion can find its owning request and range.
//! * Partial reads (`ret` smaller than requested, which is legal per
//!   `read(2)`) queue a follow-up SQE for the remainder instead of being
//!   silently truncated.
//! * A request's [`File`] lives in the per-slot state, so the raw fd
//!   referenced by in-flight SQEs stays valid until every completion for
//!   that slot is drained.

use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use bytes::Bytes;
use io_uring::{IoUring, opcode, types};
use object_store::Result;
use tokio::sync::{mpsc, oneshot};

/// Ring size (number of SQ entries). 256 is a good default that
/// handles typical Parquet column-chunk batches without overflow.
const RING_ENTRIES: u32 = 256;

/// Maximum requests held in the worker at once. Requests arriving beyond
/// this are backpressured by the bounded command channel.
const MAX_IN_FLIGHT_REQUESTS: usize = 64;

/// Capacity of the command channel. The channel carries one element per
/// pending read request (typically one per Parquet `get_ranges` call), so
/// this is an upper bound on queued-but-not-yet-submitted requests.
pub(crate) const COMMAND_CHANNEL_CAPACITY: usize = 1024;

/// Command sent from the async ObjectStore methods to the io_uring thread.
pub(crate) enum IoCommand {
    ReadRanges {
        path: PathBuf,
        ranges: Vec<Range<u64>>,
        response: oneshot::Sender<Result<Vec<Bytes>>>,
    },
}

/// Per-request state living in the worker's slab while SQEs are in flight.
struct PendingRequest {
    /// Kept alive so the raw fd stored in in-flight SQEs stays valid.
    _file: std::fs::File,
    fd: i32,
    ranges: Vec<Range<u64>>,
    buffers: Vec<Vec<u8>>,
    filled: Vec<usize>,
    /// Number of ranges that still need more bytes read.
    ranges_remaining: usize,
    /// Number of SQEs still owed to us — either sitting in the backlog or
    /// in flight in the ring. We cannot free this slot until this reaches
    /// zero, otherwise a late CQE could land on a reallocated slot and
    /// scribble into an unrelated request's buffer.
    sqes_outstanding: usize,
    /// `None` after the response has been sent (success or error).
    response: Option<oneshot::Sender<Result<Vec<Bytes>>>>,
    /// Set once we have dispatched a terminal error; further completions
    /// for this slot are accounted for but otherwise ignored.
    errored: bool,
}

fn pack_user_data(slot: u32, range_idx: u32) -> u64 {
    ((slot as u64) << 32) | (range_idx as u64)
}

fn unpack_user_data(user_data: u64) -> (u32, u32) {
    ((user_data >> 32) as u32, (user_data & 0xFFFF_FFFF) as u32)
}

/// Slab holding `PendingRequest`s. `slots[i]` is `Some(..)` iff slot `i`
/// is currently in use. `free` tracks previously allocated slot indices
/// that are now reusable.
struct Slab {
    slots: Vec<Option<PendingRequest>>,
    free: Vec<u32>,
    in_flight: usize,
}

impl Slab {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            in_flight: 0,
        }
    }

    fn alloc(&mut self, req: PendingRequest) -> u32 {
        self.in_flight += 1;
        if let Some(id) = self.free.pop() {
            self.slots[id as usize] = Some(req);
            id
        } else {
            let id = self.slots.len() as u32;
            self.slots.push(Some(req));
            id
        }
    }

    fn free(&mut self, id: u32) {
        self.slots[id as usize] = None;
        self.free.push(id);
        self.in_flight -= 1;
    }

    fn get_mut(&mut self, id: u32) -> Option<&mut PendingRequest> {
        self.slots.get_mut(id as usize).and_then(|s| s.as_mut())
    }
}

/// Main loop for the io_uring worker thread.
///
/// Runs until the command channel is closed and all in-flight requests are
/// drained.
pub(crate) fn run_uring_loop(mut rx: mpsc::Receiver<IoCommand>) {
    let mut ring = match IoUring::new(RING_ENTRIES) {
        Ok(ring) => ring,
        Err(e) => {
            log::error!("Failed to create io_uring instance: {e}");
            drain_and_error(&mut rx, &e.to_string());
            return;
        }
    };

    let sq_capacity = ring.params().sq_entries() as usize;
    let mut slab = Slab::new();
    // Pending SQEs waiting for room in the ring. `(slot_id, range_idx)`.
    let mut sqe_backlog: Vec<(u32, u32)> = Vec::new();
    let mut rx_closed = false;

    loop {
        // 1) Pull new commands while we have room for more requests AND the
        //    channel has not been closed.
        while !rx_closed && slab.in_flight < MAX_IN_FLIGHT_REQUESTS {
            let cmd = if slab.in_flight == 0 && sqe_backlog.is_empty() {
                // Nothing in flight — block for the next command so the
                // worker thread doesn't busy-loop when idle.
                match rx.blocking_recv() {
                    Some(cmd) => cmd,
                    None => {
                        rx_closed = true;
                        break;
                    }
                }
            } else {
                match rx.try_recv() {
                    Ok(cmd) => cmd,
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        rx_closed = true;
                        break;
                    }
                }
            };
            accept_command(cmd, &mut slab, &mut sqe_backlog);
        }

        // 2) Push as many pending SQEs into the ring as will fit, then
        //    submit. `sq.push` returns Err when full.
        submit_sqes(&mut ring, &mut slab, &mut sqe_backlog);

        // 3) If there's work in the kernel, wait for at least one
        //    completion; otherwise loop back to receive more commands.
        if slab.in_flight == 0 {
            if rx_closed {
                break;
            }
            continue;
        }

        if let Err(e) = ring.submit_and_wait(1) {
            fail_all(&mut slab, &format!("io_uring submit failed: {e}"));
            continue;
        }

        // 4) Drain all available completions and route them back. If a
        //    completion is short, enqueue a follow-up SQE to fill the tail.
        drain_completions(&mut ring, &mut slab, &mut sqe_backlog, sq_capacity);
    }
}

/// Drain whatever is left on `rx` and respond to every waiter with `reason`.
fn drain_and_error(rx: &mut mpsc::Receiver<IoCommand>, reason: &str) {
    while let Some(cmd) = rx.blocking_recv() {
        let IoCommand::ReadRanges { response, .. } = cmd;
        let _ = response.send(Err(object_store::Error::Generic {
            store: "IoUringObjectStore",
            source: format!("io_uring init failed: {reason}").into(),
        }));
    }
}

/// Allocate a slab slot for the incoming command and queue one SQE per
/// range.  Errors (path not found, 32-bit overflow, etc.) are reported
/// immediately via the response channel and do not occupy a slot.
fn accept_command(cmd: IoCommand, slab: &mut Slab, backlog: &mut Vec<(u32, u32)>) {
    let IoCommand::ReadRanges {
        path,
        ranges,
        response,
    } = cmd;

    if ranges.is_empty() {
        let _ = response.send(Ok(vec![]));
        return;
    }

    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            let err = if e.kind() == std::io::ErrorKind::NotFound {
                object_store::Error::NotFound {
                    path: path.display().to_string(),
                    source: e.into(),
                }
            } else {
                object_store::Error::Generic {
                    store: "IoUringObjectStore",
                    source: e.into(),
                }
            };
            let _ = response.send(Err(err));
            return;
        }
    };

    let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(ranges.len());
    for r in &ranges {
        let len = r.end.saturating_sub(r.start);
        match usize::try_from(len) {
            Ok(n) => buffers.push(vec![0u8; n]),
            Err(_) => {
                let _ = response.send(Err(object_store::Error::Generic {
                    store: "IoUringObjectStore",
                    source: format!("range length {len} exceeds usize").into(),
                }));
                return;
            }
        }
    }

    let fd = file.as_raw_fd();
    let n = ranges.len();
    let filled = vec![0usize; n];

    let slot = slab.alloc(PendingRequest {
        _file: file,
        fd,
        ranges,
        buffers,
        filled,
        ranges_remaining: n,
        sqes_outstanding: n,
        response: Some(response),
        errored: false,
    });

    for idx in 0..n as u32 {
        backlog.push((slot, idx));
    }
}

/// Push as many backlog SQEs as fit into the submission queue, then submit.
fn submit_sqes(ring: &mut IoUring, slab: &mut Slab, backlog: &mut Vec<(u32, u32)>) {
    if backlog.is_empty() {
        return;
    }

    // SAFETY: Each `buffers[i]` lives inside `slab.slots[slot]`, which is
    // not freed until the matching completion is processed by
    // `drain_completions`. The raw pointer and length are valid for the
    // lifetime of every SQE we push here.
    unsafe {
        let mut sq = ring.submission();
        let mut i = 0;
        while i < backlog.len() {
            let (slot, range_idx) = backlog[i];
            let Some(req) = slab.get_mut(slot) else {
                // Slot was freed early (error path) — just skip.
                i += 1;
                continue;
            };
            let idx = range_idx as usize;
            let remaining_bytes = req.buffers[idx].len() - req.filled[idx];
            debug_assert!(remaining_bytes > 0, "queued SQE for already-full range");
            let ptr = req.buffers[idx].as_mut_ptr().add(req.filled[idx]);
            let entry = opcode::Read::new(types::Fd(req.fd), ptr, remaining_bytes as u32)
                .offset(req.ranges[idx].start + req.filled[idx] as u64)
                .build()
                .user_data(pack_user_data(slot, range_idx));

            if sq.push(&entry).is_err() {
                // SQ full — keep remaining in the backlog for the next
                // iteration.
                break;
            }
            i += 1;
        }
        sq.sync();

        // Drop the submitted entries from the backlog.
        if i > 0 {
            backlog.drain(0..i);
        }
    }

    if let Err(e) = ring.submit() {
        fail_all(slab, &format!("io_uring submit failed: {e}"));
    }
}

/// Consume all currently-available CQEs and dispatch them to their owning
/// requests. Short reads re-queue a follow-up SQE onto `backlog`.
fn drain_completions(
    ring: &mut IoUring,
    slab: &mut Slab,
    backlog: &mut Vec<(u32, u32)>,
    _sq_capacity: usize,
) {
    let cq = ring.completion();
    for cqe in cq {
        let (slot, range_idx) = unpack_user_data(cqe.user_data());
        let idx = range_idx as usize;

        let Some(req) = slab.get_mut(slot) else {
            // Slot was freed — the request completed (or errored) and all
            // its SQEs were already accounted for. Ignore.
            continue;
        };

        // Every CQE represents one SQE that is no longer in flight.
        debug_assert!(req.sqes_outstanding > 0);
        req.sqes_outstanding -= 1;

        if req.errored || req.response.is_none() {
            // A previous CQE already dispatched a terminal result for this
            // request; just track the SQE count down.
            maybe_free(slab, slot);
            continue;
        }

        if idx >= req.buffers.len() {
            let err = object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: format!(
                    "io_uring cqe with invalid user_data ({slot}, {range_idx})"
                )
                .into(),
            };
            finish_with_error(slab, slot, err);
            continue;
        }

        let ret = cqe.result();
        if ret < 0 {
            let err = object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: std::io::Error::from_raw_os_error(-ret).into(),
            };
            finish_with_error(slab, slot, err);
            continue;
        }

        let bytes_read = ret as usize;
        if bytes_read == 0 {
            let range = req.ranges[idx].clone();
            let err = object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: format!("unexpected EOF reading {}..{}", range.start, range.end)
                    .into(),
            };
            finish_with_error(slab, slot, err);
            continue;
        }

        req.filled[idx] += bytes_read;
        if req.filled[idx] < req.buffers[idx].len() {
            // Partial read — submit a follow-up SQE for the remainder.
            backlog.push((slot, range_idx));
            req.sqes_outstanding += 1;
            continue;
        }

        // This range is complete.
        req.ranges_remaining -= 1;
        if req.ranges_remaining == 0 {
            finish_ok(slab, slot);
        }
    }
}

fn finish_ok(slab: &mut Slab, slot: u32) {
    let Some(req) = slab.slots[slot as usize].as_mut() else {
        return;
    };
    let buffers = std::mem::take(&mut req.buffers);
    let response = req.response.take();
    if let Some(tx) = response {
        let bytes: Vec<Bytes> = buffers.into_iter().map(Bytes::from).collect();
        let _ = tx.send(Ok(bytes));
    }
    maybe_free(slab, slot);
}

fn finish_with_error(slab: &mut Slab, slot: u32, err: object_store::Error) {
    let Some(req) = slab.slots[slot as usize].as_mut() else {
        return;
    };
    req.errored = true;
    if let Some(tx) = req.response.take() {
        let _ = tx.send(Err(err));
    }
    maybe_free(slab, slot);
}

/// Free the slot iff no SQEs for it remain in the backlog or in the ring.
fn maybe_free(slab: &mut Slab, slot: u32) {
    let Some(req) = slab.slots[slot as usize].as_ref() else {
        return;
    };
    if req.response.is_none() && req.sqes_outstanding == 0 {
        slab.free(slot);
    }
}

fn fail_all(slab: &mut Slab, msg: &str) {
    let n = slab.slots.len();
    for slot in 0..n {
        if slab.slots[slot].is_some() {
            let err = object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: msg.to_string().into(),
            };
            finish_with_error(slab, slot as u32, err);
        }
    }
}
