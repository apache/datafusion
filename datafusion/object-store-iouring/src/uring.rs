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
//! A single worker thread drains an unbounded mpsc of [`IoCommand`]s and
//! pipelines every request's reads through one shared [`IoUring`]. SQE
//! `user_data` packs `(slot_id << 32) | range_idx` so each completion finds
//! its owning request and range. Partial reads re-enqueue a follow-up SQE
//! for the remainder; the per-request [`File`] stays alive until every
//! completion for that slot has been drained.

use std::collections::VecDeque;
use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use bytes::Bytes;
use io_uring::{IoUring, opcode, types};
use object_store::Result;
use tokio::sync::{mpsc, oneshot};

/// Number of submission-queue entries. 256 comfortably absorbs typical
/// Parquet column-chunk batches without spilling to the backlog.
const RING_ENTRIES: u32 = 256;

/// Command sent from the async ObjectStore methods to the io_uring thread.
pub(crate) enum IoCommand {
    ReadRanges {
        path: PathBuf,
        ranges: Vec<Range<u64>>,
        response: oneshot::Sender<Result<Vec<Bytes>>>,
    },
}

/// Per-request state living in the slab while SQEs are in flight.
///
/// The `buffers[i]` [`Vec`] grows as completions arrive — `len()` is the
/// bytes filled so far, `capacity() >= target_len[i]` is the allocation.
/// This avoids the zero-fill cost of `vec![0u8; n]` without touching
/// uninitialized memory from safe code.
struct PendingRequest {
    /// Kept alive so the raw fd stored in in-flight SQEs stays valid.
    _file: std::fs::File,
    fd: i32,
    ranges: Vec<Range<u64>>,
    buffers: Vec<Vec<u8>>,
    target_len: Vec<usize>,
    /// Ranges that still need more bytes read.
    ranges_remaining: u32,
    /// SQEs still owed — either in the backlog or in flight. A slot may
    /// only be freed when this hits zero; otherwise a late CQE could land
    /// on a reallocated slot and corrupt an unrelated request.
    sqes_outstanding: u32,
    /// `None` after the response has been sent (success or error).
    response: Option<oneshot::Sender<Result<Vec<Bytes>>>>,
    errored: bool,
}

#[inline]
fn pack_user_data(slot: u32, range_idx: u32) -> u64 {
    ((slot as u64) << 32) | (range_idx as u64)
}

#[inline]
fn unpack_user_data(user_data: u64) -> (u32, u32) {
    ((user_data >> 32) as u32, user_data as u32)
}

/// Slab of active requests. `slots[i]` is `Some` iff slot `i` is live.
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

    #[inline]
    fn get_mut(&mut self, id: u32) -> Option<&mut PendingRequest> {
        self.slots.get_mut(id as usize).and_then(|s| s.as_mut())
    }
}

/// Main loop for the io_uring worker thread.
///
/// Runs until the command channel is closed and all in-flight requests are
/// drained.
pub(crate) fn run_uring_loop(mut rx: mpsc::UnboundedReceiver<IoCommand>) {
    let mut ring = match IoUring::new(RING_ENTRIES) {
        Ok(ring) => ring,
        Err(e) => {
            log::error!("Failed to create io_uring instance: {e}");
            drain_and_error(&mut rx, &e.to_string());
            return;
        }
    };

    let mut slab = Slab::new();
    // SQEs waiting for room in the ring. `(slot, range_idx)`.
    let mut backlog: VecDeque<(u32, u32)> = VecDeque::new();
    let mut rx_closed = false;

    loop {
        // 1) Pull every command currently waiting. When nothing is in flight
        //    the thread blocks here so the worker idles without spinning.
        while !rx_closed {
            let cmd = if slab.in_flight == 0 && backlog.is_empty() {
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
            accept_command(cmd, &mut slab, &mut backlog);
        }

        // 2) Push as many backlog SQEs into the SQ as will fit.
        push_backlog(&mut ring, &mut slab, &mut backlog);

        if slab.in_flight == 0 {
            if rx_closed {
                break;
            }
            continue;
        }

        // 3) Single syscall that both submits the queued SQEs and waits for
        //    at least one completion.
        if let Err(e) = ring.submit_and_wait(1) {
            fail_all(&mut slab, &format!("io_uring submit failed: {e}"));
            continue;
        }

        // 4) Drain every available CQE; short reads re-queue a follow-up SQE.
        drain_completions(&mut ring, &mut slab, &mut backlog);
    }
}

/// Respond to every queued command with `reason` after init failure.
fn drain_and_error(rx: &mut mpsc::UnboundedReceiver<IoCommand>, reason: &str) {
    while let Some(cmd) = rx.blocking_recv() {
        let IoCommand::ReadRanges { response, .. } = cmd;
        let _ = response.send(Err(object_store::Error::Generic {
            store: "IoUringObjectStore",
            source: format!("io_uring init failed: {reason}").into(),
        }));
    }
}

/// Allocate a slab slot for the incoming command and queue one SQE per range.
/// Errors (missing file, 32-bit overflow) are reported immediately without
/// occupying a slot.
fn accept_command(cmd: IoCommand, slab: &mut Slab, backlog: &mut VecDeque<(u32, u32)>) {
    let IoCommand::ReadRanges {
        path,
        ranges,
        response,
    } = cmd;

    if ranges.is_empty() {
        let _ = response.send(Ok(Vec::new()));
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

    let n = ranges.len();
    let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut target_len: Vec<usize> = Vec::with_capacity(n);
    for r in &ranges {
        let len = r.end.saturating_sub(r.start);
        let Ok(len) = usize::try_from(len) else {
            let _ = response.send(Err(object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: format!("range length {len} exceeds usize").into(),
            }));
            return;
        };
        // Preallocate the full range without zero-filling. Each buffer
        // grows via `set_len` as kernel completions arrive, so the Vec
        // always reflects exactly the bytes initialized so far.
        buffers.push(Vec::with_capacity(len));
        target_len.push(len);
    }

    let fd = file.as_raw_fd();
    let n_u32 = n as u32;
    let slot = slab.alloc(PendingRequest {
        _file: file,
        fd,
        ranges,
        buffers,
        target_len,
        ranges_remaining: n_u32,
        sqes_outstanding: n_u32,
        response: Some(response),
        errored: false,
    });

    backlog.reserve(n);
    for idx in 0..n_u32 {
        backlog.push_back((slot, idx));
    }
}

/// Push backlog SQEs into the submission queue until either the backlog or
/// the SQ is exhausted.
fn push_backlog(ring: &mut IoUring, slab: &mut Slab, backlog: &mut VecDeque<(u32, u32)>) {
    if backlog.is_empty() {
        return;
    }

    // SAFETY: every `buffers[idx]` lives inside the slab slot, which is
    // not freed until the matching completion is processed — so the raw
    // pointer and length remain valid for the lifetime of every SQE.
    unsafe {
        let mut sq = ring.submission();
        while let Some(&(slot, range_idx)) = backlog.front() {
            let Some(req) = slab.get_mut(slot) else {
                // Slot was freed early (error path) — drop the stale SQE.
                backlog.pop_front();
                continue;
            };
            let idx = range_idx as usize;
            let filled = req.buffers[idx].len();
            let remaining = req.target_len[idx] - filled;
            debug_assert!(remaining > 0, "queued SQE for already-full range");

            let ptr = req.buffers[idx].as_mut_ptr().add(filled);
            let entry = opcode::Read::new(types::Fd(req.fd), ptr, remaining as u32)
                .offset(req.ranges[idx].start + filled as u64)
                .build()
                .user_data(pack_user_data(slot, range_idx));

            if sq.push(&entry).is_err() {
                break;
            }
            backlog.pop_front();
        }
        sq.sync();
    }
}

/// Consume every available CQE and route it back to its owning request.
/// Short reads re-queue a follow-up SQE onto the backlog.
fn drain_completions(
    ring: &mut IoUring,
    slab: &mut Slab,
    backlog: &mut VecDeque<(u32, u32)>,
) {
    let cq = ring.completion();
    for cqe in cq {
        let (slot, range_idx) = unpack_user_data(cqe.user_data());
        let idx = range_idx as usize;

        let Some(req) = slab.get_mut(slot) else {
            // Slot freed — the request already terminated and every
            // outstanding SQE was accounted for.
            continue;
        };

        debug_assert!(req.sqes_outstanding > 0);
        req.sqes_outstanding -= 1;

        if req.errored || req.response.is_none() {
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

        // SAFETY: the kernel just wrote `bytes_read` bytes starting at
        // `buf.as_mut_ptr().add(buf.len())` (see `push_backlog`), so
        // advancing `len` by that amount exposes only initialized bytes.
        // `capacity == target_len[idx] >= new_len` since we never queue
        // more than `target_len - filled` for a single SQE.
        let buf = &mut req.buffers[idx];
        let new_len = buf.len() + bytes_read;
        debug_assert!(new_len <= req.target_len[idx]);
        unsafe {
            buf.set_len(new_len);
        }

        if new_len < req.target_len[idx] {
            // Partial read — queue a follow-up for the remainder.
            backlog.push_back((slot, range_idx));
            req.sqes_outstanding += 1;
            continue;
        }

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
    if let Some(tx) = req.response.take() {
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

/// Free the slot iff no SQEs remain in the backlog or in flight.
fn maybe_free(slab: &mut Slab, slot: u32) {
    let Some(req) = slab.slots[slot as usize].as_ref() else {
        return;
    };
    if req.response.is_none() && req.sqes_outstanding == 0 {
        slab.free(slot);
    }
}

fn fail_all(slab: &mut Slab, msg: &str) {
    for slot in 0..slab.slots.len() {
        if slab.slots[slot].is_some() {
            let err = object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: msg.to_string().into(),
            };
            finish_with_error(slab, slot as u32, err);
        }
    }
}
