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
//! Receives [`IoCommand`] requests via an unbounded channel, submits
//! batched read SQEs to the kernel, collects CQEs, and sends results
//! back via oneshot channels.

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

/// Probe whether the kernel supports io_uring (may fail with EPERM in
/// Docker / seccomp-restricted environments).
pub(crate) fn is_available() -> std::result::Result<(), std::io::Error> {
    IoUring::new(2).map(|_| ())
}

/// Command sent from the async ObjectStore methods to the io_uring thread.
pub(crate) enum IoCommand {
    /// Read one or more byte ranges from a file.
    /// All ranges are submitted as a batch in a single `io_uring_enter()`.
    ReadRanges {
        path: PathBuf,
        ranges: Vec<Range<u64>>,
        response: oneshot::Sender<Result<Vec<Bytes>>>,
    },
}

/// Main loop for the io_uring worker thread.
///
/// Blocks on the channel receiver, processes one [`IoCommand`] at a time,
/// and sends results back. The thread exits when the channel is closed
/// (i.e., when all senders are dropped).
pub(crate) fn run_uring_loop(mut rx: mpsc::UnboundedReceiver<IoCommand>) {
    let mut ring = match IoUring::new(RING_ENTRIES) {
        Ok(ring) => ring,
        Err(e) => {
            log::error!("Failed to create io_uring instance: {e}");
            // Drain and error all pending requests
            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    IoCommand::ReadRanges { response, .. } => {
                        let _ = response.send(Err(object_store::Error::Generic {
                            store: "IoUringObjectStore",
                            source: format!("io_uring init failed: {e}").into(),
                        }));
                    }
                }
            }
            return;
        }
    };

    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            IoCommand::ReadRanges {
                path,
                ranges,
                response,
            } => {
                let result = execute_read_ranges(&mut ring, &path, &ranges);
                let _ = response.send(result);
            }
        }
    }

    log::debug!("io_uring worker thread exiting");
}

/// Execute a batch of byte-range reads using io_uring.
///
/// Opens the file, submits all read SQEs (chunked by ring capacity),
/// waits for CQEs, and returns the results in order.
#[allow(clippy::result_large_err)] // object_store::Error is large by design
fn execute_read_ranges(
    ring: &mut IoUring,
    path: &std::path::Path,
    ranges: &[Range<u64>],
) -> Result<Vec<Bytes>> {
    let file = std::fs::File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            object_store::Error::NotFound {
                path: path.display().to_string(),
                source: e.into(),
            }
        } else {
            object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: e.into(),
            }
        }
    })?;

    let fd = file.as_raw_fd();

    // Allocate buffers for all ranges up front
    let mut buffers: Vec<Vec<u8>> = ranges
        .iter()
        .map(|r| vec![0u8; (r.end - r.start) as usize])
        .collect();

    let sq_capacity = ring.params().sq_entries() as usize;

    // Process ranges in chunks that fit the submission queue
    for chunk_start in (0..ranges.len()).step_by(sq_capacity) {
        let chunk_end = (chunk_start + sq_capacity).min(ranges.len());
        let chunk_len = chunk_end - chunk_start;

        // Submit read SQEs for this chunk
        // SAFETY: buffers[i] is valid, properly sized, and lives until
        // we collect the corresponding CQE below.
        unsafe {
            let mut sq = ring.submission();
            for i in chunk_start..chunk_end {
                let entry = opcode::Read::new(
                    types::Fd(fd),
                    buffers[i].as_mut_ptr(),
                    buffers[i].len() as u32,
                )
                .offset(ranges[i].start)
                .build()
                .user_data(i as u64);

                // If the SQ is full (shouldn't happen since we chunk),
                // drop the queue, submit, and retry.
                if sq.push(&entry).is_err() {
                    drop(sq);
                    ring.submit().map_err(io_err)?;
                    sq = ring.submission();
                    sq.push(&entry).expect("SQ should have space after submit");
                }
            }
            sq.sync();
        }

        // Submit and wait for all reads in this chunk to complete
        ring.submit_and_wait(chunk_len).map_err(io_err)?;

        // Collect completions
        let mut completed = 0;
        while completed < chunk_len {
            let cq = ring.completion();
            for cqe in cq {
                let idx = cqe.user_data() as usize;
                let ret = cqe.result();
                if ret < 0 {
                    return Err(io_err(std::io::Error::from_raw_os_error(-ret)));
                }
                let bytes_read = ret as usize;
                buffers[idx].truncate(bytes_read);
                completed += 1;
            }
        }
    }

    // Convert buffers to Bytes
    Ok(buffers.into_iter().map(Bytes::from).collect())
}

fn io_err(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "IoUringObjectStore",
        source: e.into(),
    }
}
