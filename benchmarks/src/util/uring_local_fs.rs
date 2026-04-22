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

//! io_uring-backed [`ObjectStore`] for local files (Linux only).
//!
//! A dedicated driver thread owns the io_uring instance. `ObjectStore`
//! callers enqueue [`IORING_OP_READ`] requests via an unbounded mpsc and
//! await completion on a per-request [`oneshot`].
//!
//! - Non-read operations (`put`, `list`, `head`, `copy`, …) are delegated
//!   verbatim to the inner [`LocalFileSystem`]; only `get_opts` with a
//!   byte range and `get_ranges` go through io_uring.
//! - Each `get_ranges` call performs one `open(2)` and submits all N
//!   ranges concurrently; the kernel services them with whatever queue
//!   depth the underlying device supports.
//! - The file descriptor is closed when the Vec of completion results is
//!   collected. io_uring operations hold a ref to the [`std::fs::File`]
//!   for their lifetime so this is race-free.
//!
//! Rough + incomplete: no fd caching, no registered buffers, no
//! IORING_OP_READV, no cancellation, no metrics. Good enough for a
//! ClickBench A/B comparison.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use io_uring::{IoUring, opcode, types};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as OSResult,
};
use object_store::{Error as OSError, local::LocalFileSystem};
use tokio::sync::{mpsc, oneshot};

/// Default submission/completion queue depth. Must be a power of two.
const RING_ENTRIES: u32 = 256;

/// Convert an `object_store::Path` to an absolute filesystem path. This
/// mirrors `LocalFileSystem::new()` (root at `/`). Callers that need a
/// different root can keep using the inner store for non-read ops.
fn to_fs_path(location: &Path) -> PathBuf {
    let mut p = PathBuf::from("/");
    for part in location.parts() {
        p.push(part.as_ref());
    }
    p
}

/// Command sent from any tokio task to the io_uring driver thread.
enum Cmd {
    Read {
        /// Keeps the file open for the duration of the submission. We
        /// hold the `Arc<File>` inside the `InFlight` entry below rather
        /// than just the raw fd, so the fd cannot be closed concurrently
        /// from a dropped future while the kernel still holds a
        /// submission referencing it.
        file: Arc<File>,
        offset: u64,
        /// Owned heap buffer the kernel writes into. Kept alive until
        /// the CQ entry for this op is processed.
        buf: Box<[u8]>,
        resp: oneshot::Sender<io::Result<Bytes>>,
    },
    Shutdown,
}

/// Per-submission state retained until the CQ entry arrives. Holding
/// `file` and `buf` here (rather than on the stack of the calling task)
/// guarantees the kernel's backing memory stays valid.
struct InFlight {
    #[allow(dead_code)]
    file: Arc<File>,
    buf: Box<[u8]>,
    resp: oneshot::Sender<io::Result<Bytes>>,
}

/// Shared handle to the driver thread. Cheap to clone (`Arc`).
#[derive(Clone, Debug)]
pub struct UringSubmitter {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl UringSubmitter {
    /// Spawn the driver thread and return a handle. The thread runs
    /// until the last `UringSubmitter` clone is dropped (no senders
    /// left) or until explicit `shutdown()`.
    pub fn spawn() -> io::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        // Build the ring on the caller thread so a construction error
        // surfaces synchronously. Move it into the driver after.
        let ring = IoUring::new(RING_ENTRIES)?;
        thread::Builder::new()
            .name("io-uring-driver".into())
            .spawn(move || driver_loop(ring, rx))?;
        Ok(Self { tx })
    }

    /// Enqueue an `IORING_OP_READ` synchronously (channel send only) and
    /// return the `oneshot::Receiver` the caller can await. Submitting
    /// eagerly (rather than inside an `async fn`) guarantees that when
    /// `get_ranges` submits N reads and then awaits them, all N are in
    /// the driver's mpsc at once — the driver sees them as a single
    /// SQ-fill batch and dispatches them concurrently to the kernel.
    fn submit_read(
        &self,
        file: Arc<File>,
        offset: u64,
        len: usize,
    ) -> io::Result<oneshot::Receiver<io::Result<Bytes>>> {
        let (resp, rx) = oneshot::channel();
        let buf = vec![0u8; len].into_boxed_slice();
        self.tx
            .send(Cmd::Read {
                file,
                offset,
                buf,
                resp,
            })
            .map_err(|_| broken_pipe("io_uring driver gone"))?;
        Ok(rx)
    }

    /// Convenience: request a graceful shutdown and let the driver
    /// thread exit on the next loop iteration. Not used by the
    /// ObjectStore path; here for completeness / test usage.
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown);
    }
}

fn broken_pipe(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, msg)
}

/// The driver loop. Runs on its own OS thread (no tokio runtime).
///
/// Each iteration:
///   1. Drain as many commands as we have SQ capacity for (non-blocking
///      recv until the SQ is full or the channel is empty).
///   2. `submit_and_wait(n)` where `n = 1 if in_flight else 0`; this
///      both kicks the kernel on newly-pushed SQEs and blocks for at
///      least one completion when we still have inflight ops.
///   3. Drain the CQ and fire oneshots.
///   4. If we have nothing at all in flight and no pending commands,
///      `blocking_recv()` for the next command.
fn driver_loop(mut ring: IoUring, mut rx: mpsc::UnboundedReceiver<Cmd>) {
    let mut in_flight: HashMap<u64, InFlight> = HashMap::with_capacity(RING_ENTRIES as usize);
    let mut next_id: u64 = 0;
    let mut shutdown_requested = false;

    'outer: loop {
        // 1. Drain pending commands into the SQ (up to free SQ slots).
        //    If the channel is empty and we have no in-flight work, we
        //    block for one command so the driver doesn't spin.
        loop {
            let free_slots = (RING_ENTRIES as usize).saturating_sub(in_flight.len());
            if free_slots == 0 {
                break;
            }

            let cmd = if in_flight.is_empty() && !shutdown_requested {
                match rx.blocking_recv() {
                    Some(c) => c,
                    None => break 'outer, // senders gone
                }
            } else {
                match rx.try_recv() {
                    Ok(c) => c,
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Drain remaining in-flight ops then exit.
                        shutdown_requested = true;
                        break;
                    }
                }
            };

            match cmd {
                Cmd::Shutdown => {
                    shutdown_requested = true;
                    // Still let any in-flight ops complete before exit.
                    if in_flight.is_empty() {
                        break 'outer;
                    }
                }
                Cmd::Read {
                    file,
                    offset,
                    mut buf,
                    resp,
                } => {
                    let id = next_id;
                    next_id = next_id.wrapping_add(1);

                    let fd = file.as_raw_fd();
                    let ptr = buf.as_mut_ptr();
                    let len = buf.len() as u32;

                    let sqe = opcode::Read::new(types::Fd(fd), ptr, len)
                        .offset(offset)
                        .build()
                        .user_data(id);

                    // SAFETY: `file` and `buf` are stashed in `in_flight` for
                    // the lifetime of this submission, keeping the fd open and
                    // the buffer allocation live until the CQ entry arrives.
                    unsafe {
                        // If SQ is full, break out to submit+drain first.
                        if ring.submission().push(&sqe).is_err() {
                            // Put the command back — regrettably the mpsc
                            // doesn't support that, so we push *this one*
                            // directly into in_flight-less state: drop it
                            // with an error. In practice this branch is
                            // unreachable because we checked `free_slots`
                            // above. Leaving defensive handling in case
                            // the SQ capacity differs from RING_ENTRIES.
                            let _ = resp.send(Err(io::Error::other(
                                "io_uring submission queue unexpectedly full",
                            )));
                            break;
                        }
                    }
                    in_flight.insert(id, InFlight { file, buf, resp });
                }
            }
        }

        // 2. Submit queued SQEs and wait for at least one completion
        //    (if we actually have work outstanding).
        let wait_for = if in_flight.is_empty() { 0 } else { 1 };
        if let Err(e) = ring.submit_and_wait(wait_for) {
            // Fatal — fail every inflight op so no caller hangs.
            for (_, inf) in in_flight.drain() {
                let _ = inf.resp.send(Err(io::Error::new(e.kind(), e.to_string())));
            }
            break 'outer;
        }

        // 3. Drain CQ entries and fire responses.
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let id = cqe.user_data();
            let Some(mut inf) = in_flight.remove(&id) else {
                continue;
            };
            let ret = cqe.result();
            let result = if ret < 0 {
                Err(io::Error::from_raw_os_error(-ret))
            } else {
                let n = ret as usize;
                let mut v = std::mem::take(&mut inf.buf).into_vec();
                v.truncate(n);
                Ok(Bytes::from(v))
            };
            let _ = inf.resp.send(result);
        }
        drop(cq);

        // 4. Exit condition: shutdown requested and everything drained.
        if shutdown_requested && in_flight.is_empty() {
            break 'outer;
        }
    }
}

/// `ObjectStore` wrapper: io_uring for reads, [`LocalFileSystem`] for
/// everything else.
#[derive(Debug)]
pub struct UringLocalFileSystem {
    inner: LocalFileSystem,
    submitter: UringSubmitter,
}

impl UringLocalFileSystem {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            inner: LocalFileSystem::new(),
            submitter: UringSubmitter::spawn()?,
        })
    }

    async fn read_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> OSResult<Vec<Bytes>> {
        let fs_path = to_fs_path(location);
        let file = Arc::new(
            File::open(&fs_path).map_err(|e| map_io_err(e, location, "open"))?,
        );

        // Synchronously enqueue all N submissions — this gets them into
        // the driver's mpsc in one burst so the driver can push them to
        // the SQ together and issue a single submit_and_wait covering
        // all N. Then we await the receivers in the original order so
        // the returned Vec aligns with `ranges`.
        let mut rxs = Vec::with_capacity(ranges.len());
        for r in ranges {
            let len = (r.end - r.start) as usize;
            let rx = self
                .submitter
                .submit_read(Arc::clone(&file), r.start, len)
                .map_err(|e| map_io_err(e, location, "uring submit"))?;
            rxs.push(rx);
        }

        let mut out = Vec::with_capacity(rxs.len());
        for (i, rx) in rxs.into_iter().enumerate() {
            let res = rx
                .await
                .map_err(|_| map_io_err(broken_pipe("driver dropped response"), location, "uring await"))?;
            let bytes = res.map_err(|e| map_io_err(e, location, "uring read"))?;
            let want = (ranges[i].end - ranges[i].start) as usize;
            if bytes.len() != want {
                return Err(OSError::Generic {
                    store: "UringLocalFileSystem",
                    source: format!(
                        "short read: requested {} bytes, got {}",
                        want,
                        bytes.len()
                    )
                    .into(),
                });
            }
            out.push(bytes);
        }
        // `file` drops here once every submission's Arc<File> clone has
        // been retired (the driver releases each one when it fires the
        // corresponding oneshot above).
        Ok(out)
    }
}

impl std::fmt::Display for UringLocalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UringLocalFileSystem({})", self.inner)
    }
}

fn map_io_err(e: io::Error, location: &Path, op: &'static str) -> OSError {
    OSError::Generic {
        store: "UringLocalFileSystem",
        source: format!("{op} failed for {location}: {e}").into(),
    }
}

#[async_trait]
impl ObjectStore for UringLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> OSResult<GetResult> {
        // HEAD and full-file GETs fall through to the inner impl. Only
        // bounded-range GETs go through io_uring. Suffix/offset ranges
        // require knowing the file size first; also via inner.
        let Some(range) = options.range.clone() else {
            return self.inner.get_opts(location, options).await;
        };
        let GetRange::Bounded(r) = range else {
            return self.inner.get_opts(location, options).await;
        };

        // Pull the ObjectMeta through inner (HEAD) so we can build an
        // accurate GetResult; this is one extra syscall but keeps the
        // surface consistent.
        let meta_opts = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        let meta = self.inner.get_opts(location, meta_opts).await?.meta;

        let bytes = self
            .read_ranges(location, std::slice::from_ref(&r))
            .await?
            .pop()
            .expect("exactly one range requested");

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range: r,
            attributes: Default::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> OSResult<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(vec![]);
        }
        self.read_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> OSResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> OSResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// Pull `futures::StreamExt` into scope for the `.boxed()` call used in
// `get_opts` above. Isolated here so we don't pollute the top-level
// imports with a trait we only need once.
use futures::StreamExt as _;
