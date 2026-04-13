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

//! Streaming boundary-aligned wrapper for newline-delimited JSON range reads.
//!
//! [`AlignedBoundaryStream`] wraps a raw byte stream and lazily aligns to
//! record (newline) boundaries, avoiding the need for separate `get_opts`
//! calls to locate boundary positions.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::stream::{BoxStream, Stream};
use futures::{StreamExt, TryFutureExt};
use object_store::{GetOptions, GetRange, ObjectStore};

/// How far past `raw_end` the initial bounded fetch covers. If the terminating
/// newline is not found within this window, `ScanningLastTerminator` issues
/// successive same-sized GETs until the newline is located or EOF is reached.
pub const END_SCAN_LOOKAHEAD: u64 = 16 * 1024; // 16 KiB

/// Phase of the boundary alignment state machine.
#[derive(Debug)]
enum Phase {
    /// Scanning for the first newline to align the start boundary.
    ScanningFirstTerminator,
    /// Passing through aligned data, tracking byte position.
    FetchingChunks,
    /// Past the end boundary, scanning for terminating newline.
    ScanningLastTerminator,
    /// Stream is exhausted.
    Done,
}

/// A stream wrapper that lazily aligns byte boundaries to newline characters.
///
/// Given a raw byte stream starting from `fetch_start` (which is `start - 1`
/// for non-zero starts, or `0`), this stream:
///
/// 1. Skips bytes until the first newline is found (start alignment)
/// 2. Passes through data until the `end` boundary is reached
/// 3. Continues past `end` to find the terminating newline (end alignment)
///
/// When the initial byte stream is exhausted during step 3 and the file has
/// not been fully read, `ScanningLastTerminator` issues additional bounded
/// `get_opts` calls (`END_SCAN_LOOKAHEAD` bytes each) until the newline is
/// found or EOF is reached.
pub struct AlignedBoundaryStream {
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    terminator: u8,
    /// Effective end boundary. Set to `u64::MAX` when `end >= file_size`
    /// (last partition), so `FetchingChunks` never transitions to
    /// `ScanningLastTerminator` and simply streams until EOF is reached.
    end: u64,
    /// Cumulative bytes consumed from `inner` (relative to `fetch_start`).
    bytes_consumed: u64,
    /// The offset where the current `inner` stream begins.
    fetch_start: u64,
    phase: Phase,
    /// Remainder bytes from `ScanningFirstTerminator` that still need
    /// end-boundary processing. Consumed by `FetchingChunks` before polling
    /// `inner`.
    pending: Option<Bytes>,
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    /// Total file size; overflow stops when `abs_pos() >= file_size`.
    file_size: u64,
}

/// Fetch a bounded byte range from `store` and return it as a stream
async fn get_stream(
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    range: std::ops::Range<u64>,
) -> object_store::Result<BoxStream<'static, object_store::Result<Bytes>>> {
    let opts = GetOptions {
        range: Some(GetRange::Bounded(range)),
        ..Default::default()
    };
    let result = store.get_opts(&location, opts).await?;
    Ok(result.into_stream())
}

impl AlignedBoundaryStream {
    /// Open a ranged byte stream from `store` and return a ready-to-poll
    /// `AlignedBoundaryStream`.
    ///
    /// Issues a single bounded `get_opts` call covering
    /// `[fetch_start, raw_end + END_SCAN_LOOKAHEAD)`.  If the terminating
    /// newline is not found within that window, `ScanningLastTerminator`
    /// automatically issues additional `END_SCAN_LOOKAHEAD`-sized GETs
    /// via `store` until the newline is found or EOF is reached.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        location: object_store::path::Path,
        raw_start: u64,
        raw_end: u64,
        file_size: u64,
        terminator: u8,
    ) -> object_store::Result<Self> {
        if raw_start >= raw_end || raw_start >= file_size {
            return Ok(Self {
                inner: futures::stream::empty().boxed(),
                terminator,
                end: 0,
                bytes_consumed: 0,
                fetch_start: 0,
                phase: Phase::Done,
                pending: None,
                store,
                location,
                file_size,
            });
        }

        let (fetch_start, phase) = if raw_start == 0 {
            (0, Phase::FetchingChunks)
        } else {
            (raw_start - 1, Phase::ScanningFirstTerminator)
        };

        let initial_fetch_end = raw_end.saturating_add(END_SCAN_LOOKAHEAD).min(file_size);

        let inner = get_stream(
            Arc::clone(&store),
            location.clone(),
            fetch_start..initial_fetch_end,
        )
        .await?;

        // Last partition reads until EOF is reached — no end-boundary scanning needed.
        let end = if raw_end >= file_size {
            u64::MAX
        } else {
            raw_end
        };

        Ok(Self {
            inner,
            terminator,
            end,
            bytes_consumed: 0,
            fetch_start,
            phase,
            pending: None,
            store,
            location,
            file_size,
        })
    }

    /// Current absolute position in the file.
    fn abs_pos(&self) -> u64 {
        self.fetch_start + self.bytes_consumed
    }
}

impl Stream for AlignedBoundaryStream {
    type Item = object_store::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.phase {
                Phase::Done => return Poll::Ready(None),

                Phase::ScanningFirstTerminator => {
                    // Find the first terminator and skip everything up to
                    // and including it. Store any remainder in `pending`
                    // so `FetchingChunks` can apply end-boundary logic to it.
                    match this.inner.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => {
                            this.phase = Phase::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.phase = Phase::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(Some(Ok(chunk))) => {
                            this.bytes_consumed += chunk.len() as u64;
                            match chunk.iter().position(|&b| b == this.terminator) {
                                Some(pos) => {
                                    let remainder = chunk.slice((pos + 1)..);
                                    // The aligned start position is where
                                    // data begins after the newline.
                                    let aligned_start =
                                        this.abs_pos() - remainder.len() as u64;
                                    if aligned_start >= this.end {
                                        // Start alignment landed at or past
                                        // the end boundary — no complete
                                        // lines in this partition's range.
                                        this.phase = Phase::Done;
                                        return Poll::Ready(None);
                                    }
                                    if !remainder.is_empty() {
                                        this.pending = Some(remainder);
                                    }
                                    this.phase = Phase::FetchingChunks;
                                    continue;
                                }
                                None => continue,
                            }
                        }
                    }
                }

                Phase::FetchingChunks => {
                    // Get the next chunk: pending remainder or inner stream.
                    let chunk = if let Some(pending) = this.pending.take() {
                        pending
                    } else {
                        match this.inner.poll_next_unpin(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(None) => {
                                this.phase = Phase::Done;
                                return Poll::Ready(None);
                            }
                            Poll::Ready(Some(Err(e))) => {
                                this.phase = Phase::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Ready(Some(Ok(chunk))) => {
                                this.bytes_consumed += chunk.len() as u64;
                                chunk
                            }
                        }
                    };

                    let pos_after = this.abs_pos();

                    // When end == u64::MAX (last partition), this is always
                    // true and we stream straight through until EOF is reached.
                    if pos_after < this.end {
                        return Poll::Ready(Some(Ok(chunk)));
                    }

                    if pos_after == this.end {
                        // Chunk ends exactly at the boundary.
                        if chunk.last() == Some(&this.terminator) {
                            this.phase = Phase::Done;
                        } else {
                            // No terminator at boundary; any following data
                            // is past end, so switch to end-scanning.
                            this.phase = Phase::ScanningLastTerminator;
                        }
                        return Poll::Ready(Some(Ok(chunk)));
                    }

                    // Chunk crosses the end boundary (`pos_after > this.end`).
                    // Find the first terminator at or after file position
                    // `this.end - 1` and yield everything up to and
                    // including it.
                    //
                    // `pos_before` is the absolute file position of chunk[0].
                    // `chunk_in_range_len` is how many bytes of this chunk
                    // fall within [pos_before, this.end), so chunk[0..
                    // chunk_in_range_len] is the in-range portion.
                    // `search_from` is the chunk index of the last in-range
                    // byte (file position this.end - 1).
                    //
                    // Example A: "line1\nline2\nline3\n" (18 bytes), end=8,
                    // one large chunk arriving with pos_after=18:
                    //   pos_before         = 18 - 18 = 0
                    //   chunk_in_range_len =  8 -  0 = 8
                    //   search_from        = 7   (chunk[7] is file pos 7)
                    //   chunk[7]='i', chunk[11]='\n' → rel=4
                    //   yield chunk[..7+4+1] = chunk[..12] = "line1\nline2\n"
                    //
                    // Example B: same data, 3-byte chunks, end=8.
                    // "lin"(pos 0-2) and "e1\n"(pos 3-5) yielded already.
                    // Now chunk="lin" arrives with pos_after=9:
                    //   pos_before         = 9 - 3 = 6
                    //   chunk_in_range_len = 8 - 6 = 2
                    //   search_from        = 1   (chunk[1] is file pos 7)
                    //   chunk[1]='i', no '\n' in chunk[1..] → EndScan
                    let pos_before = pos_after - chunk.len() as u64;
                    let chunk_in_range_len = (this.end - pos_before) as usize;
                    let search_from = chunk_in_range_len - 1;
                    if let Some(rel) = chunk[search_from..]
                        .iter()
                        .position(|&b| b == this.terminator)
                    {
                        this.phase = Phase::Done;
                        return Poll::Ready(Some(Ok(
                            chunk.slice(..search_from + rel + 1)
                        )));
                    }

                    // No terminator found; continue scanning in EndScan.
                    this.phase = Phase::ScanningLastTerminator;
                    return Poll::Ready(Some(Ok(chunk)));
                }

                Phase::ScanningLastTerminator => {
                    match this.inner.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => {
                            // Inner exhausted. Issue the next overflow GET if
                            // the file has not been fully read yet.
                            let pos = this.abs_pos();
                            if pos < this.file_size {
                                let fetch_end = pos
                                    .saturating_add(END_SCAN_LOOKAHEAD)
                                    .min(this.file_size);
                                let store = Arc::clone(&this.store);
                                let location = this.location.clone();
                                this.inner = get_stream(store, location, pos..fetch_end)
                                    .try_flatten_stream()
                                    .boxed();
                                continue;
                            }
                            this.phase = Phase::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.phase = Phase::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(Some(Ok(chunk))) => {
                            this.bytes_consumed += chunk.len() as u64;
                            if let Some(pos) =
                                chunk.iter().position(|&b| b == this.terminator)
                            {
                                this.phase = Phase::Done;
                                return Poll::Ready(Some(Ok(chunk.slice(..pos + 1))));
                            }
                            // No terminator yet; yield and keep scanning.
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{CHUNK_SIZES, make_chunked_store};
    use futures::TryStreamExt;

    async fn collect_stream(stream: AlignedBoundaryStream) -> Vec<u8> {
        stream.try_collect::<Vec<Bytes>>().await.unwrap().concat()
    }

    #[tokio::test]
    async fn test_start_at_zero_no_end_scan() {
        // start=0, end >= file_size → pass through everything
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 0, 100, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(collect_stream(s).await, DATA, "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_start_aligned_on_newline() {
        // Data: "line1\nline2\nline3\n"
        //        0    5 6   11 12  17
        // start=6 → fetch_start=5. Byte at offset 5 is '\n'.
        // Should skip the leading '\n' and yield "line2\nline3\n".
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 6, 100, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(
                collect_stream(s).await,
                b"line2\nline3\n",
                "chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_start_mid_line() {
        // start=3, fetch_start=2. Bytes from offset 2: "ne1\nline2\nline3\n".
        // Should skip "ne1\n" and yield "line2\nline3\n".
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 3, 100, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(
                collect_stream(s).await,
                b"line2\nline3\n",
                "chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_end_boundary_mid_line() {
        // Data: "line1\nline2\nline3\n"
        //        0    5 6   11 12  17
        // start=0, end=8. End is mid "line2".
        // Should yield "line1\nline2\n" (continue past end to find newline).
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 0, 8, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(
                collect_stream(s).await,
                b"line1\nline2\n",
                "chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_end_at_eof() {
        // end >= file_size → no end scanning, pass through everything.
        static DATA: &[u8] = b"line1\nline2\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 0, 12, 12, b'\n')
                .await
                .unwrap();
            assert_eq!(collect_stream(s).await, DATA, "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_no_newline_in_range() {
        // start=2, fetch_start=1. Bytes from offset 1: "bcdef" — no newline.
        // No complete line → empty output.
        static DATA: &[u8] = b"abcdef";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 2, 6, 6, b'\n')
                .await
                .unwrap();
            assert!(collect_stream(s).await.is_empty(), "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_start_and_end_alignment() {
        // Data: "line1\nline2\nline3\nline4\n"
        //        0    5 6   11 12  17 18  23
        // start=3, end=14, file_size=24
        // fetch_start=2, bytes from offset 2: "ne1\nline2\nline3\nline4\n"
        // Start aligns past "ne1\n"; end=14 is mid "line3", scan to '\n'.
        // Expected: "line2\nline3\n"
        static DATA: &[u8] = b"line1\nline2\nline3\nline4\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 3, 14, 24, b'\n')
                .await
                .unwrap();
            assert_eq!(
                collect_stream(s).await,
                b"line2\nline3\n",
                "chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_end_scan_across_chunks() {
        // end boundary falls before a newline; the terminating newline must be
        // found by scanning past the end in subsequent chunks.
        // Data: "line1\nline2\nline3\n" (18 bytes)
        // start=0, end=7 (mid "line2"), file_size=18 → "line1\nline2\n"
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 0, 7, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(
                collect_stream(s).await,
                b"line1\nline2\n",
                "chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_empty_range() {
        // start >= end — no complete line can exist, regardless of data.
        static DATA: &[u8] = b"line1\nline2\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;

            // start > end (non-zero start)
            let s = AlignedBoundaryStream::new(
                Arc::clone(&store),
                path.clone(),
                10,
                5,
                20,
                b'\n',
            )
            .await
            .unwrap();
            assert!(
                collect_stream(s).await.is_empty(),
                "start>end chunk_size={cs}"
            );

            // start == end == 0 (zero start, previously unguarded)
            let s = AlignedBoundaryStream::new(
                Arc::clone(&store),
                path.clone(),
                0,
                0,
                12,
                b'\n',
            )
            .await
            .unwrap();
            assert!(
                collect_stream(s).await.is_empty(),
                "start==end==0 chunk_size={cs}"
            );

            // start == end (non-zero)
            let s = AlignedBoundaryStream::new(
                Arc::clone(&store),
                path.clone(),
                6,
                6,
                12,
                b'\n',
            )
            .await
            .unwrap();
            assert!(
                collect_stream(s).await.is_empty(),
                "start==end==6 chunk_size={cs}"
            );
        }
    }

    #[tokio::test]
    async fn test_start_align_across_chunks() {
        // The newline needed for start alignment may arrive in any chunk.
        // fetch_start=0 (start=1). Data: "abcdef\nline2\n" (13 bytes)
        // Start aligns past "abcdef\n", yielding "line2\n".
        static DATA: &[u8] = b"abcdef\nline2\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 1, 100, 13, b'\n')
                .await
                .unwrap();
            assert_eq!(collect_stream(s).await, b"line2\n", "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_end_aligned_on_newline() {
        // end falls right on a newline — line is complete, no end-scan needed.
        // Data: "line1\nline2\nline3\n"
        //        0    5 6   11 12  17
        // start=0, end=6 → byte 5 is '\n' → yield only "line1\n".
        static DATA: &[u8] = b"line1\nline2\nline3\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 0, 6, 18, b'\n')
                .await
                .unwrap();
            assert_eq!(collect_stream(s).await, b"line1\n", "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_adjacent_partitions_no_overlap() {
        // Three adjacent partitions over "line1\nline2\nline3\n".
        // Partition 1: [0, 6), fetch_start=0  → stream full file
        // Partition 2: [6, 12), fetch_start=5 → stream from offset 5
        // Partition 3: [12, 18), fetch_start=11 → stream from offset 11
        static DATA: &[u8] = b"line1\nline2\nline3\n"; // 18 bytes

        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let r1 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    0,
                    6,
                    18,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;
            let r2 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    6,
                    12,
                    18,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;
            let r3 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    12,
                    18,
                    18,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            assert_eq!(r1, b"line1\n", "p1 chunk_size={cs}");
            assert_eq!(r2, b"line2\n", "p2 chunk_size={cs}");
            assert_eq!(r3, b"line3\n", "p3 chunk_size={cs}");

            let mut combined = r1;
            combined.extend(r2);
            combined.extend(r3);
            assert_eq!(combined, DATA, "combined chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_start_align_past_end_returns_empty() {
        // The first aligned start lands at or past the end boundary.
        // Data: "abcdefghij\nkl\n" (14 bytes)
        //        0         10 11 13
        // Partition [3, 6): start=3, end=6, fetch_start=2
        // Bytes from offset 2: "cdefghij\nkl\n". First '\n' at offset 10;
        // aligned start = 11, which is >= end = 6 → empty.
        static DATA: &[u8] = b"abcdefghij\nkl\n";
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;
            let s = AlignedBoundaryStream::new(store, path, 3, 6, 14, b'\n')
                .await
                .unwrap();
            assert!(collect_stream(s).await.is_empty(), "chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_unaligned_partitions_no_overlap() {
        // Partitions that don't fall on line boundaries.
        // Data: "aaa\nbbb\nccc\n" (12 bytes)
        //        0  3 4  7 8  11
        // Partitions: [0, 5), [5, 10), [10, 12)
        static DATA: &[u8] = b"aaa\nbbb\nccc\n"; // 12 bytes

        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;

            // [0, 5): no start alignment; end=5 mid "bbb", scans to '\n' at 7.
            let r1 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    0,
                    5,
                    12,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            // [5, 10): fetch_start=4, bytes from offset 4: "bbb\nccc\n".
            // '\n' at pos 3 → aligned start=8 ("ccc\n"). End=10 mid "ccc",
            // scans to '\n' at 11 → yields "ccc\n".
            let r2 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    5,
                    10,
                    12,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            // [10, 12): fetch_start=9, bytes from offset 9: "cc\n".
            // '\n' at pos 2 → aligned start=12. end=12==file_size → end=MAX.
            // Remainder after '\n' is empty; Passthrough polls inner → Done.
            let r3 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    10,
                    12,
                    12,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            assert_eq!(r1, b"aaa\nbbb\n", "p1 chunk_size={cs}");
            assert_eq!(r2, b"ccc\n", "p2 chunk_size={cs}");
            assert!(r3.is_empty(), "p3 chunk_size={cs}");

            let mut combined = r1;
            combined.extend(r2);
            combined.extend(r3);
            assert_eq!(combined, DATA, "combined chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_no_trailing_newline() {
        // Last partition of a file that does not end with a newline.
        // end >= file_size → this.end = u64::MAX, so Passthrough streams straight
        // until EOF is reached and yields the final incomplete line as-is.
        static DATA: &[u8] = b"line1\nline2"; // 11 bytes, no trailing '\n'
        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(DATA, cs).await;

            // Single partition covering the whole file.
            let s = AlignedBoundaryStream::new(
                Arc::clone(&store),
                path.clone(),
                0,
                11,
                11,
                b'\n',
            )
            .await
            .unwrap();
            assert_eq!(collect_stream(s).await, DATA, "chunk_size={cs}");

            // Last partition starting mid-file (start=6, fetch_start=5).
            // Bytes from offset 5: "\nline2".
            // StartAlign consumes '\n', remainder "line2" is yielded as-is.
            let s = AlignedBoundaryStream::new(
                Arc::clone(&store),
                path.clone(),
                6,
                11,
                11,
                b'\n',
            )
            .await
            .unwrap();
            assert_eq!(collect_stream(s).await, b"line2", "tail chunk_size={cs}");
        }
    }

    #[tokio::test]
    async fn test_overflow_fetch() {
        // First line is longer than 2 * END_SCAN_LOOKAHEAD so the initial
        // bounded fetch [fetch_start, raw_end + END_SCAN_LOOKAHEAD) does not
        // reach its newline.  ScanningLastTerminator must issue overflow GETs
        // to find it.
        //
        // Partition [0, 1): raw_end=1, initial_fetch_end=1+16384=16385.
        // The newline is at byte 32768 > 16385 → one overflow GET required.
        // Partition [1, file_size): start=1 lands mid line-1; ScanningFirstTerminator
        // skips to byte 32769, then yields "line2\nline3\n".
        let long_line: Vec<u8> =
            std::iter::repeat_n(b'A', 2 * END_SCAN_LOOKAHEAD as usize)
                .chain(std::iter::once(b'\n'))
                .collect();
        let rest = b"line2\nline3\n";
        let mut data = long_line.clone();
        data.extend_from_slice(rest);
        let file_size = data.len() as u64;

        for &cs in CHUNK_SIZES {
            let (store, path) = make_chunked_store(&data, cs).await;

            let r1 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    0,
                    1,
                    file_size,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            let r2 = collect_stream(
                AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    path.clone(),
                    1,
                    file_size,
                    file_size,
                    b'\n',
                )
                .await
                .unwrap(),
            )
            .await;

            assert_eq!(r1, long_line, "p1 chunk_size={cs}");
            assert_eq!(r2, rest.as_slice(), "p2 chunk_size={cs}");

            let mut combined = r1;
            combined.extend(r2);
            assert_eq!(combined, data, "combined chunk_size={cs}");
        }
    }
}
