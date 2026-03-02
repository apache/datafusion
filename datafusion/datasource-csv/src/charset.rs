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

use std::fmt::Debug;
use std::io::{BufRead, Read};

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::decoder::Decoder;
use encoding_rs::{CoderResult, Encoding, UTF_8};

use self::buffer::Buffer;

/// Default capacity of the buffer used to decode non-UTF-8 charset streams
static DECODE_BUFFER_CAP: usize = 8 * 1024;

pub fn lookup_charset(enc: Option<&str>) -> Result<Option<&'static Encoding>> {
    match enc {
        Some(enc) => match Encoding::for_label(enc.as_bytes()) {
            Some(enc) => Ok(Some(enc).filter(|enc| *enc != UTF_8)),
            None => Err(DataFusionError::Configuration(format!(
                "Unknown character set '{enc}'"
            )))?,
        },
        None => Ok(None),
    }
}

/// A record batch `Decoder` that decodes input bytes from the specified
/// character encoding to UTF-8 before passing them onto the inner `Decoder`.
#[derive(Debug)]
pub struct CharsetBatchDecoder<T> {
    inner: T,
    decoder: CharsetDecoder,
}

impl<T> CharsetBatchDecoder<T> {
    pub fn new(inner: T, encoding: &'static Encoding) -> Self {
        let decoder = CharsetDecoder::new(encoding);
        Self { inner, decoder }
    }
}

impl<T: Decoder> Decoder for CharsetBatchDecoder<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let last = buf.is_empty();
        let mut buf_offset = 0;

        if !self.decoder.is_empty() {
            let decoded = self.inner.decode(self.decoder.read())?;
            self.decoder.consume(decoded);

            if decoded == 0 {
                return Ok(buf_offset);
            }
        }

        loop {
            let (read, input_empty) = self.decoder.fill(&buf[buf_offset..], last);
            buf_offset += read;

            let decoded = self.inner.decode(self.decoder.read())?;
            self.decoder.consume(decoded);

            if input_empty || decoded == 0 {
                break;
            }
        }

        Ok(buf_offset)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        self.inner.can_flush_early()
    }
}

/// A `BufRead` adapter that decodes input bytes from the
/// specified character encoding to UTF-8.
#[derive(Debug)]
pub struct CharsetReader<R> {
    inner: R,
    decoder: CharsetDecoder,
}

impl<R: BufRead> CharsetReader<R> {
    pub fn new(inner: R, encoding: &'static Encoding) -> Self {
        let decoder = CharsetDecoder::new(encoding);
        Self { inner, decoder }
    }
}

impl<R: BufRead> Read for CharsetReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let src = self.fill_buf()?;
        let len = src.len().min(buf.len());
        buf[..len].copy_from_slice(&src[..len]);
        Ok(len)
    }
}

impl<R: BufRead> BufRead for CharsetReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.decoder.needs_input() {
            let buf = self.inner.fill_buf()?;
            let (read, _) = self.decoder.fill(buf, buf.is_empty());
            self.inner.consume(read);
        }

        Ok(self.decoder.read())
    }

    fn consume(&mut self, amount: usize) {
        self.decoder.consume(amount);
    }
}

/// Converts bytes from some character encoding to UTF-8,
/// using an internal fixed-size buffer
pub struct CharsetDecoder {
    charset_decoder: encoding_rs::Decoder,
    buffer: Buffer,
    finished: bool,
}

impl CharsetDecoder {
    /// Creates a new `CharsetDecoder`.
    fn new(encoding: &'static Encoding) -> Self {
        Self {
            charset_decoder: encoding.new_decoder(),
            buffer: Buffer::with_capacity(DECODE_BUFFER_CAP),
            finished: false,
        }
    }

    /// Returns `true` if the internal buffer is empty.
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns `true` if the decoder needs more input to make progress.
    fn needs_input(&self) -> bool {
        !self.finished && self.buffer.is_empty()
    }

    /// Fills the internal buffer by decoding the provided bytes, returning
    /// the number of bytes consumed and whether the input was exhausted.
    fn fill(&mut self, src: &[u8], last: bool) -> (usize, bool) {
        if self.finished {
            return (0, true);
        }

        self.buffer.backshift();

        let dst = self.buffer.write_buf();
        let (res, read, written, _) = self.charset_decoder.decode_to_utf8(src, dst, last);
        self.buffer.advance(written);

        if last && res == CoderResult::InputEmpty {
            self.finished = true;
        }

        (read, res == CoderResult::InputEmpty)
    }

    /// Returns the unread decoded bytes in the internal buffer.
    fn read(&self) -> &[u8] {
        self.buffer.read_buf()
    }

    /// Marks the given amount of bytes from the internal buffer as having been read.
    /// Subsequent calls to `read` only return bytes that have not been marked as read.
    fn consume(&mut self, amount: usize) {
        self.buffer.consume(amount);
    }
}

impl Debug for CharsetDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CharsetDecoder")
            .field("charset_decoder", self.charset_decoder.encoding())
            .field("buffer", &self.buffer)
            .field("finished", &self.finished)
            .finish()
    }
}

mod buffer {
    /// A fixed-sized buffer that maintains both
    /// a read position and a write position
    #[derive(Debug)]
    pub struct Buffer {
        buf: Box<[u8]>,
        read_ptr: usize,
        write_ptr: usize,
    }

    impl Buffer {
        /// Creates a new `Buffer` with the specified capacity
        #[inline]
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                buf: vec![0; capacity].into_boxed_slice(),
                read_ptr: 0,
                write_ptr: 0,
            }
        }

        /// Whether there are no more bytes available to be read
        pub fn is_empty(&self) -> bool {
            self.read_ptr == self.write_ptr
        }

        /// Returns the unread portion of the buffer
        pub fn read_buf(&self) -> &[u8] {
            &self.buf[self.read_ptr..self.write_ptr]
        }

        /// Advances the read position by `amount` bytes
        pub fn consume(&mut self, amount: usize) {
            self.read_ptr += amount;
            debug_assert!(self.read_ptr <= self.write_ptr);
        }

        /// Returns the portion of the buffer available for writing
        pub fn write_buf(&mut self) -> &mut [u8] {
            &mut self.buf[self.write_ptr..]
        }

        /// Advances the write position by `amount` bytes
        pub fn advance(&mut self, amount: usize) {
            self.write_ptr += amount;
            debug_assert!(self.write_ptr <= self.buf.len())
        }

        /// Moves any unread bytes to the start of the buffer,
        /// creating more space for writing new data
        pub fn backshift(&mut self) {
            self.buf.copy_within(self.read_ptr..self.write_ptr, 0);
            self.write_ptr -= self.read_ptr;
            self.read_ptr = 0;
        }
    }
}
