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

//! Copy of parquet::util::io::FileSource for thread safe parquet reader

use std::sync::Mutex;
use std::{cmp, fmt, io::*};

use crate::datasource::object_store::ThreadSafeRead;
use crate::parquet::file::reader::Length;
use crate::parquet::util::io::{ParquetReader, Position};

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

// ----------------------------------------------------------------------

/// ParquetReader is the interface which needs to be fulfilled to be able to parse a
/// parquet source.
pub trait ThreadSafeParquetReader: ParquetReader + Send + Sync + 'static {}
impl<T: ParquetReader + Send + Sync + 'static> ThreadSafeParquetReader for T {}

/// Struct that represents a slice of a file data with independent start position and
/// length. Internally clones provided file handle, wraps with a custom implementation
/// of BufReader that resets position before any read.
///
/// This is workaround and alternative for `file.try_clone()` method. It clones `File`
/// while preserving independent position, which is not available with `try_clone()`.
///
/// Designed after `arrow::io::RandomAccessFile` and `std::io::BufReader`
pub struct FileSource2<R: ThreadSafeParquetReader> {
    reader: Mutex<R>,
    start: u64,     // start position in a file
    end: u64,       // end position in a file
    buf: Vec<u8>,   // buffer where bytes read in advance are stored
    buf_pos: usize, // current position of the reader in the buffer
    buf_cap: usize, // current number of bytes read into the buffer
}

impl<R: ThreadSafeParquetReader> fmt::Debug for FileSource2<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSource")
            .field("reader", &"OPAQUE")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("buf.len", &self.buf.len())
            .field("buf_pos", &self.buf_pos)
            .field("buf_cap", &self.buf_cap)
            .finish()
    }
}

impl<R: ThreadSafeParquetReader> FileSource2<R> {
    /// Creates new file reader with start and length from a file handle
    pub fn new(fd: &R, start: u64, length: usize) -> Self {
        let reader = Mutex::new(fd.try_clone().unwrap());
        Self {
            reader,
            start,
            end: start + length as u64,
            buf: vec![0_u8; DEFAULT_BUF_SIZE],
            buf_pos: 0,
            buf_cap: 0,
        }
    }

    fn fill_inner_buf(&mut self) -> Result<&[u8]> {
        if self.buf_pos >= self.buf_cap {
            // If we've reached the end of our internal buffer then we need to fetch
            // some more data from the underlying reader.
            // Branch using `>=` instead of the more correct `==`
            // to tell the compiler that the pos..cap slice is always valid.
            debug_assert!(self.buf_pos == self.buf_cap);
            let mut reader = self.reader.lock().unwrap();
            reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
            self.buf_cap = reader.read(&mut self.buf)?;
            self.buf_pos = 0;
        }
        Ok(&self.buf[self.buf_pos..self.buf_cap])
    }

    fn skip_inner_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        // discard buffer
        self.buf_pos = 0;
        self.buf_cap = 0;
        // read directly into param buffer
        let mut reader = self.reader.lock().unwrap();
        reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
        let nread = reader.read(buf)?;
        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: ThreadSafeParquetReader> Read for FileSource2<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_to_read = cmp::min(buf.len(), (self.end - self.start) as usize);
        let buf = &mut buf[0..bytes_to_read];

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.buf_pos == self.buf_cap && buf.len() >= self.buf.len() {
            return self.skip_inner_buf(buf);
        }
        let nread = {
            let mut rem = self.fill_inner_buf()?;
            // copy the data from the inner buffer to the param buffer
            rem.read(buf)?
        };
        // consume from buffer
        self.buf_pos = cmp::min(self.buf_pos + nread, self.buf_cap);

        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: ThreadSafeParquetReader> Position for FileSource2<R> {
    fn pos(&self) -> u64 {
        self.start
    }
}

impl<R: ThreadSafeParquetReader> Length for FileSource2<R> {
    fn len(&self) -> u64 {
        self.end - self.start
    }
}

impl<R: ThreadSafeParquetReader> ThreadSafeRead for FileSource2<R> {}
