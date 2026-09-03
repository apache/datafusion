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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_common::Result;
use futures::Stream;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// Abstraction over a spill file backend.
/// Implementations handle their own quota enforcement and blocking concerns.
#[async_trait]
pub trait SpillFile: Send + Sync {
    /// Returns the OS path if this is a local file, None otherwise.
    fn path(&self) -> Option<&Path> {
        None
    }

    /// Returns current size in bytes if cheaply available.
    fn size(&self) -> Option<u64>;

    /// Returns file contents as an async stream of byte chunks.
    fn read_stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>;

    /// Opens a writer for appending data to this file.
    fn open_writer(&self) -> Result<Box<dyn SpillWriter>>;

    /// Opens an asynchronous writer for appending data to this file.
    ///
    /// The default implementation adapts [`Self::open_writer`] for backwards
    /// compatibility. Backends with native asynchronous I/O should override
    /// this method to avoid blocking an async executor.
    async fn open_async_writer(&self) -> Result<Box<dyn AsyncSpillWriter>> {
        Ok(Box::new(BlockingSpillWriterAdapter {
            inner: self.open_writer()?,
        }))
    }
}

/// Writer for spill file backends.
pub trait SpillWriter: Write + Send {
    /// Intended for close/sync/commit operations.
    fn finish(&mut self) -> Result<()>;
}

/// Asynchronous writer for spill file backends.
///
/// The writer accepts owned [`Bytes`] so asynchronous backends can retain a
/// buffer across an await within [`Self::write_all`] without copying it. Calls
/// are made sequentially and buffers must be persisted in the order received.
/// After `write_all` returns, implementations must not retain the input's
/// backing allocation unless that retained memory is accounted for separately.
/// Backends that buffer a tail for a later call should copy that tail into an
/// allocation sized for the retained bytes.
///
/// DataFusion makes a best-effort attempt to abort writers dropped by query
/// cancellation. Backends should still configure lifecycle cleanup for
/// abandoned uploads because cleanup cannot run after process termination.
#[async_trait]
pub trait AsyncSpillWriter: Send {
    /// Writes all bytes in `data` to the spill file.
    async fn write_all(&mut self, data: Bytes) -> Result<()>;

    /// Flushes buffered data, if supported by the backend.
    ///
    /// This does not finish the writer or require the spill file to become
    /// visible to readers. [`Self::finish`] is the commit boundary.
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    /// Finishes and commits the spill file.
    ///
    /// A successful call is terminal. If committing returns an error, the
    /// caller will invoke [`Self::abort`] before dropping the writer.
    async fn finish(&mut self) -> Result<()>;

    /// Aborts an uncommitted spill write and cleans up backend resources.
    ///
    /// Multipart backends should override this method to explicitly abort an
    /// in-progress upload. A successful call is terminal. DataFusion may retry
    /// an abort that returns an error, including during best-effort drop cleanup.
    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

struct BlockingSpillWriterAdapter {
    inner: Box<dyn SpillWriter>,
}

#[async_trait]
impl AsyncSpillWriter for BlockingSpillWriterAdapter {
    async fn write_all(&mut self, data: Bytes) -> Result<()> {
        self.inner.write_all(&data)?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.inner.flush()?;
        self.inner.finish()
    }
}

/// Factory for creating spill files.
pub trait TempFileFactory: Send + Sync {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>>;
}
