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

use bytes::Bytes;
use datafusion_common::Result;
use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// Abstraction over a spill file backend.
/// Implementations handle their own quota enforcement and blocking concerns.
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
}

/// Writer for spill file backends.
/// Receives zero-copy `Bytes` payloads from the IPCStreamWriter adapter.
pub trait SpillWriter: Send {
    fn write(&mut self, data: Bytes) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    /// Finalizes the write after all data has been flushed.
    ///
    /// Implementations must not call `flush` internally.
    /// Intended for close/sync/commit operations.    
    fn finish(&mut self) -> Result<()>;
}

/// Factory for creating spill files.
pub trait TempFileFactory:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>>;
}
