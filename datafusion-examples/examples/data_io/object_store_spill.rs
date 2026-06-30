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

//! See `main.rs` for how to run it.
//!
//! [`object_store_spill`] demonstrates how to use the [`TempFileFactory`] API to configure
//! DataFusion to spill intermediate results to remote storage when it exceeds
//! the configured memory limits.
//!
//! See [`datafusion::execution::memory_pool`] for more information on how
//! DataFusion decides when operators should spill, and [`SpillFile`] for the
//! spill file abstraction this example implements.
use std::future::Future;
use std::io::Write;
use std::path::Path as StdPath;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use datafusion::common::Result;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SpillFile, SpillWriter, TempFileFactory};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::exec_err;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tempfile::tempdir;

/// Demonstrates configuring DataFusion with spill files backed by an ObjectStore.
pub async fn object_store_spill() -> Result<()> {
    // A real system would use S3, GCS, Azure, or some other ObjectStore for
    // remote spills. This example uses a local-file-backed ObjectStore for
    // simplicity.
    let tmp_dir = tempdir()?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path())?);

    // Create the custom TempFileFactory that creates spill files in the ObjectStore.
    let temp_file_factory = Arc::new(ObjectStoreTempFileFactory::new(store));
    let disk_manager_builder =
        DiskManagerBuilder::default().with_temp_file_factory(temp_file_factory.clone());
    let runtime = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(disk_manager_builder) // use the factory
        // and set a small memory limit so the example spills
        .with_memory_limit(1024 * 1024, 1.0)
        .build_arc()?;

    // Configure a SessionContext for running queries; use a single partition
    // and no sort spill reservation to make the example deterministic and keep
    // the spill behavior easy to observe.
    let config = SessionConfig::new()
        .with_sort_spill_reservation_bytes(0)
        .with_sort_in_place_threshold_bytes(0)
        .with_target_partitions(1);
    let ctx = SessionContext::new_with_config_rt(config, Arc::clone(&runtime));

    // Run an SQL query that sorts a "large" amount of data. Given the
    // SessionContext's low memory limit, the sort will spill.
    let row_count = 10_000_000;
    let mut stream = ctx
        .sql(&format!(
            "SELECT * FROM generate_series(1, {row_count}) AS t(v) ORDER BY v DESC"
        ))
        .await?
        .execute_stream()
        .await?;

    // Drive the query to completion, and verify output
    let mut output_rows = 0;
    while let Some(batch) = stream.next().await {
        output_rows += batch?.num_rows();
    }

    assert_eq!(output_rows, row_count as usize);
    assert!(
        temp_file_factory.created_files() > 0,
        "expected the custom TempFileFactory to be used for spilling"
    );

    Ok(())
}

/// Creates spill files backed by an [`ObjectStore`].
///
/// DataFusion calls this factory whenever an operator needs a new temporary
/// file for spilling. A remote deployment would use the same pattern with an
/// S3, GCS, Azure, or other remote ObjectStore implementation.
struct ObjectStoreTempFileFactory {
    /// ObjectStore used for spill file reads and writes.
    store: Arc<dyn ObjectStore>,
    /// Monotonic counter used to create unique object paths.
    counter: AtomicU64,
    /// Counts how many spill files DataFusion requested from this factory.
    created_files: AtomicU64,
}

impl ObjectStoreTempFileFactory {
    /// Create a new spill file factory that stores spill data in `store`.
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            counter: AtomicU64::new(0),
            created_files: AtomicU64::new(0),
        }
    }

    /// Return the number of spill files created through this factory.
    fn created_files(&self) -> u64 {
        self.created_files.load(Ordering::Relaxed)
    }
}

impl TempFileFactory for ObjectStoreTempFileFactory {
    /// Create one logical spill file backed by an ObjectStore path.
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        self.created_files.fetch_add(1, Ordering::Relaxed);

        // Convert a query-provided spill description into an ObjectStore-safe path component.
        //
        // For example, `"Sort Spill: partition 0"` becomes `"Sort_Spill__partition_0"`.
        let cleaned_description: String = description
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let location = Path::from(format!("spill/{cleaned_description}-{id}.bin"));

        // Return a SpillFile implementation that reads and writes this ObjectStore path.
        Ok(Arc::new(ObjectStoreSpillFile {
            store: Arc::clone(&self.store),
            location,
            size: Arc::new(AtomicU64::new(0)),
        }))
    }
}

/// Logical spill file stored at an ObjectStore path.
///
/// DataFusion writes spill data by calling [`SpillFile::open_writer`] and reads
/// it back by calling [`SpillFile::read_stream`].
struct ObjectStoreSpillFile {
    /// ObjectStore containing the spill object.
    store: Arc<dyn ObjectStore>,
    /// ObjectStore path for this spill object.
    location: Path,
    /// Last committed object size, updated when the writer finishes.
    size: Arc<AtomicU64>,
}

impl SpillFile for ObjectStoreSpillFile {
    /// Return no local filesystem path because the spill file is accessed through ObjectStore.
    fn path(&self) -> Option<&StdPath> {
        None // Remote ObjectStores do not have a local OS path.
    }

    /// Return the size of the uploaded object
    fn size(&self) -> Option<u64> {
        // Return the last committed size, which this example tracks after upload.
        Some(self.size.load(Ordering::Relaxed))
    }

    /// Read the spill file contents as a byte stream.
    fn read_stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        let store = Arc::clone(&self.store);
        let location = self.location.clone();

        // Note: we use `stream::once` to defer the ObjectStore read until
        // DataFusion polls the returned stream.
        let result_stream =
            async move { store.get(&location).await.map(|r| r.into_stream()) };
        // combine the result of get and the stream into a single stream
        let stream = stream::once(result_stream)
            .try_flatten()
            .map_err(Into::into);

        Ok(Box::pin(stream))
    }

    /// Open a synchronous writer for this spill file.
    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        // Create a writer that buffers bytes and uploads them on finish.
        Ok(Box::new(ObjectStoreSpillWriter {
            store: Arc::clone(&self.store),
            location: self.location.clone(),
            size: Arc::clone(&self.size),
            buffer: Vec::new(),
        }))
    }
}

/// Adapts DataFusion's [`SpillWriter`] API to ObjectStore.
///
/// This simple example buffers bytes in memory and uploads them in
/// [`SpillWriter::finish`]. A production remote implementation should consider
/// multipart or streaming uploads.
struct ObjectStoreSpillWriter {
    /// ObjectStore to read/write bytes to.
    store: Arc<dyn ObjectStore>,
    /// ObjectStore path to upload to.
    location: Path,
    /// Shared size field on the corresponding [`ObjectStoreSpillFile`].
    size: Arc<AtomicU64>,
    /// Buffered spill bytes waiting to be uploaded.
    ///
    /// This simple example buffers the spill and uploads it on finish.
    /// Production remote stores should consider multipart or streaming uploads.
    buffer: Vec<u8>,
}

impl Write for ObjectStoreSpillWriter {
    /// Append bytes to the in-memory buffer.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Buffer bytes written through the synchronous Write API.
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    /// No-op because data is committed in [`SpillWriter::finish`].
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SpillWriter for ObjectStoreSpillWriter {
    /// Upload buffered bytes to ObjectStore and mark the spill file complete.
    fn finish(&mut self) -> Result<()> {
        // Move the buffered bytes into the upload future.
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let data = std::mem::take(&mut self.buffer);
        let size = data.len() as u64;

        // This simple example buffers the spill and uploads it on finish.
        // Production remote stores should consider multipart or streaming uploads.
        block_on_object_store(async move {
            store
                .put(&location, PutPayload::from_bytes(data.into()))
                .await?;
            Ok(())
        })?;

        self.size.store(size, Ordering::Relaxed);
        Ok(())
    }
}

/// Run an async ObjectStore operation.
///
/// Adding a native async API is tracked in <https://github.com/apache/datafusion/issues/23247>
fn block_on_object_store<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        exec_err!("No current Tokio runtime available")
    }
}
