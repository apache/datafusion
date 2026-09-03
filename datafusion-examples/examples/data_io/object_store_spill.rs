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
//!
//! This example exercises the asynchronous external-sort spill path. Execution
//! paths that require a partially written local file to be readable still use
//! the synchronous spill API.
use std::path::Path as StdPath;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::{Result, not_impl_err};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{AsyncSpillWriter, SpillFile, SpillWriter, TempFileFactory};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayloadMut};
use tempfile::tempdir;

/// Most remote object stores require non-final multipart parts to be at least 5 MiB.
const OBJECT_STORE_PART_SIZE: usize = 5 * 1024 * 1024;

/// Demonstrates configuring DataFusion with spill files backed by an ObjectStore.
pub async fn object_store_spill() -> Result<()> {
    // A real system would use S3, GCS, Azure, or some other ObjectStore for
    // remote spills. This example uses a local-file-backed ObjectStore for
    // simplicity.
    let tmp_dir = tempdir()?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path())?);

    // Create the custom TempFileFactory that creates spill files in the ObjectStore.
    let temp_file_factory = Arc::new(ObjectStoreTempFileFactory::new(Arc::clone(&store)));
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
    // Ensure the workload crosses a multipart boundary so the example uploads
    // at least one complete part before the final `finish` call.
    let spill_prefix = Path::from("spill");
    let spill_objects = store
        .list(Some(&spill_prefix))
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        spill_objects
            .iter()
            .any(|object| object.size >= OBJECT_STORE_PART_SIZE as u64),
        "expected at least one spill object to exceed the multipart part size"
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
/// DataFusion writes spill data by calling [`SpillFile::open_async_writer`] and
/// reads it back by calling [`SpillFile::read_stream`].
struct ObjectStoreSpillFile {
    /// ObjectStore containing the spill object.
    store: Arc<dyn ObjectStore>,
    /// ObjectStore path for this spill object.
    location: Path,
    /// Last committed object size, updated when the writer finishes.
    size: Arc<AtomicU64>,
}

#[async_trait]
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

        // Use `stream::once` to defer the ObjectStore read until DataFusion
        // polls the returned stream.
        let result_stream =
            async move { store.get(&location).await.map(|r| r.into_stream()) };
        let stream = stream::once(result_stream)
            .try_flatten()
            .map_err(Into::into);

        Ok(Box::pin(stream))
    }

    /// This example backend supports the asynchronous external-sort spill path only.
    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        not_impl_err!("Synchronous spill writing is not supported by this backend")
    }

    /// Open an asynchronous, multipart-capable writer for this spill file.
    async fn open_async_writer(&self) -> Result<Box<dyn AsyncSpillWriter>> {
        let upload = self.store.put_multipart(&self.location).await?;
        Ok(Box::new(ObjectStoreSpillWriter {
            upload,
            buffer: PutPayloadMut::new(),
            size: Arc::clone(&self.size),
            bytes_written: 0,
        }))
    }
}

/// Adapts DataFusion's [`AsyncSpillWriter`] API to ObjectStore.
struct ObjectStoreSpillWriter {
    /// Multipart upload used to stream completed parts to the store.
    upload: Box<dyn MultipartUpload>,
    /// Buffers at most one part while preserving owned `Bytes` chunks.
    buffer: PutPayloadMut,
    /// Shared size field on the corresponding [`ObjectStoreSpillFile`].
    size: Arc<AtomicU64>,
    /// Number of bytes passed to the writer.
    bytes_written: u64,
}

impl ObjectStoreSpillWriter {
    async fn flush_part(&mut self) -> object_store::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let part = std::mem::take(&mut self.buffer).freeze();
        self.upload.put_part(part).await
    }
}

#[async_trait]
impl AsyncSpillWriter for ObjectStoreSpillWriter {
    async fn write_all(&mut self, mut data: Bytes) -> Result<()> {
        let len = data.len() as u64;
        while !data.is_empty() {
            let remaining = OBJECT_STORE_PART_SIZE - self.buffer.content_length();
            if data.len() < remaining {
                // A Bytes slice can pin an entire Arrow allocation. Copy only
                // the tail retained after this call so it remains accurately
                // bounded after DataFusion releases the batch reservation.
                self.buffer.push(Bytes::copy_from_slice(&data));
                break;
            }

            self.buffer.push(data.split_to(remaining));
            self.flush_part().await?;
        }
        self.bytes_written += len;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.flush_part().await?;
        self.upload.complete().await?;
        self.size.store(self.bytes_written, Ordering::Relaxed);
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        // Release locally buffered spill data before waiting on remote cleanup.
        self.buffer = PutPayloadMut::new();
        self.upload.abort().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutPayload, PutResult, UploadPart};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    #[derive(Debug)]
    struct RecordingMultipartUpload {
        part_sizes: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl MultipartUpload for RecordingMultipartUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            self.part_sizes.lock().unwrap().push(data.content_length());
            Box::pin(async { Ok(()) })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            Ok(())
        }
    }

    struct DropTrackingOwner {
        data: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for DropTrackingOwner {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for DropTrackingOwner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn uploads_full_part_before_finish() -> Result<()> {
        let part_sizes = Arc::new(Mutex::new(Vec::new()));
        let upload = RecordingMultipartUpload {
            part_sizes: Arc::clone(&part_sizes),
        };
        let mut writer = ObjectStoreSpillWriter {
            upload: Box::new(upload),
            buffer: PutPayloadMut::new(),
            size: Arc::new(AtomicU64::new(0)),
            bytes_written: 0,
        };

        writer
            .write_all(Bytes::from(vec![42; OBJECT_STORE_PART_SIZE + 1]))
            .await?;
        assert_eq!(
            part_sizes.lock().unwrap().as_slice(),
            &[OBJECT_STORE_PART_SIZE]
        );
        assert_eq!(writer.buffer.content_length(), 1);
        writer.abort().await?;
        Ok(())
    }

    #[tokio::test]
    async fn buffered_tail_does_not_retain_input_allocation() -> Result<()> {
        let tmp_dir = tempdir()?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path())?);
        let location = Path::from("tail-retention-test");
        let upload = store.put_multipart(&location).await?;
        let dropped = Arc::new(AtomicBool::new(false));
        let data = Bytes::from_owner(DropTrackingOwner {
            data: vec![42; 128],
            dropped: Arc::clone(&dropped),
        });
        let mut writer = ObjectStoreSpillWriter {
            upload,
            buffer: PutPayloadMut::new(),
            size: Arc::new(AtomicU64::new(0)),
            bytes_written: 0,
        };

        writer.write_all(data).await?;
        assert!(
            dropped.load(Ordering::Relaxed),
            "buffered tail should not retain the input allocation"
        );
        writer.abort().await?;
        Ok(())
    }
}
