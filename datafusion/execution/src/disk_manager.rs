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

//! [`DiskManager`]: Manages files generated during query execution

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use datafusion_common::{
    config_err, exec_datafusion_err, internal_err, resources_datafusion_err,
    resources_err, DataFusionError, Result,
};
use log::debug;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile, TempDir};

use crate::memory_pool::human_readable_size;
use crate::metrics::{Count, SpillMetrics};

const DEFAULT_MAX_TEMP_DIRECTORY_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB

/// Configuration for temporary disk access
#[derive(Debug, Clone)]
pub enum DiskManagerConfig {
    /// Use the provided [DiskManager] instance
    Existing(Arc<DiskManager>),

    /// Create a new [DiskManager] that creates temporary files within
    /// a temporary directory chosen by the OS
    NewOs,

    /// Create a new [DiskManager] that creates temporary files within
    /// the specified directories
    NewSpecified(Vec<PathBuf>),

    /// Disable disk manager, attempts to create temporary files will error
    Disabled,
}

impl Default for DiskManagerConfig {
    fn default() -> Self {
        Self::NewOs
    }
}

impl DiskManagerConfig {
    /// Create temporary files in a temporary directory chosen by the OS
    pub fn new() -> Self {
        Self::default()
    }

    /// Create temporary files using the provided disk manager
    pub fn new_existing(existing: Arc<DiskManager>) -> Self {
        Self::Existing(existing)
    }

    /// Create temporary files in the specified directories
    pub fn new_specified(paths: Vec<PathBuf>) -> Self {
        Self::NewSpecified(paths)
    }
}

/// Manages files generated during query execution, e.g. spill files generated
/// while processing dataset larger than available memory.
#[derive(Debug)]
pub struct DiskManager {
    /// TempDirs to put temporary files in.
    ///
    /// If `Some(vec![])` a new OS specified temporary directory will be created
    /// If `None` an error will be returned (configured not to spill)
    local_dirs: Mutex<Option<Vec<Arc<TempDir>>>>,

    /// The maximum amount of data (in bytes) stored inside the temporary directories.
    /// Default to 100GB
    max_temp_directory_size: u64,

    /// Used disk space in the temporary directories. Now only spilled data for
    /// external executors are counted.
    used_disk_space: Count,
}

impl DiskManager {
    /// Create a DiskManager given the configuration
    pub fn try_new(config: DiskManagerConfig) -> Result<Arc<Self>> {
        match config {
            DiskManagerConfig::Existing(manager) => Ok(manager),
            DiskManagerConfig::NewOs => Ok(Arc::new(Self {
                local_dirs: Mutex::new(Some(vec![])),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Count::default(),
            })),
            DiskManagerConfig::NewSpecified(conf_dirs) => {
                let local_dirs = create_local_dirs(conf_dirs)?;
                debug!(
                    "Created local dirs {:?} as DataFusion working directory",
                    local_dirs
                );
                Ok(Arc::new(Self {
                    local_dirs: Mutex::new(Some(local_dirs)),
                    max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                    used_disk_space: Count::default(),
                }))
            }
            DiskManagerConfig::Disabled => Ok(Arc::new(Self {
                local_dirs: Mutex::new(None),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Count::default(),
            })),
        }
    }

    pub fn try_new_without_arc(config: DiskManagerConfig) -> Result<Self> {
        match config {
            DiskManagerConfig::Existing(manager) => {
                Arc::try_unwrap(manager).map_err(|_| {
                    DataFusionError::Internal("Failed to unwrap Arc".to_string())
                })
            }
            DiskManagerConfig::NewOs => Ok(Self {
                local_dirs: Mutex::new(Some(vec![])),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Count::default(),
            }),
            DiskManagerConfig::NewSpecified(conf_dirs) => {
                let local_dirs = create_local_dirs(conf_dirs)?;
                debug!(
                    "Created local dirs {:?} as DataFusion working directory",
                    local_dirs
                );
                Ok(Self {
                    local_dirs: Mutex::new(Some(local_dirs)),
                    max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                    used_disk_space: Count::default(),
                })
            }
            DiskManagerConfig::Disabled => Ok(Self {
                local_dirs: Mutex::new(None),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Count::default(),
            }),
        }
    }

    /// Set the maximum amount of data (in bytes) stored inside the temporary directories.
    pub fn with_max_temp_directory_size(
        mut self,
        max_temp_directory_size: u64,
    ) -> Result<Self> {
        // If the disk manager is disabled and `max_temp_directory_size` is not 0,
        // this operation is not meaningful, fail early.
        if self.local_dirs.lock().is_none() && max_temp_directory_size != 0 {
            return config_err!(
                "Cannot set max temp directory size for disabled disk manager"
            );
        }

        self.max_temp_directory_size = max_temp_directory_size;
        Ok(self)
    }

    /// Return true if this disk manager supports creating temporary
    /// files. If this returns false, any call to `create_tmp_file`
    /// will error.
    pub fn tmp_files_enabled(&self) -> bool {
        self.local_dirs.lock().is_some()
    }

    /// Return a temporary file from a randomized choice in the configured locations
    ///
    /// If the file can not be created for some reason, returns an
    /// error message referencing the request description
    pub fn create_tmp_file(
        &self,
        request_description: &str,
    ) -> Result<RefCountedTempFile> {
        let mut guard = self.local_dirs.lock();
        let local_dirs = guard.as_mut().ok_or_else(|| {
            resources_datafusion_err!(
                "Memory Exhausted while {request_description} (DiskManager is disabled)"
            )
        })?;

        // Create a temporary directory if needed
        if local_dirs.is_empty() {
            let tempdir = tempfile::tempdir().map_err(DataFusionError::IoError)?;

            debug!(
                "Created directory '{:?}' as DataFusion tempfile directory for {}",
                tempdir.path().to_string_lossy(),
                request_description,
            );

            local_dirs.push(Arc::new(tempdir));
        }

        let dir_index = thread_rng().gen_range(0..local_dirs.len());
        Ok(RefCountedTempFile {
            _parent_temp_dir: Arc::clone(&local_dirs[dir_index]),
            tempfile: Builder::new()
                .tempfile_in(local_dirs[dir_index].as_ref())
                .map_err(DataFusionError::IoError)?,
        })
    }

    /// Write record batches to a temporary file, and return the spill file handle.
    ///
    /// This method is used within executors with spilling capabilities to write
    /// temporary `RecordBatch`es to disk. Resource errors are returned if the written
    /// file size exceeds the disk limit specified in `max_temp_directory_size` from
    /// `DiskManager`.
    ///
    /// # Arguments
    ///
    /// * `batches` - A slice of `RecordBatch` to be written to disk. Note that this
    ///   slice can't be empty.
    /// * `request_description` - A description of the request for logging and error messages.
    /// * `caller_spill_metrics` - Metrics to be updated with the spill operation details, from the calling exeuctor.
    pub fn try_spill_record_batches(
        &self,
        batches: &[RecordBatch],
        request_description: &str,
        caller_spill_metrics: &mut SpillMetrics,
    ) -> Result<RefCountedTempFile> {
        if batches.is_empty() {
            return internal_err!(
                "`try_spill_record_batches` requires at least one batch"
            );
        }

        let spill_file = self.create_tmp_file(request_description)?;
        let schema = batches[0].schema();

        let mut stream_writer = IPCStreamWriter::new(spill_file.path(), schema.as_ref())?;

        for batch in batches {
            // The IPC Stream writer does not have a mechanism to avoid writing duplicate
            // `Buffer`s repeatedly, so we do not use `get_record_batch_memory_size()`
            // to estimate the memory size with duplicated `Buffer`s.
            let estimate_extra_size = batch.get_array_memory_size();

            if (self.used_disk_space.value() + estimate_extra_size) as u64
                > self.max_temp_directory_size
            {
                return resources_err!(
                    "The used disk space during the spilling process has exceeded the allowable limit of {}. Try increasing the `max_temp_directory_size` in the disk manager configuration.",
                    human_readable_size(self.max_temp_directory_size as usize)
                );
            }

            self.used_disk_space.add(estimate_extra_size);
            stream_writer.write(batch)?;
        }

        stream_writer.finish()?;

        // Update calling executor's spill metrics
        caller_spill_metrics
            .spilled_bytes
            .add(stream_writer.num_bytes);
        caller_spill_metrics
            .spilled_rows
            .add(stream_writer.num_rows);
        caller_spill_metrics.spill_file_count.add(1);

        Ok(spill_file)
    }

    /// Refer to the documentation for [`Self::try_spill_record_batches`]. This method
    /// additionally spills the `RecordBatch` into smaller batches, divided by `row_limit`.
    pub fn try_spill_record_batch_by_size(
        &self,
        batch: &RecordBatch,
        request_description: &str,
        spill_metrics: &mut SpillMetrics,
        row_limit: usize,
    ) -> Result<RefCountedTempFile> {
        let total_rows = batch.num_rows();
        let mut batches = Vec::new();
        let mut offset = 0;

        // It's ok to calculate all slices first, because slicing is zero-copy.
        while offset < total_rows {
            let length = std::cmp::min(total_rows - offset, row_limit);
            let sliced_batch = batch.slice(offset, length);
            batches.push(sliced_batch);
            offset += length;
        }

        // Spill the sliced batches to disk
        self.try_spill_record_batches(&batches, request_description, spill_metrics)
    }
}

/// A wrapper around a [`NamedTempFile`] that also contains
/// a reference to its parent temporary directory
#[derive(Debug)]
pub struct RefCountedTempFile {
    /// The reference to the directory in which temporary files are created to ensure
    /// it is not cleaned up prior to the NamedTempFile
    _parent_temp_dir: Arc<TempDir>,
    tempfile: NamedTempFile,
}

impl RefCountedTempFile {
    pub fn path(&self) -> &Path {
        self.tempfile.path()
    }

    pub fn inner(&self) -> &NamedTempFile {
        &self.tempfile
    }
}

/// Setup local dirs by creating one new dir in each of the given dirs
fn create_local_dirs(local_dirs: Vec<PathBuf>) -> Result<Vec<Arc<TempDir>>> {
    local_dirs
        .iter()
        .map(|root| {
            if !Path::new(root).exists() {
                std::fs::create_dir(root)?;
            }
            Builder::new()
                .prefix("datafusion-")
                .tempdir_in(root)
                .map_err(DataFusionError::IoError)
        })
        .map(|result| result.map(Arc::new))
        .collect()
}

/// Write in Arrow IPC Stream format to a file.
///
/// Stream format is used for spill because it supports dictionary replacement, and the random
/// access of IPC File format is not needed (IPC File format doesn't support dictionary replacement).
pub struct IPCStreamWriter {
    /// Inner writer
    pub writer: StreamWriter<File>,
    /// Batches written
    pub num_batches: usize,
    /// Rows written
    pub num_rows: usize,
    /// Bytes written
    pub num_bytes: usize,
}

impl IPCStreamWriter {
    /// Create new writer
    pub fn new(path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            exec_datafusion_err!("Failed to create partition file at {path:?}: {e:?}")
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            writer: StreamWriter::try_new(file, schema)?,
        })
    }

    /// Write one single batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows();
        let num_bytes: usize = batch.get_array_memory_size();
        self.num_bytes += num_bytes;
        Ok(())
    }

    /// Finish the writer
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_temp_dir_creation() -> Result<()> {
        // A default configuration should not create temp files until requested
        let config = DiskManagerConfig::new();
        let dm = DiskManager::try_new(config)?;

        assert_eq!(0, local_dir_snapshot(&dm).len());

        // can still create a tempfile however:
        let actual = dm.create_tmp_file("Testing")?;

        // Now the tempdir has been created on demand
        assert_eq!(1, local_dir_snapshot(&dm).len());

        // the returned tempfile file should be in the temp directory
        let local_dirs = local_dir_snapshot(&dm);
        assert_path_in_dirs(actual.path(), local_dirs.iter().map(|p| p.as_path()));

        Ok(())
    }

    fn local_dir_snapshot(dm: &DiskManager) -> Vec<PathBuf> {
        dm.local_dirs
            .lock()
            .iter()
            .flatten()
            .map(|p| p.path().into())
            .collect()
    }

    #[test]
    fn file_in_right_dir() -> Result<()> {
        let local_dir1 = TempDir::new()?;
        let local_dir2 = TempDir::new()?;
        let local_dir3 = TempDir::new()?;
        let local_dirs = vec![local_dir1.path(), local_dir2.path(), local_dir3.path()];
        let config = DiskManagerConfig::new_specified(
            local_dirs.iter().map(|p| p.into()).collect(),
        );

        let dm = DiskManager::try_new(config)?;
        assert!(dm.tmp_files_enabled());
        let actual = dm.create_tmp_file("Testing")?;

        // the file should be in one of the specified local directories
        assert_path_in_dirs(actual.path(), local_dirs.into_iter());

        Ok(())
    }

    #[test]
    fn test_disabled_disk_manager() {
        let config = DiskManagerConfig::Disabled;
        let manager = DiskManager::try_new(config).unwrap();
        assert!(!manager.tmp_files_enabled());
        assert_eq!(
            manager.create_tmp_file("Testing").unwrap_err().strip_backtrace(),
            "Resources exhausted: Memory Exhausted while Testing (DiskManager is disabled)",
        )
    }

    #[test]
    fn test_disk_manager_create_spill_folder() {
        let dir = TempDir::new().unwrap();
        let config = DiskManagerConfig::new_specified(vec![dir.path().to_owned()]);

        DiskManager::try_new(config)
            .unwrap()
            .create_tmp_file("Testing")
            .unwrap();
    }

    /// Asserts that `file_path` is found anywhere in any of `dir` directories
    fn assert_path_in_dirs<'a>(
        file_path: &'a Path,
        dirs: impl Iterator<Item = &'a Path>,
    ) {
        let dirs: Vec<&Path> = dirs.collect();

        let found = dirs.iter().any(|dir_path| {
            file_path
                .ancestors()
                .any(|candidate_path| *dir_path == candidate_path)
        });

        assert!(found, "Can't find {file_path:?} in dirs: {dirs:?}");
    }

    #[test]
    fn test_temp_file_still_alive_after_disk_manager_dropped() -> Result<()> {
        // Test for the case using OS arranged temporary directory
        let config = DiskManagerConfig::new();
        let dm = DiskManager::try_new(config)?;
        let temp_file = dm.create_tmp_file("Testing")?;
        let temp_file_path = temp_file.path().to_owned();
        assert!(temp_file_path.exists());

        drop(dm);
        assert!(temp_file_path.exists());

        drop(temp_file);
        assert!(!temp_file_path.exists());

        // Test for the case using specified directories
        let local_dir1 = TempDir::new()?;
        let local_dir2 = TempDir::new()?;
        let local_dir3 = TempDir::new()?;
        let local_dirs = [local_dir1.path(), local_dir2.path(), local_dir3.path()];
        let config = DiskManagerConfig::new_specified(
            local_dirs.iter().map(|p| p.into()).collect(),
        );
        let dm = DiskManager::try_new(config)?;
        let temp_file = dm.create_tmp_file("Testing")?;
        let temp_file_path = temp_file.path().to_owned();
        assert!(temp_file_path.exists());

        drop(dm);
        assert!(temp_file_path.exists());

        drop(temp_file);
        assert!(!temp_file_path.exists());

        Ok(())
    }
}
