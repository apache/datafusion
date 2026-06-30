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

use crate::spill_file::{SpillFile, SpillWriter, TempFileFactory};
use bytes::Bytes;
use datafusion_common::human_readable_size;
use datafusion_common::{DataFusionError, Result, config_err, resources_datafusion_err};
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt;
use log::debug;
use parking_lot::Mutex;
use rand::{Rng, rng};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tempfile::{Builder, NamedTempFile, TempDir};
pub const DEFAULT_MAX_TEMP_DIRECTORY_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB

/// Builder pattern for the [DiskManager] structure
#[derive(Clone)]
pub struct DiskManagerBuilder {
    /// The storage mode of the disk manager
    mode: DiskManagerMode,
    /// The maximum amount of data (in bytes) stored inside the temporary directories.
    /// Default to 100GB
    max_temp_directory_size: u64,
}
impl Debug for DiskManagerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskManagerBuilder")
            .field("mode", &self.mode)
            .field("max_temp_directory_size", &self.max_temp_directory_size)
            .finish()
    }
}
impl Default for DiskManagerBuilder {
    fn default() -> Self {
        Self {
            mode: DiskManagerMode::OsTmpDirectory,
            max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
        }
    }
}

impl DiskManagerBuilder {
    pub fn set_mode(&mut self, mode: DiskManagerMode) {
        self.mode = mode;
    }

    pub fn with_mode(mut self, mode: DiskManagerMode) -> Self {
        self.set_mode(mode);
        self
    }

    pub fn set_max_temp_directory_size(&mut self, value: u64) {
        self.max_temp_directory_size = value;
    }

    pub fn with_max_temp_directory_size(mut self, value: u64) -> Self {
        self.set_max_temp_directory_size(value);
        self
    }

    /// Create a DiskManager given the builder
    pub fn build(self) -> Result<DiskManager> {
        match self.mode {
            DiskManagerMode::OsTmpDirectory => Ok(DiskManager {
                local_dirs: Mutex::new(Some(vec![])),
                max_temp_directory_size: AtomicU64::new(self.max_temp_directory_size),
                used_disk_space: Arc::new(AtomicU64::new(0)),
                active_files_count: Arc::new(AtomicUsize::new(0)),
                factory: None,
            }),
            DiskManagerMode::Directories(conf_dirs) => {
                let local_dirs = create_local_dirs(&conf_dirs)?;
                debug!(
                    "Created local dirs {local_dirs:?} as DataFusion working directory"
                );
                Ok(DiskManager {
                    local_dirs: Mutex::new(Some(local_dirs)),
                    max_temp_directory_size: AtomicU64::new(self.max_temp_directory_size),
                    used_disk_space: Arc::new(AtomicU64::new(0)),
                    active_files_count: Arc::new(AtomicUsize::new(0)),
                    factory: None,
                })
            }
            DiskManagerMode::Disabled => Ok(DiskManager {
                local_dirs: Mutex::new(None),
                max_temp_directory_size: AtomicU64::new(self.max_temp_directory_size),
                used_disk_space: Arc::new(AtomicU64::new(0)),
                active_files_count: Arc::new(AtomicUsize::new(0)),
                factory: None,
            }),
            DiskManagerMode::Custom(factory) => Ok(DiskManager {
                local_dirs: Mutex::new(None),
                max_temp_directory_size: AtomicU64::new(self.max_temp_directory_size),
                used_disk_space: Arc::new(AtomicU64::new(0)),
                active_files_count: Arc::new(AtomicUsize::new(0)),
                factory: Some(factory),
            }),
        }
    }
}

#[derive(Clone, Default)]
pub enum DiskManagerMode {
    /// Create a new [DiskManager] that creates temporary files within
    /// a temporary directory chosen by the OS
    #[default]
    OsTmpDirectory,

    /// Create a new [DiskManager] that creates temporary files within
    /// the specified directories. One of the directories will be chosen
    /// at random for each temporary file created.
    Directories(Vec<PathBuf>),

    /// Create a new [DiskManager] with a cutstom backend
    Custom(Arc<dyn TempFileFactory>),

    /// Disable disk manager, attempts to create temporary files will error
    Disabled,
}

impl Debug for DiskManagerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OsTmpDirectory => write!(f, "OsTmpDirectory"),
            Self::Directories(dirs) => f.debug_tuple("Directories").field(dirs).finish(),
            Self::Disabled => write!(f, "Disabled"),
            Self::Custom(_) => write!(f, "Custom(Arc<dyn TempFileFactory>)"),
        }
    }
}

/// Manages files generated during query execution, e.g. spill files generated
/// while processing dataset larger than available memory.
pub struct DiskManager {
    /// TempDirs to put temporary files in.
    ///
    /// If `Some(vec![])` a new OS specified temporary directory will be created
    /// If `None` an error will be returned (configured not to spill)
    local_dirs: Mutex<Option<Vec<Arc<TempDir>>>>,
    /// The maximum amount of data (in bytes) stored inside the temporary directories.
    /// Default to 100GB. Stored as `AtomicU64` so it can be adjusted at runtime
    /// without requiring exclusive (`&mut`) access to the `DiskManager`.
    max_temp_directory_size: AtomicU64,
    /// Used disk space in the temporary directories. Now only spilled data for
    /// external executors are counted.
    used_disk_space: Arc<AtomicU64>,
    /// Number of active temporary files created by this disk manager
    active_files_count: Arc<AtomicUsize>,
    /// Factory
    factory: Option<Arc<dyn TempFileFactory>>,
}
impl Debug for DiskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskManager")
            .field("local_dirs", &self.local_dirs)
            .field("max_temp_directory_size", &self.max_temp_directory_size)
            .field("used_disk_space", &self.used_disk_space)
            .field("active_files_count", &self.active_files_count)
            .field("factory", &self.factory.is_some())
            .finish()
    }
}
/// Information about the current disk usage for spilling
#[derive(Debug, Clone, Copy)]
pub struct SpillingProgress {
    /// Total bytes currently used on disk for spilling
    pub current_bytes: u64,
    /// Total number of active spill files
    pub active_files_count: usize,
}

impl DiskManager {
    /// Creates a builder for [DiskManager]
    pub fn builder() -> DiskManagerBuilder {
        DiskManagerBuilder::default()
    }

    /// Atomically set the max temp directory size at runtime.
    ///
    /// Takes `&self`, so it works through `Arc<DiskManager>` without requiring
    /// exclusive access. Takes effect immediately for subsequent spill writes.
    ///
    /// Use this when you need to adjust the limit dynamically while queries
    /// are running (e.g., adapting to available disk space).
    pub fn set_max_temp_directory_size(
        &self,
        max_temp_directory_size: u64,
    ) -> Result<()> {
        // If the disk manager is disabled and `max_temp_directory_size` is not 0,
        // this operation is not meaningful, fail early.
        if self.local_dirs.lock().is_none()
            && max_temp_directory_size != 0
            && self.factory.is_none()
        {
            return config_err!(
                "Cannot set max temp directory size for a disk manager that spilling is disabled"
            );
        }

        self.max_temp_directory_size
            .store(max_temp_directory_size, Ordering::Relaxed);
        Ok(())
    }

    #[deprecated(
        since = "54.0.0",
        note = "Use `set_max_temp_directory_size` directly, it now takes &self"
    )]
    pub fn set_arc_max_temp_directory_size(
        this: &Arc<Self>,
        max_temp_directory_size: u64,
    ) -> Result<()> {
        this.set_max_temp_directory_size(max_temp_directory_size)
    }

    pub fn with_max_temp_directory_size(
        self,
        max_temp_directory_size: u64,
    ) -> Result<Self> {
        self.set_max_temp_directory_size(max_temp_directory_size)?;
        Ok(self)
    }

    pub fn used_disk_space(&self) -> u64 {
        self.used_disk_space.load(Ordering::Relaxed)
    }

    /// Returns the maximum temporary directory size in bytes
    pub fn max_temp_directory_size(&self) -> u64 {
        self.max_temp_directory_size.load(Ordering::Relaxed)
    }

    /// Returns the current spilling progress
    pub fn spilling_progress(&self) -> SpillingProgress {
        SpillingProgress {
            current_bytes: self.used_disk_space.load(Ordering::Relaxed),
            active_files_count: self.active_files_count.load(Ordering::Relaxed),
        }
    }

    /// Returns the temporary directory paths
    pub fn temp_dir_paths(&self) -> Vec<PathBuf> {
        self.local_dirs
            .lock()
            .as_ref()
            .map(|dirs| {
                dirs.iter()
                    .map(|temp_dir| temp_dir.path().to_path_buf())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return true if this disk manager supports creating temporary
    /// files. If this returns false, any call to `create_tmp_file`
    /// will error.
    pub fn tmp_files_enabled(&self) -> bool {
        self.factory.is_some() || self.local_dirs.lock().is_some()
    }

    /// Return a temporary file from a randomized choice in the configured locations
    ///
    /// If the file can not be created for some reason, returns an
    /// error message referencing the request description
    pub fn create_tmp_file(
        self: &Arc<Self>,
        request_description: &str,
    ) -> Result<Arc<dyn SpillFile>> {
        // Delegate to custom backend if configured
        if let Some(factory) = &self.factory {
            return factory.create_temp_file(request_description);
        }
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

        let dir_index = rng().random_range(0..local_dirs.len());
        self.active_files_count.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(RefCountedTempFile {
            parent_temp_dir: Arc::clone(&local_dirs[dir_index]),
            tempfile: Arc::new(
                Builder::new()
                    .tempfile_in(local_dirs[dir_index].as_ref())
                    .map_err(DataFusionError::IoError)?,
            ),
            current_file_disk_usage: Arc::new(AtomicU64::new(0)),
            disk_manager: Arc::clone(self),
        }))
    }
}

/// A wrapper around a [`NamedTempFile`] that also contains
/// a reference to its parent temporary directory.
///
/// This type is Clone-able, allowing multiple references to the same underlying file.
/// The file is deleted only when the last reference is dropped.
///
/// The parent temporary directory is also kept alive as long as any reference to
/// this file exists, preventing premature cleanup of the directory.
///
/// Once all references to this file are dropped, the file is deleted, and the
/// disk usage is subtracted from the disk manager's total.
#[derive(Debug)]
pub struct RefCountedTempFile {
    /// The reference to the directory in which temporary files are created to ensure
    /// it is not cleaned up prior to the NamedTempFile
    parent_temp_dir: Arc<TempDir>,
    /// The underlying temporary file, wrapped in Arc to allow cloning
    tempfile: Arc<NamedTempFile>,
    /// Tracks the current disk usage of this temporary file.
    ///
    /// This is wrapped in `Arc<AtomicU64>` so that all clones share the same
    /// disk usage tracking, preventing incorrect accounting when clones are dropped.
    current_file_disk_usage: Arc<AtomicU64>,
    /// The disk manager that created and manages this temporary file
    disk_manager: Arc<DiskManager>,
}

impl Clone for RefCountedTempFile {
    fn clone(&self) -> Self {
        Self {
            parent_temp_dir: Arc::clone(&self.parent_temp_dir),
            tempfile: Arc::clone(&self.tempfile),
            current_file_disk_usage: Arc::clone(&self.current_file_disk_usage),
            disk_manager: Arc::clone(&self.disk_manager),
        }
    }
}

impl RefCountedTempFile {
    pub fn path(&self) -> &Path {
        self.tempfile.path()
    }

    pub fn inner(&self) -> &NamedTempFile {
        self.tempfile.as_ref()
    }

    fn current_disk_usage(&self) -> u64 {
        self.current_file_disk_usage.load(Ordering::Relaxed)
    }
}

/// When the temporary file is dropped, subtract its disk usage from the disk manager's total
impl Drop for RefCountedTempFile {
    fn drop(&mut self) {
        // Only subtract disk usage when this is the last reference to the file
        // Check if we're the last one by seeing if there's only one strong reference
        // left to the underlying tempfile (the one we're holding)
        if Arc::strong_count(&self.tempfile) == 1 {
            let current_usage = self.current_file_disk_usage.load(Ordering::Relaxed);
            self.disk_manager
                .used_disk_space
                .fetch_sub(current_usage, Ordering::Relaxed);
            self.disk_manager
                .active_files_count
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Setup local dirs by creating one new dir in each of the given dirs
fn create_local_dirs(local_dirs: &[PathBuf]) -> Result<Vec<Arc<TempDir>>> {
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

pub struct FileSpillWriter {
    file: std::fs::File,
    disk_manager: Arc<DiskManager>,
    current_file_disk_usage: Arc<AtomicU64>,
}

impl std::io::Write for FileSpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len() as u64;
        if len == 0 {
            return Ok(0);
        }

        let new_global = self
            .disk_manager
            .used_disk_space
            .fetch_add(len, Ordering::Relaxed)
            + len;

        let limit = self.disk_manager.max_temp_directory_size();

        if new_global > limit {
            self.disk_manager
                .used_disk_space
                .fetch_sub(len, Ordering::Relaxed);

            return Err(std::io::Error::other(format!(
                "The used disk space during the spilling process has exceeded the allowable limit of {}. \
                        Please try increasing the config: `datafusion.runtime.max_temp_directory_size`.",
                human_readable_size(limit as usize)
            )));
        }

        self.file.write_all(buf).map_err(DataFusionError::IoError)?;

        self.current_file_disk_usage
            .fetch_add(len, Ordering::Relaxed);

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl SpillWriter for FileSpillWriter {
    fn finish(&mut self) -> Result<()> {
        // flush() is handled by Arrow, nothing left to do here
        Ok(())
    }
}

impl SpillFile for RefCountedTempFile {
    fn path(&self) -> Option<&Path> {
        Some(self.tempfile.path())
    }

    fn size(&self) -> Option<u64> {
        Some(self.current_disk_usage())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_stream(
        &self,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes>> + Send>>>
    {
        let path = self.path().to_owned();

        let stream =
            futures::stream::once(async move {
                tokio::fs::File::open(&path)
                    .await
                    .map_err(DataFusionError::IoError)
            })
            .flat_map(
                |open_result| -> std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<Bytes>> + Send>,
                > {
                    match open_result {
                        Ok(file) => Box::pin(
                            // Use a 128KB read buffer. The default 8KB causes excessive async
                            // poll overhead when reading multi-MB spill files back into memory.
                            tokio_util::io::ReaderStream::with_capacity(file, 128 * 1024)
                                .map(|r| r.map_err(DataFusionError::IoError)),
                        ),
                        Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
                    }
                },
            );

        Ok(Box::pin(stream))
    }

    #[cfg(target_arch = "wasm32")]
    fn read_stream(
        &self,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes>> + Send>>>
    {
        datafusion_common::exec_err!(
            "Default OS file spilling is not supported on WASM. Configure DiskManager with a Custom TempFileFactory."
        )
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        let file = self
            .tempfile
            .as_file()
            .try_clone()
            .map_err(DataFusionError::IoError)?;
        Ok(Box::new(FileSpillWriter {
            file,
            disk_manager: Arc::clone(&self.disk_manager),
            current_file_disk_usage: Arc::clone(&self.current_file_disk_usage),
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_temp_dir_creation() -> Result<()> {
        // A default configuration should not create temp files until requested
        let dm = Arc::new(DiskManagerBuilder::default().build()?);

        assert_eq!(0, local_dir_snapshot(&dm).len());

        // can still create a tempfile however:
        let actual = dm.create_tmp_file("Testing")?;

        // Now the tempdir has been created on demand
        assert_eq!(1, local_dir_snapshot(&dm).len());

        // the returned tempfile file should be in the temp directory
        let local_dirs = local_dir_snapshot(&dm);
        assert_path_in_dirs(
            actual.path().unwrap(),
            local_dirs.iter().map(|p| p.as_path()),
        );

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
        let dm = Arc::new(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(
                    local_dirs.iter().map(|p| p.into()).collect(),
                ))
                .build()?,
        );

        assert!(dm.tmp_files_enabled());
        let actual = dm.create_tmp_file("Testing")?;

        // the file should be in one of the specified local directories
        assert_path_in_dirs(actual.path().unwrap(), local_dirs.into_iter());

        Ok(())
    }

    #[test]
    fn test_disabled_disk_manager() {
        let manager = Arc::new(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Disabled)
                .build()
                .unwrap(),
        );
        assert!(!manager.tmp_files_enabled());
        match manager.create_tmp_file("Testing") {
            Err(e) => {
                assert_eq!(
                    e.strip_backtrace(),
                    "Resources exhausted: Memory Exhausted while Testing (DiskManager is disabled)"
                );
            }
            Ok(_) => {
                panic!("Expected DiskManager to fail creating a file when disabled!")
            }
        }
    }

    #[test]
    fn test_disk_manager_create_spill_folder() {
        let dir = TempDir::new().unwrap();
        DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![dir.path().to_path_buf()]))
            .build()
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
        let dm = Arc::new(DiskManagerBuilder::default().build()?);
        let temp_file = dm.create_tmp_file("Testing")?;
        let temp_file_path = temp_file.path().unwrap().to_owned();
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
        let dm = Arc::new(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(
                    local_dirs.iter().map(|p| p.into()).collect(),
                ))
                .build()?,
        );
        let temp_file = dm.create_tmp_file("Testing")?;
        let temp_file_path = temp_file.path().unwrap().to_owned();
        assert!(temp_file_path.exists());

        drop(dm);
        assert!(temp_file_path.exists());

        drop(temp_file);
        assert!(!temp_file_path.exists());

        Ok(())
    }

    #[test]
    fn test_disk_usage_basic() -> Result<()> {
        let dm = Arc::new(DiskManagerBuilder::default().build()?);
        let temp_file = dm.create_tmp_file("Testing")?;
        let mut writer = temp_file.open_writer()?;
        // Initially, disk usage should be 0
        assert_eq!(dm.used_disk_space(), 0);
        assert_eq!(temp_file.size().unwrap(), 0);

        // Write some data to the file
        writer.write_all(b"hello world")?;

        // Disk usage should now reflect the written data
        let expected_usage = temp_file.size().unwrap();
        assert!(expected_usage > 0);
        assert_eq!(dm.used_disk_space(), expected_usage);

        // Write more data
        writer.write_all(b"more_data")?;

        // Disk usage should increase
        let new_usage = temp_file.size().unwrap();
        assert!(new_usage > expected_usage);
        assert_eq!(dm.used_disk_space(), new_usage);

        // Drop the file
        drop(temp_file);

        // Disk usage should return to 0
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }

    #[test]
    fn test_disk_usage_with_clones() -> Result<()> {
        let dm = Arc::new(DiskManagerBuilder::default().build()?);
        let temp_file = dm.create_tmp_file("Testing")?;

        // Write some data
        let mut writer = temp_file.open_writer()?;
        writer.write_all(b"test data")?;

        let usage_after_write = temp_file.size().unwrap();
        assert!(usage_after_write > 0);
        assert_eq!(dm.used_disk_space(), usage_after_write);

        // Clone the file
        let clone1 = Arc::clone(&temp_file);
        let clone2 = Arc::clone(&temp_file);

        // All clones should see the same disk usage
        assert_eq!(clone1.size().unwrap(), usage_after_write);
        assert_eq!(clone2.size().unwrap(), usage_after_write);
        // Global disk usage should still be the same (not multiplied by number of clones)
        assert_eq!(dm.used_disk_space(), usage_after_write);

        // Write more data through one clone
        let mut clone_writer = clone1.open_writer()?;
        clone_writer.write_all(b" more data")?;

        let new_usage = clone1.size().unwrap();
        assert!(new_usage > usage_after_write);
        // All clones should see the updated disk usage
        assert_eq!(temp_file.size().unwrap(), new_usage);
        assert_eq!(clone2.size().unwrap(), new_usage);
        assert_eq!(clone1.size().unwrap(), new_usage);

        // Global disk usage should reflect the new size (not multiplied)
        assert_eq!(dm.used_disk_space(), new_usage);

        // Drop one clone
        drop(clone_writer);
        drop(clone1);

        // Disk usage should NOT change (other clones still exist)
        assert_eq!(dm.used_disk_space(), new_usage);
        assert_eq!(temp_file.size().unwrap(), new_usage);
        assert_eq!(clone2.size().unwrap(), new_usage);

        // Drop another clone
        drop(clone2);

        // Disk usage should still NOT change (original still exists)
        assert_eq!(dm.used_disk_space(), new_usage);
        assert_eq!(temp_file.size().unwrap(), new_usage);

        // Drop the original
        drop(writer);
        drop(temp_file);
        // Now disk usage should return to 0 (last reference dropped)
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }

    #[test]
    fn test_disk_usage_clones_dropped_out_of_order() -> Result<()> {
        let dm = Arc::new(DiskManagerBuilder::default().build()?);
        let temp_file = dm.create_tmp_file("Testing")?;
        let mut writer = temp_file.open_writer()?;

        // Write data
        writer.write_all(b"test")?;

        let usage = temp_file.size().unwrap();
        assert_eq!(dm.used_disk_space(), usage);

        // Create multiple clones
        let clone1 = Arc::clone(&temp_file);
        let clone2 = Arc::clone(&temp_file);
        let clone3 = Arc::clone(&temp_file);

        // Drop the original first (out of order)
        drop(temp_file);

        // Disk usage should still be tracked (clones exist)
        assert_eq!(dm.used_disk_space(), usage);
        assert_eq!(clone1.size().unwrap(), usage);

        // Drop clones in different order
        drop(clone2);
        assert_eq!(dm.used_disk_space(), usage);

        drop(clone1);
        assert_eq!(dm.used_disk_space(), usage);

        // Drop the last clone
        drop(clone3);

        // Now disk usage should be 0
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }

    #[test]
    fn test_disk_usage_multiple_files() -> Result<()> {
        let dm = Arc::new(DiskManagerBuilder::default().build()?);

        // Create multiple temp files
        let file1 = dm.create_tmp_file("Testing1")?;
        let file2 = dm.create_tmp_file("Testing2")?;

        let mut writer1 = file1.open_writer()?;
        let mut writer2 = file2.open_writer()?;

        // Write to first file
        writer1.write_all(b"file1")?;
        let usage1 = file1.size().unwrap();

        assert_eq!(dm.used_disk_space(), usage1);

        // Write to second file
        writer2.write_all(b"file2 data")?;
        let usage2 = file2.size().unwrap();

        // Global usage should be sum of both files
        assert_eq!(dm.used_disk_space(), usage1 + usage2);

        // Drop first file
        drop(file1);

        // Usage should only reflect second file
        assert_eq!(dm.used_disk_space(), usage2);

        // Drop second file
        drop(file2);

        // Usage should be 0
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }

    #[test]
    fn test_dynamic_limit_adjustment_through_shared_ref() -> Result<()> {
        // Verify that set_max_temp_directory_size works through &self (not &mut self).
        // This is the key behavioral change: the limit can be adjusted at runtime
        // without exclusive access, enabling dynamic resize while queries are running.
        let dm = DiskManager::builder()
            .with_max_temp_directory_size(1024)
            .build()?;
        let dm = Arc::new(dm);

        assert_eq!(dm.max_temp_directory_size(), 1024);

        // Adjust through shared reference (simulates concurrent access via Arc)
        dm.set_max_temp_directory_size(2048)?;
        assert_eq!(dm.max_temp_directory_size(), 2048);

        // Can also decrease
        dm.set_max_temp_directory_size(512)?;
        assert_eq!(dm.max_temp_directory_size(), 512);

        Ok(())
    }

    #[test]
    fn test_dynamic_limit_concurrent_access() -> Result<()> {
        // Verify that multiple threads can read and write the limit concurrently
        let dm = Arc::new(
            DiskManager::builder()
                .with_max_temp_directory_size(1000)
                .build()?,
        );

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let dm = Arc::clone(&dm);
                std::thread::spawn(move || {
                    // Each thread sets a different limit and reads it back
                    let new_limit = (i + 1) * 1000;
                    dm.set_max_temp_directory_size(new_limit).unwrap();
                    // Read should return SOME value set by one of the threads
                    let current = dm.max_temp_directory_size();
                    assert!((1000..=8000).contains(&current));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Final value should be one of the values set by threads
        let final_val = dm.max_temp_directory_size();
        assert!((1000..=8000).contains(&final_val));

        Ok(())
    }

    #[test]
    fn test_disabled_disk_manager_rejects_nonzero_limit() -> Result<()> {
        let dm = DiskManager::builder()
            .with_mode(DiskManagerMode::Disabled)
            .build()?;
        let dm = Arc::new(dm);

        // Setting non-zero limit on disabled DiskManager should error
        let result = dm.set_max_temp_directory_size(1024);
        assert!(result.is_err());

        // Setting zero is OK
        assert!(dm.set_max_temp_directory_size(0).is_ok());

        Ok(())
    }

    #[test]
    fn test_limit_decrease_below_current_usage() -> Result<()> {
        // Scenario: DiskManager has 100GB limit, currently using 80GB.
        // Admin lowers limit to 60GB. What happens?
        //
        // Expected behavior:
        // - Existing spill files remain on disk (not deleted)
        // - used_disk_space still reports 80GB
        // - New spill writes FAIL immediately (80GB > 60GB new limit)
        // - Once old queries complete and release their files (used drops below 60GB),
        //   new spill writes succeed again
        //
        // This demonstrates graceful degradation: lowering the limit doesn't
        // reclaim existing files (would break running queries), but prevents
        // additional spilling until usage drops naturally.
        let dm = DiskManager::builder()
            .with_max_temp_directory_size(100 * 1024 * 1024 * 1024) // 100GB
            .build()?;
        let dm = Arc::new(dm);

        // Simulate 80GB of existing spill usage
        dm.used_disk_space
            .store(80 * 1024 * 1024 * 1024, Ordering::Relaxed);

        assert_eq!(dm.max_temp_directory_size(), 100 * 1024 * 1024 * 1024);
        assert_eq!(dm.used_disk_space(), 80 * 1024 * 1024 * 1024);

        // Lower the limit to 60GB (below current usage)
        dm.set_max_temp_directory_size(60 * 1024 * 1024 * 1024)?;
        assert_eq!(dm.max_temp_directory_size(), 60 * 1024 * 1024 * 1024);

        // Current usage (80GB) now exceeds the new limit (60GB).
        // The used_disk_space is NOT reclaimed — existing files stay.
        assert_eq!(dm.used_disk_space(), 80 * 1024 * 1024 * 1024);

        // Any attempt to write MORE would be rejected at the SpillWriter level
        // because used_disk_space(80GB) > max_temp_directory_size(60GB).
        // (SpillWriter check: `global_disk_usage > limit` returns ResourcesExhausted)

        // Simulate old queries completing: usage drops to 50GB
        dm.used_disk_space
            .store(50 * 1024 * 1024 * 1024, Ordering::Relaxed);

        // Now usage (50GB) < limit (60GB) — new spill writes would succeed again
        assert!(dm.used_disk_space() < dm.max_temp_directory_size());

        Ok(())
    }

    #[test]
    fn test_limit_decrease_with_concurrent_queries() -> Result<()> {
        // Scenario: Multiple threads spilling while limit is lowered concurrently.
        // Demonstrates that:
        // 1. In-flight spills that started before the limit change complete normally
        //    (they already incremented used_disk_space)
        // 2. New spills after the limit change respect the new lower limit
        // 3. No data corruption or panics from concurrent access
        let dm = Arc::new(
            DiskManager::builder()
                .with_max_temp_directory_size(100 * 1024 * 1024) // 100MB
                .build()?,
        );

        let barrier = Arc::new(std::sync::Barrier::new(5));

        // 4 threads simulate concurrent spilling
        let spill_handles: Vec<_> = (0..4)
            .map(|_| {
                let dm = Arc::clone(&dm);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    // Simulate spill: increment used_disk_space
                    dm.used_disk_space
                        .fetch_add(10 * 1024 * 1024, Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    // Simulate cleanup
                    dm.used_disk_space
                        .fetch_sub(10 * 1024 * 1024, Ordering::Relaxed);
                })
            })
            .collect();

        // 1 thread lowers the limit mid-flight
        let dm_resize = Arc::clone(&dm);
        let resize_barrier = Arc::clone(&barrier);
        let resize_handle = std::thread::spawn(move || {
            resize_barrier.wait();
            // Lower limit while spills are in progress
            dm_resize
                .set_max_temp_directory_size(30 * 1024 * 1024) // 30MB
                .unwrap();
        });

        for h in spill_handles {
            h.join().unwrap();
        }
        resize_handle.join().unwrap();

        // After all threads complete:
        // - Limit is 30MB (last set by resize thread)
        // - used_disk_space is 0 (all spills cleaned up)
        // - No panics, no corruption
        assert_eq!(dm.max_temp_directory_size(), 30 * 1024 * 1024);
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }

    #[test]
    fn test_rollback_on_limit_exceeded_then_drop_returns_to_zero() -> Result<()> {
        // This test verifies that lowering the limit, failing a spill write,
        // and then dropping the file leaves used_disk_space at zero.
        //
        // Without the rollback fix, the global counter would be permanently
        // inflated by the delta between the new and old file sizes.

        let dm = Arc::new(
            DiskManager::builder()
                .with_max_temp_directory_size(10 * 1024 * 1024) // 10MB
                .build()?,
        );

        let file = dm.create_tmp_file("test_rollback")?;

        let mut writer = file.open_writer()?;

        // Create a temp file and write some data
        {
            let data = vec![0u8; 1024]; // 1KB
            writer.write_all(&data)?;
        }

        let usage_after_first_write = dm.used_disk_space();
        assert!(usage_after_first_write > 0);

        // Write more data to grow the file
        {
            let data = vec![0u8; 4 * 1024]; // 4KB more
            writer.write_all(&data)?;
        }

        let usage_after_second_write = dm.used_disk_space();
        assert!(usage_after_second_write > usage_after_first_write);

        // Now lower the limit to 1 byte — below current usage
        dm.set_max_temp_directory_size(1)?;

        // Write even more data
        {
            let data = vec![0u8; 2 * 1024]; // 2KB more

            // This write should FAIL (exceeds new 1-byte limit)
            let result = writer.write_all(&data);

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("exceeded the allowable limit")
            );
        }

        // Critical check: used_disk_space should still equal the LAST
        // successful update (before the failed one), not be inflated
        assert_eq!(dm.used_disk_space(), usage_after_second_write);

        // Drop the writer and file — should subtract the last successful file size
        drop(writer);
        drop(file);

        // After drop: used_disk_space must be zero (no leak)
        assert_eq!(dm.used_disk_space(), 0);

        Ok(())
    }
}
