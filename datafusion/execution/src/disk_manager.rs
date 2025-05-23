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

use datafusion_common::{
    config_err, resources_datafusion_err, resources_err, DataFusionError, Result,
};
use log::debug;
use parking_lot::Mutex;
use rand::{rng, Rng};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile, TempDir};

use crate::memory_pool::human_readable_size;

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
    used_disk_space: Arc<AtomicU64>,
}

impl DiskManager {
    /// Create a DiskManager given the configuration
    pub fn try_new(config: DiskManagerConfig) -> Result<Arc<Self>> {
        match config {
            DiskManagerConfig::Existing(manager) => Ok(manager),
            DiskManagerConfig::NewOs => Ok(Arc::new(Self {
                local_dirs: Mutex::new(Some(vec![])),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Arc::new(AtomicU64::new(0)),
            })),
            DiskManagerConfig::NewSpecified(conf_dirs) => {
                let local_dirs = create_local_dirs(conf_dirs)?;
                debug!(
                    "Created local dirs {local_dirs:?} as DataFusion working directory"
                );
                Ok(Arc::new(Self {
                    local_dirs: Mutex::new(Some(local_dirs)),
                    max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                    used_disk_space: Arc::new(AtomicU64::new(0)),
                }))
            }
            DiskManagerConfig::Disabled => Ok(Arc::new(Self {
                local_dirs: Mutex::new(None),
                max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
                used_disk_space: Arc::new(AtomicU64::new(0)),
            })),
        }
    }

    pub fn set_max_temp_directory_size(
        &mut self,
        max_temp_directory_size: u64,
    ) -> Result<()> {
        // If the disk manager is disabled and `max_temp_directory_size` is not 0,
        // this operation is not meaningful, fail early.
        if self.local_dirs.lock().is_none() && max_temp_directory_size != 0 {
            return config_err!(
                "Cannot set max temp directory size for a disk manager that spilling is disabled"
            );
        }

        self.max_temp_directory_size = max_temp_directory_size;
        Ok(())
    }

    pub fn set_arc_max_temp_directory_size(
        this: &mut Arc<Self>,
        max_temp_directory_size: u64,
    ) -> Result<()> {
        if let Some(inner) = Arc::get_mut(this) {
            inner.set_max_temp_directory_size(max_temp_directory_size)?;
            Ok(())
        } else {
            config_err!("DiskManager should be a single instance")
        }
    }

    pub fn with_max_temp_directory_size(
        mut self,
        max_temp_directory_size: u64,
    ) -> Result<Self> {
        self.set_max_temp_directory_size(max_temp_directory_size)?;
        Ok(self)
    }

    pub fn used_disk_space(&self) -> u64 {
        self.used_disk_space.load(Ordering::Relaxed)
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
        self: &Arc<Self>,
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

        let dir_index = rng().random_range(0..local_dirs.len());
        Ok(RefCountedTempFile {
            _parent_temp_dir: Arc::clone(&local_dirs[dir_index]),
            tempfile: Builder::new()
                .tempfile_in(local_dirs[dir_index].as_ref())
                .map_err(DataFusionError::IoError)?,
            current_file_disk_usage: 0,
            disk_manager: Arc::clone(self),
        })
    }
}

/// A wrapper around a [`NamedTempFile`] that also contains
/// a reference to its parent temporary directory.
///
/// # Note
/// After any modification to the underlying file (e.g., writing data to it), the caller
/// must invoke [`Self::update_disk_usage`] to update the global disk usage counter.
/// This ensures the disk manager can properly enforce usage limits configured by
/// [`DiskManager::with_max_temp_directory_size`].
#[derive(Debug)]
pub struct RefCountedTempFile {
    /// The reference to the directory in which temporary files are created to ensure
    /// it is not cleaned up prior to the NamedTempFile
    _parent_temp_dir: Arc<TempDir>,
    tempfile: NamedTempFile,
    /// Tracks the current disk usage of this temporary file. See
    /// [`Self::update_disk_usage`] for more details.
    current_file_disk_usage: u64,
    /// The disk manager that created and manages this temporary file
    disk_manager: Arc<DiskManager>,
}

impl RefCountedTempFile {
    pub fn path(&self) -> &Path {
        self.tempfile.path()
    }

    pub fn inner(&self) -> &NamedTempFile {
        &self.tempfile
    }

    /// Updates the global disk usage counter after modifications to the underlying file.
    ///
    /// # Errors
    /// - Returns an error if the global disk usage exceeds the configured limit.
    pub fn update_disk_usage(&mut self) -> Result<()> {
        // Get new file size from OS
        let metadata = self.tempfile.as_file().metadata()?;
        let new_disk_usage = metadata.len();

        // Update the global disk usage by:
        // 1. Subtracting the old file size from the global counter
        self.disk_manager
            .used_disk_space
            .fetch_sub(self.current_file_disk_usage, Ordering::Relaxed);
        // 2. Adding the new file size to the global counter
        self.disk_manager
            .used_disk_space
            .fetch_add(new_disk_usage, Ordering::Relaxed);

        // 3. Check if the updated global disk usage exceeds the configured limit
        let global_disk_usage = self.disk_manager.used_disk_space.load(Ordering::Relaxed);
        if global_disk_usage > self.disk_manager.max_temp_directory_size {
            return resources_err!(
                "The used disk space during the spilling process has exceeded the allowable limit of {}. Try increasing the `max_temp_directory_size` in the disk manager configuration.",
                human_readable_size(self.disk_manager.max_temp_directory_size as usize)
            );
        }

        // 4. Update the local file size tracking
        self.current_file_disk_usage = new_disk_usage;

        Ok(())
    }
}

/// When the temporary file is dropped, subtract its disk usage from the disk manager's total
impl Drop for RefCountedTempFile {
    fn drop(&mut self) {
        // Subtract the current file's disk usage from the global counter
        self.disk_manager
            .used_disk_space
            .fetch_sub(self.current_file_disk_usage, Ordering::Relaxed);
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
