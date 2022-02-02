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

//! Manages files generated during query execution, files are
//! hashed among the directories listed in RuntimeConfig::local_dirs.

use crate::error::{DataFusionError, Result};
use log::debug;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile, TempDir};

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
    /// TempDirs to put temporary files in. A new OS specified
    /// temporary directory will be created if this list is empty.
    local_dirs: Mutex<Vec<TempDir>>,
}

impl DiskManager {
    /// Create a DiskManager given the configuration
    pub fn try_new(config: DiskManagerConfig) -> Result<Arc<Self>> {
        match config {
            DiskManagerConfig::Existing(manager) => Ok(manager),
            DiskManagerConfig::NewOs => Ok(Arc::new(Self {
                local_dirs: Mutex::new(vec![]),
            })),
            DiskManagerConfig::NewSpecified(conf_dirs) => {
                let local_dirs = create_local_dirs(conf_dirs)?;
                debug!(
                    "Created local dirs {:?} as DataFusion working directory",
                    local_dirs
                );
                Ok(Arc::new(Self {
                    local_dirs: Mutex::new(local_dirs),
                }))
            }
        }
    }

    /// Return a temporary file from a randomized choice in the configured locations
    pub fn create_tmp_file(&self) -> Result<NamedTempFile> {
        let mut local_dirs = self.local_dirs.lock();

        // Create a temporary directory if needed
        if local_dirs.is_empty() {
            let tempdir = tempfile::tempdir().map_err(DataFusionError::IoError)?;

            debug!(
                "Created directory '{:?}' as DataFusion tempfile directory",
                tempdir.path().to_string_lossy()
            );

            local_dirs.push(tempdir);
        }

        create_tmp_file(&local_dirs)
    }
}

/// Setup local dirs by creating one new dir in each of the given dirs
fn create_local_dirs(local_dirs: Vec<PathBuf>) -> Result<Vec<TempDir>> {
    local_dirs
        .iter()
        .map(|root| {
            Builder::new()
                .prefix("datafusion-")
                .tempdir_in(root)
                .map_err(DataFusionError::IoError)
        })
        .collect()
}

fn create_tmp_file(local_dirs: &[TempDir]) -> Result<NamedTempFile> {
    let dir_index = thread_rng().gen_range(0..local_dirs.len());
    let dir = local_dirs.get(dir_index).ok_or_else(|| {
        DataFusionError::Internal("No directories available to DiskManager".into())
    })?;

    Builder::new()
        .tempfile_in(dir)
        .map_err(DataFusionError::IoError)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::error::Result;
    use tempfile::TempDir;

    #[test]
    fn lazy_temp_dir_creation() -> Result<()> {
        // A default configuration should not create temp files until requested
        let config = DiskManagerConfig::new();
        let dm = DiskManager::try_new(config)?;

        assert_eq!(0, local_dir_snapshot(&dm).len());

        // can still create a tempfile however:
        let actual = dm.create_tmp_file()?;

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
        let actual = dm.create_tmp_file()?;

        // the file should be in one of the specified local directories
        assert_path_in_dirs(actual.path(), local_dirs.into_iter());

        Ok(())
    }

    /// Asserts that `file_path` is found anywhere in any of `dir` directories
    fn assert_path_in_dirs<'a>(
        file_path: &'a Path,
        dirs: impl Iterator<Item = &'a Path>,
    ) {
        let dirs: Vec<&Path> = dirs.collect();

        let found = dirs.iter().any(|file_path| {
            file_path
                .ancestors()
                .any(|candidate_path| *file_path == candidate_path)
        });

        assert!(found, "Can't find {:?} in dirs: {:?}", file_path, dirs);
    }
}
