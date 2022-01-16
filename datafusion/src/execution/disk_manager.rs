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
use log::info;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tempfile::{Builder, TempDir};

/// Manages files generated during query execution, e.g. spill files generated
/// while processing dataset larger than available memory.
pub struct DiskManager {
    local_dirs: Vec<TempDir>,
}

impl DiskManager {
    /// Create local dirs inside user provided dirs through conf
    pub fn new(conf_dirs: &[String]) -> Result<Self> {
        let local_dirs = create_local_dirs(conf_dirs)?;
        info!(
            "Created local dirs {:?} as DataFusion working directory",
            local_dirs
        );
        Ok(Self { local_dirs })
    }

    /// Create a file in conf dirs in randomized manner and return the file path
    pub fn create_tmp_file(&self) -> Result<String> {
        create_tmp_file(&self.local_dirs)
    }
}

/// Setup local dirs by creating one new dir in each of the given dirs
fn create_local_dirs(local_dir: &[String]) -> Result<Vec<TempDir>> {
    local_dir
        .iter()
        .map(|root| create_dir(root, "datafusion-"))
        .collect()
}

fn create_dir(root: &str, prefix: &str) -> Result<TempDir> {
    Builder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .map_err(DataFusionError::IoError)
}

fn get_file(file_name: &str, local_dirs: &[TempDir]) -> String {
    let mut hasher = DefaultHasher::new();
    file_name.hash(&mut hasher);
    let hash = hasher.finish();
    let dir = &local_dirs[hash.rem_euclid(local_dirs.len() as u64) as usize];
    let mut path = PathBuf::new();
    path.push(dir);
    path.push(file_name);
    path.to_str().unwrap().to_string()
}

fn create_tmp_file(local_dirs: &[TempDir]) -> Result<String> {
    let name = rand_name();
    let mut path = get_file(&*name, local_dirs);
    while Path::new(path.as_str()).exists() {
        path = get_file(&rand_name(), local_dirs);
    }
    File::create(&path)?;
    Ok(path)
}

/// Return a random string suitable for use as a database name
fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::execution::disk_manager::{get_file, DiskManager};
    use tempfile::TempDir;

    #[test]
    fn file_in_right_dir() -> Result<()> {
        let local_dir1 = TempDir::new()?;
        let local_dir2 = TempDir::new()?;
        let local_dir3 = TempDir::new()?;
        let local_dirs = vec![
            local_dir1.path().to_str().unwrap().to_string(),
            local_dir2.path().to_str().unwrap().to_string(),
            local_dir3.path().to_str().unwrap().to_string(),
        ];

        let dm = DiskManager::new(&local_dirs)?;
        let actual = dm.create_tmp_file()?;
        let name = actual.rsplit_once(std::path::MAIN_SEPARATOR).unwrap().1;

        let expected = get_file(name, &dm.local_dirs);
        // file should be located in dir by it's name hash
        assert_eq!(actual, expected);
        Ok(())
    }
}
