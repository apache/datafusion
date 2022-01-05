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
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Manages files generated during query execution, e.g. spill files generated
/// while processing dataset larger than available memory.
pub struct DiskManager {
    local_dirs: Vec<String>,
}

impl DiskManager {
    /// Create local dirs inside user provided dirs through conf
    pub fn new(conf_dirs: &[String]) -> Result<Self> {
        Ok(Self {
            local_dirs: create_local_dirs(conf_dirs)?,
        })
    }

    /// Create a file in conf dirs in randomized manner and return the file path
    pub fn create_tmp_file(&self) -> Result<String> {
        create_tmp_file(&self.local_dirs)
    }

    #[allow(dead_code)]
    fn cleanup_resource(&mut self) -> Result<()> {
        for dir in self.local_dirs.drain(..) {
            fs::remove_dir(dir)?;
        }
        Ok(())
    }
}

/// Setup local dirs by creating one new dir in each of the given dirs
fn create_local_dirs(local_dir: &[String]) -> Result<Vec<String>> {
    local_dir
        .iter()
        .map(|root| create_directory(root, "datafusion"))
        .collect()
}

const MAX_DIR_CREATION_ATTEMPTS: i32 = 10;

fn create_directory(root: &str, prefix: &str) -> Result<String> {
    let mut attempt = 0;
    while attempt < MAX_DIR_CREATION_ATTEMPTS {
        let mut path = PathBuf::from(root);
        path.push(format!("{}-{}", prefix, Uuid::new_v4().to_string()));
        let path = path.as_path();
        if !path.exists() {
            fs::create_dir(path)?;
            return Ok(path.canonicalize().unwrap().to_str().unwrap().to_string());
        }
        attempt += 1;
    }
    Err(DataFusionError::Execution(format!(
        "Failed to create a temp dir under {} after {} attempts",
        root, MAX_DIR_CREATION_ATTEMPTS
    )))
}

fn get_file(file_name: &str, local_dirs: &[String]) -> String {
    let mut hasher = DefaultHasher::new();
    file_name.hash(&mut hasher);
    let hash = hasher.finish();
    let dir = &local_dirs[hash.rem_euclid(local_dirs.len() as u64) as usize];
    let mut path = PathBuf::new();
    path.push(dir);
    path.push(file_name);
    path.to_str().unwrap().to_string()
}

fn create_tmp_file(local_dirs: &[String]) -> Result<String> {
    let name = Uuid::new_v4().to_string();
    let mut path = get_file(&*name, local_dirs);
    while Path::new(path.as_str()).exists() {
        path = get_file(&*Uuid::new_v4().to_string(), local_dirs);
    }
    File::create(&path)?;
    Ok(path)
}
