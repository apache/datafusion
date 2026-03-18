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

use std::path::{Path, PathBuf};

use datafusion_common::{DataFusionError, Result};

use crate::tests::ForeignLibraryModule;

/// Compute the path to the built cdylib. Checks debug, release, and ci profile dirs.
fn compute_library_dir(target_path: &Path) -> PathBuf {
    let debug_dir = target_path.join("debug");
    let release_dir = target_path.join("release");
    let ci_dir = target_path.join("ci");

    let all_dirs = vec![debug_dir.clone(), release_dir, ci_dir];

    all_dirs
        .into_iter()
        .filter(|dir| dir.join("deps").exists())
        .filter_map(|dir| {
            dir.join("deps")
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .map(|date| (dir, date))
        })
        .max_by_key(|(_, date)| *date)
        .map(|(dir, _)| dir)
        .unwrap_or(debug_dir)
}

/// Find the cdylib file for datafusion_ffi in the given directory.
fn find_cdylib(deps_dir: &Path) -> Result<PathBuf> {
    let lib_prefix = if cfg!(target_os = "windows") {
        ""
    } else {
        "lib"
    };
    let lib_ext = if cfg!(target_os = "macos") {
        "dylib"
    } else if cfg!(target_os = "windows") {
        "dll"
    } else {
        "so"
    };

    let pattern = format!("{lib_prefix}datafusion_ffi.{lib_ext}");
    let lib_path = deps_dir.join(&pattern);

    if lib_path.exists() {
        return Ok(lib_path);
    }

    Err(DataFusionError::External(
        format!("Could not find library at {}", lib_path.display()).into(),
    ))
}

pub fn get_module() -> Result<ForeignLibraryModule> {
    let expected_version = crate::version();

    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_dir = crate_root
        .parent()
        .expect("Failed to find crate parent")
        .parent()
        .expect("Failed to find workspace root")
        .join("target");

    let library_dir = compute_library_dir(target_dir.as_path());
    let lib_path = find_cdylib(&library_dir.join("deps"))?;

    // Load the library using libloading
    let lib = unsafe {
        libloading::Library::new(&lib_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    };

    let get_module: libloading::Symbol<extern "C" fn() -> ForeignLibraryModule> = unsafe {
        lib.get(b"datafusion_ffi_get_module")
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    };

    let module = get_module();

    assert_eq!((module.version)(), expected_version);

    // Leak the library to keep it loaded for the duration of the test
    std::mem::forget(lib);

    Ok(module)
}
