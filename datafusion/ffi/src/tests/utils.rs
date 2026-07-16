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

/// Locate the built `datafusion_ffi` cdylib.
///
/// The cdylib sits next to the running test binary, so this follows Cargo's
/// actual output directory and is robust to the active profile and a custom
/// `--target-dir` (e.g. `cargo llvm-cov`).
fn find_library() -> Result<PathBuf> {
    let exe =
        std::env::current_exe().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let deps_dir = exe.parent().ok_or_else(|| {
        DataFusionError::External("Failed to find test binary directory".into())
    })?;
    find_cdylib(deps_dir)
}

pub fn get_module() -> Result<ForeignLibraryModule> {
    let expected_version = crate::version();

    let lib_path = find_library()?;

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
