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

use std::path::Path;

use abi_stable::library::RootModule;
use datafusion_common::{DataFusionError, Result};

use crate::tests::ForeignLibraryModuleRef;

/// Compute the path to the library. It would be preferable to simply use
/// abi_stable::library::development_utils::compute_library_path however
/// our current CI pipeline has a `ci` profile that we need to use to
/// find the library.
pub fn compute_library_path<M: RootModule>(
    target_path: &Path,
) -> std::io::Result<std::path::PathBuf> {
    let debug_dir = target_path.join("debug");
    let release_dir = target_path.join("release");
    let ci_dir = target_path.join("ci");

    let debug_path = M::get_library_path(&debug_dir.join("deps"));
    let release_path = M::get_library_path(&release_dir.join("deps"));
    let ci_path = M::get_library_path(&ci_dir.join("deps"));

    let all_paths = vec![
        (debug_dir.clone(), debug_path),
        (release_dir, release_path),
        (ci_dir, ci_path),
    ];

    let best_path = all_paths
        .into_iter()
        .filter(|(_, path)| path.exists())
        .filter_map(|(dir, path)| path.metadata().map(|m| (dir, m)).ok())
        .filter_map(|(dir, meta)| meta.modified().map(|m| (dir, m)).ok())
        .max_by_key(|(_, date)| *date)
        .map(|(dir, _)| dir)
        .unwrap_or(debug_dir);

    Ok(best_path)
}

pub fn get_module() -> Result<ForeignLibraryModuleRef> {
    let expected_version = crate::version();

    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_dir = crate_root
        .parent()
        .expect("Failed to find crate parent")
        .parent()
        .expect("Failed to find workspace root")
        .join("target");

    // Find the location of the library. This is specific to the build environment,
    // so you will need to change the approach here based on your use case.
    // let target: &std::path::Path = "../../../../target/".as_ref();
    let library_path =
        compute_library_path::<ForeignLibraryModuleRef>(target_dir.as_path())
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .join("deps");

    // Load the module
    let module = ForeignLibraryModuleRef::load_from_directory(&library_path)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    assert_eq!(
        module
            .version()
            .expect("Unable to call version on FFI module")(),
        expected_version
    );

    Ok(module)
}
