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
//! Object store implementation used for testing
use crate::prelude::SessionContext;
use futures::FutureExt;
use object_store::{memory::InMemory, path::Path, ObjectMeta, ObjectStore};
use std::sync::Arc;

/// Returns a test object store with the provided `ctx`
pub fn register_test_store(ctx: &SessionContext, files: &[(&str, u64)]) {
    ctx.runtime_env()
        .register_object_store("test", "", make_test_store(files));
}

/// Create a test object store with the provided files
pub fn make_test_store(files: &[(&str, u64)]) -> Arc<dyn ObjectStore> {
    let memory = InMemory::new();

    for (name, size) in files {
        memory
            .put(&Path::from(*name), vec![0; *size as usize].into())
            .now_or_never()
            .unwrap()
            .unwrap();
    }

    Arc::new(memory)
}

/// Helper method to fetch the file size and date at given path and create a `ObjectMeta`
pub fn local_unpartitioned_file(path: impl AsRef<std::path::Path>) -> ObjectMeta {
    let location = Path::from_filesystem_path(path.as_ref()).unwrap();
    let metadata = std::fs::metadata(path).expect("Local file metadata");
    ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len() as usize,
    }
}
