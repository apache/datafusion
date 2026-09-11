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

//! Storage fixtures shared by format and execution tests.
use datafusion_storage::{StorageBinding, StorageUrl};
use datafusion_storage_object_store::ObjectStoreStorage;
use std::sync::Arc;

/// Bind a fixture backend to the local namespace used by format tests.
pub fn object_store(store: Arc<dyn object_store::ObjectStore>) -> Arc<StorageBinding> {
    Arc::new(StorageBinding::new(
        StorageUrl::local_filesystem(),
        Arc::new(ObjectStoreStorage::new(store)),
    ))
}

pub fn local() -> Arc<StorageBinding> {
    object_store(Arc::new(object_store::local::LocalFileSystem::new()))
}

/// Convert SDK fixture metadata at the adapter boundary.
pub use datafusion_storage_object_store::file_info;
