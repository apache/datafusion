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

mod api;
pub use api::Storage;

mod file_metadata;
pub use file_metadata::StorageFileMetadata;

mod list;

mod read;
pub use read::ReadRange;
pub use read::StorageReadOptions;

mod write;
pub use write::StorageFileWrite;
pub use write::StorageFileWriter;
pub use write::StorageWriteOptions;
pub use write::StorageWriteResult;

mod context;
mod error;
mod registry;
mod stat;

pub use registry::DefaultStorageRegistry;
pub use registry::StorageRegistry;
pub use registry::StorageUrl;
