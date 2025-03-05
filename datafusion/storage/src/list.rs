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

use crate::context::StorageContext;
use crate::file_metadata::StorageFileMetadata;
use crate::StorageWriteResult;
use async_trait::async_trait;
use bytes::Bytes;

pub struct StorageListOptions {
    pub recursive: bool,
}

#[async_trait]
pub trait StorageFileList: Send + Sync + 'static {
    /// Write a single chunk of data to the file
    async fn next(
        &mut self,
        ctx: &StorageContext,
    ) -> datafusion_common::Result<StorageFileMetadata>;
}

pub struct StorageFileLister {
    location: String,
    inner: Box<dyn StorageFileList>,
}

impl StorageFileLister {
    pub fn into_stream(self) -> StorageFileMetadataStream {
        StorageFileMetadataStream {}
    }
}

/// Adapter to allow using `futures::Stream` with `StorageFileLister`
pub struct StorageFileMetadataStream {}
