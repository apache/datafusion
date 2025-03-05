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
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion_common::Result;
use futures::AsyncWrite;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageWriteOptions {
    /// A version indicator for the newly created object
    pub version: Option<String>,
}

/// Result for a write request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageWriteResult {
    /// The unique identifier for the newly created object
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// A version indicator for the newly created object
    pub version: Option<String>,
}

#[async_trait]
pub trait StorageFileWrite: Send + Sync + 'static {
    /// Write a single chunk of data to the file
    async fn write(&mut self, ctx: &StorageContext, data: Bytes) -> Result<()>;

    /// Finish writing to the file and return the result
    async fn finish(&mut self, ctx: &StorageContext) -> Result<StorageWriteResult>;
}

pub struct StorageFileWriter {
    inner: Box<dyn StorageFileWrite>,
}

/// Expose public API for writing to a file
impl StorageFileWriter {
    pub fn into_future_writer(self) -> StorageFileFuturesWriter {
        todo!()
    }

    pub fn into_sink(self) -> StorageFileBytesSink {
        todo!()
    }

    pub fn into_tokio_writer(self) -> StorageFileTokioWriter {
        todo!()
    }
}

/// Adapter to allow using `futures::io::AsyncWrite` with `StorageFileWriter`
pub struct StorageFileFuturesWriter {}

/// Adapter to allow using `futures::io::Sink` with `StorageFileWriter`
pub struct StorageFileBytesSink {}

/// Adapter to allow using `tokio::io::AsyncWrite` with `StorageFileWriter`
pub struct StorageFileTokioWriter {}
