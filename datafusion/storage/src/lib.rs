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

//! Backend-independent file operations and storage registration.

pub mod path;
mod read;
mod registry;

pub use read::{FileReader, ReadRange};
pub use registry::{StorageBinding, StorageRegistry, StorageUrl};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::BoxStream};
use path::Path;
use std::{fmt::Debug, sync::Arc};
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;

/// An error independent of the selected storage SDK.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("File not found: {0}")]
    NotFound(String),
    #[error("Storage operation not supported: {0}")]
    NotSupported(String),
    #[error("Invalid file access: {0}")]
    InvalidInput(String),
    #[error(transparent)]
    Path(#[from] path::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{backend}: {source}")]
    Backend {
        backend: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// File metadata relative to a storage namespace.
///
/// ETags and versions are observations returned by the backend. Reading a file
/// does not automatically turn these fields into conditional requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub location: Path,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl FileInfo {
    /// Describe an explicitly supplied file without performing metadata I/O.
    pub fn new(location: Path, size: u64) -> Self {
        Self {
            location,
            size,
            last_modified: DateTime::UNIX_EPOCH,
            e_tag: None,
            version: None,
        }
    }
}

/// Access state scoped to one planning operation or query execution.
/// Backends may use it to associate their work with the calling query.
#[derive(Clone, Debug, Default)]
pub struct FileAccessContext {
    pub query_id: Arc<str>,
    pub cancellation: CancellationToken,
}

impl FileAccessContext {
    pub fn new(query_id: impl Into<Arc<str>>) -> Self {
        Self {
            query_id: query_id.into(),
            cancellation: CancellationToken::new(),
        }
    }
}

/// Immediate files and common directory prefixes returned by delimiter listing.
#[derive(Debug, Default)]
pub struct DirectoryListing {
    pub files: Vec<FileInfo>,
    pub directories: Vec<Path>,
}

/// Existing output buffering configuration, interpreted by the backend writer.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriterOptions {
    pub buffer_size: Option<usize>,
}

/// Output consumed by the existing format encoders and compression wrappers.
/// `shutdown` completes the output using the backend's normal write semantics.
pub type FileOutput = Box<dyn AsyncWrite + Send + Unpin>;

/// File operations within one storage namespace.
///
/// Implementations own clients, configuration, and backend-specific behavior.
/// Unsupported operations fail at the backend; registration does not probe
/// capabilities or select a different backend. Default errors allow read-only
/// implementations to omit operations they do not provide.
#[async_trait]
pub trait Storage: Debug + Send + Sync {
    /// Open a reusable reader without requiring a preceding metadata request.
    async fn open(
        &self,
        path: &Path,
        context: FileAccessContext,
    ) -> Result<Arc<dyn FileReader>>;

    async fn stat(&self, _path: &Path, _context: &FileAccessContext) -> Result<FileInfo> {
        Err(Error::NotSupported("stat".into()))
    }

    /// Recursively enumerate files matching a path prefix.
    fn list(
        &self,
        _prefix: &Path,
        _context: FileAccessContext,
    ) -> BoxStream<'_, Result<FileInfo>> {
        futures::stream::once(async { Err(Error::NotSupported("list".into())) }).boxed()
    }

    /// List direct children, preserving backend directory-pruning support.
    async fn list_with_delimiter(
        &self,
        _prefix: &Path,
        _context: &FileAccessContext,
    ) -> Result<DirectoryListing> {
        Err(Error::NotSupported("list_with_delimiter".into()))
    }

    async fn writer(
        &self,
        _path: &Path,
        _options: WriterOptions,
        _context: FileAccessContext,
    ) -> Result<FileOutput> {
        Err(Error::NotSupported("writer".into()))
    }
}

/// Collect a byte stream for formats that require a complete in-memory input.
pub async fn collect_bytes(
    mut stream: BoxStream<'_, Result<bytes::Bytes>>,
) -> Result<bytes::Bytes> {
    use futures::TryStreamExt;
    let mut bytes = bytes::BytesMut::new();
    while let Some(chunk) = stream.try_next().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}
