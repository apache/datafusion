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

//! Object Store abstracts access to an underlying file/object storage.

pub mod local;

use std::fmt::Debug;
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{AsyncRead, Stream, StreamExt};
use tokio::io::AsyncWrite;

use crate::{FileMeta, ListEntry, Result, SizedFile};

/// Stream of files listed from object store
pub type FileMetaStream =
    Pin<Box<dyn Stream<Item = Result<FileMeta>> + Send + Sync + 'static>>;

/// Stream of list entries obtained from object store
pub type ListEntryStream =
    Pin<Box<dyn Stream<Item = Result<ListEntry>> + Send + Sync + 'static>>;

/// Stream readers opened on a given object store
pub type ObjectReaderStream =
    Pin<Box<dyn Stream<Item = Result<Arc<dyn ObjectReader>>> + Send + Sync>>;

/// Object Reader for one file in an object store.
///
/// Note that the dynamic dispatch on the reader might
/// have some performance impacts.
#[async_trait]
pub trait ObjectReader: Send + Sync {
    /// Get reader for a part [start, start + length] in the file asynchronously
    async fn chunk_reader(&self, start: u64, length: usize)
        -> Result<Box<dyn AsyncRead>>;

    /// Get reader for a part [start, start + length] in the file
    fn sync_chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>>;

    /// Get reader for the entire file
    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.sync_chunk_reader(0, self.length() as usize)
    }

    /// Get the size of the file
    fn length(&self) -> u64;
}

/// Object Writer for one file in an object store.
#[async_trait]
pub trait ObjectWriter: Send + Sync {
    async fn writer(&self) -> Result<Pin<Box<dyn AsyncWrite>>>;

    fn sync_writer(&self) -> Result<Box<dyn Write + Send + Sync>>;
}

/// A ObjectStore abstracts access to an underlying file/object storage.
/// It maps strings (e.g. URLs, filesystem paths, etc) to sources of bytes
#[async_trait]
pub trait ObjectStore: Sync + Send + Debug {
    /// Returns all the files in path `prefix`
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream>;

    /// Calls `list_file` with a suffix filter
    async fn list_file_with_suffix(
        &self,
        prefix: &str,
        suffix: &str,
    ) -> Result<FileMetaStream> {
        let file_stream = self.list_file(prefix).await?;
        let suffix = suffix.to_owned();
        Ok(Box::pin(file_stream.filter(move |fr| {
            let has_suffix = match fr {
                Ok(f) => f.path().ends_with(&suffix),
                Err(_) => true,
            };
            async move { has_suffix }
        })))
    }

    /// Returns all the files in `prefix` if the `prefix` is already a leaf dir,
    /// or all paths between the `prefix` and the first occurrence of the `delimiter` if it is provided.
    async fn list_dir(
        &self,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Result<ListEntryStream>;

    /// Get object reader for one file
    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>>;

    /// Get object writer for one file
    fn file_writer(&self, path: &str) -> Result<Arc<dyn ObjectWriter>>;

    /// Create directory, recursively if requested
    ///
    /// If directory already exists, will return Ok
    async fn create_dir(&self, path: &str, recursive: bool) -> Result<()>;

    /// Delete directory and its contents, recursively
    async fn remove_dir_all(&self, path: &str) -> Result<()>;

    /// Delete directory contents recursively
    ///
    /// Unlike [delete_dir], will not delete directory itself
    async fn remove_dir_contents(&self, path: &str) -> Result<()>;

    /// Delete a file
    ///
    /// If file does not exist, will return error kind [std::io::ErrorKind::NotFound]
    /// If attempted on a directory, will return error kind [std::io::ErrorKind::InvalidInput]
    async fn remove_file(&self, path: &str) -> Result<()>;

    /// Rename a file or directory
    ///
    /// If dest exists, source will replace it unless dest is a non-empty directory,
    /// in which case an [std::io::ErrorKind::AlreadyExists] or
    /// [std::io::ErrorKind::DirectoryNotEmpty] error will be returned
    async fn rename(&self, source: &str, dest: &str) -> Result<()>;

    /// Copy a file or directory
    ///
    /// If the destination exists and is a directory, an error is returned.
    /// Otherwise, it is replaced.
    async fn copy(&self, source: &str, dest: &str) -> Result<()>;
}

// TODO: Document below when we do and do not expect a scheme
/// Return path without scheme
///
/// # Examples
///
/// ```
/// let path = "file://path/to/object";
/// assert_eq(path_without_scheme(path), "path/to/object");
/// ```
pub fn path_without_scheme(full_path: &str) -> &str {
    if let Some((_scheme, path)) = full_path.split_once("://") {
        path
    } else {
        full_path
    }
}
