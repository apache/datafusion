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
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{AsyncRead, Stream, StreamExt};
use glob::Pattern;

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
        /* could use this, but it's slower, so lets' stick with the original
        if prefix.ends_with(suffix) {
            self.list_file(prefix).await
        } else {
            if(prefix.ends_with("/")) {
                let path = format!("{}*{}", prefix, suffix);
                self.glob_file(&path).await
            } else {
                let path = format!("{}/**/
*{}", prefix, suffix);
                self.glob_file(&path).await
            }
        }*/
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

    /// Calls `list_file` with a glob_pattern
    async fn glob_file(&self, path: &str) -> Result<FileMetaStream> {
        const GLOB_CHARS: [char; 7] = ['{', '}', '[', ']', '*', '?', '\\'];

        /// Determine whether the path contains a globbing character
        fn is_glob_path(path: &str) -> bool {
            path.chars().any(|c| GLOB_CHARS.contains(&c))
        }

        if !is_glob_path(path) {
            let result = self.list_file(path).await?;
            Ok(result)
        } else {
            // take path up to first occurence of a glob char
            let path_to_first_glob_character: Vec<&str> =
                path.splitn(2, |c| GLOB_CHARS.contains(&c)).collect();
            // find last occurrence of folder /
            let path_parts: Vec<&str> = path_to_first_glob_character[0]
                .rsplitn(2, |c| c == '/')
                .collect();
            let start_path = path_parts[1];

            let file_stream = self.list_file(start_path).await?;
            let pattern = Pattern::new(path).unwrap();
            Ok(Box::pin(file_stream.filter(move |fr| {
                let matches_pattern = match fr {
                    Ok(f) => pattern.matches(f.path()),
                    Err(_) => true,
                };
                async move { matches_pattern }
            })))
        }
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
}
