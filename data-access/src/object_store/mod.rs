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
use std::path::Path;
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
        self.glob_file_with_suffix(prefix, suffix).await
    }

    /// Returns all the files matching `glob_pattern`
    async fn glob_file(&self, glob_pattern: &str) -> Result<FileMetaStream> {
        if !is_glob_path(glob_pattern) {
            self.list_file(glob_pattern).await
        } else {
            let start_path = find_longest_base_path(glob_pattern);
            let file_stream = self.list_file(start_path).await?;
            let pattern = Pattern::new(glob_pattern).unwrap();
            Ok(Box::pin(file_stream.filter(move |fr| {
                let matches_pattern = match fr {
                    Ok(f) => pattern.matches(f.path()),
                    Err(_) => true,
                };
                async move { matches_pattern }
            })))
        }
    }

    /// Calls `glob_file` with a suffix filter
    async fn glob_file_with_suffix(
        &self,
        glob_pattern: &str,
        suffix: &str,
    ) -> Result<FileMetaStream> {
        let files_to_consider = match is_glob_path(glob_pattern) {
            true => self.glob_file(glob_pattern).await,
            false => self.list_file(glob_pattern).await,
        }?;

        match suffix.is_empty() {
            true => Ok(files_to_consider),
            false => filter_suffix(files_to_consider, suffix).await,
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

const GLOB_CHARS: [char; 7] = ['{', '}', '[', ']', '*', '?', '\\'];

/// Determine whether the path contains a globbing character
fn is_glob_path(path: &str) -> bool {
    path.chars().any(|c| GLOB_CHARS.contains(&c))
}

/// Filters the file_stream to only contain files that end with suffix
async fn filter_suffix(
    file_stream: FileMetaStream,
    suffix: &str,
) -> Result<FileMetaStream> {
    let suffix = suffix.to_owned();
    Ok(Box::pin(file_stream.filter(move |fr| {
        let has_suffix = match fr {
            Ok(f) => f.path().ends_with(&suffix),
            Err(_) => true,
        };
        async move { has_suffix }
    })))
}

fn find_longest_base_path(glob_pattern: &str) -> &str {
    // in case the glob_pattern is not actually a glob pattern, take the entire thing
    if !is_glob_path(&glob_pattern) {
        glob_pattern //.to_string()
    } else {
        // take path up to first occurence of a glob char
        let path_to_first_glob_character = glob_pattern
            .splitn(2, |c| GLOB_CHARS.contains(&c))
            .collect::<Vec<&str>>()[0]; // always find one, because otherwise is_glob_pattern would not be true
        let path = Path::new(path_to_first_glob_character);

        let dir_path = if path.is_file() {
            path.parent().unwrap_or(Path::new("/"))
        } else {
            path
        };
        dir_path.to_str().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_is_glob_path() -> Result<()> {
        assert!(!is_glob_path("/"));
        assert!(!is_glob_path("/test"));
        assert!(!is_glob_path("/test/"));
        assert!(is_glob_path("/test*"));
        Ok(())
    }

    #[tokio::test]
    async fn test_find_longest_base_path() -> Result<()> {
        assert_eq!(
            find_longest_base_path("/"),
            "/",
            "testing longest_path with {}",
            "/"
        );
        assert_eq!(
            find_longest_base_path("/a.txt"),
            "/a.txt",
            "testing longest_path with {}",
            "/a.txt"
        );
        assert_eq!(
            find_longest_base_path("/a"),
            "/a",
            "testing longest_path with {}",
            "/a"
        );
        assert_eq!(
            find_longest_base_path("/a/"),
            "/a/",
            "testing longest_path with {}",
            "/a/"
        );
        assert_eq!(
            find_longest_base_path("/a/b"),
            "/a/b",
            "testing longest_path with {}",
            "/a/b"
        );
        assert_eq!(
            find_longest_base_path("/a/b/"),
            "/a/b/",
            "testing longest_path with {}",
            "/a/b/"
        );
        assert_eq!(
            find_longest_base_path("/a/b.txt"),
            "/a/b.txt",
            "testing longest_path with {}",
            "/a/bt.xt"
        );
        assert_eq!(
            find_longest_base_path("/a/b/c.txt"),
            "/a/b/c.txt",
            "testing longest_path with {}",
            "/a/b/c.txt"
        );
        assert_eq!(
            find_longest_base_path("/*.txt"),
            "/",
            "testing longest_path with {}",
            "/*.txt"
        );
        assert_eq!(
            find_longest_base_path("/a/*b.txt"),
            "/a/",
            "testing longest_path with {}",
            "/a/*b.txt"
        );
        assert_eq!(
            find_longest_base_path("/a/*/b.txt"),
            "/a/",
            "testing longest_path with {}",
            "/a/*/b.txt"
        );
        assert_eq!(
            find_longest_base_path("/a/b/[123]/file*.txt"),
            "/a/b/",
            "testing longest_path with {}",
            "/a/b/[123]/file*.txt"
        );
        Ok(())
    }
}
