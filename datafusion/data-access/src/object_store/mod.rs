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
use std::path;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::ready;
use futures::{AsyncRead, Stream, StreamExt, TryStreamExt};
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
        if !contains_glob_start_char(glob_pattern) {
            self.list_file(glob_pattern).await
        } else {
            let normalized_glob_pb = normalize_path(Path::new(glob_pattern));
            let normalized_glob_pattern =
                normalized_glob_pb.as_os_str().to_str().unwrap();
            let start_path =
                find_longest_search_path_without_glob_pattern(normalized_glob_pattern);
            let file_stream = self.list_file(&start_path).await?;
            let pattern = Pattern::new(normalized_glob_pattern).unwrap();
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
        let files_to_consider = match contains_glob_start_char(glob_pattern) {
            true => self.glob_file(glob_pattern).await,
            false => self.list_file(glob_pattern).await,
        }?;

        match suffix.is_empty() {
            true => Ok(files_to_consider),
            false => filter_suffix(files_to_consider, suffix),
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

/// Normalize a path without requiring it to exist on the filesystem (path::canonicalize)
pub fn normalize_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let ends_with_slash = path
        .as_ref()
        .to_str()
        .map_or(false, |s| s.ends_with(path::MAIN_SEPARATOR));
    let mut normalized = PathBuf::new();
    for component in path.as_ref().components() {
        match &component {
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component);
                }
            }
            _ => {
                normalized.push(component);
            }
        }
    }
    if ends_with_slash {
        normalized.push("");
    }
    normalized
}

const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Determine whether the path contains a globbing character
fn contains_glob_start_char(path: &str) -> bool {
    path.chars().any(|c| GLOB_START_CHARS.contains(&c))
}

/// Filters the file_stream to only contain files that end with suffix
fn filter_suffix(file_stream: FileMetaStream, suffix: &str) -> Result<FileMetaStream> {
    let suffix = suffix.to_owned();
    Ok(Box::pin(
        file_stream.try_filter(move |f| ready(f.path().ends_with(&suffix))),
    ))
}

fn find_longest_search_path_without_glob_pattern(glob_pattern: &str) -> String {
    // in case the glob_pattern is not actually a glob pattern, take the entire thing
    if !contains_glob_start_char(glob_pattern) {
        glob_pattern.to_string()
    } else {
        // take all the components of the path (left-to-right) which do not contain a glob pattern
        let components_in_glob_pattern = Path::new(glob_pattern).components();
        let mut path_buf_for_longest_search_path_without_glob_pattern = PathBuf::new();
        for component_in_glob_pattern in components_in_glob_pattern {
            let component_as_str =
                component_in_glob_pattern.as_os_str().to_str().unwrap();
            if contains_glob_start_char(component_as_str) {
                break;
            }
            path_buf_for_longest_search_path_without_glob_pattern
                .push(component_in_glob_pattern);
        }

        let mut result = path_buf_for_longest_search_path_without_glob_pattern
            .to_str()
            .unwrap()
            .to_string();

        // when we're not at the root, append a separator
        if path_buf_for_longest_search_path_without_glob_pattern
            .components()
            .count()
            > 1
        {
            result.push(path::MAIN_SEPARATOR);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_is_glob_path() -> Result<()> {
        assert!(!contains_glob_start_char("/"));
        assert!(!contains_glob_start_char("/test"));
        assert!(!contains_glob_start_char("/test/"));
        assert!(contains_glob_start_char("/test*"));
        Ok(())
    }

    fn test_longest_base_path(input: &str, expected: &str) {
        assert_eq!(
            find_longest_search_path_without_glob_pattern(input),
            expected,
            "testing find_longest_search_path_without_glob_pattern with {}",
            input
        );
    }

    #[tokio::test]
    async fn test_find_longest_search_path_without_glob_pattern() -> Result<()> {
        // no glob patterns, thus we get the full path (as-is)
        test_longest_base_path("/", "/");
        test_longest_base_path("/a.txt", "/a.txt");
        test_longest_base_path("/a", "/a");
        test_longest_base_path("/a/", "/a/");
        test_longest_base_path("/a/b", "/a/b");
        test_longest_base_path("/a/b/", "/a/b/");
        test_longest_base_path("/a/b.txt", "/a/b.txt");
        test_longest_base_path("/a/b/c.txt", "/a/b/c.txt");
        // glob patterns, thus we build the longest path (os-specific)
        use path::MAIN_SEPARATOR;
        test_longest_base_path("/*.txt", &format!("{MAIN_SEPARATOR}"));
        test_longest_base_path(
            "/a/*b.txt",
            &format!("{MAIN_SEPARATOR}a{MAIN_SEPARATOR}"),
        );
        test_longest_base_path(
            "/a/*/b.txt",
            &format!("{MAIN_SEPARATOR}a{MAIN_SEPARATOR}"),
        );
        test_longest_base_path(
            "/a/b/[123]/file*.txt",
            &format!("{MAIN_SEPARATOR}a{MAIN_SEPARATOR}b{MAIN_SEPARATOR}"),
        );
        test_longest_base_path(
            "/a/b*.txt",
            &format!("{MAIN_SEPARATOR}a{MAIN_SEPARATOR}"),
        );
        test_longest_base_path(
            "/a/b/**/c*.txt",
            &format!("{MAIN_SEPARATOR}a{MAIN_SEPARATOR}b{MAIN_SEPARATOR}"),
        );
        test_longest_base_path(
            &format!("{}/alltypes_plain*.parquet", "/a/b/c//"), // https://github.com/apache/arrow-datafusion/issues/2465
            &format!(
                "{MAIN_SEPARATOR}a{MAIN_SEPARATOR}b{MAIN_SEPARATOR}c{MAIN_SEPARATOR}"
            ),
        );
        Ok(())
    }
}
