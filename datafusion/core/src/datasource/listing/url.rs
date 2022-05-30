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

use crate::datasource::object_store::ObjectStoreUrl;
use datafusion_common::{DataFusionError, Result};
use datafusion_data_access::object_store::ObjectStore;
use datafusion_data_access::FileMeta;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use std::path::is_separator;
use url::Url;

/// A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]
/// for more information on the supported expressions
#[derive(Debug, Clone)]
pub struct ListingTableUrl {
    /// A URL that identifies a file or directory to list files from
    url: Url,
    /// An optional glob expression used to filter files
    glob: Option<Pattern>,
}

impl ListingTableUrl {
    /// Parse a provided string as a `ListingTableUrl`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// If the path contains any of `'?', '*', '['`, it will be considered
    /// a glob expression and resolved as described in the section below.
    ///
    /// Otherwise, the path will be resolved to an absolute path, returning
    /// an error if it does not exist, and converted to a [file URI]
    ///
    /// If you wish to specify a path that does not exist on the local
    /// machine you must provide it as a fully-qualified [file URI]
    /// e.g. `file:///myfile.txt`
    ///
    /// ## Glob File Paths
    ///
    /// If no scheme is provided, and the path contains a glob expression, it will
    /// be resolved as follows.
    ///
    /// The string up to the first path segment containing a glob expression will be extracted,
    /// and resolved in the same manner as a normal scheme-less path. That is, resolved to
    /// an absolute path on the local filesystem, returning an error if it does not exist,
    /// and converted to a [file URI]
    ///
    /// The remaining string will be interpreted as a [`glob::Pattern`] and used as a
    /// filter when listing files from object storage
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self { url, glob: None }),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// Creates a new [`ListingTableUrl`] interpreting `s` as a filesystem path
    fn parse_path(s: &str) -> Result<Self> {
        let (prefix, glob) = match split_glob_expression(s) {
            Some((prefix, glob)) => {
                let glob = Pattern::new(glob)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (prefix, Some(glob))
            }
            None => (s, None),
        };

        let path = std::path::Path::new(prefix).canonicalize()?;
        let url = match path.is_file() {
            true => Url::from_file_path(path).unwrap(),
            false => Url::from_directory_path(path).unwrap(),
        };

        Ok(Self { url, glob })
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Returns the path as expected by [`ObjectStore`]
    ///
    /// In particular for file scheme URLs, this is an absolute
    /// on the local filesystem in the OS-specific path representation
    ///
    /// For other URLs, this is a the host and path of the URL,
    /// delimited by `/`, and with no leading `/`
    ///
    /// TODO: Handle paths consistently (#2489)
    fn prefix(&self) -> &str {
        match self.scheme() {
            "file" => match cfg!(target_family = "windows") {
                true => self.url.path().strip_prefix('/').unwrap(),
                false => self.url.path(),
            },
            _ => &self.url[url::Position::BeforeHost..url::Position::AfterPath],
        }
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    ///
    /// TODO: Handle paths consistently (#2489)
    pub(crate) fn strip_prefix<'a, 'b: 'a>(
        &'a self,
        path: &'b str,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        let prefix = self.prefix();
        // Ignore empty path segments
        let diff = itertools::diff_with(
            path.split(is_separator).filter(|s| !s.is_empty()),
            prefix.split(is_separator).filter(|s| !s.is_empty()),
            |a, b| a == b,
        );

        match diff {
            // Match with remaining
            Some(itertools::Diff::Shorter(_, subpath)) => Some(subpath),
            _ => None,
        }
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub(crate) fn list_all_files<'a>(
        &'a self,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> BoxStream<'a, Result<FileMeta>> {
        futures::stream::once(async move {
            let prefix = self.prefix();
            store.list_file(prefix.as_ref()).await
        })
        .try_flatten()
        .map_err(DataFusionError::IoError)
        .try_filter(move |meta| {
            let path = meta.path();

            let extension_match = path.ends_with(file_extension);
            let glob_match = match &self.glob {
                Some(glob) => match self.strip_prefix(path) {
                    Some(mut segments) => {
                        let stripped = segments.join("/");
                        glob.matches(&stripped)
                    }
                    None => false,
                },
                None => true,
            };

            futures::future::ready(extension_match && glob_match)
        })
        .boxed()
    }

    /// Returns this [`ListingTableUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Return the [`ObjectStoreUrl`] for this [`ListingTableUrl`]
    pub fn object_store(&self) -> ObjectStoreUrl {
        let url = &self.url[url::Position::BeforeScheme..url::Position::BeforePath];
        ObjectStoreUrl::parse(url).unwrap()
    }
}

impl AsRef<str> for ListingTableUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for ListingTableUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for ListingTableUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
    let mut last_separator = 0;

    for (byte_idx, char) in path.char_indices() {
        if GLOB_START_CHARS.contains(&char) {
            if last_separator == 0 {
                return Some((".", path));
            }
            return Some(path.split_at(last_separator));
        }

        if std::path::is_separator(char) {
            last_separator = byte_idx + char.len_utf8();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_path() {
        let root = std::env::current_dir().unwrap();
        let root = root.to_string_lossy();

        let url = ListingTableUrl::parse(&root).unwrap();
        let child = format!("{}/partition/file", root);

        let prefix: Vec<_> = url.strip_prefix(&child).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "file"]);
    }

    #[test]
    fn test_prefix_s3() {
        let url = ListingTableUrl::parse("s3://bucket/foo/bar").unwrap();
        assert_eq!(url.prefix(), "bucket/foo/bar");

        let path = "bucket/foo/bar/partition/foo.parquet";
        let prefix: Vec<_> = url.strip_prefix(path).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "foo.parquet"]);

        let path = "other-bucket/foo/bar/partition/foo.parquet";
        assert!(url.strip_prefix(path).is_none());
    }

    #[test]
    fn test_split_glob() {
        fn test(input: &str, expected: Option<(&str, &str)>) {
            assert_eq!(
                split_glob_expression(input),
                expected,
                "testing split_glob_expression with {}",
                input
            );
        }

        // no glob patterns
        test("/", None);
        test("/a.txt", None);
        test("/a", None);
        test("/a/", None);
        test("/a/b", None);
        test("/a/b/", None);
        test("/a/b.txt", None);
        test("/a/b/c.txt", None);
        // glob patterns, thus we build the longest path (os-specific)
        test("*.txt", Some((".", "*.txt")));
        test("/*.txt", Some(("/", "*.txt")));
        test("/a/*b.txt", Some(("/a/", "*b.txt")));
        test("/a/*/b.txt", Some(("/a/", "*/b.txt")));
        test("/a/b/[123]/file*.txt", Some(("/a/b/", "[123]/file*.txt")));
        test("/a/b*.txt", Some(("/a/", "b*.txt")));
        test("/a/b/**/c*.txt", Some(("/a/b/", "**/c*.txt")));

        // https://github.com/apache/arrow-datafusion/issues/2465
        test(
            "/a/b/c//alltypes_plain*.parquet",
            Some(("/a/b/c//", "alltypes_plain*.parquet")),
        );
    }
}
