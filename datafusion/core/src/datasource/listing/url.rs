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
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use home;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use percent_encoding;
use std::env;
use url::Url;

/// A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]
/// for more information on the supported expressions
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListingTableUrl {
    /// A URL that identifies a file or directory to list files from
    url: Url,
    /// The path prefix
    prefix: Path,
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
            Ok(url) => Ok(Self::new(url, None)),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// Perform shell-like path expansions
    /// * Home directory expansion: "~/test.csv" expands to "/Users/user1/test.csv"
    /// * Environment variable expansion: "$HOME/$DATA/test.csv" expands to
    /// "/Users/user1/data/test.csv"
    fn expand_path_prefix(path: &str) -> Result<String, DataFusionError> {
        let home_dir = home::home_dir()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();

        let path = path.replace('~', &home_dir);

        let parts = path.split("/").collect::<Vec<_>>();
        let mut expanded_parts = Vec::new();

        for part in parts {
            if part.starts_with('$') {
                let envvar_name = &part[1..];
                match env::var(envvar_name) {
                    Ok(value) => expanded_parts.push(value),
                    Err(_) => {
                        return Err(DataFusionError::Internal(format!(
                            "Failed to perform shell expansion in path: {}\nUnable to find environment variable ${}",
                            path, envvar_name
                        )))
                    }
                }
            } else {
                expanded_parts.push(part.to_string());
            }
        }

        Ok(expanded_parts.join("/"))
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

        // Expand path like "~/$DATA/test.csv" (not supported by std::path)
        let prefix = if !cfg!(target_os = "windows")
            && (prefix.starts_with('~') || prefix.contains('$'))
        {
            Self::expand_path_prefix(prefix)?
        } else {
            prefix.to_string()
        };

        let path = std::path::Path::new(&prefix).canonicalize()?;
        let url = if path.is_dir() {
            Url::from_directory_path(path)
        } else {
            Url::from_file_path(path)
        }
        .map_err(|_| DataFusionError::Internal(format!("Can not open path: {s}")))?;
        // TODO: Currently we do not have an IO-related error variant that accepts ()
        //       or a string. Once we have such a variant, change the error type above.

        Ok(Self::new(url, glob))
    }

    /// Creates a new [`ListingTableUrl`] from a url and optional glob expression
    fn new(url: Url, glob: Option<Pattern>) -> Self {
        let decoded_path =
            percent_encoding::percent_decode_str(url.path()).decode_utf8_lossy();
        let prefix = Path::from(decoded_path.as_ref());
        Self { url, prefix, glob }
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Return the prefix from which to list files
    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// Returns `true` if `path` matches this [`ListingTableUrl`]
    pub fn contains(&self, path: &Path) -> bool {
        match self.strip_prefix(path) {
            Some(mut segments) => match &self.glob {
                Some(glob) => {
                    let stripped = segments.join("/");
                    glob.matches(&stripped)
                }
                None => true,
            },
            None => false,
        }
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    pub(crate) fn strip_prefix<'a, 'b: 'a>(
        &'a self,
        path: &'b Path,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        use object_store::path::DELIMITER;
        let mut stripped = path.as_ref().strip_prefix(self.prefix.as_ref())?;
        if !stripped.is_empty() && !self.prefix.as_ref().is_empty() {
            stripped = stripped.strip_prefix(DELIMITER)?;
        }
        Some(stripped.split_terminator(DELIMITER))
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub(crate) fn list_all_files<'a>(
        &'a self,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> BoxStream<'a, Result<ObjectMeta>> {
        // If the prefix is a file, use a head request, otherwise list
        let is_dir = self.url.as_str().ends_with('/');
        let list = match is_dir {
            true => futures::stream::once(store.list(Some(&self.prefix)))
                .try_flatten()
                .boxed(),
            false => futures::stream::once(store.head(&self.prefix)).boxed(),
        };

        list.map_err(Into::into)
            .try_filter(move |meta| {
                let path = &meta.location;
                let extension_match = path.as_ref().ends_with(file_extension);
                let glob_match = self.contains(path);
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
    use crate::prelude::*;

    #[test]
    fn test_prefix_path() {
        let root = std::env::current_dir().unwrap();
        let root = root.to_string_lossy();

        let url = ListingTableUrl::parse(root).unwrap();
        let child = url.prefix.child("partition").child("file");

        let prefix: Vec<_> = url.strip_prefix(&child).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "file"]);

        let url = ListingTableUrl::parse("file:///").unwrap();
        let child = Path::parse("/foo/bar").unwrap();
        let prefix: Vec<_> = url.strip_prefix(&child).unwrap().collect();
        assert_eq!(prefix, vec!["foo", "bar"]);

        let url = ListingTableUrl::parse("file:///foo").unwrap();
        let child = Path::parse("/foob/bar").unwrap();
        assert!(url.strip_prefix(&child).is_none());

        let url = ListingTableUrl::parse("file:///foo/file").unwrap();
        let child = Path::parse("/foo/file").unwrap();
        assert_eq!(url.strip_prefix(&child).unwrap().count(), 0);

        let url = ListingTableUrl::parse("file:///foo/ bar").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/ bar");

        let url = ListingTableUrl::parse("file:///foo/bar?").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar");

        let url = ListingTableUrl::parse("file:///foo/😺").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/%F0%9F%98%BA");
    }

    #[test]
    fn test_prefix_s3() {
        let url = ListingTableUrl::parse("s3://bucket/foo/bar").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar");

        let path = Path::from("foo/bar/partition/foo.parquet");
        let prefix: Vec<_> = url.strip_prefix(&path).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "foo.parquet"]);

        let path = Path::from("other/bar/partition/foo.parquet");
        assert!(url.strip_prefix(&path).is_none());
    }

    #[test]
    fn test_split_glob() {
        fn test(input: &str, expected: Option<(&str, &str)>) {
            assert_eq!(
                split_glob_expression(input),
                expected,
                "testing split_glob_expression with {input}"
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

    mod tests_path_expansion {
        use super::*;
        use crate::assert_batches_eq;
        use crate::test_util;
        use home;
        use std::env;

        #[cfg(not(target_os = "windows"))]
        #[test]
        fn test_path_expansion_homedir() -> Result<()> {
            let expanded_home_dir = ListingTableUrl::expand_path_prefix("~")?;
            let home_dir = home::home_dir()
                .unwrap()
                .into_os_string()
                .into_string()
                .unwrap();
            assert_eq!(home_dir, expanded_home_dir);

            let expanded_data_dir = ListingTableUrl::expand_path_prefix("~/data/")?;
            assert_eq!(home_dir + "/data/", expanded_data_dir);

            Ok(())
        }

        #[cfg(not(target_os = "windows"))]
        #[tokio::test]
        async fn test_path_expansion_envvar() -> Result<()> {
            let ctx = SessionContext::new();
            let testdata = test_util::arrow_test_data();
            env::set_var("TESTDIR", testdata);
            env::set_var("AGGR_FILE_NAME", "aggregate_test_100.csv");
            ctx.register_csv(
                "aggr",
                "$TESTDIR/csv/$AGGR_FILE_NAME",
                CsvReadOptions::new(),
            )
            .await?;

            let query = "select sum(c2) from aggr;";
            let query_result = ctx.sql(query).await?.collect().await?;

            #[rustfmt::skip]
            let expected = vec![
                "+--------------+",
                "| SUM(aggr.c2) |",
                "+--------------+",
                "| 285          |",
                "+--------------+",
            ];
            assert_batches_eq!(expected, &query_result);
            Ok(())
        }

        #[cfg(not(target_os = "windows"))]
        #[tokio::test]
        async fn test_path_expansion_invalid() {
            let ctx = SessionContext::new();
            let result = ctx
                .sql(
                    "CREATE EXTERNAL TABLE foo (
                  c1 FLOAT NOT NULL,
                  c2 DOUBLE NOT NULL,
                  c3 BOOLEAN NOT NULL
                )
                STORED AS CSV
                WITH HEADER ROW
                LOCATION '~/$INVALID_ENVVAR/foo.csv'",
                )
                .await;

            match result {
                Ok(_) => panic!("Should panic for invalid environment variable."),
                Err(e) => assert!(format!("{:?}", e)
                    .contains("Unable to find environment variable $INVALID_ENVVAR")),
            }
        }
    }
}
