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

use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_session::Session;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use log::debug;
use object_store::path::Path;
use object_store::path::DELIMITER;
use object_store::{ObjectMeta, ObjectStore};
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
    /// A URL can either refer to a single object, or a collection of objects with a
    /// common prefix, with the presence of a trailing `/` indicating a collection.
    ///
    /// For example, `file:///foo.txt` refers to the file at `/foo.txt`, whereas
    /// `file:///foo/` refers to all the files under the directory `/foo` and its
    /// subdirectories.
    ///
    /// Similarly `s3://BUCKET/blob.csv` refers to `blob.csv` in the S3 bucket `BUCKET`,
    /// whereas `s3://BUCKET/foo/` refers to all objects with the prefix `foo/` in the
    /// S3 bucket `BUCKET`
    ///
    /// # URL Encoding
    ///
    /// URL paths are expected to be URL-encoded. That is, the URL for a file named `bar%2Efoo`
    /// would be `file:///bar%252Efoo`, as per the [URL] specification.
    ///
    /// It should be noted that some tools, such as the AWS CLI, take a different approach and
    /// instead interpret the URL path verbatim. For example the object `bar%2Efoo` would be
    /// addressed as `s3://BUCKET/bar%252Efoo` using [`ListingTableUrl`] but `s3://BUCKET/bar%2Efoo`
    /// when using the aws-cli.
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined by [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// If the path contains any of `'?', '*', '['`, it will be considered
    /// a glob expression and resolved as described in the section below.
    ///
    /// Otherwise, the path will be resolved to an absolute path based on the current
    /// working directory, and converted to a [file URI].
    ///
    /// If the path already exists in the local filesystem this will be used to determine if this
    /// [`ListingTableUrl`] refers to a collection or a single object, otherwise the presence
    /// of a trailing path delimiter will be used to indicate a directory. For the avoidance
    /// of ambiguity it is recommended users always include trailing `/` when intending to
    /// refer to a directory.
    ///
    /// ## Glob File Paths
    ///
    /// If no scheme is provided, and the path contains a glob expression, it will
    /// be resolved as follows.
    ///
    /// The string up to the first path segment containing a glob expression will be extracted,
    /// and resolved in the same manner as a normal scheme-less path above.
    ///
    /// The remaining string will be interpreted as a [`glob::Pattern`] and used as a
    /// filter when listing files from object storage
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    /// [URL]: https://url.spec.whatwg.org/
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        #[cfg(not(target_arch = "wasm32"))]
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Self::try_new(url, None),
            #[cfg(not(target_arch = "wasm32"))]
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// Creates a new [`ListingTableUrl`] interpreting `s` as a filesystem path
    #[cfg(not(target_arch = "wasm32"))]
    fn parse_path(s: &str) -> Result<Self> {
        let (path, glob) = match split_glob_expression(s) {
            Some((prefix, glob)) => {
                let glob = Pattern::new(glob)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (prefix, Some(glob))
            }
            None => (s, None),
        };

        let url = url_from_filesystem_path(path).ok_or_else(|| {
            DataFusionError::External(
                format!("Failed to convert path to URL: {path}").into(),
            )
        })?;

        Self::try_new(url, glob)
    }

    /// Creates a new [`ListingTableUrl`] from a url and optional glob expression
    ///
    /// [`Self::parse`] supports glob expression only for file system paths.
    /// However, some applications may want to support glob expression for URLs with a scheme.
    /// The application can split the URL into a base URL and a glob expression and use this method
    /// to create a [`ListingTableUrl`].
    pub fn try_new(url: Url, glob: Option<Pattern>) -> Result<Self> {
        let prefix = Path::from_url_path(url.path())?;
        Ok(Self { url, prefix, glob })
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Return the URL path not excluding any glob expression
    ///
    /// If [`Self::is_collection`], this is the listing prefix
    /// Otherwise, this is the path to the object
    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// Returns `true` if `path` matches this [`ListingTableUrl`]
    pub fn contains(&self, path: &Path, ignore_subdirectory: bool) -> bool {
        let Some(all_segments) = self.strip_prefix(path) else {
            return false;
        };

        // remove any segments that contain `=` as they are allowed even
        // when ignore subdirectories is `true`.
        let mut segments = all_segments.filter(|s| !s.contains('='));

        match &self.glob {
            Some(glob) => {
                if ignore_subdirectory {
                    segments
                        .next()
                        .is_some_and(|file_name| glob.matches(file_name))
                } else {
                    let stripped = segments.join(DELIMITER);
                    glob.matches(&stripped)
                }
            }
            // where we are ignoring subdirectories, we require
            // the path to be either empty, or contain just the
            // final file name segment.
            None if ignore_subdirectory => segments.count() <= 1,
            // in this case, any valid path at or below the url is allowed
            None => true,
        }
    }

    /// Returns `true` if `path` refers to a collection of objects
    pub fn is_collection(&self) -> bool {
        self.url.path().ends_with(DELIMITER)
    }

    /// Returns the file extension of the last path segment if it exists
    ///
    /// Examples:
    /// ```rust
    /// use datafusion_datasource::ListingTableUrl;
    /// let url = ListingTableUrl::parse("file:///foo/bar.csv").unwrap();
    /// assert_eq!(url.file_extension(), Some("csv"));
    /// let url = ListingTableUrl::parse("file:///foo/bar").unwrap();
    /// assert_eq!(url.file_extension(), None);
    /// let url = ListingTableUrl::parse("file:///foo/bar.").unwrap();
    /// assert_eq!(url.file_extension(), None);
    /// ```
    pub fn file_extension(&self) -> Option<&str> {
        if let Some(mut segments) = self.url.path_segments() {
            if let Some(last_segment) = segments.next_back() {
                if last_segment.contains(".") && !last_segment.ends_with(".") {
                    return last_segment.split('.').next_back();
                }
            }
        }

        None
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    pub fn strip_prefix<'a, 'b: 'a>(
        &'a self,
        path: &'b Path,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        let mut stripped = path.as_ref().strip_prefix(self.prefix.as_ref())?;
        if !stripped.is_empty() && !self.prefix.as_ref().is_empty() {
            stripped = stripped.strip_prefix(DELIMITER)?;
        }
        Some(stripped.split_terminator(DELIMITER))
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub async fn list_all_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        let exec_options = &ctx.config_options().execution;
        let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;
        // If the prefix is a file, use a head request, otherwise list
        let list = match self.is_collection() {
            true => match ctx.runtime_env().cache_manager.get_list_files_cache() {
                None => store.list(Some(&self.prefix)),
                Some(cache) => {
                    if let Some(res) = cache.get(&self.prefix) {
                        debug!("Hit list all files cache");
                        futures::stream::iter(res.as_ref().clone().into_iter().map(Ok))
                            .boxed()
                    } else {
                        let list_res = store.list(Some(&self.prefix));
                        let vec = list_res.try_collect::<Vec<ObjectMeta>>().await?;
                        cache.put(&self.prefix, Arc::new(vec.clone()));
                        futures::stream::iter(vec.into_iter().map(Ok)).boxed()
                    }
                }
            },
            false => futures::stream::once(store.head(&self.prefix)).boxed(),
        };
        Ok(list
            .try_filter(move |meta| {
                let path = &meta.location;
                let extension_match = path.as_ref().ends_with(file_extension);
                let glob_match = self.contains(path, ignore_subdirectory);
                futures::future::ready(extension_match && glob_match)
            })
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
            .boxed())
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

    /// Returns true if the [`ListingTableUrl`] points to the folder
    pub fn is_folder(&self) -> bool {
        self.url.scheme() == "file" && self.is_collection()
    }

    /// Return the `url` for [`ListingTableUrl`]
    pub fn get_url(&self) -> &Url {
        &self.url
    }

    /// Return the `glob` for [`ListingTableUrl`]
    pub fn get_glob(&self) -> &Option<Pattern> {
        &self.glob
    }

    /// Returns a copy of current [`ListingTableUrl`] with a specified `glob`
    pub fn with_glob(self, glob: &str) -> Result<Self> {
        let glob =
            Pattern::new(glob).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Self::try_new(self.url, Some(glob))
    }
}

/// Creates a file URL from a potentially relative filesystem path
#[cfg(not(target_arch = "wasm32"))]
fn url_from_filesystem_path(s: &str) -> Option<Url> {
    let path = std::path::Path::new(s);
    let is_dir = match path.exists() {
        true => path.is_dir(),
        // Fallback to inferring from trailing separator
        false => std::path::is_separator(s.chars().last()?),
    };

    let from_absolute_path = |p| {
        let first = match is_dir {
            true => Url::from_directory_path(p).ok(),
            false => Url::from_file_path(p).ok(),
        }?;

        // By default from_*_path preserve relative path segments
        // We therefore parse the URL again to resolve these
        Url::parse(first.as_str()).ok()
    };

    if path.is_absolute() {
        return from_absolute_path(path);
    }

    let absolute = std::env::current_dir().ok()?.join(path);
    from_absolute_path(&absolute)
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

#[cfg(not(target_arch = "wasm32"))]
const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
#[cfg(not(target_arch = "wasm32"))]
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
    use tempfile::tempdir;

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
        assert_eq!(url.prefix.as_ref(), "foo/😺");

        let url = ListingTableUrl::parse("file:///foo/bar%2Efoo").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar.foo");

        let url = ListingTableUrl::parse("file:///foo/bar%2Efoo").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar.foo");

        let url = ListingTableUrl::parse("file:///foo/bar%252Ffoo").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar%2Ffoo");

        let url = ListingTableUrl::parse("file:///foo/a%252Fb.txt").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/a%2Fb.txt");

        let dir = tempdir().unwrap();
        let path = dir.path().join("bar%2Ffoo");
        std::fs::File::create(&path).unwrap();

        let url = ListingTableUrl::parse(path.to_str().unwrap()).unwrap();
        assert!(url.prefix.as_ref().ends_with("bar%2Ffoo"), "{}", url.prefix);

        let url = ListingTableUrl::parse("file:///foo/../a%252Fb.txt").unwrap();
        assert_eq!(url.prefix.as_ref(), "a%2Fb.txt");

        let url =
            ListingTableUrl::parse("file:///foo/./bar/../../baz/./test.txt").unwrap();
        assert_eq!(url.prefix.as_ref(), "baz/test.txt");

        let workdir = std::env::current_dir().unwrap();
        let t = workdir.join("non-existent");
        let a = ListingTableUrl::parse(t.to_str().unwrap()).unwrap();
        let b = ListingTableUrl::parse("non-existent").unwrap();
        assert_eq!(a, b);
        assert!(a.prefix.as_ref().ends_with("non-existent"));

        let t = workdir.parent().unwrap();
        let a = ListingTableUrl::parse(t.to_str().unwrap()).unwrap();
        let b = ListingTableUrl::parse("..").unwrap();
        assert_eq!(a, b);

        let t = t.join("bar");
        let a = ListingTableUrl::parse(t.to_str().unwrap()).unwrap();
        let b = ListingTableUrl::parse("../bar").unwrap();
        assert_eq!(a, b);
        assert!(a.prefix.as_ref().ends_with("bar"));

        let t = t.join(".").join("foo").join("..").join("baz");
        let a = ListingTableUrl::parse(t.to_str().unwrap()).unwrap();
        let b = ListingTableUrl::parse("../bar/./foo/../baz").unwrap();
        assert_eq!(a, b);
        assert!(a.prefix.as_ref().ends_with("bar/baz"));
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

        // https://github.com/apache/datafusion/issues/2465
        test(
            "/a/b/c//alltypes_plain*.parquet",
            Some(("/a/b/c//", "alltypes_plain*.parquet")),
        );
    }

    #[test]
    fn test_is_collection() {
        fn test(input: &str, expected: bool, message: &str) {
            let url = ListingTableUrl::parse(input).unwrap();
            assert_eq!(url.is_collection(), expected, "{message}");
        }

        test("https://a.b.c/path/", true, "path ends with / - collection");
        test(
            "https://a.b.c/path/?a=b",
            true,
            "path ends with / - with query args - collection",
        );
        test(
            "https://a.b.c/path?a=b/",
            false,
            "path not ends with / - query ends with / - not collection",
        );
        test(
            "https://a.b.c/path/#a=b",
            true,
            "path ends with / - with fragment - collection",
        );
        test(
            "https://a.b.c/path#a=b/",
            false,
            "path not ends with / - fragment ends with / - not collection",
        );
    }

    #[test]
    fn test_file_extension() {
        fn test(input: &str, expected: Option<&str>, message: &str) {
            let url = ListingTableUrl::parse(input).unwrap();
            assert_eq!(url.file_extension(), expected, "{message}");
        }

        test("https://a.b.c/path/", None, "path ends with / - not a file");
        test(
            "https://a.b.c/path/?a=b",
            None,
            "path ends with / - with query args - not a file",
        );
        test(
            "https://a.b.c/path?a=b/",
            None,
            "path not ends with / - query ends with / but no file extension",
        );
        test(
            "https://a.b.c/path/#a=b",
            None,
            "path ends with / - with fragment - not a file",
        );
        test(
            "https://a.b.c/path#a=b/",
            None,
            "path not ends with / - fragment ends with / but no file extension",
        );
        test(
            "file///some/path/",
            None,
            "file path ends with / - not a file",
        );
        test(
            "file///some/path/file",
            None,
            "file path does not end with - no extension",
        );
        test(
            "file///some/path/file.",
            None,
            "file path ends with . - no value after .",
        );
        test(
            "file///some/path/file.ext",
            Some("ext"),
            "file path ends with .ext - extension is ext",
        );
    }
}
