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

use crate::listing_glob::LISTING_GLOB_MATCH_OPTIONS;
#[cfg(not(target_arch = "wasm32"))]
use crate::listing_glob::{
    find_url_path_start, normalize_glob_separators, split_glob_expression,
};
use datafusion_common::{DataFusionError, Result, TableReference};
use datafusion_execution::cache::TableScopedPath;
use datafusion_execution::cache::cache_manager::CachedFileList;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_session::Session;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use log::debug;
use object_store::path::DELIMITER;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
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
    /// Optional table reference for the table this url belongs to
    table_ref: Option<TableReference>,
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
    /// ## Glob Expressions
    ///
    /// Glob expressions (`*`, `**`, `?`, `[abc]`) are supported in both
    /// scheme-less filesystem paths and scheme URLs (e.g.
    /// `s3://bucket/data/**/x.parquet`, `file:///foo/*.parquet`).
    ///
    /// The string up to the first path segment containing a glob
    /// character will be extracted as the literal listing prefix
    /// (passed to `ObjectStore::list`). The remaining string will be
    /// interpreted as a [`glob::Pattern`] and used to filter the
    /// listed paths.
    ///
    /// Match semantics mirror DuckDB's filesystem glob:
    ///
    /// * `*` matches any characters within a single path segment; it
    ///   does **not** cross `/`.
    /// * `**` matches zero or more directory levels (required for
    ///   recursion across subdirectories).
    /// * `?` matches exactly one non-separator character. **Only in
    ///   scheme-less paths** — in scheme URLs, `?` is reserved for the
    ///   URL query delimiter; use `[abc]` or `*` to match arbitrary
    ///   single characters in remote-store globs.
    /// * `[abc]` / `[a-z]` matches one character from the class.
    /// * Partition-style `key=value` directory segments are matched
    ///   literally (e.g. `year=*/x.parquet`).
    ///
    /// When an explicit glob is present, the
    /// `datafusion.execution.listing_table_ignore_subdirectory`
    /// session option is **not** consulted — the pattern is
    /// authoritative.
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
            #[cfg(not(target_arch = "wasm32"))]
            Ok(_) => Self::parse_url(s),
            #[cfg(target_arch = "wasm32")]
            Ok(url) => Self::try_new(url, None),
            #[cfg(not(target_arch = "wasm32"))]
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// Creates a new [`ListingTableUrl`] from a string that
    /// [`Url::parse`] accepts, detecting any trailing glob expression
    /// in the *path* component (the segment between the authority and
    /// the optional `?query`/`#fragment`).
    ///
    /// We split on the raw input string rather than on the parsed URL
    /// because `Url::parse` strips path information into structured
    /// components, but we want the literal text to feed into
    /// [`split_glob_expression`].
    ///
    /// `?` is **not** treated as a glob wildcard inside scheme URLs —
    /// it is reserved for the URL query delimiter to avoid ambiguity
    /// with HTTP-style queries (e.g. `https://host/p?a=b`). The other
    /// wildcards (`*`, `**`, `[abc]`) are unaffected and remain the
    /// recommended way to glob remote-store paths.
    #[cfg(not(target_arch = "wasm32"))]
    fn parse_url(s: &str) -> Result<Self> {
        let Some(path_start) = find_url_path_start(s) else {
            // Should be unreachable: `Url::parse` accepted `s`, so a
            // `://` separator must be present.
            let url =
                Url::parse(s).map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Self::try_new(url, None);
        };
        let after_authority = &s[path_start..];
        // Slice the URL path off at the first `?` (query) or `#`
        // (fragment) so glob detection only sees the structural path.
        let path_end = after_authority
            .find(['?', '#'])
            .unwrap_or(after_authority.len());
        let path = &after_authority[..path_end];
        let suffix = &after_authority[path_end..];

        let (prefix, glob) = match split_glob_expression(path) {
            Some((prefix, glob)) => {
                let glob = Pattern::new(&normalize_glob_separators(glob))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (prefix, Some(glob))
            }
            None => (path, None),
        };
        let reconstructed = format!("{}{}{}", &s[..path_start], prefix, suffix);
        let url = Url::parse(&reconstructed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Self::try_new(url, glob)
    }

    /// Creates a new [`ListingTableUrl`] interpreting `s` as a filesystem path
    #[cfg(not(target_arch = "wasm32"))]
    fn parse_path(s: &str) -> Result<Self> {
        let (path, glob) = match split_glob_expression(s) {
            Some((prefix, glob)) => {
                // On Windows the glob substring can contain `\` because
                // [`std::path::is_separator`] treats it as a separator;
                // object-store paths always use `/`, so normalize here.
                let glob = Pattern::new(&normalize_glob_separators(glob))
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
        Ok(Self {
            url,
            prefix,
            glob,
            table_ref: None,
        })
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
    ///
    /// When an explicit glob is present, it is matched against the full
    /// relative path below this URL's prefix using DuckDB-compatible
    /// semantics: `*` does not cross
    /// `/`, `**` is required for recursion, partition-style `key=value`
    /// directory segments are matched literally, and the
    /// `ignore_subdirectory` option is **not** consulted — the explicit
    /// pattern is authoritative.
    ///
    /// When no glob is present, `ignore_subdirectory` controls whether
    /// the listing recurses; `key=value` segments (Hive partition dirs)
    /// are still followed in either case so that partitioned tables work.
    pub fn contains(&self, path: &Path, ignore_subdirectory: bool) -> bool {
        let Some(mut all_segments) = self.strip_prefix(path) else {
            return false;
        };

        match &self.glob {
            Some(glob) => {
                // Explicit glob: match the full relative path (including
                // any `key=value` segments). `ignore_subdirectory` is
                // intentionally ignored — `*`/`**` in the pattern fully
                // describe the user's intent re: directory traversal.
                let stripped = all_segments.join(DELIMITER);
                glob.matches_with(&stripped, LISTING_GLOB_MATCH_OPTIONS)
            }
            // No glob: when ignoring subdirectories, only the final file
            // segment is accepted, but `key=value` (Hive partition)
            // segments are skipped so partitioned tables still work.
            None if ignore_subdirectory => {
                all_segments.filter(|s| !s.contains('=')).count() <= 1
            }
            // No glob and recursion permitted: any valid path at or
            // below the URL is allowed.
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
        if let Some(mut segments) = self.url.path_segments()
            && let Some(last_segment) = segments.next_back()
            && last_segment.contains(".")
            && !last_segment.ends_with(".")
        {
            return last_segment.split('.').next_back();
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

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`,
    /// optionally filtering by a path prefix
    pub async fn list_prefixed_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
        store: &'a dyn ObjectStore,
        prefix: Option<Path>,
        file_extension: &'a str,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        let exec_options = &ctx.config_options().execution;
        let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;

        // Build full_prefix for non-cached path and head() calls
        let full_prefix = if let Some(ref p) = prefix {
            let mut parts = self.prefix.parts().collect::<Vec<_>>();
            parts.extend(p.parts());
            Path::from_iter(parts)
        } else {
            self.prefix.clone()
        };

        let list: BoxStream<'a, Result<ObjectMeta>> = if self.is_collection() {
            list_with_cache(
                ctx,
                store,
                self.table_ref.as_ref(),
                &self.prefix,
                prefix.as_ref(),
            )
            .await?
        } else {
            match store.head(&full_prefix).await {
                Ok(meta) => futures::stream::once(async { Ok(meta) })
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
                    .boxed(),
                // If the head command fails, it is likely that object doesn't exist.
                // Retry as though it were a prefix (aka a collection)
                Err(object_store::Error::NotFound { .. }) => {
                    list_with_cache(
                        ctx,
                        store,
                        self.table_ref.as_ref(),
                        &self.prefix,
                        prefix.as_ref(),
                    )
                    .await?
                }
                Err(e) => return Err(e.into()),
            }
        };

        Ok(list
            .try_filter(move |meta| {
                let path = &meta.location;
                let extension_match = path.as_ref().ends_with(file_extension);
                let glob_match = self.contains(path, ignore_subdirectory);
                futures::future::ready(extension_match && glob_match)
            })
            .boxed())
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub async fn list_all_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        self.list_prefixed_files(ctx, store, None, file_extension)
            .await
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
    pub fn with_glob(mut self, glob: &str) -> Result<Self> {
        self.glob =
            Some(Pattern::new(glob).map_err(|e| DataFusionError::External(Box::new(e)))?);
        Ok(self)
    }

    /// Set the table reference for this [`ListingTableUrl`]
    pub fn with_table_ref(mut self, table_ref: TableReference) -> Self {
        self.table_ref = Some(table_ref);
        self
    }

    /// Return the table reference for this [`ListingTableUrl`]
    pub fn get_table_ref(&self) -> &Option<TableReference> {
        &self.table_ref
    }
}

/// Lists files with cache support, using prefix-aware lookups.
///
/// # Arguments
/// * `ctx` - The session context
/// * `store` - The object store to list from
/// * `table_base_path` - The table's base path (the stable cache key)
/// * `prefix` - Optional prefix relative to table base for filtering results
///
/// # Cache Behavior:
/// The cache key is always `table_base_path`. When a prefix-filtered listing
/// is requested via `prefix`, the cache:
/// - Looks up `table_base_path` in the cache
/// - Filters results to match `table_base_path/prefix`
/// - Returns filtered results without a storage call
///
/// On cache miss, the full table is always listed and cached, ensuring
/// subsequent prefix queries can be served from cache.
async fn list_with_cache<'b>(
    ctx: &'b dyn Session,
    store: &'b dyn ObjectStore,
    table_ref: Option<&TableReference>,
    table_base_path: &Path,
    prefix: Option<&Path>,
) -> Result<BoxStream<'b, Result<ObjectMeta>>> {
    // Build the full listing path (table_base + prefix)
    let full_prefix = match prefix {
        Some(p) => {
            let mut parts: Vec<_> = table_base_path.parts().collect();
            parts.extend(p.parts());
            Path::from_iter(parts)
        }
        None => table_base_path.clone(),
    };

    match ctx.runtime_env().cache_manager.get_list_files_cache() {
        None => Ok(store
            .list(Some(&full_prefix))
            .map(|res| res.map_err(|e| DataFusionError::ObjectStore(Box::new(e))))
            .boxed()),
        Some(cache) => {
            // Build the filter prefix (only Some if prefix was requested)
            let filter_prefix = prefix.is_some().then(|| full_prefix.clone());

            let table_scoped_base_path = TableScopedPath {
                table: table_ref.cloned(),
                path: table_base_path.clone(),
            };

            // Try cache lookup - get returns CachedFileList
            let vec = if let Some(cached) = cache.get(&table_scoped_base_path) {
                debug!("Hit list files cache");
                cached.files_matching_prefix(&filter_prefix)
            } else {
                // Cache miss - always list and cache the full table
                // This ensures we have complete data for future prefix queries
                let mut vec = store
                    .list(Some(table_base_path))
                    .try_collect::<Vec<ObjectMeta>>()
                    .await?;
                vec.shrink_to_fit(); // Right-size before caching
                let cached: CachedFileList = vec.into();
                let result = cached.files_matching_prefix(&filter_prefix);
                cache.put(&table_scoped_base_path, cached);
                result
            };
            Ok(
                futures::stream::iter(Arc::unwrap_or_clone(vec).into_iter().map(Ok))
                    .boxed(),
            )
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion_common::DFSchema;
    use datafusion_common::config::TableOptions;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::registry::ExtensionTypeRegistryRef;
    use datafusion_expr::{
        AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
    };
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_plan::ExecutionPlan;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        PutMultipartOptions, PutPayload,
    };
    use std::any::Any;
    use std::collections::HashMap;
    use std::ops::Range;
    use tempfile::tempdir;

    #[test]
    fn test_prefix_path() {
        let root = std::env::current_dir().unwrap();
        let root = root.to_string_lossy();

        let url = ListingTableUrl::parse(root).unwrap();
        let child = url.prefix.clone().join("partition").join("file");

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

    #[tokio::test]
    async fn test_list_files() -> Result<()> {
        let store = MockObjectStore {
            in_mem: object_store::memory::InMemory::new(),
            forbidden_paths: vec!["forbidden/e.parquet".into()],
        };

        // Create some files:
        create_file(&store, "a.parquet").await;
        create_file(&store, "/t/b.parquet").await;
        create_file(&store, "/t/c.csv").await;
        create_file(&store, "/t/d.csv").await;

        // This file returns a permission error.
        create_file(&store, "/forbidden/e.parquet").await;

        assert_eq!(
            list_all_files("/", &store, "parquet").await?,
            vec!["a.parquet"],
        );

        // test with and without trailing slash
        assert_eq!(
            list_all_files("/t/", &store, "parquet").await?,
            vec!["t/b.parquet"],
        );
        assert_eq!(
            list_all_files("/t", &store, "parquet").await?,
            vec!["t/b.parquet"],
        );

        // test with and without trailing slash
        assert_eq!(
            list_all_files("/t", &store, "csv").await?,
            vec!["t/c.csv", "t/d.csv"],
        );
        assert_eq!(
            list_all_files("/t/", &store, "csv").await?,
            vec!["t/c.csv", "t/d.csv"],
        );

        // Test a non existing prefix
        assert_eq!(
            list_all_files("/NonExisting", &store, "csv").await?,
            vec![] as Vec<String>
        );
        assert_eq!(
            list_all_files("/NonExisting/", &store, "csv").await?,
            vec![] as Vec<String>
        );

        // Including forbidden.parquet generates an error.
        let Err(DataFusionError::ObjectStore(err)) =
            list_all_files("/forbidden/e.parquet", &store, "parquet").await
        else {
            panic!("Expected ObjectStore error");
        };

        let object_store::Error::PermissionDenied { .. } = &*err else {
            panic!("Expected PermissionDenied error");
        };

        // Test prefix filtering with partition-style paths
        create_file(&store, "/data/a=1/file1.parquet").await;
        create_file(&store, "/data/a=1/b=100/file2.parquet").await;
        create_file(&store, "/data/a=2/b=200/file3.parquet").await;
        create_file(&store, "/data/a=2/b=200/file4.csv").await;

        assert_eq!(
            list_prefixed_files("/data/", &store, Some(Path::from("a=1")), "parquet")
                .await?,
            vec!["data/a=1/b=100/file2.parquet", "data/a=1/file1.parquet"],
        );

        assert_eq!(
            list_prefixed_files(
                "/data/",
                &store,
                Some(Path::from("a=1/b=100")),
                "parquet"
            )
            .await?,
            vec!["data/a=1/b=100/file2.parquet"],
        );

        assert_eq!(
            list_prefixed_files("/data/", &store, Some(Path::from("a=2")), "parquet")
                .await?,
            vec!["data/a=2/b=200/file3.parquet"],
        );

        Ok(())
    }

    /// Tests that the cached code path produces identical results to the non-cached path.
    ///
    /// This is critical: the cache is a transparent optimization, so both paths
    /// MUST return the same files. Note: order is not guaranteed by ObjectStore::list,
    /// so we sort results before comparison.
    #[tokio::test]
    async fn test_cache_path_equivalence() -> Result<()> {
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let store = MockObjectStore {
            in_mem: object_store::memory::InMemory::new(),
            forbidden_paths: vec![],
        };

        // Create test files with partition-style paths
        create_file(&store, "/table/year=2023/data1.parquet").await;
        create_file(&store, "/table/year=2023/month=01/data2.parquet").await;
        create_file(&store, "/table/year=2024/data3.parquet").await;
        create_file(&store, "/table/year=2024/month=06/data4.parquet").await;
        create_file(&store, "/table/year=2024/month=12/data5.parquet").await;

        // Session WITHOUT cache
        let session_no_cache = MockSession::new();

        // Session WITH cache - use RuntimeEnvBuilder with cache limit (no TTL needed for this test)
        let runtime_with_cache = RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(1024 * 1024) // 1MB limit
            .build_arc()?;
        let session_with_cache = MockSession::with_runtime_env(runtime_with_cache);

        // Test cases: (url, prefix, description)
        let test_cases = vec![
            ("/table/", None, "full table listing"),
            (
                "/table/",
                Some(Path::from("year=2023")),
                "single partition filter",
            ),
            (
                "/table/",
                Some(Path::from("year=2024")),
                "different partition filter",
            ),
            (
                "/table/",
                Some(Path::from("year=2024/month=06")),
                "nested partition filter",
            ),
            (
                "/table/",
                Some(Path::from("year=2025")),
                "non-existent partition",
            ),
        ];

        for (url_str, prefix, description) in test_cases {
            let url = ListingTableUrl::parse(url_str)?;

            // Get results WITHOUT cache (sorted for comparison)
            let mut results_no_cache: Vec<String> = url
                .list_prefixed_files(&session_no_cache, &store, prefix.clone(), "parquet")
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .map(|m| m.location.to_string())
                .collect();
            results_no_cache.sort();

            // Get results WITH cache (first call - cache miss, sorted for comparison)
            let mut results_with_cache_miss: Vec<String> = url
                .list_prefixed_files(
                    &session_with_cache,
                    &store,
                    prefix.clone(),
                    "parquet",
                )
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .map(|m| m.location.to_string())
                .collect();
            results_with_cache_miss.sort();

            // Get results WITH cache (second call - cache hit, sorted for comparison)
            let mut results_with_cache_hit: Vec<String> = url
                .list_prefixed_files(&session_with_cache, &store, prefix, "parquet")
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .map(|m| m.location.to_string())
                .collect();
            results_with_cache_hit.sort();

            // All three should contain the same files
            assert_eq!(
                results_no_cache, results_with_cache_miss,
                "Cache miss path should match non-cached path for: {description}"
            );
            assert_eq!(
                results_no_cache, results_with_cache_hit,
                "Cache hit path should match non-cached path for: {description}"
            );
        }

        Ok(())
    }

    /// Tests that prefix queries can be served from a cached full-table listing
    #[tokio::test]
    async fn test_cache_serves_partition_from_full_listing() -> Result<()> {
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let store = MockObjectStore {
            in_mem: object_store::memory::InMemory::new(),
            forbidden_paths: vec![],
        };

        // Create test files
        create_file(&store, "/sales/region=US/q1.parquet").await;
        create_file(&store, "/sales/region=US/q2.parquet").await;
        create_file(&store, "/sales/region=EU/q1.parquet").await;

        // Create session with cache (no TTL needed for this test)
        let runtime = RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(1024 * 1024) // 1MB limit
            .build_arc()?;
        let session = MockSession::with_runtime_env(runtime);

        let url = ListingTableUrl::parse("/sales/")?;

        // First: query full table (populates cache)
        let full_results: Vec<String> = url
            .list_prefixed_files(&session, &store, None, "parquet")
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(full_results.len(), 3);

        // Second: query with prefix (should be served from cache)
        let mut us_results: Vec<String> = url
            .list_prefixed_files(
                &session,
                &store,
                Some(Path::from("region=US")),
                "parquet",
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        us_results.sort();

        assert_eq!(
            us_results,
            vec!["sales/region=US/q1.parquet", "sales/region=US/q2.parquet"]
        );

        // Third: different prefix (also from cache)
        let eu_results: Vec<String> = url
            .list_prefixed_files(
                &session,
                &store,
                Some(Path::from("region=EU")),
                "parquet",
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();

        assert_eq!(eu_results, vec!["sales/region=EU/q1.parquet"]);

        Ok(())
    }

    /// Creates a file with "hello world" content at the specified path
    async fn create_file(object_store: &dyn ObjectStore, path: &str) {
        object_store
            .put(&Path::from(path), PutPayload::from_static(b"hello world"))
            .await
            .expect("failed to create test file");
    }

    /// Runs "list_prefixed_files"  with no prefix to list all files and returns their paths
    ///
    /// Panic's on error
    async fn list_all_files(
        url: &str,
        store: &dyn ObjectStore,
        file_extension: &str,
    ) -> Result<Vec<String>> {
        try_list_prefixed_files(url, store, None, file_extension).await
    }

    /// Runs "list_prefixed_files" and returns their paths
    ///
    /// Panic's on error
    async fn list_prefixed_files(
        url: &str,
        store: &dyn ObjectStore,
        prefix: Option<Path>,
        file_extension: &str,
    ) -> Result<Vec<String>> {
        try_list_prefixed_files(url, store, prefix, file_extension).await
    }

    /// Runs "list_prefixed_files" and returns their paths
    async fn try_list_prefixed_files(
        url: &str,
        store: &dyn ObjectStore,
        prefix: Option<Path>,
        file_extension: &str,
    ) -> Result<Vec<String>> {
        let session = MockSession::new();
        let url = ListingTableUrl::parse(url)?;
        let files = url
            .list_prefixed_files(&session, store, prefix, file_extension)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|meta| meta.location.as_ref().to_string())
            .collect();
        Ok(files)
    }

    #[derive(Debug)]
    struct MockObjectStore {
        in_mem: object_store::memory::InMemory,
        forbidden_paths: Vec<Path>,
    }

    impl std::fmt::Display for MockObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.in_mem.fmt(f)
        }
    }

    #[async_trait]
    impl ObjectStore for MockObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.in_mem.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.in_mem.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head && self.forbidden_paths.contains(location) {
                Err(object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source: "forbidden".into(),
                })
            } else {
                self.in_mem.get_opts(location, options).await
            }
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.in_mem.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.in_mem.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.in_mem.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.in_mem.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.in_mem.copy_opts(from, to, options).await
        }
    }

    struct MockSession {
        config: SessionConfig,
        runtime_env: Arc<RuntimeEnv>,
    }

    impl MockSession {
        fn new() -> Self {
            Self {
                config: SessionConfig::new(),
                runtime_env: Arc::new(RuntimeEnv::default()),
            }
        }

        /// Create a MockSession with a custom RuntimeEnv (for cache testing)
        fn with_runtime_env(runtime_env: Arc<RuntimeEnv>) -> Self {
            Self {
                config: SessionConfig::new(),
                runtime_env,
            }
        }
    }

    #[async_trait::async_trait]
    impl Session for MockSession {
        fn session_id(&self) -> &str {
            unimplemented!()
        }

        fn config(&self) -> &SessionConfig {
            &self.config
        }

        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn create_physical_expr(
            &self,
            _expr: Expr,
            _df_schema: &DFSchema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            unimplemented!()
        }

        fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
            unimplemented!()
        }

        fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
            unimplemented!()
        }

        fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
            unimplemented!()
        }

        fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
            unimplemented!()
        }

        fn runtime_env(&self) -> &Arc<RuntimeEnv> {
            &self.runtime_env
        }

        fn execution_props(&self) -> &ExecutionProps {
            unimplemented!()
        }

        fn as_any(&self) -> &dyn Any {
            unimplemented!()
        }

        fn table_options(&self) -> &TableOptions {
            unimplemented!()
        }

        fn table_options_mut(&mut self) -> &mut TableOptions {
            unimplemented!()
        }

        fn task_ctx(&self) -> Arc<TaskContext> {
            unimplemented!()
        }
    }
}
