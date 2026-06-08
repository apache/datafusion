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

//! Helpers for detecting, splitting, and matching glob expressions inside
//! listing-table paths and URLs.
//!
//! This module owns the small surface that decides what counts as a glob
//! expression in a path (`?`, `*`, `[`), how to split a path into a
//! literal listing prefix and the trailing glob pattern, and the
//! [`glob::MatchOptions`] used to match listed object-store paths against
//! the pattern. `glob::Pattern` construction itself lives in [`crate::url`].

use std::borrow::Cow;

use glob::MatchOptions;

/// Glob matching options used when checking listed object-store paths
/// against an explicit glob in a [`crate::url::ListingTableUrl`].
///
/// These mirror DuckDB's filesystem glob semantics:
///
/// * `*` matches any characters within a single path segment and does
///   **not** cross `/`.
/// * `**` is required to match across directory levels (matches zero or
///   more directory components).
/// * `?` matches exactly one non-separator character.
/// * `[abc]` / `[a-z]` matches one character from a class.
///
/// Hidden files (segments starting with `.`) are not auto-excluded;
/// DataFusion's object-store listing does not special-case them either,
/// so this options struct mirrors that for consistency.
pub(crate) const LISTING_GLOB_MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: false,
};

/// Characters whose presence in a path segment marks the start of a glob
/// expression: `?` (single character), `*` (any characters), `[` (character
/// class).
pub(crate) const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Splits `path` at the first path segment containing a glob expression,
/// returning `None` if no glob expression is found.
///
/// The returned tuple is `(literal_prefix, glob_pattern)` where
/// `literal_prefix` is suitable for passing to an object-store `list` call
/// and `glob_pattern` is the trailing portion to match against listed paths.
///
/// Path delimiters are determined using [`std::path::is_separator`], which
/// permits `/` as a path delimiter even on Windows platforms.
pub(crate) fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
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

/// Normalize OS-specific path separators in a glob pattern to `/` so that
/// the pattern can be matched against object-store paths (which always
/// use `/` as the delimiter).
///
/// On Unix this is a no-op. On Windows the raw glob substring returned
/// by [`split_glob_expression`] may contain `\`, because
/// [`std::path::is_separator`] treats `\` as a separator on that
/// platform.
pub(crate) fn normalize_glob_separators(glob: &str) -> Cow<'_, str> {
    if cfg!(windows) && glob.contains('\\') {
        Cow::Owned(glob.replace('\\', "/"))
    } else {
        Cow::Borrowed(glob)
    }
}

/// Locate the byte index at which the path component of a URL string
/// begins, so callers can apply [`split_glob_expression`] to the raw
/// path *before* `Url::parse` runs (URL parsing eats `?` as the
/// query-string delimiter and would otherwise prevent globs that use
/// `?` from being detected in remote-store URLs).
///
/// Returns `None` if the string contains no `://` scheme separator at
/// all. For a URL with a scheme but no path (e.g. `s3://bucket`),
/// returns `s.len()` — the path is the empty suffix.
///
/// The authority (the segment between `://` and the first path `/`)
/// may include an IPv6 host enclosed in `[...]`; `/` characters inside
/// the brackets are ignored when locating the path start.
pub(crate) fn find_url_path_start(s: &str) -> Option<usize> {
    let after_scheme = s.find("://")? + 3;
    let bytes = s.as_bytes();
    let mut i = after_scheme;
    let mut in_brackets = false;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => in_brackets = true,
            b']' => in_brackets = false,
            b'/' if !in_brackets => return Some(i),
            _ => {}
        }
        i += 1;
    }
    Some(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use glob::Pattern;

    /// Verifies the `LISTING_GLOB_MATCH_OPTIONS` reproduces DuckDB's
    /// glob semantics for paths and patterns produced by
    /// [`split_glob_expression`]. The expected results were
    /// cross-checked against `duckdb` v1.5.2 on the same inputs.
    #[test]
    fn test_listing_glob_match_options_duckdb_parity() {
        fn m(pat: &str, s: &str) -> bool {
            Pattern::new(pat)
                .unwrap()
                .matches_with(s, LISTING_GLOB_MATCH_OPTIONS)
        }

        // `*` does not cross `/`
        assert!(m("abc*123.parquet", "abc_X_123.parquet"));
        assert!(m("abc*123.parquet", "abc123.parquet")); // `*` matches empty
        assert!(!m("abc*123.parquet", "sub/abc_S_123.parquet"));
        assert!(!m("*123.parquet", "sub/abc_S_123.parquet"));

        // `**` matches zero or more directory components
        assert!(m("**/abc*123.parquet", "abc_X_123.parquet"));
        assert!(m("**/abc*123.parquet", "sub/abc_S_123.parquet"));
        assert!(m("**/abc*123.parquet", "a/b/abc_X_123.parquet"));

        // partition-style (`key=value`) directories match literally
        assert!(m("**/abc*123.parquet", "year=2021/abc_X_123.parquet"));
        assert!(m("year=*/abc*123.parquet", "year=2021/abc_X_123.parquet"));

        // `*` occupies exactly one directory segment
        assert!(m("*/abc*123.parquet", "year=2021/abc_X_123.parquet"));
        assert!(!m("*/abc*123.parquet", "a/b/abc_X_123.parquet"));

        // `?` matches exactly one non-separator character
        assert!(m("abc?123.parquet", "abcX123.parquet"));
        assert!(!m("abc?123.parquet", "abc123.parquet"));
        assert!(!m("abc?123.parquet", "abcXY123.parquet"));

        // character classes
        assert!(m("abc[X9]*.parquet", "abc999.parquet"));
        assert!(m("abc[X9]*.parquet", "abcX123.parquet"));
        assert!(!m("abc[X9]*.parquet", "abcZ123.parquet"));
    }

    #[test]
    fn test_normalize_glob_separators() {
        assert!(matches!(
            normalize_glob_separators("**/x.parquet"),
            Cow::Borrowed("**/x.parquet")
        ));
        // Unix paths never contain `\`, so it is preserved as a literal.
        if !cfg!(windows) {
            assert!(matches!(
                normalize_glob_separators("a\\b*.parquet"),
                Cow::Borrowed("a\\b*.parquet")
            ));
        } else {
            assert_eq!(normalize_glob_separators("**\\x.parquet"), "**/x.parquet");
        }
    }

    #[test]
    fn test_find_url_path_start() {
        fn split(s: &str) -> Option<(&str, &str)> {
            find_url_path_start(s).map(|i| (&s[..i], &s[i..]))
        }
        assert_eq!(
            split("s3://bucket/p/*.parquet"),
            Some(("s3://bucket", "/p/*.parquet"))
        );
        // `s3://bucket` — no path component
        assert_eq!(split("s3://bucket"), Some(("s3://bucket", "")));
        // `file:///foo` — empty authority, path starts at the third `/`
        assert_eq!(
            split("file:///foo/**/x.parquet"),
            Some(("file://", "/foo/**/x.parquet"))
        );
        // IPv6 host — `/` inside `[...]` is part of the authority,
        // not the start of the path
        assert_eq!(
            split("http://[::1]:8080/p/*.parquet"),
            Some(("http://[::1]:8080", "/p/*.parquet"))
        );
        assert_eq!(
            split("http://[::1]/p/*.parquet"),
            Some(("http://[::1]", "/p/*.parquet"))
        );
        // No scheme separator
        assert_eq!(find_url_path_start("/local/path/*.parquet"), None);
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
}
