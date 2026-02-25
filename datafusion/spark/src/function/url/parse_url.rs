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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, GenericStringBuilder, LargeStringArray, StringArray,
    StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use url::{ParseError, Url};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseUrl {
    signature: Signature,
}

impl Default for ParseUrl {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseUrl {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
    /// Parses a URL and extracts the specified component.
    ///
    /// This function takes a URL string and extracts different parts of it based on the
    /// `part` parameter. For query parameters, an optional `key` can be specified to
    /// extract a specific query parameter value.
    ///
    /// # Arguments
    ///
    /// * `value` - The URL string to parse
    /// * `part` - The component of the URL to extract. Valid values are:
    ///   - `"HOST"` - The hostname (e.g., "example.com")
    ///   - `"PATH"` - The path portion (e.g., "/path/to/resource")
    ///   - `"QUERY"` - The query string or a specific query parameter
    ///   - `"REF"` - The fragment/anchor (the part after #)
    ///   - `"PROTOCOL"` - The URL scheme (e.g., "https", "http")
    ///   - `"FILE"` - The path with query string (e.g., "/path?query=value")
    ///   - `"AUTHORITY"` - The authority component (host:port)
    ///   - `"USERINFO"` - The user information (username:password)
    /// * `key` - Optional parameter used only with `"QUERY"`. When provided, extracts
    ///   the value of the specific query parameter with this key name.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` - The extracted URL component as a string
    /// * `Ok(None)` - If the requested component doesn't exist or is empty
    /// * `Err(DataFusionError)` - If the URL is malformed and cannot be parsed
    fn parse(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
        let url: std::result::Result<Url, ParseError> = Url::parse(value);
        if let Err(ParseError::RelativeUrlWithoutBase) = url {
            return if !value.contains("://") {
                Ok(None)
            } else {
                Err(exec_datafusion_err!(
                    "The url is invalid: {value}. Use `try_parse_url` to tolerate invalid URL and return NULL instead. SQLSTATE: 22P02"
                ))
            };
        };
        url.map_err(|e| exec_datafusion_err!("{e:?}"))
            .map(|url| match part {
                "HOST" => url.host_str().map(String::from),
                "PATH" => {
                    let path: String = url.path().to_string();
                    let path: String = if path == "/" { "".to_string() } else { path };
                    Some(path)
                }
                "QUERY" => match key {
                    None => url.query().map(String::from),
                    Some(key) => url
                        .query_pairs()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.into_owned()),
                },
                "REF" => url.fragment().map(String::from),
                "PROTOCOL" => Some(url.scheme().to_string()),
                "FILE" => {
                    let path = url.path();
                    match url.query() {
                        Some(query) => Some(format!("{path}?{query}")),
                        None => Some(path.to_string()),
                    }
                }
                "AUTHORITY" => Some(url.authority().to_string()),
                "USERINFO" => {
                    let username = url.username();
                    if username.is_empty() {
                        return None;
                    }
                    match url.password() {
                        Some(password) => Some(format!("{username}:{password}")),
                        None => Some(username.to_string()),
                    }
                }
                _ => None,
            })
    }
}

impl ScalarUDFImpl for ParseUrl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parse_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_parse_url, vec![])(&args)
    }
}

/// Core implementation of URL parsing function.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRef containing the input arrays:
///   - `args[0]` - URL array: The URLs to parse
///   - `args[1]` - Part array: The URL components to extract (HOST, PATH, QUERY, etc.)
///   - `args[2]` - Key array (optional): For QUERY part, the specific parameter names to extract
///
/// # Return Value
///
/// Returns `Result<ArrayRef>` containing:
/// - A string array with extracted URL components
/// - `None` values where extraction failed or component doesn't exist
/// - The output array type (StringArray or LargeStringArray) is determined by input types
fn spark_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
    spark_handled_parse_url(args, |x| x)
}

pub fn spark_handled_parse_url(
    args: &[ArrayRef],
    handler_err: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "{} expects 2 or 3 arguments, but got {}",
            "`parse_url`",
            args.len()
        );
    }
    // Required arguments
    let url = &args[0];
    let part = &args[1];

    if args.len() == 3 {
        // In this case, the 'key' argument is passed
        let key = &args[2];

        match (url.data_type(), part.data_type(), key.data_type()) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringViewArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                )
            }
            _ => exec_err!(
                "`parse_url` expects STRING arguments, got ({}, {}, {})",
                url.data_type(),
                part.data_type(),
                key.data_type()
            ),
        }
    } else {
        // The 'key' argument is omitted, assume all values are null
        // Create 'null' string array for 'key' argument
        let mut builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        for _ in 0..args[0].len() {
            builder.append_null();
        }
        let key = builder.finish();

        match (url.data_type(), part.data_type()) {
            (DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    &key,
                    handler_err,
                )
            }
            (DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringViewArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    &key,
                    handler_err,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    &key,
                    handler_err,
                )
            }
            _ => exec_err!(
                "`parse_url` expects STRING arguments, got ({}, {})",
                url.data_type(),
                part.data_type()
            ),
        }
    }
}

fn process_parse_url<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
    handle: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ArrayRef>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    &'a C: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .zip(part_array.iter())
        .zip(key_array.iter())
        .map(|((url, part), key)| {
            if let (Some(url), Some(part), key) = (url, part, key) {
                handle(ParseUrl::parse(url, part, key))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion_common::Result;
    use std::array::from_ref;
    use std::sync::Arc;

    fn sa(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec())) as ArrayRef
    }

    #[test]
    fn test_parse_host() -> Result<()> {
        let got = ParseUrl::parse("https://example.com/a?x=1", "HOST", None)?;
        assert_eq!(got, Some("example.com".to_string()));
        Ok(())
    }

    #[test]
    fn test_parse_query_no_key_vs_with_key() -> Result<()> {
        let got_all = ParseUrl::parse("https://ex.com/p?a=1&b=2", "QUERY", None)?;
        assert_eq!(got_all, Some("a=1&b=2".to_string()));

        let got_a = ParseUrl::parse("https://ex.com/p?a=1&b=2", "QUERY", Some("a"))?;
        assert_eq!(got_a, Some("1".to_string()));

        let got_c = ParseUrl::parse("https://ex.com/p?a=1&b=2", "QUERY", Some("c"))?;
        assert_eq!(got_c, None);
        Ok(())
    }

    #[test]
    fn test_parse_ref_protocol_userinfo_file_authority() -> Result<()> {
        let url = "ftp://user:pwd@ftp.example.com:21/files?x=1#frag";
        assert_eq!(ParseUrl::parse(url, "REF", None)?, Some("frag".to_string()));
        assert_eq!(
            ParseUrl::parse(url, "PROTOCOL", None)?,
            Some("ftp".to_string())
        );
        assert_eq!(
            ParseUrl::parse(url, "USERINFO", None)?,
            Some("user:pwd".to_string())
        );
        assert_eq!(
            ParseUrl::parse(url, "FILE", None)?,
            Some("/files?x=1".to_string())
        );
        assert_eq!(
            ParseUrl::parse(url, "AUTHORITY", None)?,
            Some("user:pwd@ftp.example.com".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_parse_path_root_is_empty_string() -> Result<()> {
        let got = ParseUrl::parse("https://example.com/", "PATH", None)?;
        assert_eq!(got, Some("".to_string()));
        Ok(())
    }

    #[test]
    fn test_parse_malformed_url_returns_error() -> Result<()> {
        let got = ParseUrl::parse("notaurl", "HOST", None)?;
        assert_eq!(got, None);
        Ok(())
    }

    #[test]
    fn test_spark_utf8_two_args() -> Result<()> {
        let urls = sa(&[Some("https://example.com/a?x=1"), Some("https://ex.com/")]);
        let parts = sa(&[Some("HOST"), Some("PATH")]);

        let out = spark_handled_parse_url(&[urls, parts], |x| x)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(out_sa.len(), 2);
        assert_eq!(out_sa.value(0), "example.com");
        assert_eq!(out_sa.value(1), "");
        Ok(())
    }

    #[test]
    fn test_spark_utf8_three_args_query_key() -> Result<()> {
        let urls = sa(&[
            Some("https://example.com/a?x=1&y=2"),
            Some("https://ex.com/?a=1"),
        ]);
        let parts = sa(&[Some("QUERY"), Some("QUERY")]);
        let keys = sa(&[Some("y"), Some("b")]);

        let out = spark_handled_parse_url(&[urls, parts, keys], |x| x)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(out_sa.len(), 2);
        assert_eq!(out_sa.value(0), "2");
        assert!(out_sa.is_null(1));
        Ok(())
    }

    #[test]
    fn test_spark_userinfo_and_nulls() -> Result<()> {
        let urls = sa(&[
            Some("ftp://user:pwd@ftp.example.com:21/files"),
            Some("https://example.com"),
            None,
        ]);
        let parts = sa(&[Some("USERINFO"), Some("USERINFO"), Some("USERINFO")]);

        let out = spark_handled_parse_url(&[urls, parts], |x| x)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(out_sa.len(), 3);
        assert_eq!(out_sa.value(0), "user:pwd");
        assert!(out_sa.is_null(1));
        assert!(out_sa.is_null(2));
        Ok(())
    }

    #[test]
    fn test_invalid_arg_count() {
        let urls = sa(&[Some("https://example.com")]);
        let err = spark_handled_parse_url(from_ref(&urls), |x| x).unwrap_err();
        assert!(format!("{err}").contains("expects 2 or 3 arguments"));

        let parts = sa(&[Some("HOST")]);
        let keys = sa(&[Some("x")]);
        let err =
            spark_handled_parse_url(&[urls, parts, keys, sa(&[Some("extra")])], |x| x)
                .unwrap_err();
        assert!(format!("{err}").contains("expects 2 or 3 arguments"));
    }

    #[test]
    fn test_non_string_types_error() {
        let urls = sa(&[Some("https://example.com")]);
        let bad_part = Arc::new(Int32Array::from(vec![1])) as ArrayRef;

        let err = spark_handled_parse_url(&[urls, bad_part], |x| x).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("expects STRING arguments"));
    }
}
