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
    Array, ArrayRef, GenericStringBuilder, LargeStringArray, StringArray, StringArrayType,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use url::Url;

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
                vec![
                    TypeSignature::Uniform(
                        1,
                        vec![DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8],
                    ),
                    TypeSignature::Uniform(
                        2,
                        vec![DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8],
                    ),
                    TypeSignature::Uniform(
                        3,
                        vec![DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8],
                    ),
                ],
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
    ///
    fn parse(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
        Url::parse(value)
            .map_err(|e| exec_datafusion_err!("{e:?}"))
            .map(|url| match part {
                "HOST" => url.host_str().map(String::from),
                "PATH" => Some(url.path().to_string()),
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
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "{} expects 2 or 3 arguments, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        match arg_types.len() {
            2 | 3 => {
                if arg_types
                    .iter()
                    .any(|arg| matches!(arg, DataType::LargeUtf8))
                {
                    Ok(DataType::LargeUtf8)
                } else if arg_types
                    .iter()
                    .any(|arg| matches!(arg, DataType::Utf8View))
                {
                    Ok(DataType::Utf8View)
                } else {
                    Ok(DataType::Utf8)
                }
            }
            _ => plan_err!(
                "`{}` expects 2 or 3 arguments, got {}",
                &self.name(),
                arg_types.len()
            ),
        }
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
///
fn spark_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
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

    let result = if args.len() == 3 {
        let key = &args[2];

        match (url.data_type(), part.data_type(), key.data_type()) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            _ => exec_err!("{} expects STRING arguments, got {:?}", "`parse_url`", args),
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
                )
            }
            (DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    &key,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    &key,
                )
            }
            _ => exec_err!("{} expects STRING arguments, got {:?}", "`parse_url`", args),
        }
    };
    result
}

fn process_parse_url<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
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
                ParseUrl::parse(url, part, key)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| Arc::new(array) as ArrayRef)
}
