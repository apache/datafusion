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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

pub mod arrow_wrappers;
pub mod catalog_provider;
pub mod catalog_provider_list;
pub mod execution_plan;
pub mod insert_op;
pub mod plan_properties;
pub mod record_batch_stream;
pub mod schema_provider;
pub mod session_config;
pub mod table_provider;
pub mod table_source;
pub mod udaf;
pub mod udf;
pub mod udtf;
pub mod udwf;
pub mod util;
pub mod volatility;

#[cfg(feature = "integration-tests")]
pub mod tests;

/// Returns the major version of the FFI implementation. If the API evolves,
/// we use the major version to identify compatibility over the unsafe
/// boundary. This call is intended to be used by implementers to validate
/// they have compatible libraries.
pub extern "C" fn version() -> u64 {
    let version_str = env!("CARGO_PKG_VERSION");
    let version = semver::Version::parse(version_str).expect("Invalid version string");
    version.major
}

static LIBRARY_MARKER: u8 = 0;

/// This utility is used to determine if two FFI structs are within
/// the same library. It is possible that the interplay between
/// foreign and local functions calls create one FFI struct that
/// references another. It is helpful to determine if a foreign
/// struct in the same library or called from a different one.
/// If we are in the same library, then we can access the underlying
/// types directly.
///
/// This function works by checking the address of the library
/// marker. Each library that implements the FFI code will have
/// a different address for the marker. By checking the marker
/// address we can determine if a struct is truly foreign or is
/// actually within the same originating library.
///
/// See the crate's `README.md` for additional information.
pub extern "C" fn get_library_marker_id() -> usize {
    &LIBRARY_MARKER as *const u8 as usize
}

/// For unit testing in this crate we need to trick the providers
/// into thinking we have a foreign call. We do this by overwriting
/// their `library_marker_id` function to return a different value.
#[cfg(test)]
pub(crate) extern "C" fn mock_foreign_marker_id() -> usize {
    get_library_marker_id() + 1
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);
