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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

pub mod arrow_wrappers;
pub mod catalog_provider;
pub mod execution_plan;
pub mod insert_op;
pub mod plan_properties;
pub mod record_batch_stream;
pub mod schema_provider;
pub mod session_config;
pub mod table_provider;
pub mod table_source;
pub mod udf;
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

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);
