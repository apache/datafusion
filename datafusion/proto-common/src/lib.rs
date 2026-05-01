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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

//! `prost`-generated DataFusion protobuf model types + Arrow conversion impls.
//!
//! This crate is the schema source of truth for DataFusion's logical and
//! physical plan protobuf schemas (see `proto/datafusion.proto` and
//! `proto/datafusion_common.proto`). It owns:
//!
//! - the prost-generated structs and pbjson serde impls, and
//! - the arrow-side `From`/`TryFrom` conversion impls (Schema, Field,
//!   DataType, TimeUnit, IntervalUnit) — these have to live here because
//!   both `arrow::*` and the proto types are foreign to every other crate.
//!
//! Conversions to and from `datafusion-common` types live in
//! `datafusion-common::proto` (under the `proto` feature) so this crate
//! has no `datafusion-*` runtime deps.
//!
//! Most users should depend on [`datafusion-proto`] instead, which re-exports
//! these types under [`datafusion_proto::protobuf`].
//!
//! [`datafusion-proto`]: https://crates.io/crates/datafusion-proto
//! [`datafusion_proto::protobuf`]: https://docs.rs/datafusion-proto/latest/datafusion_proto/protobuf/index.html

pub mod common;
pub mod from_proto;
pub mod generated;
pub mod to_proto;

pub use from_proto::Error as FromProtoError;
pub use generated::common_pkg as protobuf_common;
pub use generated::common_pkg::*;
pub use to_proto::Error as ToProtoError;

/// All DataFusion protobuf model types (both `package datafusion` and
/// `package datafusion_common`) re-exported from a single flat namespace.
///
/// This is the canonical location for these prost-generated types.
/// `datafusion-proto` re-exports it as `datafusion_proto::protobuf`.
pub mod protobuf {
    pub use crate::generated::common_pkg::*;
    pub use crate::generated::datafusion::*;
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);
