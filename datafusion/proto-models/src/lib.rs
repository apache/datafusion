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

//! `prost`-generated DataFusion protobuf model types.
//!
//! This crate contains only the generated structs for DataFusion's logical and
//! physical plan protobuf schemas (see `proto/datafusion.proto`). It is the
//! schema source of truth for [`datafusion-proto`] and intentionally has no
//! DataFusion dependencies beyond [`datafusion-proto-common`].
//!
//! Most users should depend on [`datafusion-proto`] instead, which re-exports
//! these types under [`datafusion_proto::protobuf`].
//!
//! [`datafusion-proto`]: https://crates.io/crates/datafusion-proto
//! [`datafusion-proto-common`]: https://crates.io/crates/datafusion-proto-common
//! [`datafusion_proto::protobuf`]: https://docs.rs/datafusion-proto/latest/datafusion_proto/protobuf/index.html

pub mod generated;

/// All DataFusion protobuf model types.
///
/// Includes both the types declared in `datafusion.proto` and the
/// `datafusion_proto_common` types it imports, in a single flat namespace
/// so consumers can `use datafusion_proto_models::protobuf::*;`.
pub mod protobuf {
    pub use crate::generated::datafusion::*;
}

/// Re-export of the `datafusion_proto_common` types as exposed through this
/// crate's generated module, for callers that want the common-only namespace.
pub use generated::datafusion_common;
