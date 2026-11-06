// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//! This crate contains the generated structs for DataFusion's logical and
//! physical plan protobuf schemas (see `proto/datafusion.proto`), plus the
//! [`From`] / [`TryFrom`] conversions between them and the `datafusion-common`
//! types they mirror. Those conversions live here because their DataFusion side
//! sits *below* this crate in the dependency graph and so cannot host the impls
//! itself — see [`from_proto`] and [`to_proto`]. It is the schema source of
//! truth for [`datafusion-proto`].
//!
//! It also hosts [`ProtoDecoderRegistry`], the
//! one store of extension decoders every serializable kind shares, for the same
//! layering reason: it sits below every crate that owns one of those traits.
//!
//! Most users should depend on [`datafusion-proto`] instead, which re-exports
//! these types under [`datafusion_proto::protobuf`].
//!
//! [`datafusion-proto`]: https://crates.io/crates/datafusion-proto
//! [`datafusion-proto-common`]: https://crates.io/crates/datafusion-proto-common
//! [`datafusion_proto::protobuf`]: https://docs.rs/datafusion-proto/latest/datafusion_proto/protobuf/index.html

pub mod from_proto;
pub mod generated;
pub mod registry;
pub mod to_proto;

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
pub use registry::ProtoDecoderRegistry;

#[cfg(all(test, feature = "json"))]
mod tests {
    use crate::protobuf::FileScanExecConf;

    #[test]
    fn file_scan_preserve_order_json_distinguishes_false_from_absent() {
        for (json, preserve_order) in
            [(r#"{"preserveOrder":false}"#, Some(false)), ("{}", None)]
        {
            let config: FileScanExecConf = serde_json::from_str(json).unwrap();
            assert_eq!(config.preserve_order, preserve_order);
            assert_eq!(serde_json::to_string(&config).unwrap(), json);
        }
    }
}
