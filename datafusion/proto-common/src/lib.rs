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

//! Serialize / Deserialize DataFusion Primitive Types to bytes
//!
//! This crate provides support for serializing and deserializing the
//! following structures to and from bytes:
//!
//! 1. [`ScalarValue`]'s
//!
//! [`ScalarValue`]: datafusion_common::ScalarValue
//!
//! Internally, this crate is implemented by converting the common types to [protocol
//! buffers] using [prost].
//!
//! [protocol buffers]: https://developers.google.com/protocol-buffers
//! [prost]: https://docs.rs/prost/latest/prost/
//!
//! # Version Compatibility
//!
//! The serialized form are not guaranteed to be compatible across
//! DataFusion versions. A plan serialized with one version of DataFusion
//! may not be able to deserialized with a different version.
//!
//! # See Also
//!
//! The binary format created by this crate supports the full range of DataFusion
//! plans, but is DataFusion specific. See [datafusion-substrait] for a crate
//! which can encode many DataFusion plans using the [substrait.io] standard.
//!
//! [datafusion-substrait]: https://docs.rs/datafusion-substrait/latest/datafusion_substrait
//! [substrait.io]: https://substrait.io
//!
//! # Example: Serializing [`ScalarValue`]s
//! TODO: Add example

pub mod from_proto;
pub mod generated;
pub mod to_proto;

pub use generated::datafusion_proto_common as protobuf_common;

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);
