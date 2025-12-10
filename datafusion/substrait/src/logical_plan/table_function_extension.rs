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

//! Protobuf message definitions for table function extensions in Substrait.
//!
//! This module defines the shared protobuf message structure used by both the
//! producer and consumer to encode/decode table function metadata in ReadRel
//! advanced_extension fields.

/// Protobuf message for encoding table function metadata in Substrait ReadRel.
///
/// This message is embedded in a `ReadRel.advanced_extension` field using the
/// [`TABLE_FUNCTION_TYPE_URL`](super::constants::TABLE_FUNCTION_TYPE_URL) type URL.
/// It allows the producer to record the function name and evaluated arguments,
/// enabling consumers to reconstruct table function calls during deserialization.
///
/// # Example
///
/// For a query like `SELECT * FROM generate_series(1, 10, 2)`, the producer will:
/// 1. Create a `TableFunctionReadRelExtension` with:
///    - `name = "generate_series"`
///    - `arguments = [Literal(1), Literal(10), Literal(2)]`
/// 2. Encode it into a protobuf `Any` message
/// 3. Attach it to the `ReadRel.advanced_extension`
///
/// The consumer can then decode this metadata and recreate the table function call.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableFunctionReadRelExtension {
    /// The name of the table function (e.g., "generate_series", "range")
    #[prost(string, tag = "1")]
    pub name: String,

    /// The evaluated argument literals passed to the function.
    /// For table functions with optional parameters, this contains the
    /// complete argument list after defaults and type coercions have been applied.
    #[prost(message, repeated, tag = "2")]
    pub arguments: Vec<substrait::proto::expression::Literal>,
}
