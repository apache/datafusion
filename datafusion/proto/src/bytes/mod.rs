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

//! Serialization / Deserialization to Bytes
use std::{collections::HashSet, sync::Arc};

use crate::{from_proto::parse_expr, protobuf};
use datafusion::{
    common::{DataFusionError, Result},
    logical_expr::{AggregateUDF, ScalarUDF},
    logical_plan::{Expr, FunctionRegistry},
};
use prost::{bytes::BytesMut, Message};

// Reexport Bytes which appears in the API
pub use prost::bytes::Bytes;

/// Encodes an [`Expr`] into a stream of bytes. See
/// [`deserialize_expr`] to convert a stream of bytes back to an Expr
///
/// Open Questions:
/// Should this be its own crate / API (aka datafusion-serde?) that can be implemented using proto?
///
///
/// Example:
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::logical_plan::Expr;
/// use datafusion_proto::bytes::Serializeable;
///
/// // Create a new `Expr` a < 32
/// let expr = col("a").lt(lit(5i32));
///
/// // Convert it to an opaque form
/// let bytes = expr.to_bytes().unwrap();
///
/// // Decode bytes from somewhere (over network, etc.
/// let decoded_expr = Expr::from_bytes(&bytes).unwrap();
/// assert_eq!(expr, decoded_expr);
/// ```
pub trait Serializeable: Sized {
    /// Convert `self` to a serialized form (the internal format is not guaranteed)
    fn to_bytes(&self) -> Result<Bytes>;

    /// Convenience wy to convert the `bytes` (output of [`to_bytes`]
    /// back into an [`Expr']. This will error if there are any user
    /// defined functions, in which case use [`from_bytes_with_registry`]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes_with_registry(bytes, &NoRegistry {})
    }

    /// convert the output of serialize back into an Expr, given the
    /// specfified function registry for resolving UDFs
    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self>;
}

impl Serializeable for Expr {
    fn to_bytes(&self) -> Result<Bytes> {
        let mut buffer = BytesMut::new();
        let protobuf: protobuf::LogicalExprNode = self.try_into().map_err(|e| {
            DataFusionError::Plan(format!("Error encoding expr as protobuf: {}", e))
        })?;

        protobuf.encode(&mut buffer).map_err(|e| {
            DataFusionError::Plan(format!("Error encoding protobuf as bytes: {}", e))
        })?;

        Ok(buffer.into())
    }

    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self> {
        let protobuf = protobuf::LogicalExprNode::decode(bytes).map_err(|e| {
            DataFusionError::Plan(format!("Error decoding expr as protobuf: {}", e))
        })?;

        parse_expr(&protobuf, registry).map_err(|e| {
            DataFusionError::Plan(format!("Error parsing protobuf into Expr: {}", e))
        })
    }
}

/// TODO Move this function registry into its own module
///
/// A default registry that does not have any user defined functions supplied
struct NoRegistry {}

impl FunctionRegistry for NoRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Function '{}'", name))
        )
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{}'", name))
        )
    }
}
